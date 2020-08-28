# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    AbstractSet,
    ClassVar,
    FrozenSet,
    Generic,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)

import sqlalchemy

from ..named import NamedKeyDict, NamedKeyMapping, NamedValueAbstractSet
from ..timespan import DatabaseTimespanRepresentation
from ._elements import (
    Dimension,
    DimensionElement,
    DimensionGroup,
    DimensionUniverse,
    RelationshipFamily,
    SpatialFamily,
    TemporalFamily,
)


@dataclass
class QueryVertexSelectResult:
    selectable: sqlalchemy.sql.FromClause
    dimensions: NamedKeyMapping[Dimension, sqlalchemy.sql.ColumnElement]
    region: Optional[sqlalchemy.sql.ColumnElement]
    timespan: Optional[DatabaseTimespanRepresentation]
    extra: Mapping[str, sqlalchemy.sql.ColumnElement]


@dataclass
class QueryVertexSelectSpec:
    extra: Set[str]
    spatial: bool = False
    temporal: bool = False


class QueryVertex(ABC):

    name: str

    @property
    @abstractmethod
    def dimension_sets(self) -> Iterator[NamedValueAbstractSet[Dimension]]:
        raise NotImplementedError()

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return None

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return None

    @property
    def spatial_edges_embedded(self) -> Iterator[QueryEdge]:
        yield from ()

    @property
    def temporal_edges_embedded(self) -> Iterator[QueryEdge]:
        yield from ()

    @abstractmethod
    def select(self, spec: QueryVertexSelectSpec) -> QueryVertexSelectResult:
        raise NotImplementedError()


class QueryEdge(Tuple[QueryVertex, QueryVertex]):

    def __new__(cls, first: QueryVertex, second: QueryVertex) -> QueryEdge:
        # mypy claims __new__ needs an iterable below, but complains about
        # passing it a tuple.  Seems like a bug, so these ignores can
        # hopefully go away someday.
        if first.name < second.name:
            return super().__new__(cls, (first, second))  # type: ignore
        elif first.name > second.name:
            return super().__new__(cls, (second, first))  # type: ignore
        else:
            raise ValueError(
                f"Identical or baf vertices for edge: {first.name}, {second.name}."
            )

    @property
    def names(self) -> Tuple[str, str]:
        return (self[0].name, self[1].name)


F = TypeVar("F", bound=RelationshipFamily)


class QueryJoinGenerator(Generic[F]):

    Auto: ClassVar[Type[AutoIntersect]]
    Manual: ClassVar[Type[ManualEdges]]

    @abstractmethod
    def visit(self, graph: QueryGraph, embedded: AbstractSet[QueryEdge]) -> Iterator[QueryEdge]:
        pass


class AutoIntersect(QueryJoinGenerator[F]):
    overrides: NamedKeyMapping[F, QueryVertex]

    def visit(self, graph: QueryGraph, embedded: AbstractSet[QueryEdge]) -> Iterator[QueryEdge]:
        family_names = {}


class ManualEdges(QueryJoinGenerator[F]):
    edges: Set[QueryEdge]

    def visit(self, graph: QueryGraph, embedded: AbstractSet[QueryEdge]) -> Iterator[QueryEdge]:
        yield from self.edges


QueryJoinGenerator.Auto = AutoIntersect
QueryJoinGenerator.Manual = ManualEdges


class QueryWhereExpression:

    dimensions: NamedValueAbstractSet[Dimension]
    """All dimensions referenced by the expression, including dimensions
    recursively referenced by the keys of `metadata` (`NamedValueAbstractSet`
    of `Dimension`).
    """

    extra: NamedKeyMapping[QueryVertex, Set[str]]
    """All fields referenced by the expression, other than the primary keys of
    dimensions (`NamedKeyMapping` mapping `QueryVertex` to a `set` of field
    names).
    """


class DimensionManager(ABC):

    @abstractmethod
    def make_query_vertex(self, element: DimensionElement) -> Optional[QueryVertex]:
        raise NotImplementedError()

    @abstractmethod
    def get_vertices_for_spatial_edge(self, edge: QueryEdge) -> Iterator[QueryVertex]:
        raise NotImplementedError()

    @abstractmethod
    def get_vertices_for_temporal_edge(self, edge: QueryEdge) -> Iterator[QueryVertex]:
        raise NotImplementedError()


K = TypeVar("K")


class SupersetAccumulator(Generic[K]):

    def __init__(self, sets: Iterable[AbstractSet[K]] = ()):
        self._data: Set[FrozenSet[K]] = set()
        for s in sets:
            self.add(s)

    def __contains__(self, s: AbstractSet[K]) -> bool:
        for existing in self._data:
            if existing.issuperset(s):
                return True
            if existing.issubset(s):
                return False
        return False

    def add(self, s: AbstractSet[K]) -> None:
        to_drop: Set[FrozenSet[K]] = set()
        for existing in self._data:
            if existing.issuperset(s):
                return
            if existing.issubset(s):
                to_drop.add(existing)
        self._data -= to_drop
        self._data.add(frozenset(s))


class QueryGraph:

    @property
    def universe(self) -> DimensionUniverse:
        return self.requested_dimensions.universe

    @property
    def full_dimensions(self) -> DimensionGroup:
        names = set(self.requested_dimensions.names)
        names.update(self.where_expression.dimensions.names)
        for vertex in self.extra_vertices:
            for dimension_set in vertex.dimension_sets:
                names.update(dimension_set.names)
        return self.universe.group(names)

    def compute_select_specs(
        self,
        manager: DimensionManager
    ) -> NamedKeyMapping[QueryVertex, QueryVertexSelectSpec]:
        result: NamedKeyDict[QueryVertex, QueryVertexSelectSpec] = NamedKeyDict()
        dimension_sets_seen: SupersetAccumulator[str] = SupersetAccumulator()
        spatial_edges_seen: Set[QueryEdge] = set()
        temporal_edges_seen: Set[QueryEdge] = set()

        def add_vertex(v: QueryVertex) -> QueryVertexSelectSpec:
            spec = result.get(v)
            if spec is None:
                spec = QueryVertexSelectSpec(extra=self.where_expression.extra.get(v, ()))
                result[v] = spec
                for dimension_set in v.dimension_sets:
                    dimension_sets_seen.add(dimension_set.names)
                spatial_edges_seen.update(v.spatial_edges_embedded)
                temporal_edges_seen.update(v.temporal_edges_embedded)
            return spec

        for vertex in self.extra_vertices:
            add_vertex(vertex)
        for vertex in self.where_expression.extra.keys():
            add_vertex(vertex)
        for edge in self.spatial_join_generator.visit(self, spatial_edges_seen):
            for join_vertex in manager.get_vertices_for_spatial_edge(edge):
                add_vertex(vertex).spatial = True
        for edge in self.temporal_join_generator.visit(self, temporal_edges_seen):
            for join_vertex in manager.get_vertices_for_temporal_edge(edge):
                add_vertex(vertex).temporal = True
        for element in reversed(list(self.full_dimensions.elements)):
            element_vertex = manager.make_query_vertex(element)
            if (element_vertex is not None
                    and any(ds.names not in dimension_sets_seen for ds in element_vertex.dimension_sets)):
                add_vertex(element_vertex)
        return result.freeze()

    requested_dimensions: DimensionGroup
    spatial_join_generator: QueryJoinGenerator[SpatialFamily]
    temporal_join_generator: QueryJoinGenerator[TemporalFamily]
    where_expression: QueryWhereExpression
    extra_vertices: NamedValueAbstractSet[QueryVertex]
