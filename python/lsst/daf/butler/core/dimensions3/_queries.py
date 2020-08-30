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
from collections import defaultdict
from dataclasses import dataclass
import itertools
from typing import (
    AbstractSet,
    ClassVar,
    Dict,
    FrozenSet,
    Generic,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)

import sqlalchemy

from ..named import NamedKeyDict, NamedKeyMapping, NamedValueAbstractSet, NamedValueSet
from ..timespan import DatabaseTimespanRepresentation
from ._elements import (
    Dimension,
    DimensionElement,
    DimensionGroup,
    DimensionUniverse,
    RelationshipCategory,
    RelationshipFamily,
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
    relationships: Set[RelationshipCategory]


class QueryVertex(ABC):

    name: str

    @property
    @abstractmethod
    def dimension_sets(self) -> Iterator[NamedValueAbstractSet[Dimension]]:
        raise NotImplementedError()

    @property
    def families(self) -> Mapping[RelationshipCategory, RelationshipFamily]:
        return {}

    @property
    def embedded_relationship_edges(self) -> Mapping[RelationshipCategory, AbstractSet[QueryEdge]]:
        return {}

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


class QueryEdgeGenerator(ABC):

    Auto: ClassVar[Type[AutoIntersect]]
    Manual: ClassVar[Type[ManualEdges]]

    @abstractmethod
    def visit(
        self,
        graph: QueryGraph,
        category: RelationshipCategory,
        manager: DimensionManager,
    ) -> Iterator[QueryEdge]:
        raise NotImplementedError()


class AutoIntersect(QueryEdgeGenerator):
    overrides: NamedKeyMapping[RelationshipFamily, QueryVertex]

    def visit(
        self,
        graph: QueryGraph,
        category: RelationshipCategory,
        manager: DimensionManager,
    ) -> Iterator[QueryEdge]:
        best_vertices_by_family: NamedKeyDict[RelationshipFamily, QueryVertex] = NamedKeyDict()
        for vertex in graph.extra_vertices:
            family = vertex.families.get(category)
            if family is not None and family not in self.overrides:
                if family in best_vertices_by_family:
                    raise RuntimeError(
                        f"Cannot compute automatic intersection because both {vertex.name} "
                        f"and {best_vertices_by_family[family.name]} are associated with the same "
                        f"relationship ({category.name}) family, and no automatic preference exists."
                    )
                best_vertices_by_family[family.name] = vertex
        dimension_elements_by_family: NamedKeyDict[RelationshipFamily, List[DimensionElement]] \
            = NamedKeyDict()
        for element in graph.full_dimensions.elements:
            family = element.families.get(category)
            if family is not None and family not in self.overrides:
                if family not in best_vertices_by_family:
                    # A QueryVertex in extra_vertices always supersedes
                    # a DimensionElement, but there isn't one for this family.
                    dimension_elements_by_family[family].append(element)
        for family, elements in dimension_elements_by_family.items():
            best_element = family.choose(elements)
            element_vertex = manager.make_query_vertex(best_element)
            # FIXME: element_vertex might be None
            best_vertices_by_family[family.name] = element_vertex
        best_vertices_by_family.update(self.overrides)
        # TODO: avoid embedded edges
        for v1, v2 in itertools.combinations(best_vertices_by_family.values(), 2):
            yield QueryEdge(v1, v2)


class ManualEdges(QueryEdgeGenerator):
    edges: Set[QueryEdge]

    def visit(
        self,
        graph: QueryGraph,
        category: RelationshipCategory,
        manager: DimensionManager,
    ) -> Iterator[QueryEdge]:
        yield from self.edges


QueryEdgeGenerator.Auto = AutoIntersect
QueryEdgeGenerator.Manual = ManualEdges


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
    def get_vertices_for_relationship_edge(
        self,
        edge: QueryEdge,
        category: RelationshipCategory,
    ) -> Iterator[QueryVertex]:
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

    def gather_relationship_vertices(
        self,
        manager: DimensionManager,
        category: RelationshipCategory,
    ) -> NamedKeyMapping[RelationshipFamily, NamedValueAbstractSet[QueryVertex]]:
        result: NamedKeyDict[RelationshipFamily, NamedValueSet[QueryVertex]] = NamedKeyDict()
        for element in self.full_dimensions.elements:
            family = element.families.get(category)
            if family is not None:
                members = result.get(family)
                if members is None:
                    members = NamedValueSet()
                    result[family] = members
                vertex = manager.make_query_vertex(element)
                assert vertex is not None
                members.add(vertex)
        for vertex in self.extra_vertices:
            family = element.families.get(category)
            if family is not None:
                members = result.get(family)
                if members is None:
                    members = NamedValueSet()
                    result[family] = members
                members.add(vertex)
        return result.freeze()

    def compute_select_specs(
        self,
        manager: DimensionManager,
    ) -> NamedKeyMapping[QueryVertex, QueryVertexSelectSpec]:
        result: NamedKeyDict[QueryVertex, QueryVertexSelectSpec] = NamedKeyDict()
        dimension_sets_seen: SupersetAccumulator[str] = SupersetAccumulator()
        embedded_relationship_edges: Dict[RelationshipCategory, Set[QueryEdge]] = defaultdict(set)

        def add_vertex(v: QueryVertex) -> QueryVertexSelectSpec:
            spec = result.get(v)
            if spec is None:
                spec = QueryVertexSelectSpec(
                    extra=self.where_expression.extra.get(v, ()),
                    relationships=set(),
                )
                result[v] = spec
                for dimension_set in v.dimension_sets:
                    dimension_sets_seen.add(dimension_set.names)
                for category, edges in v.embedded_relationship_edges.items():
                    embedded_relationship_edges[category].update(edges)
            return spec

        for vertex in self.extra_vertices:
            add_vertex(vertex)
        for vertex in self.where_expression.extra.keys():
            add_vertex(vertex)
        del vertex
        for category in RelationshipCategory.__members__.values():
            edge_iter = self.edge_generators[category].visit(self, category, manager)
            for edge in edge_iter:
                for join_vertex in manager.get_vertices_for_relationship_edge(edge, category):
                    add_vertex(join_vertex).relationships.add(category)
        for element in reversed(list(self.full_dimensions.elements)):
            element_vertex = manager.make_query_vertex(element)
            if (element_vertex is not None
                    and any(ds.names not in dimension_sets_seen for ds in element_vertex.dimension_sets)):
                add_vertex(element_vertex)
        return result.freeze()

    requested_dimensions: DimensionGroup
    edge_generators: Mapping[RelationshipCategory, QueryEdgeGenerator]
    where_expression: QueryWhereExpression
    extra_vertices: NamedValueAbstractSet[QueryVertex]
