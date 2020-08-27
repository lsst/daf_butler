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
import itertools
from typing import (
    ClassVar,
    FrozenSet,
    Generic,
    Iterator,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)

import sqlalchemy

from ..named import NamedKeyMapping, NamedValueAbstractSet
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
class QueryJoinable:
    selectable: sqlalchemy.sql.FromClause
    dimensions: NamedKeyMapping[Dimension, sqlalchemy.sql.ColumnElement]
    timespan: Optional[DatabaseTimespanRepresentation]
    extra: Mapping[str, sqlalchemy.sql.ColumnElement]


class QueryVertex(ABC):

    name: str

    @property
    @abstractmethod
    def dimensions(self) -> NamedValueAbstractSet[Dimension]:
        raise NotImplementedError()

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return None

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return None


F = TypeVar("F", bound=RelationshipFamily)


class QueryJoinGenerator(Generic[F]):

    FamilyCombinations: ClassVar[Type[FamilyCombinations]]
    ManualEdges: ClassVar[Type[ManualEdges]]

    @abstractmethod
    def visit(self, dimensions: DimensionGroup, manager: DimensionManager) -> Iterator[QueryVertex]:
        pass


class FamilyCombinations(QueryJoinGenerator[F]):
    overrides: NamedKeyMapping[F, QueryVertex]

    def visit(self, dimensions: DimensionGroup, manager: DimensionManager) -> Iterator[QueryVertex]:
        raise NotImplementedError("TODO")


class ManualEdges(QueryJoinGenerator[F]):
    edges: Set[Tuple[QueryVertex, QueryVertex]]

    def visit(self, dimensions: DimensionGroup, manager: DimensionManager) -> Iterator[QueryVertex]:
        raise NotImplementedError("TODO")


QueryJoinGenerator.FamilyCombinations = FamilyCombinations
QueryJoinGenerator.ManualEdges = ManualEdges


class QueryWhereExpression:

    dimensions: NamedValueAbstractSet[Dimension]
    """All dimensions referenced by the expression, including dimensions
    recursively referenced by the keys of `metadata` (`NamedValueAbstractSet`
    of `Dimension`).
    """

    metadata: NamedKeyMapping[QueryVertex, Set[str]]
    """All fields referenced by the expression, other than the primary keys of
    dimensions (`NamedKeyMapping` mapping `QueryVertex` to a `set` of field
    names).
    """


class DimensionManager(ABC):

    @abstractmethod
    def make_query_vertex(self, element: DimensionElement) -> Optional[QueryVertex]:
        raise NotImplementedError()


class QueryGraph:

    @property
    def universe(self) -> DimensionUniverse:
        return self.requested_dimensions.universe

    @property
    def full_dimensions(self) -> DimensionGroup:
        names = set(self.requested_dimensions.names)
        names.update(self.where_expression.dimensions.names)
        for vertex in self.extra_vertices:
            names.update(vertex.dimensions.names)
        return self.universe.group(names)

    def compute_vertices(self, manager: DimensionManager) -> Iterator[QueryVertex]:
        dimension_name_sets_seen: Set[FrozenSet[str]] = set()
        vertex_names_seen: Set[str] = set()

        def process_vertex(v: QueryVertex) -> Iterator[QueryVertex]:
            if v.name not in vertex_names_seen:
                yield v
                vertex_names_seen.add(v.name)
                dimension_name_sets_seen.add(frozenset(v.dimensions.names))

        necessary: Iterator[QueryVertex] = itertools.chain(
            self.extra_vertices,
            self.temporal_join_generator.visit(self.full_dimensions, manager),
            self.spatial_join_generator.visit(self.full_dimensions, manager),
        )
        for vertex in necessary:
            yield from process_vertex(vertex)
        for element in reversed(list(self.full_dimensions.elements)):
            element_vertex = manager.make_query_vertex(element)
            if element_vertex is None:
                continue
            is_necessary = (
                # Element has a metadata column referenced by the WHERE
                # expression.
                self.where_expression.metadata.get(element_vertex)
                # Element references a dimension that isn't yet in the query.
                or frozenset(element_vertex.dimensions.names) not in dimension_name_sets_seen
            )
            if is_necessary:
                yield from process_vertex(element_vertex)

    requested_dimensions: DimensionGroup
    spatial_join_generator: QueryJoinGenerator[SpatialFamily]
    temporal_join_generator: QueryJoinGenerator[TemporalFamily]
    where_expression: QueryWhereExpression
    extra_vertices: NamedValueAbstractSet[QueryVertex]
