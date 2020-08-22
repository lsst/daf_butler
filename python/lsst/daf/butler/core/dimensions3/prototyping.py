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
import dataclasses
from typing import (
    AbstractSet,
    Any,
    ClassVar,
    Generic,
    Iterable,
    Iterator,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)

import sqlalchemy
from lsst.sphgeom import Pixelization

from .. import ddl
from ..named import NamedKeyDict, NamedKeyMapping, NamedValueSet, NamedValueAbstractSet
from ..simpleQuery import SimpleQuery


class DimensionUniverse:
    pass


class RelationshipFamily(ABC):
    name: str
    universe: DimensionUniverse


class SpatialFamily(RelationshipFamily):

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, SpatialFamily):
            return self.name == other.name
        return False


class TemporalFamily(RelationshipFamily):

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, TemporalFamily):
            return self.name == other.name
        return False


class QueryVertex(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def universe(self) -> DimensionUniverse:
        pass

    @abstractmethod
    def is_selectable(self) -> bool:
        raise NotImplementedError()

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return None

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return None

    @property
    def dimensions(self) -> DimensionGroup:
        raise NotImplementedError("TODO")

    @property
    def requires(self) -> NamedValueAbstractSet[Dimension]:
        return NamedValueSet().freeze()

    @property
    def implies(self) -> NamedValueAbstractSet[Dimension]:
        return NamedValueSet().freeze()

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        for dimension in self.requires:
            yield dataclasses.replace(dimension.key_field_spec, name=dimension.name)
        for dimension in self.implies:
            yield dataclasses.replace(dimension.key_field_spec, name=dimension.name,
                                      primary_key=False, nullable=True)

    def get_unique_constraints(self) -> Iterator[Tuple[str, ...]]:
        yield from ()


class Dimension(QueryVertex):

    _subclass_count: ClassVar[int] = 0
    _subclass_index: ClassVar[int]

    @classmethod
    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        # TODO: guard against multi-level inheritance or check that it works.
        cls._subclass_index = Dimension._subclass_count
        Dimension._subclass_count += 1

    @property
    @abstractmethod
    def key_field_spec(self) -> ddl.FieldSpec:
        raise NotImplementedError()

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Dimension):
            return False
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(self.name)

    @abstractmethod
    def __lt__(self, other: Dimension) -> bool:
        return self._subclass_index < other._subclass_index

    def __gt__(self, other: Dimension) -> bool:
        return self != other and not self < other

    def __le__(self, other: Dimension) -> bool:
        return self == other or self < other

    def __ge__(self, other: Dimension) -> bool:
        return self == other or not self < other

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        for dimension in self.requires:
            yield dataclasses.replace(
                dimension.key_field_spec,
                name=dimension.name,
            )
        yield self.key_field_spec
        for dimension in self.implies:
            yield dataclasses.replace(
                dimension.key_field_spec,
                name=dimension.name,
                primaryKey=False,
                nullable=True,
            )


class SkyPixSpatialFamily(SpatialFamily):
    max_level: int
    pixelization_cls: Type[Pixelization]


class SkyPixDimension(Dimension):

    def __init__(self, family: SkyPixSpatialFamily, level: int):
        self._name = f"{family.name}{level}"
        self._family = family
        self.level = level
        self.pixelization = self._family.pixelization_cls(level)

    @property
    def name(self) -> str:
        return self._name

    @property
    def universe(self) -> DimensionUniverse:
        return self._family.universe

    def is_selectable(self) -> bool:
        return False

    @property
    def spatial_family(self) -> SkyPixSpatialFamily:
        return self._family

    def __lt__(self, other: Dimension) -> bool:
        if isinstance(other, SkyPixDimension):
            if self._family == other._family:
                return self.level < other.level
            else:
                return self._family.name < other._family.name
        return super().__lt__(other)

    @property
    def key_field_spec(self) -> ddl.FieldSpec:
        return ddl.FieldSpec(
            name=f"{self._family.name}_id",
            dtype=sqlalchemy.BigInteger,
            primaryKey=True,
            nullable=False
        )

    level: int
    pixelization: Pixelization


class LabelDimension(Dimension):

    def __init__(self, name: str, universe: DimensionUniverse):
        self._name = name
        self._universe = universe

    LABEL_COLUMN_LENGTH = 128

    @property
    def name(self) -> str:
        return self._name

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    def is_selectable(self) -> bool:
        return False

    def __lt__(self, other: Dimension) -> bool:
        if isinstance(other, LabelDimension):
            return self._name < other._name
        return super().__lt__(other)

    @property
    def key_field_spec(self) -> ddl.FieldSpec:
        return ddl.FieldSpec(
            name=self._name,
            dtype=sqlalchemy.String,
            length=self.LABEL_COLUMN_LENGTH,
            primaryKey=True,
            nullable=False
        )


class StandardDimension(Dimension):

    def __init__(
        self,
        name: str,
        universe: DimensionUniverse, *,
        requires: NamedValueAbstractSet[Dimension],
        implies: NamedValueAbstractSet[Dimension],
        spatial_family: Optional[SpatialFamily],
        temporal_family: Optional[TemporalFamily],
        key_field_spec: ddl.FieldSpec,
        unique_constraints: AbstractSet[Tuple[str, ...]],
        metadata: NamedValueAbstractSet[ddl.FieldSpec],
        index: int,
    ):
        self._name = name
        self._universe = universe
        self._requires = requires
        self._implies = implies
        self._spatial_family = spatial_family
        self._temporal_family = temporal_family
        self._key_field_spec = key_field_spec
        self._unique_constraints = unique_constraints
        self._metadata = metadata
        self._index = index
        # TODO: check that unique constraints are all valid field names.

    @property
    def name(self) -> str:
        return self._name

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    def is_selectable(self) -> bool:
        return True

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return self._spatial_family

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return self._temporal_family

    @property
    def requires(self) -> NamedValueAbstractSet[Dimension]:
        return self._requires

    @property
    def implies(self) -> NamedValueAbstractSet[Dimension]:
        return self._implies

    def __lt__(self, other: Dimension) -> bool:
        if isinstance(other, StandardDimension):
            return self._index < other._index
        return NotImplemented

    @property
    def key_field_spec(self) -> ddl.FieldSpec:
        return self._key_field_spec

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        yield from super().get_standard_field_specs()
        yield from self._metadata

    def get_unique_constraints(self) -> Iterator[Tuple[str, ...]]:
        return iter(self._unique_constraints)


class DimensionCombination(QueryVertex):

    def __init__(
        self,
        name: str,
        universe: DimensionUniverse, *,
        combines: DimensionGroup,
        implies: NamedValueAbstractSet[Dimension],
        spatial_family: Optional[SpatialFamily],
        temporal_family: Optional[TemporalFamily],
        metadata: NamedValueAbstractSet[ddl.FieldSpec],
    ):
        self._name = name
        self._universe = universe
        self.combines = combines
        self._implies = implies
        self._spatial_family = spatial_family
        self._temporal_family = temporal_family
        self._metadata = metadata

    @property
    def name(self) -> str:
        return self._name

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    def is_selectable(self) -> bool:
        return True

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return self._spatial_family

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return self._temporal_family

    @property
    def requires(self) -> NamedValueAbstractSet[Dimension]:
        return self.combines.required

    @property
    def implies(self) -> NamedValueAbstractSet[Dimension]:
        return self._implies

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        yield from super().get_standard_field_specs()
        yield from self._metadata

    combines: DimensionGroup


F = TypeVar("F", bound=RelationshipFamily)


class MaterializedOverlap(QueryVertex, Generic[F]):

    def __init__(
        self,
        name: str,
        universe: DimensionUniverse, *,
        relates: Tuple[Tuple[F, QueryVertex], Tuple[F, QueryVertex]]
    ):
        self._name = name
        self._universe = universe
        self.relates = relates

    @property
    def name(self) -> str:
        return self._name

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    def is_selectable(self) -> bool:
        return True

    @property
    def requires(self) -> NamedValueAbstractSet[Dimension]:
        return (self.relates[0][1].dimensions | self.relates[1][1].dimensions).required

    relates: Tuple[Tuple[F, QueryVertex], Tuple[F, QueryVertex]]


class DimensionGroup:

    def __init__(
        self,
        universe: DimensionUniverse, *,
        dimensions: Optional[Iterable[Dimension]] = None,
        names: Optional[Iterable[str]] = None,
        conform: bool = True
    ):
        raise NotImplementedError("TODO")

    @property
    def names(self) -> AbstractSet[str]:
        raise NotImplementedError("TODO")

    def __iter__(self) -> Iterator[Dimension]:
        raise NotImplementedError("TODO")

    # TODO: more container interface

    def asSet(self) -> NamedValueAbstractSet[Dimension]:
        raise NotImplementedError("TODO")

    def __or__(self, other: DimensionGroup) -> DimensionGroup:
        raise NotImplementedError("TODO")

    universe: DimensionUniverse
    required: NamedValueAbstractSet[Dimension]
    implied: NamedValueAbstractSet[Dimension]
    combinations: NamedValueAbstractSet[DimensionCombination]


class QueryRelationship(Generic[F]):

    FamilyCombinations: ClassVar[Type[FamilyCombinations]]
    ManualEdges: ClassVar[Type[ManualEdges]]

    @abstractmethod
    def visit(self, dimensions: DimensionGroup) -> Iterator[QueryVertex]:
        pass


class FamilyCombinations(QueryRelationship[F]):
    overrides: NamedKeyMapping[F, QueryVertex]

    def visit(self, dimensions: DimensionGroup) -> Iterator[QueryVertex]:
        raise NotImplementedError("TODO")


class ManualEdges(QueryRelationship[F]):
    edges: Set[Tuple[QueryVertex, QueryVertex]]

    def visit(self, dimensions: DimensionGroup) -> Iterator[QueryVertex]:
        raise NotImplementedError("TODO")


QueryRelationship.FamilyCombinations = FamilyCombinations
QueryRelationship.ManualEdges = ManualEdges


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


@dataclasses.dataclass
class JoinEntry:

    vertex: QueryVertex
    dimensions: Iterator[Tuple[Dimension, Iterator[QueryVertex]]]


class QueryGraph:

    requested_dimensions: DimensionGroup
    spatial: QueryRelationship[SpatialFamily]
    temporal: QueryRelationship[TemporalFamily]
    where_expression: QueryWhereExpression
    extra_vertices: NamedValueAbstractSet[QueryVertex]

    @property
    def universe(self) -> DimensionUniverse:
        return self.requested_dimensions.universe

    @property
    def full_dimensions(self) -> DimensionGroup:
        names = set(self.requested_dimensions.names)
        names.update(self.where_expression.dimensions.names)
        for vertex in self.extra_vertices:
            names.update(vertex.dimensions.names)
        return DimensionGroup(self.universe, names=names)

    def expand_necessary_vertices(self) -> Iterator[QueryVertex]:
        yield from self.extra_vertices
        yield from self.temporal.visit(self.full_dimensions)
        yield from self.spatial.visit(self.full_dimensions)

    def build(self):
        # {dimension.name}
        dimension_keys_seen: Set[str] = set()
        # {(implied_dimension.name, implying_vertex.name)}
        implied_dimension_relationships_seen: Set[Tuple[str, str]]
        # {vertex.name}
        vertices_seen: Set[str] = set()
        for vertex in self.expand_necessary_vertices():
            if vertex.name in vertices_seen:
                continue
            vertices_seen.add(vertex.name)
            join_on = {
                name: dimension_keys_selected[name]
                for dim in vertex.required.names
            }
            if vertex.implies:
                implied_dimension_relationships_seen.update(
                    (name, vertex.name)
                    for name in vertex.implies.names
                )
                join_on.update(
                    (name, dimension_keys_selected[name])
                    for name in vertex.implies.names
                )
            if isinstance(vertex, Dimension):
                join_on[vertex.name] = dimension_keys_selected[name]
            for dimension_name, previously_joined in join_on.items():
                
                