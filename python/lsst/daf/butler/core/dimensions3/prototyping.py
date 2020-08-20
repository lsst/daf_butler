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
import itertools
from typing import (
    AbstractSet,
    Any,
    ClassVar,
    Generic,
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
from ..named import NamedKeyMapping


K_co = TypeVar("K_co", covariant=True)


# Will be replaced with a better NamedValueSet later; current one is mutable
# and doesn't do type covariance the way we want.
class AbstractNamedValueSet(AbstractSet[K_co]):
    pass


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

    @property
    def dimension_group(self) -> DimensionGroup:
        raise NotImplementedError("TODO")

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return None

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return None

    @abstractmethod
    def get_dimension_columns(self) -> Iterator[Tuple[Dimension, str]]:
        pass

    @abstractmethod
    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        pass

    def get_unique_constraints(self) -> Iterator[Tuple[str, ...]]:
        return iter(())


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
    def primary_key(self) -> ddl.FieldSpec:
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

    def get_dimension_columns(self) -> Iterator[Tuple[Dimension, str]]:
        yield (self, self.primary_key.name)

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        yield self.primary_key


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
    def primary_key(self) -> ddl.FieldSpec:
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

    def __lt__(self, other: Dimension) -> bool:
        if isinstance(other, LabelDimension):
            return self._name < other._name
        return super().__lt__(other)

    @property
    def primary_key(self) -> ddl.FieldSpec:
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
        requires: AbstractNamedValueSet[StandardDimension],
        implies: AbstractNamedValueSet[StandardDimension],
        spatial_family: Optional[SpatialFamily],
        temporal_family: Optional[TemporalFamily],
        primary_key: ddl.FieldSpec,
        unique_constraints: AbstractSet[Tuple[str, ...]],
        metadata: AbstractNamedValueSet[ddl.FieldSpec],
        index: int,
    ):
        self._name = name
        self._universe = universe
        self.requires = requires
        self.implies = implies
        self._spatial_family = spatial_family
        self._temporal_family = temporal_family
        self._primary_key = primary_key
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

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return self._spatial_family

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return self._temporal_family

    def __lt__(self, other: Dimension) -> bool:
        if isinstance(other, StandardDimension):
            return self._index < other._index
        return NotImplemented

    @property
    def primary_key(self) -> ddl.FieldSpec:
        return self._primary_key

    def get_dimension_columns(self) -> Iterator[Tuple[Dimension, str]]:
        for dimension in self.requires:
            yield (dimension, dimension.name)
        yield (self, self.primary_key.name)
        for dimension in self.implies:
            yield (dimension, dimension.name)

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        for dimension in self.requires:
            yield dataclasses.replace(
                dimension.primary_key,
                name=dimension.name,
                primaryKey=False,
                nullable=False,
            )
        yield self.primary_key
        for dimension in self.implies:
            yield dataclasses.replace(
                dimension.primary_key,
                name=dimension.name,
                primaryKey=False,
                nullable=True,
            )
        yield from self._metadata

    def get_unique_constraints(self) -> Iterator[Tuple[str, ...]]:
        return iter(self._unique_constraints)

    requires: AbstractNamedValueSet[StandardDimension]

    implies: AbstractNamedValueSet[StandardDimension]


class DimensionCombination(QueryVertex):

    def __init__(
        self,
        name: str,
        universe: DimensionUniverse, *,
        combines: DimensionGroup,
        spatial_family: Optional[SpatialFamily],
        temporal_family: Optional[TemporalFamily],
        metadata: AbstractNamedValueSet[ddl.FieldSpec],
    ):
        self._name = name
        self._universe = universe
        self.combines = combines
        self._spatial_family = spatial_family
        self._temporal_family = temporal_family
        self._metadata = metadata

    @property
    def name(self) -> str:
        return self._name

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return self._spatial_family

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return self._temporal_family

    def get_dimension_columns(self) -> Iterator[Tuple[Dimension, str]]:
        for dimension in self.combines.required:
            yield (dimension, dimension.name)

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        for dimension, name in self.get_dimension_columns():
            yield dataclasses.replace(dimension.primary_key, name=name)
        yield from self._metadata

    combines: DimensionGroup


class DimensionAggregateDefinition(QueryVertex):

    def __init__(
        self,
        name: str,
        universe: DimensionUniverse, *,
        parent: StandardDimension,
        child: Dimension,
        mediator: StandardDimension,
        spatial_family: Optional[SpatialFamily],
        temporal_family: Optional[TemporalFamily],
        metadata: AbstractNamedValueSet[ddl.FieldSpec],
    ):
        self._name = name
        self._universe = universe
        self.parent = parent
        self.child = child
        self.mediator = mediator
        self._spatial_family = spatial_family
        self._temporal_family = temporal_family
        self._metadata = metadata
        # TODO: make asserts into better exceptions
        assert mediator in parent.requires or mediator in parent.implies
        assert (parent.spatial_family is None or child.spatial_family is None
                or parent.spatial_family == child.spatial_family)
        assert (parent.temporal_family is None or child.temporal_family is None
                or parent.temporal_family == child.temporal_family)

    @property
    def name(self) -> str:
        return self._name

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    def get_dimension_columns(self) -> Iterator[Tuple[Dimension, str]]:
        seen: Set[str] = set()
        for dimension, _ in itertools.chain(self.mediator.get_dimension_columns(),
                                            self.child.get_dimension_columns(),
                                            self.parent.get_dimension_columns()):
            if dimension.name not in seen:
                yield (dimension, dimension.name)
                seen.add(dimension.name)

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        seen: Set[str] = set()
        for dimension, _ in self.mediator.get_dimension_columns():
            if dimension.name not in seen:
                yield dataclasses.replace(
                    dimension.primary_key,
                    name=dimension.name,
                )
                seen.add(dimension.name)
        for dimension, _ in self.child.get_dimension_columns():
            if dimension.name not in seen:
                yield dataclasses.replace(
                    dimension.primary_key,
                    name=dimension.name,
                )
                seen.add(dimension.name)
        for dimension, _ in self.parent.get_dimension_columns():
            if dimension.name not in seen:
                yield dataclasses.replace(
                    dimension.primary_key,
                    name=dimension.name,
                    primaryKey=False,
                    nullable=False,
                )
                seen.add(dimension.name)
        yield from self._metadata

    # TODO: in some contexts, we _could_ define unique constraints here.
    #  - How do they depend on whether mediator is required by or implied by
    #    parent?
    #  - Do we need to, or are they already enforced indirectly?

    parent: StandardDimension
    child: Dimension
    mediator: StandardDimension


F = TypeVar("F", bound=RelationshipFamily)


class MaterializedOverlap(QueryVertex, Generic[F]):

    def __init__(
        self,
        name: str,
        universe: DimensionUniverse, *,
        relates: Tuple[NamedKeyMapping[F, QueryVertex], NamedKeyMapping[F, QueryVertex]]
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

    def get_dimension_columns(self) -> Iterator[Tuple[Dimension, str]]:
        seen: Set[str] = set()
        for side in self.relates:
            for vertex in side.values():
                for dimension, _ in vertex.get_dimension_columns():
                    if dimension.name not in seen:
                        yield (dimension, dimension.name)
                        seen.add(dimension.name)

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        for dimension, name in self.get_dimension_columns():
            yield dataclasses.replace(dimension.primary_key, name=name)

    relates: Tuple[NamedKeyMapping[F, QueryVertex], NamedKeyMapping[F, QueryVertex]]


class DimensionGroup:

    @property
    def names(self) -> AbstractSet[str]:
        pass

    def __iter__(self) -> Iterator[Dimension]:
        pass

    # TODO: more container interface

    def asSet(self) -> AbstractNamedValueSet[Dimension]:
        pass

    universe: DimensionUniverse
    required: AbstractNamedValueSet[Dimension]
    implied: AbstractNamedValueSet[Dimension]
    aggregations: AbstractNamedValueSet[DimensionAggregateDefinition]
    combinations: AbstractNamedValueSet[DimensionCombinationSupplement]


class QueryRelationship(Generic[F]):

    FamilyCombinations: ClassVar[Type[FamilyCombinations]]
    ManualEdges: ClassVar[Type[ManualEdges]]

    @abstractmethod
    def visit(self, dimensions: DimensionGroup) -> AbstractNamedValueSet[QueryVertex]:
        pass


class FamilyCombinations(QueryRelationship[F]):
    overrides: NamedKeyMapping[F, QueryVertex]

    def visit(self, dimensions: DimensionGroup) -> AbstractNamedValueSet[QueryVertex]:
        pass


class ManualEdges(QueryRelationship[F]):
    edges: Set[Tuple[QueryVertex, QueryVertex]]

    def visit(self, dimensions: DimensionGroup) -> AbstractNamedValueSet[QueryVertex]:
        pass


QueryRelationship.FamilyCombinations = FamilyCombinations
QueryRelationship.ManualEdges = ManualEdges


class QueryGraph:

    requested: DimensionGroup

    spatial: QueryRelationship[SpatialFamily]
    temporal: QueryRelationship[TemporalFamily]

    extra: AbstractNamedValueSet[QueryVertex]

    def get_vertices(self) -> Iterator[QueryVertex]:
        raise NotImplementedError("TODO")
