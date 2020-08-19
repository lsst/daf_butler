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
    Union,
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


class Vertex(ABC):

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def getDimensionColumns(self) -> Iterator[Tuple[Dimension, str]]:
        pass

    @property
    @abstractmethod
    def universe(self) -> DimensionUniverse:
        pass

    @property
    @abstractmethod
    def spatial(self) -> Optional[SpatialFamily]:
        pass

    @property
    @abstractmethod
    def temporal(self) -> Optional[TemporalFamily]:
        pass

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Vertex):
            return False
        return self.name == other.name

    @abstractmethod
    def __lt__(self, other: Vertex) -> bool:
        pass

    def __gt__(self, other: Vertex) -> bool:
        return self != other and not self < other

    def __le__(self, other: Vertex) -> bool:
        return self == other or self < other

    def __ge__(self, other: Vertex) -> bool:
        return self == other or not self < other

    name: str


class Dimension(Vertex):

    def __init__(self, name: str, key: ddl.FieldSpec):
        super().__init__(name)
        self.key = key
        assert self.key.primaryKey and not self.key.nullable

    key: ddl.FieldSpec


class DatabaseSpatialFamily(SpatialFamily):
    parent: DatabaseDimension
    children: AbstractNamedValueSet[Union[DatabaseDimension, Combination]]


class DatabaseDimension(Dimension):
    identifiers: AbstractNamedValueSet[ddl.FieldSpec]
    metadata: AbstractNamedValueSet[ddl.FieldSpec]
    requires: AbstractNamedValueSet[DatabaseDimension]
    implies: AbstractNamedValueSet[DatabaseDimension]

    def __init__(
        self,
        name: str,
        universe: DimensionUniverse, *,
        requires: AbstractNamedValueSet[DatabaseDimension],
        implies: AbstractNamedValueSet[DatabaseDimension],
        spatial: Optional[SpatialFamily],
        temporal: Optional[TemporalFamily],
        key: ddl.FieldSpec,
        identifiers: AbstractNamedValueSet[ddl.FieldSpec],
        metadata: AbstractNamedValueSet[ddl.FieldSpec],
        index: int,
    ):
        super().__init__(name=name, key=key)
        self.identifiers = identifiers
        self.metadata = metadata
        self.requires = requires
        self.implies = implies
        self._index = index
        self._universe = universe
        self._spatial = spatial
        self._temporal = temporal

    def getDimensionColumns(self) -> Iterator[Tuple[Dimension, str]]:
        for dimension in self.requires:
            yield (dimension, dimension.name)
        yield (self, self.key.name)

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    @property
    def spatial(self) -> Optional[SpatialFamily]:
        return self._spatial

    @property
    def temporal(self) -> Optional[TemporalFamily]:
        return self._temporal

    def __lt__(self, other: Vertex) -> bool:
        if isinstance(other, DatabaseDimension):
            return self._index < other._index
        else:
            return True


class SkyPixSpatialFamily(SpatialFamily):
    max_level: int
    pixelization_cls: Type[Pixelization]


class SkyPixDimension(Dimension):

    def __init__(self, family: SkyPixSpatialFamily, level: int):
        super().__init__(
            name=f"{family.name}{level}",
            key=ddl.FieldSpec(f"{family.name}_id", dtype=sqlalchemy.BigInteger,
                              nullable=False, primaryKey=True)
        )
        self._family = family
        self.level = level
        self.pixelization = self._family.pixelization_cls(level)

    def getDimensionColumns(self) -> Iterator[Tuple[Dimension, str]]:
        yield (self, self.key.name)

    @property
    def universe(self) -> DimensionUniverse:
        return self._family.universe

    @property
    def spatial(self) -> SkyPixSpatialFamily:
        return self._family

    @property
    def temporal(self) -> None:
        return None

    def __lt__(self, other: Vertex) -> bool:
        if isinstance(other, LabelDimension):
            return True
        elif isinstance(other, SkyPixDimension):
            if self._family == other._family:
                return self.level < other.level
            else:
                return self._family.name < other._family.name
        else:
            return False

    level: int
    pixelization: Pixelization


class LabelDimension(Dimension):

    STRING_LABEL_LENGTH = 128

    def __init__(self, name: str, dtype: type, universe: DimensionUniverse):
        length: Optional[int]
        if dtype is str:
            dtype = sqlalchemy.String
            length = self.STRING_LABEL_LENGTH
        elif dtype is int:
            dtype = sqlalchemy.BigInteger
            length = None
        else:
            raise TypeError(f"dtype for label dimension must be int or str; got {dtype}")
        super().__init__(
            name=name,
            key=ddl.FieldSpec(name=name, dtype=dtype, length=length, nullable=False, primaryKey=True),
        )
        self._universe = universe

    def getDimensionColumns(self) -> Iterator[Tuple[Dimension, str]]:
        yield (self, self.key.name)

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    @property
    def spatial(self) -> None:
        return None

    @property
    def temporal(self) -> None:
        return None

    def __lt__(self, other: Vertex) -> bool:
        if isinstance(other, LabelDimension):
            return self.name < other.name
        return False


class Combination(Vertex):
    combines: AbstractNamedValueSet[DatabaseDimension]
    metadata: AbstractNamedValueSet[ddl.FieldSpec]


class Aggregation(Vertex):
    parent: DatabaseDimension
    child: DatabaseDimension
    mediator: DatabaseDimension

    # Invariants:
    # - parent must imply or require mediator
    # - parent and child may not belong to different
    #   spatial or temporal families.


F = TypeVar("F", bound=RelationshipFamily)


class MaterializedOverlap(Vertex, Generic[F]):
    relates: Tuple[NamedKeyMapping[F, Vertex], NamedKeyMapping[F, Vertex]]


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
    aggregations: AbstractNamedValueSet[Aggregation]
    combinations: AbstractNamedValueSet[Combination]
    vertices: AbstractNamedValueSet[Vertex]


class DimensionQueryRelationships(Generic[F]):

    FamilyCombinations: ClassVar[Type[FamilyCombinations]]
    ManualEdges: ClassVar[Type[ManualEdges]]

    @abstractmethod
    def visit(self, dimensions: DimensionGroup) -> AbstractNamedValueSet[Vertex]:
        pass


class FamilyCombinations(DimensionQueryRelationships[F]):
    overrides: NamedKeyMapping[F, Vertex]

    def visit(self, dimensions: DimensionGroup) -> AbstractNamedValueSet[Vertex]:
        pass


class ManualEdges(DimensionQueryRelationships[F]):
    edges: Set[Tuple[Vertex, Vertex]]

    def visit(self, dimensions: DimensionGroup) -> AbstractNamedValueSet[Vertex]:
        pass


DimensionQueryRelationships.FamilyCombinations = FamilyCombinations
DimensionQueryRelationships.ManualEdges = ManualEdges


class DimensionQueryGraph:

    requested: DimensionGroup

    spatial: DimensionQueryRelationships[SpatialFamily]
    temporal: DimensionQueryRelationships[TemporalFamily]

    extra: AbstractNamedValueSet[Vertex]

    @property
    def dimensions(self) -> DimensionGroup:
        pass

    @property
    def vertices(self) -> AbstractNamedValueSet[Vertex]:
        pass
