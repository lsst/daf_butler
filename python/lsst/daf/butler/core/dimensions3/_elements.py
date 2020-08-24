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
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import sqlalchemy
from lsst.sphgeom import Pixelization

from .. import ddl
from ..named import NamedValueSet, NamedValueAbstractSet


class RelationshipFamily(ABC):

    def __new__(
        cls,
        name: str,
        universe: DimensionUniverse
    ) -> RelationshipFamily:
        self = super().__new__(cls)
        self.name = name
        self.universe = universe
        # TODO: register with universe
        return self

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


T = TypeVar("T", bound="DimensionElement")


class DimensionElement(ABC):

    def __new__(
        cls: Type[T],
        name: str,
        universe: DimensionUniverse,
        **kwargs: Any,
    ) -> T:
        self = super().__new__(cls)
        self.name = name
        self.universe = universe
        # TODO: register with universe
        return self

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return None

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return None

    @property
    def span(self) -> DimensionGroup:
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

    name: str
    universe: DimensionUniverse


S = TypeVar("S", bound="AliasDimensionElement")


class AliasDimensionElement(DimensionElement):

    def __new__(
        cls,
        name: str, target: DimensionElement,
        **kwargs: Any,
    ) -> AliasDimensionElement:
        return super().__new__(cls, name, target.universe)

    def __init__(self, name: str, target: DimensionElement, *,
                 requires: NamedValueAbstractSet[Dimension],
                 implies: NamedValueAbstractSet[Dimension],
                 unique_constraints: AbstractSet[Tuple[str, ...]]):
        self.target = target
        self._requires = implies
        self._implies = requires
        self._unique_constraints = unique_constraints
        # TODO: handle relationship families?

    @classmethod
    def from_overrides(cls: Type[S], name: str, target: DimensionElement, *,
                       overrides: Mapping[str, str]) -> S:
        requires = NamedValueSet({
            target.universe[overrides.get(name, name)]  # type: ignore
            for name in target.requires.names
        })
        implies = NamedValueSet({
            target.universe[overrides.get(name, name)]  # type: ignore
            for name in target.implies.names
        })
        unique_constraints = frozenset(
            tuple(overrides.get(name, name) for name in constraint)
            for constraint in target.get_unique_constraints()
        )
        return cls(name, target,
                   requires=requires.freeze(),  # type: ignore
                   implies=implies.freeze(),  # type: ignore
                   unique_constraints=unique_constraints)

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return self.target.spatial_family

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return self.target.temporal_family

    @property
    def requires(self) -> NamedValueAbstractSet[Dimension]:
        return self._requires

    @property
    def implies(self) -> NamedValueAbstractSet[Dimension]:
        return self._implies

    def get_unique_constraints(self) -> Iterator[Tuple[str, ...]]:
        return iter(self._unique_constraints)


class Dimension(DimensionElement):

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

    def __lt__(self, other: Dimension) -> bool:
        raise NotImplementedError("TODO")

    def __gt__(self, other: Dimension) -> bool:
        return self != other and not self < other

    def __le__(self, other: Dimension) -> bool:
        return self == other or self < other

    def __ge__(self, other: Dimension) -> bool:
        return self == other or not self < other

    def alias(self, name: str, **kwargs: str) -> AliasDimension:
        return AliasDimension.from_overrides(name, self, overrides=kwargs)

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


class DimensionCombination(DimensionElement):

    def alias(self, name: str, **kwargs: str) -> AliasDimensionCombination:
        return AliasDimensionCombination.from_overrides(name, self, overrides=kwargs)


class AliasDimension(Dimension, AliasDimensionElement):

    @property
    def key_field_spec(self) -> ddl.FieldSpec:
        return self.target.key_field_spec

    target: Dimension


class AliasDimensionCombination(DimensionCombination, AliasDimensionElement):

    target: DimensionCombination


class SkyPixFamily(SpatialFamily):
    max_level: int
    pixelization_cls: Type[Pixelization]


class SkyPixDimension(Dimension):

    def __init__(self, family: SkyPixFamily, level: int):
        self._name = f"{family.name}{level}"
        self._family = family
        self.level = level
        self.pixelization = self._family.pixelization_cls(level)

    @property
    def spatial_family(self) -> SkyPixFamily:
        return self._family

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

    @property
    def key_field_spec(self) -> ddl.FieldSpec:
        return self._key_field_spec

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        yield from super().get_standard_field_specs()
        yield from self._metadata

    def get_unique_constraints(self) -> Iterator[Tuple[str, ...]]:
        return iter(self._unique_constraints)


class StandardDimensionCombination(DimensionCombination):

    def __init__(
        self,
        name: str,
        universe: DimensionUniverse, *,
        requires: NamedValueAbstractSet[Dimension],
        implies: NamedValueAbstractSet[Dimension],
        spatial_family: Optional[SpatialFamily],
        temporal_family: Optional[TemporalFamily],
        metadata: NamedValueAbstractSet[ddl.FieldSpec],
    ):
        self._requires = requires
        self._implies = implies
        self._spatial_family = spatial_family
        self._temporal_family = temporal_family
        self._metadata = metadata

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

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        yield from super().get_standard_field_specs()
        yield from self._metadata


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
    elements: NamedValueAbstractSet[DimensionElement]


class DimensionUniverse:

    def __getitem__(self, name: str) -> DimensionElement:
        raise NotImplementedError("TODO")

    def register_skypix(self, name: str, max_level: int, cls: Type[Pixelization]) -> SkyPixFamily:
        raise NotImplementedError("TODO")
