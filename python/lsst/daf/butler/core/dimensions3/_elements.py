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
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    Mapping,
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
from ..named import NamedValueSet, NamedValueAbstractSet


class RelationshipFamily(ABC):

    def __init__(
        self,
        universe: DimensionUniverse,
        name: str,
    ):
        self.universe = universe
        self.name = name

    universe: DimensionUniverse
    name: str


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


class DimensionElement(ABC):

    def __init__(
        self,
        universe: DimensionUniverse,
        name: str,
    ):
        self.universe = universe
        self.name = name

    @property
    def spatial_family(self) -> Optional[SpatialFamily]:
        return None

    @property
    def temporal_family(self) -> Optional[TemporalFamily]:
        return None

    @property
    def spanning_group(self) -> DimensionGroup:
        raise NotImplementedError("TODO")

    @property
    def requires(self) -> NamedValueAbstractSet[Dimension]:
        return NamedValueSet().freeze()

    @property
    def implies(self) -> NamedValueAbstractSet[Dimension]:
        return NamedValueSet().freeze()

    @property
    def dependencies(self) -> NamedValueAbstractSet[Dimension]:
        raise NotImplementedError("TODO")

    def get_standard_field_specs(self) -> Iterator[ddl.FieldSpec]:
        for dimension in self.requires:
            yield dataclasses.replace(dimension.key_field_spec, name=dimension.name)
        for dimension in self.implies:
            yield dataclasses.replace(dimension.key_field_spec, name=dimension.name,
                                      primary_key=False, nullable=True)

    def get_unique_constraints(self) -> Iterator[Tuple[str, ...]]:
        yield from ()

    universe: DimensionUniverse
    name: str


S = TypeVar("S", bound="AliasDimensionElement")


class AliasDimensionElement(DimensionElement):

    def __init__(
        self,
        target: DimensionElement,
        name: str, *,
        requires: NamedValueAbstractSet[Dimension],
        implies: NamedValueAbstractSet[Dimension],
        unique_constraints: AbstractSet[Tuple[str, ...]],
    ):
        super().__init__(target.universe, name)
        self.target = target
        self._requires = implies
        self._implies = requires
        self._unique_constraints = unique_constraints
        # TODO: handle relationship families?

    @classmethod
    def from_overrides(cls: Type[S], target: DimensionElement, name: str, *,
                       overrides: Mapping[str, str]) -> S:
        requires = NamedValueSet({
            target.universe.dimensions[overrides.get(name, name)]
            for name in target.requires.names
        })
        implies = NamedValueSet({
            target.universe.dimensions[overrides.get(name, name)]
            for name in target.implies.names
        })
        unique_constraints = frozenset(
            tuple(overrides.get(name, name) for name in constraint)
            for constraint in target.get_unique_constraints()
        )
        return cls(target, name,
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

    def register_alias(self, name: str, **kwargs: str) -> AliasDimension:
        return AliasDimension.from_overrides(self, name, overrides=kwargs)

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

    def register_alias(self, name: str, **kwargs: str) -> AliasDimensionCombination:
        return AliasDimensionCombination.from_overrides(self, name, overrides=kwargs)


class AliasDimension(Dimension, AliasDimensionElement):

    @property
    def key_field_spec(self) -> ddl.FieldSpec:
        return self.target.key_field_spec

    target: Dimension


class AliasDimensionCombination(DimensionCombination, AliasDimensionElement):

    target: DimensionCombination


class SkyPixFamily(SpatialFamily):

    def __init__(
        self,
        universe: DimensionUniverse,
        name: str, *,
        max_level: int,
        pixelization_cls: Type[Pixelization],
    ):
        super().__init__(universe, name)
        self.max_level = max_level
        self.pixelization_cls = pixelization_cls

    def register_level(self, level: int) -> SkyPixDimension:
        return SkyPixDimension(self, level)

    max_level: int
    pixelization_cls: Type[Pixelization]


class SkyPixDimension(Dimension):

    def __init__(self, family: SkyPixFamily, level: int):
        super().__init__(family.universe, f"{family.name}{level}")
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


class StandardSpatialFamily(SpatialFamily):

    def __init__(
        self,
        universe: DimensionUniverse,
        name: str,
        mediator: Optional[Dimension] = None,
    ):
        super().__init__(universe, name)
        self.mediator = mediator

    mediator: Optional[Dimension]


class StandardTemporalFamily(TemporalFamily):

    def __init__(
        self,
        universe: DimensionUniverse,
        name: str,
        mediator: Optional[Dimension] = None,
    ):
        super().__init__(universe, name)
        self.mediator = mediator

    mediator: Optional[Dimension]


class StandardDimension(Dimension):

    def __init__(
        self,
        universe: DimensionUniverse,
        name: str, *,
        requires: NamedValueAbstractSet[Dimension],
        implies: NamedValueAbstractSet[Dimension],
        spatial_family: Optional[SpatialFamily],
        temporal_family: Optional[TemporalFamily],
        key_field_spec: ddl.FieldSpec,
        unique_constraints: AbstractSet[Tuple[str, ...]],
        metadata: NamedValueAbstractSet[ddl.FieldSpec],
    ):
        super().__init__(universe, name)
        self._requires = requires
        self._implies = implies
        self._spatial_family = spatial_family
        self._temporal_family = temporal_family
        self._key_field_spec = key_field_spec
        self._unique_constraints = unique_constraints
        self._metadata = metadata
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
        universe: DimensionUniverse,
        name: str, *,
        requires: NamedValueAbstractSet[Dimension],
        implies: NamedValueAbstractSet[Dimension],
        spatial_family: Optional[SpatialFamily],
        temporal_family: Optional[TemporalFamily],
        metadata: NamedValueAbstractSet[ddl.FieldSpec],
    ):
        super().__init__(universe, name)
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
        universe: DimensionUniverse,
        dimensions: NamedValueAbstractSet[Dimension],
        combinations: NamedValueAbstractSet[DimensionCombination],
    ):
        self.universe = universe
        self._as_set = dimensions
        self.combinations = combinations
        implied_names: Set[str] = set()
        for dimension in dimensions:
            implied_names.update(dimension.implies.names)
        if implied_names:
            self.required = NamedValueSet(d for d in dimensions if d.name not in implied_names).freeze()
            self.implied = NamedValueSet(d for d in dimensions if d.name in implied_names).freeze()
        else:
            self.required = dimensions
            self.implied = NamedValueSet().freeze()
        elements: NamedValueSet[DimensionElement] = NamedValueSet()
        elements.update(dimensions)
        elements.update(combinations)
        self.elements = elements.freeze()

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

    def __init__(self) -> None:
        self.dimensions = NamedValueSet()
        self._group_cache: Dict[FrozenSet[str], DimensionGroup] = {}

    def __getitem__(self, name: str) -> DimensionElement:
        raise NotImplementedError("TODO")

    def group(
        self,
        dimensions: Iterable[Union[str, Dimension]], *,
        conform: bool = True
    ) -> DimensionGroup:
        names: Set[str]
        try:
            names = set(dimensions.names)  # type: ignore
        except AttributeError:
            names = {
                item if isinstance(item, str) else item.name
                for item in dimensions
            }
        if conform:
            for name in frozenset(names):
                names.update(self.dimensions[name].dependencies.names)
        cache_key = frozenset(names)
        result = self._group_cache.get(cache_key)
        if result is not None:
            return result
        raise NotImplementedError()

    def register_skypix_family(self, name: str, max_level: int, cls: Type[Pixelization]) -> SkyPixFamily:
        raise NotImplementedError("TODO")

    def register_spatial_family(
        self,
        name: str,
        mediator: Optional[Dimension] = None,
    ) -> StandardSpatialFamily:
        # TODO: friendlier argument types.
        raise NotImplementedError("TODO")

    def register_temporal_family(
        self,
        name: str,
        mediator: Optional[Dimension] = None,
    ) -> StandardTemporalFamily:
        # TODO: friendlier argument types.
        raise NotImplementedError("TODO")

    def register_dimension(
        self,
        name: str,
        key_field_spec: ddl.FieldSpec, *,
        requires: NamedValueAbstractSet[Dimension],
        implies: NamedValueAbstractSet[Dimension],
        spatial_family: Optional[SpatialFamily],
        temporal_family: Optional[TemporalFamily],
        unique_constraints: AbstractSet[Tuple[str, ...]],
        metadata: NamedValueAbstractSet[ddl.FieldSpec],
    ) -> StandardDimension:
        # TODO: friendlier argument types.
        raise NotImplementedError("TODO")

    def register_combination(
        self,
        name: str,
        requires: NamedValueAbstractSet[Dimension],
        implies: NamedValueAbstractSet[Dimension],
        spatial_family: Optional[SpatialFamily],
        temporal_family: Optional[TemporalFamily],
        unique_constraints: AbstractSet[Tuple[str, ...]],
        metadata: NamedValueAbstractSet[ddl.FieldSpec],
    ) -> StandardDimensionCombination:
        # TODO: friendlier argument types.
        raise NotImplementedError("TODO")

    dimensions: NamedValueAbstractSet[Dimension]
