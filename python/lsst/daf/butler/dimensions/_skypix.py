# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from __future__ import annotations

__all__ = (
    "SkyPixDimension",
    "SkyPixSystem",
)

from collections.abc import Iterator, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from lsst.sphgeom import PixelizationABC

from .._named import NamedValueAbstractSet, NamedValueSet
from .._topology import TopologicalFamily, TopologicalRelationshipEndpoint, TopologicalSpace
from ..column_spec import IntColumnSpec
from ._elements import Dimension, KeyColumnSpec, MetadataColumnSpec

if TYPE_CHECKING:
    from ..queries.tree import DimensionFieldReference
    from ._group import DimensionGroup


class SkyPixSystem(TopologicalFamily):
    """Class for hierarchical pixelization of the sky.

    A `TopologicalFamily` that represents a hierarchical pixelization of the
    sky.

    Parameters
    ----------
    name : `str`
        Name of the system.
    maxLevel : `int`
        Maximum level (inclusive) of the hierarchy.
    PixelizationClass : `type` (`lsst.sphgeom.PixelizationABC` subclass)
        Class whose instances represent a particular level of this
        pixelization.
    """

    def __init__(
        self,
        name: str,
        *,
        maxLevel: int,
        PixelizationClass: type[PixelizationABC],
    ):
        super().__init__(name, TopologicalSpace.SPATIAL)
        self.maxLevel = maxLevel
        self.PixelizationClass = PixelizationClass
        self._members: dict[int, SkyPixDimension] = {}
        for level in range(maxLevel + 1):
            self._members[level] = SkyPixDimension(self, level)

    def choose(self, dimensions: DimensionGroup) -> SkyPixDimension:
        # Docstring inherited from TopologicalFamily.
        best: SkyPixDimension | None = None
        for endpoint_name in dimensions.skypix:
            endpoint = dimensions.universe[endpoint_name]
            if endpoint not in self:
                continue
            assert isinstance(endpoint, SkyPixDimension)
            if best is None or best.level < endpoint.level:
                best = endpoint
        if best is None:
            raise RuntimeError(f"No recognized endpoints for {self.name} in {dimensions}.")
        return best

    def __getitem__(self, level: int) -> SkyPixDimension:
        return self._members[level]

    def __iter__(self) -> Iterator[SkyPixDimension]:
        return iter(self._members.values())

    def __len__(self) -> int:
        return len(self._members)

    def make_column_reference(self, endpoint: TopologicalRelationshipEndpoint) -> DimensionFieldReference:
        from ..queries.tree import DimensionFieldReference

        return DimensionFieldReference(element=cast(SkyPixDimension, endpoint), field="region")


class SkyPixDimension(Dimension):
    """Special dimension for sky pixelizations.

    A special `Dimension` subclass for hierarchical pixelizations of the
    sky at a particular level.

    Unlike most other dimensions, skypix dimension records are not stored in
    the database, as these records only contain an integer pixel ID and a
    region on the sky, and each of these can be computed directly from the
    other.

    Parameters
    ----------
    system : `SkyPixSystem`
        Pixelization system this dimension belongs to.
    level : `int`
        Integer level of this pixelization (smaller numbers are coarser grids).
    """

    def __init__(self, system: SkyPixSystem, level: int):
        self.system = system
        self.level = level
        self.pixelization = system.PixelizationClass(level)

    @property
    def name(self) -> str:
        return f"{self.system.name}{self.level}"

    @property
    def required(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return NamedValueSet({self}).freeze()

    @property
    def implied(self) -> NamedValueAbstractSet[Dimension]:
        # Docstring inherited from DimensionElement.
        return NamedValueSet().freeze()

    @property
    def topology(self) -> Mapping[TopologicalSpace, TopologicalFamily]:
        # Docstring inherited from TopologicalRelationshipEndpoint
        return MappingProxyType({TopologicalSpace.SPATIAL: self.system})

    @property
    def metadata_columns(self) -> NamedValueAbstractSet[MetadataColumnSpec]:
        # Docstring inherited from DimensionElement.
        return NamedValueSet().freeze()

    @property
    def documentation(self) -> str:
        # Docstring inherited from DimensionElement.
        return f"Level {self.level} of the {self.system.name!r} sky pixelization system."

    def hasTable(self) -> bool:
        # Docstring inherited from DimensionElement.hasTable.
        return False

    @property
    def has_own_table(self) -> bool:
        # Docstring inherited from DimensionElement.
        return False

    @property
    def unique_keys(self) -> NamedValueAbstractSet[KeyColumnSpec]:
        # Docstring inherited from DimensionElement.
        return NamedValueSet([IntColumnSpec(name="id", nullable=False)]).freeze()

    # Class attributes below are shadowed by instance attributes, and are
    # present just to hold the docstrings for those instance attributes.

    system: SkyPixSystem
    """Pixelization system this dimension belongs to (`SkyPixSystem`).
    """

    level: int
    """Integer level of this pixelization (smaller numbers are coarser grids).
    """

    pixelization: PixelizationABC
    """Pixelization instance that can compute regions from IDs and IDs from
    points (`sphgeom.PixelizationABC`).
    """
