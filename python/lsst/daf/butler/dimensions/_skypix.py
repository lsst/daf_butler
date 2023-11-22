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

from collections.abc import Iterator, Mapping, Set
from types import MappingProxyType
from typing import TYPE_CHECKING

import sqlalchemy
from lsst.sphgeom import PixelizationABC
from lsst.utils import doImportType

from .. import ddl
from .._named import NamedValueAbstractSet, NamedValueSet
from .._topology import TopologicalFamily, TopologicalSpace
from ._elements import Dimension
from .construction import DimensionConstructionBuilder, DimensionConstructionVisitor

if TYPE_CHECKING:
    from ..registry.interfaces import SkyPixDimensionRecordStorage
    from ._universe import DimensionUniverse


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

    def choose(self, endpoints: Set[str], universe: DimensionUniverse) -> SkyPixDimension:
        # Docstring inherited from TopologicalFamily.
        best: SkyPixDimension | None = None
        for endpoint_name in endpoints:
            endpoint = universe[endpoint_name]
            if endpoint not in self:
                continue
            assert isinstance(endpoint, SkyPixDimension)
            if best is None or best.level < endpoint.level:
                best = endpoint
        if best is None:
            raise RuntimeError(f"No recognized endpoints for {self.name} in {endpoints}.")
        return best

    def __getitem__(self, level: int) -> SkyPixDimension:
        return self._members[level]

    def __iter__(self) -> Iterator[SkyPixDimension]:
        return iter(self._members.values())

    def __len__(self) -> int:
        return len(self._members)


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
    def metadata(self) -> NamedValueAbstractSet[ddl.FieldSpec]:
        # Docstring inherited from DimensionElement.
        return NamedValueSet().freeze()

    def hasTable(self) -> bool:
        # Docstring inherited from DimensionElement.hasTable.
        return False

    def makeStorage(self) -> SkyPixDimensionRecordStorage:
        """Make the storage record.

        Constructs the `DimensionRecordStorage` instance that should
        be used to back this element in a registry.

        Returns
        -------
        storage : `SkyPixDimensionRecordStorage`
            Storage object that should back this element in a registry.
        """
        from ..registry.dimensions.skypix import BasicSkyPixDimensionRecordStorage

        return BasicSkyPixDimensionRecordStorage(self)

    @property
    def uniqueKeys(self) -> NamedValueAbstractSet[ddl.FieldSpec]:
        # Docstring inherited from DimensionElement.
        return NamedValueSet(
            {
                ddl.FieldSpec(
                    name="id",
                    dtype=sqlalchemy.BigInteger,
                    primaryKey=True,
                    nullable=False,
                )
            }
        ).freeze()

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


class SkyPixConstructionVisitor(DimensionConstructionVisitor):
    """Builder visitor for a single `SkyPixSystem` and its dimensions.

    Parameters
    ----------
    name : `str`
        Name of the `SkyPixSystem` to be constructed.
    pixelizationClassName : `str`
        Fully-qualified name of the class whose instances represent a
        particular level of this pixelization.
    maxLevel : `int`, optional
        Maximum level (inclusive) of the hierarchy.  If not provided, an
        attempt will be made to obtain it from a ``MAX_LEVEL`` attribute of the
        pixelization class.

    Notes
    -----
    At present, this class adds both a new `SkyPixSystem` instance all possible
    `SkyPixDimension` to the builder that invokes it.  In the future, it may
    add only the `SkyPixSystem`, with dimension instances created on-the-fly by
    the `DimensionUniverse`; this depends on eliminating assumptions about the
    set of dimensions in a universe being static.
    """

    def __init__(self, name: str, pixelizationClassName: str, maxLevel: int | None = None):
        super().__init__(name)
        self._pixelizationClassName = pixelizationClassName
        self._maxLevel = maxLevel

    def hasDependenciesIn(self, others: Set[str]) -> bool:
        # Docstring inherited from DimensionConstructionVisitor.
        return False

    def visit(self, builder: DimensionConstructionBuilder) -> None:
        # Docstring inherited from DimensionConstructionVisitor.
        PixelizationClass = doImportType(self._pixelizationClassName)
        assert issubclass(PixelizationClass, PixelizationABC)
        if self._maxLevel is not None:
            maxLevel = self._maxLevel
        else:
            # MyPy does not know the return type of getattr.
            max_level = getattr(PixelizationClass, "MAX_LEVEL", None)
            if max_level is None:
                raise TypeError(
                    f"Skypix pixelization class {self._pixelizationClassName} does"
                    " not have MAX_LEVEL but no max level has been set explicitly."
                )
            assert isinstance(max_level, int)
            maxLevel = max_level
        system = SkyPixSystem(
            self.name,
            maxLevel=maxLevel,
            PixelizationClass=PixelizationClass,
        )
        builder.topology[TopologicalSpace.SPATIAL].add(system)
        for level in range(maxLevel + 1):
            dimension = system[level]
            builder.dimensions.add(dimension)
            builder.elements.add(dimension)
