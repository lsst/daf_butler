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
from __future__ import annotations

__all__ = ["BasicSkyPixDimensionRecordStorage"]

from typing import TYPE_CHECKING, Iterable, Optional

import sqlalchemy

from ...core import (
    DataCoordinateIterable,
    DimensionElement,
    DimensionRecord,
    NamedKeyDict,
    SkyPixDimension,
    SpatialRegionDatabaseRepresentation,
    TimespanDatabaseRepresentation,
)
from ..interfaces import SkyPixDimensionRecordStorage
from ..interfaces.queries import Relation
from ..queries import QueryBuilder

if TYPE_CHECKING:
    from lsst.sphgeom import RangeSet

    from ..summaries import GovernorDimensionRestriction


class BasicSkyPixDimensionRecordStorage(SkyPixDimensionRecordStorage):
    """A storage implementation specialized for `SkyPixDimension` records.

    `SkyPixDimension` records are never stored in a database, but are instead
    generated-on-the-fly from a `sphgeom.Pixelization` instance.

    Parameters
    ----------
    dimension : `SkyPixDimension`
        The dimension for which this instance will simulate storage.
    """

    def __init__(self, dimension: SkyPixDimension):
        self._dimension = dimension

    @property
    def element(self) -> SkyPixDimension:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._dimension

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def join(
        self,
        builder: QueryBuilder,
        *,
        regions: Optional[NamedKeyDict[DimensionElement, SpatialRegionDatabaseRepresentation]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, TimespanDatabaseRepresentation]] = None,
    ) -> None:
        if builder.hasDimensionKey(self._dimension):
            # If joining some other element or dataset type already brought in
            # the key for this dimension, there's nothing left to do, because
            # a SkyPix dimension never has metadata or implied dependencies,
            # and its regions are never stored in the database.  This code path
            # is the usual case for the storage instance that manages
            # ``DimensionUniverse.commonSkyPix`` instance, which has no table
            # of its own but many overlap tables.
            # Storage instances for other skypix dimensions will probably hit
            # the error below, but we don't currently have a use case for
            # joining them in anyway.
            return
        raise NotImplementedError(f"Cannot includeSkyPix dimension {self.element.name} directly in query.")

    def select(
        self, restriction: GovernorDimensionRestriction, spatial_bounds: Optional[RangeSet] = None
    ) -> Relation:
        if spatial_bounds is None:
            raise RuntimeError(f"Cannot select {self.element.name} records without spatial bounds.")
        # TODO: Iterate over spatial_bounds and generate a list of IDs (and
        # regions?) to pass to Session.upload, once we figure out how to get
        # a session here and manage the temp table's lifetime.
        raise NotImplementedError(f"TODO: cannot select {self.element.name} without temp table context.")

    def insert(self, *records: DimensionRecord, replace: bool = False) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        raise TypeError(f"Cannot insert into SkyPix dimension {self._dimension.name}.")

    def sync(self, record: DimensionRecord, update: bool = False) -> bool:
        # Docstring inherited from DimensionRecordStorage.sync.
        raise TypeError(f"Cannot sync SkyPixdimension {self._dimension.name}.")

    def fetch(self, dataIds: DataCoordinateIterable) -> Iterable[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        RecordClass = self._dimension.RecordClass
        for dataId in dataIds:
            index = dataId[self._dimension.name]
            yield RecordClass(id=index, region=self._dimension.pixelization.pixel(index))

    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        return []
