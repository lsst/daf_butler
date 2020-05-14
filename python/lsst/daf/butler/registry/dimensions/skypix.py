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

__all__ = ["SkyPixDimensionRecordStorage"]

from typing import Optional

import sqlalchemy

from ...core import (
    DataCoordinate,
    DimensionElement,
    DimensionRecord,
    NamedKeyDict,
    SkyPixDimension,
    Timespan,
)
from ..queries import QueryBuilder
from ..interfaces import Database, DimensionRecordStorage, StaticTablesContext


class SkyPixDimensionRecordStorage(DimensionRecordStorage):
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

    @classmethod
    def initialize(cls, db: Database, element: DimensionElement, *,
                   context: Optional[StaticTablesContext] = None) -> DimensionRecordStorage:
        # Docstring inherited from DimensionRecordStorage.
        return cls(element)

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._dimension

    def clearCaches(self):
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def join(
        self,
        builder: QueryBuilder, *,
        regions: Optional[NamedKeyDict[DimensionElement, sqlalchemy.sql.ColumnElement]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, Timespan[sqlalchemy.sql.ColumnElement]]] = None,
    ):
        if builder.hasDimensionKey(self.element):
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

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        raise TypeError(f"Cannot insert into SkyPix dimension {self._dimension.name}.")

    def sync(self, record: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.sync.
        raise TypeError(f"Cannot sync SkyPixdimension {self._dimension.name}.")

    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        return self._dimension.RecordClass(dataId[self._dimension.name],
                                           self._dimension.pixelization.pixel(dataId[self._dimension.name]))
