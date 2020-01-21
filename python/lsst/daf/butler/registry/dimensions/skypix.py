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

from sqlalchemy.sql import FromClause

from ...core import DataCoordinate, DataId, DimensionElement, DimensionRecord, SkyPixDimension
from ..interfaces import DimensionRecordStorage


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

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._dimension

    def clearCaches(self):
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def getElementTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from DimensionRecordStorage.getElementTable.
        raise TypeError(f"SkyPix dimension {self._dimension.name} has no database representation.")

    def getCommonSkyPixOverlapTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from
        # DimensionRecordStorage.getCommonSkyPixOverlapTable.
        raise TypeError(f"SkyPix dimension {self._dimension.name} has no database representation.")

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        raise TypeError(f"Cannot insert into SkyPix dimension {self._dimension.name}.")

    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        return self._dimension.RecordClass(dataId[self._dimension.name],
                                           self._dimension.pixelization.pixel(dataId[self._dimension.name]))
