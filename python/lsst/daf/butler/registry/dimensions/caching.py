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

__all__ = ["CachingDimensionRecordStorage"]

from typing import Optional

import sqlalchemy

from ...core import DataCoordinate, DimensionElement, DimensionRecord, Timespan
from ...core.utils import NamedKeyDict
from ..interfaces import DimensionRecordStorage
from ..queries import QueryBuilder


class CachingDimensionRecordStorage(DimensionRecordStorage):
    """A record storage implementation that adds caching to some other nested
    storage implementation.

    Parameters
    ----------
    nested : `DimensionRecordStorage`
        The other storage to cache fetches from and to delegate all other
        operations to.
    """

    def __init__(self, nested: DimensionRecordStorage):
        self._nested = nested
        self._cache = {}

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._nested.element

    def clearCaches(self):
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        self._cache.clear()
        self._nested.clearCaches()

    def join(
        self,
        builder: QueryBuilder, *,
        regions: Optional[NamedKeyDict[DimensionElement, sqlalchemy.sql.ColumnElement]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, Timespan[sqlalchemy.sql.ColumnElement]]] = None,
    ):
        self._nested.join(builder, regions=regions, timespans=timespans)

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        self._nested.insert(*records)
        for record in records:
            self._cache[record.dataId] = record

    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        dataId = DataCoordinate.standardize(dataId, graph=self.element.graph)
        record = self._cache.get(dataId)
        if record is None:
            record = self._nested.fetch(dataId)
            self._cache[dataId] = record
        return record
