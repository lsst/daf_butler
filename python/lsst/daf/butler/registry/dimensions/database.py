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

__all__ = ["DatabaseDimensionRecordStorage"]

from typing import Optional

from sqlalchemy.sql import FromClause, select, and_
from sqlalchemy.engine import Connection

from ...core import DataCoordinate, DataId, DimensionElement, DimensionRecord
from ..interfaces import DimensionRecordStorage


class DatabaseDimensionRecordStorage(DimensionRecordStorage):
    """A record storage implementation that uses a SQL database by sharing
    a SQLAlchemy connection with a `Registry`.

    Parameters
    ----------
    connection : `sqlalchemy.engine.Connection`
        The SQLAlchemy connection to use for inserts and fetches.
    element : `DimensionElement`
        The element whose records this storage will manage.
    elementTable : `sqlalchemy.sql.FromClause`
        The logical table for the element.  May be a select query instead
        of a true table.
    commonSkyPixOvelapTable : `sqlalchemy.sql.FromClause`, optional
        The logical table for the overlap table with the dimension universe's
        common skypix dimension.
    """

    def __init__(self, connection: Connection, element: DimensionElement, *,
                 elementTable: FromClause,
                 commonSkyPixOverlapTable: Optional[FromClause] = None):
        self._connection = connection
        self._element = element
        self._elementTable = elementTable
        self._commonSkyPixOverlapTable = commonSkyPixOverlapTable
        if element.hasTable() and element.spatial and commonSkyPixOverlapTable is None:
            raise TypeError(f"No common skypix table provided for element {element.name}.")

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._element

    def clearCaches(self):
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def getElementTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from DimensionRecordStorage.getElementTable.
        return self._elementTable

    def getCommonSkyPixOverlapTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from
        # DimensionRecordStorage.getCommonSkyPixOverlapTable.
        return self._commonSkyPixOverlapTable

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        if self.element.viewOf is not None:
            raise TypeError(f"Cannot insert {self.element.name} records because its "
                            f"logical table is a view into {self.element.viewOf}.")
        # Build lists of dicts to insert first, before any database operations,
        # to minimize the time spent in the transaction.
        elementRows = []
        if self.element.spatial:
            commonSkyPixRows = []
            commonSkyPix = self.element.universe.commonSkyPix
        for record in records:
            elementRows.append(record.toDict())
            if self.element.spatial:
                if record.region is None:
                    # TODO: should we warn about this case?
                    continue
                base = record.dataId.byName()
                for begin, end in commonSkyPix.pixelization.envelope(record.region):
                    for skypix in range(begin, end):
                        row = base.copy()
                        row[commonSkyPix.name] = skypix
                        commonSkyPixRows.append(row)
        # TODO: wrap the operations below in a transaction.
        self._connection.execute(self._elementTable.insert(), *elementRows)
        if self.element.spatial and commonSkyPixRows:
            self._connection.execute(self._commonSkyPixOverlapTable.insert(), *commonSkyPixRows)

    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        RecordClass = self.element.RecordClass
        # I don't know how expensive it is to construct the query below, and
        # hence how much gain there might be to caching it, so I'm going to
        # wait for it to appear as a hotspot in a profile before trying that.
        nRequired = len(self.element.graph.required)
        if self.element.viewOf is not None:
            whereColumns = [self._elementTable.columns[dimension.name]
                            for dimension in self.element.graph.required]
        else:
            whereColumns = [self._elementTable.columns[fieldName]
                            for fieldName in RecordClass.__slots__[:nRequired]]
        selectColumns = whereColumns + [self._elementTable.columns[name]
                                        for name in RecordClass.__slots__[nRequired:]]
        query = select(
            selectColumns
        ).select_from(self._elementTable).where(
            and_(*[column == dataId[dimension.name]
                   for column, dimension in zip(whereColumns, self.element.graph.required)])
        )
        row = self._connection.execute(query).fetchone()
        if row is None:
            return None
        return RecordClass(*row)
