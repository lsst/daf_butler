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

__all__ = ["TableDimensionRecordStorage"]

from typing import Optional

import sqlalchemy

from ...core import DataCoordinate, DimensionElement, DimensionRecord, Timespan, TIMESPAN_FIELD_SPECS
from ...core.dimensions.schema import makeElementTableSpec
from ...core.utils import NamedKeyDict
from ..interfaces import Database, DimensionRecordStorage, StaticTablesContext
from ..queries import QueryBuilder


class TableDimensionRecordStorage(DimensionRecordStorage):
    """A record storage implementation uses a regular database table.

    For spatial dimension elements, use `SpatialDimensionRecordStorage`
    instead.

    Parameters
    ----------
    db : `Database`
        Interface to the database engine and namespace that will hold these
        dimension records.
    element : `DimensionElement`
        The element whose records this storage will manage.
    table : `sqlalchemy.schema.Table`
        The logical table for the element.
    """
    def __init__(self, db: Database, element: DimensionElement, *, table: sqlalchemy.schema.Table):
        self._db = db
        self._table = table
        self._element = element

    @classmethod
    def initialize(cls, db: Database, element: DimensionElement, *,
                   context: Optional[StaticTablesContext] = None) -> DimensionRecordStorage:
        # Docstring inherited from DimensionRecordStorage.
        spec = makeElementTableSpec(element)
        if context is not None:
            table = context.addTable(element.name, spec)
        else:
            table = db.ensureTableExists(element.name, spec)
        return cls(db, element, table=table)

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._element

    def clearCaches(self):
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def join(
        self,
        builder: QueryBuilder, *,
        regions: Optional[NamedKeyDict[DimensionElement, sqlalchemy.sql.ColumnElement]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, Timespan[sqlalchemy.sql.ColumnElement]]] = None,
    ):
        # Docstring inherited from DimensionRecordStorage.
        assert regions is None, "This implementation does not handle spatial joins."
        joinDimensions = list(self.element.required)
        joinDimensions.extend(self.element.implied)
        joinOn = builder.startJoin(self._table, joinDimensions, self.element.RecordClass.__slots__)
        if timespans is not None:
            timespanInTable = Timespan(
                begin=self._table.columns[TIMESPAN_FIELD_SPECS.begin.name],
                end=self._table.columns[TIMESPAN_FIELD_SPECS.end.name],
            )
            for timespanInQuery in timespans.values():
                joinOn.append(timespanInQuery.overlaps(timespanInTable, ops=sqlalchemy.sql))
            timespans[self.element] = timespanInTable
        builder.finishJoin(self._table, joinOn)
        return self._table

    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        RecordClass = self.element.RecordClass
        # I don't know how expensive it is to construct the query below, and
        # hence how much gain there might be to caching it, so I'm going to
        # wait for it to appear as a hotspot in a profile before trying that.
        whereTerms = [self._table.columns[fieldName] == dataId[dimension.name]
                      for fieldName, dimension in zip(RecordClass.__slots__, self.element.required)]
        query = sqlalchemy.sql.select(
            [self._table.columns[name] for name in RecordClass.__slots__]
        ).select_from(
            self._table
        ).where(sqlalchemy.sql.and_(*whereTerms))
        row = self._db.query(query).fetchone()
        if row is None:
            return None
        return RecordClass(*row)

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        elementRows = [record.toDict() for record in records]
        with self._db.transaction():
            self._db.insert(self._table, *elementRows)

    def sync(self, record: DimensionRecord) -> bool:
        # Docstring inherited from DimensionRecordStorage.sync.
        n = len(self.element.required)
        _, inserted = self._db.sync(
            self._table,
            keys={k: getattr(record, k) for k in record.__slots__[:n]},
            compared={k: getattr(record, k) for k in record.__slots__[n:]},
        )
        return inserted
