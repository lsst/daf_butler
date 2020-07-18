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

import itertools
from typing import Dict, Iterable, Optional

import sqlalchemy

from ...core import (
    DataCoordinateIterable,
    DimensionElement,
    DimensionRecord,
    makeDimensionElementTableSpec,
    NamedKeyDict,
    SimpleQuery,
    Timespan,
    TIMESPAN_FIELD_SPECS,
)
from ..interfaces import Database, DimensionRecordStorage, StaticTablesContext
from ..queries import QueryBuilder


MAX_FETCH_CHUNK = 1000
"""Maximum number of data IDs we fetch records at a time.

Barring something database-engine-specific, this sets the size of the actual
SQL query, not just the number of result rows, because the only way to query
for multiple data IDs in a single SELECT query via SQLAlchemy is to have an OR
term in the WHERE clause for each one.
"""


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
        self._fetchColumns: Dict[str, sqlalchemy.sql.ColumnElement] = {
            dimension.name: self._table.columns[name]
            for dimension, name in zip(itertools.chain(self._element.required, self._element.implied),
                                       self._element.RecordClass.__slots__)
        }

    @classmethod
    def initialize(cls, db: Database, element: DimensionElement, *,
                   context: Optional[StaticTablesContext] = None) -> DimensionRecordStorage:
        # Docstring inherited from DimensionRecordStorage.
        spec = makeDimensionElementTableSpec(element)
        if context is not None:
            table = context.addTable(element.name, spec)
        else:
            table = db.ensureTableExists(element.name, spec)
        return cls(db, element, table=table)

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._element

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def join(
        self,
        builder: QueryBuilder, *,
        regions: Optional[NamedKeyDict[DimensionElement, sqlalchemy.sql.ColumnElement]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, Timespan[sqlalchemy.sql.ColumnElement]]] = None,
    ) -> None:
        # Docstring inherited from DimensionRecordStorage.
        assert regions is None, "This implementation does not handle spatial joins."
        joinDimensions = list(self.element.required)
        joinDimensions.extend(self.element.implied)
        joinOn = builder.startJoin(self._table, joinDimensions, self.element.RecordClass.__slots__)
        if timespans is not None:
            timespanInTable: Timespan[sqlalchemy.sql.ColumnElement] = Timespan(
                begin=self._table.columns[TIMESPAN_FIELD_SPECS.begin.name],
                end=self._table.columns[TIMESPAN_FIELD_SPECS.end.name],
            )
            for timespanInQuery in timespans.values():
                joinOn.append(timespanInQuery.overlaps(timespanInTable, ops=sqlalchemy.sql))
            timespans[self.element] = timespanInTable
        builder.finishJoin(self._table, joinOn)
        return self._table

    def fetch(self, dataIds: DataCoordinateIterable) -> Iterable[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        RecordClass = self.element.RecordClass
        query = SimpleQuery()
        query.columns.extend(self._table.columns[name] for name in RecordClass.__slots__)
        query.join(self._table)
        dataIds.constrain(query, lambda name: self._fetchColumns[name])
        for row in self._db.query(query.combine()):
            yield RecordClass(*row)

    def insert(self, *records: DimensionRecord) -> None:
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
