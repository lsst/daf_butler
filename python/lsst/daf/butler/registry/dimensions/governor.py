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

__all__ = ["BasicGovernorDimensionRecordStorage"]

from typing import AbstractSet, Any, Callable, Dict, Iterable, List, Mapping, Optional

import sqlalchemy

from ...core import (
    DataCoordinateIterable,
    DimensionElement,
    DimensionRecord,
    DirectLogicalTable,
    GovernorDimension,
    LogicalTable,
    NamedKeyDict,
    TimespanDatabaseRepresentation,
)
from ..interfaces import (
    Database,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
)
from ..queries import QueryBuilder


class BasicGovernorDimensionRecordStorage(GovernorDimensionRecordStorage):
    """A record storage implementation for `GovernorDimension` that
    aggressively fetches and caches all values from the database.

    Parameters
    ----------
    db : `Database`
        Interface to the database engine and namespace that will hold these
        dimension records.
    dimension : `GovernorDimension`
        The dimension whose records this storage will manage.
    table : `sqlalchemy.schema.Table`
        The logical table for the dimension.
    """
    def __init__(self, db: Database, dimension: GovernorDimension, table: sqlalchemy.schema.Table):
        self._db = db
        self._dimension = dimension
        self._table = table
        self._cache: Dict[str, DimensionRecord] = {}
        self._callbacks: List[Callable[[DimensionRecord], None]] = []

    @classmethod
    def initialize(cls, db: Database, element: GovernorDimension, *,
                   context: Optional[StaticTablesContext] = None,
                   config: Mapping[str, Any]) -> GovernorDimensionRecordStorage:
        # Docstring inherited from GovernorDimensionRecordStorage.
        spec = element.RecordClass.fields.makeTableSpec(
            RegionReprClass=db.getSpatialRegionRepresentation(),
            TimespanReprClass=db.getTimespanRepresentation(),
        )
        if context is not None:
            table = context.addTable(element.name, spec)
        else:
            table = db.ensureTableExists(element.name, spec)
        return cls(db, element, table)

    @property
    def element(self) -> GovernorDimension:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._dimension

    def refresh(self) -> None:
        # Docstring inherited from GovernorDimensionRecordStorage.
        RecordClass = self._dimension.RecordClass
        sql = sqlalchemy.sql.select(
            [self._table.columns[name] for name in RecordClass.fields.standard.names]
        ).select_from(self._table)
        cache: Dict[str, DimensionRecord] = {}
        for row in self._db.query(sql):
            record = RecordClass(**dict(row))
            cache[getattr(record, self._dimension.primaryKey.name)] = record
        self._cache = cache

    @property
    def values(self) -> AbstractSet[str]:
        # Docstring inherited from GovernorDimensionRecordStorage.
        return self._cache.keys()

    @property
    def table(self) -> sqlalchemy.schema.Table:
        return self._table

    def registerInsertionListener(self, callback: Callable[[DimensionRecord], None]) -> None:
        # Docstring inherited from GovernorDimensionRecordStorage.
        self._callbacks.append(callback)

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        self._cache.clear()

    def join(
        self,
        builder: QueryBuilder, *,
        regions: Optional[NamedKeyDict[DimensionElement, sqlalchemy.sql.ColumnElement]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, TimespanDatabaseRepresentation]] = None,
    ) -> None:
        # Docstring inherited from DimensionRecordStorage.
        joinOn = builder.startJoin(self._table, self.element.dimensions,
                                   self.element.RecordClass.fields.dimensions.names)
        builder.finishJoin(self._table, joinOn)
        return self._table

    def insert(self, *records: DimensionRecord) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        elementRows = [record.toDict() for record in records]
        with self._db.transaction():
            self._db.insert(self._table, *elementRows)
        for record in records:
            self._cache[getattr(record, self.element.primaryKey.name)] = record
            for callback in self._callbacks:
                callback(record)

    def sync(self, record: DimensionRecord) -> bool:
        # Docstring inherited from DimensionRecordStorage.sync.
        compared = record.toDict()
        keys = {}
        for name in record.fields.required.names:
            keys[name] = compared.pop(name)
        with self._db.transaction():
            _, inserted = self._db.sync(
                self._table,
                keys=keys,
                compared=compared,
            )
        if inserted:
            self._cache[getattr(record, self.element.primaryKey.name)] = record
            for callback in self._callbacks:
                callback(record)
        return inserted

    def fetch(self, dataIds: DataCoordinateIterable) -> Iterable[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        try:
            return [self._cache[dataId[self.element]] for dataId in dataIds]  # type: ignore
        except KeyError:
            pass
        # If at first we don't succeed, refresh and try again.  But this time
        # we use dict.get to return None if we don't find something.
        self.refresh()
        return [self._cache.get(dataId[self.element]) for dataId in dataIds]  # type: ignore

    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        return [self._table]

    def makeLogicalTable(self) -> LogicalTable:
        # Docstring inherited from DatabaseDimensionRecordStorage.
        return DirectLogicalTable(
            self._table,
            dimensions=self.element.dimensions,
            facts=self.element.RecordClass.fields.facts.names,
            name=self.element.name,
            column_names={self.element: self.element.primaryKey.name},
        )
