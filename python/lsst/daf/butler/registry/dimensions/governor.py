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

from typing import AbstractSet, Any, Callable, Iterable, List, Mapping, Optional

import sqlalchemy

from ...core import (
    DataCoordinateIterable,
    DimensionElement,
    DimensionRecord,
    GovernorDimension,
    HomogeneousDimensionRecordCache,
    NamedKeyDict,
    SimpleQuery,
    TimespanDatabaseRepresentation,
)
from ..interfaces import (
    Database,
    GovernorDimensionRecordStorage,
    StaticTablesContext,
)
from ..queries import DimensionRecordQueryResults, QueryBuilder


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
        self._cache = HomogeneousDimensionRecordCache(dimension, self._fetch_from_db)
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
        cache = HomogeneousDimensionRecordCache(self.element, self._fetch_from_db)
        cache.update(self._fetch_from_db(None))
        self._cache = cache

    @property
    def values(self) -> AbstractSet[str]:
        # Docstring inherited from GovernorDimensionRecordStorage.
        return {data_id[self.element] for data_id in self._cache.data_ids}  # type: ignore

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
            self._cache.add(record)
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
            self._cache.add(record)
            for callback in self._callbacks:
                callback(record)
        return inserted

    def fetch(self, dataIds: DataCoordinateIterable) -> HomogeneousDimensionRecordCache:
        # Docstring inherited from DimensionRecordStorage.fetch.
        return self._cache.extract(dataIds)

    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        return [self._table]

    def _fetch_from_db(self, data_ids: Optional[DataCoordinateIterable]) -> DimensionRecordQueryResults:
        """Fetch records directly from the database, bypassing the cache.

        Parameters
        ----------
        data_ids : `DataCoordinateIterable`, optional
            Data IDs of the records to fetch.  If `None`, fetch all records
            for this dimension.

        Returns
        -------
        records : `DimensionRecordQueryResults`
            Fetched records, as a lazily-evaluated iterable.
        """
        RecordClass = self._dimension.RecordClass
        query = SimpleQuery()
        query.join(self._table)
        query.columns.extend(self._table.columns[name] for name in RecordClass.fields.standard.names)
        if data_ids is not None:
            if data_ids.graph != self.element.graph:
                raise ValueError(
                    f"Invalid dimensions for dimension record lookup; expected {self.element.graph}, "
                    f"got {data_ids.graph}."
                )
            # GovernorDimensions never have dependencies, so the callable below
            # that extracts compound-primary-key columns can always just
            # return the table's single primary-key column.
            data_ids.constrain(query, lambda _: self._table.columns[self.element.primaryKey.name])
        return DimensionRecordQueryResults(self._db, query.combine(), self.element, data_ids)
