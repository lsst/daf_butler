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

from collections.abc import Callable, Mapping
from typing import Any, cast

import sqlalchemy
from lsst.daf.relation import Relation

from ...core import DataCoordinate, DimensionRecord, GovernorDimension
from .. import queries
from ..interfaces import Database, GovernorDimensionRecordStorage, StaticTablesContext


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

    def __init__(
        self,
        db: Database,
        dimension: GovernorDimension,
        table: sqlalchemy.schema.Table,
    ):
        self._db = db
        self._dimension = dimension
        self._table = table
        # We need to allow the cache to be None so we have some recourse when
        # it is cleared as part of transaction rollback - we can't run
        # queries to repopulate them at that point, so we need to defer it
        # until next use.
        self._cache: dict[DataCoordinate, DimensionRecord] | None = None
        self._callbacks: list[Callable[[DimensionRecord], None]] = []

    @classmethod
    def initialize(
        cls,
        db: Database,
        element: GovernorDimension,
        *,
        context: StaticTablesContext | None = None,
        config: Mapping[str, Any],
    ) -> GovernorDimensionRecordStorage:
        # Docstring inherited from GovernorDimensionRecordStorage.
        spec = element.RecordClass.fields.makeTableSpec(
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

    @property
    def table(self) -> sqlalchemy.schema.Table:
        return self._table

    def registerInsertionListener(self, callback: Callable[[DimensionRecord], None]) -> None:
        # Docstring inherited from GovernorDimensionRecordStorage.
        self._callbacks.append(callback)

    def clearCaches(self) -> None:
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        self._cache = None

    def make_relation(self, context: queries.SqlQueryContext, _sized: bool = True) -> Relation:
        # Docstring inherited.
        payload = self._build_sql_payload(self._table, context.column_types)
        if _sized:
            cache = self.get_record_cache(context)
        return context.sql_engine.make_leaf(
            payload.columns_available.keys(),
            name=self.element.name,
            payload=payload,
            min_rows=len(cache) if _sized else 0,
            max_rows=len(cache) if _sized else None,
        )

    def insert(self, *records: DimensionRecord, replace: bool = False, skip_existing: bool = False) -> None:
        # Docstring inherited from DimensionRecordStorage.insert.
        elementRows = [record.toDict() for record in records]
        with self._db.transaction():
            if replace:
                self._db.replace(self._table, *elementRows)
            elif skip_existing:
                self._db.ensure(self._table, *elementRows, primary_key_only=True)
            else:
                self._db.insert(self._table, *elementRows)
        for record in records:
            # We really shouldn't ever get into a situation where the
            # record here differs from the one in the DB, but the last
            # thing we want is to make it harder to debug by making the
            # cache different from the DB.
            if self._cache is not None:
                # We really shouldn't ever get into a situation where the
                # record here differs from the one in the DB, but the last
                # thing we want is to make it harder to debug by making the
                # cache different from the DB.
                if skip_existing:
                    self._cache.setdefault(record.dataId, record)
                else:
                    self._cache[record.dataId] = record
            for callback in self._callbacks:
                callback(record)

    def sync(self, record: DimensionRecord, update: bool = False) -> bool | dict[str, Any]:
        # Docstring inherited from DimensionRecordStorage.sync.
        compared = record.toDict()
        keys = {}
        for name in record.fields.required.names:
            keys[name] = compared.pop(name)
        with self._db.transaction():
            _, inserted_or_updated = self._db.sync(
                self._table,
                keys=keys,
                compared=compared,
                update=update,
            )
        if inserted_or_updated:
            if self._cache is not None:
                self._cache[record.dataId] = record
            for callback in self._callbacks:
                callback(record)
        return inserted_or_updated

    def fetch_one(self, data_id: DataCoordinate, context: queries.SqlQueryContext) -> DimensionRecord | None:
        # Docstring inherited.
        cache = self.get_record_cache(context)
        return cache.get(data_id)

    def get_record_cache(self, context: queries.SqlQueryContext) -> Mapping[DataCoordinate, DimensionRecord]:
        # Docstring inherited.
        if self._cache is None:
            reader = queries.DimensionRecordReader(self.element)
            cache = {}
            for row in context.fetch_iterable(self.make_relation(context, _sized=False)):
                record = reader.read(row)
                cache[record.dataId] = record
            self._cache = cache
        return cast(Mapping[DataCoordinate, DimensionRecord], self._cache)

    def digestTables(self) -> list[sqlalchemy.schema.Table]:
        # Docstring inherited from DimensionRecordStorage.digestTables.
        return [self._table]
