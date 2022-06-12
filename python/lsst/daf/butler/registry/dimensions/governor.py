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

from typing import AbstractSet, Any, Callable, Dict, Iterable, List, Mapping, Optional, Union

import sqlalchemy

from ...core import DataCoordinateIterable, DimensionRecord, GovernorDimension, sql
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
    column_types : `sql.ColumnTypeInfo`
        Information about column types that can vary with registry
        configuration.
    """

    def __init__(
        self,
        db: Database,
        dimension: GovernorDimension,
        table: sqlalchemy.schema.Table,
        column_types: sql.ColumnTypeInfo,
    ):
        self._db = db
        self._dimension = dimension
        self._table = table
        self._cache: Dict[str, DimensionRecord] = {}
        self._callbacks: List[Callable[[DimensionRecord], None]] = []
        self._column_types = column_types

    @classmethod
    def initialize(
        cls,
        db: Database,
        element: GovernorDimension,
        *,
        context: Optional[StaticTablesContext] = None,
        config: Mapping[str, Any],
        column_types: sql.ColumnTypeInfo,
    ) -> GovernorDimensionRecordStorage:
        # Docstring inherited from GovernorDimensionRecordStorage.
        spec = element.RecordClass.fields.makeTableSpec(
            RegionReprClass=db.getSpatialRegionRepresentation(),
            TimespanReprClass=db.getTimespanRepresentation(),
        )
        if context is not None:
            table = context.addTable(element.name, spec)
        else:
            table = db.ensureTableExists(element.name, spec)
        return cls(db, element, table, column_types)

    @property
    def element(self) -> GovernorDimension:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._dimension

    def refresh(self) -> None:
        # Docstring inherited from GovernorDimensionRecordStorage.
        RecordClass = self._dimension.RecordClass
        sql = sqlalchemy.sql.select(
            *[self._table.columns[name] for name in RecordClass.fields.standard.names]
        ).select_from(self._table)
        cache: Dict[str, DimensionRecord] = {}
        for row in self._db.query(sql):
            record = RecordClass(**row._asdict())
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
        self.refresh()

    def join_results_postprocessed(self) -> bool:
        # Docstring inherited.
        return True

    def join(
        self,
        relation: sql.Relation,
        sql_columns: AbstractSet[str],
        *,
        constraints: sql.LocalConstraints | None = None,
        result_records: bool = False,
        result_columns: AbstractSet[str] = frozenset(),
    ) -> sql.Relation:
        # Docstring inherited from DimensionRecordStorage.
        if result_records or result_columns:
            # Caller wants at least some columns in result records, which we
            # could satisfy via a postprocessor based on the cache of all
            # records.
            postprocessors: list[sql.Postprocessor] = [
                _GovernorDimensionRecordPostprocessor(self._dimension, self._cache)
            ]
            if result_columns:
                postprocessors.append(
                    sql.Postprocessor.make_dimension_column_extractor(
                        self._dimension.RecordClass, result_columns
                    )
                )
            if not sql_columns and self._dimension.dimensions.names <= relation.columns.dimensions:
                # All keys are already present, so we don't need to join in
                # this dimension's table at all.
                return relation.postprocessed(*postprocessors)
            else:
                # We need to join in this table, but we'll still use
                # postprocessors to provide result records and/or columns, to
                # keep the result row as narrow as possible.
                return relation.join(
                    self._build_leaf_relation(
                        self.table, self._column_types, sql_columns, constraints=self.get_local_constraints()
                    ).postprocessed(*postprocessors)
                )
        else:
            # All requested columns are for use in the query (e.g. WHERE
            # clause), so no postprocessors in play.
            return relation.join(
                self._build_leaf_relation(
                    self.table, self._column_types, sql_columns, constraints=self.get_local_constraints()
                )
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
            # We really shouldn't ever get into a situation where the record
            # here differs from the one in the DB, but the last thing we want
            # is to make it harder to debug by making the cache different from
            # the DB.
            if skip_existing:
                self._cache.setdefault(getattr(record, self.element.primaryKey.name), record)
            else:
                self._cache[getattr(record, self.element.primaryKey.name)] = record
            for callback in self._callbacks:
                callback(record)

    def sync(self, record: DimensionRecord, update: bool = False) -> Union[bool, Dict[str, Any]]:
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
            self._cache[getattr(record, self.element.primaryKey.name)] = record
            for callback in self._callbacks:
                callback(record)
        return inserted_or_updated

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


class _GovernorDimensionRecordPostprocessor(sql.Postprocessor):
    def __init__(self, dimension: GovernorDimension, cache: dict[str, DimensionRecord]):
        self._cache = cache
        self._lookup_tag = sql.DimensionKeyColumnTag(dimension.name)
        self._cls = dimension.RecordClass

    __slots__ = ("_cache",)

    @property
    def columns_provided(self) -> AbstractSet[sql.ResultTag]:
        return {self._cls}

    @property
    def columns_required(self) -> AbstractSet[sql.ResultTag]:
        return {self._lookup_tag}

    @property
    def row_multiplier(self) -> float:
        return 1.0

    def apply(self, row: sql.ResultRow) -> sql.ResultRow:
        record = self._cache[row[self._lookup_tag]]
        row[self._cls] = record
        return row
