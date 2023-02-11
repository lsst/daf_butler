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

"""The default concrete implementations of the classes that manage
opaque tables for `Registry`.
"""

__all__ = ["ByNameOpaqueTableStorage", "ByNameOpaqueTableStorageManager"]

import itertools
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Iterable, Iterator, List, Mapping, Optional

import sqlalchemy

from ..core.ddl import FieldSpec, TableSpec
from .interfaces import (
    Database,
    OpaqueTableStorage,
    OpaqueTableStorageManager,
    StaticTablesContext,
    VersionTuple,
)

if TYPE_CHECKING:
    from ..core.datastore import DatastoreTransaction

# This has to be updated on every schema change
_VERSION = VersionTuple(0, 2, 0)


class ByNameOpaqueTableStorage(OpaqueTableStorage):
    """An implementation of `OpaqueTableStorage` that simply creates a true
    table for each different named opaque logical table.

    A `ByNameOpaqueTableStorageManager` instance should always be used to
    construct and manage instances of this class.

    Parameters
    ----------
    db : `Database`
        Database engine interface for the namespace in which this table lives.
    name : `str`
        Name of the logical table (also used as the name of the actual table).
    table : `sqlalchemy.schema.Table`
        SQLAlchemy representation of the table, which must have already been
        created in the namespace managed by ``db`` (this is the responsibility
        of `ByNameOpaqueTableStorageManager`).
    """

    def __init__(self, *, db: Database, name: str, table: sqlalchemy.schema.Table):
        super().__init__(name=name)
        self._db = db
        self._table = table

    def insert(self, *data: dict, transaction: DatastoreTransaction | None = None) -> None:
        # Docstring inherited from OpaqueTableStorage.
        # The provided transaction object can be ignored since we rely on
        # the database itself providing any rollback functionality.
        self._db.insert(self._table, *data)

    def fetch(self, **where: Any) -> Iterator[Mapping[str, Any]]:
        # Docstring inherited from OpaqueTableStorage.

        def _batch_in_clause(
            column: sqlalchemy.schema.Column, values: Iterable[Any]
        ) -> Iterator[sqlalchemy.sql.expression.ClauseElement]:
            """Split one long IN clause into a series of shorter ones."""
            in_limit = 1000
            # We have to remove possible duplicates from values; and in many
            # cases it should be helpful to order the items in the clause.
            values = sorted(set(values))
            for iposn in range(0, len(values), in_limit):
                in_clause = column.in_(values[iposn : iposn + in_limit])
                yield in_clause

        def _batch_in_clauses(**where: Any) -> Iterator[sqlalchemy.sql.expression.ColumnElement]:
            """Generate a sequence of WHERE clauses with a limited number of
            items in IN clauses.
            """
            batches: List[Iterable[Any]] = []
            for k, v in where.items():
                column = self._table.columns[k]
                if isinstance(v, (list, tuple, set)):
                    batches.append(_batch_in_clause(column, v))
                else:
                    # single "batch" for a regular eq operator
                    batches.append([column == v])

            for clauses in itertools.product(*batches):
                yield sqlalchemy.sql.and_(*clauses)

        sql = self._table.select()
        if where:
            # Split long IN clauses into shorter batches
            batched_sql = [sql.where(clause) for clause in _batch_in_clauses(**where)]
        else:
            batched_sql = [sql]
        for sql_batch in batched_sql:
            with self._db.query(sql_batch) as sql_result:
                sql_mappings = sql_result.mappings().fetchall()
            yield from sql_mappings

    def delete(self, columns: Iterable[str], *rows: dict) -> None:
        # Docstring inherited from OpaqueTableStorage.
        self._db.delete(self._table, columns, *rows)


class ByNameOpaqueTableStorageManager(OpaqueTableStorageManager):
    """An implementation of `OpaqueTableStorageManager` that simply creates a
    true table for each different named opaque logical table.

    Instances of this class should generally be constructed via the
    `initialize` class method instead of invoking ``__init__`` directly.

    Parameters
    ----------
    db : `Database`
        Database engine interface for the namespace in which this table lives.
    metaTable : `sqlalchemy.schema.Table`
        SQLAlchemy representation of the table that records which opaque
        logical tables exist.
    """

    def __init__(self, db: Database, metaTable: sqlalchemy.schema.Table):
        self._db = db
        self._metaTable = metaTable
        self._storage: Dict[str, OpaqueTableStorage] = {}

    _META_TABLE_NAME: ClassVar[str] = "opaque_meta"

    _META_TABLE_SPEC: ClassVar[TableSpec] = TableSpec(
        fields=[
            FieldSpec("table_name", dtype=sqlalchemy.String, length=128, primaryKey=True),
        ],
    )

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext) -> OpaqueTableStorageManager:
        # Docstring inherited from OpaqueTableStorageManager.
        metaTable = context.addTable(cls._META_TABLE_NAME, cls._META_TABLE_SPEC)
        return cls(db=db, metaTable=metaTable)

    def get(self, name: str) -> Optional[OpaqueTableStorage]:
        # Docstring inherited from OpaqueTableStorageManager.
        return self._storage.get(name)

    def register(self, name: str, spec: TableSpec) -> OpaqueTableStorage:
        # Docstring inherited from OpaqueTableStorageManager.
        result = self._storage.get(name)
        if result is None:
            # Create the table itself.  If it already exists but wasn't in
            # the dict because it was added by another client since this one
            # was initialized, that's fine.
            table = self._db.ensureTableExists(name, spec)
            # Add a row to the meta table so we can find this table in the
            # future.  Also okay if that already exists, so we use sync.
            self._db.sync(self._metaTable, keys={"table_name": name})
            result = ByNameOpaqueTableStorage(name=name, table=table, db=self._db)
            self._storage[name] = result
        return result

    @classmethod
    def currentVersion(cls) -> Optional[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return _VERSION

    def schemaDigest(self) -> Optional[str]:
        # Docstring inherited from VersionedExtension.
        return self._defaultSchemaDigest([self._metaTable], self._db.dialect)
