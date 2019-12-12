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

__all__ = ["DatabaseTests"]

from abc import ABC, abstractmethod
from collections import namedtuple
from typing import ContextManager

import sqlalchemy

from ..interfaces import (
    Database,
    ReadOnlyDatabaseError,
    TransactionInterruption,
)
from .. import ddl

StaticTablesTuple = namedtuple("StaticTablesTuple", ["a", "b", "c"])

STATIC_TABLE_SPECS = StaticTablesTuple(
    a=ddl.TableSpec(
        fields=[
            ddl.FieldSpec("name", dtype=sqlalchemy.String, length=16, primaryKey=True),
            ddl.FieldSpec("region", dtype=ddl.Base64Region, nbytes=128),
        ]
    ),
    b=ddl.TableSpec(
        fields=[
            ddl.FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            ddl.FieldSpec("name", dtype=sqlalchemy.String, length=16, nullable=False),
            ddl.FieldSpec("value", dtype=sqlalchemy.SmallInteger, nullable=True),
        ],
        unique=[("name",)],
    ),
    c=ddl.TableSpec(
        fields=[
            ddl.FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            ddl.FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            ddl.FieldSpec("b_id", dtype=sqlalchemy.BigInteger, nullable=True),
        ],
        foreignKeys=[
            ddl.ForeignKeySpec("b", source=("b_id",), target=("id",), onDelete="SET NULL"),
        ]
    ),
)

DYNAMIC_TABLE_SPEC = ddl.TableSpec(
    fields=[
        ddl.FieldSpec("c_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
        ddl.FieldSpec("c_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
        ddl.FieldSpec("a_name", dtype=sqlalchemy.String, length=16, nullable=False),
    ],
    foreignKeys=[
        ddl.ForeignKeySpec("c", source=("c_id", "c_origin"), target=("id", "origin"), onDelete="CASCADE"),
        ddl.ForeignKeySpec("a", source=("a_name",), target=("name",), onDelete="CASCADE"),
    ]
)


class DatabaseTests(ABC):
    """Generic tests for the `Database` interface that can be subclassed to
    generate tests for concrete implementations.
    """

    @abstractmethod
    def makeEmptyDatabase(self, origin: int = 0) -> Database:
        """Return an empty `Database` with the given origin, or an
        automatically-generated one if ``origin`` is `None`.
        """
        raise NotImplementedError()

    @abstractmethod
    def asReadOnly(self, database: Database) -> ContextManager[Database]:
        """Return a context manager for a read-only connection into the given
        database.

        The original database should be considered unusable within the context
        but safe to use again afterwards (this allows the context manager to
        block write access by temporarily changing user permissions to really
        guarantee that write operations are not performed).
        """
        raise NotImplementedError()

    @abstractmethod
    def getNewConnection(self, database: Database, *, writeable: bool) -> Database:
        """Return a new `Database` instance that points to the same underlying
        storage as the given one.
        """
        raise NotImplementedError()

    def checkTable(self, spec: ddl.TableSpec, table: sqlalchemy.schema.Table):
        self.assertCountEqual(spec.fields.names, table.columns.keys())
        # Checking more than this currently seems fragile, as it might restrict
        # what Database implementations do; we don't care if the spec is
        # actually preserved in terms of types and constraints as long as we
        # can use the returned table as if it was.

    def checkStaticSchema(self, tables: StaticTablesTuple):
        self.checkTable(STATIC_TABLE_SPECS.a, tables.a)
        self.checkTable(STATIC_TABLE_SPECS.b, tables.b)
        self.checkTable(STATIC_TABLE_SPECS.c, tables.c)

    def testDeclareStaticTables(self):
        """Tests for `Database.declareStaticSchema` and the methods it
        delegates to.
        """
        # Create the static schema in a new, empty database.
        newDatabase = self.makeEmptyDatabase()
        with newDatabase.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        self.checkStaticSchema(tables)
        # Check that we can load that schema even from a read-only connection.
        with self.asReadOnly(newDatabase) as existingReadOnlyDatabase:
            with existingReadOnlyDatabase.declareStaticTables(create=False) as context:
                tables = context.addTableTuple(STATIC_TABLE_SPECS)
            self.checkStaticSchema(tables)

    def testDynamicTables(self):
        """Tests for `Database.ensureTableExists` and
        `Database.getExistingTable`.
        """
        # Need to start with the static schema.
        newDatabase = self.makeEmptyDatabase()
        with newDatabase.declareStaticTables(create=True) as context:
            context.addTableTuple(STATIC_TABLE_SPECS)
        # Try to ensure the dyamic table exists in a read-only version of that
        # database, which should fail because we can't create it.
        with self.asReadOnly(newDatabase) as existingReadOnlyDatabase:
            with existingReadOnlyDatabase.declareStaticTables(create=False) as context:
                context.addTableTuple(STATIC_TABLE_SPECS)
            with self.assertRaises(ReadOnlyDatabaseError):
                existingReadOnlyDatabase.ensureTableExists("d", DYNAMIC_TABLE_SPEC)
        # Just getting the dynamic table before it exists should return None.
        self.assertIsNone(newDatabase.getExistingTable("d", DYNAMIC_TABLE_SPEC))
        # Ensure the new table exists back in the original database, which
        # should create it.
        table = newDatabase.ensureTableExists("d", DYNAMIC_TABLE_SPEC)
        self.checkTable(DYNAMIC_TABLE_SPEC, table)
        # Ensuring that it exists should just return the exact same table
        # instance again.
        self.assertIs(newDatabase.ensureTableExists("d", DYNAMIC_TABLE_SPEC), table)
        # Try again from the read-only database.
        with self.asReadOnly(newDatabase) as existingReadOnlyDatabase:
            with existingReadOnlyDatabase.declareStaticTables(create=False) as context:
                context.addTableTuple(STATIC_TABLE_SPECS)
            # Just getting the dynamic table should now work...
            self.assertIsNotNone(existingReadOnlyDatabase.getExistingTable("d", DYNAMIC_TABLE_SPEC))
            # ...as should ensuring that it exists, since it now does.
            existingReadOnlyDatabase.ensureTableExists("d", DYNAMIC_TABLE_SPEC)
            self.checkTable(DYNAMIC_TABLE_SPEC, table)
        # Trying to get the table with a different specification (at least
        # in terms of what columns are present) should raise.
        with self.assertRaises(RuntimeError):
            newDatabase.ensureTableExists(
                "d",
                ddl.TableSpec(
                    fields=[ddl.FieldSpec("name", dtype=sqlalchemy.String, length=4, primaryKey=True)]
                )
            )
        # Calling ensureTableExists inside a transaction block is an error,
        # even if it would do nothing.
        with newDatabase.transaction():
            with self.assertRaises(TransactionInterruption):
                newDatabase.ensureTableExists("d", DYNAMIC_TABLE_SPEC)

    def testSchemaSeparation(self):
        """Test that creating two different `Database` instances allows us
        to create different tables with the same name in each.
        """
        db1 = self.makeEmptyDatabase(origin=1)
        with db1.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        self.checkStaticSchema(tables)

        db2 = self.makeEmptyDatabase(origin=2)
        # Make the DDL here intentionally different so we'll definitely
        # notice if db1 and db2 are pointing at the same schema.
        spec = ddl.TableSpec(fields=[ddl.FieldSpec("id", dtype=sqlalchemy.Integer, primaryKey=True)])
        with db2.declareStaticTables(create=True) as context:
            # Make the DDL here intentionally different so we'll definitely
            # notice if db1 and db2 are pointing at the same schema.
            table = context.addTable("a", spec)
        self.checkTable(spec, table)
