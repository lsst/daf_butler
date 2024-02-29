# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from ... import ddl

__all__ = ["DatabaseTests"]

import asyncio
import itertools
import warnings
from abc import ABC, abstractmethod
from collections import namedtuple
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, contextmanager
from typing import Any

import astropy.time
import sqlalchemy
from lsst.sphgeom import Circle, ConvexPolygon, Mq3cPixelization, UnionRegion, UnitVector3d

from ..._timespan import Timespan
from ..interfaces import Database, DatabaseConflictError, ReadOnlyDatabaseError, SchemaAlreadyDefinedError

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
            ddl.FieldSpec("b_id", dtype=sqlalchemy.BigInteger, nullable=True),
        ],
        foreignKeys=[
            ddl.ForeignKeySpec("b", source=("b_id",), target=("id",), onDelete="SET NULL"),
        ],
    ),
)

DYNAMIC_TABLE_SPEC = ddl.TableSpec(
    fields=[
        ddl.FieldSpec("c_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
        ddl.FieldSpec("a_name", dtype=sqlalchemy.String, length=16, nullable=False),
    ],
    foreignKeys=[
        ddl.ForeignKeySpec("c", source=("c_id",), target=("id",), onDelete="CASCADE"),
        ddl.ForeignKeySpec("a", source=("a_name",), target=("name",), onDelete="CASCADE"),
    ],
)

TEMPORARY_TABLE_SPEC = ddl.TableSpec(
    fields=[
        ddl.FieldSpec("a_name", dtype=sqlalchemy.String, length=16, primaryKey=True),
        ddl.FieldSpec("b_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
    ],
)


@contextmanager
def _patch_getExistingTable(db: Database) -> Database:
    """Patch getExistingTable method in a database instance to test concurrent
    creation of tables. This patch obviously depends on knowning internals of
    ``ensureTableExists()`` implementation.
    """
    original_method = db.getExistingTable

    def _getExistingTable(name: str, spec: ddl.TableSpec) -> sqlalchemy.schema.Table | None:
        # Return None on first call, but forward to original method after that
        db.getExistingTable = original_method
        return None

    db.getExistingTable = _getExistingTable
    yield db
    db.getExistingTable = original_method


class DatabaseTests(ABC):
    """Generic tests for the `Database` interface that can be subclassed to
    generate tests for concrete implementations.
    """

    @abstractmethod
    def makeEmptyDatabase(self, origin: int = 0) -> Database:
        """Return an empty `Database` with the given origin, or an
        automatically-generated one if ``origin`` is `None`.

        Parameters
        ----------
        origin : `int` or `None`
            Origin to use for the database.

        Returns
        -------
        db : `Database`
            Empty database with given origin or auto-generated origin.
        """
        raise NotImplementedError()

    @abstractmethod
    def asReadOnly(self, database: Database) -> AbstractContextManager[Database]:
        """Return a context manager for a read-only connection into the given
        database.

        Parameters
        ----------
        database : `Database`
            The database to use.

        Yields
        ------
        `Database`
            The new database connection.

        Notes
        -----
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

        Parameters
        ----------
        database : `Database`
            The current database.
        writeable : `bool`
            Whether the connection should be writeable or not.

        Returns
        -------
        db : `Database`
            The new database connection.
        """
        raise NotImplementedError()

    def query_list(
        self, database: Database, executable: sqlalchemy.sql.expression.SelectBase
    ) -> list[sqlalchemy.engine.Row]:
        """Run a SELECT or other read-only query against the database and
        return the results as a list.

        Parameters
        ----------
        database : `Database`
            The database to use.
        executable : `sqlalchemy.sql.expression.SelectBase`
            Expression to execute.

        Returns
        -------
        results : `list` of `sqlalchemy.engine.Row`
            The results.

        Notes
        -----
        This is a thin wrapper around database.query() that just avoids
        context-manager boilerplate that is usefully verbose in production code
        but just noise in tests.
        """
        with database.transaction(), database.query(executable) as result:
            return result.fetchall()

    def query_scalar(self, database: Database, executable: sqlalchemy.sql.expression.SelectBase) -> Any:
        """Run a SELECT query that yields a single column and row against the
        database and return its value.

        Parameters
        ----------
        database : `Database`
            The database to use.
        executable : `sqlalchemy.sql.expression.SelectBase`
            Expression to execute.

        Returns
        -------
        results : `~typing.Any`
            The results.

        Notes
        -----
        This is a thin wrapper around database.query() that just avoids
        context-manager boilerplate that is usefully verbose in production code
        but just noise in tests.
        """
        with database.query(executable) as result:
            return result.scalar()

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

    def testDeclareStaticTablesTwice(self):
        """Tests for `Database.declareStaticSchema` being called twice."""
        # Create the static schema in a new, empty database.
        newDatabase = self.makeEmptyDatabase()
        with newDatabase.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        self.checkStaticSchema(tables)
        # Second time it should raise
        with self.assertRaises(SchemaAlreadyDefinedError):
            with newDatabase.declareStaticTables(create=True) as context:
                tables = context.addTableTuple(STATIC_TABLE_SPECS)
        # Check schema, it should still contain all tables, and maybe some
        # extra.
        with newDatabase.declareStaticTables(create=False) as context:
            self.assertLessEqual(frozenset(STATIC_TABLE_SPECS._fields), context._tableNames)

    def testRepr(self):
        """Test that repr does not return a generic thing."""
        newDatabase = self.makeEmptyDatabase()
        rep = repr(newDatabase)
        # Check that stringification works and gives us something different
        self.assertNotEqual(rep, str(newDatabase))
        self.assertNotIn("object at 0x", rep, "Check default repr was not used")
        self.assertIn("://", rep)

    def testDynamicTables(self):
        """Tests for `Database.ensureTableExists` and
        `Database.getExistingTable`.
        """
        # Need to start with the static schema.
        newDatabase = self.makeEmptyDatabase()
        with newDatabase.declareStaticTables(create=True) as context:
            context.addTableTuple(STATIC_TABLE_SPECS)
        # Try to ensure the dynamic table exists in a read-only version of that
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
        with self.assertRaises(DatabaseConflictError):
            newDatabase.ensureTableExists(
                "d",
                ddl.TableSpec(
                    fields=[ddl.FieldSpec("name", dtype=sqlalchemy.String, length=4, primaryKey=True)]
                ),
            )
        # Calling ensureTableExists inside a transaction block is an error,
        # even if it would do nothing.
        with newDatabase.transaction():
            with self.assertRaises(AssertionError):
                newDatabase.ensureTableExists("d", DYNAMIC_TABLE_SPEC)

    def testDynamicTablesConcurrency(self):
        """Tests for `Database.ensureTableExists` concurrent use."""
        # We cannot really run things concurrently in a deterministic way, here
        # we just simulate a situation when the table is created by other
        # process between the call to getExistingTable() and actual table
        # creation.
        db1 = self.makeEmptyDatabase()
        with db1.declareStaticTables(create=True) as context:
            context.addTableTuple(STATIC_TABLE_SPECS)
        self.assertIsNone(db1.getExistingTable("d", DYNAMIC_TABLE_SPEC))

        # Make a dynamic table using separate connection
        db2 = self.getNewConnection(db1, writeable=True)
        with db2.declareStaticTables(create=False) as context:
            context.addTableTuple(STATIC_TABLE_SPECS)
        table = db2.ensureTableExists("d", DYNAMIC_TABLE_SPEC)
        self.checkTable(DYNAMIC_TABLE_SPEC, table)

        # Call it again but trick it into thinking that table is not there.
        # This test depends on knowing implementation of ensureTableExists()
        # which initially calls getExistingTable() to check that table may
        # exist, the patch intercepts that call and returns None.
        with _patch_getExistingTable(db1):
            table = db1.ensureTableExists("d", DYNAMIC_TABLE_SPEC)

    def testTemporaryTables(self):
        """Tests for `Database.temporary_table`, and `Database.insert` with the
        ``select`` argument.
        """
        # Need to start with the static schema; also insert some test data.
        newDatabase = self.makeEmptyDatabase()
        with newDatabase.declareStaticTables(create=True) as context:
            static = context.addTableTuple(STATIC_TABLE_SPECS)
        newDatabase.insert(static.a, {"name": "a1", "region": None}, {"name": "a2", "region": None})
        bIds = newDatabase.insert(
            static.b,
            {"name": "b1", "value": 11},
            {"name": "b2", "value": 12},
            {"name": "b3", "value": 13},
            returnIds=True,
        )
        # Create the table.
        with newDatabase.session():
            with newDatabase.temporary_table(TEMPORARY_TABLE_SPEC, "e1") as table1:
                self.checkTable(TEMPORARY_TABLE_SPEC, table1)
                # Insert via a INSERT INTO ... SELECT query.
                newDatabase.insert(
                    table1,
                    select=sqlalchemy.sql.select(
                        static.a.columns.name.label("a_name"), static.b.columns.id.label("b_id")
                    )
                    .select_from(static.a.join(static.b, onclause=sqlalchemy.sql.literal(True)))
                    .where(
                        sqlalchemy.sql.and_(
                            static.a.columns.name == "a1",
                            static.b.columns.value <= 12,
                        )
                    ),
                )
                # Check that the inserted rows are present.
                self.assertCountEqual(
                    [{"a_name": "a1", "b_id": bId} for bId in bIds[:2]],
                    [row._asdict() for row in self.query_list(newDatabase, table1.select())],
                )
            # Create another one via a read-only connection to the
            # database.  We _do_ allow temporary table modifications in
            # read-only databases.
            with self.asReadOnly(newDatabase) as existingReadOnlyDatabase:
                with existingReadOnlyDatabase.declareStaticTables(create=False) as context:
                    context.addTableTuple(STATIC_TABLE_SPECS)
                with existingReadOnlyDatabase.temporary_table(TEMPORARY_TABLE_SPEC) as table2:
                    self.checkTable(TEMPORARY_TABLE_SPEC, table2)
                    # Those tables should not be the same, despite having
                    # the same ddl.
                    self.assertIsNot(table1, table2)
                    # Do a slightly different insert into this table, to
                    # check that it works in a read-only database.  This
                    # time we pass column names as a kwarg to insert
                    # instead of by labeling the columns in the select.
                    existingReadOnlyDatabase.insert(
                        table2,
                        select=sqlalchemy.sql.select(static.a.columns.name, static.b.columns.id)
                        .select_from(static.a.join(static.b, onclause=sqlalchemy.sql.literal(True)))
                        .where(
                            sqlalchemy.sql.and_(
                                static.a.columns.name == "a2",
                                static.b.columns.value >= 12,
                            )
                        ),
                        names=["a_name", "b_id"],
                    )
                    # Check that the inserted rows are present.
                    self.assertCountEqual(
                        [{"a_name": "a2", "b_id": bId} for bId in bIds[1:]],
                        [row._asdict() for row in self.query_list(existingReadOnlyDatabase, table2.select())],
                    )
            # Exiting the context managers will drop the temporary tables from
            # the read-only DB.  It's unspecified whether attempting to use it
            # after this point is an error or just never returns any results,
            # so we can't test what it does, only that it's not an error.

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

    def testInsertQueryDelete(self):
        """Test the `Database.insert`, `Database.query`, and `Database.delete`
        methods, as well as the `Base64Region` type and the ``onDelete``
        argument to `ddl.ForeignKeySpec`.
        """
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        # Insert a single, non-autoincrement row that contains a region and
        # query to get it back.
        region = ConvexPolygon((UnitVector3d(1, 0, 0), UnitVector3d(0, 1, 0), UnitVector3d(0, 0, 1)))
        row = {"name": "a1", "region": region}
        db.insert(tables.a, row)
        self.assertEqual([r._asdict() for r in self.query_list(db, tables.a.select())], [row])
        # Insert multiple autoincrement rows but do not try to get the IDs
        # back immediately.
        db.insert(tables.b, {"name": "b1", "value": 10}, {"name": "b2", "value": 20})
        results = [r._asdict() for r in self.query_list(db, tables.b.select().order_by("id"))]
        self.assertEqual(len(results), 2)
        for row in results:
            self.assertIn(row["name"], ("b1", "b2"))
            self.assertIsInstance(row["id"], int)
        self.assertGreater(results[1]["id"], results[0]["id"])
        # Insert multiple autoincrement rows and get the IDs back from insert.
        rows = [{"name": "b3", "value": 30}, {"name": "b4", "value": 40}]
        ids = db.insert(tables.b, *rows, returnIds=True)
        results = [
            r._asdict()
            for r in self.query_list(db, tables.b.select().where(tables.b.columns.id > results[1]["id"]))
        ]
        expected = [dict(row, id=id) for row, id in zip(rows, ids, strict=True)]
        self.assertCountEqual(results, expected)
        self.assertTrue(all(result["id"] is not None for result in results))
        # Insert multiple rows into a table with an autoincrement primary key,
        # then use the returned IDs to insert into a dynamic table.
        rows = [{"b_id": results[0]["id"]}, {"b_id": None}]
        ids = db.insert(tables.c, *rows, returnIds=True)
        results = [r._asdict() for r in self.query_list(db, tables.c.select())]
        expected = [dict(row, id=id) for row, id in zip(rows, ids, strict=True)]
        self.assertCountEqual(results, expected)
        self.assertTrue(all(result["id"] is not None for result in results))
        # Add the dynamic table.
        d = db.ensureTableExists("d", DYNAMIC_TABLE_SPEC)
        # Insert into it.
        rows = [{"c_id": id, "a_name": "a1"} for id in ids]
        db.insert(d, *rows)
        results = [r._asdict() for r in self.query_list(db, d.select())]
        self.assertCountEqual(rows, results)
        # Insert multiple rows into a table with an autoincrement primary key,
        # but pass in a value for the autoincrement key.
        rows2 = [
            {"id": 700, "b_id": None},
            {"id": 701, "b_id": None},
        ]
        db.insert(tables.c, *rows2)
        results = [r._asdict() for r in self.query_list(db, tables.c.select())]
        self.assertCountEqual(results, expected + rows2)
        self.assertTrue(all(result["id"] is not None for result in results))

        # Define 'SELECT COUNT(*)' query for later use.
        count = sqlalchemy.sql.select(sqlalchemy.sql.func.count())
        # Get the values we inserted into table b.
        bValues = [r._asdict() for r in self.query_list(db, tables.b.select())]
        # Remove two row from table b by ID.
        n = db.delete(tables.b, ["id"], {"id": bValues[0]["id"]}, {"id": bValues[1]["id"]})
        self.assertEqual(n, 2)
        # Remove the other two rows from table b by name.
        n = db.delete(tables.b, ["name"], {"name": bValues[2]["name"]}, {"name": bValues[3]["name"]})
        self.assertEqual(n, 2)
        # There should now be no rows in table b.
        self.assertEqual(self.query_scalar(db, count.select_from(tables.b)), 0)
        # All b_id values in table c should now be NULL, because there's an
        # onDelete='SET NULL' foreign key.
        self.assertEqual(
            self.query_scalar(
                db,
                count.select_from(tables.c).where(tables.c.columns.b_id != None),  # noqa:E711
            ),
            0,
        )
        # Remove all rows in table a (there's only one); this should remove all
        # rows in d due to onDelete='CASCADE'.
        n = db.delete(tables.a, [])
        self.assertEqual(n, 1)
        self.assertEqual(self.query_scalar(db, count.select_from(tables.a)), 0)
        self.assertEqual(self.query_scalar(db, count.select_from(d)), 0)

    def testDeleteWhere(self):
        """Tests for `Database.deleteWhere`."""
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        db.insert(tables.b, *[{"id": i, "name": f"b{i}"} for i in range(10)])
        count = sqlalchemy.sql.select(sqlalchemy.sql.func.count())

        n = db.deleteWhere(tables.b, tables.b.columns.id.in_([0, 1, 2]))
        self.assertEqual(n, 3)
        self.assertEqual(self.query_scalar(db, count.select_from(tables.b)), 7)

        n = db.deleteWhere(
            tables.b,
            tables.b.columns.id.in_(
                sqlalchemy.sql.select(tables.b.columns.id).where(tables.b.columns.id > 5)
            ),
        )
        self.assertEqual(n, 4)
        self.assertEqual(self.query_scalar(db, count.select_from(tables.b)), 3)

        n = db.deleteWhere(tables.b, tables.b.columns.name == "b5")
        self.assertEqual(n, 1)
        self.assertEqual(self.query_scalar(db, count.select_from(tables.b)), 2)

        n = db.deleteWhere(tables.b, sqlalchemy.sql.literal(True))
        self.assertEqual(n, 2)
        self.assertEqual(self.query_scalar(db, count.select_from(tables.b)), 0)

    def testUpdate(self):
        """Tests for `Database.update`."""
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        # Insert two rows into table a, both without regions.
        db.insert(tables.a, {"name": "a1"}, {"name": "a2"})
        # Update one of the rows with a region.
        region = ConvexPolygon((UnitVector3d(1, 0, 0), UnitVector3d(0, 1, 0), UnitVector3d(0, 0, 1)))
        n = db.update(tables.a, {"name": "k"}, {"k": "a2", "region": region})
        self.assertEqual(n, 1)
        sql = sqlalchemy.sql.select(tables.a.columns.name, tables.a.columns.region).select_from(tables.a)
        self.assertCountEqual(
            [r._asdict() for r in self.query_list(db, sql)],
            [{"name": "a1", "region": None}, {"name": "a2", "region": region}],
        )

    def testSync(self):
        """Tests for `Database.sync`."""
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        # Insert a row with sync, because it doesn't exist yet.
        values, inserted = db.sync(tables.b, keys={"name": "b1"}, extra={"value": 10}, returning=["id"])
        self.assertTrue(inserted)
        self.assertEqual(
            [{"id": values["id"], "name": "b1", "value": 10}],
            [r._asdict() for r in self.query_list(db, tables.b.select())],
        )
        # Repeat that operation, which should do nothing but return the
        # requested values.
        values, inserted = db.sync(tables.b, keys={"name": "b1"}, extra={"value": 10}, returning=["id"])
        self.assertFalse(inserted)
        self.assertEqual(
            [{"id": values["id"], "name": "b1", "value": 10}],
            [r._asdict() for r in self.query_list(db, tables.b.select())],
        )
        # Repeat the operation without the 'extra' arg, which should also just
        # return the existing row.
        values, inserted = db.sync(tables.b, keys={"name": "b1"}, returning=["id"])
        self.assertFalse(inserted)
        self.assertEqual(
            [{"id": values["id"], "name": "b1", "value": 10}],
            [r._asdict() for r in self.query_list(db, tables.b.select())],
        )
        # Repeat the operation with a different value in 'extra'.  That still
        # shouldn't be an error, because 'extra' is only used if we really do
        # insert.  Also drop the 'returning' argument.
        _, inserted = db.sync(tables.b, keys={"name": "b1"}, extra={"value": 20})
        self.assertFalse(inserted)
        self.assertEqual(
            [{"id": values["id"], "name": "b1", "value": 10}],
            [r._asdict() for r in self.query_list(db, tables.b.select())],
        )
        # Repeat the operation with the correct value in 'compared' instead of
        # 'extra'.
        _, inserted = db.sync(tables.b, keys={"name": "b1"}, compared={"value": 10})
        self.assertFalse(inserted)
        self.assertEqual(
            [{"id": values["id"], "name": "b1", "value": 10}],
            [r._asdict() for r in self.query_list(db, tables.b.select())],
        )
        # Repeat the operation with an incorrect value in 'compared'; this
        # should raise.
        with self.assertRaises(DatabaseConflictError):
            db.sync(tables.b, keys={"name": "b1"}, compared={"value": 20})
        # Try to sync in a read-only database.  This should work if and only
        # if the matching row already exists.
        with self.asReadOnly(db) as rodb:
            with rodb.declareStaticTables(create=False) as context:
                tables = context.addTableTuple(STATIC_TABLE_SPECS)
            _, inserted = rodb.sync(tables.b, keys={"name": "b1"})
            self.assertFalse(inserted)
            self.assertEqual(
                [{"id": values["id"], "name": "b1", "value": 10}],
                [r._asdict() for r in self.query_list(rodb, tables.b.select())],
            )
            with self.assertRaises(ReadOnlyDatabaseError):
                rodb.sync(tables.b, keys={"name": "b2"}, extra={"value": 20})
        # Repeat the operation with a different value in 'compared' and ask to
        # update.
        _, updated = db.sync(tables.b, keys={"name": "b1"}, compared={"value": 20}, update=True)
        self.assertEqual(updated, {"value": 10})
        self.assertEqual(
            [{"id": values["id"], "name": "b1", "value": 20}],
            [r._asdict() for r in self.query_list(db, tables.b.select())],
        )

    def testReplace(self):
        """Tests for `Database.replace`."""
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        # Use 'replace' to insert a single row that contains a region and
        # query to get it back.
        region = ConvexPolygon((UnitVector3d(1, 0, 0), UnitVector3d(0, 1, 0), UnitVector3d(0, 0, 1)))
        row1 = {"name": "a1", "region": region}
        db.replace(tables.a, row1)
        self.assertEqual([r._asdict() for r in self.query_list(db, tables.a.select())], [row1])
        # Insert another row without a region.
        row2 = {"name": "a2", "region": None}
        db.replace(tables.a, row2)
        self.assertCountEqual([r._asdict() for r in self.query_list(db, tables.a.select())], [row1, row2])
        # Use replace to re-insert both of those rows again, which should do
        # nothing.
        db.replace(tables.a, row1, row2)
        self.assertCountEqual([r._asdict() for r in self.query_list(db, tables.a.select())], [row1, row2])
        # Replace row1 with a row with no region, while reinserting row2.
        row1a = {"name": "a1", "region": None}
        db.replace(tables.a, row1a, row2)
        self.assertCountEqual([r._asdict() for r in self.query_list(db, tables.a.select())], [row1a, row2])
        # Replace both rows, returning row1 to its original state, while adding
        # a new one.  Pass them in in a different order.
        row2a = {"name": "a2", "region": region}
        row3 = {"name": "a3", "region": None}
        db.replace(tables.a, row3, row2a, row1)
        self.assertCountEqual(
            [r._asdict() for r in self.query_list(db, tables.a.select())], [row1, row2a, row3]
        )

    def testEnsure(self):
        """Tests for `Database.ensure`."""
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        # Use 'ensure' to insert a single row that contains a region and
        # query to get it back.
        region = ConvexPolygon((UnitVector3d(1, 0, 0), UnitVector3d(0, 1, 0), UnitVector3d(0, 0, 1)))
        row1 = {"name": "a1", "region": region}
        self.assertEqual(db.ensure(tables.a, row1), 1)
        self.assertEqual([r._asdict() for r in self.query_list(db, tables.a.select())], [row1])
        # Insert another row without a region.
        row2 = {"name": "a2", "region": None}
        self.assertEqual(db.ensure(tables.a, row2), 1)
        self.assertCountEqual([r._asdict() for r in self.query_list(db, tables.a.select())], [row1, row2])
        # Use ensure to re-insert both of those rows again, which should do
        # nothing.
        self.assertEqual(db.ensure(tables.a, row1, row2), 0)
        self.assertCountEqual([r._asdict() for r in self.query_list(db, tables.a.select())], [row1, row2])
        # Attempt to insert row1's key with no region, while
        # reinserting row2.  This should also do nothing.
        row1a = {"name": "a1", "region": None}
        self.assertEqual(db.ensure(tables.a, row1a, row2), 0)
        self.assertCountEqual([r._asdict() for r in self.query_list(db, tables.a.select())], [row1, row2])
        # Attempt to insert new rows for both existing keys, this time also
        # adding a new row.  Pass them in in a different order.  Only the new
        # row should be added.
        row2a = {"name": "a2", "region": region}
        row3 = {"name": "a3", "region": None}
        self.assertEqual(db.ensure(tables.a, row3, row2a, row1a), 1)
        self.assertCountEqual(
            [r._asdict() for r in self.query_list(db, tables.a.select())], [row1, row2, row3]
        )
        # Add some data to a table with both a primary key and a different
        # unique constraint.
        row_b = {"id": 5, "name": "five", "value": 50}
        db.insert(tables.b, row_b)
        # Attempt ensure with primary_key_only=False and a conflict for the
        # non-PK constraint.  This should do nothing.
        db.ensure(tables.b, {"id": 10, "name": "five", "value": 200})
        self.assertEqual([r._asdict() for r in self.query_list(db, tables.b.select())], [row_b])
        # Now use primary_key_only=True with conflict in only the non-PK field.
        # This should be an integrity error and nothing should change.
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            db.ensure(tables.b, {"id": 10, "name": "five", "value": 200}, primary_key_only=True)
        self.assertEqual([r._asdict() for r in self.query_list(db, tables.b.select())], [row_b])
        # With primary_key_only=True a conflict in the primary key is ignored
        # regardless of whether there is a conflict elsewhere.
        db.ensure(tables.b, {"id": 5, "name": "ten", "value": 100}, primary_key_only=True)
        self.assertEqual([r._asdict() for r in self.query_list(db, tables.b.select())], [row_b])
        db.ensure(tables.b, {"id": 5, "name": "five", "value": 100}, primary_key_only=True)
        self.assertEqual([r._asdict() for r in self.query_list(db, tables.b.select())], [row_b])

    def testTransactionNesting(self):
        """Test that transactions can be nested with the behavior in the
        presence of exceptions working as documented.
        """
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        # Insert one row so we can trigger integrity errors by trying to insert
        # a duplicate of it below.
        db.insert(tables.a, {"name": "a1"})
        # First test: error recovery via explicit savepoint=True in the inner
        # transaction.
        with db.transaction():
            # This insert should succeed, and should not be rolled back because
            # the assertRaises context should catch any exception before it
            # propagates up to the outer transaction.
            db.insert(tables.a, {"name": "a2"})
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                with db.transaction(savepoint=True):
                    # This insert should succeed, but should be rolled back.
                    db.insert(tables.a, {"name": "a4"})
                    # This insert should fail (duplicate primary key), raising
                    # an exception.
                    db.insert(tables.a, {"name": "a1"})
        self.assertCountEqual(
            [r._asdict() for r in self.query_list(db, tables.a.select())],
            [{"name": "a1", "region": None}, {"name": "a2", "region": None}],
        )
        # Second test: error recovery via implicit savepoint=True, when the
        # innermost transaction is inside a savepoint=True transaction.
        with db.transaction():
            # This insert should succeed, and should not be rolled back
            # because the assertRaises context should catch any
            # exception before it propagates up to the outer
            # transaction.
            db.insert(tables.a, {"name": "a3"})
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                with db.transaction(savepoint=True):
                    # This insert should succeed, but should be rolled back.
                    db.insert(tables.a, {"name": "a4"})
                    with db.transaction():
                        # This insert should succeed, but should be rolled
                        # back.
                        db.insert(tables.a, {"name": "a5"})
                        # This insert should fail (duplicate primary key),
                        # raising an exception.
                        db.insert(tables.a, {"name": "a1"})
        self.assertCountEqual(
            [r._asdict() for r in self.query_list(db, tables.a.select())],
            [{"name": "a1", "region": None}, {"name": "a2", "region": None}, {"name": "a3", "region": None}],
        )

    def testTransactionLocking(self):
        """Test that `Database.transaction` can be used to acquire a lock
        that prohibits concurrent writes.
        """
        db1 = self.makeEmptyDatabase(origin=1)
        with db1.declareStaticTables(create=True) as context:
            tables1 = context.addTableTuple(STATIC_TABLE_SPECS)

        async def _side1(lock: Iterable[str] = ()) -> tuple[set[str], set[str]]:
            """One side of the concurrent locking test.

            Parameters
            ----------
            lock : `~collections.abc.Iterable` of `str`
                Locks.

            Notes
            -----
            This optionally locks the table (and maybe the whole database),
            does a select for its contents, inserts a new row, and then selects
            again, with some waiting in between to make sure the other side has
            a chance to _attempt_ to insert in between.  If the locking is
            enabled and works, the difference between the selects should just
            be the insert done on this thread.
            """
            # Give Side2 a chance to create a connection
            await asyncio.sleep(1.0)
            with db1.transaction(lock=lock):
                names1 = {row.name for row in self.query_list(db1, tables1.a.select())}
                # Give Side2 a chance to insert (which will be blocked if
                # we've acquired a lock).
                await asyncio.sleep(2.0)
                db1.insert(tables1.a, {"name": "a1"})
                names2 = {row.name for row in self.query_list(db1, tables1.a.select())}
            return names1, names2

        async def _side2() -> None:
            """Other side of the concurrent locking test.

            Notes
            -----
            This side just waits a bit and then tries to insert a row into the
            table that the other side is trying to lock.  Hopefully that
            waiting is enough to give the other side a chance to acquire the
            lock and thus make this side block until the lock is released.  If
            this side manages to do the insert before side1 acquires the lock,
            we'll just warn about not succeeding at testing the locking,
            because we can only make that unlikely, not impossible.
            """

            def _toRunInThread():
                """Create new SQLite connection for use in thread.

                SQLite locking isn't asyncio-friendly unless we actually
                run it in another thread.  And SQLite gets very unhappy if
                we try to use a connection from multiple threads, so we have
                to create the new connection here instead of out in the main
                body of the test function.
                """
                db2 = self.getNewConnection(db1, writeable=True)
                with db2.declareStaticTables(create=False) as context:
                    tables2 = context.addTableTuple(STATIC_TABLE_SPECS)
                with db2.transaction():
                    db2.insert(tables2.a, {"name": "a2"})

            await asyncio.sleep(2.0)
            loop = asyncio.get_running_loop()
            with ThreadPoolExecutor() as pool:
                await loop.run_in_executor(pool, _toRunInThread)

        async def _testProblemsWithNoLocking() -> None:
            """Run side1 and side2 with no locking, attempting to demonstrate
            the problem that locking is supposed to solve.  If we get unlucky
            with scheduling, side2 will just happen to insert after side1 is
            done, and we won't have anything definitive.  We just warn in that
            case because we really don't want spurious test failures.
            """
            task1 = asyncio.create_task(_side1())
            task2 = asyncio.create_task(_side2())

            names1, names2 = await task1
            await task2
            if "a2" in names1:
                warnings.warn(
                    "Unlucky scheduling in no-locking test: concurrent INSERT happened before first SELECT.",
                    stacklevel=1,
                )
                self.assertEqual(names1, {"a2"})
                self.assertEqual(names2, {"a1", "a2"})
            elif "a2" not in names2:
                warnings.warn(
                    "Unlucky scheduling in no-locking test: concurrent INSERT "
                    "happened after second SELECT even without locking.",
                    stacklevel=1,
                )
                self.assertEqual(names1, set())
                self.assertEqual(names2, {"a1"})
            else:
                # This is the expected case: both INSERTS happen between the
                # two SELECTS.  If we don't get this almost all of the time we
                # should adjust the sleep amounts.
                self.assertEqual(names1, set())
                self.assertEqual(names2, {"a1", "a2"})

        asyncio.run(_testProblemsWithNoLocking())

        # Clean up after first test.
        db1.delete(tables1.a, ["name"], {"name": "a1"}, {"name": "a2"})

        async def _testSolutionWithLocking() -> None:
            """Run side1 and side2 with locking, which should make side2 block
            its insert until side2 releases its lock.
            """
            task1 = asyncio.create_task(_side1(lock=[tables1.a]))
            task2 = asyncio.create_task(_side2())

            names1, names2 = await task1
            await task2
            if "a2" in names1:
                warnings.warn(
                    "Unlucky scheduling in locking test: concurrent INSERT happened before first SELECT.",
                    stacklevel=1,
                )
                self.assertEqual(names1, {"a2"})
                self.assertEqual(names2, {"a1", "a2"})
            else:
                # This is the expected case: the side2 INSERT happens after the
                # last SELECT on side1.  This can also happen due to unlucky
                # scheduling, and we have no way to detect that here, but the
                # similar "no-locking" test has at least some chance of being
                # affected by the same problem and warning about it.
                self.assertEqual(names1, set())
                self.assertEqual(names2, {"a1"})

        asyncio.run(_testSolutionWithLocking())

    def testTimespanDatabaseRepresentation(self):
        """Tests for `TimespanDatabaseRepresentation` and the `Database`
        methods that interact with it.
        """
        # Make some test timespans to play with, with the full suite of
        # topological relationships.
        start = astropy.time.Time("2020-01-01T00:00:00", format="isot", scale="tai")
        offset = astropy.time.TimeDelta(60, format="sec")
        timestamps = [start + offset * n for n in range(3)]
        aTimespans = [Timespan(begin=None, end=None)]
        aTimespans.extend(Timespan(begin=None, end=t) for t in timestamps)
        aTimespans.extend(Timespan(begin=t, end=None) for t in timestamps)
        aTimespans.extend(Timespan.fromInstant(t) for t in timestamps)
        aTimespans.append(Timespan.makeEmpty())
        aTimespans.extend(Timespan(begin=t1, end=t2) for t1, t2 in itertools.combinations(timestamps, 2))
        # Make another list of timespans that span the full range but don't
        # overlap.  This is a subset of the previous list.
        bTimespans = [Timespan(begin=None, end=timestamps[0])]
        bTimespans.extend(
            Timespan(begin=t1, end=t2) for t1, t2 in zip(timestamps[:-1], timestamps[1:], strict=True)
        )
        bTimespans.append(Timespan(begin=timestamps[-1], end=None))
        # Make a database and create a table with that database's timespan
        # representation.  This one will have no exclusion constraint and
        # a nullable timespan.
        db = self.makeEmptyDatabase(origin=1)
        TimespanReprClass = db.getTimespanRepresentation()
        aSpec = ddl.TableSpec(
            fields=[
                ddl.FieldSpec(name="id", dtype=sqlalchemy.Integer, primaryKey=True),
            ],
        )
        for fieldSpec in TimespanReprClass.makeFieldSpecs(nullable=True):
            aSpec.fields.add(fieldSpec)
        with db.declareStaticTables(create=True) as context:
            aTable = context.addTable("a", aSpec)
        self.maxDiff = None

        def _convertRowForInsert(row: dict) -> dict:
            """Convert a row containing a Timespan instance into one suitable
            for insertion into the database.
            """
            result = row.copy()
            ts = result.pop(TimespanReprClass.NAME)
            return TimespanReprClass.update(ts, result=result)

        def _convertRowFromSelect(row: dict) -> dict:
            """Convert a row from the database into one containing a Timespan.

            Parameters
            ----------
            row : `dict`
                Original row.

            Returns
            -------
            row : `dict`
                The updated row.
            """
            result = row.copy()
            timespan = TimespanReprClass.extract(result)
            for name in TimespanReprClass.getFieldNames():
                del result[name]
            result[TimespanReprClass.NAME] = timespan
            return result

        # Insert rows into table A, in chunks just to make things interesting.
        # Include one with a NULL timespan.
        aRows = [{"id": n, TimespanReprClass.NAME: t} for n, t in enumerate(aTimespans)]
        aRows.append({"id": len(aRows), TimespanReprClass.NAME: None})
        db.insert(aTable, _convertRowForInsert(aRows[0]))
        db.insert(aTable, *[_convertRowForInsert(r) for r in aRows[1:3]])
        db.insert(aTable, *[_convertRowForInsert(r) for r in aRows[3:]])
        # Add another one with a NULL timespan, but this time by invoking
        # the server-side default.
        aRows.append({"id": len(aRows)})
        db.insert(aTable, aRows[-1])
        aRows[-1][TimespanReprClass.NAME] = None
        # Test basic round-trip through database.
        self.assertEqual(
            aRows,
            [
                _convertRowFromSelect(row._asdict())
                for row in self.query_list(db, aTable.select().order_by(aTable.columns.id))
            ],
        )
        # Create another table B with a not-null timespan and (if the database
        # supports it), an exclusion constraint.  Use ensureTableExists this
        # time to check that mode of table creation vs. timespans.
        bSpec = ddl.TableSpec(
            fields=[
                ddl.FieldSpec(name="id", dtype=sqlalchemy.Integer, primaryKey=True),
                ddl.FieldSpec(name="key", dtype=sqlalchemy.Integer, nullable=False),
            ],
        )
        for fieldSpec in TimespanReprClass.makeFieldSpecs(nullable=False):
            bSpec.fields.add(fieldSpec)
        if TimespanReprClass.hasExclusionConstraint():
            bSpec.exclusion.add(("key", TimespanReprClass))
        bTable = db.ensureTableExists("b", bSpec)
        # Insert rows into table B, again in chunks.  Each Timespan appears
        # twice, but with different values for the 'key' field (which should
        # still be okay for any exclusion constraint we may have defined).
        bRows = [{"id": n, "key": 1, TimespanReprClass.NAME: t} for n, t in enumerate(bTimespans)]
        offset = len(bRows)
        bRows.extend(
            {"id": n + offset, "key": 2, TimespanReprClass.NAME: t} for n, t in enumerate(bTimespans)
        )
        db.insert(bTable, *[_convertRowForInsert(r) for r in bRows[:2]])
        db.insert(bTable, _convertRowForInsert(bRows[2]))
        db.insert(bTable, *[_convertRowForInsert(r) for r in bRows[3:]])
        # Insert a row with no timespan into table B.  This should invoke the
        # server-side default, which is a timespan over (-∞, ∞).  We set
        # key=3 to avoid upsetting an exclusion constraint that might exist.
        bRows.append({"id": len(bRows), "key": 3})
        db.insert(bTable, bRows[-1])
        bRows[-1][TimespanReprClass.NAME] = Timespan(None, None)
        # Test basic round-trip through database.
        self.assertEqual(
            bRows,
            [
                _convertRowFromSelect(row._asdict())
                for row in self.query_list(db, bTable.select().order_by(bTable.columns.id))
            ],
        )
        # Test that we can't insert timespan=None into this table.
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            db.insert(
                bTable, _convertRowForInsert({"id": len(bRows), "key": 4, TimespanReprClass.NAME: None})
            )
        # IFF this database supports exclusion constraints, test that they
        # also prevent inserts.
        if TimespanReprClass.hasExclusionConstraint():
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                db.insert(
                    bTable,
                    _convertRowForInsert(
                        {"id": len(bRows), "key": 1, TimespanReprClass.NAME: Timespan(None, timestamps[1])}
                    ),
                )
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                db.insert(
                    bTable,
                    _convertRowForInsert(
                        {
                            "id": len(bRows),
                            "key": 1,
                            TimespanReprClass.NAME: Timespan(timestamps[0], timestamps[2]),
                        }
                    ),
                )
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                db.insert(
                    bTable,
                    _convertRowForInsert(
                        {"id": len(bRows), "key": 1, TimespanReprClass.NAME: Timespan(timestamps[2], None)}
                    ),
                )
        # Test NULL checks in SELECT queries, on both tables.
        aRepr = TimespanReprClass.from_columns(aTable.columns)
        self.assertEqual(
            [row[TimespanReprClass.NAME] is None for row in aRows],
            [
                row.f
                for row in self.query_list(
                    db, sqlalchemy.sql.select(aRepr.isNull().label("f")).order_by(aTable.columns.id)
                )
            ],
        )
        bRepr = TimespanReprClass.from_columns(bTable.columns)
        self.assertEqual(
            [False for row in bRows],
            [
                row.f
                for row in self.query_list(
                    db, sqlalchemy.sql.select(bRepr.isNull().label("f")).order_by(bTable.columns.id)
                )
            ],
        )
        # Test relationships expressions that relate in-database timespans to
        # Python-literal timespans, all from the more complete 'a' set; check
        # that this is consistent with Python-only relationship tests.
        for rhsRow in aRows:
            if rhsRow[TimespanReprClass.NAME] is None:
                continue
            with self.subTest(rhsRow=rhsRow):
                expected = {}
                for lhsRow in aRows:
                    if lhsRow[TimespanReprClass.NAME] is None:
                        expected[lhsRow["id"]] = (None, None, None, None)
                    else:
                        expected[lhsRow["id"]] = (
                            lhsRow[TimespanReprClass.NAME].overlaps(rhsRow[TimespanReprClass.NAME]),
                            lhsRow[TimespanReprClass.NAME].contains(rhsRow[TimespanReprClass.NAME]),
                            lhsRow[TimespanReprClass.NAME] < rhsRow[TimespanReprClass.NAME],
                            lhsRow[TimespanReprClass.NAME] > rhsRow[TimespanReprClass.NAME],
                        )
                rhsRepr = TimespanReprClass.fromLiteral(rhsRow[TimespanReprClass.NAME])
                sql = sqlalchemy.sql.select(
                    aTable.columns.id.label("lhs"),
                    aRepr.overlaps(rhsRepr).label("overlaps"),
                    aRepr.contains(rhsRepr).label("contains"),
                    (aRepr < rhsRepr).label("less_than"),
                    (aRepr > rhsRepr).label("greater_than"),
                ).select_from(aTable)
                queried = {
                    row.lhs: (row.overlaps, row.contains, row.less_than, row.greater_than)
                    for row in self.query_list(db, sql)
                }
                self.assertEqual(expected, queried)
        # Test relationship expressions that relate in-database timespans to
        # each other, all from the more complete 'a' set; check that this is
        # consistent with Python-only relationship tests.
        expected = {}
        for lhs, rhs in itertools.product(aRows, aRows):
            lhsT = lhs[TimespanReprClass.NAME]
            rhsT = rhs[TimespanReprClass.NAME]
            if lhsT is not None and rhsT is not None:
                expected[lhs["id"], rhs["id"]] = (
                    lhsT.overlaps(rhsT),
                    lhsT.contains(rhsT),
                    lhsT < rhsT,
                    lhsT > rhsT,
                )
            else:
                expected[lhs["id"], rhs["id"]] = (None, None, None, None)
        lhsSubquery = aTable.alias("lhs")
        rhsSubquery = aTable.alias("rhs")
        lhsRepr = TimespanReprClass.from_columns(lhsSubquery.columns)
        rhsRepr = TimespanReprClass.from_columns(rhsSubquery.columns)
        sql = sqlalchemy.sql.select(
            lhsSubquery.columns.id.label("lhs"),
            rhsSubquery.columns.id.label("rhs"),
            lhsRepr.overlaps(rhsRepr).label("overlaps"),
            lhsRepr.contains(rhsRepr).label("contains"),
            (lhsRepr < rhsRepr).label("less_than"),
            (lhsRepr > rhsRepr).label("greater_than"),
        ).select_from(lhsSubquery.join(rhsSubquery, onclause=sqlalchemy.sql.literal(True)))
        queried = {
            (row.lhs, row.rhs): (row.overlaps, row.contains, row.less_than, row.greater_than)
            for row in self.query_list(db, sql)
        }
        self.assertEqual(expected, queried)
        # Test relationship expressions between in-database timespans and
        # Python-literal instantaneous times.
        for t in timestamps:
            with self.subTest(t=t):
                expected = {}
                for lhsRow in aRows:
                    if lhsRow[TimespanReprClass.NAME] is None:
                        expected[lhsRow["id"]] = (None, None, None, None)
                    else:
                        expected[lhsRow["id"]] = (
                            lhsRow[TimespanReprClass.NAME].contains(t),
                            lhsRow[TimespanReprClass.NAME].overlaps(t),
                            lhsRow[TimespanReprClass.NAME] < t,
                            lhsRow[TimespanReprClass.NAME] > t,
                        )
                rhs = sqlalchemy.sql.literal(t, type_=ddl.AstropyTimeNsecTai)
                sql = sqlalchemy.sql.select(
                    aTable.columns.id.label("lhs"),
                    aRepr.contains(rhs).label("contains"),
                    aRepr.overlaps(rhs).label("overlaps_point"),
                    (aRepr < rhs).label("less_than"),
                    (aRepr > rhs).label("greater_than"),
                ).select_from(aTable)
                queried = {
                    row.lhs: (row.contains, row.overlaps_point, row.less_than, row.greater_than)
                    for row in self.query_list(db, sql)
                }
                self.assertEqual(expected, queried)

    def testConstantRows(self):
        """Test Database.constant_rows."""
        new_db = self.makeEmptyDatabase()
        with new_db.declareStaticTables(create=True) as context:
            static = context.addTableTuple(STATIC_TABLE_SPECS)
        b_ids = new_db.insert(
            static.b,
            {"name": "b1", "value": 11},
            {"name": "b2", "value": 12},
            {"name": "b3", "value": 13},
            returnIds=True,
        )
        values_spec = ddl.TableSpec(
            [
                ddl.FieldSpec(name="b", dtype=sqlalchemy.BigInteger),
                ddl.FieldSpec(name="s", dtype=sqlalchemy.String(8)),
                ddl.FieldSpec(name="r", dtype=ddl.Base64Region()),
            ],
        )
        values_data = [
            {"b": b_ids[0], "s": "b1", "r": None},
            {"b": b_ids[2], "s": "b3", "r": Circle.empty()},
        ]
        values = new_db.constant_rows(values_spec.fields, *values_data)
        select_values_alone = sqlalchemy.sql.select(
            values.columns["b"], values.columns["s"], values.columns["r"]
        )
        self.assertCountEqual(
            [row._mapping for row in self.query_list(new_db, select_values_alone)],
            values_data,
        )
        select_values_joined = sqlalchemy.sql.select(
            values.columns["s"].label("name"), static.b.columns["value"].label("value")
        ).select_from(values.join(static.b, onclause=static.b.columns["id"] == values.columns["b"]))
        self.assertCountEqual(
            [row._mapping for row in self.query_list(new_db, select_values_joined)],
            [{"value": 11, "name": "b1"}, {"value": 13, "name": "b3"}],
        )

    def test_aggregate(self) -> None:
        """Test Database.apply_any_aggregate, ddl.Base64Region.union_aggregate,
        and TimespanDatabaseRepresetnation.apply_any_aggregate.
        """
        db = self.makeEmptyDatabase()
        with db.declareStaticTables(create=True) as context:
            t = context.addTable(
                "t",
                ddl.TableSpec(
                    [
                        ddl.FieldSpec("id", sqlalchemy.BigInteger(), nullable=False),
                        ddl.FieldSpec("name", sqlalchemy.String(16), nullable=False),
                        ddl.FieldSpec.for_region(),
                    ]
                    + list(db.getTimespanRepresentation().makeFieldSpecs(nullable=True)),
                ),
            )
        pixelization = Mq3cPixelization(10)
        start = astropy.time.Time("2020-01-01T00:00:00", format="isot", scale="tai")
        offset = astropy.time.TimeDelta(60, format="sec")
        timespans = [Timespan(begin=start + offset * n, end=start + offset * (n + 1)) for n in range(3)]
        ts_cls = db.getTimespanRepresentation()
        ts_col = ts_cls.from_columns(t.columns)
        db.insert(
            t,
            ts_cls.update(timespans[0], result={"id": 1, "name": "a", "region": pixelization.quad(12058870)}),
            ts_cls.update(timespans[1], result={"id": 2, "name": "a", "region": pixelization.quad(12058871)}),
            ts_cls.update(timespans[2], result={"id": 3, "name": "b", "region": pixelization.quad(12058872)}),
            ts_cls.update(timespans[2], result={"id": 3, "name": "b", "region": pixelization.quad(12058873)}),
        )
        # This should use DISTINCT ON in PostgreSQL and GROUP BY in SQLite.
        if db.has_distinct_on:
            sql = (
                sqlalchemy.select(
                    t.c.id.label("i"),
                    t.c.name.label("n"),
                    *ts_col.flatten("t"),
                )
                .select_from(t)
                .distinct(t.c.id)
            )
        elif db.has_any_aggregate:
            sql = (
                sqlalchemy.select(
                    t.c.id.label("i"),
                    db.apply_any_aggregate(t.c.name).label("n"),
                    *ts_col.apply_any_aggregate(db.apply_any_aggregate).flatten("t"),
                )
                .select_from(t)
                .group_by(t.c.id)
            )
        else:
            raise AssertionError(
                "PostgreSQL should support DISTINCT ON and SQLite should support no-op any aggregates."
            )
        self.assertCountEqual(
            [(row.i, row.n, ts_cls.extract(row._mapping, "t")) for row in self.query_list(db, sql)],
            [(1, "a", timespans[0]), (2, "a", timespans[1]), (3, "b", timespans[2])],
        )
        # Test union_aggregate in all versions of both database, with a GROUP
        # BY that does not need apply_any_aggregate.
        self.assertCountEqual(
            [
                (row.i, row.r.encode())
                for row in self.query_list(
                    db,
                    sqlalchemy.select(
                        t.c.id.label("i"), ddl.Base64Region.union_aggregate(t.c.region).label("r")
                    )
                    .select_from(t)
                    .group_by(t.c.id),
                )
            ],
            [
                (1, pixelization.quad(12058870).encode()),
                (2, pixelization.quad(12058871).encode()),
                (3, UnionRegion(pixelization.quad(12058872), pixelization.quad(12058873)).encode()),
            ],
        )
        if db.has_any_aggregate:
            # This should use run in SQLite and PostgreSQL 16+.
            self.assertCountEqual(
                [
                    (row.i, row.n, row.r.encode())
                    for row in self.query_list(
                        db,
                        sqlalchemy.select(
                            t.c.id.label("i"),
                            db.apply_any_aggregate(t.c.name).label("n"),
                            ddl.Base64Region.union_aggregate(t.c.region).label("r"),
                        )
                        .select_from(t)
                        .group_by(t.c.id),
                    )
                ],
                [
                    (1, "a", pixelization.quad(12058870).encode()),
                    (2, "a", pixelization.quad(12058871).encode()),
                    (3, "b", UnionRegion(pixelization.quad(12058872), pixelization.quad(12058873)).encode()),
                ],
            )
