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

from lsst.sphgeom import ConvexPolygon, UnitVector3d
from ..interfaces import (
    Database,
    ReadOnlyDatabaseError,
    DatabaseConflictError,
)
from ...core import ddl

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

TEMPORARY_TABLE_SPEC = ddl.TableSpec(
    fields=[
        ddl.FieldSpec("a_name", dtype=sqlalchemy.String, length=16, primaryKey=True),
        ddl.FieldSpec("b_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
    ],
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
        with self.assertRaises(DatabaseConflictError):
            newDatabase.ensureTableExists(
                "d",
                ddl.TableSpec(
                    fields=[ddl.FieldSpec("name", dtype=sqlalchemy.String, length=4, primaryKey=True)]
                )
            )
        # Calling ensureTableExists inside a transaction block is an error,
        # even if it would do nothing.
        with newDatabase.transaction():
            with self.assertRaises(AssertionError):
                newDatabase.ensureTableExists("d", DYNAMIC_TABLE_SPEC)

    def testTemporaryTables(self):
        """Tests for `Database.makeTemporaryTable`,
        `Database.dropTemporaryTable`, and `Database.insert` with
        the ``select`` argument.
        """
        # Need to start with the static schema; also insert some test data.
        newDatabase = self.makeEmptyDatabase()
        with newDatabase.declareStaticTables(create=True) as context:
            static = context.addTableTuple(STATIC_TABLE_SPECS)
        newDatabase.insert(static.a,
                           {"name": "a1", "region": None},
                           {"name": "a2", "region": None})
        bIds = newDatabase.insert(static.b,
                                  {"name": "b1", "value": 11},
                                  {"name": "b2", "value": 12},
                                  {"name": "b3", "value": 13},
                                  returnIds=True)
        # Create the table.
        table1 = newDatabase.makeTemporaryTable(TEMPORARY_TABLE_SPEC, "e1")
        self.checkTable(TEMPORARY_TABLE_SPEC, table1)
        # Insert via a INSERT INTO ... SELECT query.
        newDatabase.insert(
            table1,
            select=sqlalchemy.sql.select(
                [static.a.columns.name.label("a_name"), static.b.columns.id.label("b_id")]
            ).select_from(
                static.a.join(static.b, onclause=sqlalchemy.sql.literal(True))
            ).where(
                sqlalchemy.sql.and_(
                    static.a.columns.name == "a1",
                    static.b.columns.value <= 12,
                )
            )
        )
        # Check that the inserted rows are present.
        self.assertCountEqual(
            [{"a_name": "a1", "b_id": bId} for bId in bIds[:2]],
            [dict(row) for row in newDatabase.query(table1.select())]
        )
        # Create another one via a read-only connection to the database.
        # We _do_ allow temporary table modifications in read-only databases.
        with self.asReadOnly(newDatabase) as existingReadOnlyDatabase:
            with existingReadOnlyDatabase.declareStaticTables(create=False) as context:
                context.addTableTuple(STATIC_TABLE_SPECS)
            table2 = existingReadOnlyDatabase.makeTemporaryTable(TEMPORARY_TABLE_SPEC)
            self.checkTable(TEMPORARY_TABLE_SPEC, table2)
            # Those tables should not be the same, despite having the same ddl.
            self.assertIsNot(table1, table2)
            # Do a slightly different insert into this table, to check that
            # it works in a read-only database.  This time we pass column
            # names as a kwarg to insert instead of by labeling the columns in
            # the select.
            existingReadOnlyDatabase.insert(
                table2,
                select=sqlalchemy.sql.select(
                    [static.a.columns.name, static.b.columns.id]
                ).select_from(
                    static.a.join(static.b, onclause=sqlalchemy.sql.literal(True))
                ).where(
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
                [dict(row) for row in existingReadOnlyDatabase.query(table2.select())]
            )
            # Drop the temporary table from the read-only DB.  It's unspecified
            # whether attempting to use it after this point is an error or just
            # never returns any results, so we can't test what it does, only
            # that it's not an error.
            existingReadOnlyDatabase.dropTemporaryTable(table2)
        # Drop the original temporary table.
        newDatabase.dropTemporaryTable(table1)

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
        self.assertEqual([dict(r) for r in db.query(tables.a.select()).fetchall()], [row])
        # Insert multiple autoincrement rows but do not try to get the IDs
        # back immediately.
        db.insert(tables.b, {"name": "b1", "value": 10}, {"name": "b2", "value": 20})
        results = [dict(r) for r in db.query(tables.b.select().order_by("id")).fetchall()]
        self.assertEqual(len(results), 2)
        for row in results:
            self.assertIn(row["name"], ("b1", "b2"))
            self.assertIsInstance(row["id"], int)
        self.assertGreater(results[1]["id"], results[0]["id"])
        # Insert multiple autoincrement rows and get the IDs back from insert.
        rows = [{"name": "b3", "value": 30}, {"name": "b4", "value": 40}]
        ids = db.insert(tables.b, *rows, returnIds=True)
        results = [
            dict(r) for r in db.query(
                tables.b.select().where(tables.b.columns.id > results[1]["id"])
            ).fetchall()
        ]
        expected = [dict(row, id=id) for row, id in zip(rows, ids)]
        self.assertCountEqual(results, expected)
        self.assertTrue(all(result["id"] is not None for result in results))
        # Insert multiple rows into a table with an autoincrement+origin
        # primary key, then use the returned IDs to insert into a dynamic
        # table.
        rows = [{"origin": db.origin, "b_id": results[0]["id"]},
                {"origin": db.origin, "b_id": None}]
        ids = db.insert(tables.c, *rows, returnIds=True)
        results = [dict(r) for r in db.query(tables.c.select()).fetchall()]
        expected = [dict(row, id=id) for row, id in zip(rows, ids)]
        self.assertCountEqual(results, expected)
        self.assertTrue(all(result["id"] is not None for result in results))
        # Add the dynamic table.
        d = db.ensureTableExists("d", DYNAMIC_TABLE_SPEC)
        # Insert into it.
        rows = [{"c_origin": db.origin, "c_id": id, "a_name": "a1"} for id in ids]
        db.insert(d, *rows)
        results = [dict(r) for r in db.query(d.select()).fetchall()]
        self.assertCountEqual(rows, results)
        # Insert multiple rows into a table with an autoincrement+origin
        # primary key (this is especially tricky for SQLite, but good to test
        # for all DBs), but pass in a value for the autoincrement key.
        # For extra complexity, we re-use the autoincrement value with a
        # different value for origin.
        rows2 = [{"id": 700, "origin": db.origin, "b_id": None},
                 {"id": 700, "origin": 60, "b_id": None},
                 {"id": 1, "origin": 60, "b_id": None}]
        db.insert(tables.c, *rows2)
        results = [dict(r) for r in db.query(tables.c.select()).fetchall()]
        self.assertCountEqual(results, expected + rows2)
        self.assertTrue(all(result["id"] is not None for result in results))

        # Define 'SELECT COUNT(*)' query for later use.
        count = sqlalchemy.sql.select([sqlalchemy.sql.func.count()])
        # Get the values we inserted into table b.
        bValues = [dict(r) for r in db.query(tables.b.select()).fetchall()]
        # Remove two row from table b by ID.
        n = db.delete(tables.b, ["id"], {"id": bValues[0]["id"]}, {"id": bValues[1]["id"]})
        self.assertEqual(n, 2)
        # Remove the other two rows from table b by name.
        n = db.delete(tables.b, ["name"], {"name": bValues[2]["name"]}, {"name": bValues[3]["name"]})
        self.assertEqual(n, 2)
        # There should now be no rows in table b.
        self.assertEqual(
            db.query(count.select_from(tables.b)).scalar(),
            0
        )
        # All b_id values in table c should now be NULL, because there's an
        # onDelete='SET NULL' foreign key.
        self.assertEqual(
            db.query(count.select_from(tables.c).where(tables.c.columns.b_id != None)).scalar(),  # noqa:E711
            0
        )
        # Remove all rows in table a (there's only one); this should remove all
        # rows in d due to onDelete='CASCADE'.
        n = db.delete(tables.a, [])
        self.assertEqual(n, 1)
        self.assertEqual(db.query(count.select_from(tables.a)).scalar(), 0)
        self.assertEqual(db.query(count.select_from(d)).scalar(), 0)

    def testUpdate(self):
        """Tests for `Database.update`.
        """
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        # Insert two rows into table a, both without regions.
        db.insert(tables.a, {"name": "a1"}, {"name": "a2"})
        # Update one of the rows with a region.
        region = ConvexPolygon((UnitVector3d(1, 0, 0), UnitVector3d(0, 1, 0), UnitVector3d(0, 0, 1)))
        n = db.update(tables.a, {"name": "k"}, {"k": "a2", "region": region})
        self.assertEqual(n, 1)
        sql = sqlalchemy.sql.select([tables.a.columns.name, tables.a.columns.region]).select_from(tables.a)
        self.assertCountEqual(
            [dict(r) for r in db.query(sql).fetchall()],
            [{"name": "a1", "region": None}, {"name": "a2", "region": region}]
        )

    def testSync(self):
        """Tests for `Database.sync`.
        """
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        # Insert a row with sync, because it doesn't exist yet.
        values, inserted = db.sync(tables.b, keys={"name": "b1"}, extra={"value": 10}, returning=["id"])
        self.assertTrue(inserted)
        self.assertEqual([{"id": values["id"], "name": "b1", "value": 10}],
                         [dict(r) for r in db.query(tables.b.select()).fetchall()])
        # Repeat that operation, which should do nothing but return the
        # requested values.
        values, inserted = db.sync(tables.b, keys={"name": "b1"}, extra={"value": 10}, returning=["id"])
        self.assertFalse(inserted)
        self.assertEqual([{"id": values["id"], "name": "b1", "value": 10}],
                         [dict(r) for r in db.query(tables.b.select()).fetchall()])
        # Repeat the operation without the 'extra' arg, which should also just
        # return the existing row.
        values, inserted = db.sync(tables.b, keys={"name": "b1"}, returning=["id"])
        self.assertFalse(inserted)
        self.assertEqual([{"id": values["id"], "name": "b1", "value": 10}],
                         [dict(r) for r in db.query(tables.b.select()).fetchall()])
        # Repeat the operation with a different value in 'extra'.  That still
        # shouldn't be an error, because 'extra' is only used if we really do
        # insert.  Also drop the 'returning' argument.
        _, inserted = db.sync(tables.b, keys={"name": "b1"}, extra={"value": 20})
        self.assertFalse(inserted)
        self.assertEqual([{"id": values["id"], "name": "b1", "value": 10}],
                         [dict(r) for r in db.query(tables.b.select()).fetchall()])
        # Repeat the operation with the correct value in 'compared' instead of
        # 'extra'.
        _, inserted = db.sync(tables.b, keys={"name": "b1"}, compared={"value": 10})
        self.assertFalse(inserted)
        self.assertEqual([{"id": values["id"], "name": "b1", "value": 10}],
                         [dict(r) for r in db.query(tables.b.select()).fetchall()])
        # Repeat the operation with an incorrect value in 'compared'; this
        # should raise.
        with self.assertRaises(DatabaseConflictError):
            db.sync(tables.b, keys={"name": "b1"}, compared={"value": 20})
        # Try to sync inside a transaction.  That's always an error, regardless
        # of whether there would be an insertion or not.
        with self.assertRaises(AssertionError):
            with db.transaction():
                db.sync(tables.b, keys={"name": "b1"}, extra={"value": 10})
        with self.assertRaises(AssertionError):
            with db.transaction():
                db.sync(tables.b, keys={"name": "b2"}, extra={"value": 20})
        # Try to sync in a read-only database.  This should work if and only
        # if the matching row already exists.
        with self.asReadOnly(db) as rodb:
            with rodb.declareStaticTables(create=False) as context:
                tables = context.addTableTuple(STATIC_TABLE_SPECS)
            _, inserted = rodb.sync(tables.b, keys={"name": "b1"})
            self.assertFalse(inserted)
            self.assertEqual([{"id": values["id"], "name": "b1", "value": 10}],
                             [dict(r) for r in rodb.query(tables.b.select()).fetchall()])
            with self.assertRaises(ReadOnlyDatabaseError):
                rodb.sync(tables.b, keys={"name": "b2"}, extra={"value": 20})

    def testReplace(self):
        """Tests for `Database.replace`.
        """
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            tables = context.addTableTuple(STATIC_TABLE_SPECS)
        # Use 'replace' to insert a single row that contains a region and
        # query to get it back.
        region = ConvexPolygon((UnitVector3d(1, 0, 0), UnitVector3d(0, 1, 0), UnitVector3d(0, 0, 1)))
        row1 = {"name": "a1", "region": region}
        db.replace(tables.a, row1)
        self.assertEqual([dict(r) for r in db.query(tables.a.select()).fetchall()], [row1])
        # Insert another row without a region.
        row2 = {"name": "a2", "region": None}
        db.replace(tables.a, row2)
        self.assertCountEqual([dict(r) for r in db.query(tables.a.select()).fetchall()], [row1, row2])
        # Use replace to re-insert both of those rows again, which should do
        # nothing.
        db.replace(tables.a, row1, row2)
        self.assertCountEqual([dict(r) for r in db.query(tables.a.select()).fetchall()], [row1, row2])
        # Replace row1 with a row with no region, while reinserting row2.
        row1a = {"name": "a1", "region": None}
        db.replace(tables.a, row1a, row2)
        self.assertCountEqual([dict(r) for r in db.query(tables.a.select()).fetchall()], [row1a, row2])
        # Replace both rows, returning row1 to its original state, while adding
        # a new one.  Pass them in in a different order.
        row2a = {"name": "a2", "region": region}
        row3 = {"name": "a3", "region": None}
        db.replace(tables.a, row3, row2a, row1)
        self.assertCountEqual([dict(r) for r in db.query(tables.a.select()).fetchall()], [row1, row2a, row3])
