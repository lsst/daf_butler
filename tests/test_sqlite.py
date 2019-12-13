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

from contextlib import contextmanager
import os
import os.path
import shutil
import stat
import tempfile
import unittest

import sqlalchemy

from lsst.daf.butler.registry.databases.sqlite import SqliteDatabase
from lsst.daf.butler.registry.tests import DatabaseTests
from lsst.daf.butler.registry import ddl

TESTDIR = os.path.abspath(os.path.dirname(__file__))


@contextmanager
def removeWritePermission(filename):
    mode = os.stat(filename).st_mode
    try:
        os.chmod(filename, stat.S_IREAD)
        yield
    finally:
        os.chmod(filename, mode)


class SqliteDatabaseTestCase(unittest.TestCase, DatabaseTests):

    def setUp(self):
        self.root = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def makeEmptyDatabase(self, origin: int = 0) -> SqliteDatabase:
        _, filename = tempfile.mkstemp(dir=self.root, suffix=".sqlite3")
        cs = SqliteDatabase.connect(filename=filename)
        return SqliteDatabase.fromConnectionStruct(cs=cs, origin=origin)

    def getNewConnection(self, database: SqliteDatabase, *, writeable: bool) -> SqliteDatabase:
        cs = SqliteDatabase.connect(filename=database.filename, writeable=writeable)
        return SqliteDatabase.fromConnectionStruct(origin=database.origin, cs=cs, writeable=writeable)

    @contextmanager
    def asReadOnly(self, database: SqliteDatabase) -> SqliteDatabase:
        with removeWritePermission(database.filename):
            yield self.getNewConnection(database, writeable=False)

    def isEmptyDatabaseActuallyWriteable(self, database: SqliteDatabase) -> bool:
        """Check whether we really can modify a database.

        This intentionally allows any exception to be raised (not just
        `ReadOnlyDatabaseError`) to deal with cases where the file is read-only
        but the Database was initialized (incorrectly) with writeable=True.
        """
        try:
            with database.declareStaticTables(create=True) as context:
                context.addTable(
                    "a",
                    ddl.TableSpec(fields=[ddl.FieldSpec("b", dtype=sqlalchemy.Integer, primaryKey=True)])
                )
            return True
        except Exception:
            return False

    def testConnection(self):
        """Test that different ways of connecting to a SQLite database
        are equivalent.
        """
        # Create an in-memory database by passing filename=None.
        memFilename = SqliteDatabase.fromConnectionStruct(SqliteDatabase.connect(filename=None), origin=0)
        self.assertIsNone(memFilename.filename)
        self.assertEqual(memFilename.origin, 0)
        self.assertTrue(memFilename.isWriteable())
        self.assertTrue(self.isEmptyDatabaseActuallyWriteable(memFilename))
        # Create an in-memory database via a URI.
        memUri = SqliteDatabase.fromUri("sqlite://", origin=0)
        self.assertIsNone(memUri.filename)
        self.assertEqual(memUri.origin, 0)
        self.assertTrue(memUri.isWriteable())
        self.assertTrue(self.isEmptyDatabaseActuallyWriteable(memUri))
        # We don't support SQLite URIs inside SQLAlchemy URIs.
        with self.assertRaises(NotImplementedError):
            SqliteDatabase.connect(uri="sqlite:///:memory:?uri=true")
        # We don't support read-only in-memory databases.
        with self.assertRaises(NotImplementedError):
            SqliteDatabase.connect(filename=None, writeable=False)

        # Make a temporary file for the rest of the next set of tests.
        _, filename = tempfile.mkstemp(dir=self.root, suffix=".sqlite3")
        # Create a read-write database by passing in the filename.
        rwFilename = SqliteDatabase.fromConnectionStruct(SqliteDatabase.connect(filename=filename), origin=0)
        self.assertEqual(rwFilename.filename, filename)
        self.assertEqual(rwFilename.origin, 0)
        self.assertTrue(rwFilename.isWriteable())
        self.assertTrue(self.isEmptyDatabaseActuallyWriteable(rwFilename))
        # Create a read-write database via a URI.
        rwUri = SqliteDatabase.fromUri(f"sqlite:///{filename}", origin=0)
        self.assertEqual(rwUri.filename, filename)
        self.assertEqual(rwUri.origin, 0)
        self.assertTrue(rwUri.isWriteable())
        self.assertTrue(self.isEmptyDatabaseActuallyWriteable(rwUri))
        # We don't support SQLite URIs inside SQLAlchemy URIs.
        with self.assertRaises(NotImplementedError):
            SqliteDatabase.connect(uri=f"sqlite:///file:{filename}?uri=true")

        # Test read-only connections against a read-only file.
        with removeWritePermission(filename):
            # Create a read-only database by passing in the filename.
            roFilename = SqliteDatabase.fromConnectionStruct(SqliteDatabase.connect(filename=filename),
                                                             origin=0, writeable=False)
            self.assertEqual(roFilename.filename, filename)
            self.assertEqual(roFilename.origin, 0)
            self.assertFalse(roFilename.isWriteable())
            self.assertFalse(self.isEmptyDatabaseActuallyWriteable(roFilename))
            # Create a read-write database via a URI.
            roUri = SqliteDatabase.fromUri(f"sqlite:///{filename}", origin=0, writeable=False)
            self.assertEqual(roUri.filename, filename)
            self.assertEqual(roUri.origin, 0)
            self.assertFalse(roUri.isWriteable())
            self.assertFalse(self.isEmptyDatabaseActuallyWriteable(roUri))


if __name__ == "__main__":
    unittest.main()
