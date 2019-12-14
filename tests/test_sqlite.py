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

from lsst.daf.butler.core.registryConfig import RegistryConfig
from lsst.daf.butler.registry.databases.sqlite import SqliteDatabase
from lsst.daf.butler.registry.tests import DatabaseTests, RegistryTests
from lsst.daf.butler.registry import Registry, ddl

TESTDIR = os.path.abspath(os.path.dirname(__file__))


@contextmanager
def removeWritePermission(filename):
    mode = os.stat(filename).st_mode
    try:
        os.chmod(filename, stat.S_IREAD)
        yield
    finally:
        os.chmod(filename, mode)


def isEmptyDatabaseActuallyWriteable(database: SqliteDatabase) -> bool:
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


class SqliteFileDatabaseTestCase(unittest.TestCase, DatabaseTests):
    """Tests for `SqliteDatabase` using a standard file-based database.
    """

    def setUp(self):
        self.root = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def makeEmptyDatabase(self, origin: int = 0) -> SqliteDatabase:
        _, filename = tempfile.mkstemp(dir=self.root, suffix=".sqlite3")
        connection = SqliteDatabase.connect(filename=filename)
        return SqliteDatabase.fromConnection(connection=connection, origin=origin)

    def getNewConnection(self, database: SqliteDatabase, *, writeable: bool) -> SqliteDatabase:
        connection = SqliteDatabase.connect(filename=database.filename, writeable=writeable)
        return SqliteDatabase.fromConnection(origin=database.origin, connection=connection,
                                             writeable=writeable)

    @contextmanager
    def asReadOnly(self, database: SqliteDatabase) -> SqliteDatabase:
        with removeWritePermission(database.filename):
            yield self.getNewConnection(database, writeable=False)

    def testConnection(self):
        """Test that different ways of connecting to a SQLite database
        are equivalent.
        """
        _, filename = tempfile.mkstemp(dir=self.root, suffix=".sqlite3")
        # Create a read-write database by passing in the filename.
        rwFromFilename = SqliteDatabase.fromConnection(SqliteDatabase.connect(filename=filename), origin=0)
        self.assertEqual(rwFromFilename.filename, filename)
        self.assertEqual(rwFromFilename.origin, 0)
        self.assertTrue(rwFromFilename.isWriteable())
        self.assertTrue(isEmptyDatabaseActuallyWriteable(rwFromFilename))
        # Create a read-write database via a URI.
        rwFromUri = SqliteDatabase.fromUri(f"sqlite:///{filename}", origin=0)
        self.assertEqual(rwFromUri.filename, filename)
        self.assertEqual(rwFromUri.origin, 0)
        self.assertTrue(rwFromUri.isWriteable())
        self.assertTrue(isEmptyDatabaseActuallyWriteable(rwFromUri))
        # We don't support SQLite URIs inside SQLAlchemy URIs.
        with self.assertRaises(NotImplementedError):
            SqliteDatabase.connect(uri=f"sqlite:///file:{filename}?uri=true")

        # Test read-only connections against a read-only file.
        with removeWritePermission(filename):
            # Create a read-only database by passing in the filename.
            roFromFilename = SqliteDatabase.fromConnection(SqliteDatabase.connect(filename=filename),
                                                           origin=0, writeable=False)
            self.assertEqual(roFromFilename.filename, filename)
            self.assertEqual(roFromFilename.origin, 0)
            self.assertFalse(roFromFilename.isWriteable())
            self.assertFalse(isEmptyDatabaseActuallyWriteable(roFromFilename))
            # Create a read-write database via a URI.
            roFromUri = SqliteDatabase.fromUri(f"sqlite:///{filename}", origin=0, writeable=False)
            self.assertEqual(roFromUri.filename, filename)
            self.assertEqual(roFromUri.origin, 0)
            self.assertFalse(roFromUri.isWriteable())
            self.assertFalse(isEmptyDatabaseActuallyWriteable(roFromUri))


class SqliteMemoryDatabaseTestCase(unittest.TestCase, DatabaseTests):
    """Tests for `SqliteDatabase` using an in-memory database.
    """

    def makeEmptyDatabase(self, origin: int = 0) -> SqliteDatabase:
        connection = SqliteDatabase.connect(filename=None)
        return SqliteDatabase.fromConnection(connection=connection, origin=origin)

    def getNewConnection(self, database: SqliteDatabase, *, writeable: bool) -> SqliteDatabase:
        return SqliteDatabase.fromConnection(origin=database.origin, connection=database._connection,
                                             writeable=writeable)

    @contextmanager
    def asReadOnly(self, database: SqliteDatabase) -> SqliteDatabase:
        yield self.getNewConnection(database, writeable=False)

    def testConnection(self):
        """Test that different ways of connecting to a SQLite database
        are equivalent.
        """
        # Create an in-memory database by passing filename=None.
        memFromFilename = SqliteDatabase.fromConnection(SqliteDatabase.connect(filename=None), origin=0)
        self.assertIsNone(memFromFilename.filename)
        self.assertEqual(memFromFilename.origin, 0)
        self.assertTrue(memFromFilename.isWriteable())
        self.assertTrue(isEmptyDatabaseActuallyWriteable(memFromFilename))
        # Create an in-memory database via a URI.
        memFromUri = SqliteDatabase.fromUri("sqlite://", origin=0)
        self.assertIsNone(memFromUri.filename)
        self.assertEqual(memFromUri.origin, 0)
        self.assertTrue(memFromUri.isWriteable())
        self.assertTrue(isEmptyDatabaseActuallyWriteable(memFromUri))
        # We don't support SQLite URIs inside SQLAlchemy URIs.
        with self.assertRaises(NotImplementedError):
            SqliteDatabase.connect(uri="sqlite:///:memory:?uri=true")
        # We don't support read-only in-memory databases.
        with self.assertRaises(NotImplementedError):
            SqliteDatabase.connect(filename=None, writeable=False)


class SqliteFileRegistryTestCase(unittest.TestCase, RegistryTests):
    """Tests for `Registry` backed by a SQLite file-based database.
    """

    def setUp(self):
        self.root = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def makeRegistry(self) -> Registry:
        _, filename = tempfile.mkstemp(dir=self.root, suffix=".sqlite3")
        config = RegistryConfig()
        config["db"] = f"sqlite:///{filename}"
        return Registry.fromConfig(config, create=True, butlerRoot=self.root)


class SqliteMemoryRegistryTestCase(unittest.TestCase, RegistryTests):
    """Tests for `Registry` backed by a SQLite in-memory database.
    """

    def makeRegistry(self) -> Registry:
        config = RegistryConfig()
        config["db"] = f"sqlite://"
        return Registry.fromConfig(config, create=True)


if __name__ == "__main__":
    unittest.main()
