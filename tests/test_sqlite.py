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

import os
import os.path
import stat
import tempfile
import unittest
from contextlib import contextmanager

import sqlalchemy

from lsst.daf.butler import Butler, Config, ddl
from lsst.daf.butler.registry import RegistryConfig, _RegistryFactory
from lsst.daf.butler.registry.databases.sqlite import SqliteDatabase
from lsst.daf.butler.registry.tests import DatabaseTests, RegistryTests
from lsst.daf.butler.tests import makeTestRepo
from lsst.daf.butler.tests.utils import create_populated_sqlite_registry, makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


@contextmanager
def removeWritePermission(filename):
    """Remove the write permission on a file."""
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
            table = context.addTable(
                "a", ddl.TableSpec(fields=[ddl.FieldSpec("b", dtype=sqlalchemy.Integer, primaryKey=True)])
            )
        # Drop created table so that schema remains empty.
        database._metadata._metadata.drop_all(database._engine, tables=[table])
        return True
    except Exception:
        return False


class SqliteFileDatabaseTestCase(unittest.TestCase, DatabaseTests):
    """Tests for `SqliteDatabase` using a standard file-based database."""

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self):
        removeTestTempDir(self.root)

    def makeEmptyDatabase(self, origin: int = 0) -> SqliteDatabase:
        _, filename = tempfile.mkstemp(dir=self.root, suffix=".sqlite3")
        engine = SqliteDatabase.makeEngine(filename=filename)
        return SqliteDatabase.fromEngine(engine=engine, origin=origin)

    def getNewConnection(self, database: SqliteDatabase, *, writeable: bool) -> SqliteDatabase:
        engine = SqliteDatabase.makeEngine(filename=database.filename, writeable=writeable)
        return SqliteDatabase.fromEngine(origin=database.origin, engine=engine, writeable=writeable)

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
        rwFromFilename = SqliteDatabase.fromEngine(SqliteDatabase.makeEngine(filename=filename), origin=0)
        self.assertEqual(os.path.realpath(rwFromFilename.filename), os.path.realpath(filename))
        self.assertEqual(rwFromFilename.origin, 0)
        self.assertTrue(rwFromFilename.isWriteable())
        self.assertTrue(isEmptyDatabaseActuallyWriteable(rwFromFilename))
        # Create a read-write database via a URI.
        rwFromUri = SqliteDatabase.fromUri(f"sqlite:///{filename}", origin=0)
        self.assertEqual(os.path.realpath(rwFromUri.filename), os.path.realpath(filename))
        self.assertEqual(rwFromUri.origin, 0)
        self.assertTrue(rwFromUri.isWriteable())
        self.assertTrue(isEmptyDatabaseActuallyWriteable(rwFromUri))
        # We don't support SQLite URIs inside SQLAlchemy URIs.
        with self.assertRaises(NotImplementedError):
            SqliteDatabase.makeEngine(uri=f"sqlite:///file:{filename}?uri=true")

        # Test read-only connections against a read-only file.
        with removeWritePermission(filename):
            # Create a read-only database by passing in the filename.
            roFromFilename = SqliteDatabase.fromEngine(
                SqliteDatabase.makeEngine(filename=filename), origin=0, writeable=False
            )
            self.assertEqual(os.path.realpath(roFromFilename.filename), os.path.realpath(filename))
            self.assertEqual(roFromFilename.origin, 0)
            self.assertFalse(roFromFilename.isWriteable())
            self.assertFalse(isEmptyDatabaseActuallyWriteable(roFromFilename))
            # Create a read-write database via a URI.
            roFromUri = SqliteDatabase.fromUri(f"sqlite:///{filename}", origin=0, writeable=False)
            self.assertEqual(os.path.realpath(roFromUri.filename), os.path.realpath(filename))
            self.assertEqual(roFromUri.origin, 0)
            self.assertFalse(roFromUri.isWriteable())
            self.assertFalse(isEmptyDatabaseActuallyWriteable(roFromUri))

    def testTransactionLocking(self):
        # This (inherited) test can't run on SQLite because of our use of an
        # aggressive locking strategy there.
        pass


class SqliteMemoryDatabaseTestCase(unittest.TestCase, DatabaseTests):
    """Tests for `SqliteDatabase` using an in-memory database."""

    def makeEmptyDatabase(self, origin: int = 0) -> SqliteDatabase:
        engine = SqliteDatabase.makeEngine(filename=None)
        return SqliteDatabase.fromEngine(engine=engine, origin=origin)

    def getNewConnection(self, database: SqliteDatabase, *, writeable: bool) -> SqliteDatabase:
        return SqliteDatabase.fromEngine(origin=database.origin, engine=database._engine, writeable=writeable)

    @contextmanager
    def asReadOnly(self, database: SqliteDatabase) -> SqliteDatabase:
        yield self.getNewConnection(database, writeable=False)

    def testConnection(self):
        """Test that different ways of connecting to a SQLite database
        are equivalent.
        """
        # Create an in-memory database by passing filename=None.
        memFromFilename = SqliteDatabase.fromEngine(SqliteDatabase.makeEngine(filename=None), origin=0)
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
            SqliteDatabase.makeEngine(uri="sqlite:///:memory:?uri=true")
        # We don't support read-only in-memory databases.
        with self.assertRaises(NotImplementedError):
            SqliteDatabase.makeEngine(filename=None, writeable=False)

    def testTransactionLocking(self):
        # This (inherited) test can't run on SQLite because of our use of an
        # aggressive locking strategy there.
        pass


class SqliteFileRegistryTests(RegistryTests):
    """Tests for `Registry` backed by a SQLite file-based database.

    Notes
    -----
    This is not a subclass of `unittest.TestCase` but to avoid repetition it
    defines methods that override `unittest.TestCase` methods. To make this
    work sublasses have to have this class first in the bases list.
    """

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self):
        removeTestTempDir(self.root)

    @classmethod
    def getDataDir(cls) -> str:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "registry"))

    def make_butler(self, registry_config: RegistryConfig | None = None) -> Butler:
        config = Config()
        if registry_config is None:
            registry_config = self.makeRegistryConfig()
        config["registry"] = registry_config
        return makeTestRepo(self.root, config=config)


class SqliteFileRegistryNameKeyCollMgrUUIDTestCase(SqliteFileRegistryTests, unittest.TestCase):
    """Tests for `Registry` backed by a SQLite file-based database.

    This test case uses NameKeyCollectionManager and
    ByDimensionsDatasetRecordStorageManagerUUID.
    """

    collectionsManager = "lsst.daf.butler.registry.collections.nameKey.NameKeyCollectionManager"
    datasetsManager = (
        "lsst.daf.butler.registry.datasets.byDimensions.ByDimensionsDatasetRecordStorageManagerUUID"
    )


class ClonedSqliteFileRegistryNameKeyCollMgrUUIDTestCase(
    SqliteFileRegistryNameKeyCollMgrUUIDTestCase, unittest.TestCase
):
    """Test that NameKeyCollectionManager still works after cloning."""

    def make_butler(self, registry_config: RegistryConfig | None = None) -> Butler:
        original = super().make_butler(registry_config)
        return original.clone()


class SqliteFileRegistrySynthIntKeyCollMgrUUIDTestCase(SqliteFileRegistryTests, unittest.TestCase):
    """Tests for `Registry` backed by a SQLite file-based database.

    This test case uses SynthIntKeyCollectionManager and
    ByDimensionsDatasetRecordStorageManagerUUID.
    """

    collectionsManager = "lsst.daf.butler.registry.collections.synthIntKey.SynthIntKeyCollectionManager"
    datasetsManager = (
        "lsst.daf.butler.registry.datasets.byDimensions.ByDimensionsDatasetRecordStorageManagerUUID"
    )


class SqliteMemoryRegistryTests(RegistryTests):
    """Tests for `Registry` backed by a SQLite in-memory database."""

    @classmethod
    def getDataDir(cls) -> str:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "registry"))

    def make_butler(self, registry_config: RegistryConfig | None = None) -> Butler:
        # This helper function always return in-memory registry
        # with default managers.
        if registry_config is None:
            registry_config = self.makeRegistryConfig()
        return create_populated_sqlite_registry(registry_config=registry_config)

    def testMissingAttributes(self):
        """Test for instantiating a registry against outdated schema which
        misses butler_attributes table.
        """
        # TODO: Once we have stable gen3 schema everywhere this test can be
        # dropped (DM-27373).
        config = self.makeRegistryConfig()
        config["db"] = "sqlite://"
        with self.assertRaises(LookupError):
            _RegistryFactory(config).from_config()


class SqliteMemoryRegistryNameKeyCollMgrUUIDTestCase(unittest.TestCase, SqliteMemoryRegistryTests):
    """Tests for `Registry` backed by a SQLite in-memory database.

    This test case uses NameKeyCollectionManager and
    ByDimensionsDatasetRecordStorageManagerUUID.
    """

    collectionsManager = "lsst.daf.butler.registry.collections.nameKey.NameKeyCollectionManager"
    datasetsManager = (
        "lsst.daf.butler.registry.datasets.byDimensions.ByDimensionsDatasetRecordStorageManagerUUID"
    )


class SqliteMemoryRegistrySynthIntKeyCollMgrUUIDTestCase(unittest.TestCase, SqliteMemoryRegistryTests):
    """Tests for `Registry` backed by a SQLite in-memory database.

    This test case uses SynthIntKeyCollectionManager and
    ByDimensionsDatasetRecordStorageManagerUUID.
    """

    collectionsManager = "lsst.daf.butler.registry.collections.synthIntKey.SynthIntKeyCollectionManager"
    datasetsManager = (
        "lsst.daf.butler.registry.datasets.byDimensions.ByDimensionsDatasetRecordStorageManagerUUID"
    )


class SqliteMemoryRegistryAstropyIngestDateTestCase(unittest.TestCase, SqliteMemoryRegistryTests):
    """Tests for `Registry` backed by a SQLite in-memory database.

    This test case uses version schema with ingest_date as nanoseconds instead
    of DATETIME. This tests can be removed when/if we switch to nanoseconds as
    default.
    """

    collectionsManager = "lsst.daf.butler.registry.collections.synthIntKey.SynthIntKeyCollectionManager"
    datasetsManager = {
        "cls": "lsst.daf.butler.registry.datasets.byDimensions.ByDimensionsDatasetRecordStorageManagerUUID",
        "schema_version": "2.0.0",
    }


if __name__ == "__main__":
    unittest.main()
