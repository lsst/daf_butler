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
import itertools
import os
import os.path
import shutil
import stat
import tempfile
import unittest

import sqlalchemy

from lsst.sphgeom import ConvexPolygon, UnitVector3d

from lsst.daf.butler import ddl
from lsst.daf.butler.registry.databases.sqlite import SqliteDatabase
from lsst.daf.butler.registry.tests import DatabaseTests, RegistryTests
from lsst.daf.butler.registry import Registry

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


class SqliteFileRegistryTests(RegistryTests):
    """Tests for `Registry` backed by a SQLite file-based database.

    Note
    ----
    This is not a subclass of `unittest.TestCase` but to avoid repetition it
    defines methods that override `unittest.TestCase` methods. To make this
    work sublasses have to have this class first in the bases list.
    """

    def setUp(self):
        self.root = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    @classmethod
    def getDataDir(cls) -> str:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "registry"))

    def makeRegistry(self) -> Registry:
        _, filename = tempfile.mkstemp(dir=self.root, suffix=".sqlite3")
        config = self.makeRegistryConfig()
        config["db"] = f"sqlite:///{filename}"
        return Registry.fromConfig(config, create=True, butlerRoot=self.root)


class SqliteFileRegistryNameKeyCollMgrTestCase(SqliteFileRegistryTests, unittest.TestCase):
    """Tests for `Registry` backed by a SQLite file-based database.

    This test case uses NameKeyCollectionManager.
    """
    collectionsManager = "lsst.daf.butler.registry.collections.nameKey.NameKeyCollectionManager"


class SqliteFileRegistrySynthIntKeyCollMgrTestCase(SqliteFileRegistryTests, unittest.TestCase):
    """Tests for `Registry` backed by a SQLite file-based database.

    This test case uses SynthIntKeyCollectionManager.
    """
    collectionsManager = "lsst.daf.butler.registry.collections.synthIntKey.SynthIntKeyCollectionManager"


class SqliteMemoryRegistryTests(RegistryTests):
    """Tests for `Registry` backed by a SQLite in-memory database.
    """

    @classmethod
    def getDataDir(cls) -> str:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "registry"))

    def makeRegistry(self) -> Registry:
        config = self.makeRegistryConfig()
        config["db"] = "sqlite://"
        return Registry.fromConfig(config, create=True)

    def testRegions(self):
        """Tests for using region fields in `Registry` dimensions.
        """
        # TODO: the test regions used here are enormous (significant fractions
        # of the sphere), and that makes this test prohibitively slow on
        # most real databases.  These should be made more realistic, and the
        # test moved to daf/butler/registry/tests/registry.py.
        registry = self.makeRegistry()
        regionTract = ConvexPolygon((UnitVector3d(1, 0, 0),
                                     UnitVector3d(0, 1, 0),
                                     UnitVector3d(0, 0, 1)))
        regionPatch = ConvexPolygon((UnitVector3d(1, 1, 0),
                                     UnitVector3d(0, 1, 0),
                                     UnitVector3d(0, 0, 1)))
        regionVisit = ConvexPolygon((UnitVector3d(1, 0, 0),
                                     UnitVector3d(0, 1, 1),
                                     UnitVector3d(0, 0, 1)))
        regionVisitDetector = ConvexPolygon((UnitVector3d(1, 0, 0),
                                             UnitVector3d(0, 1, 0),
                                             UnitVector3d(0, 1, 1)))
        for a, b in itertools.combinations((regionTract, regionPatch, regionVisit, regionVisitDetector), 2):
            self.assertNotEqual(a, b)

        # This depends on current dimensions.yaml definitions
        self.assertEqual(len(list(registry.queryDimensions(["patch", "htm7"]))), 0)

        # Add some dimension entries
        registry.insertDimensionData("instrument", {"name": "DummyCam"})
        registry.insertDimensionData("physical_filter",
                                     {"instrument": "DummyCam", "name": "dummy_r", "abstract_filter": "r"},
                                     {"instrument": "DummyCam", "name": "dummy_i", "abstract_filter": "i"})
        for detector in (1, 2, 3, 4, 5):
            registry.insertDimensionData("detector", {"instrument": "DummyCam", "id": detector,
                                                      "full_name": str(detector)})
        registry.insertDimensionData("visit",
                                     {"instrument": "DummyCam", "id": 0, "name": "zero",
                                      "physical_filter": "dummy_r", "region": regionVisit},
                                     {"instrument": "DummyCam", "id": 1, "name": "one",
                                      "physical_filter": "dummy_i"})
        registry.insertDimensionData("skymap", {"skymap": "DummySkyMap", "hash": bytes()})
        registry.insertDimensionData("tract", {"skymap": "DummySkyMap", "tract": 0, "region": regionTract})
        registry.insertDimensionData("patch",
                                     {"skymap": "DummySkyMap", "tract": 0, "patch": 0,
                                      "cell_x": 0, "cell_y": 0, "region": regionPatch})
        registry.insertDimensionData("visit_detector_region",
                                     {"instrument": "DummyCam", "visit": 0, "detector": 2,
                                      "region": regionVisitDetector})

        def getRegion(dataId):
            return registry.expandDataId(dataId).region

        # Get region for a tract
        self.assertEqual(regionTract, getRegion({"skymap": "DummySkyMap", "tract": 0}))
        # Attempt to get region for a non-existent tract
        with self.assertRaises(LookupError):
            getRegion({"skymap": "DummySkyMap", "tract": 1})
        # Get region for a (tract, patch) combination
        self.assertEqual(regionPatch, getRegion({"skymap": "DummySkyMap", "tract": 0, "patch": 0}))
        # Get region for a non-existent (tract, patch) combination
        with self.assertRaises(LookupError):
            getRegion({"skymap": "DummySkyMap", "tract": 0, "patch": 1})
        # Get region for a visit
        self.assertEqual(regionVisit, getRegion({"instrument": "DummyCam", "visit": 0}))
        # Attempt to get region for a non-existent visit
        with self.assertRaises(LookupError):
            getRegion({"instrument": "DummyCam", "visit": 10})
        # Get region for a (visit, detector) combination
        self.assertEqual(regionVisitDetector,
                         getRegion({"instrument": "DummyCam", "visit": 0, "detector": 2}))
        # Attempt to get region for a non-existent (visit, detector)
        # combination.  This returns None rather than raising because we don't
        # want to require the region record to be present.
        self.assertIsNone(getRegion({"instrument": "DummyCam", "visit": 0, "detector": 3}))
        # getRegion for a dataId containing no spatial dimensions should
        # return None
        self.assertIsNone(getRegion({"instrument": "DummyCam"}))
        # Expanding a data ID with a mix of spatial dimensions should not fail,
        # but it may not be implemented, so we don't test that we can get the
        # region.
        registry.expandDataId({"instrument": "DummyCam", "visit": 0, "detector": 2,
                               "skymap": "DummySkyMap", "tract": 0})
        # Check if we can get the region for a skypix
        self.assertIsInstance(getRegion({"htm9": 1000}), ConvexPolygon)
        # patch_htm7_overlap should not be empty
        self.assertNotEqual(len(list(registry.queryDimensions(["patch", "htm7"]))), 0)


class SqliteMemoryRegistryNameKeyCollMgrTestCase(unittest.TestCase, SqliteMemoryRegistryTests):
    """Tests for `Registry` backed by a SQLite in-memory database.

    This test case uses NameKeyCollectionManager.
    """
    collectionsManager = "lsst.daf.butler.registry.collections.nameKey.NameKeyCollectionManager"


class SqliteMemoryRegistrySynthIntKeyCollMgrTestCase(unittest.TestCase, SqliteMemoryRegistryTests):
    """Tests for `Registry` backed by a SQLite in-memory database.

    This test case uses SynthIntKeyCollectionManager.
    """
    collectionsManager = "lsst.daf.butler.registry.collections.synthIntKey.SynthIntKeyCollectionManager"


if __name__ == "__main__":
    unittest.main()
