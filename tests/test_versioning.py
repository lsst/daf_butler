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
import tempfile
import unittest

from lsst.daf.butler.registry.attributes import DefaultButlerAttributeManager
from lsst.daf.butler.registry.databases.sqlite import SqliteDatabase
from lsst.daf.butler.registry.interfaces import (
    Database,
    IncompatibleVersionError,
    VersionedExtension,
    VersionTuple,
)
from lsst.daf.butler.registry.versions import ButlerVersionsManager
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))

# Assorted version numbers used throughout the tests
V_1_0_0 = VersionTuple(major=1, minor=0, patch=0)
V_1_0_1 = VersionTuple(major=1, minor=0, patch=1)
V_1_1_0 = VersionTuple(major=1, minor=1, patch=0)
V_2_0_0 = VersionTuple(major=2, minor=0, patch=0)
V_2_0_1 = VersionTuple(major=2, minor=0, patch=1)


class Manager0(VersionedExtension):
    """Versioned extension implementation for tests."""

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        return []


class Manager1(VersionedExtension):
    """Versioned extension implementation for tests."""

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        return [V_1_0_0]


class Manager1_1(VersionedExtension):  # noqa: N801
    """Versioned extension implementation for tests."""

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        return [V_1_1_0]


class Manager2(VersionedExtension):
    """Versioned extension implementation for tests.

    This extension supports two schema versions.
    """

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        return [V_1_0_0, V_2_0_0]

    @classmethod
    def _newDefaultSchemaVersion(cls) -> VersionTuple:
        return V_1_0_0


class SchemaVersioningTestCase(unittest.TestCase):
    """Tests for schema versioning classes."""

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self):
        removeTestTempDir(self.root)

    def makeEmptyDatabase(self, origin: int = 0) -> Database:
        _, filename = tempfile.mkstemp(dir=self.root, suffix=".sqlite3")
        engine = SqliteDatabase.makeEngine(filename=filename)
        return SqliteDatabase.fromEngine(engine=engine, origin=origin)

    def test_new_schema(self) -> None:
        """Test for creating new database schema."""
        # Check that managers know what schema versions they can make.
        Manager1.checkNewSchemaVersion(V_1_0_0)
        Manager2.checkNewSchemaVersion(V_1_0_0)
        Manager2.checkNewSchemaVersion(V_2_0_0)
        with self.assertRaises(IncompatibleVersionError):
            Manager1.checkNewSchemaVersion(V_1_0_1)
        with self.assertRaises(IncompatibleVersionError):
            Manager1.checkNewSchemaVersion(V_1_1_0)
        with self.assertRaises(IncompatibleVersionError):
            Manager2.checkNewSchemaVersion(V_1_0_1)

        manager_versions = (
            ((None, V_1_0_0), (None, V_1_0_0)),
            ((V_1_0_0, V_1_0_0), (V_1_0_0, V_1_0_0)),
            ((None, V_1_0_0), (V_2_0_0, V_2_0_0)),
        )

        for (v1, result1), (v2, result2) in manager_versions:
            # This is roughly what RegistryManagerTypes.makeRepo does.
            if v1 is not None:
                Manager1.checkNewSchemaVersion(v1)
            if v2 is not None:
                Manager2.checkNewSchemaVersion(v2)
            manager0 = Manager0()
            manager1 = Manager1(registry_schema_version=v1)
            manager2 = Manager2(registry_schema_version=v2)
            self.assertEqual(manager1.newSchemaVersion(), result1)
            self.assertEqual(manager2.newSchemaVersion(), result2)

            database = self.makeEmptyDatabase()
            with database.declareStaticTables(create=True) as context:
                attributes = DefaultButlerAttributeManager.initialize(database, context)

            vmgr = ButlerVersionsManager(attributes)
            vmgr.storeManagersConfig({"manager0": manager0, "manager1": manager1, "manager2": manager2})

            attr_dict = dict(attributes.items())
            expected = {
                "config:registry.managers.manager0": Manager0.extensionName(),
                "config:registry.managers.manager1": Manager1.extensionName(),
                "config:registry.managers.manager2": Manager2.extensionName(),
                f"version:{Manager1.extensionName()}": str(result1),
                f"version:{Manager2.extensionName()}": str(result2),
            }
            self.assertEqual(attr_dict, expected)

    def test_existing_schema(self) -> None:
        """Test for reading manager versions from existing database."""
        manager_versions = (
            ((None, V_1_0_0), (None, V_1_0_0)),
            ((V_1_0_0, V_1_0_0), (V_1_0_0, V_1_0_0)),
        )

        for (v1, result1), (v2, result2) in manager_versions:
            # This is roughly what RegistryManagerTypes.loadRepo does.
            if v1 is not None:
                Manager1.checkNewSchemaVersion(v1)
            if v2 is not None:
                Manager2.checkNewSchemaVersion(v2)
            manager0 = Manager0()
            manager1 = Manager1(registry_schema_version=v1)
            manager2 = Manager2(registry_schema_version=v2)

            # Create new schema first.
            database = self.makeEmptyDatabase()
            with database.declareStaticTables(create=True) as context:
                attributes = DefaultButlerAttributeManager.initialize(database, context)

            vmgr = ButlerVersionsManager(attributes)
            vmgr.storeManagersConfig({"manager0": manager0, "manager1": manager1, "manager2": manager2})

            # Switch to reading existing manager configs/versions.
            with database.declareStaticTables(create=False) as context:
                attributes = DefaultButlerAttributeManager.initialize(database, context)

            vmgr = ButlerVersionsManager(attributes)
            vmgr.checkManagersConfig({"manager0": Manager0, "manager1": Manager1, "manager2": Manager2})
            versions = vmgr.managerVersions()

            Manager1.checkCompatibility(result1, database.isWriteable())
            Manager2.checkCompatibility(result2, database.isWriteable())

            # Make manager instances using versions from registry.
            manager0 = Manager0(registry_schema_version=versions.get("manager0"))
            manager1 = Manager1(registry_schema_version=versions.get("manager1"))
            manager2 = Manager2(registry_schema_version=versions.get("manager2"))
            self.assertIsNone(manager0._registry_schema_version)
            self.assertEqual(manager1._registry_schema_version, result1)
            self.assertEqual(manager2._registry_schema_version, result2)

    def test_compatibility(self) -> None:
        """Test for version compatibility rules."""
        #    Manager,  version, update, compatible
        compat_matrix = (
            (Manager0, V_1_0_0, False, True),
            (Manager0, V_1_0_0, True, True),
            (Manager1, V_1_0_0, False, True),
            (Manager1, V_1_0_0, True, True),
            (Manager1, V_1_0_1, False, True),
            (Manager1, V_1_0_1, True, True),
            (Manager1, V_1_1_0, False, False),
            (Manager1, V_1_1_0, True, False),
            (Manager1, V_2_0_0, False, False),
            (Manager1, V_2_0_0, True, False),
            (Manager1_1, V_1_0_0, False, True),
            (Manager1_1, V_1_0_0, True, False),
            (Manager1_1, V_1_0_1, False, True),
            (Manager1_1, V_1_0_1, True, False),
            (Manager1_1, V_1_1_0, False, True),
            (Manager1_1, V_1_1_0, True, True),
            (Manager2, V_1_0_0, False, True),
            (Manager2, V_1_0_0, True, True),
            (Manager2, V_1_0_1, False, True),
            (Manager2, V_1_0_1, True, True),
            (Manager2, V_1_1_0, False, False),
            (Manager2, V_1_1_0, True, False),
            (Manager2, V_2_0_0, False, True),
            (Manager2, V_2_0_0, True, True),
        )

        for Manager, version, update, compatible in compat_matrix:
            with self.subTest(test=(Manager, version, update, compatible)):
                if compatible:
                    Manager.checkCompatibility(version, update)
                else:
                    with self.assertRaises(IncompatibleVersionError):
                        Manager.checkCompatibility(version, update)


if __name__ == "__main__":
    unittest.main()
