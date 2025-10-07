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

import gc
import os
import secrets
import unittest
from contextlib import contextmanager

try:
    # It's possible but silly to have testing.mysqld installed without
    # having the postgresql server installed (because then nothing in
    # testing.mysqld would work), so we use the presence of that module
    # to test whether we can expect the server to be available.
    import testing.mysqld
except ImportError:
    testing = None

import sqlalchemy
from lsst.daf.butler import ddl
from lsst.daf.butler.registry import Registry
from lsst.daf.butler.registry.databases.mysql import MySqlDatabase
from lsst.daf.butler.registry.tests import DatabaseTests, RegistryTests


@unittest.skipUnless(testing is not None, "testing.mysqld module not found")
class MySqlDatabaseTestCase(unittest.TestCase, DatabaseTests):
    @classmethod
    def setUpClass(cls):
        # MariaDB requires the anonymous user
        cls.server = testing.mysqld.Mysqld(user="", my_cnf={"skip-networking": None})

    @classmethod
    def tearDownClass(cls):
        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        cls.server.stop()

    def makeEmptyDatabase(self, origin: int = 0) -> MySqlDatabase:
        namespace = f"namespace_{secrets.token_hex(8).lower()}"
        return MySqlDatabase.fromUri(origin=origin, uri=self.server.url(), namespace=namespace)

    def getNewConnection(self, database: MySqlDatabase, *, writeable: bool) -> MySqlDatabase:
        return MySqlDatabase.fromUri(
            origin=database.origin, uri=self.server.url(), namespace=database.namespace, writeable=writeable
        )

    @contextmanager
    def asReadOnly(self, database: MySqlDatabase) -> MySqlDatabase:
        yield self.getNewConnection(database, writeable=False)

    def testNameShrinking(self):
        """Test that too-long names for database entities other than tables
        and columns (which we preserve, and just expect to fit) are shrunk.
        """
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            # Table and field names are each below the 63-char limit even when
            # accounting for the prefix, but their combination (which will
            # appear in sequences and constraints) is not.
            tableName = "a_table_with_a_very_very_long_42_char_name"
            fieldName1 = "a_column_with_a_very_very_long_43_char_name"
            fieldName2 = "another_column_with_a_very_very_long_49_char_name"
            context.addTable(
                tableName,
                ddl.TableSpec(
                    fields=[
                        ddl.FieldSpec(
                            fieldName1, dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True
                        ),
                        ddl.FieldSpec(
                            fieldName2,
                            dtype=sqlalchemy.String,
                            length=16,
                            nullable=False,
                        ),
                    ],
                    unique={(fieldName2,)},
                ),
            )
        # Add another table, this time dynamically, with a foreign key to the
        # first table.
        db.ensureTableExists(
            tableName + "_b",
            ddl.TableSpec(
                fields=[
                    ddl.FieldSpec(
                        fieldName1 + "_b", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True
                    ),
                    ddl.FieldSpec(
                        fieldName2 + "_b",
                        dtype=sqlalchemy.String,
                        length=16,
                        nullable=False,
                    ),
                ],
                foreignKeys=[
                    ddl.ForeignKeySpec(tableName, source=(fieldName2 + "_b",), target=(fieldName2,)),
                ],
            ),
        )


@unittest.skipUnless(testing is not None, "testing.mysqld module not found")
class MySqlRegistryTests(RegistryTests):
    """Tests for `Registry` backed by a PostgreSQL database.

    Note
    ----
    This is not a subclass of `unittest.TestCase` but to avoid repetition it
    defines methods that override `unittest.TestCase` methods. To make this
    work sublasses have to have this class first in the bases list.
    """

    @classmethod
    def setUpClass(cls):
        # MariaDB requires the anonymous user
        cls.server = testing.mysqld.Mysqld(user="", my_cnf={"skip-networking": None})

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    @classmethod
    def getDataDir(cls) -> str:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "registry"))

    def makeRegistry(self) -> Registry:
        namespace = f"namespace_{secrets.token_hex(8).lower()}"
        config = self.makeRegistryConfig()
        config["db"] = self.server.url()
        config["namespace"] = namespace
        return Registry.fromConfig(config, create=True)


class MySqlRegistryNameKeyCollMgrTestCase(MySqlRegistryTests, unittest.TestCase):
    """Tests for `Registry` backed by a PostgreSQL database.

    This test case uses NameKeyCollectionManager.
    """

    collectionsManager = "lsst.daf.butler.registry.collections.nameKey.NameKeyCollectionManager"


class MySqlRegistrySynthIntKeyCollMgrTestCase(MySqlRegistryTests, unittest.TestCase):
    """Tests for `Registry` backed by a PostgreSQL database.

    This test case uses SynthIntKeyCollectionManager.
    """

    collectionsManager = "lsst.daf.butler.registry.collections.synthIntKey.SynthIntKeyCollectionManager"


if __name__ == "__main__":
    unittest.main()
