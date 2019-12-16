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
import secrets
import unittest

try:
    # It's possible but silly to have testing.postgresql installed without
    # having the postgresql server installed (because then nothing in
    # testing.postgresql would work), so we use the presence of that module
    # to test whether we can expect the server to be available.
    import testing.postgresql
except ImportError:
    testing = None

import sqlalchemy

from lsst.daf.butler.core.registryConfig import RegistryConfig
from lsst.daf.butler.registry.databases.postgresql import PostgresqlDatabase
from lsst.daf.butler.registry import ddl, Registry
from lsst.daf.butler.registry.tests import DatabaseTests, RegistryTests


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class PostgresqlDatabaseTestCase(unittest.TestCase, DatabaseTests):

    @classmethod
    def setUpClass(cls):
        cls.server = testing.postgresql.Postgresql()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def makeEmptyDatabase(self, origin: int = 0) -> PostgresqlDatabase:
        namespace = f"namespace_{secrets.token_hex(8).lower()}"
        return PostgresqlDatabase.fromUri(origin=origin, uri=self.server.url(), namespace=namespace)

    def getNewConnection(self, database: PostgresqlDatabase, *, writeable: bool) -> PostgresqlDatabase:
        return PostgresqlDatabase.fromUri(origin=database.origin, uri=self.server.url(),
                                          namespace=database.namespace, writeable=writeable)

    @contextmanager
    def asReadOnly(self, database: PostgresqlDatabase) -> PostgresqlDatabase:
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
                            fieldName1,
                            dtype=sqlalchemy.BigInteger,
                            autoincrement=True,
                            primaryKey=True
                        ),
                        ddl.FieldSpec(
                            fieldName2,
                            dtype=sqlalchemy.String,
                            length=16,
                            nullable=False,
                        ),
                    ],
                    unique={(fieldName2,)},
                )
            )
        # Add another table, this time dynamically, with a foreign key to the
        # first table.
        db.ensureTableExists(
            tableName + "_b",
            ddl.TableSpec(
                fields=[
                    ddl.FieldSpec(
                        fieldName1 + "_b",
                        dtype=sqlalchemy.BigInteger,
                        autoincrement=True,
                        primaryKey=True
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
                ]
            )
        )


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class PostgresqlRegistryTestCase(unittest.TestCase, RegistryTests):
    """Tests for `Registry` backed by a PostgreSQL database.
    """

    @classmethod
    def setUpClass(cls):
        cls.server = testing.postgresql.Postgresql()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def makeRegistry(self) -> Registry:
        namespace = f"namespace_{secrets.token_hex(8).lower()}"
        config = RegistryConfig()
        config["db"] = self.server.url()
        config["namespace"] = namespace
        return Registry.fromConfig(config, create=True)


if __name__ == "__main__":
    unittest.main()
