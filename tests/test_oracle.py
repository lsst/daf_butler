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
import secrets
import unittest

import sqlalchemy

from lsst.daf.butler.registry.databases.oracle import OracleDatabase
from lsst.daf.butler.registry import ddl
from lsst.daf.butler.registry.tests import DatabaseTests

ENVVAR = "DAF_BUTLER_ORACLE_TEST_URI"
TEST_URI = os.environ.get(ENVVAR)


@unittest.skipUnless(TEST_URI is not None, f"{ENVVAR} environment variable not set.")
class OracleDatabaseTestCase(unittest.TestCase, DatabaseTests):

    @classmethod
    def setUpClass(cls):
        # Create a single engine for all Database instances we create, to avoid
        # repeatedly spending time connecting.
        cls._connection = OracleDatabase.connect(TEST_URI)
        cls._prefixes = []

    @classmethod
    def tearDownClass(cls):
        commands = []
        dbapi = cls._connection.connection
        cursor = dbapi.cursor()
        for objectType, objectName in cursor.execute("SELECT object_type, object_name FROM user_objects"):
            if not any(objectName.lower().startswith(prefix) for prefix in cls._prefixes):
                continue
            if objectType == "TABLE":
                commands.append(f'DROP TABLE "{objectName}" CASCADE CONSTRAINTS')
            elif objectType in ("VIEW", "PROCEDURE", "SEQUENCE"):
                commands.append(f'DROP {objectType} "{objectName}"')
        for command in commands:
            cursor.execute(command)
        cursor.close()

    def makeEmptyDatabase(self, origin: int = 0) -> OracleDatabase:
        prefix = f"test_{secrets.token_hex(8).lower()}_"
        self._prefixes.append(prefix)
        return OracleDatabase(origin=origin, connection=self._connection, prefix=prefix)

    def getNewConnection(self, database: OracleDatabase, *, writeable: bool) -> OracleDatabase:
        return OracleDatabase(origin=database.origin, connection=self._connection,
                              prefix=database.prefix, writeable=writeable)

    @contextmanager
    def asReadOnly(self, database: OracleDatabase) -> OracleDatabase:
        yield self.getNewConnection(database, writeable=False)

    def testNameShrinking(self):
        """Test that too-long names for database entities other than tables
        and columns (which we preserve, and just expect to fit) are shrunk.
        """
        db = self.makeEmptyDatabase(origin=1)
        with db.declareStaticTables(create=True) as context:
            # Table and field names are each below the 128-char limit even when
            # accounting for the prefix, but their combination (which will
            # appear in sequences and constraints) is not.
            tableName = "a_table_with_a_very_very_very_very_very_very_very_very_long_72_char_name"
            fieldName1 = "a_column_with_a_very_very_very_very_very_very_very_very_long_73_char_name"
            fieldName2 = "another_column_with_a_very_very_very_very_very_very_very_very_long_79_char_name"
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


if __name__ == "__main__":
    unittest.main()
