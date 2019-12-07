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

from lsst.daf.butler.registry.databases.postgresql import PostgresqlDatabase
from lsst.daf.butler.registry.tests import DatabaseTests


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
        return PostgresqlDatabase(origin=origin, uri=self.server.url(), namespace=namespace)

    def getNewConnection(self, database: PostgresqlDatabase, *, writeable: bool) -> PostgresqlDatabase:
        return PostgresqlDatabase(origin=database.origin, uri=self.server.url(),
                                  namespace=database.namespace, writeable=writeable)

    @contextmanager
    def asReadOnly(self, database: PostgresqlDatabase) -> PostgresqlDatabase:
        yield self.getNewConnection(database, writeable=False)


if __name__ == "__main__":
    unittest.main()
