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

from lsst.daf.butler.registry.databases.sqlite import SqliteDatabase
from lsst.daf.butler.registry.tests import DatabaseTests

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SqliteDatabaseTestCase(unittest.TestCase, DatabaseTests):

    def setUp(self):
        self.root = tempfile.mkdtemp(dir=TESTDIR)

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def makeEmptyDatabase(self, origin: int = 0) -> SqliteDatabase:
        _, filename = tempfile.mkstemp(dir=self.root, suffix=".sqlite3")
        return SqliteDatabase(origin=origin, filename=filename)

    def getNewConnection(self, database: SqliteDatabase, *, writeable: bool) -> SqliteDatabase:
        return SqliteDatabase(origin=database.origin, filename=database.filename, writeable=writeable)

    @contextmanager
    def asReadOnly(self, database: SqliteDatabase) -> SqliteDatabase:
        mode = os.stat(database.filename).st_mode
        try:
            os.chmod(database.filename, stat.S_IREAD)
            yield self.getNewConnection(database, writeable=False)
        finally:
            os.chmod(database.filename, mode)


if __name__ == "__main__":
    unittest.main()
