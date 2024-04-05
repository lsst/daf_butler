# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Tests for DirectButler._query with PostgreSQL.
"""

from __future__ import annotations

import gc
import os
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
from lsst.daf.butler import Butler, ButlerConfig, StorageClassFactory
from lsst.daf.butler.datastore import NullDatastore
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.registry.sql_registry import SqlRegistry
from lsst.daf.butler.tests.butler_queries import ButlerQueryTests
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


def _start_server(root):
    """Start a PostgreSQL server and create a database within it, returning
    an object encapsulating both.
    """
    server = testing.postgresql.Postgresql(base_dir=root)
    engine = sqlalchemy.create_engine(server.url())
    with engine.begin() as connection:
        connection.execute(sqlalchemy.text("CREATE EXTENSION btree_gist;"))
    return server


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class DirectButlerPostgreSQLTests(ButlerQueryTests, unittest.TestCase):
    """Tests for DirectButler._query with PostgreSQL."""

    data_dir = os.path.join(TESTDIR, "data/registry")

    @classmethod
    def setUpClass(cls):
        cls.root = makeTestTempDir(TESTDIR)
        cls.server = _start_server(cls.root)

    @classmethod
    def tearDownClass(cls):
        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        cls.server.stop()
        removeTestTempDir(cls.root)

    def make_butler(self, *args: str) -> Butler:
        config = ButlerConfig()
        config[".registry.db"] = self.server.url()
        config[".registry.namespace"] = f"namespace_{secrets.token_hex(8).lower()}"
        registry = SqlRegistry.createFromConfig(config)
        for arg in args:
            self.load_data(registry, arg)
        return DirectButler(
            config=config,
            registry=registry,
            datastore=NullDatastore(None, None),
            storageClasses=StorageClassFactory(),
        )

    # TODO (DM-43697): these tests fail due to something going awry with cursor
    # and temporary table lifetime management; PostgreSQL says we can't drop
    # temp tables because there are still active queries against them.  The
    # logic looks fine but is obfuscated by the many levels of competing
    # context managers in the Database class, so we'll punt this to DM-43697.

    @unittest.expectedFailure
    def test_data_coordinate_upload_force_temp_table(self) -> None:
        super().test_data_coordinate_upload_force_temp_table()

    @unittest.expectedFailure
    def test_materialization(self) -> None:
        return super().test_materialization()


if __name__ == "__main__":
    unittest.main()
