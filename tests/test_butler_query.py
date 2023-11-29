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

from __future__ import annotations

import gc
import os
import unittest
from typing import Any

import sqlalchemy

try:
    # It's possible but silly to have testing.postgresql installed without
    # having the postgresql server installed (because then nothing in
    # testing.postgresql would work), so we use the presence of that module
    # to test whether we can expect the server to be available.
    import testing.postgresql  # type: ignore[import]
except ImportError:
    testing = None

from lsst.daf.butler import Butler, Config
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.tests.butler_query import ButlerQueryTests
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class ButlerQueryTestsDirectSQLite(ButlerQueryTests, unittest.TestCase):
    """Unit tests for DirectButler.query methods with sqlite database."""

    data_dir = os.path.join(TESTDIR, "data/registry")
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    root: str
    tmpConfigFile: str

    def setUp(self) -> None:
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        Butler.makeRepo(self.root, config=Config(self.configFile))
        self.tmpConfigFile = os.path.join(self.root, "butler.yaml")
        super().setUp()

    def tearDown(self) -> None:
        removeTestTempDir(self.root)
        super().tearDown()

    def make_butler(self, *args: str) -> Butler:
        # Docstring inherited.
        butler = Butler.from_config(self.tmpConfigFile, run="run")
        assert isinstance(butler, DirectButler)

        for filename in args:
            self.load_data(butler._registry, filename)

        self.make_bias_collection(butler._registry)

        return butler


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class ButlerQueryTestsDirectPostgres(ButlerQueryTests, unittest.TestCase):
    """Unit tests for DirectButler.query methods with postgres database."""

    data_dir = os.path.join(TESTDIR, "data/registry")
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    root: str
    tmpConfigFile: str

    @staticmethod
    def _handler(postgresql: Any) -> None:
        engine = sqlalchemy.engine.create_engine(postgresql.url())
        with engine.begin() as connection:
            connection.execute(sqlalchemy.text("CREATE EXTENSION btree_gist;"))

    @classmethod
    def setUpClass(cls) -> None:
        # Create the postgres test server.
        cls.postgresql = testing.postgresql.PostgresqlFactory(
            cache_initialized_db=True, on_initialized=cls._handler
        )
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        cls.postgresql.clear_cache()
        super().tearDownClass()

    def setUp(self) -> None:
        self.server = self.postgresql()

        self.root = makeTestTempDir(TESTDIR)
        self.tmpConfigFile = os.path.join(self.root, "butler.yaml")

        # Use default config but update database URI.
        config = Config(self.configFile)
        config["registry", "db"] = self.server.url()
        Butler.makeRepo(self.root, config=config)

        super().setUp()

    def tearDown(self) -> None:
        self.server.stop()
        removeTestTempDir(self.root)
        super().tearDown()

    def make_butler(self, *args: str) -> Butler:
        # Docstring inherited.
        butler = Butler.from_config(self.tmpConfigFile, run="run")
        assert isinstance(butler, DirectButler)

        for filename in args:
            self.load_data(butler._registry, filename)

        self.make_bias_collection(butler._registry)

        return butler


if __name__ == "__main__":
    unittest.main()
