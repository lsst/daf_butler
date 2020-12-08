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

"""Unit tests for daf_butler CLI prune-collections subcommand.
"""

from astropy.table import Table
from numpy import array
import os
import shutil
import tempfile
import unittest

from lsst.daf.butler import Butler
from lsst.daf.butler.cli.butler import cli as butlerCli
from lsst.daf.butler.cli.utils import clickResultMsg, LogCliRunner
from lsst.daf.butler.tests.utils import ButlerTestHelper, MetricTestRepo, readTable


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class PruneCollectionsTest(unittest.TestCase):

    def setUp(self):
        self.runner = LogCliRunner()

    def testPruneCollections(self):
        """Test removing a collection and run from a repository using the
        butler prune-collection subcommand."""
        with self.runner.isolated_filesystem():
            repoName = "myRepo"
            runName = "myRun"
            taggedName = "taggedCollection"

            # Add the run and the tagged collection to the repo:
            result = self.runner.invoke(butlerCli, ["create", repoName])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            # Use the butler initalizer to create the run and tagged
            # collection.
            Butler(repoName, run=runName, tags=[taggedName])

            # Verify the run and tag show up in query-collections:
            result = self.runner.invoke(butlerCli, ["query-collections", repoName])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(runName, result.output)
            self.assertIn(taggedName, result.output)

            # Verify the tagged collection can be removed:
            result = self.runner.invoke(butlerCli, ["prune-collection", repoName,
                                                    taggedName,
                                                    "--unstore"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            result = self.runner.invoke(butlerCli, ["query-collections", repoName])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(runName, result.output)
            self.assertNotIn(taggedName, result.output)

            # Verify the run can be removed:
            result = self.runner.invoke(butlerCli, ["prune-collection", repoName,
                                                    runName,
                                                    "--purge",
                                                    "--unstore"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertNotIn(runName, result.output)
            self.assertNotIn(taggedName, result.output)


class PruneCollectionExecutionTest(unittest.TestCase, ButlerTestHelper):
    """Test executing a small number of basic prune-collections commands to
    verify collections can be pruned.
    """

    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.pruneCollection"

    def setUp(self):
        self.runner = LogCliRunner()

        self.root = tempfile.mkdtemp(dir=TESTDIR)
        self.testRepo = MetricTestRepo(self.root,
                                       configFile=os.path.join(TESTDIR, "config/basic/butler.yaml"))

    def tearDown(self):
        if os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def testPruneRun(self):
        result = self.runner.invoke(butlerCli, ["query-collections", self.root])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expected = Table(array((("ingest/run", "RUN"),
                                ("ingest", "TAGGED"))),
                         names=("Name", "Type"))
        self.assertAstropyTablesEqual(readTable(result.output), expected)

        # Try pruning RUN without purge or unstore, should fail.
        result = self.runner.invoke(butlerCli, ["prune-collection", self.root, "ingest/run"])
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))

        # Try pruning RUN without unstore, should fail.
        result = self.runner.invoke(butlerCli, ["prune-collection", self.root, "ingest/run",
                                                "--purge"])
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))

        # Try pruning RUN without purge, should fail.
        result = self.runner.invoke(butlerCli, ["prune-collection", self.root, "ingest/run",
                                                "--unstore"])
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))

        # Try pruning RUN with purge and unstore, should succeed.
        result = self.runner.invoke(butlerCli, ["prune-collection", self.root, "ingest/run",
                                                "--purge", "--unstore"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        result = self.runner.invoke(butlerCli, ["query-collections", self.root])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expected = Table((["ingest"], ["TAGGED"]),
                         names=("Name", "Type"))
        self.assertAstropyTablesEqual(readTable(result.output), expected)

    def testPruneTagged(self):
        result = self.runner.invoke(butlerCli, ["query-collections", self.root])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expected = Table(array((("ingest/run", "RUN"),
                                ("ingest", "TAGGED"))),
                         names=("Name", "Type"))
        self.assertAstropyTablesEqual(readTable(result.output), expected)

        # Try pruning TAGGED, should succeed.
        result = self.runner.invoke(butlerCli, ["prune-collection", self.root, "ingest", "--unstore"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        result = self.runner.invoke(butlerCli, ["query-collections", self.root])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expected = Table((["ingest/run"], ["RUN"]),
                         names=("Name", "Type"))
        self.assertAstropyTablesEqual(readTable(result.output), expected)


if __name__ == "__main__":
    unittest.main()
