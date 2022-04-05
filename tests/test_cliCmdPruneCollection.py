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

import os
import unittest

from astropy.table import Table
from lsst.daf.butler import Butler, CollectionType
from lsst.daf.butler.cli.butler import cli as butlerCli
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests.utils import (
    ButlerTestHelper,
    MetricTestRepo,
    makeTestTempDir,
    readTable,
    removeTestTempDir,
)
from numpy import array

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
            # Use the butler initalizer to create the run, then create a tagged
            # collection.
            butler = Butler(repoName, run=runName)
            butler.registry.registerCollection(taggedName, CollectionType.TAGGED)

            # Verify the run and tag show up in query-collections:
            result = self.runner.invoke(butlerCli, ["query-collections", repoName])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(runName, result.output)
            self.assertIn(taggedName, result.output)

            # Verify the tagged collection can be removed:
            with self.assertWarns(FutureWarning):  # Capture the deprecation warning
                result = self.runner.invoke(
                    butlerCli,
                    ["prune-collection", repoName, taggedName, "--unstore"],
                    input="yes",
                )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            result = self.runner.invoke(butlerCli, ["query-collections", repoName])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(runName, result.output)
            self.assertNotIn(taggedName, result.output)

            # Verify the run can be removed:
            with self.assertWarns(FutureWarning):  # Capture the deprecation warning
                result = self.runner.invoke(
                    butlerCli,
                    ["prune-collection", repoName, runName, "--purge", "--unstore"],
                    input="yes",
                )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            result = self.runner.invoke(butlerCli, ["query-collections", repoName])
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

        self.root = makeTestTempDir(TESTDIR)
        self.testRepo = MetricTestRepo(
            self.root, configFile=os.path.join(TESTDIR, "config/basic/butler.yaml")
        )

    def tearDown(self):
        removeTestTempDir(self.root)

    def testPruneRun(self):
        def confirm_initial_tables():
            result = self.runner.invoke(butlerCli, ["query-collections", self.root])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = Table(array((("ingest", "TAGGED"), ("ingest/run", "RUN"))), names=("Name", "Type"))
            self.assertAstropyTablesEqual(readTable(result.output), expected, unorderedRows=True)

        confirm_initial_tables()

        # Try pruning RUN without purge or unstore, should fail.
        with self.assertWarns(FutureWarning):  # Capture the deprecation warning
            result = self.runner.invoke(
                butlerCli,
                ["prune-collection", self.root, "ingest/run"],
                input="yes",
            )
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))

        # Try pruning RUN without unstore, should fail.
        with self.assertWarns(FutureWarning):  # Capture the deprecation warning
            result = self.runner.invoke(
                butlerCli,
                ["prune-collection", self.root, "ingest/run", "--purge"],
                input="yes",
            )
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))

        # Try pruning RUN without purge, should fail.
        with self.assertWarns(FutureWarning):  # Capture the deprecation warning
            result = self.runner.invoke(
                butlerCli,
                ["prune-collection", self.root, "ingest/run", "--unstore"],
                input="yes",
            )
        self.assertEqual(result.exit_code, 1, clickResultMsg(result))

        # Try pruning RUN with purge and unstore but say "no" for confirmation,
        # should succeed but not change datasets.
        with self.assertWarns(FutureWarning):  # Capture the deprecation warning
            result = self.runner.invoke(
                butlerCli,
                ["prune-collection", self.root, "ingest/run", "--purge", "--unstore"],
                input="no",
            )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        confirm_initial_tables()

        # Try pruning RUN with purge and unstore, should succeed.
        with self.assertWarns(FutureWarning):  # Capture the deprecation warning
            result = self.runner.invoke(
                butlerCli,
                ["prune-collection", self.root, "ingest/run", "--purge", "--unstore"],
                input="no",
            )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # Try pruning RUN with purge and unstore, and use --no-confirm instead
        # of confirm dialog, should succeed.
        with self.assertWarns(FutureWarning):  # Capture the deprecation warning
            result = self.runner.invoke(
                butlerCli,
                ["prune-collection", self.root, "ingest/run", "--purge", "--unstore", "--no-confirm"],
            )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        result = self.runner.invoke(
            butlerCli,
            ["query-collections", self.root],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expected = Table((["ingest"], ["TAGGED"]), names=("Name", "Type"))
        self.assertAstropyTablesEqual(readTable(result.output), expected)

    def testPruneTagged(self):
        result = self.runner.invoke(butlerCli, ["query-collections", self.root])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expected = Table(array((("ingest", "TAGGED"), ("ingest/run", "RUN"))), names=("Name", "Type"))
        self.assertAstropyTablesEqual(readTable(result.output), expected, unorderedRows=True)

        # Try pruning TAGGED, should succeed.
        with self.assertWarns(FutureWarning):  # Capture the deprecation warning
            result = self.runner.invoke(
                butlerCli,
                ["prune-collection", self.root, "ingest", "--unstore"],
                input="yes",
            )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        result = self.runner.invoke(butlerCli, ["query-collections", self.root])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expected = Table((["ingest/run"], ["RUN"]), names=("Name", "Type"))
        self.assertAstropyTablesEqual(readTable(result.output), expected)


if __name__ == "__main__":
    unittest.main()
