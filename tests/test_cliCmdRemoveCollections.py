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

"""Unit tests for daf_butler CLI prune-collections subcommand.
"""

import os
import unittest
from collections.abc import Sequence

from astropy.table import Table
from lsst.daf.butler import Butler, CollectionType
from lsst.daf.butler.cli.butler import cli as butlerCli
from lsst.daf.butler.cli.cmd._remove_collections import (
    abortedMsg,
    canNotRemoveFoundRuns,
    didNotRemoveFoundRuns,
    noNonRunCollectionsMsg,
    removedCollectionsMsg,
    willRemoveCollectionMsg,
)
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.script.removeCollections import removeCollections
from lsst.daf.butler.tests.utils import (
    ButlerTestHelper,
    MetricTestRepo,
    makeTestTempDir,
    readTable,
    removeTestTempDir,
)
from numpy import array

TESTDIR = os.path.abspath(os.path.dirname(__file__))

QueryCollectionsRow = tuple[str, str] | tuple[str, str, str]
RemoveCollectionRow = tuple[str, str]


class RemoveCollectionTest(unittest.TestCase, ButlerTestHelper):
    """Test executing remove collection."""

    def setUp(self):
        self.runner = LogCliRunner()

        self.root = makeTestTempDir(TESTDIR)
        self.testRepo = MetricTestRepo(
            self.root, configFile=os.path.join(TESTDIR, "config/basic/butler.yaml")
        )

    def tearDown(self):
        removeTestTempDir(self.root)

    def _verify_remove(
        self,
        collection: str,
        before_rows: Sequence[QueryCollectionsRow],
        remove_rows: Sequence[RemoveCollectionRow],
        after_rows: Sequence[QueryCollectionsRow],
    ):
        """Remove collections, with verification that expected collections are
        present before removing, that the command reports expected collections
        to be removed, and that expected collections are present after removal.

        Parameters
        ----------
        collection : `str`
            The name of the collection, or glob pattern for collections, to
            remove.
        before_rows : `Sequence` [ `QueryCollectionsRow` ]
            The rows that should be in the table returned by query-collections
            before removing the collection.
        remove_rows : `Sequence` [ `RemoveCollectionRow` ]
            The rows that should be in the "will remove" table while removing
            collections.
        after_rows : `Sequence` [ `QueryCollectionsRow` ]
            The rows that should be in the table returned by query-collections
            after removing the collection.
        """

        def _query_collection_column_names(rows):
            # If there is a chained collection in the table then there is a
            # definition column, otherwise there is only the name and type
            # columns.
            if len(rows[0]) == 2:
                return ("Name", "Type")
            elif len(rows[0]) == 3:
                return ("Name", "Type", "Children")
            else:
                raise RuntimeError(f"Unhandled column count: {len(rows[0])}")

        result = self.runner.invoke(butlerCli, ["query-collections", self.root, "--chains", "TABLE"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expected = Table(array(before_rows), names=_query_collection_column_names(before_rows))
        self.assertAstropyTablesEqual(readTable(result.output), expected, unorderedRows=True)

        removal = removeCollections(
            repo=self.root,
            collection=collection,
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expected = Table(array(remove_rows), names=("Collection", "Collection Type"))
        self.assertAstropyTablesEqual(removal.removeCollectionsTable, expected)
        removal.onConfirmation()

        result = self.runner.invoke(butlerCli, ["query-collections", self.root])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        expected = Table(array(after_rows), names=_query_collection_column_names(after_rows))
        self.assertAstropyTablesEqual(readTable(result.output), expected, unorderedRows=True)

    def testRemoveScript(self):
        """Test removing collections.

        Combining several tests into one case allows us to reuse the test repo,
        which saves execution time.
        """
        # Test wildcard with chained collections:

        # Add a couple chained collections
        for parent, child in (
            ("chained-run-1", "ingest/run"),
            ("chained-run-2", "ingest/run"),
        ):
            result = self.runner.invoke(
                butlerCli,
                ["collection-chain", self.root, parent, child],
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        self._verify_remove(
            collection="chained-run-*",
            before_rows=(
                ("chained-run-1", "CHAINED", "ingest/run"),
                ("chained-run-2", "CHAINED", "ingest/run"),
                ("ingest", "TAGGED", ""),
                ("ingest/run", "RUN", ""),
            ),
            remove_rows=(
                ("chained-run-1", "CHAINED"),
                ("chained-run-2", "CHAINED"),
            ),
            after_rows=(
                ("ingest", "TAGGED"),
                ("ingest/run", "RUN"),
            ),
        )

        # Test a single tagged collection:

        self._verify_remove(
            collection="ingest",
            before_rows=(
                ("ingest", "TAGGED"),
                ("ingest/run", "RUN"),
            ),
            remove_rows=(("ingest", "TAGGED"),),
            after_rows=(("ingest/run", "RUN"),),
        )

    def testRemoveCmd(self):
        """Test remove command outputs."""
        # Test expected output with a non-existent collection:

        result = self.runner.invoke(butlerCli, ["remove-collections", self.root, "fake_collection"])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn(noNonRunCollectionsMsg, result.stdout)

        # Add a couple chained collections
        for parent, child in (
            ("chained-run-1", "ingest/run"),
            ("chained-run-2", "ingest/run"),
        ):
            result = self.runner.invoke(
                butlerCli,
                ["collection-chain", self.root, parent, child],
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

        # Test aborting a removal

        result = self.runner.invoke(
            butlerCli,
            ["remove-collections", self.root, "chained-run-1"],
            input="no",
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn(abortedMsg, result.stdout)

        # Remove with --no-confirm, it's expected to run silently.

        result = self.runner.invoke(
            butlerCli, ["remove-collections", self.root, "chained-run-1", "--no-confirm"]
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn(removedCollectionsMsg, result.stdout)
        self.assertIn("chained-run-1", result.stdout)

        # verify chained-run-1 was removed:

        butler = Butler.from_config(self.root)
        collections = butler.registry.queryCollections(
            collectionTypes=frozenset(
                (
                    CollectionType.RUN,
                    CollectionType.TAGGED,
                    CollectionType.CHAINED,
                    CollectionType.CALIBRATION,
                )
            ),
        )
        self.assertCountEqual(["ingest/run", "ingest", "chained-run-2"], collections)

        # verify chained-run-2 can be removed with prompting and expected CLI
        # output

        result = self.runner.invoke(
            butlerCli,
            ["remove-collections", self.root, "chained-run-2"],
            input="yes",
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn(willRemoveCollectionMsg, result.stdout)
        self.assertIn("chained-run-2 CHAINED", result.stdout)

        # try to remove a run table, check for the "can not remove run" message

        result = self.runner.invoke(butlerCli, ["collection-chain", self.root, "run-chain", child])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        result = self.runner.invoke(
            # removes run-chain (chained collection), but can not remove the
            # run collection, and emits a message that says so.
            butlerCli,
            ["remove-collections", self.root, "*run*"],
            input="yes",
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn(canNotRemoveFoundRuns, result.stdout)
        self.assertIn("ingest/run", result.stdout)

        # try to remove a run table with --no-confirm, check for the "did not
        # remove run" message

        result = self.runner.invoke(butlerCli, ["collection-chain", self.root, "run-chain", child])
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        result = self.runner.invoke(
            # removes run-chain (chained collection), but can not remove the
            # run collection, and emits a message that says so.
            butlerCli,
            ["remove-collections", self.root, "*run*", "--no-confirm"],
        )
        self.assertEqual(result.exit_code, 0, clickResultMsg(result))
        self.assertIn(didNotRemoveFoundRuns, result.stdout)
        self.assertIn("ingest/run", result.stdout)


if __name__ == "__main__":
    unittest.main()
