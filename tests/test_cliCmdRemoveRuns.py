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

"""Unit tests for daf_butler CLI remove-runs subcommand."""

import os
import unittest

from lsst.daf.butler import DatasetType
from lsst.daf.butler.cli import butler
from lsst.daf.butler.cli.cmd._remove_runs import (
    abortedMsg,
    didRemoveDatasetsMsg,
    didRemoveRunsMsg,
    mustBeUnlinkedMsg,
    noRunCollectionsMsg,
    removedRunsMsg,
    willRemoveDatasetsMsg,
    willRemoveRunsMsg,
    willUnlinkMsg,
)
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests.utils import MetricTestRepo

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class RemoveCollectionTest(unittest.TestCase):
    """Test the butler remove-runs command."""

    def setUp(self):
        self.runner = LogCliRunner()

    def test_removeRuns(self):
        with self.runner.isolated_filesystem():
            root = "repo"
            repo = MetricTestRepo(root, configFile=os.path.join(TESTDIR, "config/basic/butler.yaml"))
            # Add a dataset type that will have no datasets to make sure it
            # isn't printed.
            repo.butler.registry.registerDatasetType(
                DatasetType("no_datasets", repo.butler.dimensions.empty, "StructuredDataDict")
            )

            # Execute remove-runs but say no, check for expected outputs.
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "ingest*"], input="no")
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(willRemoveRunsMsg, result.output)
            self.assertIn(abortedMsg, result.output)
            self.assertNotIn("no_datasets", result.output)
            self.assertIn(
                "ingest/run",
                list(repo.butler.registry.queryCollections()),
            )

            # Add the run to a CHAINED collection.
            parentCollection = "aParentCollection"
            result = self.runner.invoke(
                butler.cli, f"collection-chain {root} {parentCollection} ingest/run".split()
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            result = self.runner.invoke(butler.cli, ["query-collections", root])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(parentCollection, result.output)

            # Execute remove-runs but say no, check for expected outputs
            # including the CHAINED collection.
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "ingest*"], input="no")
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(willRemoveRunsMsg, result.output)
            self.assertIn(willRemoveDatasetsMsg, result.output)
            self.assertIn(
                willUnlinkMsg.format(run="ingest/run", parents=f'"{parentCollection}"'), result.output
            )
            self.assertIn(abortedMsg, result.output)
            self.assertNotIn("no_datasets", result.output)
            result = self.runner.invoke(butler.cli, ["query-collections", root])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn("ingest/run", result.output)
            self.assertIn(parentCollection, result.output)

            # Do the same remove-runs command, but say yes.
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "ingest*"], input="yes")
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(willRemoveRunsMsg, result.output)
            self.assertIn(willRemoveDatasetsMsg, result.output)
            self.assertIn(removedRunsMsg, result.output)
            result = self.runner.invoke(butler.cli, ["query-collections", root])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertNotIn("ingest/run", result.output)
            self.assertIn(parentCollection, result.output)

            # Now they've been deleted, try again and check for "none found".
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "ingest*"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(noRunCollectionsMsg, result.output)

            # Remake the repo and check --no-confirm option.
            root = "repo1"
            MetricTestRepo(root, configFile=os.path.join(TESTDIR, "config/basic/butler.yaml"))

            # Add the run to a CHAINED collection.
            parentCollection = "parent"
            result = self.runner.invoke(
                butler.cli, f"collection-chain {root} {parentCollection} ingest/run".split()
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            result = self.runner.invoke(butler.cli, ["query-collections", root])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn("ingest/run", result.output)
            self.assertIn(parentCollection, result.output)

            # Execute remove-runs with --no-confirm, should fail because there
            # is a parent CHAINED collection.
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "ingest*", "--no-confirm"])
            self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(
                mustBeUnlinkedMsg.format(run="ingest/run", parents=f'"{parentCollection}"'), result.output
            )
            result = self.runner.invoke(butler.cli, ["query-collections", root])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn("ingest/run", result.output)
            self.assertIn(parentCollection, result.output)

            # Execute remove-runs with --no-confirm and --force
            result = self.runner.invoke(
                butler.cli, ["remove-runs", root, "ingest*", "--no-confirm", "--force"]
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(didRemoveRunsMsg, result.output)
            self.assertIn(didRemoveDatasetsMsg, result.output)
            result = self.runner.invoke(butler.cli, ["query-collections", root])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertNotIn("ingest/run", result.output)
            self.assertIn(parentCollection, result.output)

            # Execute cmd looking for a non-existant collection
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "foo"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(noRunCollectionsMsg, result.output)


if __name__ == "__main__":
    unittest.main()
