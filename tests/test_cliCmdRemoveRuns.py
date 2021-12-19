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

"""Unit tests for daf_butler CLI remove-runs subcommand.
"""


import os
import unittest

from lsst.daf.butler.cli import butler
from lsst.daf.butler.cli.cmd._remove_runs import (
    abortedMsg,
    didRemoveDatasetsMsg,
    didRemoveRunsMsg,
    noRunCollectionsMsg,
    removedRunsMsg,
    willRemoveDatasetsMsg,
    willRemoveRunsMsg,
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
            MetricTestRepo(root, configFile=os.path.join(TESTDIR, "config/basic/butler.yaml"))

            # Execute cmd but say no, check for expected outputs.
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "ingest*"], input="no")
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(willRemoveRunsMsg, result.output)
            self.assertIn(willRemoveDatasetsMsg, result.output)
            self.assertIn(abortedMsg, result.output)

            # ...say yes
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "ingest*"], input="yes")
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(willRemoveRunsMsg, result.output)
            self.assertIn(willRemoveDatasetsMsg, result.output)
            self.assertIn(removedRunsMsg, result.output)

            # now they've been deleted, try again and check for "none found"
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "ingest*"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(noRunCollectionsMsg, result.output)

            # remake the repo and check --no-confirm option
            root = "repo1"
            MetricTestRepo(root, configFile=os.path.join(TESTDIR, "config/basic/butler.yaml"))

            # Execute cmd but say no, check for expected outputs.
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "ingest*", "--no-confirm"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(didRemoveRunsMsg, result.output)
            self.assertIn(didRemoveDatasetsMsg, result.output)

            # Execute cmd looking for a non-existant collection
            result = self.runner.invoke(butler.cli, ["remove-runs", root, "foo"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn(noRunCollectionsMsg, result.output)


if __name__ == "__main__":
    unittest.main()
