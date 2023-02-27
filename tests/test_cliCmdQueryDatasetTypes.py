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

"""Unit tests for daf_butler CLI query-collections command.
"""

import unittest

from astropy.table import Table as AstropyTable
from lsst.daf.butler.cli.butler import cli
from lsst.daf.butler.cli.cmd import query_dataset_types
from lsst.daf.butler.cli.utils import LogCliRunner, clickResultMsg
from lsst.daf.butler.tests import CliCmdTestBase
from lsst.daf.butler.tests.utils import ButlerTestHelper, readTable
from numpy import array


class QueryDatasetTypesCmdTest(CliCmdTestBase, unittest.TestCase):
    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.queryDatasetTypes"

    @staticmethod
    def defaultExpected():
        return dict(repo=None, verbose=False, glob=(), components=None)

    @staticmethod
    def command():
        return query_dataset_types

    def test_minimal(self):
        """Test only required parameters."""
        self.run_test(["query-dataset-types", "here"], self.makeExpected(repo="here"))

    def test_requiredMissing(self):
        """Test that if the required parameter is missing it fails"""
        self.run_missing(["query-dataset-types"], r"Error: Missing argument ['\"]REPO['\"].")

    def test_all(self):
        """Test all parameters."""
        self.run_test(
            ["query-dataset-types", "here", "--verbose", "foo*", "--components"],
            self.makeExpected(repo="here", verbose=True, glob=("foo*",), components=True),
        )
        self.run_test(
            ["query-dataset-types", "here", "--verbose", "foo*", "--no-components"],
            self.makeExpected(repo="here", verbose=True, glob=("foo*",), components=False),
        )


class QueryDatasetTypesScriptTest(ButlerTestHelper, unittest.TestCase):
    def testQueryDatasetTypes(self):
        self.maxDiff = None
        datasetName = "test"
        instrumentDimension = "instrument"
        visitDimension = "visit"
        storageClassName = "StructuredDataDict"
        expectedNotVerbose = AstropyTable((("test",),), names=("name",))
        runner = LogCliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["create", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            # Create the dataset type.
            result = runner.invoke(
                cli,
                [
                    "register-dataset-type",
                    "here",
                    datasetName,
                    storageClassName,
                    instrumentDimension,
                    visitDimension,
                ],
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            # Okay to create it again identically.
            result = runner.invoke(
                cli,
                [
                    "register-dataset-type",
                    "here",
                    datasetName,
                    storageClassName,
                    instrumentDimension,
                    visitDimension,
                ],
            )
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            # Not okay to create a different version of it.
            result = runner.invoke(
                cli, ["register-dataset-type", "here", datasetName, storageClassName, instrumentDimension]
            )
            self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))
            # Not okay to try to create a component dataset type.
            result = runner.invoke(
                cli, ["register-dataset-type", "here", "a.b", storageClassName, instrumentDimension]
            )
            self.assertNotEqual(result.exit_code, 0, clickResultMsg(result))
            # check not-verbose output:
            result = runner.invoke(cli, ["query-dataset-types", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertAstropyTablesEqual(readTable(result.output), expectedNotVerbose)
            # check glob output:
            result = runner.invoke(cli, ["query-dataset-types", "here", "t*"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertAstropyTablesEqual(readTable(result.output), expectedNotVerbose)
            # check verbose output:
            result = runner.invoke(cli, ["query-dataset-types", "here", "--verbose"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            expected = AstropyTable(
                array(
                    (
                        "test",
                        "['band', 'instrument', 'physical_filter', 'visit']",
                        storageClassName,
                    )
                ),
                names=("name", "dimensions", "storage class"),
            )
            self.assertAstropyTablesEqual(readTable(result.output), expected)

            # Now remove and check that it was removed
            # First a non-existent one
            result = runner.invoke(cli, ["remove-dataset-type", "here", "unreal"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

            # Now one we now has been registered
            result = runner.invoke(cli, ["remove-dataset-type", "here", datasetName])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))

            # and check that it has gone
            result = runner.invoke(cli, ["query-dataset-types", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertIn("No results", result.output)


if __name__ == "__main__":
    unittest.main()
