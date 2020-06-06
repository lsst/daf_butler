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

import click
import unittest
import yaml

from lsst.daf.butler import Butler, DatasetType, StorageClass
from lsst.daf.butler.cli.butler import cli
from lsst.daf.butler.cli.utils import clickResultMsg
from lsst.daf.butler.tests.mockeredTest import MockeredTestBase


class QueryDatasetTypesCmdTest(MockeredTestBase):

    defaultExpected = dict(repo=None,
                           verbose=False,
                           glob=(),
                           components=None)

    def test_minimal(self):
        """Test only required parameters.
        """
        self.run_test(["query-dataset-types", "here"],
                      self.makeExpected(repo="here"))

    def test_requiredMissing(self):
        """Test that if the required parameter is missing it fails"""
        self.run_missing(["query-dataset-types"], 'Error: Missing argument "REPO".')

    def test_all(self):
        """Test all parameters."""
        self.run_test(["query-dataset-types", "here", "--verbose", "foo*", "--components"],
                      self.makeExpected(repo="here", verbose=True, glob=("foo*", ), components=True))
        self.run_test(["query-dataset-types", "here", "--verbose", "foo*", "--no-components"],
                      self.makeExpected(repo="here", verbose=True, glob=("foo*", ), components=False))


class QueryDatasetTypesScriptTest(unittest.TestCase):

    def testQueryDatasetTypes(self):
        self.maxDiff = None
        datasetName = "test"
        instrumentDimension = "instrument"
        visitDimension = "visit"
        storageClassName = "testDatasetType"
        expectedNotVerbose = {"datasetTypes": [datasetName]}
        runner = click.testing.CliRunner()
        with runner.isolated_filesystem():
            butlerCfg = Butler.makeRepo("here")
            butler = Butler(butlerCfg, writeable=True)
            storageClass = StorageClass(storageClassName)
            butler.registry.storageClasses.registerStorageClass(storageClass)
            dimensions = butler.registry.dimensions.extract((instrumentDimension, visitDimension))
            datasetType = DatasetType(datasetName, dimensions, storageClass)
            butler.registry.registerDatasetType(datasetType)
            # check not-verbose output:
            result = runner.invoke(cli, ["query-dataset-types", "here"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertEqual(expectedNotVerbose, yaml.safe_load(result.output))
            # check glob output:
            result = runner.invoke(cli, ["query-dataset-types", "here", "t*"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            self.assertEqual(expectedNotVerbose, yaml.safe_load(result.output))
            # check verbose output:
            result = runner.invoke(cli, ["query-dataset-types", "here", "--verbose"])
            self.assertEqual(result.exit_code, 0, clickResultMsg(result))
            response = yaml.safe_load(result.output)
            # output dimension names contain all required dimensions, more than
            # the registered dimensions, so verify the expected components
            # individually.
            self.assertEqual(response["datasetTypes"][0]["name"], datasetName)
            self.assertEqual(response["datasetTypes"][0]["storageClass"], storageClassName)
            self.assertIn(instrumentDimension, response["datasetTypes"][0]["dimensions"])
            self.assertIn(visitDimension, response["datasetTypes"][0]["dimensions"])


if __name__ == "__main__":
    unittest.main()
