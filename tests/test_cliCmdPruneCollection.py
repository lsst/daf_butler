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

import astropy
from astropy.table import Table
from numpy import array
import os
import shutil
import tempfile
import unittest

from lsst.daf.butler import (
    Butler,
    Config,
    DatasetRef,
    DatasetType,
    StorageClassFactory
)
from lsst.daf.butler.cli.butler import cli as butlerCli
from lsst.daf.butler.cli.utils import clickResultMsg, LogCliRunner
from lsst.daf.butler.tests import MetricsExample
from lsst.daf.butler.tests.utils import ButlerTestHelper, readTable


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

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    storageClassFactory = StorageClassFactory()

    @staticmethod
    def _makeExampleMetrics():
        return MetricsExample({"AM1": 5.2, "AM2": 30.6},
                              {"a": [1, 2, 3],
                               "b": {"blue": 5, "red": "green"}},
                              [563, 234, 456.7, 752, 8, 9, 27])

    @staticmethod
    def _addDatasetType(datasetTypeName, dimensions, storageClass, registry):
        """Create a DatasetType and register it
        """
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        registry.registerDatasetType(datasetType)
        return datasetType

    def setUp(self):
        self.runner = LogCliRunner()

        self.root = tempfile.mkdtemp(dir=TESTDIR)
        Butler.makeRepo(self.root, config=Config(self.configFile))
        self.butlerConfigFile = os.path.join(self.root, "butler.yaml")
        self.storageClassFactory.addFromConfig(self.configFile)

        # New datasets will be added to run and tag, but we will only look in
        # tag when looking up datasets.
        run = "ingest/run"
        tag = "ingest"
        self.butler = Butler(self.butlerConfigFile, run=run, collections=[tag], tags=[tag])

        # There will not be a collection yet
        collections = set(self.butler.registry.queryCollections())
        self.assertEqual(collections, set([run, tag]))

        storageClass = self.storageClassFactory.getStorageClass("StructuredCompositeReadComp")

        # Create and register a DatasetType
        dimensions = self.butler.registry.dimensions.extract(["instrument", "visit"])
        datasetTypeName = "test_metric_comp"
        self.datasetType = self._addDatasetType(datasetTypeName, dimensions, storageClass,
                                                self.butler.registry)

        # Add needed Dimensions
        self.butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        self.butler.registry.insertDimensionData("physical_filter", {"instrument": "DummyCamComp",
                                                                     "name": "d-r",
                                                                     "band": "R"})
        self.butler.registry.insertDimensionData("visit_system", {"instrument": "DummyCamComp",
                                                                  "id": 1,
                                                                  "name": "default"})
        visit_start = astropy.time.Time("2020-01-01 08:00:00.123456789", scale="tai")
        visit_end = astropy.time.Time("2020-01-01 08:00:36.66", scale="tai")
        self.butler.registry.insertDimensionData("visit",
                                                 {"instrument": "DummyCamComp", "id": 423,
                                                  "name": "fourtwentythree", "physical_filter": "d-r",
                                                  "visit_system": 1, "datetime_begin": visit_start,
                                                  "datetime_end": visit_end})
        self.butler.registry.insertDimensionData("visit", {"instrument": "DummyCamComp", "id": 424,
                                                           "name": "fourtwentyfour", "physical_filter": "d-r",
                                                           "visit_system": 1})
        metric = self._makeExampleMetrics()
        dataId = {"instrument": "DummyCamComp", "visit": 423}
        ref = DatasetRef(self.datasetType, dataId, id=None)
        self.butler.put(metric, ref)

        metric = self._makeExampleMetrics()
        dataId = {"instrument": "DummyCamComp", "visit": 424}
        ref = DatasetRef(self.datasetType, dataId, id=None)
        self.butler.put(metric, ref)

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
