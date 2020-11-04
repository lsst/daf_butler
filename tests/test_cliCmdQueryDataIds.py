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

import astropy
from astropy.table import Table as AstropyTable
from numpy import array
import os
import shutil
import tempfile
import unittest

from lsst.daf.butler import (
    Butler,
    Config,
    DatasetRef,
    StorageClassFactory
)
from lsst.daf.butler import script
from lsst.daf.butler.tests import addDatasetType, MetricsExample
from lsst.daf.butler.tests.utils import ButlerTestHelper


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QueryDataIdsTest(unittest.TestCase, ButlerTestHelper):

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    storageClassFactory = StorageClassFactory()

    @staticmethod
    def _makeExampleMetrics():
        return MetricsExample({"AM1": 5.2, "AM2": 30.6},
                              {"a": [1, 2, 3],
                               "b": {"blue": 5, "red": "green"}},
                              [563, 234, 456.7, 752, 8, 9, 27])

    @staticmethod
    def _queryDataIds(repo, dimensions=(), collections=(), datasets=None, where=None):
        """Helper to populate the call to script.queryDataIds with default
        values."""
        return script.queryDataIds(repo=repo,
                                   dimensions=dimensions,
                                   collections=collections,
                                   datasets=datasets,
                                   where=where)

    def setUp(self):
        self.root = tempfile.mkdtemp(dir=TESTDIR)
        butlerConfig = Butler.makeRepo(self.root, config=Config(self.configFile))
        self.storageClassFactory.addFromConfig(self.configFile)

        # New datasets will be added to run and tag, but we will only look in
        # tag when looking up datasets.
        run = "ingest/run"
        tag = "ingest"
        self.butler = Butler(butlerConfig, run=run, collections=[tag], tags=[tag])

        # There will not be a collection yet
        collections = set(self.butler.registry.queryCollections())
        self.assertEqual(collections, set([run, tag]))

        storageClass = self.storageClassFactory.getStorageClass("StructuredCompositeReadComp")

        # Create and register a DatasetType
        dimensions = self.butler.registry.dimensions.extract(["instrument", "visit"])
        datasetTypeName = "test_metric_comp"
        self.datasetType = addDatasetType(self.butler, datasetTypeName, dimensions, storageClass)

        # Add needed Dimensions
        self.butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        self.butler.registry.insertDimensionData("instrument", {"name": "alt_DummyCamComp"})
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

    def testDimensions(self):
        """Test getting a dimension."""
        res = self._queryDataIds(self.root, dimensions=("visit",))
        expected = AstropyTable(
            array((
                ("R", "DummyCamComp", "d-r", 1, 423),
                ("R", "DummyCamComp", "d-r", 1, 424)
            )),
            names=("band", "instrument", "physical_filter", "visit_system", "visit")
        )
        self.assertAstropyTablesEqual(res, expected)

    def testNull(self):
        "Test asking for nothing."
        res = self._queryDataIds(self.root)
        self.assertEqual(res, None)

    def testDatasets(self):
        """Test getting datasets."""
        res = self._queryDataIds(self.root, datasets="test_metric_comp")
        expected = AstropyTable(
            array((
                ("R", "DummyCamComp", "d-r", 1, 423),
                ("R", "DummyCamComp", "d-r", 1, 424)
            )),
            names=("band", "instrument", "physical_filter", "visit_system", "visit")
        )
        self.assertAstropyTablesEqual(res, expected)

    def testWhere(self):
        """Test getting datasets."""
        res = self._queryDataIds(self.root, dimensions=("visit",), where="visit=423")
        expected = AstropyTable(
            array((
                ("R", "DummyCamComp", "d-r", 1, 423),
            )),
            names=("band", "instrument", "physical_filter", "visit_system", "visit")
        )
        self.assertAstropyTablesEqual(res, expected)

    def testCollections(self):
        """Test getting datasets using the collections option."""

        # Add a dataset in a different collection
        butler = Butler(self.root, run="foo")
        butler.registry.insertDimensionData("visit", {"instrument": "DummyCamComp", "id": 425,
                                                      "name": "fourtwentyfive", "physical_filter": "d-r",
                                                      "visit_system": 1})
        metric = self._makeExampleMetrics()
        dataId = {"instrument": "DummyCamComp", "visit": 425}
        ref = DatasetRef(self.datasetType, dataId, id=None)
        butler.put(metric, ref)

        # Verify the new dataset is not found in the "ingest/run" collection.
        res = self._queryDataIds(repo=self.root, dimensions=("visit",), collections=("ingest/run",),
                                 datasets="test_metric_comp")
        expected = AstropyTable(
            array((
                ("R", "DummyCamComp", "d-r", 1, 423),
                ("R", "DummyCamComp", "d-r", 1, 424)
            )),
            names=("band", "instrument", "physical_filter", "visit_system", "visit")
        )
        self.assertAstropyTablesEqual(res, expected)

        # Verify the new dataset is found in the "foo" collection.
        res = self._queryDataIds(repo=self.root, dimensions=("visit",), collections=("foo",),
                                 datasets="test_metric_comp")
        expected = AstropyTable(
            array((
                ("R", "DummyCamComp", "d-r", 1, 425),
            )),
            names=("band", "instrument", "physical_filter", "visit_system", "visit")
        )
        self.assertAstropyTablesEqual(res, expected)


if __name__ == "__main__":
    unittest.main()
