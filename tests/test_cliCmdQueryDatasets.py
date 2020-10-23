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
from astropy.utils.diff import report_diff_values
from numpy import array
import io
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
from lsst.daf.butler import script
from lsst.daf.butler.tests import MetricsExample


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QueryDatasetsTest(unittest.TestCase):

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    storageClassFactory = StorageClassFactory()

    def _assertTablesEqual(self, tables, expectedTables):
        """Verify that a list of astropy tables matches a list of expected
        astropy tables."""
        diff = io.StringIO()
        self.assertEqual(len(tables), len(expectedTables))
        for table, expected in zip(tables, expectedTables):
            # Assert that we are testing what we think we are testing
            self.assertIsInstance(table, AstropyTable)
            self.assertIsInstance(expected, AstropyTable)
            self.assertTrue(report_diff_values(table, expected, fileobj=diff), msg=diff.getvalue())

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

    @staticmethod
    def _queryDatasets(repo, glob=(), collections=(), where="", find_first=False, show_uri=False):
        return script.queryDatasets(repo, glob, collections, where, find_first, show_uri)

    def setUp(self):
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

    def testShowURI(self):
        """Test for expected output with show_uri=True."""
        tables = self._queryDatasets(repo=self.butlerConfigFile, show_uri=True)

        expectedTables = (
            AstropyTable(array((
                ("test_metric_comp.data", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                 self.butler.datastore.root.join(
                     "ingest/run/test_metric_comp.data/"
                     "test_metric_comp_v00000423_fDummyCamComp_data.yaml")),
                ("test_metric_comp.data", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                 self.butler.datastore.root.join(
                     "ingest/run/test_metric_comp.data/"
                     "test_metric_comp_v00000424_fDummyCamComp_data.yaml")))),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array((
                ("test_metric_comp.output", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                    self.butler.datastore.root.join(
                        "ingest/run/test_metric_comp.output/"
                        "test_metric_comp_v00000423_fDummyCamComp_output.yaml")),
                ("test_metric_comp.output", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                    self.butler.datastore.root.join(
                        "ingest/run/test_metric_comp.output/"
                        "test_metric_comp_v00000424_fDummyCamComp_output.yaml")))),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array((
                ("test_metric_comp.summary", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                    self.butler.datastore.root.join(
                        "ingest/run/test_metric_comp.summary/"
                        "test_metric_comp_v00000423_fDummyCamComp_summary.yaml")),
                ("test_metric_comp.summary", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                    self.butler.datastore.root.join(
                        "ingest/run/test_metric_comp.summary/"
                        "test_metric_comp_v00000424_fDummyCamComp_summary.yaml")))),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
        )

        self._assertTablesEqual(tables, expectedTables)

    def testNoShowURI(self):
        """Test for expected output without show_uri (default is False)."""
        tables = self._queryDatasets(repo=self.butlerConfigFile)

        expectedTables = (
            AstropyTable(array((
                ("test_metric_comp", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423"),
                ("test_metric_comp", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424"))),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system", "visit")
            ),
        )

        self._assertTablesEqual(tables, expectedTables)

    def testWhere(self):
        """Test using the where clause to reduce the number of rows returned.
        """
        tables = self._queryDatasets(repo=self.butlerConfigFile, where="visit=423")

        expectedTables = (
            AstropyTable(array(
                ("test_metric_comp", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423")),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system", "visit"),
            ),
        )

        self._assertTablesEqual(tables, expectedTables)

    def testGlobDatasetType(self):
        """Test specifying dataset type."""
        # Create and register an additional DatasetType
        dimensions = self.butler.registry.dimensions.extract(["instrument", "visit"])
        datasetTypeName = "alt_test_metric_comp"
        storageClass = self.storageClassFactory.getStorageClass("StructuredCompositeReadComp")
        datasetType = self._addDatasetType(datasetTypeName, dimensions, storageClass, self.butler.registry)
        self.butler.registry.insertDimensionData(
            "visit", {"instrument": "DummyCamComp", "id": 425,
                      "name": "fourtwentyfive", "physical_filter": "d-r",
                      "visit_system": 1})
        metric = self._makeExampleMetrics()
        dataId = {"instrument": "DummyCamComp", "visit": 425}
        ref = DatasetRef(datasetType, dataId, id=None)
        self.butler.put(metric, ref)

        # verify the new dataset type increases the number of tables found:
        tables = self._queryDatasets(repo=self.butlerConfigFile)

        expectedTables = (
            AstropyTable(array((
                ("test_metric_comp", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423"),
                ("test_metric_comp", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424"))),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system", "visit")
            ),
            AstropyTable(array((
                ("alt_test_metric_comp", "ingest/run", "3", "R", "DummyCamComp", "d-r", "1", "425"))),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system", "visit")
            )
        )

        self._assertTablesEqual(tables, expectedTables)

    def testFindFirstAndCollections(self):
        """Test the find-first option, and the collections option, since it
        is required for find-first."""

        # Create a new butler with a new run, and add a dataset to shadow an
        # existing dataset.
        self.butler = Butler(self.root, run="foo")
        metric = self._makeExampleMetrics()
        dataId = {"instrument": "DummyCamComp", "visit": 424}
        ref = DatasetRef(self.datasetType, dataId, id=None)
        self.butler.put(metric, ref)

        # Verify that without find-first, duplicate datasets are returned
        tables = self._queryDatasets(repo=self.butlerConfigFile,
                                     collections=["foo", "ingest/run"],
                                     show_uri=True)

        expectedTables = (
            AstropyTable(array(
                (
                    ("test_metric_comp.data", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.butler.datastore.root.join(
                            "foo/test_metric_comp.data/"
                            "test_metric_comp_v00000424_fDummyCamComp_data.yaml")),
                    ("test_metric_comp.data", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.data/"
                            "test_metric_comp_v00000423_fDummyCamComp_data.yaml")),
                    ("test_metric_comp.data", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                        self.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.data/"
                            "test_metric_comp_v00000424_fDummyCamComp_data.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array(
                (
                    ("test_metric_comp.output", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.butler.datastore.root.join(
                            "foo/test_metric_comp.output/"
                            "test_metric_comp_v00000424_fDummyCamComp_output.yaml")),
                    ("test_metric_comp.output", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.output/"
                            "test_metric_comp_v00000423_fDummyCamComp_output.yaml")),
                    ("test_metric_comp.output", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                        self.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.output/"
                            "test_metric_comp_v00000424_fDummyCamComp_output.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array(
                (
                    ("test_metric_comp.summary", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.butler.datastore.root.join(
                            "foo/test_metric_comp.summary/"
                            "test_metric_comp_v00000424_fDummyCamComp_summary.yaml")),
                    ("test_metric_comp.summary", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.summary/"
                            "test_metric_comp_v00000423_fDummyCamComp_summary.yaml")),
                    ("test_metric_comp.summary", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                        self.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.summary/"
                            "test_metric_comp_v00000424_fDummyCamComp_summary.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
        )

        self._assertTablesEqual(tables, expectedTables)

        # Verify that with find first the duplicate dataset is eliminated and
        # the more recent dataset is returned.
        tables = self._queryDatasets(repo=self.butlerConfigFile,
                                     collections=["foo", "ingest/run"],
                                     show_uri=True,
                                     find_first=True)

        expectedTables = (
            AstropyTable(array(
                (
                    ("test_metric_comp.data", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.butler.datastore.root.join(
                            "foo/test_metric_comp.data/test_metric_comp_v00000424_fDummyCamComp_data.yaml")),
                    ("test_metric_comp.data", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.data/"
                            "test_metric_comp_v00000423_fDummyCamComp_data.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array(
                (
                    ("test_metric_comp.output", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.butler.datastore.root.join(
                            "foo/test_metric_comp.output/"
                            "test_metric_comp_v00000424_fDummyCamComp_output.yaml")),
                    ("test_metric_comp.output", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.output/"
                            "test_metric_comp_v00000423_fDummyCamComp_output.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array(
                (
                    ("test_metric_comp.summary", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.butler.datastore.root.join(
                            "foo/test_metric_comp.summary/"
                            "test_metric_comp_v00000424_fDummyCamComp_summary.yaml")),
                    ("test_metric_comp.summary", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.summary/"
                            "test_metric_comp_v00000423_fDummyCamComp_summary.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
        )

        self._assertTablesEqual(tables, expectedTables)


if __name__ == "__main__":
    unittest.main()
