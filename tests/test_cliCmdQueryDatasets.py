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

from astropy.table import Table as AstropyTable
from numpy import array
import os
import unittest

from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import script
from lsst.daf.butler.tests import addDatasetType
from lsst.daf.butler.tests.utils import ButlerTestHelper, makeTestTempDir, MetricTestRepo, removeTestTempDir


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QueryDatasetsTest(unittest.TestCase, ButlerTestHelper):

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    storageClassFactory = StorageClassFactory()

    @staticmethod
    def _queryDatasets(repo, glob=(), collections=(), where="", find_first=False, show_uri=False):
        return script.QueryDatasets(repo, glob, collections, where, find_first, show_uri).getTables()

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        self.testRepo = MetricTestRepo(self.root,
                                       configFile=os.path.join(TESTDIR, "config/basic/butler.yaml"))

    def tearDown(self):
        removeTestTempDir(self.root)

    def testShowURI(self):
        """Test for expected output with show_uri=True."""
        tables = self._queryDatasets(repo=self.root, show_uri=True)

        expectedTables = (
            AstropyTable(array((
                ("test_metric_comp.data", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                 self.testRepo.butler.datastore.root.join(
                     "ingest/run/test_metric_comp.data/"
                     "test_metric_comp_v00000423_fDummyCamComp_data.yaml")),
                ("test_metric_comp.data", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                 self.testRepo.butler.datastore.root.join(
                     "ingest/run/test_metric_comp.data/"
                     "test_metric_comp_v00000424_fDummyCamComp_data.yaml")))),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array((
                ("test_metric_comp.output", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                    self.testRepo.butler.datastore.root.join(
                        "ingest/run/test_metric_comp.output/"
                        "test_metric_comp_v00000423_fDummyCamComp_output.yaml")),
                ("test_metric_comp.output", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                    self.testRepo.butler.datastore.root.join(
                        "ingest/run/test_metric_comp.output/"
                        "test_metric_comp_v00000424_fDummyCamComp_output.yaml")))),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array((
                ("test_metric_comp.summary", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                    self.testRepo.butler.datastore.root.join(
                        "ingest/run/test_metric_comp.summary/"
                        "test_metric_comp_v00000423_fDummyCamComp_summary.yaml")),
                ("test_metric_comp.summary", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                    self.testRepo.butler.datastore.root.join(
                        "ingest/run/test_metric_comp.summary/"
                        "test_metric_comp_v00000424_fDummyCamComp_summary.yaml")))),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
        )

        self.assertAstropyTablesEqual(tables, expectedTables)

    def testNoShowURI(self):
        """Test for expected output without show_uri (default is False)."""
        tables = self._queryDatasets(repo=self.root)

        expectedTables = (
            AstropyTable(array((
                ("test_metric_comp", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423"),
                ("test_metric_comp", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424"))),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system", "visit")
            ),
        )

        self.assertAstropyTablesEqual(tables, expectedTables)

    def testWhere(self):
        """Test using the where clause to reduce the number of rows returned.
        """
        tables = self._queryDatasets(repo=self.root, where="instrument='DummyCamComp' AND visit=423")

        expectedTables = (
            AstropyTable(array(
                ("test_metric_comp", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423")),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system", "visit"),
            ),
        )

        self.assertAstropyTablesEqual(tables, expectedTables)

    def testGlobDatasetType(self):
        """Test specifying dataset type."""
        # Create and register an additional DatasetType

        self.testRepo.butler.registry.insertDimensionData("visit",
                                                          {"instrument": "DummyCamComp", "id": 425,
                                                           "name": "fourtwentyfive", "physical_filter": "d-r",
                                                           "visit_system": 1})

        datasetType = addDatasetType(self.testRepo.butler,
                                     "alt_test_metric_comp",
                                     ("instrument", "visit"),
                                     "StructuredCompositeReadComp")

        self.testRepo.addDataset(dataId={"instrument": "DummyCamComp", "visit": 425}, datasetType=datasetType)

        # verify the new dataset type increases the number of tables found:
        tables = self._queryDatasets(repo=self.root)

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

        self.assertAstropyTablesEqual(tables, expectedTables)

    def testFindFirstAndCollections(self):
        """Test the find-first option, and the collections option, since it
        is required for find-first."""

        # Add a new run, and add a dataset to shadow an existing dataset.
        self.testRepo.addDataset(run="foo",
                                 dataId={"instrument": "DummyCamComp", "visit": 424})

        # Verify that without find-first, duplicate datasets are returned
        tables = self._queryDatasets(repo=self.root,
                                     collections=["foo", "ingest/run"],
                                     show_uri=True)

        expectedTables = (
            AstropyTable(array(
                (
                    ("test_metric_comp.data", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.testRepo.butler.datastore.root.join(
                            "foo/test_metric_comp.data/"
                            "test_metric_comp_v00000424_fDummyCamComp_data.yaml")),
                    ("test_metric_comp.data", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.testRepo.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.data/"
                            "test_metric_comp_v00000423_fDummyCamComp_data.yaml")),
                    ("test_metric_comp.data", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                        self.testRepo.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.data/"
                            "test_metric_comp_v00000424_fDummyCamComp_data.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array(
                (
                    ("test_metric_comp.output", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.testRepo.butler.datastore.root.join(
                            "foo/test_metric_comp.output/"
                            "test_metric_comp_v00000424_fDummyCamComp_output.yaml")),
                    ("test_metric_comp.output", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.testRepo.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.output/"
                            "test_metric_comp_v00000423_fDummyCamComp_output.yaml")),
                    ("test_metric_comp.output", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                        self.testRepo.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.output/"
                            "test_metric_comp_v00000424_fDummyCamComp_output.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array(
                (
                    ("test_metric_comp.summary", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.testRepo.butler.datastore.root.join(
                            "foo/test_metric_comp.summary/"
                            "test_metric_comp_v00000424_fDummyCamComp_summary.yaml")),
                    ("test_metric_comp.summary", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.testRepo.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.summary/"
                            "test_metric_comp_v00000423_fDummyCamComp_summary.yaml")),
                    ("test_metric_comp.summary", "ingest/run", "2", "R", "DummyCamComp", "d-r", "1", "424",
                        self.testRepo.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.summary/"
                            "test_metric_comp_v00000424_fDummyCamComp_summary.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
        )

        self.assertAstropyTablesEqual(tables, expectedTables)

        # Verify that with find first the duplicate dataset is eliminated and
        # the more recent dataset is returned.
        tables = self._queryDatasets(repo=self.root,
                                     collections=["foo", "ingest/run"],
                                     show_uri=True,
                                     find_first=True)

        expectedTables = (
            AstropyTable(array(
                (
                    ("test_metric_comp.data", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.testRepo.butler.datastore.root.join(
                            "foo/test_metric_comp.data/test_metric_comp_v00000424_fDummyCamComp_data.yaml")),
                    ("test_metric_comp.data", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.testRepo.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.data/"
                            "test_metric_comp_v00000423_fDummyCamComp_data.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array(
                (
                    ("test_metric_comp.output", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.testRepo.butler.datastore.root.join(
                            "foo/test_metric_comp.output/"
                            "test_metric_comp_v00000424_fDummyCamComp_output.yaml")),
                    ("test_metric_comp.output", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.testRepo.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.output/"
                            "test_metric_comp_v00000423_fDummyCamComp_output.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
            AstropyTable(array(
                (
                    ("test_metric_comp.summary", "foo", "3", "R", "DummyCamComp", "d-r", "1", "424",
                        self.testRepo.butler.datastore.root.join(
                            "foo/test_metric_comp.summary/"
                            "test_metric_comp_v00000424_fDummyCamComp_summary.yaml")),
                    ("test_metric_comp.summary", "ingest/run", "1", "R", "DummyCamComp", "d-r", "1", "423",
                        self.testRepo.butler.datastore.root.join(
                            "ingest/run/test_metric_comp.summary/"
                            "test_metric_comp_v00000423_fDummyCamComp_summary.yaml")),
                )),
                names=("type", "run", "id", "band", "instrument", "physical_filter", "visit_system",
                       "visit", "URI")),
        )

        self.assertAstropyTablesEqual(tables, expectedTables)


if __name__ == "__main__":
    unittest.main()
