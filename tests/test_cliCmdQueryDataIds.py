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

import os
import unittest

from astropy.table import Table as AstropyTable
from lsst.daf.butler import Butler, DatasetType, script
from lsst.daf.butler.tests.utils import ButlerTestHelper, MetricTestRepo, makeTestTempDir, removeTestTempDir
from numpy import array

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QueryDataIdsTest(unittest.TestCase, ButlerTestHelper):
    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.queryDataIds"

    @staticmethod
    def _queryDataIds(repo, dimensions=(), collections=(), datasets=None, where=""):
        """Helper to populate the call to script.queryDataIds with default
        values."""
        return script.queryDataIds(
            repo=repo,
            dimensions=dimensions,
            collections=collections,
            datasets=datasets,
            where=where,
            order_by=None,
            limit=0,
            offset=0,
        )

    def setUp(self):
        self.root = makeTestTempDir(TESTDIR)
        self.repo = MetricTestRepo(
            root=self.root, configFile=os.path.join(TESTDIR, "config/basic/butler.yaml")
        )

    def tearDown(self):
        removeTestTempDir(self.root)

    def testDimensions(self):
        """Test getting a dimension."""
        res, msg = self._queryDataIds(self.root, dimensions=("visit",))
        expected = AstropyTable(
            array((("R", "DummyCamComp", "d-r", 423), ("R", "DummyCamComp", "d-r", 424))),
            names=("band", "instrument", "physical_filter", "visit"),
        )
        self.assertFalse(msg)
        self.assertAstropyTablesEqual(res, expected)

    def testNull(self):
        "Test asking for nothing."
        res, msg = self._queryDataIds(self.root)
        self.assertIsNone(res, msg)
        self.assertEqual(msg, "")

    def testWhere(self):
        """Test with a WHERE constraint."""
        res, msg = self._queryDataIds(
            self.root, dimensions=("visit",), where="instrument='DummyCamComp' AND visit=423"
        )
        expected = AstropyTable(
            array((("R", "DummyCamComp", "d-r", 423),)),
            names=("band", "instrument", "physical_filter", "visit"),
        )
        self.assertAstropyTablesEqual(res, expected)
        self.assertIsNone(msg)

    def testDatasetsAndCollections(self):
        """Test constraining via datasets and collections."""

        # Add a dataset in a different collection
        self.butler = Butler(self.root, run="foo")
        self.repo.butler.registry.insertDimensionData(
            "visit",
            {
                "instrument": "DummyCamComp",
                "id": 425,
                "name": "fourtwentyfive",
                "physical_filter": "d-r",
            },
        )
        self.repo.addDataset(dataId={"instrument": "DummyCamComp", "visit": 425}, run="foo")

        # Verify the new dataset is not found in the "ingest/run" collection.
        res, msg = self._queryDataIds(
            repo=self.root, dimensions=("visit",), collections=("ingest/run",), datasets="test_metric_comp"
        )
        expected = AstropyTable(
            array((("R", "DummyCamComp", "d-r", 423), ("R", "DummyCamComp", "d-r", 424))),
            names=("band", "instrument", "physical_filter", "visit"),
        )
        self.assertAstropyTablesEqual(res, expected)
        self.assertIsNone(msg)

        # Verify the new dataset is found in the "foo" collection.
        res, msg = self._queryDataIds(
            repo=self.root, dimensions=("visit",), collections=("foo",), datasets="test_metric_comp"
        )
        expected = AstropyTable(
            array((("R", "DummyCamComp", "d-r", 425),)),
            names=("band", "instrument", "physical_filter", "visit"),
        )
        self.assertAstropyTablesEqual(res, expected)
        self.assertIsNone(msg)

        # Verify the new dataset is found in the "foo" collection and the
        # dimensions are determined automatically.
        with self.assertLogs("lsst.daf.butler.script.queryDataIds", "INFO") as cm:
            res, msg = self._queryDataIds(repo=self.root, collections=("foo",), datasets="test_metric_comp")
        self.assertIn("Determined dimensions", "\n".join(cm.output))
        expected = AstropyTable(
            array((("R", "DummyCamComp", "d-r", 425),)),
            names=("band", "instrument", "physical_filter", "visit"),
        )
        self.assertAstropyTablesEqual(res, expected)
        self.assertIsNone(msg)

        # Check that we get a reason if no dimensions can be inferred.
        new_dataset_type = DatasetType(
            "test_metric_dimensionless",
            (),
            "StructuredDataDict",
            universe=self.repo.butler.registry.dimensions,
        )
        self.repo.butler.registry.registerDatasetType(new_dataset_type)
        res, msg = self._queryDataIds(repo=self.root, collections=("foo",), datasets=...)
        self.assertIsNone(res)
        self.assertIn("No dimensions in common", msg)

        # Check that we get a reason returned if no dataset type is found.
        with self.assertWarns(FutureWarning):
            res, msg = self._queryDataIds(
                repo=self.root, dimensions=("visit",), collections=("foo",), datasets="raw"
            )
        self.assertIsNone(res)
        self.assertEqual(msg, "Dataset type raw is not registered.")

        # Check that we get a reason returned if no dataset is found in
        # collection.
        res, msg = self._queryDataIds(
            repo=self.root, dimensions=("visit",), collections=("ingest",), datasets="test_metric_comp"
        )
        self.assertIsNone(res)
        self.assertIn("No datasets of type test_metric_comp", msg)


if __name__ == "__main__":
    unittest.main()
