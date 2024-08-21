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

"""Unit tests for daf_butler CLI query-collections command.
"""

import os
import unittest

from astropy.table import Table as AstropyTable
from lsst.daf.butler import Butler, DatasetType, script
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.tests.utils import ButlerTestHelper, makeTestTempDir, removeTestTempDir
from numpy import array

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QueryDataIdsTest(unittest.TestCase, ButlerTestHelper):
    """Test the query-data-ids command-line."""

    mockFuncName = "lsst.daf.butler.cli.cmd.commands.script.queryDataIds"

    @staticmethod
    def _queryDataIds(repo, dimensions=(), collections=(), datasets=None, where=""):
        """Call script.queryDataIds, allowing for default values."""
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
        self.repo = Butler.makeRepo(self.root)

    def tearDown(self):
        removeTestTempDir(self.root)

    def loadData(self, *filenames: str) -> Butler:
        """Load registry test data from ``TESTDIR/data/registry/<filename>``,
        which should be a YAML import/export file.
        """
        butler = Butler.from_config(self.repo, writeable=True)
        assert isinstance(butler, DirectButler), "Test expects DirectButler"
        for filename in filenames:
            butler.import_(
                filename=os.path.join(TESTDIR, "data", "registry", filename),
                without_datastore=True,
            )
        return butler

    def testDimensions(self):
        """Test getting a dimension."""
        self.loadData("base.yaml")
        res, msg = self._queryDataIds(self.root, dimensions=("detector",))
        expected = AstropyTable(
            array((("Cam1", 1), ("Cam1", 2), ("Cam1", 3), ("Cam1", 4))), names=("instrument", "detector")
        )
        self.assertFalse(msg)
        self.assertAstropyTablesEqual(res, expected)

    def testNoDimensions(self):
        """Test asking for no dimensions."""
        res, msg = self._queryDataIds(self.root)
        self.assertIsNone(res, msg)
        self.assertEqual(
            msg, "Result has one logical row but no columns because no dimensions were requested."
        )

    def testNoResultsEasy(self):
        """Test getting no results in a way that's detectable without having
        to execute the full query.
        """
        self.loadData("base.yaml", "spatial.yaml")
        res, msg = self._queryDataIds(
            self.root,
            dimensions=("visit", "tract"),
            where="instrument='Cam1' AND skymap='SkyMap1' AND visit=1 AND tract=1",
        )
        self.assertIsNone(res, msg)
        self.assertEqual(msg, "")

    def testNoResultsHard(self):
        """Test getting no results in a way that can't be detected unless we
        run the whole query.
        """
        self.loadData("base.yaml", "spatial.yaml")
        res, msg = self._queryDataIds(
            self.root,
            dimensions=("visit", "tract"),
            where="instrument='Cam1' AND skymap='SkyMap1' AND visit=1 AND tract=0 AND patch=5",
        )
        self.assertIsNone(res, msg)
        self.assertIn("Post-query region filtering removed all rows", msg)

    def testWhere(self):
        """Test with a WHERE constraint."""
        self.loadData("base.yaml")
        res, msg = self._queryDataIds(
            self.root, dimensions=("detector",), where="instrument='Cam1' AND detector=2"
        )
        expected = AstropyTable(
            array((("Cam1", 2),)),
            names=(
                "instrument",
                "detector",
            ),
        )
        self.assertAstropyTablesEqual(res, expected)
        self.assertIsNone(msg)

    def testDatasetsAndCollections(self):
        """Test constraining via datasets and collections."""
        butler = self.loadData("base.yaml", "datasets.yaml")
        # See that the data IDs returned are constrained by that collection's
        # contents.
        res, msg = self._queryDataIds(
            repo=self.root, dimensions=("detector",), collections=("imported_g",), datasets="bias"
        )
        expected = AstropyTable(
            array((("Cam1", 1), ("Cam1", 2), ("Cam1", 3))),
            names=(
                "instrument",
                "detector",
            ),
        )
        self.assertAstropyTablesEqual(res, expected)
        self.assertIsNone(msg)

        # Check that the dimensions are inferred when not provided.
        with self.assertLogs("lsst.daf.butler.script.queryDataIds", "INFO") as cm:
            res, msg = self._queryDataIds(repo=self.root, collections=("imported_g",), datasets="bias")
        self.assertIn("Determined dimensions", "\n".join(cm.output))
        self.assertAstropyTablesEqual(res, expected)
        self.assertIsNone(msg)

        # Check that we get a reason if no dimensions can be inferred.
        new_dataset_type = DatasetType(
            "test_metric_dimensionless",
            (),
            "StructuredDataDict",
            universe=butler.dimensions,
        )
        butler.registry.registerDatasetType(new_dataset_type)
        res, msg = self._queryDataIds(repo=self.root, collections=("imported_g",), datasets=...)
        self.assertIsNone(res)
        self.assertIn("No dimensions in common", msg)

        # Check that we get a reason returned if no dataset is found in
        # collection.
        res, msg = self._queryDataIds(
            repo=self.root,
            dimensions=("detector",),
            collections=("imported_g",),
            datasets="test_metric_dimensionless",
        )
        self.assertIsNone(res)
        self.assertIn("No datasets of type 'test_metric_dimensionless'", msg)


if __name__ == "__main__":
    unittest.main()
