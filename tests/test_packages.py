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

"""Tests for PackagesFormatter."""

import os
import unittest

from lsst.daf.butler import Butler, DatasetType
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.utils.packages import Packages

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class PackagesFormatterTestCase(unittest.TestCase):
    """Tests for PackagesFormatter, using local file datastore."""

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        Butler.makeRepo(self.root)
        self.butler = Butler.from_config(self.root, run="test_run")
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "data", dimensions=(), storageClass="Packages", universe=self.butler.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testPackages(self):
        packages = Packages.fromSystem()
        self.butler.put(packages, self.datasetType, dataId={})

        stored = self.butler.get(self.datasetType, dataId={})
        self.assertEqual(stored, packages)


if __name__ == "__main__":
    unittest.main()
