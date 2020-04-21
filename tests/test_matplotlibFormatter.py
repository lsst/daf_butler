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

"""Tests for MatplotlibFormatter.
"""

import unittest
import tempfile
import os
import shutil

try:
    import matplotlib
    matplotlib.use("Agg")  # noqa:E402
    from matplotlib import pyplot
except ImportError:
    pyplot = None

import numpy as np
from lsst.daf.butler import Butler, DatasetType
import filecmp
import urllib

TESTDIR = os.path.abspath(os.path.dirname(__file__))


@unittest.skipIf(pyplot is None,
                 "skipping test because matplotlib import failed")
class MatplotlibFormatterTestCase(unittest.TestCase):
    """Test for MatplotlibFormatter.
    """

    def setUp(self):
        self.root = tempfile.mkdtemp(dir=TESTDIR)
        Butler.makeRepo(self.root)

    def tearDown(self):
        if os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def testMatplotlibFormatter(self):
        butler = Butler(self.root, run="testrun")
        datasetType = DatasetType("test_plot", [], "Plot",
                                  universe=butler.registry.dimensions)
        butler.registry.registerDatasetType(datasetType)
        pyplot.imshow(np.random.randn(3, 4))
        ref = butler.put(pyplot.gcf(), datasetType)
        parsed = urllib.parse.urlparse(butler.getUri(ref))
        with tempfile.NamedTemporaryFile(suffix=".png") as file:
            pyplot.gcf().savefig(file.name)
            self.assertTrue(
                filecmp.cmp(
                    parsed.path,
                    file.name,
                    shallow=True
                )
            )
        self.assertTrue(butler.datasetExists(ref))
        with self.assertRaises(ValueError):
            butler.get(ref)
        butler.pruneDatasets([ref], unstore=True, purge=True)
        with self.assertRaises(LookupError):
            butler.datasetExists(ref)


if __name__ == "__main__":
    unittest.main()
