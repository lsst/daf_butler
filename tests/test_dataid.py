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

import unittest
import pickle

import lsst.utils
import lsst.utils.tests

from lsst.daf.butler import DataId, DimensionGraph


class DataIdTestCase(lsst.utils.tests.TestCase):
    """Tests for DataId.

    All tests here rely on the content of ``config/dimensions.yaml``, either
    to test that the definitions there are read in properly or just as generic
    data for testing various operations.
    """

    def setUp(self):
        self.universe = DimensionGraph.fromConfig()

    #
    # TODO: more tests would be useful
    #

    def testConstructor(self):
        """Test for making new instances.
        """

        dataId = DataId(dict(instrument="DummyInstrument",
                             detector=1,
                             visit=2,
                             physical_filter="i"),
                        universe=self.universe)
        self.assertEqual(len(dataId), 4)
        self.assertCountEqual(dataId.keys(),
                              ("instrument", "detector", "visit", "physical_filter"))

    def testPickle(self):
        """Test pickle support.
        """
        dataId = DataId(dict(instrument="DummyInstrument",
                             detector=1,
                             visit=2,
                             physical_filter="i"),
                        universe=self.universe)
        dataIdOut = pickle.loads(pickle.dumps(dataId))
        self.assertIsInstance(dataIdOut, DataId)
        self.assertEqual(dataId, dataIdOut)


if __name__ == "__main__":
    unittest.main()
