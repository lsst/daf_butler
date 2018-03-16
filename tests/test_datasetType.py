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

import lsst.utils.tests

from lsst.daf.butler.core.datasets import DatasetType

"""Tests for SqlRegistry.
"""


class DatasetTypeTestCase(lsst.utils.tests.TestCase):
    """Test for DatasetType.
    """
    def setUp(self):
        pass

    def testConstructor(self):
        """Test construction preserves values.

        Note that construction doesn't check for valid storageClass or
        dataUnits parameters.
        These can only be verified for a particular schema.
        """
        datasetTypeName = "test"
        storageClass = "StructuredData"
        dataUnits = frozenset(("camera", "visit"))
        datasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        self.assertEqual(datasetType.name, datasetTypeName)
        self.assertEqual(datasetType.storageClass, storageClass)
        self.assertEqual(datasetType.dataUnits, dataUnits)

    def testEquality(self):
        self.assertEqual(DatasetType("a", "StorageA", ("UnitA", )),
                         DatasetType("a", "StorageA", ("UnitA", )))
        self.assertNotEqual(DatasetType("a", "StorageA", ("UnitA", )),
                            DatasetType("b", "StorageA", ("UnitA", )))
        self.assertNotEqual(DatasetType("a", "StorageA", ("UnitA", )),
                            DatasetType("a", "StorageB", ("UnitA", )))
        self.assertNotEqual(DatasetType("a", "StorageA", ("UnitA", )),
                            DatasetType("a", "StorageA", ("UnitB", )))

    def testHashability(self):
        """Test `DatasetType.__hash__`.

        This test is performed by checking that `DatasetType` entries can
        be inserted into a `set` and that unique values of its
        (`name`, `storageClass`, `dataUnits`) parameters result in separate
        entries (and equal ones don't).

        This does not check for uniformity of hashing or the actual values
        of the hash function.
        """
        types = []
        unique = 0
        for name in ["a", "b"]:
            for storageClass in ["c", "d"]:
                for dataUnits in [("e", ), ("f", )]:
                    datasetType = DatasetType(name, storageClass, dataUnits)
                    datasetTypeCopy = DatasetType(name, storageClass, dataUnits)
                    types.extend((datasetType, datasetTypeCopy))
                    unique += 1  # datasetType should always equal its copy
        self.assertEqual(len(set(types)), unique) # all other combinations are unique


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
