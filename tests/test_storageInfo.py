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

from lsst.daf.butler.core.storageInfo import StorageInfo

"""Tests for StorageInfo.
"""


class StorageInfoTestCase(lsst.utils.tests.TestCase):
    """Test for StorageInfo.
    """
    def testConstructor(self):
        """Test of constructor.
        """
        datastoreName = "dummy"
        checksum = "d6fb1c0c8f338044b2faaf328f91f707"
        size = 512
        storageInfo = StorageInfo(datastoreName, checksum, size)
        self.assertEqual(storageInfo.datastoreName, datastoreName)
        self.assertEqual(storageInfo.checksum, checksum)
        self.assertEqual(storageInfo.size, size)

    def testEquality(self):
        self.assertEqual(StorageInfo("a", "d6fb1c0c8f338044b2faaf328f91f707", 2),
                         StorageInfo("a", "d6fb1c0c8f338044b2faaf328f91f707", 2))
        self.assertNotEqual(StorageInfo("a", "d6fb1c0c8f338044b2faaf328f91f707", 2),
                            StorageInfo("b", "d6fb1c0c8f338044b2faaf328f91f707", 2))
        self.assertNotEqual(StorageInfo("a", "d6fb1c0c8f338044b2faaf328f91f707", 2),
                            StorageInfo("a", "20a38163c50f4aa3aa0f4047674f8ca7", 2))
        self.assertNotEqual(StorageInfo("a", "d6fb1c0c8f338044b2faaf328f91f707", 2),
                            StorageInfo("a", "d6fb1c0c8f338044b2faaf328f91f707", 3))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
