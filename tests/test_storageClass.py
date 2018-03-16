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

import lsst.daf.butler.core.storageClass as storageClass

"""Tests related to the StorageClass infrastructure.
"""


class PythonType:
    """A dummy class to test the registry of Python types."""
    pass


class StorageClassFactoryTestCase(lsst.utils.tests.TestCase):
    """Tests of the storage class infrastructure.
    """

    def testCreation(self):
        """Test that we can dynamically create storage class subclasses.

        This is critical for testing the factory functions."""
        className = "TestImage"
        newclass = storageClass.makeNewStorageClass(className, pytype=dict)
        sc = newclass()
        self.assertIsInstance(sc, storageClass.StorageClass)
        self.assertEqual(sc.name, className)
        self.assertFalse(sc.components)

        # Test the caching by using private class attribute
        self.assertIsNone(newclass._pytype)
        self.assertEqual(sc.pytype, dict)
        self.assertEqual(newclass._pytype, dict)

        # Check we can create a storageClass using the name of an importable
        # type.
        newclass = storageClass.makeNewStorageClass("TestImage2",
                                                    "lsst.daf.butler.core.storageClass.StorageClassFactory")
        self.assertIsInstance(newclass().pytype(), storageClass.StorageClassFactory)

    def testRegistry(self):
        """Check that storage classes can be created on the fly and stored
        in a registry."""
        className = "TestImage"
        factory = storageClass.StorageClassFactory()
        newclass = storageClass.makeNewStorageClass(className, pytype=PythonType)
        factory.registerStorageClass(newclass)
        sc = factory.getStorageClass(className)
        self.assertIsInstance(sc, storageClass.StorageClass)
        self.assertEqual(sc.name, className)
        self.assertFalse(sc.components)
        self.assertEqual(sc.pytype, PythonType)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
