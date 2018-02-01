#
# LSST Data Management System
#
# Copyright 2018  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

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
        newclass = storageClass.makeNewStorageClass(className, dict, None)
        sc = newclass()
        self.assertIsInstance(sc, storageClass.StorageClass)
        self.assertEqual(sc.name, className)
        self.assertIsNone(sc.components)
        self.assertEqual(sc.type, dict)

        # Check that this class is listed in the subclasses
        self.assertIn(className, newclass.subclasses)

        # Check we can create a storageClass using the name of an importable
        # type.
        newclass = storageClass.makeNewStorageClass("TestImage2",
                                                    "lsst.daf.butler.core.storageClass.StorageClassFactory")
        self.assertIsInstance(newclass.type(), storageClass.StorageClassFactory)

    def testRegistry(self):
        """Check that storage classes can be created on the fly and stored
        in a registry."""
        className = "TestImage"
        factory = storageClass.StorageClassFactory()
        factory.registerStorageClass(className, PythonType)
        sc = factory.getStorageClass(className)
        self.assertIsInstance(sc, storageClass.StorageClass)
        self.assertEqual(sc.name, className)
        self.assertIsNone(sc.components)
        self.assertEqual(sc.type, PythonType)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
