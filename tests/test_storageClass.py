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

import pickle
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
        sc = storageClass.StorageClass(className, pytype=dict)
        self.assertIsInstance(sc, storageClass.StorageClass)
        self.assertEqual(sc.name, className)
        self.assertFalse(sc.components)
        self.assertTrue(sc.validateInstance({}))
        self.assertFalse(sc.validateInstance(""))

        with self.assertRaises(ValueError):
            storageClass.StorageClass(className)

        # Include some components
        scc = storageClass.StorageClass(className, pytype=PythonType, components={"comp1": sc})
        self.assertIn("comp1", scc.components)
        self.assertIn("comp1", repr(scc))

        # Check we can create a storageClass using the name of an importable
        # type.
        sc2 = storageClass.StorageClass("TestImage2",
                                        "lsst.daf.butler.core.storageClass.StorageClassFactory")
        self.assertIsInstance(sc2.pytype(), storageClass.StorageClassFactory)
        self.assertIn("butler.core", repr(sc2))

    def testEquality(self):
        """Test that StorageClass equality works"""
        className = "TestImage"
        sc1 = storageClass.StorageClass(className, pytype=dict)
        sc2 = storageClass.StorageClass(className, pytype=dict)
        self.assertEqual(sc1, sc2)
        sc3 = storageClass.StorageClass(className + "2", pytype=str)
        self.assertNotEqual(sc1, sc3)

        # Same StorageClass name but different python type
        sc4 = storageClass.StorageClass(className, pytype=str)
        self.assertNotEqual(sc1, sc4)

        # Now with components
        sc5 = storageClass.StorageClass("Composite", pytype=PythonType,
                                        components={"comp1": sc1, "comp2": sc3})
        sc6 = storageClass.StorageClass("Composite", pytype=PythonType,
                                        components={"comp1": sc1, "comp2": sc3})
        self.assertEqual(sc5, sc6)
        self.assertNotEqual(sc5, sc3)
        sc7 = storageClass.StorageClass("Composite", pytype=PythonType,
                                        components={"comp1": sc4, "comp2": sc3})
        self.assertNotEqual(sc5, sc7)
        sc8 = storageClass.StorageClass("Composite", pytype=PythonType,
                                        components={"comp2": sc3})
        self.assertNotEqual(sc5, sc8)
        sc9 = storageClass.StorageClass("Composite", pytype=PythonType,
                                        components={"comp2": sc3}, assembler="lsst.daf.butler.Butler")
        self.assertNotEqual(sc5, sc9)

    def testRegistry(self):
        """Check that storage classes can be created on the fly and stored
        in a registry."""
        className = "TestImage"
        factory = storageClass.StorageClassFactory()
        newclass = storageClass.StorageClass(className, pytype=PythonType)
        factory.registerStorageClass(newclass)
        sc = factory.getStorageClass(className)
        self.assertIsInstance(sc, storageClass.StorageClass)
        self.assertEqual(sc.name, className)
        self.assertFalse(sc.components)
        self.assertEqual(sc.pytype, PythonType)
        self.assertIn(sc, factory)
        newclass2 = storageClass.StorageClass("Temporary2", pytype=str)
        self.assertNotIn(newclass2, factory)
        factory.registerStorageClass(newclass2)
        self.assertIn(newclass2, factory)
        self.assertIn("Temporary2", factory)
        self.assertNotIn("Temporary3", factory)
        self.assertNotIn({}, factory)

        # Make sure we can't register a storage class with the same name
        # but different values
        newclass3 = storageClass.StorageClass("Temporary2", pytype=dict)
        with self.assertRaises(ValueError):
            factory.registerStorageClass(newclass3)

        factory.unregisterStorageClass(newclass3.name)
        self.assertNotIn(newclass3, factory)
        self.assertNotIn(newclass3.name, factory)
        factory.registerStorageClass(newclass3)
        self.assertIn(newclass3, factory)
        self.assertIn(newclass3.name, factory)

        # Check you can silently insert something that is already there
        factory.registerStorageClass(newclass3)

    def testPickle(self):
        """Test that we can pickle storageclasses.
        """
        className = "TestImage"
        sc = storageClass.StorageClass(className, pytype=dict)
        self.assertIsInstance(sc, storageClass.StorageClass)
        self.assertEqual(sc.name, className)
        self.assertFalse(sc.components)
        sc2 = pickle.loads(pickle.dumps(sc))
        self.assertEqual(sc2, sc)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
