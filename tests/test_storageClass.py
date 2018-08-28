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

from lsst.daf.butler import StorageClass, StorageClassFactory, StorageClassConfig, CompositeAssembler

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
        sc = StorageClass(className, pytype=dict)
        self.assertIsInstance(sc, StorageClass)
        self.assertEqual(sc.name, className)
        self.assertFalse(sc.components)
        self.assertTrue(sc.validateInstance({}))
        self.assertFalse(sc.validateInstance(""))

        # Ensure we do not have an assembler
        with self.assertRaises(TypeError):
            sc.assembler()

        # Allow no definition of python type
        scn = StorageClass(className)
        self.assertIs(scn.pytype, object)

        # Include some components
        scc = StorageClass(className, pytype=PythonType, components={"comp1": sc})
        self.assertIn("comp1", scc.components)
        self.assertIn("comp1", repr(scc))

        # Ensure that we have an assembler
        self.assertIsInstance(scc.assembler(), CompositeAssembler)

        # Check we can create a storageClass using the name of an importable
        # type.
        sc2 = StorageClass("TestImage2",
                           "lsst.daf.butler.core.storageClass.StorageClassFactory")
        self.assertIsInstance(sc2.pytype(), StorageClassFactory)
        self.assertIn("butler.core", repr(sc2))

    def testEquality(self):
        """Test that StorageClass equality works"""
        className = "TestImage"
        sc1 = StorageClass(className, pytype=dict)
        sc2 = StorageClass(className, pytype=dict)
        self.assertEqual(sc1, sc2)
        sc3 = StorageClass(className + "2", pytype=str)
        self.assertNotEqual(sc1, sc3)

        # Same StorageClass name but different python type
        sc4 = StorageClass(className, pytype=str)
        self.assertNotEqual(sc1, sc4)

        # Now with components
        sc5 = StorageClass("Composite", pytype=PythonType,
                           components={"comp1": sc1, "comp2": sc3})
        sc6 = StorageClass("Composite", pytype=PythonType,
                           components={"comp1": sc1, "comp2": sc3})
        self.assertEqual(sc5, sc6)
        self.assertNotEqual(sc5, sc3)
        sc7 = StorageClass("Composite", pytype=PythonType,
                           components={"comp1": sc4, "comp2": sc3})
        self.assertNotEqual(sc5, sc7)
        sc8 = StorageClass("Composite", pytype=PythonType,
                           components={"comp2": sc3})
        self.assertNotEqual(sc5, sc8)
        sc9 = StorageClass("Composite", pytype=PythonType,
                           components={"comp2": sc3}, assembler="lsst.daf.butler.Butler")
        self.assertNotEqual(sc5, sc9)

    def testRegistry(self):
        """Check that storage classes can be created on the fly and stored
        in a registry."""
        className = "TestImage"
        factory = StorageClassFactory()
        newclass = StorageClass(className, pytype=PythonType)
        factory.registerStorageClass(newclass)
        sc = factory.getStorageClass(className)
        self.assertIsInstance(sc, StorageClass)
        self.assertEqual(sc.name, className)
        self.assertFalse(sc.components)
        self.assertEqual(sc.pytype, PythonType)
        self.assertIn(sc, factory)
        newclass2 = StorageClass("Temporary2", pytype=str)
        self.assertNotIn(newclass2, factory)
        factory.registerStorageClass(newclass2)
        self.assertIn(newclass2, factory)
        self.assertIn("Temporary2", factory)
        self.assertNotIn("Temporary3", factory)
        self.assertNotIn({}, factory)

        # Make sure we can't register a storage class with the same name
        # but different values
        newclass3 = StorageClass("Temporary2", pytype=dict)
        with self.assertRaises(ValueError):
            factory.registerStorageClass(newclass3)

        factory._unregisterStorageClass(newclass3.name)
        self.assertNotIn(newclass3, factory)
        self.assertNotIn(newclass3.name, factory)
        factory.registerStorageClass(newclass3)
        self.assertIn(newclass3, factory)
        self.assertIn(newclass3.name, factory)

        # Check you can silently insert something that is already there
        factory.registerStorageClass(newclass3)

    def testFactoryConfig(self):
        factory = StorageClassFactory()
        factory.addFromConfig(StorageClassConfig())
        image = factory.getStorageClass("Image")
        imageF = factory.getStorageClass("ImageF")
        self.assertIsInstance(imageF, type(image))
        self.assertNotEqual(imageF, image)

        # Check component inheritance
        exposure = factory.getStorageClass("Exposure")
        exposureF = factory.getStorageClass("ExposureF")
        self.assertIsInstance(exposureF, type(exposure))
        self.assertIsInstance(exposure.components["image"], type(image))
        self.assertNotIsInstance(exposure.components["image"], type(imageF))
        self.assertIsInstance(exposureF.components["image"], type(image))
        self.assertIsInstance(exposureF.components["image"], type(imageF))
        self.assertIn("wcs", exposure.components)
        self.assertIn("wcs", exposureF.components)

        # Check that we can't have a new StorageClass that does not
        # inherit from StorageClass
        with self.assertRaises(ValueError):
            factory.makeNewStorageClass("ClassName", baseClass=StorageClassFactory)

        sc = factory.makeNewStorageClass("ClassName")
        self.assertIsInstance(sc(), StorageClass)

    def testPickle(self):
        """Test that we can pickle storageclasses.
        """
        className = "TestImage"
        sc = StorageClass(className, pytype=dict)
        self.assertIsInstance(sc, StorageClass)
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
