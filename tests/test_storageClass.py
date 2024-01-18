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

import logging
import os
import pickle
import unittest

from lsst.daf.butler import StorageClass, StorageClassConfig, StorageClassDelegate, StorageClassFactory
from lsst.daf.butler.tests import MetricsExample
from lsst.utils.introspection import get_full_type_name

"""Tests related to the StorageClass infrastructure.
"""

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class PythonType:
    """A dummy class to test the registry of Python types."""

    pass


class PythonType2:
    """A dummy class to test the registry of Python types."""

    pass


class PythonType3:
    """A dummy class to test the registry of Python types."""

    pass


class NotCopyable:
    """Class with deep copying disabled."""

    def __deepcopy__(self, memo=None):
        raise RuntimeError("Can not be copied.")


class StorageClassFactoryTestCase(unittest.TestCase):
    """Tests of the storage class infrastructure."""

    def testCreation(self):
        """Test that we can dynamically create storage class subclasses.

        This is critical for testing the factory functions.
        """
        className = "TestImage"
        sc = StorageClass(className, pytype=dict)
        self.assertIsInstance(sc, StorageClass)
        self.assertEqual(sc.name, className)
        self.assertEqual(str(sc), className)
        self.assertFalse(sc.components)
        self.assertTrue(sc.validateInstance({}))
        self.assertFalse(sc.validateInstance(""))

        r = repr(sc)
        self.assertIn("StorageClass", r)
        self.assertIn(className, r)
        self.assertNotIn("parameters", r)
        self.assertIn("pytype='dict'", r)

        # Ensure we do not have a delegate
        with self.assertRaises(TypeError):
            sc.delegate()

        # Allow no definition of python type
        scn = StorageClass(className)
        self.assertIs(scn.pytype, object)

        # Include some components
        scc = StorageClass(className, pytype=PythonType, components={"comp1": sc, "comp2": sc})
        self.assertIn("comp1", scc.components)
        r = repr(scc)
        self.assertIn("comp1", r)
        self.assertIn("lsst.daf.butler.StorageClassDelegate", r)

        # Ensure that we have a delegate
        self.assertIsInstance(scc.delegate(), StorageClassDelegate)

        # Check that delegate copy() works.
        list1 = [1, 2, 3]
        list2 = scc.delegate().copy(list1)
        self.assertEqual(list1, list2)
        list2.append(4)
        self.assertNotEqual(list1, list2)

        with self.assertRaises(NotImplementedError):
            scc.delegate().copy(NotCopyable())

        # Check we can create a storageClass using the name of an importable
        # type.
        sc2 = StorageClass("TestImage2", "lsst.daf.butler.StorageClassFactory")
        self.assertIsInstance(sc2.pytype(), StorageClassFactory)
        self.assertIn("butler", repr(sc2))

    def testParameters(self):
        """Test that we can set parameters and validate them"""
        pt = ("a", "b")
        ps = {"a", "b"}
        pl = ["a", "b"]
        for p in (pt, ps, pl):
            sc1 = StorageClass("ParamClass", pytype=dict, parameters=p)
            self.assertEqual(sc1.parameters, ps)
            sc1.validateParameters(p)

        sc1.validateParameters()
        sc1.validateParameters({"a": None, "b": None})
        sc1.validateParameters(
            [
                "a",
            ]
        )
        with self.assertRaises(KeyError):
            sc1.validateParameters({"a", "c"})

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

        # Parameters
        scp = StorageClass("Params", pytype=PythonType, parameters=["a", "b", "c"])
        scp1 = StorageClass("Params", pytype=PythonType, parameters=["a", "b", "c"])
        scp2 = StorageClass("Params", pytype=PythonType, parameters=["a", "b", "d", "e"])
        self.assertEqual(scp, scp1)
        self.assertNotEqual(scp, scp2)

        # Now with components
        sc5 = StorageClass("Composite", pytype=PythonType, components={"comp1": sc1, "comp2": sc3})
        sc6 = StorageClass("Composite", pytype=PythonType, components={"comp1": sc1, "comp2": sc3})
        self.assertEqual(sc5, sc6)
        self.assertNotEqual(sc5, sc3)
        sc7 = StorageClass("Composite", pytype=PythonType, components={"comp1": sc4, "comp2": sc3})
        self.assertNotEqual(sc5, sc7)
        sc8 = StorageClass("Composite", pytype=PythonType, components={"comp2": sc3, "comp3": sc3})
        self.assertNotEqual(sc5, sc8)
        sc9 = StorageClass(
            "Composite",
            pytype=PythonType,
            components={"comp1": sc1, "comp2": sc3},
            delegate="lsst.daf.butler.Butler",
        )
        self.assertNotEqual(sc5, sc9)

    def testTypeEquality(self):
        sc1 = StorageClass("Something", pytype=dict)
        self.assertTrue(sc1.is_type(dict), repr(sc1))
        self.assertFalse(sc1.is_type(str), repr(sc1))

        sc2 = StorageClass("TestImage2", "lsst.daf.butler.StorageClassFactory")
        self.assertTrue(sc2.is_type(StorageClassFactory), repr(sc2))

    def testRegistry(self):
        """Check that storage classes can be created on the fly and stored
        in a registry.
        """
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
        with self.assertRaises(ValueError) as cm:
            factory.registerStorageClass(newclass3)
        self.assertIn("pytype='dict'", str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            factory.registerStorageClass(newclass3, msg="error string")
        self.assertIn("error string", str(cm.exception))

        factory._unregisterStorageClass(newclass3.name)
        self.assertNotIn(newclass3, factory)
        self.assertNotIn(newclass3.name, factory)
        factory.registerStorageClass(newclass3)
        self.assertIn(newclass3, factory)
        self.assertIn(newclass3.name, factory)

        # Check you can silently insert something that is already there
        factory.registerStorageClass(newclass3)

        # Reset the factory and check that default items are present
        # but the new ones are not.
        factory.reset()
        self.assertNotIn(newclass3.name, factory)
        self.assertIn("StructuredDataDict", factory)

    def testFactoryFind(self):
        # Finding a storage class can involve doing lots of slow imports so
        # this is a separate test.
        factory = StorageClassFactory()
        className = "PythonType3"
        newclass = StorageClass(className, pytype=PythonType3)
        factory.registerStorageClass(newclass)
        sc = factory.getStorageClass(className)

        # Can we find a storage class from a type.
        new_sc = factory.findStorageClass(PythonType3)
        self.assertEqual(new_sc, sc)

        # Now with slow mode
        new_sc = factory.findStorageClass(PythonType3, compare_types=True)
        self.assertEqual(new_sc, sc)

        # This class will never match.
        with self.assertRaises(KeyError):
            factory.findStorageClass(PythonType2, compare_types=True)

        # Check builtins.
        self.assertEqual(factory.findStorageClass(dict), factory.getStorageClass("StructuredDataDict"))

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

        # Check parameters
        factory.addFromConfig(os.path.join(TESTDIR, "config", "basic", "storageClasses.yaml"))
        thing1 = factory.getStorageClass("ThingOne")
        thing2 = factory.getStorageClass("ThingTwo")
        self.assertIsInstance(thing2, type(thing1))
        param1 = thing1.parameters
        param2 = thing2.parameters
        self.assertIn("param3", thing2.parameters)
        self.assertNotIn("param3", thing1.parameters)
        param2.remove("param3")
        self.assertEqual(param1, param2)

        # Check that we can't have a new StorageClass that does not
        # inherit from StorageClass
        with self.assertRaises(ValueError):
            factory.makeNewStorageClass("ClassName", baseClass=StorageClassFactory)

        sc = factory.makeNewStorageClass("ClassName")
        self.assertIsInstance(sc(), StorageClass)

    def testPickle(self):
        """Test that we can pickle storageclasses."""
        className = "TestImage"
        sc = StorageClass(className, pytype=dict)
        self.assertIsInstance(sc, StorageClass)
        self.assertEqual(sc.name, className)
        self.assertFalse(sc.components)
        sc2 = pickle.loads(pickle.dumps(sc))
        self.assertEqual(sc2, sc)

    @classmethod
    def _convert_type(cls, data):
        # Test helper function. Fail if the list is empty.
        if not len(data):
            raise RuntimeError("Deliberate failure.")
        return {"key": data}

    def testConverters(self):
        """Test conversion maps."""
        className = "TestConverters"
        converters = {
            "lsst.daf.butler.tests.MetricsExample": "lsst.daf.butler.tests.MetricsExample.exportAsDict",
            # Add some entries that will fail to import.
            "lsst.daf.butler.bad.type": "lsst.daf.butler.tests.MetricsExampleModel.from_metrics",
            "lsst.daf.butler.tests.MetricsExampleModel": "lsst.daf.butler.bad.function",
            "lsst.daf.butler.Butler": "lsst.daf.butler.location.__all__",
            "list": get_full_type_name(self._convert_type),
        }
        sc = StorageClass(className, pytype=dict, converters=converters)
        self.assertEqual(len(sc.converters), 5)  # Pre-filtering
        sc2 = StorageClass("Test2", pytype=set)
        sc3 = StorageClass("Test3", pytype="lsst.daf.butler.tests.MetricsExample")

        self.assertIn("lsst.daf.butler.tests.MetricsExample", repr(sc))
        # Initially the converter list is not filtered.
        self.assertIn("lsst.daf.butler.bad.type", repr(sc))
        self.assertNotIn("converters", repr(sc2))

        self.assertTrue(sc.can_convert(sc))
        self.assertFalse(sc.can_convert(sc2))
        self.assertTrue(sc.can_convert(sc3))

        # After we've processed the converters the bad ones will no longer
        # be reported.
        self.assertNotIn("lsst.daf.butler.bad.type", repr(sc))
        self.assertEqual(len(sc.converters), 2)

        self.assertIsNone(sc.coerce_type(None))

        converted = sc.coerce_type([1, 2, 3])
        self.assertEqual(converted, {"key": [1, 2, 3]})

        # Convert Metrics using a named method converter.
        metric = MetricsExample(summary={"a": 1}, data=[1, 2], output={"c": "e"})
        converted = sc.coerce_type(metric)
        self.assertEqual(converted["data"], [1, 2], converted)

        # Check that python types matching is allowed.
        sc4 = StorageClass("Test2", pytype=set)
        self.assertTrue(sc2.can_convert(sc4))
        converted = sc2.coerce_type({1, 2})
        self.assertEqual(converted, {1, 2})

        # Try to coerce a type that is not supported.
        with self.assertRaises(TypeError):
            sc.coerce_type({1, 2, 3})

        # Coerce something that will fail to convert.
        with self.assertLogs(level=logging.ERROR) as cm:
            with self.assertRaises(RuntimeError):
                sc.coerce_type([])
        self.assertIn("failed to convert type list", cm.output[0])


if __name__ == "__main__":
    unittest.main()
