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

"""Tests related to the formatter infrastructure.
"""

import inspect
import os.path
import unittest

from lsst.daf.butler.tests import DatasetTestHelper
from lsst.daf.butler import (Formatter, FormatterFactory, StorageClass, DatasetType, Config,
                             FileDescriptor, Location, DimensionUniverse, DimensionGraph)
from lsst.daf.butler.tests.testFormatters import DoNothingFormatter

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class FormatterFactoryTestCase(unittest.TestCase, DatasetTestHelper):
    """Tests of the formatter factory infrastructure.
    """

    def setUp(self):
        self.id = 0
        self.factory = FormatterFactory()

        # Dummy FileDescriptor for testing getFormatter
        self.fileDescriptor = FileDescriptor(Location("/a/b/c", "d"),
                                             StorageClass("DummyStorageClass", dict, None))

    def assertIsFormatter(self, formatter):
        """Check that the supplied parameter is either a Formatter instance
        or Formatter class."""

        if inspect.isclass(formatter):
            self.assertTrue(issubclass(formatter, Formatter), f"Is {formatter} a Formatter")
        else:
            self.assertIsInstance(formatter, Formatter)

    def testFormatter(self):
        """Check basic parameter exceptions"""
        f = DoNothingFormatter(self.fileDescriptor)
        self.assertEqual(f.writeRecipes, {})
        self.assertEqual(f.writeParameters, {})
        self.assertIn("DoNothingFormatter", repr(f))

        with self.assertRaises(TypeError):
            DoNothingFormatter()

        with self.assertRaises(ValueError):
            DoNothingFormatter(self.fileDescriptor, writeParameters={"param1": 0})

        with self.assertRaises(RuntimeError):
            DoNothingFormatter(self.fileDescriptor, writeRecipes={"label": "value"})

        with self.assertRaises(NotImplementedError):
            f.makeUpdatedLocation(Location("a", "b"))

        with self.assertRaises(NotImplementedError):
            f.write("str")

    def testRegistry(self):
        """Check that formatters can be stored in the registry.
        """
        formatterTypeName = "lsst.daf.butler.formatters.pexConfigFormatter.PexConfigFormatter"
        storageClassName = "Image"
        self.factory.registerFormatter(storageClassName, formatterTypeName)
        f = self.factory.getFormatter(storageClassName, self.fileDescriptor)
        self.assertIsFormatter(f)
        self.assertEqual(f.name(), formatterTypeName)
        self.assertIn(formatterTypeName, str(f))
        self.assertIn(self.fileDescriptor.location.path, str(f))

        fcls = self.factory.getFormatterClass(storageClassName)
        self.assertIsFormatter(fcls)
        # Defer the import so that we ensure that the infrastructure loaded
        # it on demand previously
        from lsst.daf.butler.formatters.pexConfigFormatter import PexConfigFormatter
        self.assertEqual(type(f), PexConfigFormatter)

        with self.assertRaises(TypeError):
            # Requires a constructor parameter
            self.factory.getFormatter(storageClassName)

        with self.assertRaises(KeyError):
            self.factory.getFormatter("Missing", self.fileDescriptor)

    def testRegistryWithStorageClass(self):
        """Test that the registry can be given a StorageClass object.
        """
        formatterTypeName = "lsst.daf.butler.formatters.yamlFormatter.YamlFormatter"
        storageClassName = "TestClass"
        sc = StorageClass(storageClassName, dict, None)

        universe = DimensionUniverse()
        datasetType = DatasetType("calexp", universe.empty, sc)

        # Store using an instance
        self.factory.registerFormatter(sc, formatterTypeName)

        # Retrieve using the class
        f = self.factory.getFormatter(sc, self.fileDescriptor)
        self.assertIsFormatter(f)
        self.assertEqual(f.fileDescriptor, self.fileDescriptor)

        # Retrieve using the DatasetType
        f2 = self.factory.getFormatter(datasetType, self.fileDescriptor)
        self.assertIsFormatter(f2)
        self.assertEqual(f.name(), f2.name())

        # Class directly
        f2cls = self.factory.getFormatterClass(datasetType)
        self.assertIsFormatter(f2cls)

        # This might defer the import, pytest may have already loaded it
        from lsst.daf.butler.formatters.yamlFormatter import YamlFormatter
        self.assertEqual(type(f), YamlFormatter)

        with self.assertRaises(KeyError):
            # Attempt to overwrite using a different value
            self.factory.registerFormatter(storageClassName,
                                           "lsst.daf.butler.formatters.jsonFormatter.JsonFormatter")

    def testRegistryConfig(self):
        configFile = os.path.join(TESTDIR, "config", "basic", "posixDatastore.yaml")
        config = Config(configFile)
        universe = DimensionUniverse()
        self.factory.registerFormatters(config["datastore", "formatters"], universe=universe)

        # Create a DatasetRef with and without instrument matching the
        # one in the config file.
        dimensions = universe.extract(("visit", "physical_filter", "instrument"))
        sc = StorageClass("DummySC", dict, None)
        refPviHsc = self.makeDatasetRef("pvi", dimensions, sc, {"instrument": "DummyHSC",
                                                                "physical_filter": "v"},
                                        conform=False)
        refPviHscFmt = self.factory.getFormatterClass(refPviHsc)
        self.assertIsFormatter(refPviHscFmt)
        self.assertIn("JsonFormatter", refPviHscFmt.name())

        refPviNotHsc = self.makeDatasetRef("pvi", dimensions, sc, {"instrument": "DummyNotHSC",
                                                                   "physical_filter": "v"},
                                           conform=False)
        refPviNotHscFmt = self.factory.getFormatterClass(refPviNotHsc)
        self.assertIsFormatter(refPviNotHscFmt)
        self.assertIn("PickleFormatter", refPviNotHscFmt.name())

        # Create a DatasetRef that should fall back to using Dimensions
        refPvixHsc = self.makeDatasetRef("pvix", dimensions, sc, {"instrument": "DummyHSC",
                                                                  "physical_filter": "v"},
                                         conform=False)
        refPvixNotHscFmt = self.factory.getFormatterClass(refPvixHsc)
        self.assertIsFormatter(refPvixNotHscFmt)
        self.assertIn("PickleFormatter", refPvixNotHscFmt.name())

        # Create a DatasetRef that should fall back to using StorageClass
        dimensionsNoV = DimensionGraph(universe, names=("physical_filter", "instrument"))
        refPvixNotHscDims = self.makeDatasetRef("pvix", dimensionsNoV, sc, {"instrument": "DummyHSC",
                                                                            "physical_filter": "v"},
                                                conform=False)
        refPvixNotHscDims_fmt = self.factory.getFormatterClass(refPvixNotHscDims)
        self.assertIsFormatter(refPvixNotHscDims_fmt)
        self.assertIn("YamlFormatter", refPvixNotHscDims_fmt.name())

        # Check that parameters are stored
        refParam = self.makeDatasetRef("paramtest", dimensions, sc, {"instrument": "DummyNotHSC",
                                                                     "physical_filter": "v"},
                                       conform=False)
        lookup, refParam_fmt, kwargs = self.factory.getFormatterClassWithMatch(refParam)
        self.assertIn("writeParameters", kwargs)
        expected = {"max": 5, "min": 2, "comment": "Additional commentary"}
        self.assertEqual(kwargs["writeParameters"], expected)
        self.assertIn("FormatterTest", refParam_fmt.name())

        f = self.factory.getFormatter(refParam, self.fileDescriptor)
        self.assertEqual(f.writeParameters, expected)

        f = self.factory.getFormatter(refParam, self.fileDescriptor, writeParameters={"min": 22,
                                                                                      "extra": 50})
        self.assertEqual(f.writeParameters, {"max": 5, "min": 22, "comment": "Additional commentary",
                                             "extra": 50})

        with self.assertRaises(ValueError):
            self.factory.getFormatter(refParam, self.fileDescriptor, writeParameters={"new": 1})


if __name__ == "__main__":
    unittest.main()
