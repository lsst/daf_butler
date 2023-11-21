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

"""Tests related to the formatter infrastructure.
"""

import inspect
import os.path
import unittest

from lsst.daf.butler import (
    Config,
    DataCoordinate,
    DatasetType,
    DimensionUniverse,
    FileDescriptor,
    Formatter,
    FormatterFactory,
    Location,
    StorageClass,
)
from lsst.daf.butler.tests import DatasetTestHelper
from lsst.daf.butler.tests.testFormatters import (
    DoNothingFormatter,
    MultipleExtensionsFormatter,
    SingleExtensionFormatter,
)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class FormatterFactoryTestCase(unittest.TestCase, DatasetTestHelper):
    """Tests of the formatter factory infrastructure."""

    def setUp(self):
        self.id = 0
        self.factory = FormatterFactory()
        self.universe = DimensionUniverse()
        self.dataId = DataCoordinate.make_empty(self.universe)

        # Dummy FileDescriptor for testing getFormatter
        self.fileDescriptor = FileDescriptor(
            Location("/a/b/c", "d"), StorageClass("DummyStorageClass", dict, None)
        )

    def assertIsFormatter(self, formatter):
        """Check that the supplied parameter is either a Formatter instance
        or Formatter class.
        """
        if inspect.isclass(formatter):
            self.assertTrue(issubclass(formatter, Formatter), f"Is {formatter} a Formatter")
        else:
            self.assertIsInstance(formatter, Formatter)

    def testFormatter(self):
        """Check basic parameter exceptions"""
        f = DoNothingFormatter(self.fileDescriptor, self.dataId)
        self.assertEqual(f.writeRecipes, {})
        self.assertEqual(f.writeParameters, {})
        self.assertIn("DoNothingFormatter", repr(f))

        with self.assertRaises(TypeError):
            DoNothingFormatter()

        with self.assertRaises(ValueError):
            DoNothingFormatter(self.fileDescriptor, self.dataId, writeParameters={"param1": 0})

        with self.assertRaises(RuntimeError):
            DoNothingFormatter(self.fileDescriptor, self.dataId, writeRecipes={"label": "value"})

        with self.assertRaises(NotImplementedError):
            f.makeUpdatedLocation(Location("a", "b"))

        with self.assertRaises(NotImplementedError):
            f.write("str")

    def testExtensionValidation(self):
        """Test extension validation"""
        for file, single_ok, multi_ok in (
            ("e.fits", True, True),
            ("e.fit", False, True),
            ("e.fits.fz", False, True),
            ("e.txt", False, False),
            ("e.1.4.fits", True, True),
            ("e.3.fit", False, True),
            ("e.1.4.fits.gz", False, True),
        ):
            loc = Location("/a/b/c", file)

            for formatter, passes in (
                (SingleExtensionFormatter, single_ok),
                (MultipleExtensionsFormatter, multi_ok),
            ):
                if passes:
                    formatter.validateExtension(loc)
                else:
                    with self.assertRaises(ValueError):
                        formatter.validateExtension(loc)

    def testRegistry(self):
        """Check that formatters can be stored in the registry."""
        formatterTypeName = "lsst.daf.butler.tests.deferredFormatter.DeferredFormatter"
        storageClassName = "Image"
        self.factory.registerFormatter(storageClassName, formatterTypeName)
        f = self.factory.getFormatter(storageClassName, self.fileDescriptor, self.dataId)
        self.assertIsFormatter(f)
        self.assertEqual(f.name(), formatterTypeName)
        self.assertIn(formatterTypeName, str(f))
        self.assertIn(self.fileDescriptor.location.path, str(f))

        fcls = self.factory.getFormatterClass(storageClassName)
        self.assertIsFormatter(fcls)
        # Defer the import so that we ensure that the infrastructure loaded
        # it on demand previously
        from lsst.daf.butler.tests.deferredFormatter import DeferredFormatter

        self.assertEqual(type(f), DeferredFormatter)

        with self.assertRaises(TypeError):
            # Requires a constructor parameter
            self.factory.getFormatter(storageClassName)

        with self.assertRaises(KeyError):
            self.factory.getFormatter("Missing", self.fileDescriptor)

        # Check that a bad formatter path fails
        storageClassName = "BadImage"
        self.factory.registerFormatter(storageClassName, "lsst.daf.butler.tests.deferredFormatter.Unknown")
        with self.assertRaises(ImportError):
            self.factory.getFormatter(storageClassName, self.fileDescriptor, self.dataId)

    def testRegistryWithStorageClass(self):
        """Test that the registry can be given a StorageClass object."""
        formatterTypeName = "lsst.daf.butler.formatters.yaml.YamlFormatter"
        storageClassName = "TestClass"
        sc = StorageClass(storageClassName, dict, None)

        datasetType = DatasetType("calexp", self.universe.empty, sc)

        # Store using an instance
        self.factory.registerFormatter(sc, formatterTypeName)

        # Retrieve using the class
        f = self.factory.getFormatter(sc, self.fileDescriptor, self.dataId)
        self.assertIsFormatter(f)
        self.assertEqual(f.fileDescriptor, self.fileDescriptor)

        # Retrieve using the DatasetType
        f2 = self.factory.getFormatter(datasetType, self.fileDescriptor, self.dataId)
        self.assertIsFormatter(f2)
        self.assertEqual(f.name(), f2.name())

        # Class directly
        f2cls = self.factory.getFormatterClass(datasetType)
        self.assertIsFormatter(f2cls)

        # This might defer the import, pytest may have already loaded it
        from lsst.daf.butler.formatters.yaml import YamlFormatter

        self.assertEqual(type(f), YamlFormatter)

        with self.assertRaises(KeyError):
            # Attempt to overwrite using a different value
            self.factory.registerFormatter(storageClassName, "lsst.daf.butler.formatters.json.JsonFormatter")

    def testRegistryConfig(self):
        configFile = os.path.join(TESTDIR, "config", "basic", "posixDatastore.yaml")
        config = Config(configFile)
        self.factory.registerFormatters(config["datastore", "formatters"], universe=self.universe)

        # Create a DatasetRef with and without instrument matching the
        # one in the config file.
        dimensions = self.universe.conform(("visit", "physical_filter", "instrument"))
        constant_dataId = {"physical_filter": "v", "visit": 1}
        sc = StorageClass("DummySC", dict, None)
        refPviHsc = self.makeDatasetRef(
            "pvi",
            dimensions,
            sc,
            {"instrument": "DummyHSC", **constant_dataId},
        )
        refPviHscFmt = self.factory.getFormatterClass(refPviHsc)
        self.assertIsFormatter(refPviHscFmt)
        self.assertIn("JsonFormatter", refPviHscFmt.name())

        refPviNotHsc = self.makeDatasetRef(
            "pvi",
            dimensions,
            sc,
            {"instrument": "DummyNotHSC", **constant_dataId},
        )
        refPviNotHscFmt = self.factory.getFormatterClass(refPviNotHsc)
        self.assertIsFormatter(refPviNotHscFmt)
        self.assertIn("PickleFormatter", refPviNotHscFmt.name())

        # Create a DatasetRef that should fall back to using Dimensions
        refPvixHsc = self.makeDatasetRef(
            "pvix",
            dimensions,
            sc,
            {"instrument": "DummyHSC", **constant_dataId},
        )
        refPvixNotHscFmt = self.factory.getFormatterClass(refPvixHsc)
        self.assertIsFormatter(refPvixNotHscFmt)
        self.assertIn("PickleFormatter", refPvixNotHscFmt.name())

        # Create a DatasetRef that should fall back to using StorageClass
        dimensionsNoV = self.universe.conform(("physical_filter", "instrument"))
        refPvixNotHscDims = self.makeDatasetRef(
            "pvix",
            dimensionsNoV,
            sc,
            {"instrument": "DummyHSC", "physical_filter": "v"},
        )
        refPvixNotHscDims_fmt = self.factory.getFormatterClass(refPvixNotHscDims)
        self.assertIsFormatter(refPvixNotHscDims_fmt)
        self.assertIn("YamlFormatter", refPvixNotHscDims_fmt.name())

        # Check that parameters are stored
        refParam = self.makeDatasetRef(
            "paramtest",
            dimensions,
            sc,
            {"instrument": "DummyNotHSC", **constant_dataId},
        )
        lookup, refParam_fmt, kwargs = self.factory.getFormatterClassWithMatch(refParam)
        self.assertIn("writeParameters", kwargs)
        expected = {"max": 5, "min": 2, "comment": "Additional commentary", "recipe": "recipe1"}
        self.assertEqual(kwargs["writeParameters"], expected)
        self.assertIn("FormatterTest", refParam_fmt.name())

        f = self.factory.getFormatter(refParam, self.fileDescriptor, self.dataId)
        self.assertEqual(f.writeParameters, expected)

        f = self.factory.getFormatter(
            refParam, self.fileDescriptor, self.dataId, writeParameters={"min": 22, "extra": 50}
        )
        self.assertEqual(
            f.writeParameters,
            {"max": 5, "min": 22, "comment": "Additional commentary", "extra": 50, "recipe": "recipe1"},
        )

        self.assertIn("recipe1", f.writeRecipes)
        self.assertEqual(f.writeParameters["recipe"], "recipe1")

        with self.assertRaises(ValueError):
            # "new" is not allowed as a write parameter
            self.factory.getFormatter(refParam, self.fileDescriptor, self.dataId, writeParameters={"new": 1})

        with self.assertRaises(RuntimeError):
            # "mode" is a required recipe parameter
            self.factory.getFormatter(
                refParam, self.fileDescriptor, self.dataId, writeRecipes={"recipe3": {"notmode": 1}}
            )


if __name__ == "__main__":
    unittest.main()
