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

import os.path
import unittest

from datasetsHelper import DatasetTestHelper
from lsst.daf.butler import Formatter, FormatterFactory, StorageClass, DatasetType, Config

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class FormatterFactoryTestCase(unittest.TestCase, DatasetTestHelper):
    """Tests of the formatter factory infrastructure.
    """

    def setUp(self):
        self.id = 0
        self.factory = FormatterFactory()

    def testRegistry(self):
        """Check that formatters can be stored in the registry.
        """
        formatterTypeName = "lsst.daf.butler.formatters.fitsCatalogFormatter.FitsCatalogFormatter"
        storageClassName = "Image"
        self.factory.registerFormatter(storageClassName, formatterTypeName)
        f = self.factory.getFormatter(storageClassName)
        self.assertIsInstance(f, Formatter)
        # Defer the import so that we ensure that the infrastructure loaded
        # it on demand previously
        from lsst.daf.butler.formatters.fitsCatalogFormatter import FitsCatalogFormatter
        self.assertEqual(type(f), FitsCatalogFormatter)

        with self.assertRaises(KeyError):
            f = self.factory.getFormatter("Missing")

    def testRegistryWithStorageClass(self):
        """Test that the registry can be given a StorageClass object.
        """
        formatterTypeName = "lsst.daf.butler.formatters.yamlFormatter.YamlFormatter"
        storageClassName = "TestClass"
        sc = StorageClass(storageClassName, dict, None)

        datasetType = DatasetType("calexp", {}, sc)

        # Store using an instance
        self.factory.registerFormatter(sc, formatterTypeName)

        # Retrieve using the class
        f = self.factory.getFormatter(sc)
        self.assertIsInstance(f, Formatter)

        # Retrieve using the DatasetType
        f2 = self.factory.getFormatter(datasetType)
        self.assertIsInstance(f, Formatter)
        self.assertEqual(f.name(), f2.name())

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
        self.factory.registerFormatters(config["datastore", "formatters"])

        # Create a DatasetRef with and without instrument matching the
        # one in the config file.
        dimensions = frozenset(("Visit", "PhysicalFilter", "Instrument"))
        sc = StorageClass("DummySC", dict, None)
        refPviHsc = self.makeDatasetRef("pvi", dimensions, sc, {"instrument": "DummyHSC",
                                                                "physical_filter": "v"})
        refPviHscFmt = self.factory.getFormatter(refPviHsc)
        self.assertIsInstance(refPviHscFmt, Formatter)
        self.assertIn("JsonFormatter", refPviHscFmt.name())

        refPviNotHsc = self.makeDatasetRef("pvi", dimensions, sc, {"instrument": "DummyNotHSC",
                                                                   "physical_filter": "v"})
        refPviNotHscFmt = self.factory.getFormatter(refPviNotHsc)
        self.assertIsInstance(refPviNotHscFmt, Formatter)
        self.assertIn("PickleFormatter", refPviNotHscFmt.name())

        # Create a DatasetRef that should fall back to using Dimensions
        refPvixHsc = self.makeDatasetRef("pvix", dimensions, sc, {"instrument": "DummyHSC",
                                                                  "physical_filter": "v"})
        refPvixNotHscFmt = self.factory.getFormatter(refPvixHsc)
        self.assertIsInstance(refPvixNotHscFmt, Formatter)
        self.assertIn("PickleFormatter", refPvixNotHscFmt.name())

        # Create a DatasetRef that should fall back to using StorageClass
        dimensionsNoV = frozenset(("PhysicalFilter", "Instrument"))
        refPvixNotHscDims = self.makeDatasetRef("pvix", dimensionsNoV, sc, {"instrument": "DummyHSC",
                                                                            "physical_filter": "v"})
        refPvixNotHscDims_fmt = self.factory.getFormatter(refPvixNotHscDims)
        self.assertIsInstance(refPvixNotHscDims_fmt, Formatter)
        self.assertIn("YamlFormatter", refPvixNotHscDims_fmt.name())


if __name__ == "__main__":
    unittest.main()
