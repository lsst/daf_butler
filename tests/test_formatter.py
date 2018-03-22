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

import lsst.daf.butler.core.formatter as formatter
import lsst.daf.butler.core.storageClass as storageClass

"""Tests related to the formatter infrastructure.
"""


class FormatterFactoryTestCase(lsst.utils.tests.TestCase):
    """Tests of the formatter factory infrastructure.
    """

    def setUp(self):
        self.factory = formatter.FormatterFactory()

    def testRegistry(self):
        """Check that formatters can be stored in the registry.
        """
        formatterTypeName = "lsst.daf.butler.formatters.fitsCatalogFormatter.FitsCatalogFormatter"
        storageClassName = "Image"
        self.factory.registerFormatter(storageClassName, formatterTypeName)
        f = self.factory.getFormatter(storageClassName)
        self.assertIsInstance(f, formatter.Formatter)
        # Defer the import so that we ensure that the infrastructure loaded it on demand previously
        from lsst.daf.butler.formatters.fitsCatalogFormatter import FitsCatalogFormatter
        self.assertEqual(type(f), FitsCatalogFormatter)

        with self.assertRaises(ValueError):
            # Try to store something that is not a Formatter but is a class
            self.factory.registerFormatter("NotFormatter", "lsst.daf.butler.core.formatter.FormatterFactory")

        with self.assertRaises(ValueError):
            # Try to store something that is not a Formatter but is a module
            self.factory.registerFormatter("NotFormatter", "lsst.daf.butler")

        with self.assertRaises(ValueError):
            # Try to store something that is not a Formatter and does not exist
            self.factory.registerFormatter("NotFormatter", "lsst.daf.butler.x")

        with self.assertRaises(ValueError):
            # Try to store something that is not importable
            self.factory.registerFormatter("NotImportable", "not a thing")

        with self.assertRaises(KeyError):
            f = self.factory.getFormatter("Missing", "AlsoMissing")

        with self.assertRaises(KeyError):
            f = self.factory.getFormatter("Missing")

    def testRegistryWithStorageClass(self):
        """Test that the registry can be given a StorageClass object.
        """
        formatterTypeName = "lsst.daf.butler.formatters.fitsCatalogFormatter.FitsCatalogFormatter"
        storageClassName = "TestClass"
        sc = storageClass.makeNewStorageClass(storageClassName, dict, None)

        # Store using an instance
        self.factory.registerFormatter(sc(), formatterTypeName)

        # Retrieve using the class
        f = self.factory.getFormatter(sc)
        self.assertIsInstance(f, formatter.Formatter)
        # Defer the import so that we ensure that the infrastructure loaded it on demand previously
        from lsst.daf.butler.formatters.fitsCatalogFormatter import FitsCatalogFormatter
        self.assertEqual(type(f), FitsCatalogFormatter)

        with self.assertRaises(ValueError):
            # Attempt to overwrite using the name as a simple str
            self.factory.registerFormatter(storageClassName, formatterTypeName)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
