#
# LSST Data Management System
#
# Copyright 2008-2018  AURA/LSST.
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

import os
import unittest

import lsst.utils.tests
import lsst.afw.table

from lsst.daf.butler.datastores.posixDatastore import PosixDatastore, DatastoreConfig

import datasetsHelper


class PosixDatastoreTestCase(lsst.utils.tests.TestCase):

    def setUp(self):
        self.testDir = os.path.dirname(__file__)
        self.configFile = os.path.join(self.testDir, "config/basic/butler.yaml")

    def testConstructor(self):
        datastore = PosixDatastore(config=self.configFile)
        self.assertIsNotNone(datastore)

    def testBasicPutGet(self):
        catalog = datasetsHelper.makeExampleCatalog()
        datastore = PosixDatastore(config=self.configFile)
        # Put
        storageClass = datastore.storageClassFactory.getStorageClass("SourceCatalog")
        uri, _ = datastore.put(catalog, storageClass=storageClass, storageHint="tester.fits", typeName=None)
        # Get
        catalogOut = datastore.get(uri, storageClass=storageClass, parameters=None)
        datasetsHelper.assertCatalogEqual(self, catalog, catalogOut)
        # These should raise
        with self.assertRaises(ValueError):
            # non-existing file
            datastore.get(uri="file:///non_existing.fits", storageClass=storageClass, parameters=None)
        with self.assertRaises(ValueError):
            # invalid storage class
            datastore.get(uri="file:///non_existing.fits", storageClass=object, parameters=None)

    def testRemove(self):
        catalog = datasetsHelper.makeExampleCatalog()
        datastore = PosixDatastore(config=self.configFile)
        # Put
        storageClass = datastore.storageClassFactory.getStorageClass("SourceCatalog")
        uri, _ = datastore.put(catalog, storageClass=storageClass, storageHint="tester.fits", typeName=None)
        # Get
        catalogOut = datastore.get(uri, storageClass=storageClass, parameters=None)
        datasetsHelper.assertCatalogEqual(self, catalog, catalogOut)
        # Remove
        datastore.remove(uri)
        # Get should now fail
        with self.assertRaises(ValueError):
            datastore.get(uri, storageClass=storageClass, parameters=None)
        # Can only delete once
        with self.assertRaises(FileNotFoundError):
            datastore.remove(uri)

    def testTransfer(self):
        catalog = datasetsHelper.makeExampleCatalog()
        path = "tester.fits"
        inputConfig = DatastoreConfig(self.configFile)
        inputConfig['datastore.root'] = os.path.join(self.testDir, "./test_input_datastore")
        inputPosixDatastore = PosixDatastore(config=inputConfig)
        outputConfig = inputConfig.copy()
        outputConfig['datastore.root'] = os.path.join(self.testDir, "./test_output_datastore")
        outputPosixDatastore = PosixDatastore(config=outputConfig)
        storageClass = outputPosixDatastore.storageClassFactory.getStorageClass("SourceCatalog")
        inputUri, _ = inputPosixDatastore.put(catalog, storageClass, path)
        outputUri, _ = outputPosixDatastore.transfer(inputPosixDatastore, inputUri, storageClass, path)
        catalogOut = outputPosixDatastore.get(outputUri, storageClass)
        datasetsHelper.assertCatalogEqual(self, catalog, catalogOut)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
