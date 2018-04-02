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

import os
import unittest

import lsst.utils.tests

from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler.datastores.posixDatastore import PosixDatastore, DatastoreConfig

from datasetsHelper import FitsCatalogDatasetsHelper, DatasetTestHelper

from dummyRegistry import DummyRegistry

try:
    import lsst.afw.table
    import lsst.afw.image
    import lsst.afw.geom
except ImportError:
    lsst.afw.table = None
    lsst.afw.image = None


class PosixDatastoreFitsTestCase(lsst.utils.tests.TestCase, FitsCatalogDatasetsHelper, DatasetTestHelper):

    @classmethod
    def setUpClass(cls):
        if lsst.afw.table is None:
            raise unittest.SkipTest("afw not available.")
        cls.testDir = os.path.dirname(__file__)
        cls.storageClassFactory = StorageClassFactory()
        cls.configFile = os.path.join(cls.testDir, "config/basic/butler.yaml")
        cls.storageClassFactory.addFromConfig(cls.configFile)

    def setUp(self):
        self.registry = DummyRegistry()

        # Need to keep ID for each datasetRef since we have no butler
        # for these tests
        self.id = 1

    def testConstructor(self):
        datastore = PosixDatastore(config=self.configFile, registry=self.registry)
        self.assertIsNotNone(datastore)

    def testBasicPutGet(self):
        catalog = self.makeExampleCatalog()
        datastore = PosixDatastore(config=self.configFile, registry=self.registry)
        # Put
        dataUnits = frozenset(("visit", "filter"))
        dataId = {"visit": 123456, "filter": "blue"}
        storageClass = self.storageClassFactory.getStorageClass("SourceCatalog")

        ref = self.makeDatasetRef("calexp", dataUnits, storageClass, dataId)

        datastore.put(catalog, ref)

        # Does it exist?
        self.assertTrue(datastore.exists(ref))

        uri = datastore.getUri(ref)
        self.assertTrue(uri.endswith(".fits"))
        self.assertTrue(uri.startswith("file:"))

        # Get
        catalogOut = datastore.get(ref, parameters=None)
        self.assertCatalogEqual(catalog, catalogOut)

        # These should raise
        ref = self.makeDatasetRef("calexp2", dataUnits, storageClass, dataId)
        with self.assertRaises(FileNotFoundError):
            # non-existing file
            datastore.get(ref, parameters=None)

    def testRemove(self):
        catalog = self.makeExampleCatalog()
        datastore = PosixDatastore(config=self.configFile, registry=self.registry)

        # Put
        storageClass = self.storageClassFactory.getStorageClass("SourceCatalog")
        dataUnits = frozenset(("visit", "filter"))
        dataId = {"visit": 1234567, "filter": "blue"}

        ref = self.makeDatasetRef("calexp", dataUnits, storageClass, dataId)
        datastore.put(catalog, ref)

        # Does it exist?
        self.assertTrue(datastore.exists(ref))

        # Get
        catalogOut = datastore.get(ref)
        self.assertCatalogEqual(catalog, catalogOut)

        # Remove
        datastore.remove(ref)

        # Does it exist?
        self.assertFalse(datastore.exists(ref))

        # Get should now fail
        with self.assertRaises(FileNotFoundError):
            datastore.get(ref)
        # Can only delete once
        with self.assertRaises(FileNotFoundError):
            datastore.remove(ref)

    def testTransfer(self):
        catalog = self.makeExampleCatalog()
        dataUnits = frozenset(("visit", "filter"))
        dataId = {"visit": 12345, "filter": "red"}

        storageClass = self.storageClassFactory.getStorageClass("SourceCatalog")
        ref = self.makeDatasetRef("calexp", dataUnits, storageClass, dataId)

        inputConfig = DatastoreConfig(self.configFile)
        inputConfig['datastore.root'] = os.path.join(self.testDir, "./test_input_datastore")
        inputPosixDatastore = PosixDatastore(config=inputConfig, registry=self.registry)
        outputConfig = inputConfig.copy()
        outputConfig['datastore.root'] = os.path.join(self.testDir, "./test_output_datastore")
        outputPosixDatastore = PosixDatastore(config=outputConfig,
                                              registry=DummyRegistry())

        inputPosixDatastore.put(catalog, ref)
        outputPosixDatastore.transfer(inputPosixDatastore, ref)

        catalogOut = outputPosixDatastore.get(ref)
        self.assertCatalogEqual(catalog, catalogOut)


class PosixDatastoreExposureTestCase(lsst.utils.tests.TestCase, DatasetTestHelper):

    @classmethod
    def setUpClass(cls):
        if lsst.afw.image is None:
            raise unittest.SkipTest("afw not available.")
        cls.testDir = os.path.dirname(__file__)
        cls.storageClassFactory = StorageClassFactory()
        cls.configFile = os.path.join(cls.testDir, "config/basic/butler.yaml")
        cls.storageClassFactory.addFromConfig(cls.configFile)

    def setUp(self):
        self.registry = DummyRegistry()

        # Need to keep ID for each datasetRef since we have no butler
        # for these tests
        self.id = 1

    def testExposurePutGet(self):
        example = os.path.join(self.testDir, "data", "basic", "small.fits")
        exposure = lsst.afw.image.ExposureF(example)
        datastore = PosixDatastore(config=self.configFile, registry=self.registry)
        # Put
        dataUnits = frozenset(("visit", "filter"))
        dataId = {"visit": 231, "filter": "Fc"}
        storageClass = datastore.storageClassFactory.getStorageClass("ExposureF")
        ref = self.makeDatasetRef("calexp", dataUnits, storageClass, dataId)

        datastore.put(exposure, ref)

        # Does it exist?
        self.assertTrue(datastore.exists(ref))

        # Get
        exposureOut = datastore.get(ref)
        self.assertEqual(type(exposure), type(exposureOut))

        # Get some components
        for compName in ("wcs", "image", "mask", "coaddInputs", "psf"):
            compRef = self.makeDatasetRef(ref.datasetType.componentTypeName(compName), dataUnits,
                                          storageClass.components[compName], dataId, id=ref.id)
            component = datastore.get(compRef)
            self.assertIsInstance(component, compRef.datasetType.storageClass.pytype)

        # Get the WCS component to check it
        wcsRef = self.makeDatasetRef(ref.datasetType.componentTypeName("wcs"), dataUnits,
                                     storageClass.components["wcs"], dataId, id=ref.id)
        wcs = datastore.get(wcsRef)

        # Simple check of WCS
        bbox = lsst.afw.geom.Box2I(lsst.afw.geom.Point2I(0, 0),
                                   lsst.afw.geom.Extent2I(9, 9))
        self.assertWcsAlmostEqualOverBBox(wcs, exposure.getWcs(), bbox)

    def testExposureCompositePutGet(self):
        example = os.path.join(self.testDir, "data", "basic", "small.fits")
        exposure = lsst.afw.image.ExposureF(example)
        datastore = PosixDatastore(config=self.configFile, registry=self.registry)
        # Put
        dataUnits = frozenset(("visit", "filter"))
        dataId = {"visit": 23, "filter": "F"}
        storageClass = datastore.storageClassFactory.getStorageClass("ExposureCompositeF")
        ref = self.makeDatasetRef("calexp", dataUnits, storageClass, dataId)

        # Get the predicted URI
        self.assertFalse(datastore.exists(ref))
        uri = datastore.getUri(ref, predict=True)
        self.assertTrue(uri.endswith("#predicted"))

        components = storageClass.assembler().disassemble(exposure)
        self.assertTrue(components)

        # Get a component
        compsRead = {}
        for compName in ("wcs", "image", "mask", "coaddInputs", "psf"):
            compRef = self.makeDatasetRef(ref.datasetType.componentTypeName(compName), dataUnits,
                                          components[compName].storageClass, dataId)

            datastore.put(components[compName].component, compRef)

            # Does it exist?
            self.assertTrue(datastore.exists(compRef))

            component = datastore.get(compRef)
            self.assertIsInstance(component, compRef.datasetType.storageClass.pytype)
            compsRead[compName] = component

        # Simple check of WCS
        bbox = lsst.afw.geom.Box2I(lsst.afw.geom.Point2I(0, 0),
                                   lsst.afw.geom.Extent2I(9, 9))
        self.assertWcsAlmostEqualOverBBox(compsRead["wcs"], exposure.getWcs(), bbox)

        # Try to reassemble the exposure
        retrievedExposure = storageClass.assembler().assemble(compsRead)
        self.assertIsInstance(retrievedExposure, type(exposure))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
