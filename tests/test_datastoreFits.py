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
import tempfile
import shutil

import lsst.utils.tests
from lsst.utils import doImport

from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import DatastoreConfig

from lsst.daf.butler.tests import (FitsCatalogDatasetsHelper, DatasetTestHelper, DatastoreTestHelper,
                                   DummyRegistry)

try:
    import lsst.afw.table
    import lsst.afw.image
    import lsst.geom
except ImportError:
    lsst.afw = None

TESTDIR = os.path.dirname(__file__)


class DatastoreFitsTests(FitsCatalogDatasetsHelper, DatasetTestHelper, DatastoreTestHelper):
    root = None

    @classmethod
    def setUpClass(cls):
        if lsst.afw is None:
            raise unittest.SkipTest("afw not available.")

        # Base classes need to know where the test directory is
        cls.testDir = TESTDIR

        # Storage Classes are fixed for all datastores in these tests
        scConfigFile = os.path.join(TESTDIR, "config/basic/storageClasses.yaml")
        cls.storageClassFactory = StorageClassFactory()
        cls.storageClassFactory.addFromConfig(scConfigFile)

        # Read the Datastore config so we can get the class
        # information (since we should not assume the constructor
        # name here, but rely on the configuration file itself)
        datastoreConfig = DatastoreConfig(cls.configFile)
        cls.datastoreType = doImport(datastoreConfig["cls"])

    def setUp(self):
        self.setUpDatastoreTests(DummyRegistry, DatastoreConfig)

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def testConstructor(self):
        datastore = self.makeDatastore()
        self.assertIsNotNone(datastore)

    def testBasicPutGet(self):
        catalog = self.makeExampleCatalog()
        datastore = self.makeDatastore()

        # Put
        dimensions = self.registry.dimensions.extract(("visit", "physical_filter"))
        dataId = {"visit": 123456, "physical_filter": "blue", "instrument": "dummy"}
        storageClass = self.storageClassFactory.getStorageClass("SourceCatalog")

        ref = self.makeDatasetRef("calexp", dimensions, storageClass, dataId)

        datastore.put(catalog, ref)

        # Does it exist?
        self.assertTrue(datastore.exists(ref))

        uri = datastore.getUri(ref)
        if self.fileExt is not None:
            self.assertTrue(uri.endswith(self.fileExt))
        self.assertTrue(uri.startswith(self.uriScheme))

        # Get
        catalogOut = datastore.get(ref, parameters=None)
        self.assertCatalogEqual(catalog, catalogOut)

        # These should raise
        ref = self.makeDatasetRef("calexp2", dimensions, storageClass, dataId)
        with self.assertRaises(FileNotFoundError):
            # non-existing file
            datastore.get(ref, parameters=None)

    def testRemove(self):
        catalog = self.makeExampleCatalog()
        datastore = self.makeDatastore()

        # Put
        storageClass = self.storageClassFactory.getStorageClass("SourceCatalog")
        dimensions = self.registry.dimensions.extract(("visit", "physical_filter"))
        dataId = {"visit": 1234567, "physical_filter": "blue", "instrument": "dummy"}

        ref = self.makeDatasetRef("calexp", dimensions, storageClass, dataId)
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
        dimensions = self.registry.dimensions.extract(("visit", "physical_filter"))
        dataId = {"visit": 12345, "physical_filter": "red", "instrument": "dummy"}

        storageClass = self.storageClassFactory.getStorageClass("SourceCatalog")
        ref = self.makeDatasetRef("calexp", dimensions, storageClass, dataId)

        inputDatastore = self.makeDatastore("test_input_datastore")
        outputDatastore = self.makeDatastore("test_output_datastore")

        inputDatastore.put(catalog, ref)
        outputDatastore.transfer(inputDatastore, ref)

        catalogOut = outputDatastore.get(ref)
        self.assertCatalogEqual(catalog, catalogOut)

    def testExposurePutGet(self):
        example = os.path.join(self.testDir, "data", "basic", "small.fits")
        exposure = lsst.afw.image.ExposureF(example)
        datastore = self.makeDatastore()
        # Put
        dimensions = self.registry.dimensions.extract(("visit", "physical_filter"))
        dataId = {"visit": 231, "physical_filter": "Fc", "instrument": "dummy"}
        storageClass = datastore.storageClassFactory.getStorageClass("ExposureF")
        ref = self.makeDatasetRef("calexp", dimensions, storageClass, dataId)

        datastore.put(exposure, ref)

        # Does it exist?
        self.assertTrue(datastore.exists(ref))

        # Get
        exposureOut = datastore.get(ref)
        self.assertEqual(type(exposure), type(exposureOut))

        # Get some components
        # Could not test the following components as they were not known:
        # bbox, xy0, filter, polygon, detector, extras, and exposureInfo
        for compName, isNone in (("wcs", False),
                                 ("image", False),
                                 ("mask", False),
                                 ("coaddInputs", False),
                                 ("psf", False),
                                 ("variance", False),
                                 ("photoCalib", False),
                                 ("metadata", False),
                                 ("visitInfo", False),
                                 ("apCorrMap", True),
                                 ("transmissionCurve", True),
                                 ("metadata", False)):
            with self.subTest(component=compName):
                compRef = self.makeDatasetRef(ref.datasetType.componentTypeName(compName), dimensions,
                                              storageClass.components[compName], dataId, id=ref.id)
                component = datastore.get(compRef)
                if isNone:
                    self.assertIsNone(component)
                else:
                    self.assertIsInstance(component, compRef.datasetType.storageClass.pytype)

        # Get the WCS component to check it
        wcsRef = self.makeDatasetRef(ref.datasetType.componentTypeName("wcs"), dimensions,
                                     storageClass.components["wcs"], dataId, id=ref.id)
        wcs = datastore.get(wcsRef)

        # Simple check of WCS
        bbox = lsst.geom.Box2I(lsst.geom.Point2I(0, 0),
                               lsst.geom.Extent2I(9, 9))
        self.assertWcsAlmostEqualOverBBox(wcs, exposure.getWcs(), bbox)

        # Check basic metadata
        metadataRef = self.makeDatasetRef(ref.datasetType.componentTypeName("metadata"), dimensions,
                                          storageClass.components["metadata"], dataId, id=ref.id)
        metadata = datastore.get(metadataRef)
        self.assertEqual(metadata["WCS_ID"], 3)

    def testExposureCompositePutGet(self):
        example = os.path.join(self.testDir, "data", "basic", "small.fits")
        exposure = lsst.afw.image.ExposureF(example)
        datastore = self.makeDatastore()
        # Put
        dimensions = self.registry.dimensions.extract(("visit", "physical_filter"))
        dataId = {"visit": 23, "physical_filter": "F", "instrument": "dummy"}
        storageClass = datastore.storageClassFactory.getStorageClass("ExposureCompositeF")
        ref = self.makeDatasetRef("calexp", dimensions, storageClass, dataId)

        # Get the predicted URI
        self.assertFalse(datastore.exists(ref))
        uri = datastore.getUri(ref, predict=True)
        self.assertTrue(uri.endswith("#predicted"))

        components = storageClass.assembler().disassemble(exposure)
        self.assertEqual(set(components),
                         {"wcs", "variance", "visitInfo", "image", "mask", "coaddInputs", "psf",
                          "metadata", "photoCalib"})

        # Get a component
        compsRead = {}
        for compName, datasetComponent in components.items():
            compRef = self.makeDatasetRef(ref.datasetType.componentTypeName(compName), dimensions,
                                          datasetComponent.storageClass, dataId)

            datastore.put(datasetComponent.component, compRef)

            # Does it exist?
            self.assertTrue(datastore.exists(compRef))

            component = datastore.get(compRef)
            self.assertIsInstance(component, compRef.datasetType.storageClass.pytype)
            compsRead[compName] = component

        # Simple check of WCS
        bbox = lsst.geom.Box2I(lsst.geom.Point2I(0, 0),
                               lsst.geom.Extent2I(9, 9))
        self.assertWcsAlmostEqualOverBBox(compsRead["wcs"], exposure.getWcs(), bbox)

        # Try to reassemble the exposure
        retrievedExposure = storageClass.assembler().assemble(compsRead)
        self.assertIsInstance(retrievedExposure, type(exposure))


class PosixDatastoreTestCase(DatastoreFitsTests, lsst.utils.tests.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    uriScheme = "file:"
    fileExt = ".fits"

    def setUp(self):
        # Override the working directory before calling the base class
        self.root = tempfile.mkdtemp(dir=TESTDIR)
        super().setUp()


class InMemoryDatastoreTestCase(DatastoreFitsTests, lsst.utils.tests.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/inMemoryDatastore.yaml")
    uriScheme = "mem:"
    fileExt = None


class ChainedDatastoreTestCase(PosixDatastoreTestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/chainedDatastore.yaml")


if __name__ == "__main__":
    unittest.main()
