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

from lsst.daf.butler import Butler, Config
from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import DatasetType
from datasetsHelper import FitsCatalogDatasetsHelper, DatasetTestHelper

try:
    import lsst.afw.image
    from lsst.afw.image import LOCAL
    from lsst.geom import Box2I, Point2I
except ImportError:
    lsst.afw.image = None

TESTDIR = os.path.dirname(__file__)


class ButlerFitsTests(FitsCatalogDatasetsHelper, DatasetTestHelper):
    useTempRoot = True

    @staticmethod
    def registerDatasetTypes(datasetTypeName, dataUnits, storageClass, registry):
        """Bulk register DatasetTypes
        """
        datasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        registry.registerDatasetType(datasetType)

        for compName, compStorageClass in storageClass.components.items():
            compType = DatasetType(datasetType.componentTypeName(compName), dataUnits, compStorageClass)
            registry.registerDatasetType(compType)

    @classmethod
    def setUpClass(cls):
        if lsst.afw.image is None:
            raise unittest.SkipTest("afw not available.")
        cls.storageClassFactory = StorageClassFactory()
        cls.storageClassFactory.addFromConfig(cls.configFile)

    def setUp(self):
        """Create a new butler root for each test."""
        if self.useTempRoot:
            self.root = tempfile.mkdtemp(dir=TESTDIR)
            Butler.makeRepo(self.root, config=Config(self.configFile))
            self.tmpConfigFile = os.path.join(self.root, "butler.yaml")
        else:
            self.root = None
            self.tmpConfigFile = self.configFile

    def tearDown(self):
        if self.root is not None and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

    def testExposureCompositePutGetConcrete(self):
        storageClass = self.storageClassFactory.getStorageClass("ExposureF")
        self.runExposureCompositePutGetTest(storageClass, "calexp")

    def testExposureCompositePutGetVirtual(self):
        storageClass = self.storageClassFactory.getStorageClass("ExposureCompositeF")
        self.runExposureCompositePutGetTest(storageClass, "unknown")

    def runExposureCompositePutGetTest(self, storageClass, datasetTypeName):
        example = os.path.join(TESTDIR, "data", "basic", "small.fits")
        exposure = lsst.afw.image.ExposureF(example)
        butler = Butler(self.tmpConfigFile)
        dataUnits = ("Instrument", "Visit")
        self.registerDatasetTypes(datasetTypeName, dataUnits, storageClass, butler.registry)
        dataId = {"visit": 42, "instrument": "DummyCam", "physical_filter": "d-r"}
        # Add needed DataUnits
        butler.registry.addDataUnitEntry("Instrument", {"instrument": "DummyCam"})
        butler.registry.addDataUnitEntry("PhysicalFilter", {"instrument": "DummyCam",
                                         "physical_filter": "d-r"})
        butler.registry.addDataUnitEntry("Visit", {"instrument": "DummyCam", "visit": 42,
                                                   "physical_filter": "d-r"})
        butler.put(exposure, datasetTypeName, dataId)
        # Get the full thing
        full = butler.get(datasetTypeName, dataId)  # noqa F841
        # TODO enable check for equality (fix for Exposure type)
        # self.assertEqual(full, exposure)
        # Get a component
        compsRead = {}
        for compName in ("wcs", "image", "mask", "coaddInputs", "psf"):
            compTypeName = DatasetType.nameWithComponent(datasetTypeName, compName)
            component = butler.get(compTypeName, dataId)
            # TODO enable check for component instance types
            # compRef = butler.registry.find(butler.run.collection, "calexp.{}".format(compName), dataId)
            # self.assertIsInstance(component, compRef.datasetType.storageClass.pytype)
            compsRead[compName] = component
        # Simple check of WCS
        bbox = lsst.afw.geom.Box2I(lsst.afw.geom.Point2I(0, 0),
                                   lsst.afw.geom.Extent2I(9, 9))
        self.assertWcsAlmostEqualOverBBox(compsRead["wcs"], exposure.getWcs(), bbox)

        # With parameters
        inBBox = Box2I(minimum=Point2I(0, 0), maximum=Point2I(3, 3))
        parameters = dict(bbox=inBBox, origin=LOCAL)
        subset = butler.get(datasetTypeName, dataId, parameters=parameters)
        outBBox = subset.getBBox()
        self.assertEqual(inBBox, outBBox)


class PosixDatastoreButlerTestCase(ButlerFitsTests, lsst.utils.tests.TestCase):
    """PosixDatastore specialization of a butler"""
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")


class InMemoryDatastoreButlerTestCase(ButlerFitsTests, lsst.utils.tests.TestCase):
    """InMemoryDatastore specialization of a butler"""
    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")
    useTempRoot = False


class ChainedDatastoreButlerTestCase(ButlerFitsTests, lsst.utils.tests.TestCase):
    """PosixDatastore specialization"""
    configFile = os.path.join(TESTDIR, "config/basic/butler-chained.yaml")


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
