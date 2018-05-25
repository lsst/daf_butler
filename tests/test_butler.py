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

"""Tests for Butler.
"""

import os
import unittest
from tempfile import TemporaryDirectory
import pickle

import lsst.utils.tests

from lsst.daf.butler import Butler, Config
from lsst.daf.butler import StorageClassFactory
from lsst.daf.butler import DatasetType, DatasetRef
from examplePythonTypes import MetricsExample


def makeExampleMetrics():
    return MetricsExample({"AM1": 5.2, "AM2": 30.6},
                          {"a": [1, 2, 3],
                           "b": {"blue": 5, "red": "green"}},
                          [563, 234, 456.7]
                          )


class ButlerTestCase(lsst.utils.tests.TestCase):
    """Test for Butler.
    """

    @staticmethod
    def addDatasetType(datasetTypeName, dataUnits, storageClass, registry):
        """Create a DatasetType and register it
        """
        datasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        registry.registerDatasetType(datasetType)

    @classmethod
    def setUpClass(cls):
        cls.testDir = os.path.abspath(os.path.dirname(__file__))
        cls.storageClassFactory = StorageClassFactory()
        cls.configFile = os.path.join(cls.testDir, "config/basic/butler.yaml")
        cls.storageClassFactory.addFromConfig(cls.configFile)

    def assertGetComponents(self, butler, datasetTypeName, dataId, components, reference):
        for component in components:
            compTypeName = DatasetType.nameWithComponent(datasetTypeName, component)
            result = butler.get(compTypeName, dataId)
            self.assertEqual(result, getattr(reference, component))

    def testConstructor(self):
        """Test of minimal constructor.
        """
        config = Config()
        butler = Butler(config, collection="dummy")
        self.assertIsInstance(butler, Butler)

    def testBasicPutGet(self):
        butler = Butler(self.configFile)
        # Create and register a DatasetType
        datasetTypeName = "test_metric"
        dataUnits = ("Camera", "Visit")
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        self.addDatasetType(datasetTypeName, dataUnits, storageClass, butler.registry)

        # Add needed DataUnits
        butler.registry.addDataUnitEntry("Camera", {"camera": "DummyCam"})
        butler.registry.addDataUnitEntry("PhysicalFilter", {"camera": "DummyCam", "physical_filter": "d-r"})
        butler.registry.addDataUnitEntry("Visit", {"camera": "DummyCam", "visit": 42,
                                                   "physical_filter": "d-r"})

        # Create and store a dataset
        metric = makeExampleMetrics()
        dataId = {"camera": "DummyCam", "visit": 42}
        ref = butler.put(metric, datasetTypeName, dataId)
        self.assertIsInstance(ref, DatasetRef)
        # Test getDirect
        metricOut = butler.getDirect(ref)
        self.assertEqual(metric, metricOut)
        # Test get
        metricOut = butler.get(datasetTypeName, dataId)
        self.assertEqual(metric, metricOut)

        # Check we can get components
        self.assertGetComponents(butler, datasetTypeName, dataId,
                                 ("summary", "data", "output"), metric)

    def testCompositePutGet(self):
        butler = Butler(self.configFile)
        # Create and register a DatasetType
        datasetTypeName = "test_metric_comp"
        dataUnits = ("Camera", "Visit")
        storageClass = self.storageClassFactory.getStorageClass("StructuredComposite")
        self.addDatasetType(datasetTypeName, dataUnits, storageClass, butler.registry)

        # Add needed DataUnits
        butler.registry.addDataUnitEntry("Camera", {"camera": "DummyCamComp"})
        butler.registry.addDataUnitEntry("PhysicalFilter", {"camera": "DummyCamComp",
                                                            "physical_filter": "d-r"})
        butler.registry.addDataUnitEntry("Visit", {"camera": "DummyCamComp", "visit": 423,
                                                   "physical_filter": "d-r"})

        # Create and store a dataset
        metric = makeExampleMetrics()
        dataId = {"camera": "DummyCamComp", "visit": 423}
        ref = butler.put(metric, datasetTypeName, dataId)
        self.assertIsInstance(ref, DatasetRef)
        # Test getDirect
        metricOut = butler.getDirect(ref)
        self.assertEqual(metric, metricOut)
        # Test get
        metricOut = butler.get(datasetTypeName, dataId)
        self.assertEqual(metric, metricOut)

        # Check we can get components
        self.assertGetComponents(butler, datasetTypeName, dataId,
                                 ("summary", "data", "output"), metric)

    def testMakeRepo(self):
        """Test that we can write butler configuration to a new repository via
        the Butler.makeRepo interface and then instantiate a butler from the
        repo root.
        """
        with TemporaryDirectory(prefix=self.testDir + "/") as root:
            Butler.makeRepo(root)
            limited = Config(os.path.join(root, "butler.yaml"))
            butler1 = Butler(root, collection="null")
            Butler.makeRepo(root, standalone=True)
            full = Config(os.path.join(root, "butler.yaml"))
            butler2 = Butler(root, collection="null")
        # Butlers should have the same configuration regardless of whether
        # defaults were expanded.
        self.assertEqual(butler1.config, butler2.config)
        # Config files loaded directly should not be the same.
        self.assertNotEqual(limited, full)
        # Make sure 'limited' doesn't have a few keys we know it should be
        # inheriting from defaults.
        self.assertIn("datastore.formatters", full)
        self.assertNotIn("datastore.formatters", limited)

    def testExposureCompositePutGet(self):
        example = os.path.join(self.testDir, "data", "basic", "small.fits")
        exposure = lsst.afw.image.ExposureF(example)
        butler = Butler(self.configFile)
        datasetTypeName = "calexp"
        dataUnits = ("Camera", "Visit")
        storageClass = self.storageClassFactory.getStorageClass("ExposureF")
        self.addDatasetType(datasetTypeName, dataUnits, storageClass, butler.registry)
        # Add needed DataUnits
        butler.registry.addDataUnitEntry("Camera", {"camera": "DummyCamComp"})
        butler.registry.addDataUnitEntry("PhysicalFilter", {"camera": "DummyCamComp",
                                                            "physical_filter": "d-r"})
        butler.registry.addDataUnitEntry("Visit", {"camera": "DummyCamComp", "visit": 423,
                                                   "physical_filter": "d-r"})
        dataId = {"visit": 423, "camera": "DummyCamComp"}
        butler.put(exposure, "calexp", dataId)
        # Get the full thing
        full = butler.get("calexp", dataId)  # noqa F841
        # TODO enable check for equality (fix for Exposure type)
        # self.assertEqual(full, exposure)
        # Get a component
        compsRead = {}
        for compName in ("wcs", "image", "mask", "coaddInputs", "psf"):
            component = butler.get("calexp.{}".format(compName), dataId)
            # TODO enable check for component instance types
            # compRef = butler.registry.find(butler.run.collection, "calexp.{}".format(compName), dataId)
            # self.assertIsInstance(component, compRef.datasetType.storageClass.pytype)
            compsRead[compName] = component
        # Simple check of WCS
        bbox = lsst.afw.geom.Box2I(lsst.afw.geom.Point2I(0, 0),
                                   lsst.afw.geom.Extent2I(9, 9))
        self.assertWcsAlmostEqualOverBBox(compsRead["wcs"], exposure.getWcs(), bbox)

    def testPickle(self):
        """Test pickle support.
        """
        butler = Butler(self.configFile)
        butlerOut = pickle.loads(pickle.dumps(butler))
        self.assertIsInstance(butlerOut, Butler)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
