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

import lsst.utils.tests

from lsst.daf.butler import Butler
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
        cls.testDir = os.path.dirname(__file__)
        cls.storageClassFactory = StorageClassFactory()
        cls.configFile = os.path.join(cls.testDir, "config/basic/butler.yaml")
        cls.storageClassFactory.addFromConfig(cls.configFile)

    def assertGetComponents(self, butler, datasetTypeName, dataId, components, reference):
        for component in components:
            compTypeName = DatasetType.nameWithComponent(datasetTypeName, component)
            result = butler.get(compTypeName, dataId)
            self.assertEqual(result, getattr(reference, component))

    def testConstructor(self):
        """Independent test of constructor.
        """
        butler = Butler(self.configFile)
        self.assertIsInstance(butler, Butler)

    def testBasicPutGet(self):
        butler = Butler(self.configFile)
        # Create and register a DatasetType
        datasetTypeName = "test_metric"
        dataUnits = ("camera", "visit")
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        self.registerDatasetTypes(datasetTypeName, dataUnits, storageClass, butler.registry)

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
        dataUnits = ("camera", "visit")
        storageClass = self.storageClassFactory.getStorageClass("StructuredComposite")
        self.registerDatasetTypes(datasetTypeName, dataUnits, storageClass, butler.registry)

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


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
