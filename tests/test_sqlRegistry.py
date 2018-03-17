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

from lsst.daf.butler.core.run import Run
from lsst.daf.butler.core.datasets import DatasetType
from lsst.daf.butler.core.registry import Registry
from lsst.daf.butler.registries.sqlRegistry import SqlRegistry

"""Tests for SqlRegistry.
"""


class SqlRegistryTestCase(lsst.utils.tests.TestCase):
    """Test for SqlRegistry.
    """
    def setUp(self):
        self.testDir = os.path.dirname(__file__)
        self.configFile = os.path.join(self.testDir, "config/basic/butler.yaml")

    def testInitFromConfig(self):
        registry = Registry.fromConfig(self.configFile)
        self.assertIsInstance(registry, SqlRegistry)

    def testDatasetType(self):
        registry = Registry.fromConfig(self.configFile)
        # Check valid insert
        datasetTypeName = "test"
        storageClass = "StructuredData"
        dataUnits = ("camera", "visit")
        inDatasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        registry.registerDatasetType(inDatasetType)
        outDatasetType = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType, inDatasetType)

        # Re-inserting should fail
        with self.assertRaises(KeyError):
            registry.registerDatasetType(inDatasetType)

        # Template can be None
        datasetTypeName = "testNoneTemplate"
        storageClass = "StructuredData"
        dataUnits = ("camera", "visit")
        inDatasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        registry.registerDatasetType(inDatasetType)
        outDatasetType = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType, inDatasetType)

    def testRun(self):
        registry = Registry.fromConfig(self.configFile)
        # Check insertion and retrieval with two different collections
        for collection in ["one", "two"]:
            run = registry.makeRun(collection)
            self.assertIsInstance(run, Run)
            self.assertEquals(run.collection, collection)
            # Test retrieval by collection
            runCpy1 = registry.getRun(collection=run.collection)
            self.assertEquals(runCpy1, run)
            # Test retrieval by (run/execution) id
            runCpy2 = registry.getRun(id=run.execution)
            self.assertEquals(runCpy2, run)
        # Non-existing collection should return None
        self.assertIsNone(registry.getRun(collection="bogus"))
        # Non-existing id should return None
        self.assertIsNone(registry.getRun(id=100))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
