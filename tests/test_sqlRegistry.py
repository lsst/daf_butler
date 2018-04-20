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
from datetime import datetime, timedelta

import lsst.utils.tests

from lsst.daf.butler.core.storageInfo import StorageInfo
from lsst.daf.butler.core.execution import Execution
from lsst.daf.butler.core.quantum import Quantum
from lsst.daf.butler.core.run import Run
from lsst.daf.butler.core.datasets import DatasetType
from lsst.daf.butler.core.registry import Registry
from lsst.daf.butler.registries.sqlRegistry import SqlRegistry
from lsst.daf.butler.core.storageClass import makeNewStorageClass

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
        storageClass = makeNewStorageClass("testDatasetType")()
        dataUnits = ("Camera", "Visit")
        inDatasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        registry.registerDatasetType(inDatasetType)
        outDatasetType = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType, inDatasetType)

        # Re-inserting should fail
        with self.assertRaises(KeyError):
            registry.registerDatasetType(inDatasetType)

        # Template can be None
        datasetTypeName = "testNoneTemplate"
        storageClass = makeNewStorageClass("testDatasetType2")()
        dataUnits = ("Camera", "Visit")
        inDatasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        registry.registerDatasetType(inDatasetType)
        outDatasetType = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType, inDatasetType)

    def testDataset(self):
        registry = Registry.fromConfig(self.configFile)
        run = registry.makeRun(collection="test")
        storageClass = makeNewStorageClass("testDataset")()
        datasetType = DatasetType(name="testtype", dataUnits=("Camera",), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        ref = registry.addDataset(datasetType, dataId={"camera": "DummyCam"}, run=run)
        outRef = registry.getDataset(ref.id)
        self.assertEqual(ref, outRef)

    def testComponents(self):
        registry = Registry.fromConfig(self.configFile)
        storageClass = makeNewStorageClass("testComponents")()
        parentDatasetType = DatasetType(name="parent", dataUnits=("Camera",), storageClass=storageClass)
        childDatasetType1 = DatasetType(name="child1", dataUnits=("Camera",), storageClass=storageClass)
        childDatasetType2 = DatasetType(name="child2", dataUnits=("Camera",), storageClass=storageClass)
        registry.registerDatasetType(parentDatasetType)
        registry.registerDatasetType(childDatasetType1)
        registry.registerDatasetType(childDatasetType2)
        run = registry.makeRun(collection="test")
        parent = registry.addDataset(parentDatasetType, dataId={"camera": "DummyCam"}, run=run)
        children = {"child1": registry.addDataset(childDatasetType1, dataId={"camera": "DummyCam"}, run=run),
                    "child2": registry.addDataset(childDatasetType2, dataId={"camera": "DummyCam"}, run=run)}
        for name, child in children.items():
            registry.attachComponent(name, parent, child)
        self.assertEqual(parent.components, children)
        outParent = registry.getDataset(parent.id)
        self.assertEqual(outParent.components, children)

    def testRun(self):
        registry = Registry.fromConfig(self.configFile)
        # Check insertion and retrieval with two different collections
        for collection in ["one", "two"]:
            run = registry.makeRun(collection)
            self.assertIsInstance(run, Run)
            self.assertEqual(run.collection, collection)
            # Test retrieval by collection
            runCpy1 = registry.getRun(collection=run.collection)
            self.assertEqual(runCpy1, run)
            # Test retrieval by (run/execution) id
            runCpy2 = registry.getRun(id=run.id)
            self.assertEqual(runCpy2, run)
        # Non-existing collection should return None
        self.assertIsNone(registry.getRun(collection="bogus"))
        # Non-existing id should return None
        self.assertIsNone(registry.getRun(id=100))
        # Inserting with a preexisting collection should fail
        with self.assertRaises(ValueError):
            registry.makeRun("one")

    def testExecution(self):
        registry = Registry.fromConfig(self.configFile)
        startTime = datetime(2018, 1, 1)
        endTime = startTime + timedelta(days=1, hours=5)
        host = "localhost"
        execution = Execution(startTime, endTime, host)
        self.assertIsNone(execution.id)
        registry.addExecution(execution)
        self.assertIsInstance(execution.id, int)
        outExecution = registry.getExecution(execution.id)
        self.assertEqual(outExecution, execution)

    def testQuantum(self):
        registry = Registry.fromConfig(self.configFile)
        run = registry.makeRun(collection="test")
        storageClass = makeNewStorageClass("testQuantum")()
        # Make two predicted inputs
        datasetType1 = DatasetType(name="dst1", dataUnits=("Camera",), storageClass=storageClass)
        registry.registerDatasetType(datasetType1)
        ref1 = registry.addDataset(datasetType1, dataId={"camera": "DummyCam"}, run=run)
        datasetType2 = DatasetType(name="dst2", dataUnits=("Camera",), storageClass=storageClass)
        registry.registerDatasetType(datasetType2)
        ref2 = registry.addDataset(datasetType2, dataId={"camera": "DummyCam"}, run=run)
        # Create and add a Quantum
        quantum = Quantum(run=run,
                          task="some.fully.qualified.SuperTask",
                          startTime=datetime(2018, 1, 1),
                          endTime=datetime(2018, 1, 2),
                          host="localhost")
        quantum.addPredictedInput(ref1)
        quantum.addPredictedInput(ref2)
        # Quantum is not yet in Registry, so can't mark input as actual
        with self.assertRaises(KeyError):
            registry.markInputUsed(quantum, ref1)
        registry.addQuantum(quantum)
        # Now we can
        registry.markInputUsed(quantum, ref1)
        outQuantum = registry.getQuantum(quantum.id)
        self.assertEqual(outQuantum, quantum)

    def testStorageInfo(self):
        registry = Registry.fromConfig(self.configFile)
        storageClass = makeNewStorageClass("testStorageInfo")()
        datasetType = DatasetType(name="test", dataUnits=("Camera",), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        run = registry.makeRun(collection="test")
        ref = registry.addDataset(datasetType, dataId={"camera": "DummyCam"}, run=run)
        datastoreName = "dummystore"
        checksum = "d6fb1c0c8f338044b2faaf328f91f707"
        size = 512
        storageInfo = StorageInfo(datastoreName, checksum, size)
        # Test adding information about a new dataset
        registry.addStorageInfo(ref, storageInfo)
        outStorageInfo = registry.getStorageInfo(ref, datastoreName)
        self.assertEqual(outStorageInfo, storageInfo)
        # Test updating storage information for an existing dataset
        updatedStorageInfo = StorageInfo(datastoreName, "20a38163c50f4aa3aa0f4047674f8ca7", size+1)
        registry.updateStorageInfo(ref, datastoreName, updatedStorageInfo)
        outStorageInfo = registry.getStorageInfo(ref, datastoreName)
        self.assertNotEqual(outStorageInfo, storageInfo)
        self.assertEqual(outStorageInfo, updatedStorageInfo)

    def testAssembler(self):
        registry = Registry.fromConfig(self.configFile)
        storageClass = makeNewStorageClass("testAssembler")()
        datasetType = DatasetType(name="test", dataUnits=("Camera",), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        run = registry.makeRun(collection="test")
        ref = registry.addDataset(datasetType, dataId={"camera": "DummyCam"}, run=run)
        self.assertIsNone(ref.assembler)
        assembler = "some.fully.qualified.assembler"  # TODO replace by actual dummy assember once implemented
        registry.setAssembler(ref, assembler)
        self.assertEqual(ref.assembler, assembler)
        # TODO add check that ref2.assembler is also correct when ref2 is returned by Registry.find()

    def testFind(self):
        registry = Registry.fromConfig(self.configFile)
        storageClass = makeNewStorageClass("testFind")()
        datasetType = DatasetType(name="dummytype", dataUnits=("Camera", "Visit"), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        collection = "test"
        dataId = {"camera": "DummyCam", "visit": 0}
        run = registry.makeRun(collection=collection)
        inputRef = registry.addDataset(datasetType, dataId=dataId, run=run)
        outputRef = registry.find(collection, datasetType, dataId)
        self.assertEqual(outputRef, inputRef)
        # Check that retrieval with invalid dataId raises
        with self.assertRaises(ValueError):
            dataId = {"camera": "DummyCam", "abstract_filter": "g"}  # should be visit
            registry.find(collection, datasetType, dataId)
        # Check that different dataIds match to different datasets
        dataId1 = {"camera": "DummyCam", "visit": 1}
        inputRef1 = registry.addDataset(datasetType, dataId=dataId1, run=run)
        dataId2 = {"camera": "DummyCam", "visit": 2}
        inputRef2 = registry.addDataset(datasetType, dataId=dataId2, run=run)
        dataId3 = {"camera": "MyCam", "visit": 2}
        inputRef3 = registry.addDataset(datasetType, dataId=dataId3, run=run)
        self.assertEqual(registry.find(collection, datasetType, dataId1), inputRef1)
        self.assertEqual(registry.find(collection, datasetType, dataId2), inputRef2)
        self.assertEqual(registry.find(collection, datasetType, dataId3), inputRef3)
        self.assertNotEqual(registry.find(collection, datasetType, dataId1), inputRef2)
        self.assertNotEqual(registry.find(collection, datasetType, dataId2), inputRef1)
        self.assertNotEqual(registry.find(collection, datasetType, dataId3), inputRef1)
        # Check that requesting a non-existing dataId returns None
        nonExistingDataId = {"camera": "DummyCam", "visit": 42}
        self.assertIsNone(registry.find(collection, datasetType, nonExistingDataId))

    def testCollections(self):
        registry = Registry.fromConfig(self.configFile)
        storageClass = makeNewStorageClass("testCollections")()
        datasetType = DatasetType(name="dummytype", dataUnits=("Camera", "Visit"), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        collection = "ingest"
        run = registry.makeRun(collection=collection)
        dataId1 = {"camera": "DummyCam", "visit": 0}
        inputRef1 = registry.addDataset(datasetType, dataId=dataId1, run=run)
        dataId2 = {"camera": "DummyCam", "visit": 1}
        inputRef2 = registry.addDataset(datasetType, dataId=dataId2, run=run)
        # We should be able to find both datasets in their Run.collection
        outputRef = registry.find(run.collection, datasetType, dataId1)
        self.assertEqual(outputRef, inputRef1)
        outputRef = registry.find(run.collection, datasetType, dataId2)
        self.assertEqual(outputRef, inputRef2)
        # and with the associated collection
        newCollection = "something"
        registry.associate(newCollection, [inputRef1, inputRef2])
        outputRef = registry.find(newCollection, datasetType, dataId1)
        self.assertEqual(outputRef, inputRef1)
        outputRef = registry.find(newCollection, datasetType, dataId2)
        self.assertEqual(outputRef, inputRef2)
        # but no more after disassociation
        registry.disassociate(newCollection, [inputRef1, ], remove=False)  # TODO test with removal when done
        self.assertIsNone(registry.find(newCollection, datasetType, dataId1))
        outputRef = registry.find(newCollection, datasetType, dataId2)
        self.assertEqual(outputRef, inputRef2)

    def testDatasetUnit(self):
        registry = Registry.fromConfig(self.configFile)
        dataUnitName = 'Camera'
        dataUnitValue = {'camera': 'DummyCam'}
        registry.addDataUnitEntry(dataUnitName, dataUnitValue)
        # Inserting the same value twice should fail
        with self.assertRaises(ValueError):
            registry.addDataUnitEntry(dataUnitName, dataUnitValue)
        # Find should return the entry
        self.assertEqual(registry.findDataUnitEntry(dataUnitName, dataUnitValue), dataUnitValue)
        # Find on a non-existant value should return None
        self.assertIsNone(registry.findDataUnitEntry(dataUnitName, {'camera': 'Unknown'}))
        registry.addDataUnitEntry('AbstractFilter', {'abstract_filter': 'i'})
        dataUnitName2 = 'PhysicalFilter'
        dataUnitValue2 = {'physical_filter': 'DummyCam_i', 'abstract_filter': 'i'}
        # Missing required dependency ('camera') should fail
        with self.assertRaises(ValueError):
            registry.addDataUnitEntry(dataUnitName2, dataUnitValue2)
        # Adding required dependency should fix the failure
        dataUnitValue2['camera'] = 'DummyCam'
        registry.addDataUnitEntry(dataUnitName2, dataUnitValue2)
        # Find should return the entry
        self.assertEqual(registry.findDataUnitEntry(dataUnitName2, dataUnitValue2), dataUnitValue2)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
