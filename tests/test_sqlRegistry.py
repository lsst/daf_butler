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
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from itertools import combinations

import lsst.utils.tests
import lsst.sphgeom

from sqlalchemy.exc import OperationalError
from lsst.daf.butler.core.execution import Execution
from lsst.daf.butler.core.quantum import Quantum
from lsst.daf.butler.core.run import Run
from lsst.daf.butler.core.datasets import DatasetType
from lsst.daf.butler.core.registry import Registry
from lsst.daf.butler.registries.sqlRegistry import SqlRegistry
from lsst.daf.butler.core.storageClass import StorageClass
from lsst.daf.butler.core.butlerConfig import ButlerConfig

"""Tests for SqlRegistry.
"""


class RegistryTests(metaclass=ABCMeta):
    """Test helper mixin for generic Registry tests.
    """

    @abstractmethod
    def makeRegistry(self):
        raise NotImplementedError()

    def assertRowCount(self, registry, table, count, where=None):
        """Check the number of rows in table
        """
        query = 'select count(*) as "cnt" from "{}"'.format(table)
        if where:
            query = '{} where {}'.format(query, where)
        rows = list(registry.query(query))
        self.assertEqual(rows[0]["cnt"], count)

    def testDatasetType(self):
        registry = self.makeRegistry()
        # Check valid insert
        datasetTypeName = "test"
        storageClass = StorageClass("testDatasetType")
        registry.storageClasses.registerStorageClass(storageClass)
        dataUnits = ("Camera", "Visit")
        differentDataUnits = ("Camera", "Patch")
        inDatasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        # Inserting for the first time should return True
        self.assertTrue(registry.registerDatasetType(inDatasetType))
        outDatasetType = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType, inDatasetType)

        # Re-inserting should work
        self.assertFalse(registry.registerDatasetType(inDatasetType))
        # Except when they are not identical
        with self.assertRaises(ValueError):
            nonIdenticalDatasetType = DatasetType(datasetTypeName, differentDataUnits, storageClass)
            registry.registerDatasetType(nonIdenticalDatasetType)

        # Template can be None
        datasetTypeName = "testNoneTemplate"
        storageClass = StorageClass("testDatasetType2")
        registry.storageClasses.registerStorageClass(storageClass)
        dataUnits = ("Camera", "Visit")
        inDatasetType = DatasetType(datasetTypeName, dataUnits, storageClass)
        registry.registerDatasetType(inDatasetType)
        outDatasetType = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType, inDatasetType)

    def testDataset(self):
        registry = self.makeRegistry()
        run = registry.makeRun(collection="test")
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="testtype", dataUnits=("Camera",), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        if not registry.limited:
            registry.addDataUnitEntry("Camera", {"camera": "DummyCam"})
        ref = registry.addDataset(datasetType, dataId={"camera": "DummyCam"}, run=run)
        outRef = registry.getDataset(ref.id)
        self.assertIsNotNone(ref.id)
        self.assertEqual(ref, outRef)
        with self.assertRaises(ValueError):
            ref = registry.addDataset(datasetType, dataId={"camera": "DummyCam"}, run=run)

    def testComponents(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testComponents")
        registry.storageClasses.registerStorageClass(storageClass)
        parentDatasetType = DatasetType(name="parent", dataUnits=("Camera",), storageClass=storageClass)
        childDatasetType1 = DatasetType(name="child1", dataUnits=("Camera",), storageClass=storageClass)
        childDatasetType2 = DatasetType(name="child2", dataUnits=("Camera",), storageClass=storageClass)
        registry.registerDatasetType(parentDatasetType)
        registry.registerDatasetType(childDatasetType1)
        registry.registerDatasetType(childDatasetType2)
        if not registry.limited:
            registry.addDataUnitEntry("Camera", {"camera": "DummyCam"})
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
        registry = self.makeRegistry()
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
        # Insert a new Run and check that ensureRun silently ignores it
        collection = "dummy"
        run = registry.makeRun(collection)
        registry.ensureRun(run)
        # Calling ensureRun with a different Run with the same id should fail
        run2 = Run(collection="hello")
        run2._id = run.id
        with self.assertRaises(ValueError):
            registry.ensureRun(run2)
        run2._id = None
        # Now it should work
        registry.ensureRun(run2)
        self.assertEqual(run2, registry.getRun(id=run2.id))

    def testExecution(self):
        registry = self.makeRegistry()
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
        registry = self.makeRegistry()
        if not registry.limited:
            registry.addDataUnitEntry("Camera", {"camera": "DummyCam"})
        run = registry.makeRun(collection="test")
        storageClass = StorageClass("testQuantum")
        registry.storageClasses.registerStorageClass(storageClass)
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

    def testDatasetLocations(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testStorageInfo")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="test", dataUnits=("Camera",), storageClass=storageClass)
        datasetType2 = DatasetType(name="test2", dataUnits=("Camera",), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        registry.registerDatasetType(datasetType2)
        if not registry.limited:
            registry.addDataUnitEntry("Camera", {"camera": "DummyCam"})
        run = registry.makeRun(collection="test")
        ref = registry.addDataset(datasetType, dataId={"camera": "DummyCam"}, run=run)
        ref2 = registry.addDataset(datasetType2, dataId={"camera": "DummyCam"}, run=run)
        datastoreName = "dummystore"
        datastoreName2 = "dummystore2"
        # Test adding information about a new dataset
        registry.addDatasetLocation(ref, datastoreName)
        addresses = registry.getDatasetLocations(ref)
        self.assertIn(datastoreName, addresses)
        self.assertEqual(len(addresses), 1)
        registry.addDatasetLocation(ref, datastoreName2)
        registry.addDatasetLocation(ref2, datastoreName2)
        addresses = registry.getDatasetLocations(ref)
        self.assertEqual(len(addresses), 2)
        self.assertIn(datastoreName, addresses)
        self.assertIn(datastoreName2, addresses)
        registry.removeDatasetLocation(datastoreName, ref)
        addresses = registry.getDatasetLocations(ref)
        self.assertEqual(len(addresses), 1)
        self.assertNotIn(datastoreName, addresses)
        self.assertIn(datastoreName2, addresses)
        registry.removeDatasetLocation(datastoreName2, ref)
        addresses = registry.getDatasetLocations(ref)
        self.assertEqual(len(addresses), 0)
        self.assertNotIn(datastoreName2, addresses)
        addresses = registry.getDatasetLocations(ref2)
        self.assertEqual(len(addresses), 1)
        self.assertIn(datastoreName2, addresses)

    def testFind(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testFind")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="dummytype", dataUnits=("Camera", "Visit"), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        if not registry.limited:
            registry.addDataUnitEntry("Camera", {"camera": "DummyCam"})
            registry.addDataUnitEntry("Camera", {"camera": "MyCam"})
            registry.addDataUnitEntry("PhysicalFilter", {"camera": "DummyCam", "physical_filter": "d-r"})
            registry.addDataUnitEntry("PhysicalFilter", {"camera": "MyCam", "physical_filter": "m-r"})
            registry.addDataUnitEntry("Visit", {"camera": "DummyCam", "visit": 0, "physical_filter": "d-r"})
            registry.addDataUnitEntry("Visit", {"camera": "DummyCam", "visit": 1, "physical_filter": "d-r"})
            registry.addDataUnitEntry("Visit", {"camera": "DummyCam", "visit": 2, "physical_filter": "d-r"})
            registry.addDataUnitEntry("Visit", {"camera": "MyCam", "visit": 2, "physical_filter": "m-r"})
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
        registry = self.makeRegistry()
        storageClass = StorageClass("testCollections")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="dummytype", dataUnits=("Camera", "Visit"), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        if not registry.limited:
            registry.addDataUnitEntry("Camera", {"camera": "DummyCam"})
            registry.addDataUnitEntry("PhysicalFilter", {"camera": "DummyCam", "physical_filter": "d-r"})
            registry.addDataUnitEntry("Visit", {"camera": "DummyCam", "visit": 0, "physical_filter": "d-r"})
            registry.addDataUnitEntry("Visit", {"camera": "DummyCam", "visit": 1, "physical_filter": "d-r"})
        collection = "ingest"
        run = registry.makeRun(collection=collection)
        # TODO: Dataset.physical_filter should be populated as well here
        # from the Visit DataUnit values.
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

    def testAssociate(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testAssociate")
        registry.storageClasses.registerStorageClass(storageClass)
        dataUnits = ("Camera", "Visit")
        datasetType1 = DatasetType(name="dummytype", dataUnits=dataUnits, storageClass=storageClass)
        registry.registerDatasetType(datasetType1)
        datasetType2 = DatasetType(name="smartytype", dataUnits=dataUnits, storageClass=storageClass)
        registry.registerDatasetType(datasetType2)
        if not registry.limited:
            registry.addDataUnitEntry("Camera", {"camera": "DummyCam"})
            registry.addDataUnitEntry("PhysicalFilter", {"camera": "DummyCam", "physical_filter": "d-r"})
            registry.addDataUnitEntry("Visit", {"camera": "DummyCam", "visit": 0, "physical_filter": "d-r"})
            registry.addDataUnitEntry("Visit", {"camera": "DummyCam", "visit": 1, "physical_filter": "d-r"})
        run1 = registry.makeRun(collection="ingest1")
        run2 = registry.makeRun(collection="ingest2")
        run3 = registry.makeRun(collection="ingest3")
        # TODO: Dataset.physical_filter should be populated as well here
        # from the Visit DataUnit values.
        dataId1 = {"camera": "DummyCam", "visit": 0}
        dataId2 = {"camera": "DummyCam", "visit": 1}
        ref1_run1 = registry.addDataset(datasetType1, dataId=dataId1, run=run1)
        ref2_run1 = registry.addDataset(datasetType1, dataId=dataId2, run=run1)
        ref1_run2 = registry.addDataset(datasetType2, dataId=dataId1, run=run2)
        ref2_run2 = registry.addDataset(datasetType2, dataId=dataId2, run=run2)
        ref1_run3 = registry.addDataset(datasetType2, dataId=dataId1, run=run3)
        ref2_run3 = registry.addDataset(datasetType2, dataId=dataId2, run=run3)
        # should have exactly 4 rows in Dataset
        self.assertRowCount(registry, "Dataset", 6)
        self.assertRowCount(registry, "DatasetCollection", 6)
        # adding same DatasetRef to the same run is an error
        with self.assertRaises(ValueError):
            registry.addDataset(datasetType1, dataId=dataId2, run=run1)
        # above exception must rollback and not add anything to Dataset
        self.assertRowCount(registry, "Dataset", 6)
        self.assertRowCount(registry, "DatasetCollection", 6)
        # associated refs from run1 with some other collection
        newCollection = "something"
        registry.associate(newCollection, [ref1_run1, ref2_run1])
        self.assertRowCount(registry, "DatasetCollection", 8)
        # associating same exact DatasetRef is OK (not doing anything),
        # two cases to test - single-ref and many-refs
        registry.associate(newCollection, [ref1_run1])
        registry.associate(newCollection, [ref1_run1, ref2_run1])
        self.assertRowCount(registry, "DatasetCollection", 8)
        # associated refs from run2 with same other collection, this should be OK
        # because thy have different dataset type
        registry.associate(newCollection, [ref1_run2, ref2_run2])
        self.assertRowCount(registry, "DatasetCollection", 10)
        # associating DatasetRef with the same units but different ID is not OK
        with self.assertRaises(ValueError):
            registry.associate(newCollection, [ref1_run3])
        with self.assertRaises(ValueError):
            registry.associate(newCollection, [ref1_run3, ref2_run3])

    def testDatasetUnit(self):
        registry = self.makeRegistry()
        dataUnitName = "Camera"
        dataUnitValue = {"camera": "DummyCam"}
        if registry.limited:
            with self.assertRaises(NotImplementedError):
                registry.addDataUnitEntry(dataUnitName, dataUnitValue)
            return  # the remainder of this test does not apply to limited Registry
        registry.addDataUnitEntry(dataUnitName, dataUnitValue)
        # Inserting the same value twice should fail
        with self.assertRaises(ValueError):
            registry.addDataUnitEntry(dataUnitName, dataUnitValue)
        # Find should return the entry
        self.assertEqual(registry.findDataUnitEntry(dataUnitName, dataUnitValue), dataUnitValue)
        # Find on a non-existant value should return None
        self.assertIsNone(registry.findDataUnitEntry(dataUnitName, {"camera": "Unknown"}))
        # AbstractFilter doesn't have a table; should fail.
        with self.assertRaises(OperationalError):
            registry.addDataUnitEntry("AbstractFilter", {"abstract_filter": "i"})
        dataUnitName2 = "PhysicalFilter"
        dataUnitValue2 = {"physical_filter": "DummyCam_i", "abstract_filter": "i"}
        # Missing required dependency ("camera") should fail
        with self.assertRaises(ValueError):
            registry.addDataUnitEntry(dataUnitName2, dataUnitValue2)
        # Adding required dependency should fix the failure
        dataUnitValue2["camera"] = "DummyCam"
        registry.addDataUnitEntry(dataUnitName2, dataUnitValue2)
        # Find should return the entry
        self.assertEqual(registry.findDataUnitEntry(dataUnitName2, dataUnitValue2), dataUnitValue2)

    def testBasicTransaction(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testDatasetType")
        registry.storageClasses.registerStorageClass(storageClass)
        dataUnits = ("Camera", )
        dataId = {"camera": "DummyCam"}
        datasetTypeA = DatasetType(name="A",
                                   dataUnits=dataUnits,
                                   storageClass=storageClass)
        datasetTypeB = DatasetType(name="B",
                                   dataUnits=dataUnits,
                                   storageClass=storageClass)
        datasetTypeC = DatasetType(name="C",
                                   dataUnits=dataUnits,
                                   storageClass=storageClass)
        run = registry.makeRun(collection="test")
        refId = None
        with registry.transaction():
            registry.registerDatasetType(datasetTypeA)
        with self.assertRaises(ValueError):
            with registry.transaction():
                registry.registerDatasetType(datasetTypeB)
                registry.registerDatasetType(datasetTypeC)
                if not registry.limited:
                    registry.addDataUnitEntry("Camera", {"camera": "DummyCam"})
                ref = registry.addDataset(datasetTypeA, dataId=dataId, run=run)
                refId = ref.id
                raise ValueError("Oops, something went wrong")
        # A should exist
        self.assertEqual(registry.getDatasetType("A"), datasetTypeA)
        # But B and C should both not exist
        with self.assertRaises(KeyError):
            registry.getDatasetType("B")
        with self.assertRaises(KeyError):
            registry.getDatasetType("C")
        # And neither should the dataset
        self.assertIsNotNone(refId)
        self.assertIsNone(registry.getDataset(refId))
        # Or the DataUnit entries
        if not registry.limited:
            self.assertIsNone(registry.findDataUnitEntry("Camera", {"camera": "DummyCam"}))

    def testGetRegion(self):
        registry = self.makeRegistry()
        if registry.limited:
            return
        regionTract = lsst.sphgeom.ConvexPolygon((lsst.sphgeom.UnitVector3d(1, 0, 0),
                                                  lsst.sphgeom.UnitVector3d(0, 1, 0),
                                                  lsst.sphgeom.UnitVector3d(0, 0, 1)))
        regionPatch = lsst.sphgeom.ConvexPolygon((lsst.sphgeom.UnitVector3d(1, 1, 0),
                                                  lsst.sphgeom.UnitVector3d(0, 1, 0),
                                                  lsst.sphgeom.UnitVector3d(0, 0, 1)))
        regionVisit = lsst.sphgeom.ConvexPolygon((lsst.sphgeom.UnitVector3d(1, 0, 0),
                                                  lsst.sphgeom.UnitVector3d(0, 1, 1),
                                                  lsst.sphgeom.UnitVector3d(0, 0, 1)))
        regionVisitDetector = lsst.sphgeom.ConvexPolygon((lsst.sphgeom.UnitVector3d(1, 0, 0),
                                                          lsst.sphgeom.UnitVector3d(0, 1, 0),
                                                          lsst.sphgeom.UnitVector3d(0, 1, 1)))
        for a, b in combinations((regionTract, regionPatch, regionVisit, regionVisitDetector), 2):
            self.assertNotEqual(a, b)

        # This depends on current schema.yaml definitions
        rows = list(registry.query('select count(*) as "cnt" from "PatchSkyPixJoin"'))
        self.assertEqual(rows[0]["cnt"], 0)

        # Add some dataunits
        registry.addDataUnitEntry("Camera", {"camera": "DummyCam"})
        registry.addDataUnitEntry("PhysicalFilter", {"camera": "DummyCam",
                                                     "physical_filter": "dummy_r",
                                                     "abstract_filter": "r"})
        registry.addDataUnitEntry("PhysicalFilter", {"camera": "DummyCam",
                                                     "physical_filter": "dummy_i",
                                                     "abstract_filter": "i"})
        for detector in (1, 2, 3, 4, 5):
            registry.addDataUnitEntry("Detector", {"camera": "DummyCam", "detector": detector})
        registry.addDataUnitEntry("Visit", {"camera": "DummyCam", "visit": 0, "physical_filter": "dummy_r"})
        registry.addDataUnitEntry("Visit", {"camera": "DummyCam", "visit": 1, "physical_filter": "dummy_i"})
        registry.addDataUnitEntry("SkyMap", {"skymap": "DummySkyMap", "hash": bytes()})
        registry.addDataUnitEntry("Tract", {"skymap": "DummySkyMap", "tract": 0, "region": regionTract})
        registry.addDataUnitEntry("Patch",
                                  {"skymap": "DummySkyMap",
                                   "tract": 0,
                                   "patch": 0,
                                   "cell_x": 0,
                                   "cell_y": 0,
                                   "region": regionPatch})
        registry.setDataUnitRegion(("Visit",),
                                   {"camera": "DummyCam", "visit": 0},
                                   regionVisit,
                                   update=True)
        registry.setDataUnitRegion(("Visit", "Detector"),
                                   {"camera": "DummyCam", "visit": 0, "detector": 2},
                                   regionVisitDetector,
                                   update=False)
        # Get region for a tract
        self.assertEqual(regionTract, registry.getRegion({"skymap": "DummySkyMap", "tract": 0}))
        # Attempt to get region for a non-existent tract
        self.assertIsNone(registry.getRegion({"skymap": "DummySkyMap", "tract": 1}))
        # Get region for a (tract, patch) combination
        self.assertEqual(regionPatch, registry.getRegion({"skymap": "DummySkyMap", "tract": 0, "patch": 0}))
        # Get region for a non-existent (tract, patch) combination
        self.assertIsNone(registry.getRegion({"skymap": "DummySkyMap", "tract": 0, "patch": 1}))
        # Get region for a visit
        self.assertEqual(regionVisit, registry.getRegion({"camera": "DummyCam", "visit": 0}))
        # Attempt to get region for a non-existent visit
        self.assertIsNone(registry.getRegion({"camera": "DummyCam", "visit": 10}))
        # Get region for a (visit, detector) combination
        self.assertEqual(regionVisitDetector,
                         registry.getRegion({"camera": "DummyCam", "visit": 0, "detector": 2}))
        # Attempt to get region for a non-existent (visit, detector) combination
        self.assertIsNone(registry.getRegion({"camera": "DummyCam", "visit": 0, "detector": 3}))
        # getRegion for a dataId containing no spatial dataunits should fail
        with self.assertRaises(KeyError):
            registry.getRegion({"camera": "DummyCam"})
        # getRegion for a mix of spatial dataunits should fail
        with self.assertRaises(KeyError):
            registry.getRegion({"camera": "DummyCam",
                                "visit": 0,
                                "detector": 2,
                                "skymap": "DummySkyMap",
                                "tract": 1})
        # Check if we can get the region for a skypix
        self.assertIsInstance(registry.getRegion({"skypix": 1000}), lsst.sphgeom.ConvexPolygon)
        # PatchSkyPixJoin should not be empty
        rows = list(registry.query('select count(*) as "cnt" from "PatchSkyPixJoin"'))
        self.assertNotEqual(rows[0]["cnt"], 0)


class SqlRegistryTestCase(lsst.utils.tests.TestCase, RegistryTests):
    """Test for SqlRegistry.
    """

    def makeRegistry(self):
        testDir = os.path.dirname(__file__)
        configFile = os.path.join(testDir, "config/basic/butler.yaml")
        butlerConfig = ButlerConfig(configFile)
        return Registry.fromConfig(butlerConfig, create=True)

    def testInitFromConfig(self):
        registry = self.makeRegistry()
        self.assertIsInstance(registry, SqlRegistry)
        self.assertFalse(registry.limited)


class LimitedSqlRegistryTestCase(lsst.utils.tests.TestCase, RegistryTests):
    """Test for SqlRegistry with limited=True.
    """

    def makeRegistry(self):
        testDir = os.path.dirname(__file__)
        configFile = os.path.join(testDir, "config/basic/butler.yaml")
        butlerConfig = ButlerConfig(configFile)
        butlerConfig["registry", "limited"] = True
        return Registry.fromConfig(butlerConfig, create=True)

    def testInitFromConfig(self):
        registry = self.makeRegistry()
        self.assertIsInstance(registry, SqlRegistry)
        self.assertTrue(registry.limited)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
