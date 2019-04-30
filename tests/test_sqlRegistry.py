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

import lsst.sphgeom

from sqlalchemy.exc import OperationalError
from lsst.daf.butler import (Execution, Quantum, Run, DatasetType, Registry,
                             StorageClass, ButlerConfig, DataId,
                             ConflictingDefinitionError, OrphanedRecordError)
from lsst.daf.butler.registries.sqlRegistry import SqlRegistry

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
        dimensions = ("instrument", "visit")
        differentDimensions = ("instrument", "patch")
        inDatasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        # Inserting for the first time should return True
        self.assertTrue(registry.registerDatasetType(inDatasetType))
        outDatasetType1 = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType1, inDatasetType)

        # Re-inserting should work
        self.assertFalse(registry.registerDatasetType(inDatasetType))
        # Except when they are not identical
        with self.assertRaises(ConflictingDefinitionError):
            nonIdenticalDatasetType = DatasetType(datasetTypeName, differentDimensions, storageClass)
            registry.registerDatasetType(nonIdenticalDatasetType)

        # Template can be None
        datasetTypeName = "testNoneTemplate"
        storageClass = StorageClass("testDatasetType2")
        registry.storageClasses.registerStorageClass(storageClass)
        dimensions = ("instrument", "visit")
        inDatasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        registry.registerDatasetType(inDatasetType)
        outDatasetType2 = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType2, inDatasetType)

        allTypes = registry.getAllDatasetTypes()
        self.assertEqual(allTypes, {outDatasetType1, outDatasetType2})

    def testDataset(self):
        registry = self.makeRegistry()
        run = registry.makeRun(collection="test")
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="testtype", dimensions=("instrument",), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        dataId = {"instrument": "DummyCam"}
        if not registry.limited:
            registry.addDimensionEntry("instrument", dataId)
        ref = registry.addDataset(datasetType, dataId=dataId, run=run)
        outRef = registry.getDataset(ref.id)
        self.assertIsNotNone(ref.id)
        self.assertEqual(ref, outRef)
        with self.assertRaises(ConflictingDefinitionError):
            ref = registry.addDataset(datasetType, dataId=dataId, run=run)
        registry.removeDataset(ref)
        self.assertIsNone(registry.find(run.collection, datasetType, dataId))

    def testComponents(self):
        registry = self.makeRegistry()
        childStorageClass = StorageClass("testComponentsChild")
        registry.storageClasses.registerStorageClass(childStorageClass)
        parentStorageClass = StorageClass("testComponentsParent",
                                          components={"child1": childStorageClass,
                                                      "child2": childStorageClass})
        registry.storageClasses.registerStorageClass(parentStorageClass)
        parentDatasetType = DatasetType(name="parent", dimensions=("instrument",),
                                        storageClass=parentStorageClass)
        childDatasetType1 = DatasetType(name="parent.child1", dimensions=("instrument",),
                                        storageClass=childStorageClass)
        childDatasetType2 = DatasetType(name="parent.child2", dimensions=("instrument",),
                                        storageClass=childStorageClass)
        registry.registerDatasetType(parentDatasetType)
        registry.registerDatasetType(childDatasetType1)
        registry.registerDatasetType(childDatasetType2)
        dataId = {"instrument": "DummyCam"}
        if not registry.limited:
            registry.addDimensionEntry("instrument", dataId)
        run = registry.makeRun(collection="test")
        parent = registry.addDataset(parentDatasetType, dataId=dataId, run=run)
        children = {"child1": registry.addDataset(childDatasetType1, dataId=dataId, run=run),
                    "child2": registry.addDataset(childDatasetType2, dataId=dataId, run=run)}
        for name, child in children.items():
            registry.attachComponent(name, parent, child)
        self.assertEqual(parent.components, children)
        outParent = registry.getDataset(parent.id)
        self.assertEqual(outParent.components, children)
        # Remove the parent; this should remove both children.
        registry.removeDataset(parent)
        self.assertIsNone(registry.find(run.collection, parentDatasetType, dataId))
        self.assertIsNone(registry.find(run.collection, childDatasetType1, dataId))
        self.assertIsNone(registry.find(run.collection, childDatasetType2, dataId))

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
        with self.assertRaises(ConflictingDefinitionError):
            registry.makeRun("one")
        # Insert a new Run and check that ensureRun silently ignores it
        collection = "dummy"
        run = registry.makeRun(collection)
        registry.ensureRun(run)
        # Calling ensureRun with a different Run with the same id should fail
        run2 = Run(collection="hello")
        run2._id = run.id
        with self.assertRaises(ConflictingDefinitionError):
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
            registry.addDimensionEntry("instrument", {"instrument": "DummyCam"})
        run = registry.makeRun(collection="test")
        storageClass = StorageClass("testQuantum")
        registry.storageClasses.registerStorageClass(storageClass)
        # Make two predicted inputs
        datasetType1 = DatasetType(name="dst1", dimensions=("instrument",), storageClass=storageClass)
        registry.registerDatasetType(datasetType1)
        ref1 = registry.addDataset(datasetType1, dataId={"instrument": "DummyCam"}, run=run)
        datasetType2 = DatasetType(name="dst2", dimensions=("instrument",), storageClass=storageClass)
        registry.registerDatasetType(datasetType2)
        ref2 = registry.addDataset(datasetType2, dataId={"instrument": "DummyCam"}, run=run)
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
        # Removing a predictedInput dataset should be enough to remove the
        # Quantum; we don't want to allow Quantums with inaccurate information
        # to exist.
        registry.removeDataset(ref1)
        self.assertIsNone(registry.getQuantum(quantum.id))

    def testDatasetLocations(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testStorageInfo")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="test", dimensions=("instrument",), storageClass=storageClass)
        datasetType2 = DatasetType(name="test2", dimensions=("instrument",), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        registry.registerDatasetType(datasetType2)
        if not registry.limited:
            registry.addDimensionEntry("instrument", {"instrument": "DummyCam"})
        run = registry.makeRun(collection="test")
        ref = registry.addDataset(datasetType, dataId={"instrument": "DummyCam"}, run=run)
        ref2 = registry.addDataset(datasetType2, dataId={"instrument": "DummyCam"}, run=run)
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
        with self.assertRaises(OrphanedRecordError):
            registry.removeDataset(ref)
        registry.removeDatasetLocation(datastoreName2, ref)
        addresses = registry.getDatasetLocations(ref)
        self.assertEqual(len(addresses), 0)
        self.assertNotIn(datastoreName2, addresses)
        registry.removeDataset(ref)  # should not raise
        addresses = registry.getDatasetLocations(ref2)
        self.assertEqual(len(addresses), 1)
        self.assertIn(datastoreName2, addresses)

    def testFind(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testFind")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="dummytype", dimensions=("instrument", "visit"),
                                  storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        if not registry.limited:
            registry.addDimensionEntryList("instrument", [{"instrument": "DummyCam"},
                                                          {"instrument": "MyCam"}])
            registry.addDimensionEntry("physical_filter",
                                       {"instrument": "DummyCam", "physical_filter": "d-r"})
            registry.addDimensionEntry("physical_filter",
                                       {"instrument": "MyCam", "physical_filter": "m-r"})
            registry.addDimensionEntry("visit",
                                       {"instrument": "DummyCam", "visit": 0, "physical_filter": "d-r"})
            registry.addDimensionEntry("visit",
                                       {"instrument": "DummyCam", "visit": 1, "physical_filter": "d-r"})
            registry.addDimensionEntry("visit",
                                       {"instrument": "DummyCam", "visit": 2, "physical_filter": "d-r"})
            registry.addDimensionEntry("visit",
                                       {"instrument": "MyCam", "visit": 2, "physical_filter": "m-r"})
        collection = "test"
        dataId = {"instrument": "DummyCam", "visit": 0, "physical_filter": "d-r", "abstract_filter": None}
        run = registry.makeRun(collection=collection)
        inputRef = registry.addDataset(datasetType, dataId=dataId, run=run)
        outputRef = registry.find(collection, datasetType, dataId)
        self.assertEqual(outputRef, inputRef)
        # Check that retrieval with invalid dataId raises
        with self.assertRaises(LookupError):
            dataId = {"instrument": "DummyCam", "abstract_filter": "g"}  # should be visit
            registry.find(collection, datasetType, dataId)
        # Check that different dataIds match to different datasets
        dataId1 = {"instrument": "DummyCam", "visit": 1, "physical_filter": "d-r", "abstract_filter": None}
        inputRef1 = registry.addDataset(datasetType, dataId=dataId1, run=run)
        dataId2 = {"instrument": "DummyCam", "visit": 2, "physical_filter": "d-r", "abstract_filter": None}
        inputRef2 = registry.addDataset(datasetType, dataId=dataId2, run=run)
        dataId3 = {"instrument": "MyCam", "visit": 2, "physical_filter": "m-r", "abstract_filter": None}
        inputRef3 = registry.addDataset(datasetType, dataId=dataId3, run=run)
        self.assertEqual(registry.find(collection, datasetType, dataId1), inputRef1)
        self.assertEqual(registry.find(collection, datasetType, dataId2), inputRef2)
        self.assertEqual(registry.find(collection, datasetType, dataId3), inputRef3)
        self.assertNotEqual(registry.find(collection, datasetType, dataId1), inputRef2)
        self.assertNotEqual(registry.find(collection, datasetType, dataId2), inputRef1)
        self.assertNotEqual(registry.find(collection, datasetType, dataId3), inputRef1)
        # Check that requesting a non-existing dataId returns None
        nonExistingDataId = {"instrument": "DummyCam", "visit": 42}
        self.assertIsNone(registry.find(collection, datasetType, nonExistingDataId))

    def testCollections(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testCollections")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="dummytype", dimensions=("instrument", "visit"),
                                  storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        if not registry.limited:
            registry.addDimensionEntry("instrument", {"instrument": "DummyCam"})
            registry.addDimensionEntry("physical_filter",
                                       {"instrument": "DummyCam", "physical_filter": "d-r"})
            registry.addDimensionEntry("visit",
                                       {"instrument": "DummyCam", "visit": 0, "physical_filter": "d-r"})
            registry.addDimensionEntry("visit",
                                       {"instrument": "DummyCam", "visit": 1, "physical_filter": "d-r"})
        collection = "ingest"
        run = registry.makeRun(collection=collection)
        # Dataset.physical_filter should be populated as well here from the
        # visit Dimension values, if the Registry isn't limited.
        dataId1 = {"instrument": "DummyCam", "visit": 0}
        if registry.limited:
            dataId1.update(physical_filter="d-r", abstract_filter=None)
        inputRef1 = registry.addDataset(datasetType, dataId=dataId1, run=run)
        dataId2 = {"instrument": "DummyCam", "visit": 1}
        if registry.limited:
            dataId2.update(physical_filter="d-r", abstract_filter=None)
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
        registry.disassociate(newCollection, [inputRef1, ])
        self.assertIsNone(registry.find(newCollection, datasetType, dataId1))
        outputRef = registry.find(newCollection, datasetType, dataId2)
        self.assertEqual(outputRef, inputRef2)

        collections = registry.getAllCollections()
        self.assertEqual(collections, {"something", "ingest"})

    def testAssociate(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testAssociate")
        registry.storageClasses.registerStorageClass(storageClass)
        dimensions = ("instrument", "visit")
        datasetType1 = DatasetType(name="dummytype", dimensions=dimensions, storageClass=storageClass)
        registry.registerDatasetType(datasetType1)
        datasetType2 = DatasetType(name="smartytype", dimensions=dimensions, storageClass=storageClass)
        registry.registerDatasetType(datasetType2)
        if not registry.limited:
            registry.addDimensionEntry("instrument", {"instrument": "DummyCam"})
            registry.addDimensionEntry("physical_filter",
                                       {"instrument": "DummyCam", "physical_filter": "d-r"})
            registry.addDimensionEntry("visit", {"instrument": "DummyCam", "visit": 0,
                                       "physical_filter": "d-r"})
            registry.addDimensionEntry("visit", {"instrument": "DummyCam", "visit": 1,
                                       "physical_filter": "d-r"})
        run1 = registry.makeRun(collection="ingest1")
        run2 = registry.makeRun(collection="ingest2")
        run3 = registry.makeRun(collection="ingest3")
        # Dataset.physical_filter should be populated as well here
        # from the visit Dimension values, if the Registry isn't limited.
        dataId1 = {"instrument": "DummyCam", "visit": 0}
        dataId2 = {"instrument": "DummyCam", "visit": 1}
        if registry.limited:
            dataId1.update(physical_filter="d-r", abstract_filter=None)
            dataId2.update(physical_filter="d-r", abstract_filter=None)
        ref1_run1 = registry.addDataset(datasetType1, dataId=dataId1, run=run1)
        ref2_run1 = registry.addDataset(datasetType1, dataId=dataId2, run=run1)
        ref1_run2 = registry.addDataset(datasetType2, dataId=dataId1, run=run2)
        ref2_run2 = registry.addDataset(datasetType2, dataId=dataId2, run=run2)
        ref1_run3 = registry.addDataset(datasetType2, dataId=dataId1, run=run3)
        ref2_run3 = registry.addDataset(datasetType2, dataId=dataId2, run=run3)
        for ref in (ref1_run1, ref2_run1, ref1_run2, ref2_run2, ref1_run3, ref2_run3):
            self.assertEqual(ref.dataId.entries[registry.dimensions["visit"]]["physical_filter"], "d-r")
            self.assertIsNone(ref.dataId.entries[registry.dimensions["physical_filter"]]["abstract_filter"])
        # should have exactly 4 rows in Dataset
        self.assertRowCount(registry, "dataset", 6)
        self.assertRowCount(registry, "dataset_collection", 6)
        # adding same DatasetRef to the same run is an error
        with self.assertRaises(ConflictingDefinitionError):
            registry.addDataset(datasetType1, dataId=dataId2, run=run1)
        # above exception must rollback and not add anything to Dataset
        self.assertRowCount(registry, "dataset", 6)
        self.assertRowCount(registry, "dataset_collection", 6)
        # associated refs from run1 with some other collection
        newCollection = "something"
        registry.associate(newCollection, [ref1_run1, ref2_run1])
        self.assertRowCount(registry, "dataset_collection", 8)
        # associating same exact DatasetRef is OK (not doing anything),
        # two cases to test - single-ref and many-refs
        registry.associate(newCollection, [ref1_run1])
        registry.associate(newCollection, [ref1_run1, ref2_run1])
        self.assertRowCount(registry, "dataset_collection", 8)
        # associated refs from run2 with same other collection, this should
        # be OK because thy have different dataset type
        registry.associate(newCollection, [ref1_run2, ref2_run2])
        self.assertRowCount(registry, "dataset_collection", 10)
        # associating DatasetRef with the same units but different ID is not OK
        with self.assertRaises(ConflictingDefinitionError):
            registry.associate(newCollection, [ref1_run3])
        with self.assertRaises(ConflictingDefinitionError):
            registry.associate(newCollection, [ref1_run3, ref2_run3])

    def testDatasetUnit(self):
        registry = self.makeRegistry()
        dimensionName = "instrument"
        dimensionValue = {"instrument": "DummyCam", "visit_max": 10, "exposure_max": 10, "detector_max": 2}
        if registry.limited:
            with self.assertRaises(NotImplementedError):
                registry.addDimensionEntry(dimensionName, dimensionValue)
            return  # the remainder of this test does not apply to limited Registry
        registry.addDimensionEntry(dimensionName, dimensionValue)
        # Inserting the same value twice should fail
        with self.assertRaises(ConflictingDefinitionError):
            registry.addDimensionEntry(dimensionName, dimensionValue)
        # Find should return the entry
        self.assertEqual(registry.findDimensionEntry(dimensionName, dimensionValue), dimensionValue)
        # Find on a non-existant value should return None
        self.assertIsNone(registry.findDimensionEntry(dimensionName, {"instrument": "Unknown"}))
        # abstract_filter doesn't have a table; should fail.
        with self.assertRaises(OperationalError):
            registry.addDimensionEntry("abstract_filter", {"abstract_filter": "i"})
        dimensionName2 = "physical_filter"
        dimensionValue2 = {"physical_filter": "DummyCam_i", "abstract_filter": "i"}
        # Missing required dependency ("instrument") should fail
        with self.assertRaises(LookupError):
            registry.addDimensionEntry(dimensionName2, dimensionValue2)
        # Adding required dependency should fix the failure
        dimensionValue2["instrument"] = "DummyCam"
        registry.addDimensionEntry(dimensionName2, dimensionValue2)
        # Find should return the entry
        self.assertEqual(registry.findDimensionEntry(dimensionName2, dimensionValue2), dimensionValue2)

        # Get all the instrument values
        instrumentEntries = registry.findDimensionEntries(dimensionName)
        instruments = {e["instrument"] for e in instrumentEntries}
        self.assertTrue(len(instruments), 1)
        self.assertIn(dimensionValue["instrument"], instruments)

        # Add a new instrument
        dimensionValue3 = {"instrument": "DummyCam2", "visit_max": 10, "exposure_max": 10, "detector_max": 2}
        registry.addDimensionEntry(dimensionName, dimensionValue3)

        instrumentEntries = registry.findDimensionEntries(dimensionName)
        instruments = {e["instrument"] for e in instrumentEntries}
        self.assertEqual(instruments, {"DummyCam", "DummyCam2"})

    def testBasicTransaction(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testDatasetType")
        registry.storageClasses.registerStorageClass(storageClass)
        dimensions = ("instrument", )
        dataId = {"instrument": "DummyCam"}
        datasetTypeA = DatasetType(name="A",
                                   dimensions=dimensions,
                                   storageClass=storageClass)
        datasetTypeB = DatasetType(name="B",
                                   dimensions=dimensions,
                                   storageClass=storageClass)
        datasetTypeC = DatasetType(name="C",
                                   dimensions=dimensions,
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
                    registry.addDimensionEntry("instrument", {"instrument": "DummyCam"})
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
        # Or the Dimension entries
        if not registry.limited:
            self.assertIsNone(registry.findDimensionEntry("instrument", {"instrument": "DummyCam"}))

    def testRegions(self):
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
        rows = list(registry.query('select count(*) as "cnt" from "patch_skypix_join"'))
        self.assertEqual(rows[0]["cnt"], 0)

        # Add some dimension entries
        registry.addDimensionEntry("instrument", {"instrument": "DummyCam"})
        registry.addDimensionEntry("physical_filter",
                                   {"instrument": "DummyCam", "physical_filter": "dummy_r",
                                    "abstract_filter": "r"})
        registry.addDimensionEntry("physical_filter",
                                   {"instrument": "DummyCam", "physical_filter": "dummy_i",
                                    "abstract_filter": "i"})
        for detector in (1, 2, 3, 4, 5):
            registry.addDimensionEntry("detector", {"instrument": "DummyCam", "detector": detector})
        registry.addDimensionEntry("visit",
                                   {"instrument": "DummyCam", "visit": 0, "physical_filter": "dummy_r"})
        registry.addDimensionEntry("visit",
                                   {"instrument": "DummyCam", "visit": 1, "physical_filter": "dummy_i"})
        registry.addDimensionEntry("skymap", {"skymap": "DummySkyMap", "hash": bytes()})
        registry.addDimensionEntry("tract", {"skymap": "DummySkyMap", "tract": 0, "region": regionTract})
        registry.addDimensionEntry("patch",
                                   {"skymap": "DummySkyMap",
                                    "tract": 0,
                                    "patch": 0,
                                    "cell_x": 0,
                                    "cell_y": 0,
                                    "region": regionPatch})
        registry.setDimensionRegion({"instrument": "DummyCam", "visit": 0}, dimension="visit",
                                    region=regionVisit, update=True)
        registry.setDimensionRegion(
            {"instrument": "DummyCam", "visit": 0, "detector": 2},
            dimensions=["visit", "detector"],
            region=regionVisitDetector,
            update=False
        )

        def getRegion(dataId):
            return registry.expandDataId(dataId, region=True).region

        # Get region for a tract
        self.assertEqual(regionTract, getRegion({"skymap": "DummySkyMap", "tract": 0}))
        # Attempt to get region for a non-existent tract
        with self.assertRaises(LookupError):
            getRegion({"skymap": "DummySkyMap", "tract": 1})
        # Get region for a (tract, patch) combination
        self.assertEqual(regionPatch, getRegion({"skymap": "DummySkyMap", "tract": 0, "patch": 0}))
        # Get region for a non-existent (tract, patch) combination
        with self.assertRaises(LookupError):
            getRegion({"skymap": "DummySkyMap", "tract": 0, "patch": 1})
        # Get region for a visit
        self.assertEqual(regionVisit, getRegion({"instrument": "DummyCam", "visit": 0}))
        # Attempt to get region for a non-existent visit
        with self.assertRaises(LookupError):
            getRegion({"instrument": "DummyCam", "visit": 10})
        # Get region for a (visit, detector) combination
        self.assertEqual(regionVisitDetector,
                         getRegion({"instrument": "DummyCam", "visit": 0, "detector": 2}))
        # Attempt to get region for a non-existent (visit, detector)
        # combination
        with self.assertRaises(LookupError):
            getRegion({"instrument": "DummyCam", "visit": 0, "detector": 3})
        # getRegion for a dataId containing no spatial dimensions should
        # return None
        self.assertIsNone(getRegion({"instrument": "DummyCam"}))
        # getRegion for a mix of spatial dimensions should return None
        self.assertIsNone(getRegion({"instrument": "DummyCam",
                                     "visit": 0,
                                     "detector": 2,
                                     "skymap": "DummySkyMap",
                                     "tract": 1}))
        # Check if we can get the region for a skypix
        self.assertIsInstance(getRegion({"skypix": 1000}), lsst.sphgeom.ConvexPolygon)
        # patch_skypix_join should not be empty
        rows = list(registry.query('select count(*) as "cnt" from "patch_skypix_join"'))
        self.assertNotEqual(rows[0]["cnt"], 0)

    def testDataIdPacker(self):
        registry = self.makeRegistry()
        if registry.limited:
            return
        registry.addDimensionEntry(
            "instrument",
            {"instrument": "DummyCam", "visit_max": 10, "exposure_max": 10, "detector_max": 2}
        )
        registry.addDimensionEntry(
            "physical_filter",
            {"instrument": "DummyCam", "physical_filter": "R", "abstract_filter": "r"}
        )
        registry.addDimensionEntry(
            "visit",
            {"instrument": "DummyCam", "visit": 5, "physical_filter": "R"}
        )
        registry.addDimensionEntry(
            "exposure",
            {"instrument": "DummyCam", "exposure": 4, "visit": 5, "physical_filter": "R"}
        )
        dataId0 = registry.expandDataId(instrument="DummyCam")
        with self.assertRaises(LookupError):
            registry.packDataId("visit_detector", dataId0)
            registry.packDataId("exposure_detector", dataId0)
        dataId1 = DataId(dataId0, visit=5, detector=1)
        self.assertEqual(registry.packDataId("visit_detector", dataId1), 11)
        packer = registry.makeDataIdPacker("exposure_detector", dataId0)
        dataId2 = DataId(dataId0, exposure=4, detector=0)
        self.assertEqual(packer.pack(dataId0, exposure=4, detector=0), 8)
        self.assertEqual(packer.pack(dataId2), 8)
        self.assertEqual(registry.packDataId("exposure_detector", dataId2), 8)
        dataId2a = packer.unpack(8)
        self.assertEqual(dataId2, dataId2a)


class SqlRegistryTestCase(unittest.TestCase, RegistryTests):
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

    def testNestedTransaction(self):
        registry = self.makeRegistry()
        dimension = "instrument"
        dataId1 = {"instrument": "DummyCam"}
        dataId2 = {"instrument": "DummyCam2"}
        checkpointReached = False
        with registry.transaction():
            # This should be added and (ultimately) committed.
            registry.addDimensionEntry(dimension, dataId1)
            with self.assertRaises(ConflictingDefinitionError):
                with registry.transaction():
                    # This does not conflict, and should succeed (but not
                    # be committed).
                    registry.addDimensionEntry(dimension, dataId2)
                    checkpointReached = True
                    # This should conflict and raise, triggerring a rollback
                    # of the previous insertion within the same transaction
                    # context, but not the original insertion in the outer
                    # block.
                    registry.addDimensionEntry(dimension, dataId1)
        self.assertTrue(checkpointReached)
        self.assertIsNotNone(registry.findDimensionEntry(dimension, dataId1))
        self.assertIsNone(registry.findDimensionEntry(dimension, dataId2))


class LimitedSqlRegistryTestCase(unittest.TestCase, RegistryTests):
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


if __name__ == "__main__":
    unittest.main()
