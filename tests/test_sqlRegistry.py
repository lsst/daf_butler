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
        dimensions = ("Instrument", "Visit")
        differentDimensions = ("Instrument", "Patch")
        inDatasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        # Inserting for the first time should return True
        self.assertTrue(registry.registerDatasetType(inDatasetType))
        outDatasetType = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType, inDatasetType)

        # Re-inserting should work
        self.assertFalse(registry.registerDatasetType(inDatasetType))
        # Except when they are not identical
        with self.assertRaises(ValueError):
            nonIdenticalDatasetType = DatasetType(datasetTypeName, differentDimensions, storageClass)
            registry.registerDatasetType(nonIdenticalDatasetType)

        # Template can be None
        datasetTypeName = "testNoneTemplate"
        storageClass = StorageClass("testDatasetType2")
        registry.storageClasses.registerStorageClass(storageClass)
        dimensions = ("Instrument", "Visit")
        inDatasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        registry.registerDatasetType(inDatasetType)
        outDatasetType = registry.getDatasetType(datasetTypeName)
        self.assertEqual(outDatasetType, inDatasetType)

    def testDataset(self):
        registry = self.makeRegistry()
        run = registry.makeRun(collection="test")
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="testtype", dimensions=("Instrument",), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        if not registry.limited:
            registry.addDimensionEntry("Instrument", {"instrument": "DummyCam"})
        ref = registry.addDataset(datasetType, dataId={"instrument": "DummyCam"}, run=run)
        outRef = registry.getDataset(ref.id)
        self.assertIsNotNone(ref.id)
        self.assertEqual(ref, outRef)
        with self.assertRaises(ValueError):
            ref = registry.addDataset(datasetType, dataId={"instrument": "DummyCam"}, run=run)

    def testComponents(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testComponents")
        registry.storageClasses.registerStorageClass(storageClass)
        parentDatasetType = DatasetType(name="parent", dimensions=("Instrument",), storageClass=storageClass)
        childDatasetType1 = DatasetType(name="child1", dimensions=("Instrument",), storageClass=storageClass)
        childDatasetType2 = DatasetType(name="child2", dimensions=("Instrument",), storageClass=storageClass)
        registry.registerDatasetType(parentDatasetType)
        registry.registerDatasetType(childDatasetType1)
        registry.registerDatasetType(childDatasetType2)
        if not registry.limited:
            registry.addDimensionEntry("Instrument", {"instrument": "DummyCam"})
        run = registry.makeRun(collection="test")
        parent = registry.addDataset(parentDatasetType, dataId={"instrument": "DummyCam"}, run=run)
        children = {"child1": registry.addDataset(childDatasetType1,
                                                  dataId={"instrument": "DummyCam"}, run=run),
                    "child2": registry.addDataset(childDatasetType2,
                                                  dataId={"instrument": "DummyCam"}, run=run)}
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
            registry.addDimensionEntry("Instrument", {"instrument": "DummyCam"})
        run = registry.makeRun(collection="test")
        storageClass = StorageClass("testQuantum")
        registry.storageClasses.registerStorageClass(storageClass)
        # Make two predicted inputs
        datasetType1 = DatasetType(name="dst1", dimensions=("Instrument",), storageClass=storageClass)
        registry.registerDatasetType(datasetType1)
        ref1 = registry.addDataset(datasetType1, dataId={"instrument": "DummyCam"}, run=run)
        datasetType2 = DatasetType(name="dst2", dimensions=("Instrument",), storageClass=storageClass)
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

    def testDatasetLocations(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testStorageInfo")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="test", dimensions=("Instrument",), storageClass=storageClass)
        datasetType2 = DatasetType(name="test2", dimensions=("Instrument",), storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        registry.registerDatasetType(datasetType2)
        if not registry.limited:
            registry.addDimensionEntry("Instrument", {"instrument": "DummyCam"})
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
        datasetType = DatasetType(name="dummytype", dimensions=("Instrument", "Visit"),
                                  storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        if not registry.limited:
            registry.addDimensionEntry("Instrument", {"instrument": "DummyCam"})
            registry.addDimensionEntry("Instrument", {"instrument": "MyCam"})
            registry.addDimensionEntry("PhysicalFilter",
                                       {"instrument": "DummyCam", "physical_filter": "d-r"})
            registry.addDimensionEntry("PhysicalFilter",
                                       {"instrument": "MyCam", "physical_filter": "m-r"})
            registry.addDimensionEntry("Visit",
                                       {"instrument": "DummyCam", "visit": 0, "physical_filter": "d-r"})
            registry.addDimensionEntry("Visit",
                                       {"instrument": "DummyCam", "visit": 1, "physical_filter": "d-r"})
            registry.addDimensionEntry("Visit",
                                       {"instrument": "DummyCam", "visit": 2, "physical_filter": "d-r"})
            registry.addDimensionEntry("Visit",
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
        datasetType = DatasetType(name="dummytype", dimensions=("Instrument", "Visit"),
                                  storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        if not registry.limited:
            registry.addDimensionEntry("Instrument", {"instrument": "DummyCam"})
            registry.addDimensionEntry("PhysicalFilter", {"instrument": "DummyCam", "physical_filter": "d-r"})
            registry.addDimensionEntry("Visit",
                                       {"instrument": "DummyCam", "visit": 0, "physical_filter": "d-r"})
            registry.addDimensionEntry("Visit",
                                       {"instrument": "DummyCam", "visit": 1, "physical_filter": "d-r"})
        collection = "ingest"
        run = registry.makeRun(collection=collection)
        # Dataset.physical_filter should be populated as well here from the
        # Visit Dimension values, if the Registry isn't limited.
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
        registry.disassociate(newCollection, [inputRef1, ], remove=False)  # TODO test with removal when done
        self.assertIsNone(registry.find(newCollection, datasetType, dataId1))
        outputRef = registry.find(newCollection, datasetType, dataId2)
        self.assertEqual(outputRef, inputRef2)

    def testAssociate(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testAssociate")
        registry.storageClasses.registerStorageClass(storageClass)
        dimensions = ("Instrument", "Visit")
        datasetType1 = DatasetType(name="dummytype", dimensions=dimensions, storageClass=storageClass)
        registry.registerDatasetType(datasetType1)
        datasetType2 = DatasetType(name="smartytype", dimensions=dimensions, storageClass=storageClass)
        registry.registerDatasetType(datasetType2)
        if not registry.limited:
            registry.addDimensionEntry("Instrument", {"instrument": "DummyCam"})
            registry.addDimensionEntry("PhysicalFilter", {"instrument": "DummyCam", "physical_filter": "d-r"})
            registry.addDimensionEntry("Visit", {"instrument": "DummyCam", "visit": 0,
                                       "physical_filter": "d-r"})
            registry.addDimensionEntry("Visit", {"instrument": "DummyCam", "visit": 1,
                                       "physical_filter": "d-r"})
        run1 = registry.makeRun(collection="ingest1")
        run2 = registry.makeRun(collection="ingest2")
        run3 = registry.makeRun(collection="ingest3")
        # Dataset.physical_filter should be populated as well here
        # from the Visit Dimension values, if the Registry isn't limited.
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
            self.assertEqual(ref.dataId.entries[registry.dimensions["Visit"]]["physical_filter"], "d-r")
            self.assertIsNone(ref.dataId.entries[registry.dimensions["PhysicalFilter"]]["abstract_filter"])
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
        dimensionName = "Instrument"
        dimensionValue = {"instrument": "DummyCam", "visit_max": 10, "exposure_max": 10, "detector_max": 2}
        if registry.limited:
            with self.assertRaises(NotImplementedError):
                registry.addDimensionEntry(dimensionName, dimensionValue)
            return  # the remainder of this test does not apply to limited Registry
        registry.addDimensionEntry(dimensionName, dimensionValue)
        # Inserting the same value twice should fail
        with self.assertRaises(ValueError):
            registry.addDimensionEntry(dimensionName, dimensionValue)
        # Find should return the entry
        self.assertEqual(registry.findDimensionEntry(dimensionName, dimensionValue), dimensionValue)
        # Find on a non-existant value should return None
        self.assertIsNone(registry.findDimensionEntry(dimensionName, {"instrument": "Unknown"}))
        # AbstractFilter doesn't have a table; should fail.
        with self.assertRaises(OperationalError):
            registry.addDimensionEntry("AbstractFilter", {"abstract_filter": "i"})
        dimensionName2 = "PhysicalFilter"
        dimensionValue2 = {"physical_filter": "DummyCam_i", "abstract_filter": "i"}
        # Missing required dependency ("instrument") should fail
        with self.assertRaises(LookupError):
            registry.addDimensionEntry(dimensionName2, dimensionValue2)
        # Adding required dependency should fix the failure
        dimensionValue2["instrument"] = "DummyCam"
        registry.addDimensionEntry(dimensionName2, dimensionValue2)
        # Find should return the entry
        self.assertEqual(registry.findDimensionEntry(dimensionName2, dimensionValue2), dimensionValue2)

    def testBasicTransaction(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testDatasetType")
        registry.storageClasses.registerStorageClass(storageClass)
        dimensions = ("Instrument", )
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
                    registry.addDimensionEntry("Instrument", {"instrument": "DummyCam"})
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
            self.assertIsNone(registry.findDimensionEntry("Instrument", {"instrument": "DummyCam"}))

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
        rows = list(registry.query('select count(*) as "cnt" from "PatchSkyPixJoin"'))
        self.assertEqual(rows[0]["cnt"], 0)

        # Add some dimension entries
        registry.addDimensionEntry("Instrument", {"instrument": "DummyCam"})
        registry.addDimensionEntry("PhysicalFilter", {"instrument": "DummyCam",
                                                      "physical_filter": "dummy_r",
                                                      "abstract_filter": "r"})
        registry.addDimensionEntry("PhysicalFilter", {"instrument": "DummyCam",
                                                      "physical_filter": "dummy_i",
                                                      "abstract_filter": "i"})
        for detector in (1, 2, 3, 4, 5):
            registry.addDimensionEntry("Detector", {"instrument": "DummyCam", "detector": detector})
        registry.addDimensionEntry("Visit",
                                   {"instrument": "DummyCam", "visit": 0, "physical_filter": "dummy_r"})
        registry.addDimensionEntry("Visit",
                                   {"instrument": "DummyCam", "visit": 1, "physical_filter": "dummy_i"})
        registry.addDimensionEntry("SkyMap", {"skymap": "DummySkyMap", "hash": bytes()})
        registry.addDimensionEntry("Tract", {"skymap": "DummySkyMap", "tract": 0, "region": regionTract})
        registry.addDimensionEntry("Patch",
                                   {"skymap": "DummySkyMap",
                                    "tract": 0,
                                    "patch": 0,
                                    "cell_x": 0,
                                    "cell_y": 0,
                                    "region": regionPatch})
        registry.setDimensionRegion({"instrument": "DummyCam", "visit": 0}, dimension="Visit",
                                    region=regionVisit, update=True)
        registry.setDimensionRegion(
            {"instrument": "DummyCam", "visit": 0, "detector": 2},
            dimensions=["Visit", "Detector"],
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
        # Attempt to get region for a non-existent (visit, detector) combination
        with self.assertRaises(LookupError):
            self.assertIsNone(getRegion({"instrument": "DummyCam", "visit": 0, "detector": 3}))
        # getRegion for a dataId containing no spatial dimensions should return None
        self.assertIsNone(getRegion({"instrument": "DummyCam"}))
        # getRegion for a mix of spatial dimensions should return None
        self.assertIsNone(getRegion({"instrument": "DummyCam",
                                     "visit": 0,
                                     "detector": 2,
                                     "skymap": "DummySkyMap",
                                     "tract": 1}))
        # Check if we can get the region for a skypix
        self.assertIsInstance(getRegion({"skypix": 1000}), lsst.sphgeom.ConvexPolygon)
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
