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
from itertools import combinations

from sqlalchemy import Table, Column, Integer
from sqlalchemy.schema import MetaData
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import Executable

import lsst.sphgeom

from lsst.daf.butler import (Run, DatasetType, Registry,
                             StorageClass, ButlerConfig,
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
        dimensions = registry.dimensions.extract(("instrument", "visit"))
        differentDimensions = registry.dimensions.extract(("instrument", "patch"))
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
        dimensions = registry.dimensions.extract(("instrument", "visit"))
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
        datasetType = DatasetType(name="testtype", dimensions=registry.dimensions.extract(("instrument",)),
                                  storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        dataId = {"instrument": "DummyCam"}
        registry.insertDimensionData("instrument", dataId)
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
        parentDatasetType = DatasetType(name="parent",
                                        dimensions=registry.dimensions.extract(("instrument",)),
                                        storageClass=parentStorageClass)
        childDatasetType1 = DatasetType(name="parent.child1",
                                        dimensions=registry.dimensions.extract(("instrument",)),
                                        storageClass=childStorageClass)
        childDatasetType2 = DatasetType(name="parent.child2",
                                        dimensions=registry.dimensions.extract(("instrument",)),
                                        storageClass=childStorageClass)
        registry.registerDatasetType(parentDatasetType)
        registry.registerDatasetType(childDatasetType1)
        registry.registerDatasetType(childDatasetType2)
        dataId = {"instrument": "DummyCam"}
        registry.insertDimensionData("instrument", dataId)
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

    def testDatasetLocations(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testStorageInfo")
        registry.storageClasses.registerStorageClass(storageClass)
        datasetType = DatasetType(name="test", dimensions=registry.dimensions.extract(("instrument",)),
                                  storageClass=storageClass)
        datasetType2 = DatasetType(name="test2", dimensions=registry.dimensions.extract(("instrument",)),
                                   storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        registry.registerDatasetType(datasetType2)
        registry.insertDimensionData("instrument", {"instrument": "DummyCam"})
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
        datasetType = DatasetType(name="dummytype",
                                  dimensions=registry.dimensions.extract(("instrument", "visit")),
                                  storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        registry.insertDimensionData("instrument",
                                     {"instrument": "DummyCam"},
                                     {"instrument": "MyCam"})
        registry.insertDimensionData("physical_filter",
                                     {"instrument": "DummyCam", "physical_filter": "d-r",
                                      "abstract_filter": "r"},
                                     {"instrument": "MyCam", "physical_filter": "m-r",
                                      "abstract_filter": "r"})
        registry.insertDimensionData("visit",
                                     {"instrument": "DummyCam", "id": 0, "name": "zero",
                                      "physical_filter": "d-r"},
                                     {"instrument": "DummyCam", "id": 1, "name": "one",
                                      "physical_filter": "d-r"},
                                     {"instrument": "DummyCam", "id": 2, "name": "two",
                                      "physical_filter": "d-r"},
                                     {"instrument": "MyCam", "id": 2, "name": "two",
                                      "physical_filter": "m-r"})
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
        datasetType = DatasetType(name="dummytype",
                                  dimensions=registry.dimensions.extract(("instrument", "visit")),
                                  storageClass=storageClass)
        registry.registerDatasetType(datasetType)
        registry.insertDimensionData("instrument", {"instrument": "DummyCam"})
        registry.insertDimensionData("physical_filter", {"instrument": "DummyCam", "physical_filter": "d-r",
                                                         "abstract_filter": "R"})
        registry.insertDimensionData("visit", {"instrument": "DummyCam", "id": 0, "name": "zero",
                                               "physical_filter": "d-r"})
        registry.insertDimensionData("visit", {"instrument": "DummyCam", "id": 1, "name": "one",
                                               "physical_filter": "d-r"})
        collection = "ingest"
        run = registry.makeRun(collection=collection)
        # Dataset.physical_filter should be populated as well here from the
        # visit Dimension values.
        dataId1 = {"instrument": "DummyCam", "visit": 0}
        inputRef1 = registry.addDataset(datasetType, dataId=dataId1, run=run)
        dataId2 = {"instrument": "DummyCam", "visit": 1}
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
        dimensions = registry.dimensions.extract(("instrument", "visit"))
        datasetType1 = DatasetType(name="dummytype", dimensions=dimensions, storageClass=storageClass)
        registry.registerDatasetType(datasetType1)
        datasetType2 = DatasetType(name="smartytype", dimensions=dimensions, storageClass=storageClass)
        registry.registerDatasetType(datasetType2)
        registry.insertDimensionData("instrument", {"instrument": "DummyCam"})
        registry.insertDimensionData("physical_filter", {"instrument": "DummyCam", "physical_filter": "d-r",
                                                         "abstract_filter": "R"})
        registry.insertDimensionData("visit", {"instrument": "DummyCam", "id": 0, "name": "zero",
                                               "physical_filter": "d-r"})
        registry.insertDimensionData("visit", {"instrument": "DummyCam", "id": 1, "name": "one",
                                               "physical_filter": "d-r"})
        run1 = registry.makeRun(collection="ingest1")
        run2 = registry.makeRun(collection="ingest2")
        run3 = registry.makeRun(collection="ingest3")
        # Dataset.physical_filter should be populated as well here
        # from the visit Dimension values.
        dataId1 = {"instrument": "DummyCam", "visit": 0}
        dataId2 = {"instrument": "DummyCam", "visit": 1}
        ref1_run1 = registry.addDataset(datasetType1, dataId=dataId1, run=run1)
        ref2_run1 = registry.addDataset(datasetType1, dataId=dataId2, run=run1)
        ref1_run2 = registry.addDataset(datasetType2, dataId=dataId1, run=run2)
        ref2_run2 = registry.addDataset(datasetType2, dataId=dataId2, run=run2)
        ref1_run3 = registry.addDataset(datasetType2, dataId=dataId1, run=run3)
        ref2_run3 = registry.addDataset(datasetType2, dataId=dataId2, run=run3)
        for ref in (ref1_run1, ref2_run1, ref1_run2, ref2_run2, ref1_run3, ref2_run3):
            self.assertEqual(ref.dataId.records["visit"].physical_filter, "d-r")
            self.assertEqual(ref.dataId.records["physical_filter"].abstract_filter, "R")
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

    def testDatasetDimensions(self):
        registry = self.makeRegistry()
        dimensionName = "instrument"
        dimension = registry.dimensions[dimensionName]
        dimensionValue = {"name": "DummyCam", "visit_max": 10, "exposure_max": 10, "detector_max": 2}
        registry.insertDimensionData(dimensionName, dimensionValue)
        # Inserting the same value twice should fail
        with self.assertRaises(IntegrityError):
            registry.insertDimensionData(dimensionName, dimensionValue)
        # expandDataId should retrieve the record we just inserted
        self.assertEqual(
            registry.expandDataId(
                instrument="DummyCam",
                graph=dimension.graph
            ).records[dimensionName].toDict(),
            dimensionValue
        )
        # expandDataId should raise if there is no record with the given ID.
        with self.assertRaises(LookupError):
            registry.expandDataId({"instrument": "Unknown"}, graph=dimension.graph)
        # abstract_filter doesn't have a table; insert should fail.
        with self.assertRaises(TypeError):
            registry.insertDimensionData("abstract_filter", {"abstract_filter": "i"})
        dimensionName2 = "physical_filter"
        dimension2 = registry.dimensions[dimensionName2]
        dimensionValue2 = {"name": "DummyCam_i", "abstract_filter": "i"}
        # Missing required dependency ("instrument") should fail
        with self.assertRaises(IntegrityError):
            registry.insertDimensionData(dimensionName2, dimensionValue2)
        # Adding required dependency should fix the failure
        dimensionValue2["instrument"] = "DummyCam"
        registry.insertDimensionData(dimensionName2, dimensionValue2)
        # expandDataId should retrieve the record we just inserted.
        self.assertEqual(
            registry.expandDataId(
                instrument="DummyCam", physical_filter="DummyCam_i",
                graph=dimension2.graph
            ).records[dimensionName2].toDict(),
            dimensionValue2
        )

    def testBasicTransaction(self):
        registry = self.makeRegistry()
        storageClass = StorageClass("testDatasetType")
        registry.storageClasses.registerStorageClass(storageClass)
        dimensions = registry.dimensions.extract(("instrument",))
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
                registry.insertDimensionData("instrument", {"instrument": "DummyCam"})
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
        with self.assertRaises(LookupError):
            registry.expandDataId({"instrument": "DummyCam"})

    def testRegions(self):
        registry = self.makeRegistry()
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

        # This depends on current schema.yaml and dimensions.yaml definitions
        self.assertEqual(len(list(registry.queryDimensions(["patch", "htm7"]))), 0)

        # Add some dimension entries
        registry.insertDimensionData("instrument", {"name": "DummyCam"})
        registry.insertDimensionData("physical_filter",
                                     {"instrument": "DummyCam", "name": "dummy_r", "abstract_filter": "r"},
                                     {"instrument": "DummyCam", "name": "dummy_i", "abstract_filter": "i"})
        for detector in (1, 2, 3, 4, 5):
            registry.insertDimensionData("detector", {"instrument": "DummyCam", "id": detector,
                                                      "full_name": str(detector)})
        registry.insertDimensionData("visit",
                                     {"instrument": "DummyCam", "id": 0, "name": "zero",
                                      "physical_filter": "dummy_r", "region": regionVisit},
                                     {"instrument": "DummyCam", "id": 1, "name": "one",
                                      "physical_filter": "dummy_i"})
        registry.insertDimensionData("skymap", {"skymap": "DummySkyMap", "hash": bytes()})
        registry.insertDimensionData("tract", {"skymap": "DummySkyMap", "tract": 0, "region": regionTract})
        registry.insertDimensionData("patch",
                                     {"skymap": "DummySkyMap", "tract": 0, "patch": 0,
                                      "cell_x": 0, "cell_y": 0, "region": regionPatch})
        registry.insertDimensionData("visit_detector_region",
                                     {"instrument": "DummyCam", "visit": 0, "detector": 2,
                                      "region": regionVisitDetector})

        def getRegion(dataId):
            return registry.expandDataId(dataId).region

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
        # combination.  This returns None rather than raising because we don't
        # want to require the region record to be present.
        self.assertIsNone(getRegion({"instrument": "DummyCam", "visit": 0, "detector": 3}))
        # getRegion for a dataId containing no spatial dimensions should
        # return None
        self.assertIsNone(getRegion({"instrument": "DummyCam"}))
        # getRegion for a mix of spatial dimensions should return
        # NotImplemented, at least until we get it implemented.
        self.assertIs(getRegion({"instrument": "DummyCam", "visit": 0, "detector": 2,
                                 "skymap": "DummySkyMap", "tract": 0}),
                      NotImplemented)
        # Check if we can get the region for a skypix
        self.assertIsInstance(getRegion({"htm9": 1000}), lsst.sphgeom.ConvexPolygon)
        # patch_htm7_overlap should not be empty
        self.assertNotEqual(len(list(registry.queryDimensions(["patch", "htm7"]))), 0)

    def testDimensionPacker(self):
        registry = self.makeRegistry()
        registry.insertDimensionData(
            "instrument",
            {"name": "DummyCam", "visit_max": 10, "exposure_max": 10, "detector_max": 2}
        )
        registry.insertDimensionData(
            "detector",
            {"instrument": "DummyCam", "id": 1, "full_name": "top"}
        )
        registry.insertDimensionData(
            "detector",
            {"instrument": "DummyCam", "id": 0, "full_name": "bottom"}
        )
        registry.insertDimensionData(
            "physical_filter",
            {"instrument": "DummyCam", "name": "R", "abstract_filter": "r"}
        )
        registry.insertDimensionData(
            "visit",
            {"instrument": "DummyCam", "id": 5, "name": "five", "physical_filter": "R"}
        )
        registry.insertDimensionData(
            "exposure",
            {"instrument": "DummyCam", "id": 4, "name": "four", "visit": 5, "physical_filter": "R"}
        )
        dataId0 = registry.expandDataId(instrument="DummyCam")
        with self.assertRaises(LookupError):
            dataId0.pack("visit_detector")
            dataId0.pack("exposure_detector")
        dataId1 = registry.expandDataId(dataId0, visit=5, detector=1)
        self.assertEqual(dataId1.pack("visit_detector"), 11)
        packer = registry.dimensions.makePacker("exposure_detector", dataId0)
        dataId2 = registry.expandDataId(dataId0, exposure=4, detector=0)
        self.assertEqual(packer.pack(dataId0, exposure=4, detector=0), 8)
        self.assertEqual(packer.pack(dataId2), 8)
        self.assertEqual(dataId2.pack("exposure_detector"), 8)
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

    def testNestedTransaction(self):
        registry = self.makeRegistry()
        dimension = registry.dimensions["instrument"]
        dataId1 = {"instrument": "DummyCam"}
        dataId2 = {"instrument": "DummyCam2"}
        checkpointReached = False
        with registry.transaction():
            # This should be added and (ultimately) committed.
            registry.insertDimensionData(dimension, dataId1)
            with self.assertRaises(IntegrityError):
                with registry.transaction():
                    # This does not conflict, and should succeed (but not
                    # be committed).
                    registry.insertDimensionData(dimension, dataId2)
                    checkpointReached = True
                    # This should conflict and raise, triggerring a rollback
                    # of the previous insertion within the same transaction
                    # context, but not the original insertion in the outer
                    # block.
                    registry.insertDimensionData(dimension, dataId1)
        self.assertTrue(checkpointReached)
        self.assertIsNotNone(registry.expandDataId(dataId1, graph=dimension.graph))
        with self.assertRaises(LookupError):
            registry.expandDataId(dataId2, graph=dimension.graph)

    def testInsertConflict(self):
        """Test for _insert method with different conflict options.
        """
        registry = self.makeRegistry()
        conn = registry._connection
        metadata = registry._schema.metadata

        table = Table("insert_conflict_test",
                      metadata,
                      Column('pk', Integer, primary_key=True),
                      Column('value', Integer),
                      Column('uniqval', Integer, unique=True))
        metadata.create_all(conn)

        # add initial data
        values = [
            dict(pk=0, value=0, uniqval=0),
            dict(pk=1, value=10, uniqval=100),
        ]
        conn.execute(table.insert(), values)
        rows = list(conn.execute(table.select()))
        self.assertCountEqual(rows, [(0, 0, 0), (1, 10, 100)])

        # try to insert with standard method, this fails
        with self.assertRaises(IntegrityError):
            conn.execute(table.insert(), values)

        # try to insert with ABORT option, this fails too
        with self.assertRaises(IntegrityError):
            registry._insert(table, values, onConflict=None)

        # IGNORE option only adds records with new PK
        values = [
            dict(pk=0, value=100, uniqval=1000),
            dict(pk=1, value=110, uniqval=2000),
            dict(pk=2, value=20, uniqval=200),
        ]
        registry._insert(table, values, onConflict="ignore")
        rows = list(conn.execute(table.select()))
        self.assertCountEqual(rows, [(0, 0, 0), (1, 10, 100), (2, 20, 200)])

        # REPLACE option replaces existing records and adds new
        values = [
            dict(pk=1, value=100, uniqval=10),
            dict(pk=2, value=200, uniqval=20),
            dict(pk=3, value=300, uniqval=30),
        ]
        registry._insert(table, values, onConflict="replace")
        rows = list(conn.execute(table.select()))
        self.assertCountEqual(rows, [(0, 0, 0), (1, 100, 10), (2, 200, 20), (3, 300, 30)])

        # Non-PK columns will still cause conflict
        values = [
            dict(pk=10, value=100, uniqval=10),
        ]
        for onConflict in [None, "ignore", "replace"]:
            with self.assertRaises(IntegrityError):
                registry._insert(table, values, onConflict=onConflict)


class TestOnConflictQueries(unittest.TestCase):
    """Test for onConflict support.

    This class tests dialect-specific query generators and does not require
    any pre-existing database or registry.
    """

    def makeTestTable(self):
        """Make a Table for tests below.

        No actual table is created, only metadata for table is instanciated.
        """
        metadata = MetaData()
        table = Table("insert_conflict_test",
                      metadata,
                      Column('pk', Integer, primary_key=True),
                      Column('value', Integer),
                      Column('uniqVal', Integer, unique=True))
        return table

    def testInsertConflictSqlite(self):
        """Test for SQLite implementation.
        """
        try:
            from lsst.daf.butler.registries.sqliteRegistry import InsertOnConflict
            from sqlalchemy.dialects import sqlite
        except ImportError:
            self.skipTest("Test skipped, cannot find imports for sqlite conflict testing")

        table = self.makeTestTable()

        expect = 'INSERT INTO insert_conflict_test (pk, value, "uniqVal") VALUES (?, ?, ?)' \
                 ' ON CONFLICT (pk) DO NOTHING'
        clause = InsertOnConflict(table, onConflict="ignore")
        self.assertIsInstance(clause, Executable)
        self.assertTrue(clause.supports_execution)
        query = clause.compile(dialect=sqlite.dialect())
        self.assertEqual(str(query), expect)

        expect = 'INSERT INTO insert_conflict_test (pk, value, "uniqVal") VALUES (?, ?, ?)' \
                 ' ON CONFLICT (pk) DO UPDATE SET value = excluded.value, "uniqVal" = excluded."uniqVal"'
        clause = InsertOnConflict(table, onConflict="replace")
        query = clause.compile(dialect=sqlite.dialect())
        self.assertEqual(str(query), expect)

    def testInsertConflictPG(self):
        """Test for PostgreSQL implementation.
        """
        try:
            from lsst.daf.butler.registries.postgresqlRegistry import PostgreSqlRegistry
            from sqlalchemy.dialects import postgresql
        except ImportError:
            self.skipTest("Test skipped, cannot find postgres imports")

        table = self.makeTestTable()

        expect = 'INSERT INTO insert_conflict_test (pk, value, \"uniqVal\")' \
                 ' VALUES (%(pk)s, %(value)s, %(uniqVal)s) ON CONFLICT (pk) DO NOTHING'
        clause = PostgreSqlRegistry._makeInsertWithConflictImpl(table, onConflict="ignore")
        self.assertIsInstance(clause, Executable)
        self.assertTrue(clause.supports_execution)
        query = clause.compile(dialect=postgresql.dialect())
        self.assertEqual(str(query), expect)

        expect = 'INSERT INTO insert_conflict_test (pk, value, "uniqVal")' \
                 ' VALUES (%(pk)s, %(value)s, %(uniqVal)s)' \
                 ' ON CONFLICT (pk) DO UPDATE SET value = excluded.value, "uniqVal" = excluded."uniqVal"'
        clause = PostgreSqlRegistry._makeInsertWithConflictImpl(table, onConflict="replace")
        query = clause.compile(dialect=postgresql.dialect())
        self.assertEqual(str(query), expect)

    def testInsertConflictOracle(self):
        """Test for Oracle implementation.
        """
        from lsst.daf.butler.registries.oracleRegistry import _Merge
        try:
            from sqlalchemy.dialects import oracle
        except ImportError:
            self.skipTest("Test skiped, cannot find Oracle imports")

        table = self.makeTestTable()

        expect = 'MERGE INTO insert_conflict_test t\n' \
                 'USING (SELECT :pk AS pk, :value AS value, :uniqVal AS "uniqVal" FROM DUAL) d\n' \
                 'ON (t.pk = d.pk)\n' \
                 'WHEN NOT MATCHED THEN INSERT (pk, value, "uniqVal") VALUES (d.pk, d.value, d."uniqVal")'
        clause = _Merge(table, onConflict="ignore")
        self.assertIsInstance(clause, Executable)
        self.assertTrue(clause.supports_execution)
        query = clause.compile(dialect=oracle.dialect())
        self.assertEqual(str(query), expect)

        expect = 'MERGE INTO insert_conflict_test t\n' \
                 'USING (SELECT :pk AS pk, :value AS value, :uniqVal AS "uniqVal" FROM DUAL) d\n' \
                 'ON (t.pk = d.pk)\n' \
                 'WHEN MATCHED THEN UPDATE SET t.value = d.value, t."uniqVal" = d."uniqVal"\n' \
                 'WHEN NOT MATCHED THEN INSERT (pk, value, "uniqVal") VALUES (d.pk, d.value, d."uniqVal")'
        clause = _Merge(table, onConflict="replace")
        self.assertIsInstance(clause, Executable)
        self.assertTrue(clause.supports_execution)
        query = clause.compile(dialect=oracle.dialect())
        self.assertEqual(str(query), expect)


if __name__ == "__main__":
    unittest.main()
