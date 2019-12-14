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
from __future__ import annotations

__all__ = ["RegistryTests"]

from abc import ABC, abstractmethod

import sqlalchemy

from ...core import (
    DatasetType,
    StorageClass,
)
from ..registry import Registry, ConflictingDefinitionError, OrphanedRecordError
from .. import ddl


class RegistryTests(ABC):
    """Generic tests for the `Registry` class that can be subclassed to
    generate tests for different configurations.
    """

    @abstractmethod
    def makeRegistry(self) -> Registry:
        raise NotImplementedError()

    def assertRowCount(self, registry: Registry, table: str, count: int):
        """Check the number of rows in table.
        """
        # TODO: all tests that rely on this method should be rewritten, as it
        # needs to depend on Registry implementation details to have any chance
        # of working.
        sql = sqlalchemy.sql.select(
            [sqlalchemy.sql.func.count()]
        ).select_from(
            getattr(registry._tables, table)
        )
        self.assertEqual(registry._db.query(sql).scalar(), count)

    def testOpaque(self):
        """Tests for `Registry.registerOpaqueTable`,
        `Registry.insertOpaqueData`, `Registry.fetchOpaqueData`, and
        `Registry.deleteOpaqueData`.
        """
        registry = self.makeRegistry()
        table = "opaque_table_for_testing"
        registry.registerOpaqueTable(
            table,
            spec=ddl.TableSpec(
                fields=[
                    ddl.FieldSpec("id", dtype=sqlalchemy.BigInteger, primaryKey=True),
                    ddl.FieldSpec("name", dtype=sqlalchemy.String, length=16, nullable=False),
                    ddl.FieldSpec("count", dtype=sqlalchemy.SmallInteger, nullable=True),
                ],
            )
        )
        rows = [
            {"id": 1, "name": "one", "count": None},
            {"id": 2, "name": "two", "count": 5},
            {"id": 3, "name": "three", "count": 6},
        ]
        registry.insertOpaqueData(table, *rows)
        self.assertCountEqual(rows, list(registry.fetchOpaqueData(table)))
        self.assertEqual(rows[0:1], list(registry.fetchOpaqueData(table, id=1)))
        self.assertEqual(rows[1:2], list(registry.fetchOpaqueData(table, name="two")))
        self.assertEqual([], list(registry.fetchOpaqueData(table, id=1, name="two")))
        registry.deleteOpaqueData(table, id=3)
        self.assertCountEqual(rows[:2], list(registry.fetchOpaqueData(table)))
        registry.deleteOpaqueData(table)
        self.assertEqual([], list(registry.fetchOpaqueData(table)))

    def testDatasetType(self):
        """Tests for `Registry.registerDatasetType` and
        `Registry.getDatasetType`.
        """
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

    def testDimensions(self):
        """Tests for `Registry.insertDimensionData` and
        `Registry.expandDataId`.
        """
        registry = self.makeRegistry()
        dimensionName = "instrument"
        dimension = registry.dimensions[dimensionName]
        dimensionValue = {"name": "DummyCam", "visit_max": 10, "exposure_max": 10, "detector_max": 2}
        registry.insertDimensionData(dimensionName, dimensionValue)
        # Inserting the same value twice should fail
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
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
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
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

    def testDataset(self):
        """Basic tests for `Registry.addDataset`, `Registry.getDataset`, and
        `Registry.removeDataset`.
        """
        registry = self.makeRegistry()
        run = "test"
        registry.registerRun(run)
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
        self.assertIsNone(registry.find(run, datasetType, dataId))

    def testComponents(self):
        """Tests for `Registry.attachComponent` and other dataset operations
        on composite datasets.
        """
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
        run = "test"
        registry.registerRun(run)
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
        self.assertIsNone(registry.find(run, parentDatasetType, dataId))
        self.assertIsNone(registry.find(run, childDatasetType1, dataId))
        self.assertIsNone(registry.find(run, childDatasetType2, dataId))

    def testFind(self):
        """Tests for `Registry.find`.
        """
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
        run = "test"
        dataId = {"instrument": "DummyCam", "visit": 0, "physical_filter": "d-r", "abstract_filter": None}
        registry.registerRun(run)
        inputRef = registry.addDataset(datasetType, dataId=dataId, run=run)
        outputRef = registry.find(run, datasetType, dataId)
        self.assertEqual(outputRef, inputRef)
        # Check that retrieval with invalid dataId raises
        with self.assertRaises(LookupError):
            dataId = {"instrument": "DummyCam", "abstract_filter": "g"}  # should be visit
            registry.find(run, datasetType, dataId)
        # Check that different dataIds match to different datasets
        dataId1 = {"instrument": "DummyCam", "visit": 1, "physical_filter": "d-r", "abstract_filter": None}
        inputRef1 = registry.addDataset(datasetType, dataId=dataId1, run=run)
        dataId2 = {"instrument": "DummyCam", "visit": 2, "physical_filter": "d-r", "abstract_filter": None}
        inputRef2 = registry.addDataset(datasetType, dataId=dataId2, run=run)
        dataId3 = {"instrument": "MyCam", "visit": 2, "physical_filter": "m-r", "abstract_filter": None}
        inputRef3 = registry.addDataset(datasetType, dataId=dataId3, run=run)
        self.assertEqual(registry.find(run, datasetType, dataId1), inputRef1)
        self.assertEqual(registry.find(run, datasetType, dataId2), inputRef2)
        self.assertEqual(registry.find(run, datasetType, dataId3), inputRef3)
        self.assertNotEqual(registry.find(run, datasetType, dataId1), inputRef2)
        self.assertNotEqual(registry.find(run, datasetType, dataId2), inputRef1)
        self.assertNotEqual(registry.find(run, datasetType, dataId3), inputRef1)
        # Check that requesting a non-existing dataId returns None
        nonExistingDataId = {"instrument": "DummyCam", "visit": 42}
        self.assertIsNone(registry.find(run, datasetType, nonExistingDataId))

    def testCollections(self):
        """Tests for `Registry.getAllCollections`, `Registry.registerRun`,
        `Registry.disassociate`, and interactions between collections and
        `Registry.find`.
        """
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
        run = "ingest"
        registry.registerRun(run)
        # Dataset.physical_filter should be populated as well here from the
        # visit Dimension values.
        dataId1 = {"instrument": "DummyCam", "visit": 0}
        inputRef1 = registry.addDataset(datasetType, dataId=dataId1, run=run)
        dataId2 = {"instrument": "DummyCam", "visit": 1}
        inputRef2 = registry.addDataset(datasetType, dataId=dataId2, run=run)
        # We should be able to find both datasets in their run
        outputRef = registry.find(run, datasetType, dataId1)
        self.assertEqual(outputRef, inputRef1)
        outputRef = registry.find(run, datasetType, dataId2)
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
        """Tests for `Registry.associate`.
        """
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
        run1 = "ingest1"
        registry.registerRun(run1)
        run2 = "ingest2"
        registry.registerRun(run2)
        run3 = "ingest3"
        registry.registerRun(run3)
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

    def testDatasetLocations(self):
        """Tests for `Registry.addDatasetLocation`,
        `Registry.getDatasetLocations`, and `Registry.removeDatasetLocations`.
        """
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
        run = "test"
        registry.registerRun(run)
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
