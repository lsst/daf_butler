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
from ..registry import Registry, ConflictingDefinitionError
from .. import ddl


class RegistryTests(ABC):
    """Generic tests for the `Registry` class that can be subclassed to
    generate tests for different configurations.
    """

    @abstractmethod
    def makeRegistry(self) -> Registry:
        raise NotImplementedError()

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
