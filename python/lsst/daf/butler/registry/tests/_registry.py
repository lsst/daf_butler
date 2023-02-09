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

import itertools
import logging
import os
import re
import unittest
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Iterator, Optional, Type, Union

import astropy.time
import sqlalchemy

try:
    import numpy as np
except ImportError:
    np = None

import lsst.sphgeom
from lsst.daf.relation import RelationalAlgebraError

from ...core import (
    DataCoordinate,
    DataCoordinateSet,
    DatasetAssociation,
    DatasetRef,
    DatasetType,
    DimensionGraph,
    NamedValueSet,
    SkyPixDimension,
    StorageClass,
    Timespan,
    ddl,
)
from .._collection_summary import CollectionSummary
from .._collectionType import CollectionType
from .._config import RegistryConfig
from .._exceptions import (
    ArgumentError,
    CollectionError,
    CollectionTypeError,
    ConflictingDefinitionError,
    DataIdValueError,
    DatasetTypeError,
    InconsistentDataIdError,
    MissingCollectionError,
    MissingDatasetTypeError,
    OrphanedRecordError,
)
from ..interfaces import ButlerAttributeExistsError, DatasetIdGenEnum

if TYPE_CHECKING:
    from .._registry import Registry


class RegistryTests(ABC):
    """Generic tests for the `Registry` class that can be subclassed to
    generate tests for different configurations.
    """

    collectionsManager: Optional[str] = None
    """Name of the collections manager class, if subclass provides value for
    this member then it overrides name specified in default configuration
    (`str`).
    """

    datasetsManager: Optional[str] = None
    """Name of the datasets manager class, if subclass provides value for
    this member then it overrides name specified in default configuration
    (`str`).
    """

    @classmethod
    @abstractmethod
    def getDataDir(cls) -> str:
        """Return the root directory containing test data YAML files."""
        raise NotImplementedError()

    def makeRegistryConfig(self) -> RegistryConfig:
        """Create RegistryConfig used to create a registry.

        This method should be called by a subclass from `makeRegistry`.
        Returned instance will be pre-configured based on the values of class
        members, and default-configured for all other parameters. Subclasses
        that need default configuration should just instantiate
        `RegistryConfig` directly.
        """
        config = RegistryConfig()
        if self.collectionsManager:
            config["managers", "collections"] = self.collectionsManager
        if self.datasetsManager:
            config["managers", "datasets"] = self.datasetsManager
        return config

    @abstractmethod
    def makeRegistry(self, share_repo_with: Optional[Registry] = None) -> Optional[Registry]:
        """Return the Registry instance to be tested.

        Parameters
        ----------
        share_repo_with : `Registry`, optional
            If provided, the new registry should point to the same data
            repository as this existing registry.

        Returns
        -------
        registry : `Registry`
            New `Registry` instance, or `None` *only* if `share_repo_with` is
            not `None` and this test case does not support that argument
            (e.g. it is impossible with in-memory SQLite DBs).
        """
        raise NotImplementedError()

    def loadData(self, registry: Registry, filename: str):
        """Load registry test data from ``getDataDir/<filename>``,
        which should be a YAML import/export file.
        """
        from ...transfers import YamlRepoImportBackend

        with open(os.path.join(self.getDataDir(), filename), "r") as stream:
            backend = YamlRepoImportBackend(stream, registry)
        backend.register()
        backend.load(datastore=None)

    def checkQueryResults(self, results, expected):
        """Check that a query results object contains expected values.

        Parameters
        ----------
        results : `DataCoordinateQueryResults` or `DatasetQueryResults`
            A lazy-evaluation query results object.
        expected : `list`
            A list of `DataCoordinate` o `DatasetRef` objects that should be
            equal to results of the query, aside from ordering.
        """
        self.assertCountEqual(list(results), expected)
        self.assertEqual(results.count(), len(expected))
        if expected:
            self.assertTrue(results.any())
        else:
            self.assertFalse(results.any())

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
            ),
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
        self.assertEqual(rows[0:1], list(registry.fetchOpaqueData(table, id=(1, 3), name=("one", "two"))))
        self.assertEqual(rows, list(registry.fetchOpaqueData(table, id=(1, 2, 3))))
        # Test very long IN clause which exceeds sqlite limit on number of
        # parameters. SQLite says the limit is 32k but it looks like it is
        # much higher.
        self.assertEqual(rows, list(registry.fetchOpaqueData(table, id=list(range(300_000)))))
        # Two IN clauses, each longer than 1k batch size, first with
        # duplicates, second has matching elements in different batches (after
        # sorting).
        self.assertEqual(
            rows[0:2],
            list(
                registry.fetchOpaqueData(
                    table,
                    id=list(range(1000)) + list(range(100, 0, -1)),
                    name=["one"] + [f"q{i}" for i in range(2200)] + ["two"],
                )
            ),
        )
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

        allTypes = set(registry.queryDatasetTypes())
        self.assertEqual(allTypes, {outDatasetType1, outDatasetType2})

    def testDimensions(self):
        """Tests for `Registry.insertDimensionData`,
        `Registry.syncDimensionData`, and `Registry.expandDataId`.
        """
        registry = self.makeRegistry()
        dimensionName = "instrument"
        dimension = registry.dimensions[dimensionName]
        dimensionValue = {
            "name": "DummyCam",
            "visit_max": 10,
            "visit_system": 0,
            "exposure_max": 10,
            "detector_max": 2,
            "class_name": "lsst.pipe.base.Instrument",
        }
        registry.insertDimensionData(dimensionName, dimensionValue)
        # Inserting the same value twice should fail
        with self.assertRaises(sqlalchemy.exc.IntegrityError):
            registry.insertDimensionData(dimensionName, dimensionValue)
        # expandDataId should retrieve the record we just inserted
        self.assertEqual(
            registry.expandDataId(instrument="DummyCam", graph=dimension.graph)
            .records[dimensionName]
            .toDict(),
            dimensionValue,
        )
        # expandDataId should raise if there is no record with the given ID.
        with self.assertRaises(DataIdValueError):
            registry.expandDataId({"instrument": "Unknown"}, graph=dimension.graph)
        # band doesn't have a table; insert should fail.
        with self.assertRaises(TypeError):
            registry.insertDimensionData("band", {"band": "i"})
        dimensionName2 = "physical_filter"
        dimension2 = registry.dimensions[dimensionName2]
        dimensionValue2 = {"name": "DummyCam_i", "band": "i"}
        # Missing required dependency ("instrument") should fail
        with self.assertRaises(KeyError):
            registry.insertDimensionData(dimensionName2, dimensionValue2)
        # Adding required dependency should fix the failure
        dimensionValue2["instrument"] = "DummyCam"
        registry.insertDimensionData(dimensionName2, dimensionValue2)
        # expandDataId should retrieve the record we just inserted.
        self.assertEqual(
            registry.expandDataId(instrument="DummyCam", physical_filter="DummyCam_i", graph=dimension2.graph)
            .records[dimensionName2]
            .toDict(),
            dimensionValue2,
        )
        # Use syncDimensionData to insert a new record successfully.
        dimensionName3 = "detector"
        dimensionValue3 = {
            "instrument": "DummyCam",
            "id": 1,
            "full_name": "one",
            "name_in_raft": "zero",
            "purpose": "SCIENCE",
        }
        self.assertTrue(registry.syncDimensionData(dimensionName3, dimensionValue3))
        # Sync that again.  Note that one field ("raft") is NULL, and that
        # should be okay.
        self.assertFalse(registry.syncDimensionData(dimensionName3, dimensionValue3))
        # Now try that sync with the same primary key but a different value.
        # This should fail.
        with self.assertRaises(ConflictingDefinitionError):
            registry.syncDimensionData(
                dimensionName3,
                {
                    "instrument": "DummyCam",
                    "id": 1,
                    "full_name": "one",
                    "name_in_raft": "four",
                    "purpose": "SCIENCE",
                },
            )

    @unittest.skipIf(np is None, "numpy not available.")
    def testNumpyDataId(self):
        """Test that we can use a numpy int in a dataId."""
        registry = self.makeRegistry()
        dimensionEntries = [
            ("instrument", {"instrument": "DummyCam"}),
            ("physical_filter", {"instrument": "DummyCam", "name": "d-r", "band": "R"}),
            # Using an np.int64 here fails unless Records.fromDict is also
            # patched to look for numbers.Integral
            ("visit", {"instrument": "DummyCam", "id": 42, "name": "fortytwo", "physical_filter": "d-r"}),
        ]
        for args in dimensionEntries:
            registry.insertDimensionData(*args)

        # Try a normal integer and something that looks like an int but
        # is not.
        for visit_id in (42, np.int64(42)):
            with self.subTest(visit_id=visit_id, id_type=type(visit_id).__name__):
                expanded = registry.expandDataId({"instrument": "DummyCam", "visit": visit_id})
                self.assertEqual(expanded["visit"], int(visit_id))
                self.assertIsInstance(expanded["visit"], int)

    def testDataIdRelationships(self):
        """Test that `Registry.expandDataId` raises an exception when the given
        keys are inconsistent.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        # Insert a few more dimension records for the next test.
        registry.insertDimensionData(
            "exposure",
            {"instrument": "Cam1", "id": 1, "obs_id": "one", "physical_filter": "Cam1-G"},
        )
        registry.insertDimensionData(
            "exposure",
            {"instrument": "Cam1", "id": 2, "obs_id": "two", "physical_filter": "Cam1-G"},
        )
        registry.insertDimensionData(
            "visit_system",
            {"instrument": "Cam1", "id": 0, "name": "one-to-one"},
        )
        registry.insertDimensionData(
            "visit",
            {"instrument": "Cam1", "id": 1, "name": "one", "physical_filter": "Cam1-G", "visit_system": 0},
        )
        registry.insertDimensionData(
            "visit_definition",
            {"instrument": "Cam1", "visit": 1, "exposure": 1, "visit_system": 0},
        )
        with self.assertRaises(InconsistentDataIdError):
            registry.expandDataId(
                {"instrument": "Cam1", "visit": 1, "exposure": 2},
            )

    def testDataset(self):
        """Basic tests for `Registry.insertDatasets`, `Registry.getDataset`,
        and `Registry.removeDatasets`.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        run = "tésτ"
        registry.registerRun(run)
        datasetType = registry.getDatasetType("bias")
        dataId = {"instrument": "Cam1", "detector": 2}
        (ref,) = registry.insertDatasets(datasetType, dataIds=[dataId], run=run)
        outRef = registry.getDataset(ref.id)
        self.assertIsNotNone(ref.id)
        self.assertEqual(ref, outRef)
        with self.assertRaises(ConflictingDefinitionError):
            registry.insertDatasets(datasetType, dataIds=[dataId], run=run)
        registry.removeDatasets([ref])
        self.assertIsNone(registry.findDataset(datasetType, dataId, collections=[run]))

    def testFindDataset(self):
        """Tests for `Registry.findDataset`."""
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        run = "tésτ"
        datasetType = registry.getDatasetType("bias")
        dataId = {"instrument": "Cam1", "detector": 4}
        registry.registerRun(run)
        (inputRef,) = registry.insertDatasets(datasetType, dataIds=[dataId], run=run)
        outputRef = registry.findDataset(datasetType, dataId, collections=[run])
        self.assertEqual(outputRef, inputRef)
        # Check that retrieval with invalid dataId raises
        with self.assertRaises(LookupError):
            dataId = {"instrument": "Cam1"}  # no detector
            registry.findDataset(datasetType, dataId, collections=run)
        # Check that different dataIds match to different datasets
        dataId1 = {"instrument": "Cam1", "detector": 1}
        (inputRef1,) = registry.insertDatasets(datasetType, dataIds=[dataId1], run=run)
        dataId2 = {"instrument": "Cam1", "detector": 2}
        (inputRef2,) = registry.insertDatasets(datasetType, dataIds=[dataId2], run=run)
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=run), inputRef1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=run), inputRef2)
        self.assertNotEqual(registry.findDataset(datasetType, dataId1, collections=run), inputRef2)
        self.assertNotEqual(registry.findDataset(datasetType, dataId2, collections=run), inputRef1)
        # Check that requesting a non-existing dataId returns None
        nonExistingDataId = {"instrument": "Cam1", "detector": 3}
        self.assertIsNone(registry.findDataset(datasetType, nonExistingDataId, collections=run))
        # Search more than one collection, in which two have the right
        # dataset type and another does not.
        registry.registerRun("empty")
        self.loadData(registry, "datasets-uuid.yaml")
        bias1 = registry.findDataset("bias", instrument="Cam1", detector=2, collections=["imported_g"])
        self.assertIsNotNone(bias1)
        bias2 = registry.findDataset("bias", instrument="Cam1", detector=2, collections=["imported_r"])
        self.assertIsNotNone(bias2)
        self.assertEqual(
            bias1,
            registry.findDataset(
                "bias", instrument="Cam1", detector=2, collections=["empty", "imported_g", "imported_r"]
            ),
        )
        self.assertEqual(
            bias2,
            registry.findDataset(
                "bias", instrument="Cam1", detector=2, collections=["empty", "imported_r", "imported_g"]
            ),
        )
        # Search more than one collection, with one of them a CALIBRATION
        # collection.
        registry.registerCollection("Cam1/calib", CollectionType.CALIBRATION)
        timespan = Timespan(
            begin=astropy.time.Time("2020-01-01T01:00:00", format="isot", scale="tai"),
            end=astropy.time.Time("2020-01-01T02:00:00", format="isot", scale="tai"),
        )
        registry.certify("Cam1/calib", [bias2], timespan=timespan)
        self.assertEqual(
            bias1,
            registry.findDataset(
                "bias",
                instrument="Cam1",
                detector=2,
                collections=["empty", "imported_g", "Cam1/calib"],
                timespan=timespan,
            ),
        )
        self.assertEqual(
            bias2,
            registry.findDataset(
                "bias",
                instrument="Cam1",
                detector=2,
                collections=["empty", "Cam1/calib", "imported_g"],
                timespan=timespan,
            ),
        )
        # If we try to search those same collections without a timespan, it
        # should still work, since the CALIBRATION collection is ignored.
        self.assertEqual(
            bias1,
            registry.findDataset(
                "bias", instrument="Cam1", detector=2, collections=["empty", "imported_g", "Cam1/calib"]
            ),
        )
        self.assertEqual(
            bias1,
            registry.findDataset(
                "bias", instrument="Cam1", detector=2, collections=["empty", "Cam1/calib", "imported_g"]
            ),
        )

    def testRemoveDatasetTypeSuccess(self):
        """Test that Registry.removeDatasetType works when there are no
        datasets of that type present.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        registry.removeDatasetType("flat")
        with self.assertRaises(MissingDatasetTypeError):
            registry.getDatasetType("flat")

    def testRemoveDatasetTypeFailure(self):
        """Test that Registry.removeDatasetType raises when there are datasets
        of that type present or if the dataset type is for a component.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        with self.assertRaises(OrphanedRecordError):
            registry.removeDatasetType("flat")
        with self.assertRaises(ValueError):
            registry.removeDatasetType(DatasetType.nameWithComponent("flat", "image"))

    def testImportDatasetsUUID(self):
        """Test for `Registry._importDatasets` with UUID dataset ID."""
        if not self.datasetsManager.endswith(".ByDimensionsDatasetRecordStorageManagerUUID"):
            self.skipTest(f"Unexpected dataset manager {self.datasetsManager}")

        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        for run in range(6):
            registry.registerRun(f"run{run}")
        datasetTypeBias = registry.getDatasetType("bias")
        datasetTypeFlat = registry.getDatasetType("flat")
        dataIdBias1 = {"instrument": "Cam1", "detector": 1}
        dataIdBias2 = {"instrument": "Cam1", "detector": 2}
        dataIdFlat1 = {"instrument": "Cam1", "detector": 1, "physical_filter": "Cam1-G", "band": "g"}

        dataset_id = uuid.uuid4()
        ref = DatasetRef(datasetTypeBias, dataIdBias1, id=dataset_id, run="run0")
        (ref1,) = registry._importDatasets([ref])
        # UUID is used without change
        self.assertEqual(ref.id, ref1.id)

        # All different failure modes
        refs = (
            # Importing same DatasetRef with different dataset ID is an error
            DatasetRef(datasetTypeBias, dataIdBias1, id=uuid.uuid4(), run="run0"),
            # Same DatasetId but different DataId
            DatasetRef(datasetTypeBias, dataIdBias2, id=ref1.id, run="run0"),
            DatasetRef(datasetTypeFlat, dataIdFlat1, id=ref1.id, run="run0"),
            # Same DatasetRef and DatasetId but different run
            DatasetRef(datasetTypeBias, dataIdBias1, id=ref1.id, run="run1"),
        )
        for ref in refs:
            with self.assertRaises(ConflictingDefinitionError):
                registry._importDatasets([ref])

        # Test for non-unique IDs, they can be re-imported multiple times.
        for run, idGenMode in ((2, DatasetIdGenEnum.DATAID_TYPE), (4, DatasetIdGenEnum.DATAID_TYPE_RUN)):
            with self.subTest(idGenMode=idGenMode):
                # Use integer dataset ID to force UUID calculation in _import
                ref = DatasetRef(datasetTypeBias, dataIdBias1, id=0, run=f"run{run}")
                (ref1,) = registry._importDatasets([ref], idGenerationMode=idGenMode)
                self.assertIsInstance(ref1.id, uuid.UUID)
                self.assertEqual(ref1.id.version, 5)

                # Importing it again is OK
                (ref2,) = registry._importDatasets([ref1])
                self.assertEqual(ref2.id, ref1.id)

                # Cannot import to different run with the same ID
                ref = DatasetRef(datasetTypeBias, dataIdBias1, id=ref1.id, run=f"run{run+1}")
                with self.assertRaises(ConflictingDefinitionError):
                    registry._importDatasets([ref])

                ref = DatasetRef(datasetTypeBias, dataIdBias1, id=0, run=f"run{run+1}")
                if idGenMode is DatasetIdGenEnum.DATAID_TYPE:
                    # Cannot import same DATAID_TYPE ref into a new run
                    with self.assertRaises(ConflictingDefinitionError):
                        (ref2,) = registry._importDatasets([ref], idGenerationMode=idGenMode)
                else:
                    # DATAID_TYPE_RUN ref can be imported into a new run
                    (ref2,) = registry._importDatasets([ref], idGenerationMode=idGenMode)

    def testDatasetTypeComponentQueries(self):
        """Test component options when querying for dataset types.

        All of the behavior here is deprecated, so many of these tests are
        currently wrapped in a context to check that we get a warning whenever
        a component dataset is actually returned.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        # Test querying for dataset types with different inputs.
        # First query for all dataset types; components should only be included
        # when components=True.
        self.assertEqual({"bias", "flat"}, NamedValueSet(registry.queryDatasetTypes()).names)
        self.assertEqual({"bias", "flat"}, NamedValueSet(registry.queryDatasetTypes(components=False)).names)
        with self.assertWarns(FutureWarning):
            self.assertLess(
                {"bias", "flat", "bias.wcs", "flat.photoCalib"},
                NamedValueSet(registry.queryDatasetTypes(components=True)).names,
            )
        # Use a pattern that can match either parent or components.  Again,
        # components are only returned if components=True.
        self.assertEqual({"bias"}, NamedValueSet(registry.queryDatasetTypes(re.compile("^bias.*"))).names)
        self.assertEqual(
            {"bias"}, NamedValueSet(registry.queryDatasetTypes(re.compile("^bias.*"), components=False)).names
        )
        with self.assertWarns(FutureWarning):
            self.assertLess(
                {"bias", "bias.wcs"},
                NamedValueSet(registry.queryDatasetTypes(re.compile("^bias.*"), components=True)).names,
            )
        # This pattern matches only a component.  In this case we also return
        # that component dataset type if components=None.
        with self.assertWarns(FutureWarning):
            self.assertEqual(
                {"bias.wcs"}, NamedValueSet(registry.queryDatasetTypes(re.compile(r"^bias\.wcs"))).names
            )
        self.assertEqual(
            set(),
            NamedValueSet(registry.queryDatasetTypes(re.compile(r"^bias\.wcs"), components=False)).names,
        )
        with self.assertWarns(FutureWarning):
            self.assertEqual(
                {"bias.wcs"},
                NamedValueSet(registry.queryDatasetTypes(re.compile(r"^bias\.wcs"), components=True)).names,
            )
        # Add a dataset type using a StorageClass that we'll then remove; check
        # that this does not affect our ability to query for dataset types
        # (though it will warn).
        tempStorageClass = StorageClass(
            name="TempStorageClass",
            components={
                "data1": registry.storageClasses.getStorageClass("StructuredDataDict"),
                "data2": registry.storageClasses.getStorageClass("StructuredDataDict"),
            },
        )
        registry.storageClasses.registerStorageClass(tempStorageClass)
        datasetType = DatasetType(
            "temporary",
            dimensions=["instrument"],
            storageClass=tempStorageClass,
            universe=registry.dimensions,
        )
        registry.registerDatasetType(datasetType)
        registry.storageClasses._unregisterStorageClass(tempStorageClass.name)
        datasetType._storageClass = None
        del tempStorageClass
        # Querying for all dataset types, including components, should include
        # at least all non-component dataset types (and I don't want to
        # enumerate all of the Exposure components for bias and flat here).
        with self.assertWarns(FutureWarning):
            with self.assertLogs("lsst.daf.butler.registry", logging.WARN) as cm:
                everything = NamedValueSet(registry.queryDatasetTypes(components=True))
        self.assertIn("TempStorageClass", cm.output[0])
        self.assertLess({"bias", "flat", "temporary"}, everything.names)
        # It should not include "temporary.columns", because we tried to remove
        # the storage class that would tell it about that.  So if the next line
        # fails (i.e. "temporary.columns" _is_ in everything.names), it means
        # this part of the test isn't doing anything, because the _unregister
        # call about isn't simulating the real-life case we want it to
        # simulate, in which different versions of daf_butler in entirely
        # different Python processes interact with the same repo.
        self.assertNotIn("temporary.data", everything.names)
        # Query for dataset types that start with "temp".  This should again
        # not include the component, and also not fail.
        with self.assertLogs("lsst.daf.butler.registry", logging.WARN) as cm:
            startsWithTemp = NamedValueSet(registry.queryDatasetTypes(re.compile("temp.*"), components=True))
        self.assertIn("TempStorageClass", cm.output[0])
        self.assertEqual({"temporary"}, startsWithTemp.names)
        # Querying with no components should not warn at all.
        with self.assertLogs("lsst.daf.butler.registries", logging.WARN) as cm:
            startsWithTemp = NamedValueSet(registry.queryDatasetTypes(re.compile("temp.*"), components=False))
            # Must issue a warning of our own to be captured.
            logging.getLogger("lsst.daf.butler.registries").warning("test message")
        self.assertEqual(len(cm.output), 1)
        self.assertIn("test message", cm.output[0])

    def testComponentLookups(self):
        """Test searching for component datasets via their parents.

        All of the behavior here is deprecated, so many of these tests are
        currently wrapped in a context to check that we get a warning whenever
        a component dataset is actually returned.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        # Test getting the child dataset type (which does still exist in the
        # Registry), and check for consistency with
        # DatasetRef.makeComponentRef.
        collection = "imported_g"
        parentType = registry.getDatasetType("bias")
        childType = registry.getDatasetType("bias.wcs")
        parentRefResolved = registry.findDataset(
            parentType, collections=collection, instrument="Cam1", detector=1
        )
        self.assertIsInstance(parentRefResolved, DatasetRef)
        self.assertEqual(childType, parentRefResolved.makeComponentRef("wcs").datasetType)
        # Search for a single dataset with findDataset.
        childRef1 = registry.findDataset("bias.wcs", collections=collection, dataId=parentRefResolved.dataId)
        self.assertEqual(childRef1, parentRefResolved.makeComponentRef("wcs"))
        # Search for detector data IDs constrained by component dataset
        # existence with queryDataIds.
        with self.assertWarns(FutureWarning):
            dataIds = registry.queryDataIds(
                ["detector"],
                datasets=["bias.wcs"],
                collections=collection,
            ).toSet()
        self.assertEqual(
            dataIds,
            DataCoordinateSet(
                {
                    DataCoordinate.standardize(instrument="Cam1", detector=d, graph=parentType.dimensions)
                    for d in (1, 2, 3)
                },
                parentType.dimensions,
            ),
        )
        # Search for multiple datasets of a single type with queryDatasets.
        with self.assertWarns(FutureWarning):
            childRefs2 = set(
                registry.queryDatasets(
                    "bias.wcs",
                    collections=collection,
                )
            )
        self.assertEqual(
            {ref.unresolved() for ref in childRefs2}, {DatasetRef(childType, dataId) for dataId in dataIds}
        )

    def testCollections(self):
        """Tests for registry methods that manage collections."""
        registry = self.makeRegistry()
        other_registry = self.makeRegistry(share_repo_with=registry)
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        run1 = "imported_g"
        run2 = "imported_r"
        # Test setting a collection docstring after it has been created.
        registry.setCollectionDocumentation(run1, "doc for run1")
        self.assertEqual(registry.getCollectionDocumentation(run1), "doc for run1")
        registry.setCollectionDocumentation(run1, None)
        self.assertIsNone(registry.getCollectionDocumentation(run1))
        datasetType = "bias"
        # Find some datasets via their run's collection.
        dataId1 = {"instrument": "Cam1", "detector": 1}
        ref1 = registry.findDataset(datasetType, dataId1, collections=run1)
        self.assertIsNotNone(ref1)
        dataId2 = {"instrument": "Cam1", "detector": 2}
        ref2 = registry.findDataset(datasetType, dataId2, collections=run1)
        self.assertIsNotNone(ref2)
        # Associate those into a new collection, then look for them there.
        tag1 = "tag1"
        registry.registerCollection(tag1, type=CollectionType.TAGGED, doc="doc for tag1")
        # Check that we can query for old and new collections by type.
        self.assertEqual(set(registry.queryCollections(collectionTypes=CollectionType.RUN)), {run1, run2})
        self.assertEqual(
            set(registry.queryCollections(collectionTypes={CollectionType.TAGGED, CollectionType.RUN})),
            {tag1, run1, run2},
        )
        self.assertEqual(registry.getCollectionDocumentation(tag1), "doc for tag1")
        registry.associate(tag1, [ref1, ref2])
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=tag1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=tag1), ref2)
        # Disassociate one and verify that we can't it there anymore...
        registry.disassociate(tag1, [ref1])
        self.assertIsNone(registry.findDataset(datasetType, dataId1, collections=tag1))
        # ...but we can still find ref2 in tag1, and ref1 in the run.
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=run1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=tag1), ref2)
        collections = set(registry.queryCollections())
        self.assertEqual(collections, {run1, run2, tag1})
        # Associate both refs into tag1 again; ref2 is already there, but that
        # should be a harmless no-op.
        registry.associate(tag1, [ref1, ref2])
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=tag1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=tag1), ref2)
        # Get a different dataset (from a different run) that has the same
        # dataset type and data ID as ref2.
        ref2b = registry.findDataset(datasetType, dataId2, collections=run2)
        self.assertNotEqual(ref2, ref2b)
        # Attempting to associate that into tag1 should be an error.
        with self.assertRaises(ConflictingDefinitionError):
            registry.associate(tag1, [ref2b])
        # That error shouldn't have messed up what we had before.
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=tag1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=tag1), ref2)
        # Attempt to associate the conflicting dataset again, this time with
        # a dataset that isn't in the collection and won't cause a conflict.
        # Should also fail without modifying anything.
        dataId3 = {"instrument": "Cam1", "detector": 3}
        ref3 = registry.findDataset(datasetType, dataId3, collections=run1)
        with self.assertRaises(ConflictingDefinitionError):
            registry.associate(tag1, [ref3, ref2b])
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=tag1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=tag1), ref2)
        self.assertIsNone(registry.findDataset(datasetType, dataId3, collections=tag1))
        # Register a chained collection that searches [tag1, run2]
        chain1 = "chain1"
        registry.registerCollection(chain1, type=CollectionType.CHAINED)
        self.assertIs(registry.getCollectionType(chain1), CollectionType.CHAINED)
        # Chained collection exists, but has no collections in it.
        self.assertFalse(registry.getCollectionChain(chain1))
        # If we query for all collections, we should get the chained collection
        # only if we don't ask to flatten it (i.e. yield only its children).
        self.assertEqual(set(registry.queryCollections(flattenChains=False)), {tag1, run1, run2, chain1})
        self.assertEqual(set(registry.queryCollections(flattenChains=True)), {tag1, run1, run2})
        # Attempt to set its child collections to something circular; that
        # should fail.
        with self.assertRaises(ValueError):
            registry.setCollectionChain(chain1, [tag1, chain1])
        # Add the child collections.
        registry.setCollectionChain(chain1, [tag1, run2])
        self.assertEqual(list(registry.getCollectionChain(chain1)), [tag1, run2])
        self.assertEqual(registry.getCollectionParentChains(tag1), {chain1})
        self.assertEqual(registry.getCollectionParentChains(run2), {chain1})
        # Refresh the other registry that points to the same repo, and make
        # sure it can see the things we've done (note that this does require
        # an explicit refresh(); that's the documented behavior, because
        # caching is ~impossible otherwise).
        if other_registry is not None:
            other_registry.refresh()
            self.assertEqual(list(other_registry.getCollectionChain(chain1)), [tag1, run2])
            self.assertEqual(other_registry.getCollectionParentChains(tag1), {chain1})
            self.assertEqual(other_registry.getCollectionParentChains(run2), {chain1})
        # Searching for dataId1 or dataId2 in the chain should return ref1 and
        # ref2, because both are in tag1.
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=chain1), ref1)
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=chain1), ref2)
        # Now disassociate ref2 from tag1.  The search (for bias) with
        # dataId2 in chain1 should then:
        # 1. not find it in tag1
        # 2. find a different dataset in run2
        registry.disassociate(tag1, [ref2])
        ref2b = registry.findDataset(datasetType, dataId2, collections=chain1)
        self.assertNotEqual(ref2b, ref2)
        self.assertEqual(ref2b, registry.findDataset(datasetType, dataId2, collections=run2))
        # Define a new chain so we can test recursive chains.
        chain2 = "chain2"
        registry.registerCollection(chain2, type=CollectionType.CHAINED)
        registry.setCollectionChain(chain2, [run2, chain1])
        self.assertEqual(registry.getCollectionParentChains(chain1), {chain2})
        self.assertEqual(registry.getCollectionParentChains(run2), {chain1, chain2})
        # Query for collections matching a regex.
        self.assertCountEqual(
            list(registry.queryCollections(re.compile("imported_."), flattenChains=False)),
            ["imported_r", "imported_g"],
        )
        # Query for collections matching a regex or an explicit str.
        self.assertCountEqual(
            list(registry.queryCollections([re.compile("imported_."), "chain1"], flattenChains=False)),
            ["imported_r", "imported_g", "chain1"],
        )
        # Search for bias with dataId1 should find it via tag1 in chain2,
        # recursing, because is not in run1.
        self.assertIsNone(registry.findDataset(datasetType, dataId1, collections=run2))
        self.assertEqual(registry.findDataset(datasetType, dataId1, collections=chain2), ref1)
        # Search for bias with dataId2 should find it in run2 (ref2b).
        self.assertEqual(registry.findDataset(datasetType, dataId2, collections=chain2), ref2b)
        # Search for a flat that is in run2.  That should not be found
        # at the front of chain2, because of the restriction to bias
        # on run2 there, but it should be found in at the end of chain1.
        dataId4 = {"instrument": "Cam1", "detector": 3, "physical_filter": "Cam1-R2"}
        ref4 = registry.findDataset("flat", dataId4, collections=run2)
        self.assertIsNotNone(ref4)
        self.assertEqual(ref4, registry.findDataset("flat", dataId4, collections=chain2))
        # Deleting a collection that's part of a CHAINED collection is not
        # allowed, and is exception-safe.
        with self.assertRaises(Exception):
            registry.removeCollection(run2)
        self.assertEqual(registry.getCollectionType(run2), CollectionType.RUN)
        with self.assertRaises(Exception):
            registry.removeCollection(chain1)
        self.assertEqual(registry.getCollectionType(chain1), CollectionType.CHAINED)
        # Actually remove chain2, test that it's gone by asking for its type.
        registry.removeCollection(chain2)
        with self.assertRaises(MissingCollectionError):
            registry.getCollectionType(chain2)
        # Actually remove run2 and chain1, which should work now.
        registry.removeCollection(chain1)
        registry.removeCollection(run2)
        with self.assertRaises(MissingCollectionError):
            registry.getCollectionType(run2)
        with self.assertRaises(MissingCollectionError):
            registry.getCollectionType(chain1)
        # Remove tag1 as well, just to test that we can remove TAGGED
        # collections.
        registry.removeCollection(tag1)
        with self.assertRaises(MissingCollectionError):
            registry.getCollectionType(tag1)

    def testCollectionChainFlatten(self):
        """Test that Registry.setCollectionChain obeys its 'flatten' option."""
        registry = self.makeRegistry()
        registry.registerCollection("inner", CollectionType.CHAINED)
        registry.registerCollection("innermost", CollectionType.RUN)
        registry.setCollectionChain("inner", ["innermost"])
        registry.registerCollection("outer", CollectionType.CHAINED)
        registry.setCollectionChain("outer", ["inner"], flatten=False)
        self.assertEqual(list(registry.getCollectionChain("outer")), ["inner"])
        registry.setCollectionChain("outer", ["inner"], flatten=True)
        self.assertEqual(list(registry.getCollectionChain("outer")), ["innermost"])

    def testBasicTransaction(self):
        """Test that all operations within a single transaction block are
        rolled back if an exception propagates out of the block.
        """
        registry = self.makeRegistry()
        storageClass = StorageClass("testDatasetType")
        registry.storageClasses.registerStorageClass(storageClass)
        with registry.transaction():
            registry.insertDimensionData("instrument", {"name": "Cam1", "class_name": "A"})
        with self.assertRaises(ValueError):
            with registry.transaction():
                registry.insertDimensionData("instrument", {"name": "Cam2"})
                raise ValueError("Oops, something went wrong")
        # Cam1 should exist
        self.assertEqual(registry.expandDataId(instrument="Cam1").records["instrument"].class_name, "A")
        # But Cam2 and Cam3 should both not exist
        with self.assertRaises(DataIdValueError):
            registry.expandDataId(instrument="Cam2")
        with self.assertRaises(DataIdValueError):
            registry.expandDataId(instrument="Cam3")

    def testNestedTransaction(self):
        """Test that operations within a transaction block are not rolled back
        if an exception propagates out of an inner transaction block and is
        then caught.
        """
        registry = self.makeRegistry()
        dimension = registry.dimensions["instrument"]
        dataId1 = {"instrument": "DummyCam"}
        dataId2 = {"instrument": "DummyCam2"}
        checkpointReached = False
        with registry.transaction():
            # This should be added and (ultimately) committed.
            registry.insertDimensionData(dimension, dataId1)
            with self.assertRaises(sqlalchemy.exc.IntegrityError):
                with registry.transaction(savepoint=True):
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
        with self.assertRaises(DataIdValueError):
            registry.expandDataId(dataId2, graph=dimension.graph)

    def testInstrumentDimensions(self):
        """Test queries involving only instrument dimensions, with no joins to
        skymap."""
        registry = self.makeRegistry()

        # need a bunch of dimensions and datasets for test
        registry.insertDimensionData(
            "instrument", dict(name="DummyCam", visit_max=25, exposure_max=300, detector_max=6)
        )
        registry.insertDimensionData(
            "physical_filter",
            dict(instrument="DummyCam", name="dummy_r", band="r"),
            dict(instrument="DummyCam", name="dummy_i", band="i"),
        )
        registry.insertDimensionData(
            "detector", *[dict(instrument="DummyCam", id=i, full_name=str(i)) for i in range(1, 6)]
        )
        registry.insertDimensionData(
            "visit_system",
            dict(instrument="DummyCam", id=1, name="default"),
        )
        registry.insertDimensionData(
            "visit",
            dict(instrument="DummyCam", id=10, name="ten", physical_filter="dummy_i", visit_system=1),
            dict(instrument="DummyCam", id=11, name="eleven", physical_filter="dummy_r", visit_system=1),
            dict(instrument="DummyCam", id=20, name="twelve", physical_filter="dummy_r", visit_system=1),
        )
        for i in range(1, 6):
            registry.insertDimensionData(
                "visit_detector_region",
                dict(instrument="DummyCam", visit=10, detector=i),
                dict(instrument="DummyCam", visit=11, detector=i),
                dict(instrument="DummyCam", visit=20, detector=i),
            )
        registry.insertDimensionData(
            "exposure",
            dict(instrument="DummyCam", id=100, obs_id="100", physical_filter="dummy_i"),
            dict(instrument="DummyCam", id=101, obs_id="101", physical_filter="dummy_i"),
            dict(instrument="DummyCam", id=110, obs_id="110", physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=111, obs_id="111", physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=200, obs_id="200", physical_filter="dummy_r"),
            dict(instrument="DummyCam", id=201, obs_id="201", physical_filter="dummy_r"),
        )
        registry.insertDimensionData(
            "visit_definition",
            dict(instrument="DummyCam", exposure=100, visit_system=1, visit=10),
            dict(instrument="DummyCam", exposure=101, visit_system=1, visit=10),
            dict(instrument="DummyCam", exposure=110, visit_system=1, visit=11),
            dict(instrument="DummyCam", exposure=111, visit_system=1, visit=11),
            dict(instrument="DummyCam", exposure=200, visit_system=1, visit=20),
            dict(instrument="DummyCam", exposure=201, visit_system=1, visit=20),
        )
        # dataset types
        run1 = "test1_r"
        run2 = "test2_r"
        tagged2 = "test2_t"
        registry.registerRun(run1)
        registry.registerRun(run2)
        registry.registerCollection(tagged2)
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        rawType = DatasetType(
            name="RAW",
            dimensions=registry.dimensions.extract(("instrument", "exposure", "detector")),
            storageClass=storageClass,
        )
        registry.registerDatasetType(rawType)
        calexpType = DatasetType(
            name="CALEXP",
            dimensions=registry.dimensions.extract(("instrument", "visit", "detector")),
            storageClass=storageClass,
        )
        registry.registerDatasetType(calexpType)

        # add pre-existing datasets
        for exposure in (100, 101, 110, 111):
            for detector in (1, 2, 3):
                # note that only 3 of 5 detectors have datasets
                dataId = dict(instrument="DummyCam", exposure=exposure, detector=detector)
                (ref,) = registry.insertDatasets(rawType, dataIds=[dataId], run=run1)
                # exposures 100 and 101 appear in both run1 and tagged2.
                # 100 has different datasets in the different collections
                # 101 has the same dataset in both collections.
                if exposure == 100:
                    (ref,) = registry.insertDatasets(rawType, dataIds=[dataId], run=run2)
                if exposure in (100, 101):
                    registry.associate(tagged2, [ref])
        # Add pre-existing datasets to tagged2.
        for exposure in (200, 201):
            for detector in (3, 4, 5):
                # note that only 3 of 5 detectors have datasets
                dataId = dict(instrument="DummyCam", exposure=exposure, detector=detector)
                (ref,) = registry.insertDatasets(rawType, dataIds=[dataId], run=run2)
                registry.associate(tagged2, [ref])

        dimensions = DimensionGraph(
            registry.dimensions, dimensions=(rawType.dimensions.required | calexpType.dimensions.required)
        )
        # Test that single dim string works as well as list of str
        rows = registry.queryDataIds("visit", datasets=rawType, collections=run1).expanded().toSet()
        rowsI = registry.queryDataIds(["visit"], datasets=rawType, collections=run1).expanded().toSet()
        self.assertEqual(rows, rowsI)
        # with empty expression
        rows = registry.queryDataIds(dimensions, datasets=rawType, collections=run1).expanded().toSet()
        self.assertEqual(len(rows), 4 * 3)  # 4 exposures times 3 detectors
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure", "visit"))
            packer1 = registry.dimensions.makePacker("visit_detector", dataId)
            packer2 = registry.dimensions.makePacker("exposure_detector", dataId)
            self.assertEqual(
                packer1.unpack(packer1.pack(dataId)),
                DataCoordinate.standardize(dataId, graph=packer1.dimensions),
            )
            self.assertEqual(
                packer2.unpack(packer2.pack(dataId)),
                DataCoordinate.standardize(dataId, graph=packer2.dimensions),
            )
            self.assertNotEqual(packer1.pack(dataId), packer2.pack(dataId))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101, 110, 111))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 11))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

        # second collection
        rows = registry.queryDataIds(dimensions, datasets=rawType, collections=tagged2).toSet()
        self.assertEqual(len(rows), 4 * 3)  # 4 exposures times 3 detectors
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure", "visit"))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101, 200, 201))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 20))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3, 4, 5))

        # with two input datasets
        rows = registry.queryDataIds(dimensions, datasets=rawType, collections=[run1, tagged2]).toSet()
        self.assertEqual(len(set(rows)), 6 * 3)  # 6 exposures times 3 detectors; set needed to de-dupe
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("instrument", "detector", "exposure", "visit"))
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101, 110, 111, 200, 201))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10, 11, 20))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3, 4, 5))

        # limit to single visit
        rows = registry.queryDataIds(
            dimensions, datasets=rawType, collections=run1, where="visit = 10", instrument="DummyCam"
        ).toSet()
        self.assertEqual(len(rows), 2 * 3)  # 2 exposures times 3 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

        # more limiting expression, using link names instead of Table.column
        rows = registry.queryDataIds(
            dimensions,
            datasets=rawType,
            collections=run1,
            where="visit = 10 and detector > 1 and 'DummyCam'=instrument",
        ).toSet()
        self.assertEqual(len(rows), 2 * 2)  # 2 exposures times 2 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (100, 101))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (10,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (2, 3))

        # queryDataIds with only one of `datasets` and `collections` is an
        # error.
        with self.assertRaises(CollectionError):
            registry.queryDataIds(dimensions, datasets=rawType)
        with self.assertRaises(ArgumentError):
            registry.queryDataIds(dimensions, collections=run1)

        # expression excludes everything
        rows = registry.queryDataIds(
            dimensions, datasets=rawType, collections=run1, where="visit > 1000", instrument="DummyCam"
        ).toSet()
        self.assertEqual(len(rows), 0)

        # Selecting by physical_filter, this is not in the dimensions, but it
        # is a part of the full expression so it should work too.
        rows = registry.queryDataIds(
            dimensions,
            datasets=rawType,
            collections=run1,
            where="physical_filter = 'dummy_r'",
            instrument="DummyCam",
        ).toSet()
        self.assertEqual(len(rows), 2 * 3)  # 2 exposures times 3 detectors
        self.assertCountEqual(set(dataId["exposure"] for dataId in rows), (110, 111))
        self.assertCountEqual(set(dataId["visit"] for dataId in rows), (11,))
        self.assertCountEqual(set(dataId["detector"] for dataId in rows), (1, 2, 3))

    def testSkyMapDimensions(self):
        """Tests involving only skymap dimensions, no joins to instrument."""
        registry = self.makeRegistry()

        # need a bunch of dimensions and datasets for test, we want
        # "band" in the test so also have to add physical_filter
        # dimensions
        registry.insertDimensionData("instrument", dict(instrument="DummyCam"))
        registry.insertDimensionData(
            "physical_filter",
            dict(instrument="DummyCam", name="dummy_r", band="r"),
            dict(instrument="DummyCam", name="dummy_i", band="i"),
        )
        registry.insertDimensionData("skymap", dict(name="DummyMap", hash="sha!".encode("utf8")))
        for tract in range(10):
            registry.insertDimensionData("tract", dict(skymap="DummyMap", id=tract))
            registry.insertDimensionData(
                "patch",
                *[dict(skymap="DummyMap", tract=tract, id=patch, cell_x=0, cell_y=0) for patch in range(10)],
            )

        # dataset types
        run = "tésτ"
        registry.registerRun(run)
        storageClass = StorageClass("testDataset")
        registry.storageClasses.registerStorageClass(storageClass)
        calexpType = DatasetType(
            name="deepCoadd_calexp",
            dimensions=registry.dimensions.extract(("skymap", "tract", "patch", "band")),
            storageClass=storageClass,
        )
        registry.registerDatasetType(calexpType)
        mergeType = DatasetType(
            name="deepCoadd_mergeDet",
            dimensions=registry.dimensions.extract(("skymap", "tract", "patch")),
            storageClass=storageClass,
        )
        registry.registerDatasetType(mergeType)
        measType = DatasetType(
            name="deepCoadd_meas",
            dimensions=registry.dimensions.extract(("skymap", "tract", "patch", "band")),
            storageClass=storageClass,
        )
        registry.registerDatasetType(measType)

        dimensions = DimensionGraph(
            registry.dimensions,
            dimensions=(
                calexpType.dimensions.required | mergeType.dimensions.required | measType.dimensions.required
            ),
        )

        # add pre-existing datasets
        for tract in (1, 3, 5):
            for patch in (2, 4, 6, 7):
                dataId = dict(skymap="DummyMap", tract=tract, patch=patch)
                registry.insertDatasets(mergeType, dataIds=[dataId], run=run)
                for aFilter in ("i", "r"):
                    dataId = dict(skymap="DummyMap", tract=tract, patch=patch, band=aFilter)
                    registry.insertDatasets(calexpType, dataIds=[dataId], run=run)

        # with empty expression
        rows = registry.queryDataIds(dimensions, datasets=[calexpType, mergeType], collections=run).toSet()
        self.assertEqual(len(rows), 3 * 4 * 2)  # 4 tracts x 4 patches x 2 filters
        for dataId in rows:
            self.assertCountEqual(dataId.keys(), ("skymap", "tract", "patch", "band"))
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 3, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(dataId["band"] for dataId in rows), ("i", "r"))

        # limit to 2 tracts and 2 patches
        rows = registry.queryDataIds(
            dimensions,
            datasets=[calexpType, mergeType],
            collections=run,
            where="tract IN (1, 5) AND patch IN (2, 7)",
            skymap="DummyMap",
        ).toSet()
        self.assertEqual(len(rows), 2 * 2 * 2)  # 2 tracts x 2 patches x 2 filters
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 7))
        self.assertCountEqual(set(dataId["band"] for dataId in rows), ("i", "r"))

        # limit to single filter
        rows = registry.queryDataIds(
            dimensions, datasets=[calexpType, mergeType], collections=run, where="band = 'i'"
        ).toSet()
        self.assertEqual(len(rows), 3 * 4 * 1)  # 4 tracts x 4 patches x 2 filters
        self.assertCountEqual(set(dataId["tract"] for dataId in rows), (1, 3, 5))
        self.assertCountEqual(set(dataId["patch"] for dataId in rows), (2, 4, 6, 7))
        self.assertCountEqual(set(dataId["band"] for dataId in rows), ("i",))

        # Specifying non-existing skymap is an exception
        with self.assertRaisesRegex(DataIdValueError, "Unknown values specified for governor dimension"):
            rows = registry.queryDataIds(
                dimensions, datasets=[calexpType, mergeType], collections=run, where="skymap = 'Mars'"
            ).toSet()

    def testSpatialJoin(self):
        """Test queries that involve spatial overlap joins."""
        registry = self.makeRegistry()
        self.loadData(registry, "hsc-rc2-subset.yaml")

        # Dictionary of spatial DatabaseDimensionElements, keyed by the name of
        # the TopologicalFamily they belong to.  We'll relate all elements in
        # each family to all of the elements in each other family.
        families = defaultdict(set)
        # Dictionary of {element.name: {dataId: region}}.
        regions = {}
        for element in registry.dimensions.getDatabaseElements():
            if element.spatial is not None:
                families[element.spatial.name].add(element)
                regions[element.name] = {
                    record.dataId: record.region for record in registry.queryDimensionRecords(element)
                }

        # If this check fails, it's not necessarily a problem - it may just be
        # a reasonable change to the default dimension definitions - but the
        # test below depends on there being more than one family to do anything
        # useful.
        self.assertEqual(len(families), 2)

        # Overlap DatabaseDimensionElements with each other.
        for family1, family2 in itertools.combinations(families, 2):
            for element1, element2 in itertools.product(families[family1], families[family2]):
                graph = DimensionGraph.union(element1.graph, element2.graph)
                # Construct expected set of overlapping data IDs via a
                # brute-force comparison of the regions we've already fetched.
                expected = {
                    DataCoordinate.standardize({**dataId1.byName(), **dataId2.byName()}, graph=graph)
                    for (dataId1, region1), (dataId2, region2) in itertools.product(
                        regions[element1.name].items(), regions[element2.name].items()
                    )
                    if not region1.isDisjointFrom(region2)
                }
                self.assertGreater(len(expected), 2, msg="Test that we aren't just comparing empty sets.")
                queried = set(registry.queryDataIds(graph))
                self.assertEqual(expected, queried)

        # Overlap each DatabaseDimensionElement with the commonSkyPix system.
        commonSkyPix = registry.dimensions.commonSkyPix
        for elementName, regions in regions.items():
            graph = DimensionGraph.union(registry.dimensions[elementName].graph, commonSkyPix.graph)
            expected = set()
            for dataId, region in regions.items():
                for begin, end in commonSkyPix.pixelization.envelope(region):
                    expected.update(
                        DataCoordinate.standardize({commonSkyPix.name: index, **dataId.byName()}, graph=graph)
                        for index in range(begin, end)
                    )
            self.assertGreater(len(expected), 2, msg="Test that we aren't just comparing empty sets.")
            queried = set(registry.queryDataIds(graph))
            self.assertEqual(expected, queried)

    def testAbstractQuery(self):
        """Test that we can run a query that just lists the known
        bands.  This is tricky because band is
        backed by a query against physical_filter.
        """
        registry = self.makeRegistry()
        registry.insertDimensionData("instrument", dict(name="DummyCam"))
        registry.insertDimensionData(
            "physical_filter",
            dict(instrument="DummyCam", name="dummy_i", band="i"),
            dict(instrument="DummyCam", name="dummy_i2", band="i"),
            dict(instrument="DummyCam", name="dummy_r", band="r"),
        )
        rows = registry.queryDataIds(["band"]).toSet()
        self.assertCountEqual(
            rows,
            [
                DataCoordinate.standardize(band="i", universe=registry.dimensions),
                DataCoordinate.standardize(band="r", universe=registry.dimensions),
            ],
        )

    def testAttributeManager(self):
        """Test basic functionality of attribute manager."""
        # number of attributes with schema versions in a fresh database,
        # 6 managers with 3 records per manager, plus config for dimensions
        VERSION_COUNT = 6 * 3 + 1

        registry = self.makeRegistry()
        attributes = registry._managers.attributes

        # check what get() returns for non-existing key
        self.assertIsNone(attributes.get("attr"))
        self.assertEqual(attributes.get("attr", ""), "")
        self.assertEqual(attributes.get("attr", "Value"), "Value")
        self.assertEqual(len(list(attributes.items())), VERSION_COUNT)

        # cannot store empty key or value
        with self.assertRaises(ValueError):
            attributes.set("", "value")
        with self.assertRaises(ValueError):
            attributes.set("attr", "")

        # set value of non-existing key
        attributes.set("attr", "value")
        self.assertEqual(len(list(attributes.items())), VERSION_COUNT + 1)
        self.assertEqual(attributes.get("attr"), "value")

        # update value of existing key
        with self.assertRaises(ButlerAttributeExistsError):
            attributes.set("attr", "value2")

        attributes.set("attr", "value2", force=True)
        self.assertEqual(len(list(attributes.items())), VERSION_COUNT + 1)
        self.assertEqual(attributes.get("attr"), "value2")

        # delete existing key
        self.assertTrue(attributes.delete("attr"))
        self.assertEqual(len(list(attributes.items())), VERSION_COUNT)

        # delete non-existing key
        self.assertFalse(attributes.delete("non-attr"))

        # store bunch of keys and get the list back
        data = [
            ("version.core", "1.2.3"),
            ("version.dimensions", "3.2.1"),
            ("config.managers.opaque", "ByNameOpaqueTableStorageManager"),
        ]
        for key, value in data:
            attributes.set(key, value)
        items = dict(attributes.items())
        for key, value in data:
            self.assertEqual(items[key], value)

    def testQueryDatasetsDeduplication(self):
        """Test that the findFirst option to queryDatasets selects datasets
        from collections in the order given".
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        self.assertCountEqual(
            list(registry.queryDatasets("bias", collections=["imported_g", "imported_r"])),
            [
                registry.findDataset("bias", instrument="Cam1", detector=1, collections="imported_g"),
                registry.findDataset("bias", instrument="Cam1", detector=2, collections="imported_g"),
                registry.findDataset("bias", instrument="Cam1", detector=3, collections="imported_g"),
                registry.findDataset("bias", instrument="Cam1", detector=2, collections="imported_r"),
                registry.findDataset("bias", instrument="Cam1", detector=3, collections="imported_r"),
                registry.findDataset("bias", instrument="Cam1", detector=4, collections="imported_r"),
            ],
        )
        self.assertCountEqual(
            list(registry.queryDatasets("bias", collections=["imported_g", "imported_r"], findFirst=True)),
            [
                registry.findDataset("bias", instrument="Cam1", detector=1, collections="imported_g"),
                registry.findDataset("bias", instrument="Cam1", detector=2, collections="imported_g"),
                registry.findDataset("bias", instrument="Cam1", detector=3, collections="imported_g"),
                registry.findDataset("bias", instrument="Cam1", detector=4, collections="imported_r"),
            ],
        )
        self.assertCountEqual(
            list(registry.queryDatasets("bias", collections=["imported_r", "imported_g"], findFirst=True)),
            [
                registry.findDataset("bias", instrument="Cam1", detector=1, collections="imported_g"),
                registry.findDataset("bias", instrument="Cam1", detector=2, collections="imported_r"),
                registry.findDataset("bias", instrument="Cam1", detector=3, collections="imported_r"),
                registry.findDataset("bias", instrument="Cam1", detector=4, collections="imported_r"),
            ],
        )

    def testQueryResults(self):
        """Test querying for data IDs and then manipulating the QueryResults
        object returned to perform other queries.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        bias = registry.getDatasetType("bias")
        flat = registry.getDatasetType("flat")
        # Obtain expected results from methods other than those we're testing
        # here.  That includes:
        # - the dimensions of the data IDs we want to query:
        expectedGraph = DimensionGraph(registry.dimensions, names=["detector", "physical_filter"])
        # - the dimensions of some other data IDs we'll extract from that:
        expectedSubsetGraph = DimensionGraph(registry.dimensions, names=["detector"])
        # - the data IDs we expect to obtain from the first queries:
        expectedDataIds = DataCoordinateSet(
            {
                DataCoordinate.standardize(
                    instrument="Cam1", detector=d, physical_filter=p, universe=registry.dimensions
                )
                for d, p in itertools.product({1, 2, 3}, {"Cam1-G", "Cam1-R1", "Cam1-R2"})
            },
            graph=expectedGraph,
            hasFull=False,
            hasRecords=False,
        )
        # - the flat datasets we expect to find from those data IDs, in just
        #   one collection (so deduplication is irrelevant):
        expectedFlats = [
            registry.findDataset(
                flat, instrument="Cam1", detector=1, physical_filter="Cam1-R1", collections="imported_r"
            ),
            registry.findDataset(
                flat, instrument="Cam1", detector=2, physical_filter="Cam1-R1", collections="imported_r"
            ),
            registry.findDataset(
                flat, instrument="Cam1", detector=3, physical_filter="Cam1-R2", collections="imported_r"
            ),
        ]
        # - the data IDs we expect to extract from that:
        expectedSubsetDataIds = expectedDataIds.subset(expectedSubsetGraph)
        # - the bias datasets we expect to find from those data IDs, after we
        #   subset-out the physical_filter dimension, both with duplicates:
        expectedAllBiases = [
            registry.findDataset(bias, instrument="Cam1", detector=1, collections="imported_g"),
            registry.findDataset(bias, instrument="Cam1", detector=2, collections="imported_g"),
            registry.findDataset(bias, instrument="Cam1", detector=3, collections="imported_g"),
            registry.findDataset(bias, instrument="Cam1", detector=2, collections="imported_r"),
            registry.findDataset(bias, instrument="Cam1", detector=3, collections="imported_r"),
        ]
        # - ...and without duplicates:
        expectedDeduplicatedBiases = [
            registry.findDataset(bias, instrument="Cam1", detector=1, collections="imported_g"),
            registry.findDataset(bias, instrument="Cam1", detector=2, collections="imported_r"),
            registry.findDataset(bias, instrument="Cam1", detector=3, collections="imported_r"),
        ]
        # Test against those expected results, using a "lazy" query for the
        # data IDs (which re-executes that query each time we use it to do
        # something new).
        dataIds = registry.queryDataIds(
            ["detector", "physical_filter"],
            where="detector.purpose = 'SCIENCE'",  # this rejects detector=4
            instrument="Cam1",
        )
        self.assertEqual(dataIds.graph, expectedGraph)
        self.assertEqual(dataIds.toSet(), expectedDataIds)
        self.assertCountEqual(
            list(
                dataIds.findDatasets(
                    flat,
                    collections=["imported_r"],
                )
            ),
            expectedFlats,
        )
        subsetDataIds = dataIds.subset(expectedSubsetGraph, unique=True)
        self.assertEqual(subsetDataIds.graph, expectedSubsetGraph)
        self.assertEqual(subsetDataIds.toSet(), expectedSubsetDataIds)
        self.assertCountEqual(
            list(subsetDataIds.findDatasets(bias, collections=["imported_r", "imported_g"], findFirst=False)),
            expectedAllBiases,
        )
        self.assertCountEqual(
            list(subsetDataIds.findDatasets(bias, collections=["imported_r", "imported_g"], findFirst=True)),
            expectedDeduplicatedBiases,
        )

        # Check dimensions match.
        with self.assertRaises(ValueError):
            subsetDataIds.findDatasets("flat", collections=["imported_r", "imported_g"], findFirst=True)

        # Use a component dataset type.
        self.assertCountEqual(
            [
                ref.makeComponentRef("image")
                for ref in subsetDataIds.findDatasets(
                    bias,
                    collections=["imported_r", "imported_g"],
                    findFirst=False,
                )
            ],
            [ref.makeComponentRef("image") for ref in expectedAllBiases],
        )

        # Use a named dataset type that does not exist and a dataset type
        # object that does not exist.
        unknown_type = DatasetType("not_known", dimensions=bias.dimensions, storageClass="Exposure")

        # Test both string name and dataset type object.
        test_type: Union[str, DatasetType]
        for test_type, test_type_name in (
            (unknown_type, unknown_type.name),
            (unknown_type.name, unknown_type.name),
        ):
            with self.assertRaisesRegex(DatasetTypeError, expected_regex=test_type_name):
                list(
                    subsetDataIds.findDatasets(
                        test_type, collections=["imported_r", "imported_g"], findFirst=True
                    )
                )

        # Materialize the bias dataset queries (only) by putting the results
        # into temporary tables, then repeat those tests.
        with subsetDataIds.findDatasets(
            bias, collections=["imported_r", "imported_g"], findFirst=False
        ).materialize() as biases:
            self.assertCountEqual(list(biases), expectedAllBiases)
        with subsetDataIds.findDatasets(
            bias, collections=["imported_r", "imported_g"], findFirst=True
        ).materialize() as biases:
            self.assertCountEqual(list(biases), expectedDeduplicatedBiases)
        # Materialize the data ID subset query, but not the dataset queries.
        with subsetDataIds.materialize() as subsetDataIds:
            self.assertEqual(subsetDataIds.graph, expectedSubsetGraph)
            self.assertEqual(subsetDataIds.toSet(), expectedSubsetDataIds)
            self.assertCountEqual(
                list(
                    subsetDataIds.findDatasets(
                        bias, collections=["imported_r", "imported_g"], findFirst=False
                    )
                ),
                expectedAllBiases,
            )
            self.assertCountEqual(
                list(
                    subsetDataIds.findDatasets(bias, collections=["imported_r", "imported_g"], findFirst=True)
                ),
                expectedDeduplicatedBiases,
            )
            # Materialize the dataset queries, too.
            with subsetDataIds.findDatasets(
                bias, collections=["imported_r", "imported_g"], findFirst=False
            ).materialize() as biases:
                self.assertCountEqual(list(biases), expectedAllBiases)
            with subsetDataIds.findDatasets(
                bias, collections=["imported_r", "imported_g"], findFirst=True
            ).materialize() as biases:
                self.assertCountEqual(list(biases), expectedDeduplicatedBiases)
        # Materialize the original query, but none of the follow-up queries.
        with dataIds.materialize() as dataIds:
            self.assertEqual(dataIds.graph, expectedGraph)
            self.assertEqual(dataIds.toSet(), expectedDataIds)
            self.assertCountEqual(
                list(
                    dataIds.findDatasets(
                        flat,
                        collections=["imported_r"],
                    )
                ),
                expectedFlats,
            )
            subsetDataIds = dataIds.subset(expectedSubsetGraph, unique=True)
            self.assertEqual(subsetDataIds.graph, expectedSubsetGraph)
            self.assertEqual(subsetDataIds.toSet(), expectedSubsetDataIds)
            self.assertCountEqual(
                list(
                    subsetDataIds.findDatasets(
                        bias, collections=["imported_r", "imported_g"], findFirst=False
                    )
                ),
                expectedAllBiases,
            )
            self.assertCountEqual(
                list(
                    subsetDataIds.findDatasets(bias, collections=["imported_r", "imported_g"], findFirst=True)
                ),
                expectedDeduplicatedBiases,
            )
            # Materialize just the bias dataset queries.
            with subsetDataIds.findDatasets(
                bias, collections=["imported_r", "imported_g"], findFirst=False
            ).materialize() as biases:
                self.assertCountEqual(list(biases), expectedAllBiases)
            with subsetDataIds.findDatasets(
                bias, collections=["imported_r", "imported_g"], findFirst=True
            ).materialize() as biases:
                self.assertCountEqual(list(biases), expectedDeduplicatedBiases)
            # Materialize the subset data ID query, but not the dataset
            # queries.
            with subsetDataIds.materialize() as subsetDataIds:
                self.assertEqual(subsetDataIds.graph, expectedSubsetGraph)
                self.assertEqual(subsetDataIds.toSet(), expectedSubsetDataIds)
                self.assertCountEqual(
                    list(
                        subsetDataIds.findDatasets(
                            bias, collections=["imported_r", "imported_g"], findFirst=False
                        )
                    ),
                    expectedAllBiases,
                )
                self.assertCountEqual(
                    list(
                        subsetDataIds.findDatasets(
                            bias, collections=["imported_r", "imported_g"], findFirst=True
                        )
                    ),
                    expectedDeduplicatedBiases,
                )
                # Materialize the bias dataset queries, too, so now we're
                # materializing every single step.
                with subsetDataIds.findDatasets(
                    bias, collections=["imported_r", "imported_g"], findFirst=False
                ).materialize() as biases:
                    self.assertCountEqual(list(biases), expectedAllBiases)
                with subsetDataIds.findDatasets(
                    bias, collections=["imported_r", "imported_g"], findFirst=True
                ).materialize() as biases:
                    self.assertCountEqual(list(biases), expectedDeduplicatedBiases)

    def testStorageClassPropagation(self):
        """Test that queries for datasets respect the storage class passed in
        as part of a full dataset type.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        dataset_type_in_registry = DatasetType(
            "tbl", dimensions=["instrument"], storageClass="DataFrame", universe=registry.dimensions
        )
        registry.registerDatasetType(dataset_type_in_registry)
        run = "run1"
        registry.registerRun(run)
        (inserted_ref,) = registry.insertDatasets(
            dataset_type_in_registry, [registry.expandDataId(instrument="Cam1")], run=run
        )
        self.assertEqual(inserted_ref.datasetType, dataset_type_in_registry)
        query_dataset_type = DatasetType(
            "tbl", dimensions=["instrument"], storageClass="ArrowAstropy", universe=registry.dimensions
        )
        self.assertNotEqual(dataset_type_in_registry, query_dataset_type)
        query_datasets_result = registry.queryDatasets(query_dataset_type, collections=[run])
        self.assertEqual(query_datasets_result.parentDatasetType, query_dataset_type)  # type: ignore
        (query_datasets_ref,) = query_datasets_result
        self.assertEqual(query_datasets_ref.datasetType, query_dataset_type)
        query_data_ids_find_datasets_result = registry.queryDataIds(["instrument"]).findDatasets(
            query_dataset_type, collections=[run]
        )
        self.assertEqual(query_data_ids_find_datasets_result.parentDatasetType, query_dataset_type)
        (query_data_ids_find_datasets_ref,) = query_data_ids_find_datasets_result
        self.assertEqual(query_data_ids_find_datasets_ref.datasetType, query_dataset_type)
        query_dataset_types_result = registry.queryDatasetTypes(query_dataset_type)
        self.assertEqual(list(query_dataset_types_result), [query_dataset_type])
        find_dataset_ref = registry.findDataset(query_dataset_type, instrument="Cam1", collections=[run])
        self.assertEqual(find_dataset_ref.datasetType, query_dataset_type)

    def testEmptyDimensionsQueries(self):
        """Test Query and QueryResults objects in the case where there are no
        dimensions.
        """
        # Set up test data: one dataset type, two runs, one dataset in each.
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        schema = DatasetType("schema", dimensions=registry.dimensions.empty, storageClass="Catalog")
        registry.registerDatasetType(schema)
        dataId = DataCoordinate.makeEmpty(registry.dimensions)
        run1 = "run1"
        run2 = "run2"
        registry.registerRun(run1)
        registry.registerRun(run2)
        (dataset1,) = registry.insertDatasets(schema, dataIds=[dataId], run=run1)
        (dataset2,) = registry.insertDatasets(schema, dataIds=[dataId], run=run2)
        # Query directly for both of the datasets, and each one, one at a time.
        self.checkQueryResults(
            registry.queryDatasets(schema, collections=[run1, run2], findFirst=False), [dataset1, dataset2]
        )
        self.checkQueryResults(
            registry.queryDatasets(schema, collections=[run1, run2], findFirst=True),
            [dataset1],
        )
        self.checkQueryResults(
            registry.queryDatasets(schema, collections=[run2, run1], findFirst=True),
            [dataset2],
        )
        # Query for data IDs with no dimensions.
        dataIds = registry.queryDataIds([])
        self.checkQueryResults(dataIds, [dataId])
        # Use queried data IDs to find the datasets.
        self.checkQueryResults(
            dataIds.findDatasets(schema, collections=[run1, run2], findFirst=False),
            [dataset1, dataset2],
        )
        self.checkQueryResults(
            dataIds.findDatasets(schema, collections=[run1, run2], findFirst=True),
            [dataset1],
        )
        self.checkQueryResults(
            dataIds.findDatasets(schema, collections=[run2, run1], findFirst=True),
            [dataset2],
        )
        # Now materialize the data ID query results and repeat those tests.
        with dataIds.materialize() as dataIds:
            self.checkQueryResults(dataIds, [dataId])
            self.checkQueryResults(
                dataIds.findDatasets(schema, collections=[run1, run2], findFirst=True),
                [dataset1],
            )
            self.checkQueryResults(
                dataIds.findDatasets(schema, collections=[run2, run1], findFirst=True),
                [dataset2],
            )
        # Query for non-empty data IDs, then subset that to get the empty one.
        # Repeat the above tests starting from that.
        dataIds = registry.queryDataIds(["instrument"]).subset(registry.dimensions.empty, unique=True)
        self.checkQueryResults(dataIds, [dataId])
        self.checkQueryResults(
            dataIds.findDatasets(schema, collections=[run1, run2], findFirst=False),
            [dataset1, dataset2],
        )
        self.checkQueryResults(
            dataIds.findDatasets(schema, collections=[run1, run2], findFirst=True),
            [dataset1],
        )
        self.checkQueryResults(
            dataIds.findDatasets(schema, collections=[run2, run1], findFirst=True),
            [dataset2],
        )
        with dataIds.materialize() as dataIds:
            self.checkQueryResults(dataIds, [dataId])
            self.checkQueryResults(
                dataIds.findDatasets(schema, collections=[run1, run2], findFirst=False),
                [dataset1, dataset2],
            )
            self.checkQueryResults(
                dataIds.findDatasets(schema, collections=[run1, run2], findFirst=True),
                [dataset1],
            )
            self.checkQueryResults(
                dataIds.findDatasets(schema, collections=[run2, run1], findFirst=True),
                [dataset2],
            )
        # Query for non-empty data IDs, then materialize, then subset to get
        # the empty one.  Repeat again.
        with registry.queryDataIds(["instrument"]).materialize() as nonEmptyDataIds:
            dataIds = nonEmptyDataIds.subset(registry.dimensions.empty, unique=True)
            self.checkQueryResults(dataIds, [dataId])
            self.checkQueryResults(
                dataIds.findDatasets(schema, collections=[run1, run2], findFirst=False),
                [dataset1, dataset2],
            )
            self.checkQueryResults(
                dataIds.findDatasets(schema, collections=[run1, run2], findFirst=True),
                [dataset1],
            )
            self.checkQueryResults(
                dataIds.findDatasets(schema, collections=[run2, run1], findFirst=True),
                [dataset2],
            )
            with dataIds.materialize() as dataIds:
                self.checkQueryResults(dataIds, [dataId])
                self.checkQueryResults(
                    dataIds.findDatasets(schema, collections=[run1, run2], findFirst=False),
                    [dataset1, dataset2],
                )
                self.checkQueryResults(
                    dataIds.findDatasets(schema, collections=[run1, run2], findFirst=True),
                    [dataset1],
                )
                self.checkQueryResults(
                    dataIds.findDatasets(schema, collections=[run2, run1], findFirst=True),
                    [dataset2],
                )
        # Query for non-empty data IDs with a constraint on an empty-data-ID
        # dataset that exists.
        dataIds = registry.queryDataIds(["instrument"], datasets="schema", collections=...)
        self.checkQueryResults(
            dataIds.subset(unique=True),
            [DataCoordinate.standardize(instrument="Cam1", universe=registry.dimensions)],
        )
        # Again query for non-empty data IDs with a constraint on empty-data-ID
        # datasets, but when the datasets don't exist.  We delete the existing
        # dataset and query just that collection rather than creating a new
        # empty collection because this is a bit less likely for our build-time
        # logic to shortcut-out (via the collection summaries), and such a
        # shortcut would make this test a bit more trivial than we'd like.
        registry.removeDatasets([dataset2])
        dataIds = registry.queryDataIds(["instrument"], datasets="schema", collections=run2)
        self.checkQueryResults(dataIds, [])

    def testDimensionDataModifications(self):
        """Test that modifying dimension records via:
        syncDimensionData(..., update=True) and
        insertDimensionData(..., replace=True) works as expected, even in the
        presence of datasets using those dimensions and spatial overlap
        relationships.
        """

        def unpack_range_set(ranges: lsst.sphgeom.RangeSet) -> Iterator[int]:
            """Unpack a sphgeom.RangeSet into the integers it contains."""
            for begin, end in ranges:
                yield from range(begin, end)

        def range_set_hull(
            ranges: lsst.sphgeom.RangeSet,
            pixelization: lsst.sphgeom.HtmPixelization,
        ) -> lsst.sphgeom.ConvexPolygon:
            """Create a ConvexPolygon hull of the region defined by a set of
            HTM pixelization index ranges.
            """
            points = []
            for index in unpack_range_set(ranges):
                points.extend(pixelization.triangle(index).getVertices())
            return lsst.sphgeom.ConvexPolygon(points)

        # Use HTM to set up an initial parent region (one arbitrary trixel)
        # and four child regions (the trixels within the parent at the next
        # level.  We'll use the parent as a tract/visit region and the children
        # as its patch/visit_detector regions.
        registry = self.makeRegistry()
        htm6 = registry.dimensions.skypix["htm"][6].pixelization
        commonSkyPix = registry.dimensions.commonSkyPix.pixelization
        index = 12288
        child_ranges_small = lsst.sphgeom.RangeSet(index).scaled(4)
        assert htm6.universe().contains(child_ranges_small)
        child_regions_small = [htm6.triangle(i) for i in unpack_range_set(child_ranges_small)]
        parent_region_small = lsst.sphgeom.ConvexPolygon(
            list(itertools.chain.from_iterable(c.getVertices() for c in child_regions_small))
        )
        assert all(parent_region_small.contains(c) for c in child_regions_small)
        # Make a larger version of each child region, defined to be the set of
        # htm6 trixels that overlap the original's bounding circle.  Make a new
        # parent that's the convex hull of the new children.
        child_regions_large = [
            range_set_hull(htm6.envelope(c.getBoundingCircle()), htm6) for c in child_regions_small
        ]
        assert all(large.contains(small) for large, small in zip(child_regions_large, child_regions_small))
        parent_region_large = lsst.sphgeom.ConvexPolygon(
            list(itertools.chain.from_iterable(c.getVertices() for c in child_regions_large))
        )
        assert all(parent_region_large.contains(c) for c in child_regions_large)
        assert parent_region_large.contains(parent_region_small)
        assert not parent_region_small.contains(parent_region_large)
        assert not all(parent_region_small.contains(c) for c in child_regions_large)
        # Find some commonSkyPix indices that overlap the large regions but not
        # overlap the small regions.  We use commonSkyPix here to make sure the
        # real tests later involve what's in the database, not just post-query
        # filtering of regions.
        child_difference_indices = []
        for large, small in zip(child_regions_large, child_regions_small):
            difference = list(unpack_range_set(commonSkyPix.envelope(large) - commonSkyPix.envelope(small)))
            assert difference, "if this is empty, we can't test anything useful with these regions"
            assert all(
                not commonSkyPix.triangle(d).isDisjointFrom(large)
                and commonSkyPix.triangle(d).isDisjointFrom(small)
                for d in difference
            )
            child_difference_indices.append(difference)
        parent_difference_indices = list(
            unpack_range_set(
                commonSkyPix.envelope(parent_region_large) - commonSkyPix.envelope(parent_region_small)
            )
        )
        assert parent_difference_indices, "if this is empty, we can't test anything useful with these regions"
        assert all(
            (
                not commonSkyPix.triangle(d).isDisjointFrom(parent_region_large)
                and commonSkyPix.triangle(d).isDisjointFrom(parent_region_small)
            )
            for d in parent_difference_indices
        )
        # Now that we've finally got those regions, we'll insert the large ones
        # as tract/patch dimension records.
        skymap_name = "testing_v1"
        registry.insertDimensionData(
            "skymap",
            {
                "name": skymap_name,
                "hash": bytes([42]),
                "tract_max": 1,
                "patch_nx_max": 2,
                "patch_ny_max": 2,
            },
        )
        registry.insertDimensionData("tract", {"skymap": skymap_name, "id": 0, "region": parent_region_large})
        registry.insertDimensionData(
            "patch",
            *[
                {"skymap": skymap_name, "tract": 0, "id": n, "cell_x": n % 2, "cell_y": n // 2, "region": c}
                for n, c in enumerate(child_regions_large)
            ],
        )
        # Add at dataset that uses these dimensions to make sure that modifying
        # them doesn't disrupt foreign keys (need to make sure DB doesn't
        # implement insert with replace=True as delete-then-insert).
        dataset_type = DatasetType(
            "coadd",
            dimensions=["tract", "patch"],
            universe=registry.dimensions,
            storageClass="Exposure",
        )
        registry.registerDatasetType(dataset_type)
        registry.registerCollection("the_run", CollectionType.RUN)
        registry.insertDatasets(
            dataset_type,
            [{"skymap": skymap_name, "tract": 0, "patch": 2}],
            run="the_run",
        )
        # Query for tracts and patches that overlap some "difference" htm9
        # pixels; there should be overlaps, because the database has
        # the "large" suite of regions.
        self.assertEqual(
            {0},
            {
                data_id["tract"]
                for data_id in registry.queryDataIds(
                    ["tract"],
                    skymap=skymap_name,
                    dataId={registry.dimensions.commonSkyPix.name: parent_difference_indices[0]},
                )
            },
        )
        for patch_id, patch_difference_indices in enumerate(child_difference_indices):
            self.assertIn(
                patch_id,
                {
                    data_id["patch"]
                    for data_id in registry.queryDataIds(
                        ["patch"],
                        skymap=skymap_name,
                        dataId={registry.dimensions.commonSkyPix.name: patch_difference_indices[0]},
                    )
                },
            )
        # Use sync to update the tract region and insert to update the regions
        # of the patches, to the "small" suite.
        updated = registry.syncDimensionData(
            "tract",
            {"skymap": skymap_name, "id": 0, "region": parent_region_small},
            update=True,
        )
        self.assertEqual(updated, {"region": parent_region_large})
        registry.insertDimensionData(
            "patch",
            *[
                {"skymap": skymap_name, "tract": 0, "id": n, "cell_x": n % 2, "cell_y": n // 2, "region": c}
                for n, c in enumerate(child_regions_small)
            ],
            replace=True,
        )
        # Query again; there now should be no such overlaps, because the
        # database has the "small" suite of regions.
        self.assertFalse(
            set(
                registry.queryDataIds(
                    ["tract"],
                    skymap=skymap_name,
                    dataId={registry.dimensions.commonSkyPix.name: parent_difference_indices[0]},
                )
            )
        )
        for patch_id, patch_difference_indices in enumerate(child_difference_indices):
            self.assertNotIn(
                patch_id,
                {
                    data_id["patch"]
                    for data_id in registry.queryDataIds(
                        ["patch"],
                        skymap=skymap_name,
                        dataId={registry.dimensions.commonSkyPix.name: patch_difference_indices[0]},
                    )
                },
            )
        # Update back to the large regions and query one more time.
        updated = registry.syncDimensionData(
            "tract",
            {"skymap": skymap_name, "id": 0, "region": parent_region_large},
            update=True,
        )
        self.assertEqual(updated, {"region": parent_region_small})
        registry.insertDimensionData(
            "patch",
            *[
                {"skymap": skymap_name, "tract": 0, "id": n, "cell_x": n % 2, "cell_y": n // 2, "region": c}
                for n, c in enumerate(child_regions_large)
            ],
            replace=True,
        )
        self.assertEqual(
            {0},
            {
                data_id["tract"]
                for data_id in registry.queryDataIds(
                    ["tract"],
                    skymap=skymap_name,
                    dataId={registry.dimensions.commonSkyPix.name: parent_difference_indices[0]},
                )
            },
        )
        for patch_id, patch_difference_indices in enumerate(child_difference_indices):
            self.assertIn(
                patch_id,
                {
                    data_id["patch"]
                    for data_id in registry.queryDataIds(
                        ["patch"],
                        skymap=skymap_name,
                        dataId={registry.dimensions.commonSkyPix.name: patch_difference_indices[0]},
                    )
                },
            )

    def testCalibrationCollections(self):
        """Test operations on `~CollectionType.CALIBRATION` collections,
        including `Registry.certify`, `Registry.decertify`, and
        `Registry.findDataset`.
        """
        # Setup - make a Registry, fill it with some datasets in
        # non-calibration collections.
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        # Set up some timestamps.
        t1 = astropy.time.Time("2020-01-01T01:00:00", format="isot", scale="tai")
        t2 = astropy.time.Time("2020-01-01T02:00:00", format="isot", scale="tai")
        t3 = astropy.time.Time("2020-01-01T03:00:00", format="isot", scale="tai")
        t4 = astropy.time.Time("2020-01-01T04:00:00", format="isot", scale="tai")
        t5 = astropy.time.Time("2020-01-01T05:00:00", format="isot", scale="tai")
        allTimespans = [
            Timespan(a, b) for a, b in itertools.combinations([None, t1, t2, t3, t4, t5, None], r=2)
        ]
        # Get references to some datasets.
        bias2a = registry.findDataset("bias", instrument="Cam1", detector=2, collections="imported_g")
        bias3a = registry.findDataset("bias", instrument="Cam1", detector=3, collections="imported_g")
        bias2b = registry.findDataset("bias", instrument="Cam1", detector=2, collections="imported_r")
        bias3b = registry.findDataset("bias", instrument="Cam1", detector=3, collections="imported_r")
        # Register the main calibration collection we'll be working with.
        collection = "Cam1/calibs/default"
        registry.registerCollection(collection, type=CollectionType.CALIBRATION)
        # Cannot associate into a calibration collection (no timespan).
        with self.assertRaises(CollectionTypeError):
            registry.associate(collection, [bias2a])
        # Certify 2a dataset with [t2, t4) validity.
        registry.certify(collection, [bias2a], Timespan(begin=t2, end=t4))
        # Test that we can query for this dataset via the new collection, both
        # on its own and with a RUN collection, as long as we don't try to join
        # in temporal dimensions or use findFirst=True.
        self.assertEqual(
            set(registry.queryDatasets("bias", findFirst=False, collections=collection)),
            {bias2a},
        )
        self.assertEqual(
            set(registry.queryDatasets("bias", findFirst=False, collections=[collection, "imported_r"])),
            {
                bias2a,
                bias2b,
                bias3b,
                registry.findDataset("bias", instrument="Cam1", detector=4, collections="imported_r"),
            },
        )
        self.assertEqual(
            set(registry.queryDataIds("detector", datasets="bias", collections=collection)),
            {registry.expandDataId(instrument="Cam1", detector=2)},
        )
        self.assertEqual(
            set(registry.queryDataIds("detector", datasets="bias", collections=[collection, "imported_r"])),
            {
                registry.expandDataId(instrument="Cam1", detector=2),
                registry.expandDataId(instrument="Cam1", detector=3),
                registry.expandDataId(instrument="Cam1", detector=4),
            },
        )

        # We should not be able to certify 2b with anything overlapping that
        # window.
        with self.assertRaises(ConflictingDefinitionError):
            registry.certify(collection, [bias2b], Timespan(begin=None, end=t3))
        with self.assertRaises(ConflictingDefinitionError):
            registry.certify(collection, [bias2b], Timespan(begin=None, end=t5))
        with self.assertRaises(ConflictingDefinitionError):
            registry.certify(collection, [bias2b], Timespan(begin=t1, end=t3))
        with self.assertRaises(ConflictingDefinitionError):
            registry.certify(collection, [bias2b], Timespan(begin=t1, end=t5))
        with self.assertRaises(ConflictingDefinitionError):
            registry.certify(collection, [bias2b], Timespan(begin=t1, end=None))
        with self.assertRaises(ConflictingDefinitionError):
            registry.certify(collection, [bias2b], Timespan(begin=t2, end=t3))
        with self.assertRaises(ConflictingDefinitionError):
            registry.certify(collection, [bias2b], Timespan(begin=t2, end=t5))
        with self.assertRaises(ConflictingDefinitionError):
            registry.certify(collection, [bias2b], Timespan(begin=t2, end=None))
        # We should be able to certify 3a with a range overlapping that window,
        # because it's for a different detector.
        # We'll certify 3a over [t1, t3).
        registry.certify(collection, [bias3a], Timespan(begin=t1, end=t3))
        # Now we'll certify 2b and 3b together over [t4, ∞).
        registry.certify(collection, [bias2b, bias3b], Timespan(begin=t4, end=None))

        # Fetch all associations and check that they are what we expect.
        self.assertCountEqual(
            list(
                registry.queryDatasetAssociations(
                    "bias",
                    collections=[collection, "imported_g", "imported_r"],
                )
            ),
            [
                DatasetAssociation(
                    ref=registry.findDataset("bias", instrument="Cam1", detector=1, collections="imported_g"),
                    collection="imported_g",
                    timespan=None,
                ),
                DatasetAssociation(
                    ref=registry.findDataset("bias", instrument="Cam1", detector=4, collections="imported_r"),
                    collection="imported_r",
                    timespan=None,
                ),
                DatasetAssociation(ref=bias2a, collection="imported_g", timespan=None),
                DatasetAssociation(ref=bias3a, collection="imported_g", timespan=None),
                DatasetAssociation(ref=bias2b, collection="imported_r", timespan=None),
                DatasetAssociation(ref=bias3b, collection="imported_r", timespan=None),
                DatasetAssociation(ref=bias2a, collection=collection, timespan=Timespan(begin=t2, end=t4)),
                DatasetAssociation(ref=bias3a, collection=collection, timespan=Timespan(begin=t1, end=t3)),
                DatasetAssociation(ref=bias2b, collection=collection, timespan=Timespan(begin=t4, end=None)),
                DatasetAssociation(ref=bias3b, collection=collection, timespan=Timespan(begin=t4, end=None)),
            ],
        )

        class Ambiguous:
            """Tag class to denote lookups that should be ambiguous."""

            pass

        def assertLookup(
            detector: int, timespan: Timespan, expected: Optional[Union[DatasetRef, Type[Ambiguous]]]
        ) -> None:
            """Local function that asserts that a bias lookup returns the given
            expected result.
            """
            if expected is Ambiguous:
                with self.assertRaises((DatasetTypeError, LookupError)):
                    registry.findDataset(
                        "bias",
                        collections=collection,
                        instrument="Cam1",
                        detector=detector,
                        timespan=timespan,
                    )
            else:
                self.assertEqual(
                    expected,
                    registry.findDataset(
                        "bias",
                        collections=collection,
                        instrument="Cam1",
                        detector=detector,
                        timespan=timespan,
                    ),
                )

        # Systematically test lookups against expected results.
        assertLookup(detector=2, timespan=Timespan(None, t1), expected=None)
        assertLookup(detector=2, timespan=Timespan(None, t2), expected=None)
        assertLookup(detector=2, timespan=Timespan(None, t3), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(None, t4), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(None, t5), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(None, None), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(t1, t2), expected=None)
        assertLookup(detector=2, timespan=Timespan(t1, t3), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t1, t4), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t1, t5), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(t1, None), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(t2, t3), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t2, t4), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t2, t5), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(t2, None), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(t3, t4), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t3, t5), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(t3, None), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(t4, t5), expected=bias2b)
        assertLookup(detector=2, timespan=Timespan(t4, None), expected=bias2b)
        assertLookup(detector=2, timespan=Timespan(t5, None), expected=bias2b)
        assertLookup(detector=3, timespan=Timespan(None, t1), expected=None)
        assertLookup(detector=3, timespan=Timespan(None, t2), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(None, t3), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(None, t4), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(None, t5), expected=Ambiguous)
        assertLookup(detector=3, timespan=Timespan(None, None), expected=Ambiguous)
        assertLookup(detector=3, timespan=Timespan(t1, t2), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t1, t3), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t1, t4), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t1, t5), expected=Ambiguous)
        assertLookup(detector=3, timespan=Timespan(t1, None), expected=Ambiguous)
        assertLookup(detector=3, timespan=Timespan(t2, t3), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t2, t4), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t2, t5), expected=Ambiguous)
        assertLookup(detector=3, timespan=Timespan(t2, None), expected=Ambiguous)
        assertLookup(detector=3, timespan=Timespan(t3, t4), expected=None)
        assertLookup(detector=3, timespan=Timespan(t3, t5), expected=bias3b)
        assertLookup(detector=3, timespan=Timespan(t3, None), expected=bias3b)
        assertLookup(detector=3, timespan=Timespan(t4, t5), expected=bias3b)
        assertLookup(detector=3, timespan=Timespan(t4, None), expected=bias3b)
        assertLookup(detector=3, timespan=Timespan(t5, None), expected=bias3b)

        # Decertify [t3, t5) for all data IDs, and do test lookups again.
        # This should truncate bias2a to [t2, t3), leave bias3a unchanged at
        # [t1, t3), and truncate bias2b and bias3b to [t5, ∞).
        registry.decertify(collection=collection, datasetType="bias", timespan=Timespan(t3, t5))
        assertLookup(detector=2, timespan=Timespan(None, t1), expected=None)
        assertLookup(detector=2, timespan=Timespan(None, t2), expected=None)
        assertLookup(detector=2, timespan=Timespan(None, t3), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(None, t4), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(None, t5), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(None, None), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(t1, t2), expected=None)
        assertLookup(detector=2, timespan=Timespan(t1, t3), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t1, t4), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t1, t5), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t1, None), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(t2, t3), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t2, t4), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t2, t5), expected=bias2a)
        assertLookup(detector=2, timespan=Timespan(t2, None), expected=Ambiguous)
        assertLookup(detector=2, timespan=Timespan(t3, t4), expected=None)
        assertLookup(detector=2, timespan=Timespan(t3, t5), expected=None)
        assertLookup(detector=2, timespan=Timespan(t3, None), expected=bias2b)
        assertLookup(detector=2, timespan=Timespan(t4, t5), expected=None)
        assertLookup(detector=2, timespan=Timespan(t4, None), expected=bias2b)
        assertLookup(detector=2, timespan=Timespan(t5, None), expected=bias2b)
        assertLookup(detector=3, timespan=Timespan(None, t1), expected=None)
        assertLookup(detector=3, timespan=Timespan(None, t2), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(None, t3), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(None, t4), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(None, t5), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(None, None), expected=Ambiguous)
        assertLookup(detector=3, timespan=Timespan(t1, t2), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t1, t3), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t1, t4), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t1, t5), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t1, None), expected=Ambiguous)
        assertLookup(detector=3, timespan=Timespan(t2, t3), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t2, t4), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t2, t5), expected=bias3a)
        assertLookup(detector=3, timespan=Timespan(t2, None), expected=Ambiguous)
        assertLookup(detector=3, timespan=Timespan(t3, t4), expected=None)
        assertLookup(detector=3, timespan=Timespan(t3, t5), expected=None)
        assertLookup(detector=3, timespan=Timespan(t3, None), expected=bias3b)
        assertLookup(detector=3, timespan=Timespan(t4, t5), expected=None)
        assertLookup(detector=3, timespan=Timespan(t4, None), expected=bias3b)
        assertLookup(detector=3, timespan=Timespan(t5, None), expected=bias3b)

        # Decertify everything, this time with explicit data IDs, then check
        # that no lookups succeed.
        registry.decertify(
            collection,
            "bias",
            Timespan(None, None),
            dataIds=[
                dict(instrument="Cam1", detector=2),
                dict(instrument="Cam1", detector=3),
            ],
        )
        for detector in (2, 3):
            for timespan in allTimespans:
                assertLookup(detector=detector, timespan=timespan, expected=None)
        # Certify bias2a and bias3a over (-∞, ∞), check that all lookups return
        # those.
        registry.certify(
            collection,
            [bias2a, bias3a],
            Timespan(None, None),
        )
        for timespan in allTimespans:
            assertLookup(detector=2, timespan=timespan, expected=bias2a)
            assertLookup(detector=3, timespan=timespan, expected=bias3a)
        # Decertify just bias2 over [t2, t4).
        # This should split a single certification row into two (and leave the
        # other existing row, for bias3a, alone).
        registry.decertify(
            collection, "bias", Timespan(t2, t4), dataIds=[dict(instrument="Cam1", detector=2)]
        )
        for timespan in allTimespans:
            assertLookup(detector=3, timespan=timespan, expected=bias3a)
            overlapsBefore = timespan.overlaps(Timespan(None, t2))
            overlapsAfter = timespan.overlaps(Timespan(t4, None))
            if overlapsBefore and overlapsAfter:
                expected = Ambiguous
            elif overlapsBefore or overlapsAfter:
                expected = bias2a
            else:
                expected = None
            assertLookup(detector=2, timespan=timespan, expected=expected)

    def testSkipCalibs(self):
        """Test how queries handle skipping of calibration collections."""
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")

        coll_calib = "Cam1/calibs/default"
        registry.registerCollection(coll_calib, type=CollectionType.CALIBRATION)

        # Add all biases to the calibration collection.
        # Without this, the logic that prunes dataset subqueries based on
        # datasetType-collection summary information will fire before the logic
        # we want to test below.  This is a good thing (it avoids the dreaded
        # NotImplementedError a bit more often) everywhere but here.
        registry.certify(coll_calib, registry.queryDatasets("bias", collections=...), Timespan(None, None))

        coll_list = [coll_calib, "imported_g", "imported_r"]
        chain = "Cam1/chain"
        registry.registerCollection(chain, type=CollectionType.CHAINED)
        registry.setCollectionChain(chain, coll_list)

        # explicit list will raise if findFirst=True or there are temporal
        # dimensions
        with self.assertRaises(NotImplementedError):
            registry.queryDatasets("bias", collections=coll_list, findFirst=True)
        with self.assertRaises(NotImplementedError):
            registry.queryDataIds(
                ["instrument", "detector", "exposure"], datasets="bias", collections=coll_list
            ).count()

        # chain will skip
        datasets = list(registry.queryDatasets("bias", collections=chain))
        self.assertGreater(len(datasets), 0)

        dataIds = list(registry.queryDataIds(["instrument", "detector"], datasets="bias", collections=chain))
        self.assertGreater(len(dataIds), 0)

        # glob will skip too
        datasets = list(registry.queryDatasets("bias", collections="*d*"))
        self.assertGreater(len(datasets), 0)

        # regular expression will skip too
        pattern = re.compile(".*")
        datasets = list(registry.queryDatasets("bias", collections=pattern))
        self.assertGreater(len(datasets), 0)

        # ellipsis should work as usual
        datasets = list(registry.queryDatasets("bias", collections=...))
        self.assertGreater(len(datasets), 0)

        # few tests with findFirst
        datasets = list(registry.queryDatasets("bias", collections=chain, findFirst=True))
        self.assertGreater(len(datasets), 0)

    def testIngestTimeQuery(self):
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        dt0 = datetime.utcnow()
        self.loadData(registry, "datasets.yaml")
        dt1 = datetime.utcnow()

        datasets = list(registry.queryDatasets(..., collections=...))
        len0 = len(datasets)
        self.assertGreater(len0, 0)

        where = "ingest_date > T'2000-01-01'"
        datasets = list(registry.queryDatasets(..., collections=..., where=where))
        len1 = len(datasets)
        self.assertEqual(len0, len1)

        # no one will ever use this piece of software in 30 years
        where = "ingest_date > T'2050-01-01'"
        datasets = list(registry.queryDatasets(..., collections=..., where=where))
        len2 = len(datasets)
        self.assertEqual(len2, 0)

        # Check more exact timing to make sure there is no 37 seconds offset
        # (after fixing DM-30124). SQLite time precision is 1 second, make
        # sure that we don't test with higher precision.
        tests = [
            # format: (timestamp, operator, expected_len)
            (dt0 - timedelta(seconds=1), ">", len0),
            (dt0 - timedelta(seconds=1), "<", 0),
            (dt1 + timedelta(seconds=1), "<", len0),
            (dt1 + timedelta(seconds=1), ">", 0),
        ]
        for dt, op, expect_len in tests:
            dt_str = dt.isoformat(sep=" ")

            where = f"ingest_date {op} T'{dt_str}'"
            datasets = list(registry.queryDatasets(..., collections=..., where=where))
            self.assertEqual(len(datasets), expect_len)

            # same with bind using datetime or astropy Time
            where = f"ingest_date {op} ingest_time"
            datasets = list(
                registry.queryDatasets(..., collections=..., where=where, bind={"ingest_time": dt})
            )
            self.assertEqual(len(datasets), expect_len)

            dt_astropy = astropy.time.Time(dt, format="datetime")
            datasets = list(
                registry.queryDatasets(..., collections=..., where=where, bind={"ingest_time": dt_astropy})
            )
            self.assertEqual(len(datasets), expect_len)

    def testTimespanQueries(self):
        """Test query expressions involving timespans."""
        registry = self.makeRegistry()
        self.loadData(registry, "hsc-rc2-subset.yaml")
        # All exposures in the database; mapping from ID to timespan.
        visits = {record.id: record.timespan for record in registry.queryDimensionRecords("visit")}
        # Just those IDs, sorted (which is also temporal sorting, because HSC
        # exposure IDs are monotonically increasing).
        ids = sorted(visits.keys())
        self.assertGreater(len(ids), 20)
        # Pick some quasi-random indexes into `ids` to play with.
        i1 = int(len(ids) * 0.1)
        i2 = int(len(ids) * 0.3)
        i3 = int(len(ids) * 0.6)
        i4 = int(len(ids) * 0.8)
        # Extract some times from those: just before the beginning of i1 (which
        # should be after the end of the exposure before), exactly the
        # beginning of i2, just after the beginning of i3 (and before its end),
        # and the exact end of i4.
        t1 = visits[ids[i1]].begin - astropy.time.TimeDelta(1.0, format="sec")
        self.assertGreater(t1, visits[ids[i1 - 1]].end)
        t2 = visits[ids[i2]].begin
        t3 = visits[ids[i3]].begin + astropy.time.TimeDelta(1.0, format="sec")
        self.assertLess(t3, visits[ids[i3]].end)
        t4 = visits[ids[i4]].end
        # Make sure those are actually in order.
        self.assertEqual([t1, t2, t3, t4], sorted([t4, t3, t2, t1]))

        bind = {
            "t1": t1,
            "t2": t2,
            "t3": t3,
            "t4": t4,
            "ts23": Timespan(t2, t3),
        }

        def query(where):
            """Helper function that queries for visit data IDs and returns
            results as a sorted, deduplicated list of visit IDs.
            """
            return sorted(
                {
                    dataId["visit"]
                    for dataId in registry.queryDataIds("visit", instrument="HSC", bind=bind, where=where)
                }
            )

        # Try a bunch of timespan queries, mixing up the bounds themselves,
        # where they appear in the expression, and how we get the timespan into
        # the expression.

        # t1 is before the start of i1, so this should not include i1.
        self.assertEqual(ids[:i1], query("visit.timespan OVERLAPS (null, t1)"))
        # t2 is exactly at the start of i2, but ends are exclusive, so these
        # should not include i2.
        self.assertEqual(ids[i1:i2], query("(t1, t2) OVERLAPS visit.timespan"))
        self.assertEqual(ids[:i2], query("visit.timespan < (t2, t4)"))
        # t3 is in the middle of i3, so this should include i3.
        self.assertEqual(ids[i2 : i3 + 1], query("visit.timespan OVERLAPS ts23"))
        # This one should not include t3 by the same reasoning.
        self.assertEqual(ids[i3 + 1 :], query("visit.timespan > (t1, t3)"))
        # t4 is exactly at the end of i4, so this should include i4.
        self.assertEqual(ids[i3 : i4 + 1], query(f"visit.timespan OVERLAPS (T'{t3.tai.isot}', t4)"))
        # i4's upper bound of t4 is exclusive so this should not include t4.
        self.assertEqual(ids[i4 + 1 :], query("visit.timespan OVERLAPS (t4, NULL)"))

        # Now some timespan vs. time scalar queries.
        self.assertEqual(ids[:i2], query("visit.timespan < t2"))
        self.assertEqual(ids[:i2], query("t2 > visit.timespan"))
        self.assertEqual(ids[i3 + 1 :], query("visit.timespan > t3"))
        self.assertEqual(ids[i3 + 1 :], query("t3 < visit.timespan"))
        self.assertEqual(ids[i3 : i3 + 1], query("visit.timespan OVERLAPS t3"))
        self.assertEqual(ids[i3 : i3 + 1], query(f"T'{t3.tai.isot}' OVERLAPS visit.timespan"))

        # Empty timespans should not overlap anything.
        self.assertEqual([], query("visit.timespan OVERLAPS (t3, t2)"))

    def testCollectionSummaries(self):
        """Test recording and retrieval of collection summaries."""
        self.maxDiff = None
        registry = self.makeRegistry()
        # Importing datasets from yaml should go through the code path where
        # we update collection summaries as we insert datasets.
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        flat = registry.getDatasetType("flat")
        expected1 = CollectionSummary()
        expected1.dataset_types.add(registry.getDatasetType("bias"))
        expected1.add_data_ids(
            flat, [DataCoordinate.standardize(instrument="Cam1", universe=registry.dimensions)]
        )
        self.assertEqual(registry.getCollectionSummary("imported_g"), expected1)
        self.assertEqual(registry.getCollectionSummary("imported_r"), expected1)
        # Create a chained collection with both of the imported runs; the
        # summary should be the same, because it's a union with itself.
        chain = "chain"
        registry.registerCollection(chain, CollectionType.CHAINED)
        registry.setCollectionChain(chain, ["imported_r", "imported_g"])
        self.assertEqual(registry.getCollectionSummary(chain), expected1)
        # Associate flats only into a tagged collection and a calibration
        # collection to check summaries of those.
        tag = "tag"
        registry.registerCollection(tag, CollectionType.TAGGED)
        registry.associate(tag, registry.queryDatasets(flat, collections="imported_g"))
        calibs = "calibs"
        registry.registerCollection(calibs, CollectionType.CALIBRATION)
        registry.certify(
            calibs, registry.queryDatasets(flat, collections="imported_g"), timespan=Timespan(None, None)
        )
        expected2 = expected1.copy()
        expected2.dataset_types.discard("bias")
        self.assertEqual(registry.getCollectionSummary(tag), expected2)
        self.assertEqual(registry.getCollectionSummary(calibs), expected2)
        # Explicitly calling Registry.refresh() should load those same
        # summaries, via a totally different code path.
        registry.refresh()
        self.assertEqual(registry.getCollectionSummary("imported_g"), expected1)
        self.assertEqual(registry.getCollectionSummary("imported_r"), expected1)
        self.assertEqual(registry.getCollectionSummary(tag), expected2)
        self.assertEqual(registry.getCollectionSummary(calibs), expected2)

    def testBindInQueryDatasets(self):
        """Test that the bind parameter is correctly forwarded in
        queryDatasets recursion.
        """
        registry = self.makeRegistry()
        # Importing datasets from yaml should go through the code path where
        # we update collection summaries as we insert datasets.
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        self.assertEqual(
            set(registry.queryDatasets("flat", band="r", collections=...)),
            set(registry.queryDatasets("flat", where="band=my_band", bind={"my_band": "r"}, collections=...)),
        )

    def testQueryIntRangeExpressions(self):
        """Test integer range expressions in ``where`` arguments.

        Note that our expressions use inclusive stop values, unlike Python's.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.assertEqual(
            set(registry.queryDataIds(["detector"], instrument="Cam1", where="detector IN (1..2)")),
            {registry.expandDataId(instrument="Cam1", detector=n) for n in [1, 2]},
        )
        self.assertEqual(
            set(registry.queryDataIds(["detector"], instrument="Cam1", where="detector IN (1..4:2)")),
            {registry.expandDataId(instrument="Cam1", detector=n) for n in [1, 3]},
        )
        self.assertEqual(
            set(registry.queryDataIds(["detector"], instrument="Cam1", where="detector IN (2..4:2)")),
            {registry.expandDataId(instrument="Cam1", detector=n) for n in [2, 4]},
        )

    def testQueryResultSummaries(self):
        """Test summary methods like `count`, `any`, and `explain_no_results`
        on `DataCoordinateQueryResults` and `DatasetQueryResults`
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        self.loadData(registry, "spatial.yaml")
        # Default test dataset has two collections, each with both flats and
        # biases.  Add a new collection with only biases.
        registry.registerCollection("biases", CollectionType.TAGGED)
        registry.associate("biases", registry.queryDatasets("bias", collections=["imported_g"]))
        # First query yields two results, and involves no postprocessing.
        query1 = registry.queryDataIds(["physical_filter"], band="r")
        self.assertTrue(query1.any(execute=False, exact=False))
        self.assertTrue(query1.any(execute=True, exact=False))
        self.assertTrue(query1.any(execute=True, exact=True))
        self.assertEqual(query1.count(exact=False), 2)
        self.assertEqual(query1.count(exact=True), 2)
        self.assertFalse(list(query1.explain_no_results()))
        # Second query should yield no results, which we should see when
        # we attempt to expand the data ID.
        query2 = registry.queryDataIds(["physical_filter"], band="h")
        # There's no execute=False, exact=Fals test here because the behavior
        # not something we want to guarantee in this case (and exact=False
        # says either answer is legal).
        self.assertFalse(query2.any(execute=True, exact=False))
        self.assertFalse(query2.any(execute=True, exact=True))
        self.assertEqual(query2.count(exact=False), 0)
        self.assertEqual(query2.count(exact=True), 0)
        self.assertTrue(list(query2.explain_no_results()))
        # These queries yield no results due to various problems that can be
        # spotted prior to execution, yielding helpful diagnostics.
        base_query = registry.queryDataIds(["detector", "physical_filter"])
        queries_and_snippets = [
            (
                # Dataset type name doesn't match any existing dataset types.
                registry.queryDatasets("nonexistent", collections=...),
                ["nonexistent"],
            ),
            (
                # Dataset type object isn't registered.
                registry.queryDatasets(
                    DatasetType(
                        "nonexistent",
                        dimensions=["instrument"],
                        universe=registry.dimensions,
                        storageClass="Image",
                    ),
                    collections=...,
                ),
                ["nonexistent"],
            ),
            (
                # No datasets of this type in this collection.
                registry.queryDatasets("flat", collections=["biases"]),
                ["flat", "biases"],
            ),
            (
                # No datasets of this type in this collection.
                base_query.findDatasets("flat", collections=["biases"]),
                ["flat", "biases"],
            ),
            (
                # No collections matching at all.
                registry.queryDatasets("flat", collections=re.compile("potato.+")),
                ["potato"],
            ),
        ]
        # The behavior of these additional queries is slated to change in the
        # future, so we also check for deprecation warnings.
        with self.assertWarns(FutureWarning):
            queries_and_snippets.append(
                (
                    # Dataset type name doesn't match any existing dataset
                    # types.
                    registry.queryDataIds(["detector"], datasets=["nonexistent"], collections=...),
                    ["nonexistent"],
                )
            )
        with self.assertWarns(FutureWarning):
            queries_and_snippets.append(
                (
                    # Dataset type name doesn't match any existing dataset
                    # types.
                    registry.queryDimensionRecords("detector", datasets=["nonexistent"], collections=...),
                    ["nonexistent"],
                )
            )
        for query, snippets in queries_and_snippets:
            self.assertFalse(query.any(execute=False, exact=False))
            self.assertFalse(query.any(execute=True, exact=False))
            self.assertFalse(query.any(execute=True, exact=True))
            self.assertEqual(query.count(exact=False), 0)
            self.assertEqual(query.count(exact=True), 0)
            messages = list(query.explain_no_results())
            self.assertTrue(messages)
            # Want all expected snippets to appear in at least one message.
            self.assertTrue(
                any(
                    all(snippet in message for snippet in snippets) for message in query.explain_no_results()
                ),
                messages,
            )

        # This query does yield results, but should also emit a warning because
        # dataset type patterns to queryDataIds is deprecated; just look for
        # the warning.
        with self.assertWarns(FutureWarning):
            registry.queryDataIds(["detector"], datasets=re.compile("^nonexistent$"), collections=...)

        # These queries yield no results due to problems that can be identified
        # by cheap follow-up queries, yielding helpful diagnostics.
        for query, snippets in [
            (
                # No records for one of the involved dimensions.
                registry.queryDataIds(["subfilter"]),
                ["no rows", "subfilter"],
            ),
            (
                # No records for one of the involved dimensions.
                registry.queryDimensionRecords("subfilter"),
                ["no rows", "subfilter"],
            ),
        ]:
            self.assertFalse(query.any(execute=True, exact=False))
            self.assertFalse(query.any(execute=True, exact=True))
            self.assertEqual(query.count(exact=True), 0)
            messages = list(query.explain_no_results())
            self.assertTrue(messages)
            # Want all expected snippets to appear in at least one message.
            self.assertTrue(
                any(
                    all(snippet in message for snippet in snippets) for message in query.explain_no_results()
                ),
                messages,
            )

        # This query yields four overlaps in the database, but one is filtered
        # out in postprocessing.  The count queries aren't accurate because
        # they don't account for duplication that happens due to an internal
        # join against commonSkyPix.
        query3 = registry.queryDataIds(["visit", "tract"], instrument="Cam1", skymap="SkyMap1")
        self.assertEqual(
            {
                DataCoordinate.standardize(
                    instrument="Cam1",
                    skymap="SkyMap1",
                    visit=v,
                    tract=t,
                    universe=registry.dimensions,
                )
                for v, t in [(1, 0), (2, 0), (2, 1)]
            },
            set(query3),
        )
        self.assertTrue(query3.any(execute=False, exact=False))
        self.assertTrue(query3.any(execute=True, exact=False))
        self.assertTrue(query3.any(execute=True, exact=True))
        self.assertGreaterEqual(query3.count(exact=False), 4)
        self.assertGreaterEqual(query3.count(exact=True, discard=True), 3)
        self.assertFalse(list(query3.explain_no_results()))
        # This query yields overlaps in the database, but all are filtered
        # out in postprocessing.  The count queries again aren't very useful.
        # We have to use `where=` here to avoid an optimization that
        # (currently) skips the spatial postprocess-filtering because it
        # recognizes that no spatial join is necessary.  That's not ideal, but
        # fixing it is out of scope for this ticket.
        query4 = registry.queryDataIds(
            ["visit", "tract"],
            instrument="Cam1",
            skymap="SkyMap1",
            where="visit=1 AND detector=1 AND tract=0 AND patch=4",
        )
        self.assertFalse(set(query4))
        self.assertTrue(query4.any(execute=False, exact=False))
        self.assertTrue(query4.any(execute=True, exact=False))
        self.assertFalse(query4.any(execute=True, exact=True))
        self.assertGreaterEqual(query4.count(exact=False), 1)
        self.assertEqual(query4.count(exact=True, discard=True), 0)
        messages = query4.explain_no_results()
        self.assertTrue(messages)
        self.assertTrue(any("overlap" in message for message in messages))
        # This query should yield results from one dataset type but not the
        # other, which is not registered.
        query5 = registry.queryDatasets(["bias", "nonexistent"], collections=["biases"])
        self.assertTrue(set(query5))
        self.assertTrue(query5.any(execute=False, exact=False))
        self.assertTrue(query5.any(execute=True, exact=False))
        self.assertTrue(query5.any(execute=True, exact=True))
        self.assertGreaterEqual(query5.count(exact=False), 1)
        self.assertGreaterEqual(query5.count(exact=True), 1)
        self.assertFalse(list(query5.explain_no_results()))
        # This query applies a selection that yields no results, fully in the
        # database.  Explaining why it fails involves traversing the relation
        # tree and running a LIMIT 1 query at each level that has the potential
        # to remove rows.
        query6 = registry.queryDimensionRecords(
            "detector", where="detector.purpose = 'no-purpose'", instrument="Cam1"
        )
        self.assertEqual(query6.count(exact=True), 0)
        messages = query6.explain_no_results()
        self.assertTrue(messages)
        self.assertTrue(any("no-purpose" in message for message in messages))

    def testQueryDataIdsOrderBy(self):
        """Test order_by and limit on result returned by queryDataIds()."""
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        self.loadData(registry, "spatial.yaml")

        def do_query(dimensions=("visit", "tract"), datasets=None, collections=None):
            return registry.queryDataIds(
                dimensions, datasets=datasets, collections=collections, instrument="Cam1", skymap="SkyMap1"
            )

        Test = namedtuple(
            "testQueryDataIdsOrderByTest",
            ("order_by", "keys", "result", "limit", "datasets", "collections"),
            defaults=(None, None, None),
        )

        test_data = (
            Test("tract,visit", "tract,visit", ((0, 1), (0, 1), (0, 2), (0, 2), (1, 2), (1, 2))),
            Test("-tract,visit", "tract,visit", ((1, 2), (1, 2), (0, 1), (0, 1), (0, 2), (0, 2))),
            Test("tract,-visit", "tract,visit", ((0, 2), (0, 2), (0, 1), (0, 1), (1, 2), (1, 2))),
            Test("-tract,-visit", "tract,visit", ((1, 2), (1, 2), (0, 2), (0, 2), (0, 1), (0, 1))),
            Test(
                "tract.id,visit.id",
                "tract,visit",
                ((0, 1), (0, 1), (0, 2)),
                limit=(3,),
            ),
            Test("-tract,-visit", "tract,visit", ((1, 2), (1, 2), (0, 2)), limit=(3,)),
            Test("tract,visit", "tract,visit", ((0, 2), (1, 2), (1, 2)), limit=(3, 3)),
            Test("-tract,-visit", "tract,visit", ((0, 1),), limit=(3, 5)),
            Test(
                "tract,visit.exposure_time", "tract,visit", ((0, 2), (0, 2), (0, 1), (0, 1), (1, 2), (1, 2))
            ),
            Test(
                "-tract,-visit.exposure_time", "tract,visit", ((1, 2), (1, 2), (0, 1), (0, 1), (0, 2), (0, 2))
            ),
            Test("tract,-exposure_time", "tract,visit", ((0, 1), (0, 1), (0, 2), (0, 2), (1, 2), (1, 2))),
            Test("tract,visit.name", "tract,visit", ((0, 1), (0, 1), (0, 2), (0, 2), (1, 2), (1, 2))),
            Test(
                "tract,-timespan.begin,timespan.end",
                "tract,visit",
                ((0, 2), (0, 2), (0, 1), (0, 1), (1, 2), (1, 2)),
            ),
            Test("visit.day_obs,exposure.day_obs", "visit,exposure", ()),
            Test("visit.timespan.begin,-exposure.timespan.begin", "visit,exposure", ()),
            Test(
                "tract,detector",
                "tract,detector",
                ((0, 1), (0, 2), (0, 3), (0, 4), (1, 1), (1, 2), (1, 3), (1, 4)),
                datasets="flat",
                collections="imported_r",
            ),
            Test(
                "tract,detector.full_name",
                "tract,detector",
                ((0, 1), (0, 2), (0, 3), (0, 4), (1, 1), (1, 2), (1, 3), (1, 4)),
                datasets="flat",
                collections="imported_r",
            ),
            Test(
                "tract,detector.raft,detector.name_in_raft",
                "tract,detector",
                ((0, 1), (0, 2), (0, 3), (0, 4), (1, 1), (1, 2), (1, 3), (1, 4)),
                datasets="flat",
                collections="imported_r",
            ),
        )

        for test in test_data:
            order_by = test.order_by.split(",")
            keys = test.keys.split(",")
            query = do_query(keys, test.datasets, test.collections).order_by(*order_by)
            if test.limit is not None:
                query = query.limit(*test.limit)
            dataIds = tuple(tuple(dataId[k] for k in keys) for dataId in query)
            self.assertEqual(dataIds, test.result)

            # and materialize
            query = do_query(keys).order_by(*order_by)
            if test.limit is not None:
                query = query.limit(*test.limit)
            with self.assertRaises(RelationalAlgebraError):
                with query.materialize():
                    pass

        # errors in a name
        for order_by in ("", "-"):
            with self.assertRaisesRegex(ValueError, "Empty dimension name in ORDER BY"):
                list(do_query().order_by(order_by))

        for order_by in ("undimension.name", "-undimension.name"):
            with self.assertRaisesRegex(ValueError, "Unknown dimension element name 'undimension'"):
                list(do_query().order_by(order_by))

        for order_by in ("attract", "-attract"):
            with self.assertRaisesRegex(ValueError, "Metadata 'attract' cannot be found in any dimension"):
                list(do_query().order_by(order_by))

        with self.assertRaisesRegex(ValueError, "Metadata 'exposure_time' exists in more than one dimension"):
            list(do_query(("exposure", "visit")).order_by("exposure_time"))

        with self.assertRaisesRegex(ValueError, "Timespan exists in more than one dimesion"):
            list(do_query(("exposure", "visit")).order_by("timespan.begin"))

        with self.assertRaisesRegex(
            ValueError, "Cannot find any temporal dimension element for 'timespan.begin'"
        ):
            list(do_query("tract").order_by("timespan.begin"))

        with self.assertRaisesRegex(ValueError, "Cannot use 'timespan.begin' with non-temporal element"):
            list(do_query("tract").order_by("tract.timespan.begin"))

        with self.assertRaisesRegex(ValueError, "Field 'name' does not exist in 'tract'."):
            list(do_query("tract").order_by("tract.name"))

    def testQueryDataIdsGovernorExceptions(self):
        """Test exceptions raised by queryDataIds() for incorrect governors."""
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        self.loadData(registry, "spatial.yaml")

        def do_query(dimensions, dataId=None, where="", bind=None, **kwargs):
            return registry.queryDataIds(dimensions, dataId=dataId, where=where, bind=bind, **kwargs)

        Test = namedtuple(
            "testQueryDataIdExceptionsTest",
            ("dimensions", "dataId", "where", "bind", "kwargs", "exception", "count"),
            defaults=(None, None, None, {}, None, 0),
        )

        test_data = (
            Test("tract,visit", count=6),
            Test("tract,visit", kwargs={"instrument": "Cam1", "skymap": "SkyMap1"}, count=6),
            Test(
                "tract,visit", kwargs={"instrument": "Cam2", "skymap": "SkyMap1"}, exception=DataIdValueError
            ),
            Test("tract,visit", dataId={"instrument": "Cam1", "skymap": "SkyMap1"}, count=6),
            Test(
                "tract,visit", dataId={"instrument": "Cam1", "skymap": "SkyMap2"}, exception=DataIdValueError
            ),
            Test("tract,visit", where="instrument='Cam1' AND skymap='SkyMap1'", count=6),
            Test("tract,visit", where="instrument='Cam1' AND skymap='SkyMap5'", exception=DataIdValueError),
            Test(
                "tract,visit",
                where="instrument=cam AND skymap=map",
                bind={"cam": "Cam1", "map": "SkyMap1"},
                count=6,
            ),
            Test(
                "tract,visit",
                where="instrument=cam AND skymap=map",
                bind={"cam": "Cam", "map": "SkyMap"},
                exception=DataIdValueError,
            ),
        )

        for test in test_data:
            dimensions = test.dimensions.split(",")
            if test.exception:
                with self.assertRaises(test.exception):
                    do_query(dimensions, test.dataId, test.where, bind=test.bind, **test.kwargs).count()
            else:
                query = do_query(dimensions, test.dataId, test.where, bind=test.bind, **test.kwargs)
                self.assertEqual(query.count(discard=True), test.count)

            # and materialize
            if test.exception:
                with self.assertRaises(test.exception):
                    query = do_query(dimensions, test.dataId, test.where, bind=test.bind, **test.kwargs)
                    with query.materialize() as materialized:
                        materialized.count(discard=True)
            else:
                query = do_query(dimensions, test.dataId, test.where, bind=test.bind, **test.kwargs)
                with query.materialize() as materialized:
                    self.assertEqual(materialized.count(discard=True), test.count)

    def testQueryDimensionRecordsOrderBy(self):
        """Test order_by and limit on result returned by
        queryDimensionRecords().
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        self.loadData(registry, "spatial.yaml")

        def do_query(element, datasets=None, collections=None):
            return registry.queryDimensionRecords(
                element, instrument="Cam1", datasets=datasets, collections=collections
            )

        query = do_query("detector")
        self.assertEqual(len(list(query)), 4)

        Test = namedtuple(
            "testQueryDataIdsOrderByTest",
            ("element", "order_by", "result", "limit", "datasets", "collections"),
            defaults=(None, None, None),
        )

        test_data = (
            Test("detector", "detector", (1, 2, 3, 4)),
            Test("detector", "-detector", (4, 3, 2, 1)),
            Test("detector", "raft,-name_in_raft", (2, 1, 4, 3)),
            Test("detector", "-detector.purpose", (4,), limit=(1,)),
            Test("detector", "-purpose,detector.raft,name_in_raft", (2, 3), limit=(2, 2)),
            Test("visit", "visit", (1, 2)),
            Test("visit", "-visit.id", (2, 1)),
            Test("visit", "zenith_angle", (1, 2)),
            Test("visit", "-visit.name", (2, 1)),
            Test("visit", "day_obs,-timespan.begin", (2, 1)),
        )

        for test in test_data:
            order_by = test.order_by.split(",")
            query = do_query(test.element).order_by(*order_by)
            if test.limit is not None:
                query = query.limit(*test.limit)
            dataIds = tuple(rec.id for rec in query)
            self.assertEqual(dataIds, test.result)

        # errors in a name
        for order_by in ("", "-"):
            with self.assertRaisesRegex(ValueError, "Empty dimension name in ORDER BY"):
                list(do_query("detector").order_by(order_by))

        for order_by in ("undimension.name", "-undimension.name"):
            with self.assertRaisesRegex(ValueError, "Element name mismatch: 'undimension'"):
                list(do_query("detector").order_by(order_by))

        for order_by in ("attract", "-attract"):
            with self.assertRaisesRegex(ValueError, "Field 'attract' does not exist in 'detector'."):
                list(do_query("detector").order_by(order_by))

    def testQueryDimensionRecordsExceptions(self):
        """Test exceptions raised by queryDimensionRecords()."""
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        self.loadData(registry, "spatial.yaml")

        result = registry.queryDimensionRecords("detector")
        self.assertEqual(result.count(), 4)
        result = registry.queryDimensionRecords("detector", instrument="Cam1")
        self.assertEqual(result.count(), 4)
        result = registry.queryDimensionRecords("detector", dataId={"instrument": "Cam1"})
        self.assertEqual(result.count(), 4)
        result = registry.queryDimensionRecords("detector", where="instrument='Cam1'")
        self.assertEqual(result.count(), 4)
        result = registry.queryDimensionRecords("detector", where="instrument=instr", bind={"instr": "Cam1"})
        self.assertEqual(result.count(), 4)

        with self.assertRaisesRegex(DataIdValueError, "dimension instrument"):
            result = registry.queryDimensionRecords("detector", instrument="NotCam1")
            result.count()

        with self.assertRaisesRegex(DataIdValueError, "dimension instrument"):
            result = registry.queryDimensionRecords("detector", dataId={"instrument": "NotCam1"})
            result.count()

        with self.assertRaisesRegex(DataIdValueError, "Unknown values specified for governor dimension"):
            result = registry.queryDimensionRecords("detector", where="instrument='NotCam1'")
            result.count()

        with self.assertRaisesRegex(DataIdValueError, "Unknown values specified for governor dimension"):
            result = registry.queryDimensionRecords(
                "detector", where="instrument=instr", bind={"instr": "NotCam1"}
            )
            result.count()

    def testDatasetConstrainedDimensionRecordQueries(self):
        """Test that queryDimensionRecords works even when given a dataset
        constraint whose dimensions extend beyond the requested dimension
        element's.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        self.loadData(registry, "datasets.yaml")
        # Query for physical_filter dimension records, using a dataset that
        # has both physical_filter and dataset dimensions.
        records = registry.queryDimensionRecords(
            "physical_filter",
            datasets=["flat"],
            collections="imported_r",
        )
        self.assertEqual({record.name for record in records}, {"Cam1-R1", "Cam1-R2"})
        # Trying to constrain by all dataset types is an error.
        with self.assertRaises(TypeError):
            list(registry.queryDimensionRecords("physical_filter", datasets=..., collections="imported_r"))

    def testSkyPixDatasetQueries(self):
        """Test that we can build queries involving skypix dimensions as long
        as a dataset type that uses those dimensions is included.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "base.yaml")
        dataset_type = DatasetType(
            "a", dimensions=["htm7", "instrument"], universe=registry.dimensions, storageClass="int"
        )
        registry.registerDatasetType(dataset_type)
        run = "r"
        registry.registerRun(run)
        # First try queries where there are no datasets; the concern is whether
        # we can even build and execute these queries without raising, even
        # when "doomed" query shortcuts are in play.
        self.assertFalse(
            list(registry.queryDataIds(["htm7", "instrument"], datasets=dataset_type, collections=run))
        )
        self.assertFalse(list(registry.queryDatasets(dataset_type, collections=run)))
        # Now add a dataset and see that we can get it back.
        htm7 = registry.dimensions.skypix["htm"][7].pixelization
        data_id = registry.expandDataId(instrument="Cam1", htm7=htm7.universe()[0][0])
        (ref,) = registry.insertDatasets(dataset_type, [data_id], run=run)
        self.assertEqual(
            set(registry.queryDataIds(["htm7", "instrument"], datasets=dataset_type, collections=run)),
            {data_id},
        )
        self.assertEqual(set(registry.queryDatasets(dataset_type, collections=run)), {ref})

    def testDatasetIdFactory(self):
        """Simple test for DatasetIdFactory, mostly to catch potential changes
        in its API.
        """
        registry = self.makeRegistry()
        factory = registry.datasetIdFactory
        dataset_type = DatasetType(
            "datasetType",
            dimensions=["detector", "instrument"],
            universe=registry.dimensions,
            storageClass="int",
        )
        run = "run"
        data_id = DataCoordinate.standardize(instrument="Cam1", detector=1, graph=dataset_type.dimensions)

        datasetId = factory.makeDatasetId(run, dataset_type, data_id, DatasetIdGenEnum.UNIQUE)
        self.assertIsInstance(datasetId, uuid.UUID)
        self.assertEqual(datasetId.version, 4)

        datasetId = factory.makeDatasetId(run, dataset_type, data_id, DatasetIdGenEnum.DATAID_TYPE)
        self.assertIsInstance(datasetId, uuid.UUID)
        self.assertEqual(datasetId.version, 5)

        datasetId = factory.makeDatasetId(run, dataset_type, data_id, DatasetIdGenEnum.DATAID_TYPE_RUN)
        self.assertIsInstance(datasetId, uuid.UUID)
        self.assertEqual(datasetId.version, 5)

    def testExposureQueries(self):
        """Test query methods using arguments sourced from the exposure log
        service.

        The most complete test dataset currently available to daf_butler tests
        is hsc-rc2-subset.yaml export (which is unfortunately distinct from the
        the lsst/rc2_subset GitHub repo), but that does not have 'exposure'
        dimension records as it was focused on providing nontrivial spatial
        overlaps between visit+detector and tract+patch.  So in this test we
        need to translate queries that originally used the exposure dimension
        to use the (very similar) visit dimension instead.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "hsc-rc2-subset.yaml")
        self.assertEqual(
            [
                record.id
                for record in registry.queryDimensionRecords("visit", instrument="HSC")
                .order_by("id")
                .limit(5)
            ],
            [318, 322, 326, 330, 332],
        )
        self.assertEqual(
            [
                data_id["visit"]
                for data_id in registry.queryDataIds(["visit"], instrument="HSC").order_by("id").limit(5)
            ],
            [318, 322, 326, 330, 332],
        )
        self.assertEqual(
            [
                record.id
                for record in registry.queryDimensionRecords("detector", instrument="HSC")
                .order_by("full_name")
                .limit(5)
            ],
            [73, 72, 71, 70, 65],
        )
        self.assertEqual(
            [
                data_id["detector"]
                for data_id in registry.queryDataIds(["detector"], instrument="HSC")
                .order_by("full_name")
                .limit(5)
            ],
            [73, 72, 71, 70, 65],
        )

    def test_long_query_names(self) -> None:
        """Test that queries involving very long names are handled correctly.

        This is especially important for PostgreSQL, which truncates symbols
        longer than 64 chars, but it's worth testing for all DBs.
        """
        registry = self.makeRegistry()
        name = "abcd" * 17
        registry.registerDatasetType(
            DatasetType(
                name,
                dimensions=(),
                storageClass="Exposure",
                universe=registry.dimensions,
            )
        )
        # Need to search more than one collection actually containing a
        # matching dataset to avoid optimizations that sidestep bugs due to
        # truncation by making findFirst=True a no-op.
        run1 = "run1"
        registry.registerRun(run1)
        run2 = "run2"
        registry.registerRun(run2)
        (ref1,) = registry.insertDatasets(name, [DataCoordinate.makeEmpty(registry.dimensions)], run1)
        registry.insertDatasets(name, [DataCoordinate.makeEmpty(registry.dimensions)], run2)
        self.assertEqual(
            set(registry.queryDatasets(name, collections=[run1, run2], findFirst=True)),
            {ref1},
        )

    def test_skypix_constraint_queries(self) -> None:
        """Test queries spatially constrained by a skypix data ID."""
        registry = self.makeRegistry()
        self.loadData(registry, "hsc-rc2-subset.yaml")
        patch_regions = {
            (data_id["tract"], data_id["patch"]): data_id.region
            for data_id in registry.queryDataIds(["patch"]).expanded()
        }
        skypix_dimension: SkyPixDimension = registry.dimensions["htm11"]
        # This check ensures the test doesn't become trivial due to a config
        # change; if it does, just pick a different HTML level.
        self.assertNotEqual(skypix_dimension, registry.dimensions.commonSkyPix)
        # Gather all skypix IDs that definitely overlap at least one of these
        # patches.
        relevant_skypix_ids = lsst.sphgeom.RangeSet()
        for patch_key, patch_region in patch_regions.items():
            relevant_skypix_ids |= skypix_dimension.pixelization.interior(patch_region)
        # Look for a "nontrivial" skypix_id that overlaps at least one patch
        # and does not overlap at least one other patch.
        for skypix_id in itertools.chain.from_iterable(
            range(begin, end) for begin, end in relevant_skypix_ids
        ):
            skypix_region = skypix_dimension.pixelization.pixel(skypix_id)
            overlapping_patches = {
                patch_key
                for patch_key, patch_region in patch_regions.items()
                if not patch_region.isDisjointFrom(skypix_region)
            }
            if overlapping_patches and overlapping_patches != patch_regions.keys():
                break
        else:
            raise RuntimeError("Could not find usable skypix ID for this dimension configuration.")
        self.assertEqual(
            {
                (data_id["tract"], data_id["patch"])
                for data_id in registry.queryDataIds(
                    ["patch"],
                    dataId={skypix_dimension.name: skypix_id},
                )
            },
            overlapping_patches,
        )

    def test_spatial_constraint_queries(self) -> None:
        """Test queries in which one spatial dimension in the constraint (data
        ID or ``where`` string) constrains a different spatial dimension in the
        query result columns.
        """
        registry = self.makeRegistry()
        self.loadData(registry, "hsc-rc2-subset.yaml")
        patch_regions = {
            (data_id["tract"], data_id["patch"]): data_id.region
            for data_id in registry.queryDataIds(["patch"]).expanded()
        }
        observation_regions = {
            (data_id["visit"], data_id["detector"]): data_id.region
            for data_id in registry.queryDataIds(["visit", "detector"]).expanded()
        }
        all_combos = {
            (patch_key, observation_key)
            for patch_key, observation_key in itertools.product(patch_regions, observation_regions)
        }
        overlapping_combos = {
            (patch_key, observation_key)
            for patch_key, observation_key in all_combos
            if not patch_regions[patch_key].isDisjointFrom(observation_regions[observation_key])
        }
        # Check a direct spatial join with no constraint first.
        self.assertEqual(
            {
                ((data_id["tract"], data_id["patch"]), (data_id["visit"], data_id["detector"]))
                for data_id in registry.queryDataIds(["patch", "visit", "detector"])
            },
            overlapping_combos,
        )
        overlaps_by_patch: defaultdict[tuple[int, int], set[tuple[str, str]]] = defaultdict(set)
        overlaps_by_observation: defaultdict[tuple[int, int], set[tuple[str, str]]] = defaultdict(set)
        for patch_key, observation_key in overlapping_combos:
            overlaps_by_patch[patch_key].add(observation_key)
            overlaps_by_observation[observation_key].add(patch_key)
        # Find patches and observations that overlap at least one of the other
        # but not all of the other.
        nontrivial_patch = next(
            iter(
                patch_key
                for patch_key, observation_keys in overlaps_by_patch.items()
                if observation_keys and observation_keys != observation_regions.keys()
            )
        )
        nontrivial_observation = next(
            iter(
                observation_key
                for observation_key, patch_keys in overlaps_by_observation.items()
                if patch_keys and patch_keys != patch_regions.keys()
            )
        )
        # Use the nontrivial patches and observations as constraints on the
        # other dimensions in various ways, first via a 'where' expression.
        # It's better in general to us 'bind' instead of f-strings, but these
        # all integers so there are no quoting concerns.
        self.assertEqual(
            {
                (data_id["visit"], data_id["detector"])
                for data_id in registry.queryDataIds(
                    ["visit", "detector"],
                    where=f"tract={nontrivial_patch[0]} AND patch={nontrivial_patch[1]}",
                    skymap="hsc_rings_v1",
                )
            },
            overlaps_by_patch[nontrivial_patch],
        )
        self.assertEqual(
            {
                (data_id["tract"], data_id["patch"])
                for data_id in registry.queryDataIds(
                    ["patch"],
                    where=f"visit={nontrivial_observation[0]} AND detector={nontrivial_observation[1]}",
                    instrument="HSC",
                )
            },
            overlaps_by_observation[nontrivial_observation],
        )
        # and then via the dataId argument.
        self.assertEqual(
            {
                (data_id["visit"], data_id["detector"])
                for data_id in registry.queryDataIds(
                    ["visit", "detector"],
                    dataId={
                        "tract": nontrivial_patch[0],
                        "patch": nontrivial_patch[1],
                    },
                    skymap="hsc_rings_v1",
                )
            },
            overlaps_by_patch[nontrivial_patch],
        )
        self.assertEqual(
            {
                (data_id["tract"], data_id["patch"])
                for data_id in registry.queryDataIds(
                    ["patch"],
                    dataId={
                        "visit": nontrivial_observation[0],
                        "detector": nontrivial_observation[1],
                    },
                    instrument="HSC",
                )
            },
            overlaps_by_observation[nontrivial_observation],
        )
