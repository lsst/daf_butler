# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
from __future__ import annotations

import json
import logging
import os
import pathlib
import pickle
import posixpath
import random
import re
import shutil
import string
import tempfile
import unittest
import uuid
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

try:
    import boto3
    import botocore
    from lsst.resources.s3utils import clean_test_environment_for_s3

    try:
        from moto import mock_aws  # v5
    except ImportError:
        from moto import mock_s3 as mock_aws
except ImportError:
    boto3 = None

    def mock_aws(*args: Any, **kwargs: Any) -> Any:  # type: ignore[no-untyped-def]
        """No-op decorator in case moto mock_aws can not be imported."""
        return None


try:
    from lsst.daf.butler.tests.server import create_test_server
except ImportError:
    create_test_server = None

import astropy.time
from lsst.daf.butler import (
    Butler,
    ButlerConfig,
    ButlerRepoIndex,
    CollectionCycleError,
    CollectionType,
    Config,
    DataCoordinate,
    DatasetExistence,
    DatasetNotFoundError,
    DatasetRef,
    DatasetType,
    FileDataset,
    NoDefaultCollectionError,
    StorageClassFactory,
    ValidationError,
    script,
)
from lsst.daf.butler.datastore import NullDatastore
from lsst.daf.butler.datastore.file_templates import FileTemplate, FileTemplateValidationError
from lsst.daf.butler.datastores.fileDatastore import FileDatastore
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.registry import (
    CollectionError,
    CollectionTypeError,
    ConflictingDefinitionError,
    DataIdValueError,
    DatasetTypeExpressionError,
    MissingCollectionError,
    OrphanedRecordError,
)
from lsst.daf.butler.registry.sql_registry import SqlRegistry
from lsst.daf.butler.repo_relocation import BUTLER_ROOT_TAG
from lsst.daf.butler.tests import MetricsExample, MultiDetectorFormatter
from lsst.daf.butler.tests.postgresql import TemporaryPostgresInstance, setup_postgres_test_db
from lsst.daf.butler.tests.utils import TestCaseMixin, makeTestTempDir, removeTestTempDir, safeTestTempDir
from lsst.resources import ResourcePath
from lsst.utils import doImportType
from lsst.utils.introspection import get_full_type_name

if TYPE_CHECKING:
    import types

    from lsst.daf.butler import DimensionGroup, Registry, StorageClass

TESTDIR = os.path.abspath(os.path.dirname(__file__))


def clean_environment() -> None:
    """Remove external environment variables that affect the tests."""
    for k in ("DAF_BUTLER_REPOSITORY_INDEX",):
        os.environ.pop(k, None)


def makeExampleMetrics() -> MetricsExample:
    """Return example dataset suitable for tests."""
    return MetricsExample(
        {"AM1": 5.2, "AM2": 30.6},
        {"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
        [563, 234, 456.7, 752, 8, 9, 27],
    )


class TransactionTestError(Exception):
    """Specific error for testing transactions, to prevent misdiagnosing
    that might otherwise occur when a standard exception is used.
    """

    pass


class ButlerConfigTests(unittest.TestCase):
    """Simple tests for ButlerConfig that are not tested in any other test
    cases.
    """

    def testSearchPath(self) -> None:
        configFile = os.path.join(TESTDIR, "config", "basic", "butler.yaml")
        with self.assertLogs("lsst.daf.butler", level="DEBUG") as cm:
            config1 = ButlerConfig(configFile)
        self.assertNotIn("testConfigs", "\n".join(cm.output))

        overrideDirectory = os.path.join(TESTDIR, "config", "testConfigs")
        with self.assertLogs("lsst.daf.butler", level="DEBUG") as cm:
            config2 = ButlerConfig(configFile, searchPaths=[overrideDirectory])
        self.assertIn("testConfigs", "\n".join(cm.output))

        key = ("datastore", "records", "table")
        self.assertNotEqual(config1[key], config2[key])
        self.assertEqual(config2[key], "override_record")


class ButlerPutGetTests(TestCaseMixin):
    """Helper method for running a suite of put/get tests from different
    butler configurations.
    """

    root: str
    default_run = "ingésτ😺"
    storageClassFactory: StorageClassFactory
    configFile: str | None
    tmpConfigFile: str

    @staticmethod
    def addDatasetType(
        datasetTypeName: str, dimensions: DimensionGroup, storageClass: StorageClass | str, registry: Registry
    ) -> DatasetType:
        """Create a DatasetType and register it"""
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        registry.registerDatasetType(datasetType)
        return datasetType

    @classmethod
    def setUpClass(cls) -> None:
        cls.storageClassFactory = StorageClassFactory()
        if cls.configFile is not None:
            cls.storageClassFactory.addFromConfig(cls.configFile)

    def assertGetComponents(
        self,
        butler: Butler,
        datasetRef: DatasetRef,
        components: tuple[str, ...],
        reference: Any,
        collections: Any = None,
    ) -> None:
        datasetType = datasetRef.datasetType
        dataId = datasetRef.dataId
        deferred = butler.getDeferred(datasetRef)

        for component in components:
            compTypeName = datasetType.componentTypeName(component)
            result = butler.get(compTypeName, dataId, collections=collections)
            self.assertEqual(result, getattr(reference, component))
            result_deferred = deferred.get(component=component)
            self.assertEqual(result_deferred, result)

    def tearDown(self) -> None:
        if self.root is not None:
            removeTestTempDir(self.root)

    def create_empty_butler(self, run: str | None = None, writeable: bool | None = None):
        """Create a Butler for the test repository, without inserting test
        data.
        """
        butler = Butler.from_config(self.tmpConfigFile, run=run, writeable=writeable)
        assert isinstance(butler, DirectButler), "Expect DirectButler in configuration"
        return butler

    def create_butler(
        self, run: str, storageClass: StorageClass | str, datasetTypeName: str
    ) -> tuple[Butler, DatasetType]:
        """Create a Butler for the test repository and insert some test data
        into it.
        """
        butler = self.create_empty_butler(run=run)

        collections = set(butler.collections.query("*"))
        self.assertEqual(collections, {run})
        # Create and register a DatasetType
        dimensions = butler.dimensions.conform(["instrument", "visit"])

        datasetType = self.addDatasetType(datasetTypeName, dimensions, storageClass, butler.registry)

        # Add needed Dimensions
        butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        butler.registry.insertDimensionData(
            "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
        )
        butler.registry.insertDimensionData(
            "visit_system", {"instrument": "DummyCamComp", "id": 1, "name": "default"}
        )
        butler.registry.insertDimensionData("day_obs", {"instrument": "DummyCamComp", "id": 20200101})
        visit_start = astropy.time.Time("2020-01-01 08:00:00.123456789", scale="tai")
        visit_end = astropy.time.Time("2020-01-01 08:00:36.66", scale="tai")
        butler.registry.insertDimensionData(
            "visit",
            {
                "instrument": "DummyCamComp",
                "id": 423,
                "name": "fourtwentythree",
                "physical_filter": "d-r",
                "datetime_begin": visit_start,
                "datetime_end": visit_end,
                "day_obs": 20200101,
            },
        )

        # Add more visits for some later tests
        for visit_id in (424, 425):
            butler.registry.insertDimensionData(
                "visit",
                {
                    "instrument": "DummyCamComp",
                    "id": visit_id,
                    "name": f"fourtwentyfour_{visit_id}",
                    "physical_filter": "d-r",
                    "day_obs": 20200101,
                },
            )
        return butler, datasetType

    def runPutGetTest(self, storageClass: StorageClass, datasetTypeName: str) -> Butler:
        # New datasets will be added to run and tag, but we will only look in
        # tag when looking up datasets.
        run = self.default_run
        butler, datasetType = self.create_butler(run, storageClass, datasetTypeName)
        assert butler.run is not None

        # Create and store a dataset
        metric = makeExampleMetrics()
        dataId = butler.registry.expandDataId({"instrument": "DummyCamComp", "visit": 423})

        # Dataset should not exist if we haven't added it
        with self.assertRaises(DatasetNotFoundError):
            butler.get(datasetTypeName, dataId)

        # Put and remove the dataset once as a DatasetRef, once as a dataId,
        # and once with a DatasetType

        # Keep track of any collections we add and do not clean up
        expected_collections = {run}

        counter = 0
        ref = DatasetRef(datasetType, dataId, id=uuid.UUID(int=1), run="put_run_1")
        args = tuple[DatasetRef] | tuple[str | DatasetType, DataCoordinate]
        for args in ((ref,), (datasetTypeName, dataId), (datasetType, dataId)):
            # Since we are using subTest we can get cascading failures
            # here with the first attempt failing and the others failing
            # immediately because the dataset already exists. Work around
            # this by using a distinct run collection each time
            counter += 1
            this_run = f"put_run_{counter}"
            butler.collections.register(this_run)
            expected_collections.update({this_run})

            with self.subTest(args=args):
                kwargs: dict[str, Any] = {}
                if not isinstance(args[0], DatasetRef):  # type: ignore
                    kwargs["run"] = this_run
                ref = butler.put(metric, *args, **kwargs)
                self.assertIsInstance(ref, DatasetRef)

                # Test get of a ref.
                metricOut = butler.get(ref)
                self.assertEqual(metric, metricOut)
                # Test get
                metricOut = butler.get(ref.datasetType.name, dataId, collections=this_run)
                self.assertEqual(metric, metricOut)
                # Test get with a datasetRef
                metricOut = butler.get(ref)
                self.assertEqual(metric, metricOut)
                # Test getDeferred with dataId
                metricOut = butler.getDeferred(ref.datasetType.name, dataId, collections=this_run).get()
                self.assertEqual(metric, metricOut)
                # Test getDeferred with a ref
                metricOut = butler.getDeferred(ref).get()
                self.assertEqual(metric, metricOut)

                # Check we can get components
                if storageClass.isComposite():
                    self.assertGetComponents(
                        butler, ref, ("summary", "data", "output"), metric, collections=this_run
                    )

                primary_uri, secondary_uris = butler.getURIs(ref)
                n_uris = len(secondary_uris)
                if primary_uri:
                    n_uris += 1

                # Can the artifacts themselves be retrieved?
                if not butler._datastore.isEphemeral:
                    # Create a temporary directory to hold the retrieved
                    # artifacts.
                    with tempfile.TemporaryDirectory(
                        prefix="butler-artifacts-", ignore_cleanup_errors=True
                    ) as artifact_root:
                        root_uri = ResourcePath(artifact_root, forceDirectory=True)

                        for preserve_path in (True, False):
                            destination = root_uri.join(f"{preserve_path}_{counter}/")
                            log = logging.getLogger("lsst.x")
                            log.debug("Using destination %s for args %s", destination, args)
                            # Use copy so that we can test that overwrite
                            # protection works (using "auto" for File URIs
                            # would use hard links and subsequent transfer
                            # would work because it knows they are the same
                            # file).
                            transferred = butler.retrieveArtifacts(
                                [ref], destination, preserve_path=preserve_path, transfer="copy"
                            )
                            self.assertGreater(len(transferred), 0)
                            artifacts = list(ResourcePath.findFileResources([destination]))
                            self.assertEqual(set(transferred), set(artifacts))

                            for artifact in transferred:
                                path_in_destination = artifact.relative_to(destination)
                                self.assertIsNotNone(path_in_destination)
                                assert path_in_destination is not None

                                # When path is not preserved there should not
                                # be any path separators.
                                num_seps = path_in_destination.count("/")
                                if preserve_path:
                                    self.assertGreater(num_seps, 0)
                                else:
                                    self.assertEqual(num_seps, 0)

                            self.assertEqual(
                                len(artifacts),
                                n_uris,
                                "Comparing expected artifacts vs actual:"
                                f" {artifacts} vs {primary_uri} and {secondary_uris}",
                            )

                            if preserve_path:
                                # No need to run these twice
                                with self.assertRaises(ValueError):
                                    butler.retrieveArtifacts([ref], destination, transfer="move")

                                with self.assertRaisesRegex(
                                    ValueError, "^Destination location must refer to a directory"
                                ):
                                    butler.retrieveArtifacts(
                                        [ref], ResourcePath("/some/file.txt", forceDirectory=False)
                                    )

                                with self.assertRaises(FileExistsError):
                                    butler.retrieveArtifacts([ref], destination)

                                transferred_again = butler.retrieveArtifacts(
                                    [ref], destination, preserve_path=preserve_path, overwrite=True
                                )
                                self.assertEqual(set(transferred_again), set(transferred))

                # Now remove the dataset completely.
                butler.pruneDatasets([ref], purge=True, unstore=True)
                # Lookup with original args should still fail.
                kwargs = {"collections": this_run}
                if isinstance(args[0], DatasetRef):
                    kwargs = {}  # Prevent warning from being issued.
                self.assertFalse(butler.exists(*args, **kwargs))
                # get() should still fail.
                with self.assertRaises((FileNotFoundError, DatasetNotFoundError)):
                    butler.get(ref)
                # Registry shouldn't be able to find it by dataset_id anymore.
                self.assertIsNone(butler.get_dataset(ref.id))

                # Do explicit registry removal since we know they are
                # empty
                butler.collections.x_remove(this_run)
                expected_collections.remove(this_run)

        # Create DatasetRef for put using default run.
        refIn = DatasetRef(datasetType, dataId, id=uuid.UUID(int=1), run=butler.run)

        # Check that getDeferred fails with standalone ref.
        with self.assertRaises(LookupError):
            butler.getDeferred(refIn)

        # Put the dataset again, since the last thing we did was remove it
        # and we want to use the default collection.
        ref = butler.put(metric, refIn)

        # Get with parameters
        stop = 4
        sliced = butler.get(ref, parameters={"slice": slice(stop)})
        self.assertNotEqual(metric, sliced)
        self.assertEqual(metric.summary, sliced.summary)
        self.assertEqual(metric.output, sliced.output)
        assert metric.data is not None  # for mypy
        self.assertEqual(metric.data[:stop], sliced.data)
        # getDeferred with parameters
        sliced = butler.getDeferred(ref, parameters={"slice": slice(stop)}).get()
        self.assertNotEqual(metric, sliced)
        self.assertEqual(metric.summary, sliced.summary)
        self.assertEqual(metric.output, sliced.output)
        self.assertEqual(metric.data[:stop], sliced.data)
        # getDeferred with deferred parameters
        sliced = butler.getDeferred(ref).get(parameters={"slice": slice(stop)})
        self.assertNotEqual(metric, sliced)
        self.assertEqual(metric.summary, sliced.summary)
        self.assertEqual(metric.output, sliced.output)
        self.assertEqual(metric.data[:stop], sliced.data)

        if storageClass.isComposite():
            # Check that components can be retrieved
            metricOut = butler.get(ref.datasetType.name, dataId)
            compNameS = ref.datasetType.componentTypeName("summary")
            compNameD = ref.datasetType.componentTypeName("data")
            summary = butler.get(compNameS, dataId)
            self.assertEqual(summary, metric.summary)
            data = butler.get(compNameD, dataId)
            self.assertEqual(data, metric.data)

            if "counter" in storageClass.derivedComponents:
                count = butler.get(ref.datasetType.componentTypeName("counter"), dataId)
                self.assertEqual(count, len(data))

                count = butler.get(
                    ref.datasetType.componentTypeName("counter"), dataId, parameters={"slice": slice(stop)}
                )
                self.assertEqual(count, stop)

            compRef = butler.find_dataset(compNameS, dataId, collections=butler.collections.defaults)
            assert compRef is not None
            summary = butler.get(compRef)
            self.assertEqual(summary, metric.summary)

        # Create a Dataset type that has the same name but is inconsistent.
        inconsistentDatasetType = DatasetType(
            datasetTypeName, datasetType.dimensions, self.storageClassFactory.getStorageClass("Config")
        )

        # Getting with a dataset type that does not match registry fails
        with self.assertRaisesRegex(
            ValueError,
            "(Supplied dataset type .* inconsistent with registry)"
            "|(The new storage class .* is not compatible with the existing storage class)",
        ):
            butler.get(inconsistentDatasetType, dataId)

        # Combining a DatasetRef with a dataId should fail
        with self.assertRaisesRegex(ValueError, "DatasetRef given, cannot use dataId as well"):
            butler.get(ref, dataId)
        # Getting with an explicit ref should fail if the id doesn't match.
        with self.assertRaises((FileNotFoundError, DatasetNotFoundError)):
            butler.get(DatasetRef(ref.datasetType, ref.dataId, id=uuid.UUID(int=101), run=butler.run))

        # Getting a dataset with unknown parameters should fail
        with self.assertRaisesRegex(KeyError, "Parameter 'unsupported' not understood"):
            butler.get(ref, parameters={"unsupported": True})

        # Check we have a collection
        collections = set(butler.collections.query("*"))
        self.assertEqual(collections, expected_collections)

        # Clean up to check that we can remove something that may have
        # already had a component removed
        butler.pruneDatasets([ref], unstore=True, purge=True)

        # Add the same ref again, so we can check that duplicate put fails.
        ref = butler.put(metric, datasetType, dataId)

        # Repeat put will fail.
        with self.assertRaisesRegex(
            ConflictingDefinitionError, "A database constraint failure was triggered"
        ):
            butler.put(metric, datasetType, dataId)

        # Remove the datastore entry.
        butler.pruneDatasets([ref], unstore=True, purge=False, disassociate=False)

        # Put will still fail
        with self.assertRaisesRegex(
            ConflictingDefinitionError, "A database constraint failure was triggered"
        ):
            butler.put(metric, datasetType, dataId)

        # Repeat the same sequence with resolved ref.
        butler.pruneDatasets([ref], unstore=True, purge=True)
        ref = butler.put(metric, refIn)

        # Repeat put will fail.
        with self.assertRaisesRegex(ConflictingDefinitionError, "Datastore already contains dataset"):
            butler.put(metric, refIn)

        # Remove the datastore entry.
        butler.pruneDatasets([ref], unstore=True, purge=False, disassociate=False)

        # In case of resolved ref this write will succeed.
        ref = butler.put(metric, refIn)

        # Leave the dataset in place since some downstream tests require
        # something to be present

        return butler

    def testDeferredCollectionPassing(self) -> None:
        # Construct a butler with no run or collection, but make it writeable.
        butler = self.create_empty_butler(writeable=True)
        # Create and register a DatasetType
        dimensions = butler.dimensions.conform(["instrument", "visit"])
        datasetType = self.addDatasetType(
            "example", dimensions, self.storageClassFactory.getStorageClass("StructuredData"), butler.registry
        )
        # Add needed Dimensions
        butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        butler.registry.insertDimensionData(
            "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
        )
        butler.registry.insertDimensionData("day_obs", {"instrument": "DummyCamComp", "id": 20250101})
        butler.registry.insertDimensionData(
            "visit",
            {
                "instrument": "DummyCamComp",
                "id": 423,
                "name": "fourtwentythree",
                "physical_filter": "d-r",
                "day_obs": 20250101,
            },
        )
        dataId = {"instrument": "DummyCamComp", "visit": 423}
        # Create dataset.
        metric = makeExampleMetrics()
        # Register a new run and put dataset.
        run = "deferred"
        self.assertTrue(butler.collections.register(run))
        # Second time it will be allowed but indicate no-op
        self.assertFalse(butler.collections.register(run))
        ref = butler.put(metric, datasetType, dataId, run=run)
        # Putting with no run should fail with TypeError.
        with self.assertRaises(CollectionError):
            butler.put(metric, datasetType, dataId)
        # Dataset should exist.
        self.assertTrue(butler.exists(datasetType, dataId, collections=[run]))
        # We should be able to get the dataset back, but with and without
        # a deferred dataset handle.
        self.assertEqual(metric, butler.get(datasetType, dataId, collections=[run]))
        self.assertEqual(metric, butler.getDeferred(datasetType, dataId, collections=[run]).get())
        # Trying to find the dataset without any collection is an error.
        with self.assertRaises(NoDefaultCollectionError):
            butler.exists(datasetType, dataId)
        with self.assertRaises(CollectionError):
            butler.get(datasetType, dataId)
        # Associate the dataset with a different collection.
        butler.collections.register("tagged", type=CollectionType.TAGGED)
        butler.registry.associate("tagged", [ref])
        # Deleting the dataset from the new collection should make it findable
        # in the original collection.
        butler.pruneDatasets([ref], tags=["tagged"])
        self.assertTrue(butler.exists(datasetType, dataId, collections=[run]))


class ButlerTests(ButlerPutGetTests):
    """Tests for Butler."""

    useTempRoot = True
    validationCanFail: bool
    fullConfigKey: str | None
    registryStr: str | None
    datastoreName: list[str] | None
    datastoreStr: list[str]
    predictionSupported = True
    """Does getURIs support 'prediction mode'?"""

    def setUp(self) -> None:
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        Butler.makeRepo(self.root, config=Config(self.configFile))
        self.tmpConfigFile = os.path.join(self.root, "butler.yaml")

    def are_uris_equivalent(self, uri1: ResourcePath, uri2: ResourcePath) -> bool:
        """Return True if two URIs refer to the same resource.

        Subclasses may override to handle unique requirements.
        """
        return uri1 == uri2

    def testConstructor(self) -> None:
        """Independent test of constructor."""
        butler = Butler.from_config(self.tmpConfigFile, run=self.default_run)
        self.assertIsInstance(butler, Butler)

        # Check that butler.yaml is added automatically.
        if self.tmpConfigFile.endswith(end := "/butler.yaml"):
            config_dir = self.tmpConfigFile[: -len(end)]
            butler = Butler.from_config(config_dir, run=self.default_run)
            self.assertIsInstance(butler, Butler)

            # Even with a ResourcePath.
            butler = Butler.from_config(ResourcePath(config_dir, forceDirectory=True), run=self.default_run)
            self.assertIsInstance(butler, Butler)

        collections = set(butler.collections.query("*"))
        self.assertEqual(collections, {self.default_run})

        # Check that some special characters can be included in run name.
        special_run = "u@b.c-A"
        butler_special = Butler.from_config(butler=butler, run=special_run)
        collections = set(butler_special.registry.queryCollections("*@*"))
        self.assertEqual(collections, {special_run})

        butler2 = Butler.from_config(butler=butler, collections=["other"])
        self.assertEqual(butler2.collections.defaults, ("other",))
        self.assertIsNone(butler2.run)
        self.assertEqual(type(butler._datastore), type(butler2._datastore))
        self.assertEqual(butler._datastore.config, butler2._datastore.config)

        # Test that we can use an environment variable to find this
        # repository.
        butler_index = Config()
        butler_index["label"] = self.tmpConfigFile
        for suffix in (".yaml", ".json"):
            # Ensure that the content differs so that we know that
            # we aren't reusing the cache.
            bad_label = f"file://bucket/not_real{suffix}"
            butler_index["bad_label"] = bad_label
            with ResourcePath.temporary_uri(suffix=suffix) as temp_file:
                butler_index.dumpToUri(temp_file)
                with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": str(temp_file)}):
                    self.assertEqual(Butler.get_known_repos(), {"label", "bad_label"})
                    uri = Butler.get_repo_uri("bad_label")
                    self.assertEqual(uri, ResourcePath(bad_label))
                    uri = Butler.get_repo_uri("label")
                    butler = Butler.from_config(uri, writeable=False)
                    self.assertIsInstance(butler, Butler)
                    butler = Butler.from_config("label", writeable=False)
                    self.assertIsInstance(butler, Butler)
                    with self.assertRaisesRegex(FileNotFoundError, "aliases:.*bad_label"):
                        Butler.from_config("not_there", writeable=False)
                    with self.assertRaisesRegex(FileNotFoundError, "resolved from alias 'bad_label'"):
                        Butler.from_config("bad_label")
                    with self.assertRaises(FileNotFoundError):
                        # Should ignore aliases.
                        Butler.from_config(ResourcePath("label", forceAbsolute=False))
                    with self.assertRaises(KeyError) as cm:
                        Butler.get_repo_uri("missing")
                    self.assertEqual(
                        Butler.get_repo_uri("missing", True), ResourcePath("missing", forceAbsolute=False)
                    )
                    self.assertIn("not known to", str(cm.exception))
                    # Should report no failure.
                    self.assertEqual(ButlerRepoIndex.get_failure_reason(), "")
        with ResourcePath.temporary_uri(suffix=suffix) as temp_file:
            # Now with empty configuration.
            butler_index = Config()
            butler_index.dumpToUri(temp_file)
            with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": str(temp_file)}):
                with self.assertRaisesRegex(FileNotFoundError, "(no known aliases)"):
                    Butler.from_config("label")
        with ResourcePath.temporary_uri(suffix=suffix) as temp_file:
            # Now with bad contents.
            with open(temp_file.ospath, "w") as fh:
                print("'", file=fh)
            with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": str(temp_file)}):
                with self.assertRaisesRegex(FileNotFoundError, "(no known aliases:.*could not be read)"):
                    Butler.from_config("label")
        with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": "file://not_found/x.yaml"}):
            with self.assertRaises(FileNotFoundError):
                Butler.get_repo_uri("label")
            self.assertEqual(Butler.get_known_repos(), set())

            with self.assertRaisesRegex(FileNotFoundError, "index file not found"):
                Butler.from_config("label")

            # Check that we can create Butler when the alias file is not found.
            butler = Butler.from_config(self.tmpConfigFile, writeable=False)
            self.assertIsInstance(butler, Butler)
        with self.assertRaises(RuntimeError) as cm:
            # No environment variable set.
            Butler.get_repo_uri("label")
        self.assertEqual(Butler.get_repo_uri("label", True), ResourcePath("label", forceAbsolute=False))
        self.assertIn("No repository index defined", str(cm.exception))
        with self.assertRaisesRegex(FileNotFoundError, "no known aliases.*No repository index"):
            # No aliases registered.
            Butler.from_config("not_there")
        self.assertEqual(Butler.get_known_repos(), set())

    def testDafButlerRepositories(self):
        with unittest.mock.patch.dict(
            os.environ,
            {"DAF_BUTLER_REPOSITORIES": "label: 'https://someuri.com'\notherLabel: 'https://otheruri.com'\n"},
        ):
            self.assertEqual(str(Butler.get_repo_uri("label")), "https://someuri.com")

        with unittest.mock.patch.dict(
            os.environ,
            {
                "DAF_BUTLER_REPOSITORIES": "label: https://someuri.com",
                "DAF_BUTLER_REPOSITORY_INDEX": "https://someuri.com",
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "Only one of the environment variables"):
                Butler.get_repo_uri("label")

        with unittest.mock.patch.dict(
            os.environ,
            {"DAF_BUTLER_REPOSITORIES": "invalid"},
        ):
            with self.assertRaisesRegex(ValueError, "Repository index not in expected format"):
                Butler.get_repo_uri("label")

    def testBasicPutGet(self) -> None:
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        self.runPutGetTest(storageClass, "test_metric")

    def testCompositePutGetConcrete(self) -> None:
        storageClass = self.storageClassFactory.getStorageClass("StructuredCompositeReadCompNoDisassembly")
        butler = self.runPutGetTest(storageClass, "test_metric")

        # Should *not* be disassembled
        datasets = list(butler.registry.queryDatasets(..., collections=self.default_run))
        self.assertEqual(len(datasets), 1)
        uri, components = butler.getURIs(datasets[0])
        self.assertIsInstance(uri, ResourcePath)
        self.assertFalse(components)
        self.assertEqual(uri.fragment, "", f"Checking absence of fragment in {uri}")
        self.assertIn("423", str(uri), f"Checking visit is in URI {uri}")

        # Predicted dataset
        if self.predictionSupported:
            dataId = {"instrument": "DummyCamComp", "visit": 424}
            uri, components = butler.getURIs(datasets[0].datasetType, dataId=dataId, predict=True)
            self.assertFalse(components)
            self.assertIsInstance(uri, ResourcePath)
            self.assertIn("424", str(uri), f"Checking visit is in URI {uri}")
            self.assertEqual(uri.fragment, "predicted", f"Checking for fragment in {uri}")

    def testCompositePutGetVirtual(self) -> None:
        storageClass = self.storageClassFactory.getStorageClass("StructuredCompositeReadComp")
        butler = self.runPutGetTest(storageClass, "test_metric_comp")

        # Should be disassembled
        datasets = list(butler.registry.queryDatasets(..., collections=self.default_run))
        self.assertEqual(len(datasets), 1)
        uri, components = butler.getURIs(datasets[0])

        if butler._datastore.isEphemeral:
            # Never disassemble in-memory datastore
            self.assertIsInstance(uri, ResourcePath)
            self.assertFalse(components)
            self.assertEqual(uri.fragment, "", f"Checking absence of fragment in {uri}")
            self.assertIn("423", str(uri), f"Checking visit is in URI {uri}")
        else:
            self.assertIsNone(uri)
            self.assertEqual(set(components), set(storageClass.components))
            for compuri in components.values():
                self.assertIsInstance(compuri, ResourcePath)
                self.assertIn("423", str(compuri), f"Checking visit is in URI {compuri}")
                self.assertEqual(compuri.fragment, "", f"Checking absence of fragment in {compuri}")

        if self.predictionSupported:
            # Predicted dataset
            dataId = {"instrument": "DummyCamComp", "visit": 424}
            uri, components = butler.getURIs(datasets[0].datasetType, dataId=dataId, predict=True)

            if butler._datastore.isEphemeral:
                # Never disassembled
                self.assertIsInstance(uri, ResourcePath)
                self.assertFalse(components)
                self.assertIn("424", str(uri), f"Checking visit is in URI {uri}")
                self.assertEqual(uri.fragment, "predicted", f"Checking for fragment in {uri}")
            else:
                self.assertIsNone(uri)
                self.assertEqual(set(components), set(storageClass.components))
                for compuri in components.values():
                    self.assertIsInstance(compuri, ResourcePath)
                    self.assertIn("424", str(compuri), f"Checking visit is in URI {compuri}")
                    self.assertEqual(compuri.fragment, "predicted", f"Checking for fragment in {compuri}")

    def testStorageClassOverrideGet(self) -> None:
        """Test storage class conversion on get with override."""
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        datasetTypeName = "anything"
        run = self.default_run

        butler, datasetType = self.create_butler(run, storageClass, datasetTypeName)

        # Create and store a dataset.
        metric = makeExampleMetrics()
        dataId = {"instrument": "DummyCamComp", "visit": 423}

        ref = butler.put(metric, datasetType, dataId)

        # Return native type.
        retrieved = butler.get(ref)
        self.assertEqual(retrieved, metric)

        # Specify an override.
        new_sc = self.storageClassFactory.getStorageClass("MetricsConversion")
        model = butler.get(ref, storageClass=new_sc)
        self.assertNotEqual(type(model), type(retrieved))
        self.assertIs(type(model), new_sc.pytype)
        self.assertEqual(retrieved, model)

        # Defer but override later.
        deferred = butler.getDeferred(ref)
        model = deferred.get(storageClass=new_sc)
        self.assertIs(type(model), new_sc.pytype)
        self.assertEqual(retrieved, model)

        # Defer but override up front.
        deferred = butler.getDeferred(ref, storageClass=new_sc)
        model = deferred.get()
        self.assertIs(type(model), new_sc.pytype)
        self.assertEqual(retrieved, model)

        # Retrieve a component. Should be a tuple.
        data = butler.get("anything.data", dataId, storageClass="StructuredDataDataTestTuple")
        self.assertIs(type(data), tuple)
        self.assertEqual(data, tuple(retrieved.data))

        # Parameter on the write storage class should work regardless
        # of read storage class.
        data = butler.get(
            "anything.data",
            dataId,
            storageClass="StructuredDataDataTestTuple",
            parameters={"slice": slice(2, 4)},
        )
        self.assertEqual(len(data), 2)

        # Try a parameter that is known to the read storage class but not
        # the write storage class.
        with self.assertRaises(KeyError):
            butler.get(
                "anything.data",
                dataId,
                storageClass="StructuredDataDataTestTuple",
                parameters={"xslice": slice(2, 4)},
            )

    def testPytypePutCoercion(self) -> None:
        """Test python type coercion on Butler.get and put."""
        # Store some data with the normal example storage class.
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        datasetTypeName = "test_metric"
        butler, _ = self.create_butler(self.default_run, storageClass, datasetTypeName)

        dataId = {"instrument": "DummyCamComp", "visit": 423}

        # Put a dict and this should coerce to a MetricsExample
        test_dict = {"summary": {"a": 1}, "output": {"b": 2}}
        metric_ref = butler.put(test_dict, datasetTypeName, dataId=dataId, visit=424)
        test_metric = butler.get(metric_ref)
        self.assertEqual(get_full_type_name(test_metric), "lsst.daf.butler.tests.MetricsExample")
        self.assertEqual(test_metric.summary, test_dict["summary"])
        self.assertEqual(test_metric.output, test_dict["output"])

        # Check that the put still works if a DatasetType is given with
        # a definition matching this python type.
        registry_type = butler.get_dataset_type(datasetTypeName)
        this_type = DatasetType(datasetTypeName, registry_type.dimensions, "StructuredDataDictJson")
        metric2_ref = butler.put(test_dict, this_type, dataId=dataId, visit=425)
        self.assertEqual(metric2_ref.datasetType, registry_type)

        # The get will return the type expected by registry.
        test_metric2 = butler.get(metric2_ref)
        self.assertEqual(get_full_type_name(test_metric2), "lsst.daf.butler.tests.MetricsExample")

        # Make a new DatasetRef with the compatible but different DatasetType.
        # This should now return a dict.
        new_ref = DatasetRef(this_type, metric2_ref.dataId, id=metric2_ref.id, run=metric2_ref.run)
        test_dict2 = butler.get(new_ref)
        self.assertEqual(get_full_type_name(test_dict2), "dict")

        # Get it again with the wrong dataset type definition using get()
        # rather than get(). This should be consistent with get()
        # behavior and return the type of the DatasetType.
        test_dict3 = butler.get(this_type, dataId=dataId, visit=425)
        self.assertEqual(get_full_type_name(test_dict3), "dict")

    def testIngest(self) -> None:
        butler = self.create_empty_butler(run=self.default_run)

        # Create and register a DatasetType
        dimensions = butler.dimensions.conform(["instrument", "visit", "detector"])

        storageClass = self.storageClassFactory.getStorageClass("StructuredDataDictYaml")
        datasetTypeName = "metric"

        datasetType = self.addDatasetType(datasetTypeName, dimensions, storageClass, butler.registry)

        # Add needed Dimensions
        butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        butler.registry.insertDimensionData(
            "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
        )
        butler.registry.insertDimensionData("day_obs", {"instrument": "DummyCamComp", "id": 20250101})
        for detector in (1, 2):
            butler.registry.insertDimensionData(
                "detector", {"instrument": "DummyCamComp", "id": detector, "full_name": f"detector{detector}"}
            )

        butler.registry.insertDimensionData(
            "visit",
            {
                "instrument": "DummyCamComp",
                "id": 423,
                "name": "fourtwentythree",
                "physical_filter": "d-r",
                "day_obs": 20250101,
            },
            {
                "instrument": "DummyCamComp",
                "id": 424,
                "name": "fourtwentyfour",
                "physical_filter": "d-r",
                "day_obs": 20250101,
            },
        )

        formatter = doImportType("lsst.daf.butler.formatters.yaml.YamlFormatter")
        dataRoot = os.path.join(TESTDIR, "data", "basic")
        datasets = []
        for detector in (1, 2):
            detector_name = f"detector_{detector}"
            metricFile = os.path.join(dataRoot, f"{detector_name}.yaml")
            dataId = butler.registry.expandDataId(
                {"instrument": "DummyCamComp", "visit": 423, "detector": detector}
            )
            # Create a DatasetRef for ingest
            refIn = DatasetRef(datasetType, dataId, run=self.default_run)

            datasets.append(FileDataset(path=metricFile, refs=[refIn], formatter=formatter))

        butler.ingest(*datasets, transfer="copy")

        dataId1 = {"instrument": "DummyCamComp", "detector": 1, "visit": 423}
        dataId2 = {"instrument": "DummyCamComp", "detector": 2, "visit": 423}

        metrics1 = butler.get(datasetTypeName, dataId1)
        metrics2 = butler.get(datasetTypeName, dataId2)
        self.assertNotEqual(metrics1, metrics2)

        # Compare URIs
        uri1 = butler.getURI(datasetTypeName, dataId1)
        uri2 = butler.getURI(datasetTypeName, dataId2)
        self.assertFalse(self.are_uris_equivalent(uri1, uri2), f"Cf. {uri1} with {uri2}")

        # Now do a multi-dataset but single file ingest
        metricFile = os.path.join(dataRoot, "detectors.yaml")
        refs = []
        for detector in (1, 2):
            detector_name = f"detector_{detector}"
            dataId = butler.registry.expandDataId(
                {"instrument": "DummyCamComp", "visit": 424, "detector": detector}
            )
            # Create a DatasetRef for ingest
            refs.append(DatasetRef(datasetType, dataId, run=self.default_run))

        # Test "move" transfer to ensure that the files themselves
        # have disappeared following ingest.
        with ResourcePath.temporary_uri(suffix=".yaml") as tempFile:
            tempFile.transfer_from(ResourcePath(metricFile), transfer="copy")

            datasets = []
            datasets.append(FileDataset(path=tempFile, refs=refs, formatter=MultiDetectorFormatter))

            # For first ingest use copy.
            butler.ingest(*datasets, transfer="copy", record_validation_info=False)

            # Now try to ingest again in "execution butler" mode where
            # the registry entries exist but the datastore does not have
            # the files. We also need to strip the dimension records to ensure
            # that they will be re-added by the ingest.
            ref = datasets[0].refs[0]
            datasets[0].refs = [
                cast(
                    DatasetRef,
                    butler.find_dataset(ref.datasetType, data_id=ref.dataId, collections=ref.run),
                )
                for ref in datasets[0].refs
            ]
            all_refs = []
            for dataset in datasets:
                refs = []
                for ref in dataset.refs:
                    # Create a dict from the dataId to drop the records.
                    new_data_id = dict(ref.dataId.required)
                    new_ref = butler.find_dataset(ref.datasetType, new_data_id, collections=ref.run)
                    assert new_ref is not None
                    self.assertFalse(new_ref.dataId.hasRecords())
                    refs.append(new_ref)
                dataset.refs = refs
                all_refs.extend(dataset.refs)
            butler.pruneDatasets(all_refs, disassociate=False, unstore=True, purge=False)

            # Use move mode to test that the file is deleted. Also
            # disable recording of file size.
            butler.ingest(*datasets, transfer="move", record_validation_info=False)

            # Check that every ref now has records.
            for dataset in datasets:
                for ref in dataset.refs:
                    self.assertTrue(ref.dataId.hasRecords())

            # Ensure that the file has disappeared.
            self.assertFalse(tempFile.exists())

        # Check that the datastore recorded no file size.
        # Not all datastores can support this.
        try:
            infos = butler._datastore.getStoredItemsInfo(datasets[0].refs[0])  # type: ignore[attr-defined]
            self.assertEqual(infos[0].file_size, -1)
        except AttributeError:
            pass

        dataId1 = {"instrument": "DummyCamComp", "detector": 1, "visit": 424}
        dataId2 = {"instrument": "DummyCamComp", "detector": 2, "visit": 424}

        multi1 = butler.get(datasetTypeName, dataId1)
        multi2 = butler.get(datasetTypeName, dataId2)

        self.assertEqual(multi1, metrics1)
        self.assertEqual(multi2, metrics2)

        # Compare URIs
        uri1 = butler.getURI(datasetTypeName, dataId1)
        uri2 = butler.getURI(datasetTypeName, dataId2)
        self.assertTrue(self.are_uris_equivalent(uri1, uri2), f"Cf. {uri1} with {uri2}")

        # Test that removing one does not break the second
        # This line will issue a warning log message for a ChainedDatastore
        # that uses an InMemoryDatastore since in-memory can not ingest
        # files.
        butler.pruneDatasets([datasets[0].refs[0]], unstore=True, disassociate=False)
        self.assertFalse(butler.exists(datasetTypeName, dataId1))
        self.assertTrue(butler.exists(datasetTypeName, dataId2))
        multi2b = butler.get(datasetTypeName, dataId2)
        self.assertEqual(multi2, multi2b)

        # Ensure we can ingest 0 datasets
        datasets = []
        butler.ingest(*datasets)

    def testPickle(self) -> None:
        """Test pickle support."""
        butler = self.create_empty_butler(run=self.default_run)
        assert isinstance(butler, DirectButler), "Expect DirectButler in configuration"
        butlerOut = pickle.loads(pickle.dumps(butler))
        self.assertIsInstance(butlerOut, Butler)
        self.assertEqual(butlerOut._config, butler._config)
        self.assertEqual(list(butlerOut.collections.defaults), list(butler.collections.defaults))
        self.assertEqual(butlerOut.run, butler.run)

    def testGetDatasetTypes(self) -> None:
        butler = self.create_empty_butler(run=self.default_run)
        dimensions = butler.dimensions.conform(["instrument", "visit", "physical_filter"])
        dimensionEntries: list[tuple[str, list[Mapping[str, Any]]]] = [
            (
                "instrument",
                [
                    {"instrument": "DummyCam"},
                    {"instrument": "DummyHSC"},
                    {"instrument": "DummyCamComp"},
                ],
            ),
            ("physical_filter", [{"instrument": "DummyCam", "name": "d-r", "band": "R"}]),
            ("day_obs", [{"instrument": "DummyCam", "id": 20250101}]),
            (
                "visit",
                [
                    {
                        "instrument": "DummyCam",
                        "id": 42,
                        "name": "fortytwo",
                        "physical_filter": "d-r",
                        "day_obs": 20250101,
                    }
                ],
            ),
        ]
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        # Add needed Dimensions
        for element, data in dimensionEntries:
            butler.registry.insertDimensionData(element, *data)

        # When a DatasetType is added to the registry entries are not created
        # for components but querying them can return the components.
        datasetTypeNames = {"metric", "metric2", "metric4", "metric33", "pvi", "paramtest"}
        components = set()
        for datasetTypeName in datasetTypeNames:
            # Create and register a DatasetType
            self.addDatasetType(datasetTypeName, dimensions, storageClass, butler.registry)

            for componentName in storageClass.components:
                components.add(DatasetType.nameWithComponent(datasetTypeName, componentName))

        fromRegistry: set[DatasetType] = set()
        for parent_dataset_type in butler.registry.queryDatasetTypes():
            fromRegistry.add(parent_dataset_type)
            fromRegistry.update(parent_dataset_type.makeAllComponentDatasetTypes())
        self.assertEqual({d.name for d in fromRegistry}, datasetTypeNames | components)

        # Query with wildcard.
        dataset_types = butler.registry.queryDatasetTypes("metric*")
        self.assertEqual(len(dataset_types), 4, f"Got: {dataset_types}")
        # but not regex.
        with self.assertRaises(DatasetTypeExpressionError):
            butler.registry.queryDatasetTypes(["pvi", re.compile("metric.*")])

        # Now that we have some dataset types registered, validate them
        butler.validateConfiguration(
            ignore=[
                "test_metric_comp",
                "metric3",
                "metric5",
                "calexp",
                "DummySC",
                "datasetType.component",
                "random_data",
                "random_data_2",
            ]
        )

        # Add a new datasetType that will fail template validation
        self.addDatasetType("test_metric_comp", dimensions, storageClass, butler.registry)
        if self.validationCanFail:
            with self.assertRaises(ValidationError):
                butler.validateConfiguration()

        # Rerun validation but with a subset of dataset type names
        butler.validateConfiguration(datasetTypeNames=["metric4"])

        # Rerun validation but ignore the bad datasetType
        butler.validateConfiguration(
            ignore=[
                "test_metric_comp",
                "metric3",
                "metric5",
                "calexp",
                "DummySC",
                "datasetType.component",
                "random_data",
                "random_data_2",
            ]
        )

    def testTransaction(self) -> None:
        butler = self.create_empty_butler(run=self.default_run)
        datasetTypeName = "test_metric"
        dimensions = butler.dimensions.conform(["instrument", "visit"])
        dimensionEntries: tuple[tuple[str, Mapping[str, Any]], ...] = (
            ("instrument", {"instrument": "DummyCam"}),
            ("physical_filter", {"instrument": "DummyCam", "name": "d-r", "band": "R"}),
            ("day_obs", {"instrument": "DummyCam", "id": 20250101}),
            (
                "visit",
                {
                    "instrument": "DummyCam",
                    "id": 42,
                    "name": "fortytwo",
                    "physical_filter": "d-r",
                    "day_obs": 20250101,
                },
            ),
        )
        storageClass = self.storageClassFactory.getStorageClass("StructuredData")
        metric = makeExampleMetrics()
        dataId = {"instrument": "DummyCam", "visit": 42}
        # Create and register a DatasetType
        datasetType = self.addDatasetType(datasetTypeName, dimensions, storageClass, butler.registry)
        with self.assertRaises(TransactionTestError):
            with butler.transaction():
                # Add needed Dimensions
                for args in dimensionEntries:
                    butler.registry.insertDimensionData(*args)
                # Store a dataset
                ref = butler.put(metric, datasetTypeName, dataId)
                self.assertIsInstance(ref, DatasetRef)
                # Test get of a ref.
                metricOut = butler.get(ref)
                self.assertEqual(metric, metricOut)
                # Test get
                metricOut = butler.get(datasetTypeName, dataId)
                self.assertEqual(metric, metricOut)
                # Check we can get components
                self.assertGetComponents(butler, ref, ("summary", "data", "output"), metric)
                raise TransactionTestError("This should roll back the entire transaction")
        with self.assertRaises(DataIdValueError, msg=f"Check can't expand DataId {dataId}"):
            butler.registry.expandDataId(dataId)
        # Should raise LookupError for missing data ID value
        with self.assertRaises(LookupError, msg=f"Check can't get by {datasetTypeName} and {dataId}"):
            butler.get(datasetTypeName, dataId)
        # Also check explicitly if Dataset entry is missing
        self.assertIsNone(butler.find_dataset(datasetType, dataId, collections=butler.collections.defaults))
        # Direct retrieval should not find the file in the Datastore
        with self.assertRaises(FileNotFoundError, msg=f"Check {ref} can't be retrieved directly"):
            butler.get(ref)

    def testMakeRepo(self) -> None:
        """Test that we can write butler configuration to a new repository via
        the Butler.makeRepo interface and then instantiate a butler from the
        repo root.
        """
        # Do not run the test if we know this datastore configuration does
        # not support a file system root
        if self.fullConfigKey is None:
            return

        # create two separate directories
        root1 = tempfile.mkdtemp(dir=self.root)
        root2 = tempfile.mkdtemp(dir=self.root)

        butlerConfig = Butler.makeRepo(root1, config=Config(self.configFile))
        limited = Config(self.configFile)
        butler1 = Butler.from_config(butlerConfig)
        assert isinstance(butler1, DirectButler), "Expect DirectButler in configuration"
        butlerConfig = Butler.makeRepo(root2, standalone=True, config=Config(self.configFile))
        full = Config(self.tmpConfigFile)
        butler2 = Butler.from_config(butlerConfig)
        assert isinstance(butler2, DirectButler), "Expect DirectButler in configuration"
        # Butlers should have the same configuration regardless of whether
        # defaults were expanded.
        self.assertEqual(butler1._config, butler2._config)
        # Config files loaded directly should not be the same.
        self.assertNotEqual(limited, full)
        # Make sure "limited" doesn't have a few keys we know it should be
        # inheriting from defaults.
        self.assertIn(self.fullConfigKey, full)
        self.assertNotIn(self.fullConfigKey, limited)

        # Collections don't appear until something is put in them
        collections1 = set(butler1.registry.queryCollections())
        self.assertEqual(collections1, set())
        self.assertEqual(set(butler2.registry.queryCollections()), collections1)

        # Check that a config with no associated file name will not
        # work properly with relocatable Butler repo
        butlerConfig.configFile = None
        with self.assertRaises(ValueError):
            Butler.from_config(butlerConfig)

        with self.assertRaises(FileExistsError):
            Butler.makeRepo(self.root, standalone=True, config=Config(self.configFile), overwrite=False)

    def testStringification(self) -> None:
        butler = Butler.from_config(self.tmpConfigFile, run=self.default_run)
        butlerStr = str(butler)

        if self.datastoreStr is not None:
            for testStr in self.datastoreStr:
                self.assertIn(testStr, butlerStr)
        if self.registryStr is not None:
            self.assertIn(self.registryStr, butlerStr)

        datastoreName = butler._datastore.name
        if self.datastoreName is not None:
            for testStr in self.datastoreName:
                self.assertIn(testStr, datastoreName)

    def testButlerRewriteDataId(self) -> None:
        """Test that dataIds can be rewritten based on dimension records."""
        butler = self.create_empty_butler(run=self.default_run)

        storageClass = self.storageClassFactory.getStorageClass("StructuredDataDict")
        datasetTypeName = "random_data"

        # Create dimension records.
        butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        butler.registry.insertDimensionData(
            "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
        )
        butler.registry.insertDimensionData(
            "detector", {"instrument": "DummyCamComp", "id": 1, "full_name": "det1"}
        )

        dimensions = butler.dimensions.conform(["instrument", "exposure"])
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        butler.registry.registerDatasetType(datasetType)

        n_exposures = 5
        dayobs = 20210530

        # Create records for multiple day_obs but same seq_num to test that
        # we are constraining gets properly when day_obs/seq_num is used
        # for an exposure. Second day is year in future but is not used.
        for day_obs in (dayobs, dayobs + 1_00_00):
            butler.registry.insertDimensionData("day_obs", {"instrument": "DummyCamComp", "id": day_obs})

            for i in range(n_exposures):
                group_name = f"group_{day_obs}_{i}"
                butler.registry.insertDimensionData(
                    "group", {"instrument": "DummyCamComp", "name": group_name}
                )
                butler.registry.insertDimensionData(
                    "exposure",
                    {
                        "instrument": "DummyCamComp",
                        "id": day_obs + i,
                        "obs_id": f"exp_{day_obs}_{i}",
                        "seq_num": i,
                        "day_obs": day_obs,
                        "physical_filter": "d-r",
                        "group": group_name,
                    },
                )

        # Write some data.
        for i in range(n_exposures):
            metric = {"something": i, "other": "metric", "list": [2 * x for x in range(i)]}

            # Use the seq_num for the put to test rewriting.
            dataId = {"seq_num": i, "day_obs": dayobs, "instrument": "DummyCamComp", "physical_filter": "d-r"}
            ref = butler.put(metric, datasetTypeName, dataId=dataId)

            # Check that the exposure is correct in the dataId
            self.assertEqual(ref.dataId["exposure"], dayobs + i)

            # and check that we can get the dataset back with the same dataId
            new_metric = butler.get(datasetTypeName, dataId=dataId)
            self.assertEqual(new_metric, metric)

        # Check that we can find the datasets using the day_obs or the
        # exposure.day_obs.
        datasets_1 = list(
            butler.registry.queryDatasets(
                datasetType,
                collections=self.default_run,
                where="day_obs = dayObs AND instrument = instr",
                bind={"dayObs": dayobs, "instr": "DummyCamComp"},
            )
        )
        datasets_2 = list(
            butler.registry.queryDatasets(
                datasetType,
                collections=self.default_run,
                where="exposure.day_obs = dayObs AND instrument = instr",
                bind={"dayObs": dayobs, "instr": "DummyCamComp"},
            )
        )
        self.assertEqual(datasets_1, datasets_2)

    def testGetDatasetCollectionCaching(self):
        # Prior to DM-41117, there was a bug where get_dataset would throw
        # MissingCollectionError if you tried to fetch a dataset that was added
        # after the collection cache was last updated.
        reader_butler, datasetType = self.create_butler(self.default_run, "int", "datasettypename")
        writer_butler = self.create_empty_butler(writeable=True, run="new_run")
        dataId = {"instrument": "DummyCamComp", "visit": 423}
        put_ref = writer_butler.put(123, datasetType, dataId)
        get_ref = reader_butler.get_dataset(put_ref.id)
        self.assertEqual(get_ref.id, put_ref.id)

    def testCollectionChainRedefine(self):
        butler = self._setup_to_test_collection_chain()

        butler.collections.redefine_chain("chain", "a")
        self._check_chain(butler, ["a"])

        # Duplicates are removed from the list of children
        butler.collections.redefine_chain("chain", ["c", "b", "c"])
        self._check_chain(butler, ["c", "b"])

        # Empty list clears the chain
        butler.collections.redefine_chain("chain", [])
        self._check_chain(butler, [])

        self._test_common_chain_functionality(butler, butler.collections.redefine_chain)

    def testCollectionChainPrepend(self):
        butler = self._setup_to_test_collection_chain()

        # Duplicates are removed from the list of children
        butler.collections.prepend_chain("chain", ["c", "b", "c"])
        self._check_chain(butler, ["c", "b"])

        # Prepend goes on the front of existing chain
        butler.collections.prepend_chain("chain", ["a"])
        self._check_chain(butler, ["a", "c", "b"])

        # Empty prepend does nothing
        butler.collections.prepend_chain("chain", [])
        self._check_chain(butler, ["a", "c", "b"])

        # Prepending children that already exist in the chain removes them from
        # their current position.
        butler.collections.prepend_chain("chain", ["d", "b", "c"])
        self._check_chain(butler, ["d", "b", "c", "a"])

        self._test_common_chain_functionality(butler, butler.collections.prepend_chain)

    def testCollectionChainExtend(self):
        butler = self._setup_to_test_collection_chain()

        # Duplicates are removed from the list of children
        butler.collections.extend_chain("chain", ["c", "b", "c"])
        self._check_chain(butler, ["c", "b"])

        # Extend goes on the end of existing chain
        butler.collections.extend_chain("chain", ["a"])
        self._check_chain(butler, ["c", "b", "a"])

        # Empty extend does nothing
        butler.collections.extend_chain("chain", [])
        self._check_chain(butler, ["c", "b", "a"])

        # Extending children that already exist in the chain removes them from
        # their current position.
        butler.collections.extend_chain("chain", ["d", "b", "c"])
        self._check_chain(butler, ["a", "d", "b", "c"])

        self._test_common_chain_functionality(butler, butler.collections.extend_chain)

    def testCollectionChainRemove(self) -> None:
        butler = self._setup_to_test_collection_chain()

        butler.collections.redefine_chain("chain", ["a", "b", "c", "d"])

        butler.collections.remove_from_chain("chain", "c")
        self._check_chain(butler, ["a", "b", "d"])

        # Duplicates are allowed in the list of children
        butler.collections.remove_from_chain("chain", ["b", "b", "a"])
        self._check_chain(butler, ["d"])

        # Empty remove does nothing
        butler.collections.remove_from_chain("chain", [])
        self._check_chain(butler, ["d"])

        # Removing children that aren't in the chain does nothing
        butler.collections.remove_from_chain("chain", ["a", "chain"])
        self._check_chain(butler, ["d"])

        self._test_common_chain_functionality(
            butler, butler.collections.remove_from_chain, skip_cycle_check=True
        )

    def _setup_to_test_collection_chain(self) -> Butler:
        butler = self.create_empty_butler(writeable=True)

        butler.collections.register("chain", CollectionType.CHAINED)

        runs = ["a", "b", "c", "d"]
        for run in runs:
            butler.collections.register(run)

        butler.collections.register("staticchain", CollectionType.CHAINED)
        butler.collections.redefine_chain("staticchain", ["a", "b"])

        return butler

    def _check_chain(self, butler: Butler, expected: list[str]) -> None:
        children = butler.collections.get_info("chain").children
        self.assertEqual(expected, list(children))

    def _test_common_chain_functionality(
        self, butler, func: Callable[[str, str | list[str]], Any], *, skip_cycle_check=False
    ) -> None:
        # Missing parent collection
        with self.assertRaises(MissingCollectionError):
            func("doesnotexist", [])
        # Missing child collection
        with self.assertRaises(MissingCollectionError):
            func("chain", ["doesnotexist"])
        # Forbid operations on non-chained collections
        with self.assertRaises(CollectionTypeError):
            func("d", ["a"])

        # Prevent collection cycles
        if not skip_cycle_check:
            butler.collections.register("chain2", CollectionType.CHAINED)
            func("chain2", "chain")
            with self.assertRaises(CollectionCycleError):
                func("chain", "chain2")

        # Make sure none of the earlier operations interfered with unrelated
        # chains.
        self.assertEqual(["a", "b"], list(butler.collections.get_info("staticchain").children))

        with butler._caching_context():
            with self.assertRaisesRegex(RuntimeError, "Chained collection modification not permitted"):
                func("chain", "a")


class FileDatastoreButlerTests(ButlerTests):
    """Common tests and specialization of ButlerTests for butlers backed
    by datastores that inherit from FileDatastore.
    """

    trustModeSupported = True

    def checkFileExists(self, root: str | ResourcePath, relpath: str | ResourcePath) -> bool:
        """Check if file exists at a given path (relative to root).

        Test testPutTemplates verifies actual physical existance of the files
        in the requested location.
        """
        uri = ResourcePath(root, forceDirectory=True)
        return uri.join(relpath).exists()

    def testPutTemplates(self) -> None:
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        butler = self.create_empty_butler(run=self.default_run)

        # Add needed Dimensions
        butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        butler.registry.insertDimensionData(
            "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
        )
        butler.registry.insertDimensionData("day_obs", {"instrument": "DummyCamComp", "id": 20250101})
        butler.registry.insertDimensionData(
            "visit",
            {
                "instrument": "DummyCamComp",
                "id": 423,
                "name": "v423",
                "physical_filter": "d-r",
                "day_obs": 20250101,
            },
        )
        butler.registry.insertDimensionData(
            "visit",
            {
                "instrument": "DummyCamComp",
                "id": 425,
                "name": "v425",
                "physical_filter": "d-r",
                "day_obs": 20250101,
            },
        )

        # Create and store a dataset
        metric = makeExampleMetrics()

        # Create two almost-identical DatasetTypes (both will use default
        # template)
        dimensions = butler.dimensions.conform(["instrument", "visit"])
        butler.registry.registerDatasetType(DatasetType("metric1", dimensions, storageClass))
        butler.registry.registerDatasetType(DatasetType("metric2", dimensions, storageClass))
        butler.registry.registerDatasetType(DatasetType("metric3", dimensions, storageClass))

        dataId1 = {"instrument": "DummyCamComp", "visit": 423}
        dataId2 = {"instrument": "DummyCamComp", "visit": 423, "physical_filter": "d-r"}

        # Put with exactly the data ID keys needed
        ref = butler.put(metric, "metric1", dataId1)
        uri = butler.getURI(ref)
        self.assertTrue(uri.exists())
        self.assertTrue(
            uri.unquoted_path.endswith(f"{self.default_run}/metric1/??#?/d-r/DummyCamComp_423.pickle")
        )

        # Check the template based on dimensions
        if hasattr(butler._datastore, "templates"):
            butler._datastore.templates.validateTemplates([ref])

        # Put with extra data ID keys (physical_filter is an optional
        # dependency); should not change template (at least the way we're
        # defining them  to behave now; the important thing is that they
        # must be consistent).
        ref = butler.put(metric, "metric2", dataId2)
        uri = butler.getURI(ref)
        self.assertTrue(uri.exists())
        self.assertTrue(
            uri.unquoted_path.endswith(f"{self.default_run}/metric2/d-r/DummyCamComp_v423.pickle")
        )

        # Check the template based on dimensions
        if hasattr(butler._datastore, "templates"):
            butler._datastore.templates.validateTemplates([ref])

        # Use a template that has a typo in dimension record metadata.
        # Easier to test with a butler that has a ref with records attached.
        template = FileTemplate("a/{visit.name}/{id}_{visit.namex:?}.fits")
        with self.assertLogs("lsst.daf.butler.datastore.file_templates", "INFO"):
            path = template.format(ref)
        self.assertEqual(path, f"a/v423/{ref.id}_fits")

        template = FileTemplate("a/{visit.name}/{id}_{visit.namex}.fits")
        with self.assertRaises(KeyError):
            with self.assertLogs("lsst.daf.butler.datastore.file_templates", "INFO"):
                template.format(ref)

        # Now use a file template that will not result in unique filenames
        with self.assertRaises(FileTemplateValidationError):
            butler.put(metric, "metric3", dataId1)

    def testImportExport(self) -> None:
        # Run put/get tests just to create and populate a repo.
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        self.runImportExportTest(storageClass)

    @unittest.expectedFailure
    def testImportExportVirtualComposite(self) -> None:
        # Run put/get tests just to create and populate a repo.
        storageClass = self.storageClassFactory.getStorageClass("StructuredComposite")
        self.runImportExportTest(storageClass)

    def runImportExportTest(self, storageClass: StorageClass) -> None:
        """Test exporting and importing.

        This test does an export to a temp directory and an import back
        into a new temp directory repo. It does not assume a posix datastore.
        """
        exportButler = self.runPutGetTest(storageClass, "test_metric")

        # Test that we must have a file extension.
        with self.assertRaises(ValueError):
            with exportButler.export(filename="dump", directory=".") as export:
                pass

        # Test that unknown format is not allowed.
        with self.assertRaises(ValueError):
            with exportButler.export(filename="dump.fits", directory=".") as export:
                pass

        # Test that the repo actually has at least one dataset.
        datasets = list(exportButler.registry.queryDatasets(..., collections=...))
        self.assertGreater(len(datasets), 0)
        # Add a DimensionRecord that's unused by those datasets.
        skymapRecord = {"name": "example_skymap", "hash": (50).to_bytes(8, byteorder="little")}
        exportButler.registry.insertDimensionData("skymap", skymapRecord)
        # Export and then import datasets.
        with safeTestTempDir(TESTDIR) as exportDir:
            exportFile = os.path.join(exportDir, "exports.yaml")
            with exportButler.export(filename=exportFile, directory=exportDir, transfer="auto") as export:
                export.saveDatasets(datasets)
                # Export the same datasets again. This should quietly do
                # nothing because of internal deduplication, and it shouldn't
                # complain about being asked to export the "htm7" elements even
                # though there aren't any in these datasets or in the database.
                export.saveDatasets(datasets, elements=["htm7"])
                # Save one of the data IDs again; this should be harmless
                # because of internal deduplication.
                export.saveDataIds([datasets[0].dataId])
                # Save some dimension records directly.
                export.saveDimensionData("skymap", [skymapRecord])
            self.assertTrue(os.path.exists(exportFile))
            with safeTestTempDir(TESTDIR) as importDir:
                # We always want this to be a local posix butler
                Butler.makeRepo(importDir, config=Config(os.path.join(TESTDIR, "config/basic/butler.yaml")))
                # Calling script.butlerImport tests the implementation of the
                # butler command line interface "import" subcommand. Functions
                # in the script folder are generally considered protected and
                # should not be used as public api.
                with open(exportFile) as f:
                    script.butlerImport(
                        importDir,
                        export_file=f,
                        directory=exportDir,
                        transfer="auto",
                        skip_dimensions=None,
                    )
                importButler = Butler.from_config(importDir, run=self.default_run)
                for ref in datasets:
                    with self.subTest(ref=ref):
                        # Test for existence by passing in the DatasetType and
                        # data ID separately, to avoid lookup by dataset_id.
                        self.assertTrue(importButler.exists(ref.datasetType, ref.dataId))
                self.assertEqual(
                    list(importButler.registry.queryDimensionRecords("skymap")),
                    [importButler.dimensions["skymap"].RecordClass(**skymapRecord)],
                )

    def testRemoveRuns(self) -> None:
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        butler = self.create_empty_butler(writeable=True)
        # Load registry data with dimensions to hang datasets off of.
        registryDataDir = os.path.normpath(os.path.join(os.path.dirname(__file__), "data", "registry"))
        butler.import_(filename=os.path.join(registryDataDir, "base.yaml"))
        # Add some RUN-type collection.
        run1 = "run1"
        butler.collections.register(run1)
        run2 = "run2"
        butler.collections.register(run2)
        # put a dataset in each
        metric = makeExampleMetrics()
        dimensions = butler.dimensions.conform(["instrument", "physical_filter"])
        datasetType = self.addDatasetType(
            "prune_collections_test_dataset", dimensions, storageClass, butler.registry
        )
        ref1 = butler.put(metric, datasetType, {"instrument": "Cam1", "physical_filter": "Cam1-G"}, run=run1)
        ref2 = butler.put(metric, datasetType, {"instrument": "Cam1", "physical_filter": "Cam1-G"}, run=run2)
        uri1 = butler.getURI(ref1)
        uri2 = butler.getURI(ref2)

        with self.assertRaises(OrphanedRecordError):
            butler.registry.removeDatasetType(datasetType.name)

        # Remove from both runs with different values for unstore.
        butler.removeRuns([run1], unstore=True)
        butler.removeRuns([run2], unstore=False)
        # Should be nothing in registry for either one, and datastore should
        # not think either exists.
        with self.assertRaises(MissingCollectionError):
            butler.collections.get_info(run1)
        with self.assertRaises(MissingCollectionError):
            butler.collections.get_info(run1)
        self.assertFalse(butler.stored(ref1))
        self.assertFalse(butler.stored(ref2))
        # The ref we unstored should be gone according to the URI, but the
        # one we forgot should still be around.
        self.assertFalse(uri1.exists())
        self.assertTrue(uri2.exists())

        # Now that the collections have been pruned we can remove the
        # dataset type
        butler.registry.removeDatasetType(datasetType.name)

        with self.assertLogs("lsst.daf.butler.registry", "INFO") as cm:
            butler.registry.removeDatasetType(("test*", "test*"))
        self.assertIn("not defined", "\n".join(cm.output))

    def remove_dataset_out_of_band(self, butler: Butler, ref: DatasetRef) -> None:
        """Simulate an external actor removing a file outside of Butler's
        knowledge.

        Subclasses may override to handle more complicated datastore
        configurations.
        """
        uri = butler.getURI(ref)
        uri.remove()
        datastore = cast(FileDatastore, butler._datastore)
        datastore.cacheManager.remove_from_cache(ref)

    def testPruneDatasets(self) -> None:
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        butler = self.create_empty_butler(writeable=True)
        # Load registry data with dimensions to hang datasets off of.
        registryDataDir = os.path.normpath(os.path.join(TESTDIR, "data", "registry"))
        butler.import_(filename=os.path.join(registryDataDir, "base.yaml"))
        # Add some RUN-type collections.
        run1 = "run1"
        butler.collections.register(run1)
        run2 = "run2"
        butler.collections.register(run2)
        # put some datasets.  ref1 and ref2 have the same data ID, and are in
        # different runs.  ref3 has a different data ID.
        metric = makeExampleMetrics()
        dimensions = butler.dimensions.conform(["instrument", "physical_filter"])
        datasetType = self.addDatasetType(
            "prune_collections_test_dataset", dimensions, storageClass, butler.registry
        )
        ref1 = butler.put(metric, datasetType, {"instrument": "Cam1", "physical_filter": "Cam1-G"}, run=run1)
        ref2 = butler.put(metric, datasetType, {"instrument": "Cam1", "physical_filter": "Cam1-G"}, run=run2)
        ref3 = butler.put(metric, datasetType, {"instrument": "Cam1", "physical_filter": "Cam1-R1"}, run=run1)

        many_stored = butler.stored_many([ref1, ref2, ref3])
        for ref, stored in many_stored.items():
            self.assertTrue(stored, f"Ref {ref} should be stored")

        many_exists = butler._exists_many([ref1, ref2, ref3])
        for ref, exists in many_exists.items():
            self.assertTrue(exists, f"Checking ref {ref} exists.")
            self.assertEqual(exists, DatasetExistence.VERIFIED, f"Ref {ref} should be stored")

        # Simple prune.
        butler.pruneDatasets([ref1, ref2, ref3], purge=True, unstore=True)
        self.assertFalse(butler.exists(ref1.datasetType, ref1.dataId, collections=run1))

        many_stored = butler.stored_many([ref1, ref2, ref3])
        for ref, stored in many_stored.items():
            self.assertFalse(stored, f"Ref {ref} should not be stored")

        many_exists = butler._exists_many([ref1, ref2, ref3])
        for ref, exists in many_exists.items():
            self.assertEqual(exists, DatasetExistence.UNRECOGNIZED, f"Ref {ref} should not be stored")

        # Put data back.
        ref1_new = butler.put(metric, ref1)
        self.assertEqual(ref1_new, ref1)  # Reuses original ID.
        ref2 = butler.put(metric, ref2)

        many_stored = butler.stored_many([ref1, ref2, ref3])
        self.assertTrue(many_stored[ref1])
        self.assertTrue(many_stored[ref2])
        self.assertFalse(many_stored[ref3])

        ref3 = butler.put(metric, ref3)

        many_exists = butler._exists_many([ref1, ref2, ref3])
        for ref, exists in many_exists.items():
            self.assertTrue(exists, f"Ref {ref} should not be stored")

        # Clear out the datasets from registry and start again.
        refs = [ref1, ref2, ref3]
        butler.pruneDatasets(refs, purge=True, unstore=True)
        for ref in refs:
            butler.put(metric, ref)

        # Confirm we can retrieve deferred.
        dref1 = butler.getDeferred(ref1)  # known and exists
        metric1 = dref1.get()
        self.assertEqual(metric1, metric)

        # Test different forms of file availability.
        # Need to be in a state where:
        # - one ref just has registry record.
        # - one ref has a missing file but a datastore record.
        # - one ref has a missing datastore record but file is there.
        # - one ref does not exist anywhere.
        # Do not need to test a ref that has everything since that is tested
        # above.
        ref0 = DatasetRef(
            datasetType,
            DataCoordinate.standardize(
                {"instrument": "Cam1", "physical_filter": "Cam1-G"}, universe=butler.dimensions
            ),
            run=run1,
        )

        # Delete from datastore and retain in Registry.
        butler.pruneDatasets([ref1], purge=False, unstore=True, disassociate=False)

        # File has been removed.
        self.remove_dataset_out_of_band(butler, ref2)

        # Datastore has lost track.
        butler._datastore.forget([ref3])

        # First test with a standard butler.
        exists_many = butler._exists_many([ref0, ref1, ref2, ref3], full_check=True)
        self.assertEqual(exists_many[ref0], DatasetExistence.UNRECOGNIZED)
        self.assertEqual(exists_many[ref1], DatasetExistence.RECORDED)
        self.assertEqual(exists_many[ref2], DatasetExistence.RECORDED | DatasetExistence.DATASTORE)
        self.assertEqual(exists_many[ref3], DatasetExistence.RECORDED)

        exists_many = butler._exists_many([ref0, ref1, ref2, ref3], full_check=False)
        self.assertEqual(exists_many[ref0], DatasetExistence.UNRECOGNIZED)
        self.assertEqual(exists_many[ref1], DatasetExistence.RECORDED | DatasetExistence._ASSUMED)
        self.assertEqual(exists_many[ref2], DatasetExistence.KNOWN)
        self.assertEqual(exists_many[ref3], DatasetExistence.RECORDED | DatasetExistence._ASSUMED)
        self.assertTrue(exists_many[ref2])

        # Check that per-ref query gives the same answer as many query.
        for ref, exists in exists_many.items():
            self.assertEqual(butler.exists(ref, full_check=False), exists)

        # Get deferred checks for existence before it allows it to be
        # retrieved.
        with self.assertRaises(LookupError):
            butler.getDeferred(ref3)  # not known, file exists
        dref2 = butler.getDeferred(ref2)  # known but file missing
        with self.assertRaises(FileNotFoundError):
            dref2.get()

        # Test again with a trusting butler.
        if self.trustModeSupported:
            butler._datastore.trustGetRequest = True
            exists_many = butler._exists_many([ref0, ref1, ref2, ref3], full_check=True)
            self.assertEqual(exists_many[ref0], DatasetExistence.UNRECOGNIZED)
            self.assertEqual(exists_many[ref1], DatasetExistence.RECORDED)
            self.assertEqual(exists_many[ref2], DatasetExistence.RECORDED | DatasetExistence.DATASTORE)
            self.assertEqual(exists_many[ref3], DatasetExistence.RECORDED | DatasetExistence._ARTIFACT)

            # When trusting we can get a deferred dataset handle that is not
            # known but does exist.
            dref3 = butler.getDeferred(ref3)
            metric3 = dref3.get()
            self.assertEqual(metric3, metric)

            # Check that per-ref query gives the same answer as many query.
            for ref, exists in exists_many.items():
                self.assertEqual(butler.exists(ref, full_check=True), exists)

            # Create a ref that surprisingly has the UUID of an existing ref
            # but is not the same.
            ref_bad = DatasetRef(datasetType, dataId=ref3.dataId, run=ref3.run, id=ref2.id)
            with self.assertRaises(ValueError):
                butler.exists(ref_bad)

            # Create a ref that has a compatible storage class.
            ref_compat = ref2.overrideStorageClass("StructuredDataDict")
            exists = butler.exists(ref_compat)
            self.assertEqual(exists, exists_many[ref2])

            # Remove everything and start from scratch.
            butler._datastore.trustGetRequest = False
            butler.pruneDatasets(refs, purge=True, unstore=True)
            for ref in refs:
                butler.put(metric, ref)

            # These tests mess directly with the trash table and can leave the
            # datastore in an odd state. Do them at the end.
            # Check that in normal mode, deleting the record will lead to
            # trash not touching the file.
            uri1 = butler.getURI(ref1)
            butler._datastore.bridge.moveToTrash(
                [ref1], transaction=None
            )  # Update the dataset_location table
            butler._datastore.forget([ref1])
            butler._datastore.trash(ref1)
            butler._datastore.emptyTrash()
            self.assertTrue(uri1.exists())
            uri1.remove()  # Clean it up.

            # Simulate execution butler setup by deleting the datastore
            # record but keeping the file around and trusting.
            butler._datastore.trustGetRequest = True
            uris = butler.get_many_uris([ref2, ref3])
            uri2 = uris[ref2].primaryURI
            uri3 = uris[ref3].primaryURI
            self.assertTrue(uri2.exists())
            self.assertTrue(uri3.exists())

            # Remove the datastore record.
            butler._datastore.bridge.moveToTrash(
                [ref2], transaction=None
            )  # Update the dataset_location table
            butler._datastore.forget([ref2])
            self.assertTrue(uri2.exists())
            butler._datastore.trash([ref2, ref3])
            # Immediate removal for ref2 file
            self.assertFalse(uri2.exists())
            # But ref3 has to wait for the empty.
            self.assertTrue(uri3.exists())
            butler._datastore.emptyTrash()
            self.assertFalse(uri3.exists())

            # Clear out the datasets from registry.
            butler.pruneDatasets([ref1, ref2, ref3], purge=True, unstore=True)


class PosixDatastoreButlerTestCase(FileDatastoreButlerTests, unittest.TestCase):
    """PosixDatastore specialization of a butler"""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    fullConfigKey: str | None = ".datastore.formatters"
    validationCanFail = True
    datastoreStr = ["/tmp"]
    datastoreName = [f"FileDatastore@{BUTLER_ROOT_TAG}"]
    registryStr = "/gen3.sqlite3"

    def testPathConstructor(self) -> None:
        """Independent test of constructor using PathLike."""
        butler = Butler.from_config(self.tmpConfigFile, run=self.default_run)
        self.assertIsInstance(butler, Butler)

        # And again with a Path object with the butler yaml
        path = pathlib.Path(self.tmpConfigFile)
        butler = Butler.from_config(path, writeable=False)
        self.assertIsInstance(butler, Butler)

        # And again with a Path object without the butler yaml
        # (making sure we skip it if the tmp config doesn't end
        # in butler.yaml -- which is the case for a subclass)
        if self.tmpConfigFile.endswith("butler.yaml"):
            path = pathlib.Path(os.path.dirname(self.tmpConfigFile))
            butler = Butler.from_config(path, writeable=False)
            self.assertIsInstance(butler, Butler)

    def testExportTransferCopy(self) -> None:
        """Test local export using all transfer modes"""
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        exportButler = self.runPutGetTest(storageClass, "test_metric")
        # Test that the repo actually has at least one dataset.
        datasets = list(exportButler.registry.queryDatasets(..., collections=...))
        self.assertGreater(len(datasets), 0)
        uris = [exportButler.getURI(d) for d in datasets]
        assert isinstance(exportButler._datastore, FileDatastore)
        datastoreRoot = exportButler.get_datastore_roots()[exportButler.get_datastore_names()[0]]

        pathsInStore = [uri.relative_to(datastoreRoot) for uri in uris]

        for path in pathsInStore:
            # Assume local file system
            assert path is not None
            self.assertTrue(self.checkFileExists(datastoreRoot, path), f"Checking path {path}")

        for transfer in ("copy", "link", "symlink", "relsymlink"):
            with safeTestTempDir(TESTDIR) as exportDir:
                with exportButler.export(directory=exportDir, format="yaml", transfer=transfer) as export:
                    export.saveDatasets(datasets)
                    for path in pathsInStore:
                        assert path is not None
                        self.assertTrue(
                            self.checkFileExists(exportDir, path),
                            f"Check that mode {transfer} exported files",
                        )

    def testPytypeCoercion(self) -> None:
        """Test python type coercion on Butler.get and put."""
        # Store some data with the normal example storage class.
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        datasetTypeName = "test_metric"
        butler = self.runPutGetTest(storageClass, datasetTypeName)

        dataId = {"instrument": "DummyCamComp", "visit": 423}
        metric = butler.get(datasetTypeName, dataId=dataId)
        self.assertEqual(get_full_type_name(metric), "lsst.daf.butler.tests.MetricsExample")

        datasetType_ori = butler.get_dataset_type(datasetTypeName)
        self.assertEqual(datasetType_ori.storageClass.name, "StructuredDataNoComponents")

        # Now need to hack the registry dataset type definition.
        # There is no API for this.
        assert isinstance(butler._registry, SqlRegistry)
        manager = butler._registry._managers.datasets
        assert hasattr(manager, "_db") and hasattr(manager, "_static")
        manager._db.update(
            manager._static.dataset_type,
            {"name": datasetTypeName},
            {datasetTypeName: datasetTypeName, "storage_class": "StructuredDataNoComponentsModel"},
        )

        # Force reset of dataset type cache
        butler.registry.refresh()

        datasetType_new = butler.get_dataset_type(datasetTypeName)
        self.assertEqual(datasetType_new.name, datasetType_ori.name)
        self.assertEqual(datasetType_new.storageClass.name, "StructuredDataNoComponentsModel")

        metric_model = butler.get(datasetTypeName, dataId=dataId)
        self.assertNotEqual(type(metric_model), type(metric))
        self.assertEqual(get_full_type_name(metric_model), "lsst.daf.butler.tests.MetricsExampleModel")

        # Put the model and read it back to show that everything now
        # works as normal.
        metric_ref = butler.put(metric_model, datasetTypeName, dataId=dataId, visit=424)
        metric_model_new = butler.get(metric_ref)
        self.assertEqual(metric_model_new, metric_model)

        # Hack the storage class again to something that will fail on the
        # get with no conversion class.
        manager._db.update(
            manager._static.dataset_type,
            {"name": datasetTypeName},
            {datasetTypeName: datasetTypeName, "storage_class": "StructuredDataListYaml"},
        )
        butler.registry.refresh()

        with self.assertRaises(ValueError):
            butler.get(datasetTypeName, dataId=dataId)


class PostgresPosixDatastoreButlerTestCase(FileDatastoreButlerTests, unittest.TestCase):
    """PosixDatastore specialization of a butler using Postgres"""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    fullConfigKey = ".datastore.formatters"
    validationCanFail = True
    datastoreStr = ["/tmp"]
    datastoreName = [f"FileDatastore@{BUTLER_ROOT_TAG}"]
    registryStr = "PostgreSQL@test"

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgresql = cls.enterClassContext(setup_postgres_test_db())
        super().setUpClass()

    def setUp(self) -> None:
        # Need to add a registry section to the config.
        self._temp_config = False
        config = Config(self.configFile)
        self.postgresql.patch_butler_config(config)
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as fh:
            config.dump(fh)
            self.configFile = fh.name
            self._temp_config = True
        super().setUp()

    def tearDown(self) -> None:
        if self._temp_config and os.path.exists(self.configFile):
            os.remove(self.configFile)
        super().tearDown()

    def testMakeRepo(self) -> None:
        # The base class test assumes that it's using sqlite and assumes
        # the config file is acceptable to sqlite.
        raise unittest.SkipTest("Postgres config is not compatible with this test.")


class ClonedPostgresPosixDatastoreButlerTestCase(PostgresPosixDatastoreButlerTestCase, unittest.TestCase):
    """Test that Butler with a Postgres registry still works after cloning."""

    def create_butler(
        self, run: str, storageClass: StorageClass | str, datasetTypeName: str
    ) -> tuple[DirectButler, DatasetType]:
        butler, datasetType = super().create_butler(run, storageClass, datasetTypeName)
        return butler.clone(run=run), datasetType


class InMemoryDatastoreButlerTestCase(ButlerTests, unittest.TestCase):
    """InMemoryDatastore specialization of a butler"""

    configFile = os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml")
    fullConfigKey = None
    useTempRoot = False
    validationCanFail = False
    datastoreStr = ["datastore='InMemory"]
    datastoreName = ["InMemoryDatastore@"]
    registryStr = "/gen3.sqlite3"

    def testIngest(self) -> None:
        pass


class ClonedSqliteButlerTestCase(InMemoryDatastoreButlerTestCase, unittest.TestCase):
    """Test that a Butler with a Sqlite registry still works after cloning."""

    def create_butler(
        self, run: str, storageClass: StorageClass | str, datasetTypeName: str
    ) -> tuple[DirectButler, DatasetType]:
        butler, datasetType = super().create_butler(run, storageClass, datasetTypeName)
        return butler.clone(run=run), datasetType


class ChainedDatastoreButlerTestCase(FileDatastoreButlerTests, unittest.TestCase):
    """PosixDatastore specialization"""

    configFile = os.path.join(TESTDIR, "config/basic/butler-chained.yaml")
    fullConfigKey = ".datastore.datastores.1.formatters"
    validationCanFail = True
    datastoreStr = ["datastore='InMemory", "/FileDatastore_1/,", "/FileDatastore_2/'"]
    datastoreName = [
        "InMemoryDatastore@",
        f"FileDatastore@{BUTLER_ROOT_TAG}/FileDatastore_1",
        "SecondDatastore",
    ]
    registryStr = "/gen3.sqlite3"

    def testPruneDatasets(self) -> None:
        # This test relies on manipulating files out-of-band, which is
        # impossible for this configuration because of the InMemoryDatastore in
        # the ChainedDatastore.
        pass


class ButlerExplicitRootTestCase(PosixDatastoreButlerTestCase):
    """Test that a yaml file in one location can refer to a root in another."""

    datastoreStr = ["dir1"]
    # Disable the makeRepo test since we are deliberately not using
    # butler.yaml as the config name.
    fullConfigKey = None

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)

        # Make a new repository in one place
        self.dir1 = os.path.join(self.root, "dir1")
        Butler.makeRepo(self.dir1, config=Config(self.configFile))

        # Move the yaml file to a different place and add a "root"
        self.dir2 = os.path.join(self.root, "dir2")
        os.makedirs(self.dir2, exist_ok=True)
        configFile1 = os.path.join(self.dir1, "butler.yaml")
        config = Config(configFile1)
        config["root"] = self.dir1
        configFile2 = os.path.join(self.dir2, "butler2.yaml")
        config.dumpToUri(configFile2)
        os.remove(configFile1)
        self.tmpConfigFile = configFile2

    def testFileLocations(self) -> None:
        self.assertNotEqual(self.dir1, self.dir2)
        self.assertTrue(os.path.exists(os.path.join(self.dir2, "butler2.yaml")))
        self.assertFalse(os.path.exists(os.path.join(self.dir1, "butler.yaml")))
        self.assertTrue(os.path.exists(os.path.join(self.dir1, "gen3.sqlite3")))


class ButlerMakeRepoOutfileTestCase(ButlerPutGetTests, unittest.TestCase):
    """Test that a config file created by makeRepo outside of repo works."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)
        self.root2 = makeTestTempDir(TESTDIR)

        self.tmpConfigFile = os.path.join(self.root2, "different.yaml")
        Butler.makeRepo(self.root, config=Config(self.configFile), outfile=self.tmpConfigFile)

    def tearDown(self) -> None:
        if os.path.exists(self.root2):
            shutil.rmtree(self.root2, ignore_errors=True)
        super().tearDown()

    def testConfigExistence(self) -> None:
        c = Config(self.tmpConfigFile)
        uri_config = ResourcePath(c["root"])
        uri_expected = ResourcePath(self.root, forceDirectory=True)
        self.assertEqual(uri_config.geturl(), uri_expected.geturl())
        self.assertNotIn(":", uri_config.path, "Check for URI concatenated with normal path")

    def testPutGet(self) -> None:
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataNoComponents")
        self.runPutGetTest(storageClass, "test_metric")


class ButlerMakeRepoOutfileDirTestCase(ButlerMakeRepoOutfileTestCase):
    """Test that a config file created by makeRepo outside of repo works."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)
        self.root2 = makeTestTempDir(TESTDIR)

        self.tmpConfigFile = self.root2
        Butler.makeRepo(self.root, config=Config(self.configFile), outfile=self.tmpConfigFile)

    def testConfigExistence(self) -> None:
        # Append the yaml file else Config constructor does not know the file
        # type.
        self.tmpConfigFile = os.path.join(self.tmpConfigFile, "butler.yaml")
        super().testConfigExistence()


class ButlerMakeRepoOutfileUriTestCase(ButlerMakeRepoOutfileTestCase):
    """Test that a config file created by makeRepo outside of repo works."""

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)
        self.root2 = makeTestTempDir(TESTDIR)

        self.tmpConfigFile = ResourcePath(os.path.join(self.root2, "something.yaml")).geturl()
        Butler.makeRepo(self.root, config=Config(self.configFile), outfile=self.tmpConfigFile)


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
class S3DatastoreButlerTestCase(FileDatastoreButlerTests, unittest.TestCase):
    """S3Datastore specialization of a butler; an S3 storage Datastore +
    a local in-memory SqlRegistry.
    """

    configFile = os.path.join(TESTDIR, "config/basic/butler-s3store.yaml")
    fullConfigKey = None
    validationCanFail = True

    bucketName = "anybucketname"
    """Name of the Bucket that will be used in the tests. The name is read from
    the config file used with the tests during set-up.
    """

    root = "butlerRoot/"
    """Root repository directory expected to be used in case useTempRoot=False.
    Otherwise the root is set to a 20 characters long randomly generated string
    during set-up.
    """

    datastoreStr = [f"datastore={root}"]
    """Contains all expected root locations in a format expected to be
    returned by Butler stringification.
    """

    datastoreName = ["FileDatastore@s3://{bucketName}/{root}"]
    """The expected format of the S3 Datastore string."""

    registryStr = "/gen3.sqlite3"
    """Expected format of the Registry string."""

    mock_aws = mock_aws()
    """The mocked s3 interface from moto."""

    def genRoot(self) -> str:
        """Return a random string of len 20 to serve as a root
        name for the temporary bucket repo.

        This is equivalent to tempfile.mkdtemp as this is what self.root
        becomes when useTempRoot is True.
        """
        rndstr = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))
        return rndstr + "/"

    def setUp(self) -> None:
        config = Config(self.configFile)
        uri = ResourcePath(config[".datastore.datastore.root"])
        self.bucketName = uri.netloc

        # Enable S3 mocking of tests.
        self.enterContext(clean_test_environment_for_s3())
        self.mock_aws.start()

        if self.useTempRoot:
            self.root = self.genRoot()
        rooturi = f"s3://{self.bucketName}/{self.root}"
        config.update({"datastore": {"datastore": {"root": rooturi}}})

        # need local folder to store registry database
        self.reg_dir = makeTestTempDir(TESTDIR)
        config["registry", "db"] = f"sqlite:///{self.reg_dir}/gen3.sqlite3"

        # MOTO needs to know that we expect Bucket bucketname to exist
        # (this used to be the class attribute bucketName)
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucketName)

        self.datastoreStr = [f"datastore='{rooturi}'"]
        self.datastoreName = [f"FileDatastore@{rooturi}"]
        Butler.makeRepo(rooturi, config=config, forceConfigRoot=False)
        self.tmpConfigFile = posixpath.join(rooturi, "butler.yaml")

    def tearDown(self) -> None:
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucketName)
        try:
            bucket.objects.all().delete()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # the key was not reachable - pass
                pass
            else:
                raise

        bucket = s3.Bucket(self.bucketName)
        bucket.delete()

        # Stop the S3 mock.
        self.mock_aws.stop()

        if self.reg_dir is not None and os.path.exists(self.reg_dir):
            shutil.rmtree(self.reg_dir, ignore_errors=True)

        if self.useTempRoot and os.path.exists(self.root):
            shutil.rmtree(self.root, ignore_errors=True)

        super().tearDown()


class PosixDatastoreTransfers(unittest.TestCase):
    """Test data transfers between butlers.

    Test for different managers. UUID to UUID and integer to integer are
    tested. UUID to integer is not supported since we do not currently
    want to allow that.  Integer to UUID is supported with the caveat
    that UUID4 will be generated and this will be incorrect for raw
    dataset types. The test ignores that.
    """

    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    storageClassFactory: StorageClassFactory

    @classmethod
    def setUpClass(cls) -> None:
        cls.storageClassFactory = StorageClassFactory()

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)
        self.config = Config(self.configFile)

        # Some tests cause convertors to be replaced so ensure
        # the storage class factory is reset each time.
        self.storageClassFactory.reset()
        self.storageClassFactory.addFromConfig(self.configFile)

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def create_butler(self, manager: str, label: str) -> Butler:
        config = Config(self.configFile)
        config["registry", "managers", "datasets"] = manager
        return Butler.from_config(
            Butler.makeRepo(f"{self.root}/butler{label}", config=config), writeable=True
        )

    def create_butlers(self, manager1: str | None = None, manager2: str | None = None) -> None:
        default = "lsst.daf.butler.registry.datasets.byDimensions.ByDimensionsDatasetRecordStorageManagerUUID"
        if manager1 is None:
            manager1 = default
        if manager2 is None:
            manager2 = default
        self.source_butler = self.create_butler(manager1, "1")
        self.target_butler = self.create_butler(manager2, "2")

    def testTransferUuidToUuid(self) -> None:
        self.create_butlers()
        self.assertButlerTransfers()

    def testTransferMissing(self) -> None:
        """Test transfers where datastore records are missing.

        This is how execution butler works.
        """
        self.create_butlers()

        # Configure the source butler to allow trust.
        self.source_butler._datastore._set_trust_mode(True)

        self.assertButlerTransfers(purge=True)

    def testTransferMissingDisassembly(self) -> None:
        """Test transfers where datastore records are missing.

        This is how execution butler works.
        """
        self.create_butlers()

        # Configure the source butler to allow trust.
        self.source_butler._datastore._set_trust_mode(True)

        # Test disassembly.
        self.assertButlerTransfers(purge=True, storageClassName="StructuredComposite")

    def testTransferDifferingStorageClasses(self) -> None:
        """Test transfers when the source butler dataset type has a different
        but compatible storage class.
        """
        self.create_butlers()

        self.assertButlerTransfers(storageClassNameTarget="MetricsConversion")

    def testTransferDifferingStorageClassesDisassembly(self) -> None:
        """Test transfers when the source butler dataset type has a different
        but compatible storage class and where the source butler has
        disassembled.
        """
        self.create_butlers()

        self.assertButlerTransfers(
            storageClassName="StructuredComposite", storageClassNameTarget="MetricsConversion"
        )

    def testAbsoluteURITransferDirect(self) -> None:
        """Test transfer using an absolute URI."""
        self._absolute_transfer("auto")

    def testAbsoluteURITransferCopy(self) -> None:
        """Test transfer using an absolute URI."""
        self._absolute_transfer("copy")

    def _absolute_transfer(self, transfer: str) -> None:
        self.create_butlers()

        storageClassName = "StructuredData"
        storageClass = self.storageClassFactory.getStorageClass(storageClassName)
        datasetTypeName = "random_data"
        run = "run1"
        self.source_butler.collections.register(run)

        dimensions = self.source_butler.dimensions.conform(())
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        self.source_butler.registry.registerDatasetType(datasetType)

        metrics = makeExampleMetrics()
        with ResourcePath.temporary_uri(suffix=".json") as temp:
            dataId = DataCoordinate.make_empty(self.source_butler.dimensions)
            source_refs = [DatasetRef(datasetType, dataId, run=run)]
            temp.write(json.dumps(metrics.exportAsDict()).encode())
            dataset = FileDataset(path=temp, refs=source_refs)
            self.source_butler.ingest(dataset, transfer="direct")

            self.target_butler.transfer_from(
                self.source_butler, dataset.refs, register_dataset_types=True, transfer=transfer
            )

            uri = self.target_butler.getURI(dataset.refs[0])
            if transfer == "auto":
                self.assertEqual(uri, temp)
            else:
                self.assertNotEqual(uri, temp)

    def assertButlerTransfers(
        self,
        purge: bool = False,
        storageClassName: str = "StructuredData",
        storageClassNameTarget: str | None = None,
    ) -> None:
        """Test that a run can be transferred to another butler."""
        storageClass = self.storageClassFactory.getStorageClass(storageClassName)
        if storageClassNameTarget is not None:
            storageClassTarget = self.storageClassFactory.getStorageClass(storageClassNameTarget)
        else:
            storageClassTarget = storageClass

        datasetTypeName = "random_data"

        # Test will create 3 collections and we will want to transfer
        # two of those three.
        runs = ["run1", "run2", "other"]

        # Also want to use two different dataset types to ensure that
        # grouping works.
        datasetTypeNames = ["random_data", "random_data_2"]

        # Create the run collections in the source butler.
        for run in runs:
            self.source_butler.collections.register(run)

        # Create dimensions in source butler.
        n_exposures = 30
        self.source_butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
        self.source_butler.registry.insertDimensionData(
            "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
        )
        self.source_butler.registry.insertDimensionData(
            "detector", {"instrument": "DummyCamComp", "id": 1, "full_name": "det1"}
        )
        self.source_butler.registry.insertDimensionData(
            "day_obs",
            {
                "instrument": "DummyCamComp",
                "id": 20250101,
            },
        )

        for i in range(n_exposures):
            self.source_butler.registry.insertDimensionData(
                "group", {"instrument": "DummyCamComp", "name": f"group{i}"}
            )
            self.source_butler.registry.insertDimensionData(
                "exposure",
                {
                    "instrument": "DummyCamComp",
                    "id": i,
                    "obs_id": f"exp{i}",
                    "physical_filter": "d-r",
                    "group": f"group{i}",
                    "day_obs": 20250101,
                },
            )

        # Create dataset types in the source butler.
        dimensions = self.source_butler.dimensions.conform(["instrument", "exposure"])
        for datasetTypeName in datasetTypeNames:
            datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
            self.source_butler.registry.registerDatasetType(datasetType)

        # Write a dataset to an unrelated run -- this will ensure that
        # we are rewriting integer dataset ids in the target if necessary.
        # Will not be relevant for UUID.
        run = "distraction"
        butler = Butler.from_config(butler=self.source_butler, run=run)
        butler.put(
            makeExampleMetrics(),
            datasetTypeName,
            exposure=1,
            instrument="DummyCamComp",
            physical_filter="d-r",
        )

        # Write some example metrics to the source
        butler = Butler.from_config(butler=self.source_butler)

        # Set of DatasetRefs that should be in the list of refs to transfer
        # but which will not be transferred.
        deleted: set[DatasetRef] = set()

        n_expected = 20  # Number of datasets expected to be transferred
        source_refs = []
        for i in range(n_exposures):
            # Put a third of datasets into each collection, only retain
            # two thirds.
            index = i % 3
            run = runs[index]
            datasetTypeName = datasetTypeNames[i % 2]

            metric = MetricsExample(
                summary={"counter": i}, output={"text": "metric"}, data=[2 * x for x in range(i)]
            )
            dataId = {"exposure": i, "instrument": "DummyCamComp", "physical_filter": "d-r"}
            ref = butler.put(metric, datasetTypeName, dataId=dataId, run=run)

            # Remove the datastore record using low-level API, but only
            # for a specific index.
            if purge and index == 1:
                # For one of these delete the file as well.
                # This allows the "missing" code to filter the
                # file out.
                # Access the individual datastores.
                datastores = []
                if hasattr(butler._datastore, "datastores"):
                    datastores.extend(butler._datastore.datastores)
                else:
                    datastores.append(butler._datastore)

                if not deleted:
                    # For a chained datastore we need to remove
                    # files in each chain.
                    for datastore in datastores:
                        # The file might not be known to the datastore
                        # if constraints are used.
                        try:
                            primary, uris = datastore.getURIs(ref)
                        except FileNotFoundError:
                            continue
                        if primary and primary.scheme != "mem":
                            primary.remove()
                        for uri in uris.values():
                            if uri.scheme != "mem":
                                uri.remove()
                    n_expected -= 1
                    deleted.add(ref)

                # Remove the datastore record.
                for datastore in datastores:
                    if hasattr(datastore, "removeStoredItemInfo"):
                        datastore.removeStoredItemInfo(ref)

            if index < 2:
                source_refs.append(ref)
            if ref not in deleted:
                new_metric = butler.get(ref)
                self.assertEqual(new_metric, metric)

        # Create some bad dataset types to ensure we check for inconsistent
        # definitions.
        badStorageClass = self.storageClassFactory.getStorageClass("StructuredDataList")
        for datasetTypeName in datasetTypeNames:
            datasetType = DatasetType(datasetTypeName, dimensions, badStorageClass)
            self.target_butler.registry.registerDatasetType(datasetType)
        with self.assertRaises(ConflictingDefinitionError) as cm:
            self.target_butler.transfer_from(self.source_butler, source_refs)
        self.assertIn("dataset type differs", str(cm.exception))

        # And remove the bad definitions.
        for datasetTypeName in datasetTypeNames:
            self.target_butler.registry.removeDatasetType(datasetTypeName)

        # Transfer without creating dataset types should fail.
        with self.assertRaises(KeyError):
            self.target_butler.transfer_from(self.source_butler, source_refs)

        # Transfer without creating dimensions should fail.
        with self.assertRaises(ConflictingDefinitionError) as cm:
            self.target_butler.transfer_from(self.source_butler, source_refs, register_dataset_types=True)
        self.assertIn("dimension", str(cm.exception))

        # The dry run test requires dataset types to exist. If we have
        # been given distinct storage classes for the target we have
        # to redefine at least one of the dataset types in the target butler.
        if storageClass != storageClassTarget:
            self.target_butler.registry.removeDatasetType(datasetTypeNames[0])
            datasetType = DatasetType(datasetTypeNames[0], dimensions, storageClassTarget)
            self.target_butler.registry.registerDatasetType(datasetType)

        # The failed transfer above leaves registry in an inconsistent
        # state because the run is created but then rolled back without
        # the collection cache being cleared. For now force a refresh.
        # Can remove with DM-35498.
        self.target_butler.registry.refresh()

        # Do a dry run -- this should not have any effect on the target butler.
        self.target_butler.transfer_from(self.source_butler, source_refs, dry_run=True)

        # Transfer the records for one ref to test the alternative API.
        with self.assertLogs(logger="lsst", level=logging.DEBUG) as log_cm:
            self.target_butler.transfer_dimension_records_from(self.source_butler, [source_refs[0]])
        self.assertIn("number of records transferred: 1", ";".join(log_cm.output))

        # Now transfer them to the second butler, including dimensions.
        with self.assertLogs(logger="lsst", level=logging.DEBUG) as log_cm:
            transferred = self.target_butler.transfer_from(
                self.source_butler,
                source_refs,
                register_dataset_types=True,
                transfer_dimensions=True,
            )
        self.assertEqual(len(transferred), n_expected)
        log_output = ";".join(log_cm.output)

        # A ChainedDatastore will use the in-memory datastore for mexists
        # so we can not rely on the mexists log message.
        self.assertIn("Number of datastore records found in source", log_output)
        self.assertIn("Creating output run", log_output)

        # Do the transfer twice to ensure that it will do nothing extra.
        # Only do this if purge=True because it does not work for int
        # dataset_id.
        if purge:
            # This should not need to register dataset types.
            transferred = self.target_butler.transfer_from(self.source_butler, source_refs)
            self.assertEqual(len(transferred), n_expected)

            # Also do an explicit low-level transfer to trigger some
            # edge cases.
            with self.assertLogs(level=logging.DEBUG) as log_cm:
                self.target_butler._datastore.transfer_from(self.source_butler._datastore, source_refs)
            log_output = ";".join(log_cm.output)
            self.assertIn("no file artifacts exist", log_output)

            with self.assertRaises((TypeError, AttributeError)):
                self.target_butler._datastore.transfer_from(self.source_butler, source_refs)  # type: ignore

            with self.assertRaises(ValueError):
                self.target_butler._datastore.transfer_from(
                    self.source_butler._datastore, source_refs, transfer="split"
                )

        # Now try to get the same refs from the new butler.
        for ref in source_refs:
            if ref not in deleted:
                new_metric = self.target_butler.get(ref)
                old_metric = self.source_butler.get(ref)
                self.assertEqual(new_metric, old_metric)

                # Try again without implicit storage class conversion
                # triggered by using the source ref. This will do conversion
                # since the formatter will be returning the source python type.
                target_ref = self.target_butler.get_dataset(ref.id)
                if target_ref.datasetType.storageClass != ref.datasetType.storageClass:
                    new_metric = self.target_butler.get(target_ref)
                    self.assertNotEqual(type(new_metric), type(old_metric))

                    # Remove the dataset from the target and put it again
                    # as if it was the right type all along for this butler.
                    self.target_butler.pruneDatasets(
                        [target_ref], unstore=True, purge=True, disassociate=True
                    )
                    self.target_butler.put(new_metric, target_ref)
                    new_new_metric = self.target_butler.get(target_ref)
                    new_old_metric = self.target_butler.get(
                        target_ref, storageClass=ref.datasetType.storageClass
                    )
                    self.assertEqual(new_new_metric, new_metric)
                    self.assertEqual(new_old_metric, old_metric)

        # Now prune run2 collection and create instead a CHAINED collection.
        # This should block the transfer.
        self.target_butler.removeRuns(["run2"], unstore=True)
        self.target_butler.collections.register("run2", CollectionType.CHAINED)
        with self.assertRaises(CollectionTypeError):
            # Re-importing the run1 datasets can be problematic if they
            # use integer IDs so filter those out.
            to_transfer = [ref for ref in source_refs if ref.run == "run2"]
            self.target_butler.transfer_from(self.source_butler, to_transfer)


class ChainedDatastoreTransfers(PosixDatastoreTransfers):
    """Test transfers using a chained datastore."""

    configFile = os.path.join(TESTDIR, "config/basic/butler-chained.yaml")


class NullDatastoreTestCase(unittest.TestCase):
    """Test that we can fall back to a null datastore."""

    # Need a good config to create the repo.
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    storageClassFactory: StorageClassFactory

    @classmethod
    def setUpClass(cls) -> None:
        cls.storageClassFactory = StorageClassFactory()
        cls.storageClassFactory.addFromConfig(cls.configFile)

    def setUp(self) -> None:
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        Butler.makeRepo(self.root, config=Config(self.configFile))

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def test_fallback(self) -> None:
        # Read the butler config and mess with the datastore section.
        config_path = os.path.join(self.root, "butler.yaml")
        bad_config = Config(config_path)
        bad_config["datastore", "cls"] = "lsst.not.a.datastore.Datastore"
        bad_config.dumpToUri(config_path)

        with self.assertRaises(RuntimeError):
            Butler(self.root, without_datastore=False)

        with self.assertRaises(RuntimeError):
            Butler.from_config(self.root, without_datastore=False)

        butler = Butler.from_config(self.root, writeable=True, without_datastore=True)
        self.assertIsInstance(butler._datastore, NullDatastore)

        # Check that registry is working.
        butler.collections.register("MYRUN")
        collections = butler.collections.query("*")
        self.assertIn("MYRUN", set(collections))

        # Create a ref.
        dimensions = butler.dimensions.conform([])
        storageClass = self.storageClassFactory.getStorageClass("StructuredDataDict")
        datasetTypeName = "metric"
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        butler.registry.registerDatasetType(datasetType)
        ref = DatasetRef(datasetType, {}, run="MYRUN")

        # Check that datastore will complain.
        with self.assertRaises(FileNotFoundError):
            butler.get(ref)
        with self.assertRaises(FileNotFoundError):
            butler.getURI(ref)


@unittest.skipIf(create_test_server is None, "Server dependencies not installed.")
class ButlerServerTests(FileDatastoreButlerTests):
    """Test RemoteButler and Butler server."""

    configFile = None
    predictionSupported = False
    trustModeSupported = False

    postgres: TemporaryPostgresInstance | None

    def setUp(self):
        self.server_instance = self.enterContext(create_test_server(TESTDIR))

    def tearDown(self):
        pass

    def are_uris_equivalent(self, uri1: ResourcePath, uri2: ResourcePath) -> bool:
        # S3 pre-signed URLs may end up with differing expiration times in the
        # query parameters, so ignore query parameters when comparing.
        return uri1.scheme == uri2.scheme and uri1.netloc == uri2.netloc and uri1.path == uri2.path

    def create_empty_butler(self, run: str | None = None, writeable: bool | None = None) -> Butler:
        return self.server_instance.hybrid_butler.clone(run=run)

    def remove_dataset_out_of_band(self, butler: Butler, ref: DatasetRef) -> None:
        # Can't delete a file via S3 signed URLs, so we need to reach in
        # through DirectButler to delete the dataset.
        uri = self.server_instance.direct_butler.getURI(ref)
        uri.remove()

    def testConstructor(self):
        # RemoteButler constructor is tested in test_server.py and
        # test_remote_butler.py.
        pass

    def testDafButlerRepositories(self):
        # Loading of RemoteButler via repository index is tested in
        # test_server.py.
        pass

    def testGetDatasetTypes(self) -> None:
        # This is mostly a test of validateConfiguration, which is for
        # validating Datastore configuration and thus isn't relevant to
        # RemoteButler.
        pass

    def testMakeRepo(self) -> None:
        # Only applies to DirectButler.
        pass

    # Pickling not yet implemented for RemoteButler/HybridButler.
    @unittest.expectedFailure
    def testPickle(self) -> None:
        return super().testPickle()

    def testStringification(self) -> None:
        self.assertEqual(
            str(self.server_instance.remote_butler),
            "RemoteButler(https://test.example/api/butler/repo/testrepo)",
        )

    def testTransaction(self) -> None:
        # Transactions will never be supported for RemoteButler.
        pass

    def testPutTemplates(self) -> None:
        # The Butler server instance is configured with different file naming
        # templates than this test is expecting.
        pass


@unittest.skipIf(create_test_server is None, "Server dependencies not installed.")
class ButlerServerSqliteTests(ButlerServerTests, unittest.TestCase):
    """Tests for RemoteButler's registry shim, with a SQLite DB backing the
    server.
    """

    postgres = None


@unittest.skipIf(create_test_server is None, "Server dependencies not installed.")
class ButlerServerPostgresTests(ButlerServerTests, unittest.TestCase):
    """Tests for RemoteButler's registry shim, with a Postgres DB backing the
    server.
    """

    @classmethod
    def setUpClass(cls):
        cls.postgres = cls.enterClassContext(setup_postgres_test_db())
        super().setUpClass()


def setup_module(module: types.ModuleType) -> None:
    """Set up the module for pytest."""
    clean_environment()


if __name__ == "__main__":
    clean_environment()
    unittest.main()
