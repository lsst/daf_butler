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

"""Tests for Butler."""

from __future__ import annotations

import os
import pathlib
import pickle
import tempfile
import unittest
import unittest.mock
import uuid
import warnings
import weakref
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

import astropy.time
from butler_test_support import assert_get_components, run_put_get_test

from lsst.daf.butler import (
    Butler,
    ButlerConfig,
    ButlerMetrics,
    ButlerRepoIndex,
    Config,
    DataCoordinate,
    DatasetProvenance,
    DatasetRef,
    DatasetType,
    DimensionRecord,
    FileDataset,
    StorageClassFactory,
)
from lsst.daf.butler._rubin.file_datasets import transfer_datasets_to_datastore
from lsst.daf.butler._rubin.temporary_for_ingest import TemporaryForIngest
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.registry import (
    ConflictingDefinitionError,
    DataIdValueError,
)
from lsst.daf.butler.registry.sql_registry import SqlRegistry
from lsst.daf.butler.repo_relocation import BUTLER_ROOT_TAG
from lsst.daf.butler.tests import MetricsExample, MetricsExampleModel, MultiDetectorFormatter
from lsst.daf.butler.tests._repo_template_cache import make_repo_for_test
from lsst.daf.butler.tests.postgresql import TemporaryPostgresInstance, setup_postgres_test_db
from lsst.daf.butler.tests.server_available import butler_server_import_error, butler_server_is_available
from lsst.daf.butler.tests.utils import (
    MetricTestRepo,
    TestCaseMixin,
    create_populated_sqlite_registry,
    makeTestTempDir,
    removeTestTempDir,
)
from lsst.resources import ResourcePath
from lsst.resources.tests import make_remote_test_uri
from lsst.utils import doImportType
from lsst.utils.introspection import get_full_type_name

if butler_server_is_available:
    from lsst.daf.butler.tests.server import create_test_server


if TYPE_CHECKING:
    import types

    from lsst.daf.butler import DimensionGroup, Registry, StorageClass
    from lsst.daf.butler.tests.fixtures import ButlerHarness

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
        assert_get_components(butler, datasetRef, components, reference, collections=collections)

    def tearDown(self) -> None:
        if self.root is not None:
            removeTestTempDir(self.root)

    def create_empty_butler(
        self,
        run: str | None = None,
        writeable: bool | None = None,
        metrics: ButlerMetrics | None = None,
        cleanup: bool = True,
    ):
        """Create a Butler for the test repository, without inserting test
        data.
        """
        butler = Butler.from_config(self.tmpConfigFile, run=run, writeable=writeable, metrics=metrics)
        if cleanup:
            self.enterContext(butler)
        assert isinstance(butler, DirectButler), "Expect DirectButler in configuration"
        return butler

    def create_butler(
        self,
        run: str,
        storageClass: StorageClass | str,
        datasetTypeName: str,
        metrics: ButlerMetrics | None = None,
    ) -> tuple[Butler, DatasetType]:
        """Create a Butler for the test repository and insert some test data
        into it.
        """
        butler = self.create_empty_butler(run=run, metrics=metrics)

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

    @property
    def storage_class_factory(self) -> StorageClassFactory:
        """Name the shared put/get helper expects for the storage classes."""
        return self.storageClassFactory

    def runPutGetTest(self, storageClass: StorageClass, datasetTypeName: str) -> Butler:
        # The classes left in this file still reach the helper through self,
        # which supplies the parts of ButlerHarness that the helper uses.
        return run_put_get_test(cast("ButlerHarness", self), storageClass, datasetTypeName)


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
        make_repo_for_test(self.root, config=Config(self.configFile))
        self.tmpConfigFile = os.path.join(self.root, "butler.yaml")

    def are_uris_equivalent(self, uri1: ResourcePath, uri2: ResourcePath) -> bool:
        """Return True if two URIs refer to the same resource.

        Subclasses may override to handle unique requirements.
        """
        return uri1 == uri2

    def testConstructor(self) -> None:
        """Independent test of constructor."""
        butler = Butler.from_config(self.tmpConfigFile, run=self.default_run)
        self.enterContext(butler)
        self.assertIsInstance(butler, Butler)

        # Check that butler.yaml is added automatically.
        if self.tmpConfigFile.endswith(end := "/butler.yaml"):
            config_dir = self.tmpConfigFile[: -len(end)]
            butler = Butler.from_config(config_dir, run=self.default_run)
            self.enterContext(butler)
            self.assertIsInstance(butler, Butler)

            # Even with a ResourcePath.
            butler = Butler.from_config(ResourcePath(config_dir, forceDirectory=True), run=self.default_run)
            self.enterContext(butler)
            self.assertIsInstance(butler, Butler)

        collections = set(butler.collections.query("*"))
        self.assertEqual(collections, {self.default_run})

        # Check that some special characters can be included in run name.
        special_run = "u@b.c-A"
        butler_special = Butler.from_config(butler=butler, run=special_run)
        self.enterContext(butler_special)
        collections = set(butler_special.registry.queryCollections("*@*"))
        self.assertEqual(collections, {special_run})

        butler2 = Butler.from_config(butler=butler, collections=["other"])
        self.enterContext(butler2)
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
                    butler.close()
                    butler = Butler.from_config("label", writeable=False)
                    self.assertIsInstance(butler, Butler)
                    butler.close()
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
            self.enterContext(butler)
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

    def testClose(self):
        butler = self.create_empty_butler(cleanup=False)
        is_direct_butler = isinstance(butler, DirectButler)
        if is_direct_butler:
            self.assertFalse(butler._closed)

        with butler as butler_from_context_manager:
            self.assertIs(butler, butler_from_context_manager)
        if is_direct_butler:
            self.assertTrue(butler._closed)
            with self.assertRaisesRegex(RuntimeError, "has been closed"):
                butler.get_dataset_type("raw")

        # Close may be called multiple times.
        butler.close()
        if is_direct_butler:
            self.assertTrue(butler._closed)

    def testGarbageCollection(self):
        """Test that Butler does not have any circular references that prevent
        it from being garbage collected immediately when it goes out of scope.
        """
        butler = self.create_empty_butler(cleanup=False)
        is_direct_butler = isinstance(butler, DirectButler)
        butler_ref = weakref.ref(butler)
        if is_direct_butler:
            registry_ref = weakref.ref(butler._registry)
            managers_ref = weakref.ref(butler._registry._managers)
            datastore_ref = weakref.ref(butler._datastore)
            db_ref = weakref.ref(butler._registry._db)
            engine_ref = weakref.ref(butler._registry._db._engine)

        with warnings.catch_warnings():
            # Hide warnings from unclosed database handles.
            warnings.simplefilter("ignore", ResourceWarning)
            del butler
            self.assertIsNone(butler_ref(), "Butler should have been garbage collected")
            if is_direct_butler:
                self.assertIsNone(registry_ref(), "SqlRegistry should have been garbage collected")
                self.assertIsNone(managers_ref(), "Registry managers should have been garbage collected")
                self.assertIsNone(datastore_ref(), "Datastore should have been garbage collected")
                self.assertIsNone(db_ref(), "Database should have been garbage collected")
            # SQLAlchemy has internal reference cycles, so the Engine instance
            # is not cleaned up promptly even if we release our reference to
            # it.  Explicitly clean it up here to avoid file handles leaking.
            if is_direct_butler:
                engine = engine_ref()
                if engine is not None:
                    engine.dispose()

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

    def test_ingest_zip(self) -> None:
        """Create butler, export data, delete data, import from Zip."""
        butler, dataset_type = self.create_butler(
            run=self.default_run, storageClass="StructuredData", datasetTypeName="metrics"
        )

        metric = makeExampleMetrics()
        refs = []
        for visit in (423, 424, 425):
            ref = butler.put(metric, dataset_type, instrument="DummyCamComp", visit=visit)
            refs.append(ref)

        # Retrieve a Zip file.
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            zip = butler.retrieve_artifacts_zip(refs, destination=tmpdir)

            # Ingest will fail.
            with self.assertRaises(ConflictingDefinitionError):
                butler.ingest_zip(zip)

            # Clear out the collection.
            butler.removeRuns([self.default_run])
            self.assertFalse(butler.exists(refs[0]))

            butler.ingest_zip(zip, transfer="copy")
            self.assertGreater(butler._metrics.time_in_ingest, 0.0)
            self.assertEqual(butler._metrics.n_ingest, len(refs))

            # Check that it fails if we try it again.
            with self.assertRaises(ConflictingDefinitionError):
                butler.ingest_zip(zip, transfer="copy")

            # This will be a no-op.
            butler.ingest_zip(zip, transfer="copy", skip_existing=True)

            # Create an entirely new local file butler in this temp directory.
            new_butler_cfg = make_repo_for_test(tmpdir)
            new_butler = Butler.from_config(new_butler_cfg, writeable=True)
            self.enterContext(new_butler)

            # This will fail since dimensions records are missing.
            with self.assertRaises(ConflictingDefinitionError):
                new_butler.ingest_zip(zip, transfer="copy")

            # Dry run should work.
            new_butler.ingest_zip(zip, transfer="copy", dry_run=True)

            new_butler.ingest_zip(zip, transfer="copy", transfer_dimensions=True)
            self.assertTrue(butler.exists(refs[0]))

        # Check that the refs can be read again.
        _ = [butler.get(ref) for ref in refs]

        uri = butler.getURI(refs[2])
        self.assertTrue(uri.exists())

        # Delete one dataset. The Zip file should still exist and allow
        # remaining refs to be read.
        butler.pruneDatasets([refs[0]], purge=True, unstore=True)
        self.assertTrue(uri.exists())

        metric2 = butler.get(refs[1])
        self.assertEqual(metric2, metric, msg=f"{metric2} != {metric}")

        butler.removeRuns([self.default_run])
        self.assertFalse(uri.exists())
        self.assertFalse(butler.exists(refs[-1]))

        with self.assertRaises(ValueError):
            butler.retrieve_artifacts_zip([], destination=".")

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
        # Test one DatasetRef with a run that exists, and the other with a run
        # that doesn't exist, to verify that run collections are created when
        # required.
        runs = {1: self.default_run, 2: "a/new/run"}
        for detector in (1, 2):
            detector_name = f"detector_{detector}"
            metricFile = os.path.join(dataRoot, f"{detector_name}.yaml")
            dataId = butler.registry.expandDataId(
                {"instrument": "DummyCamComp", "visit": 423, "detector": detector}
            )
            # Create a DatasetRef for ingest
            refIn = DatasetRef(datasetType, dataId, run=runs[detector])

            datasets.append(FileDataset(path=metricFile, refs=[refIn], formatter=formatter))

        butler.ingest(*datasets, transfer="copy")

        dataId1 = {"instrument": "DummyCamComp", "detector": 1, "visit": 423}
        dataId2 = {"instrument": "DummyCamComp", "detector": 2, "visit": 423}

        metrics1 = butler.get(datasetTypeName, dataId1)
        metrics2 = butler.get(datasetTypeName, dataId2, collections="a/new/run")
        self.assertNotEqual(metrics1, metrics2)

        # Compare URIs
        uri1 = butler.getURI(datasetTypeName, dataId1)
        uri2 = butler.getURI(datasetTypeName, dataId2, collections="a/new/run")
        self.assertFalse(self.are_uris_equivalent(uri1, uri2), f"Cf. {uri1} with {uri2}")

        # Re-ingesting the same datasets raises an error with
        # skip_existing=False.
        with self.assertRaises(ConflictingDefinitionError):
            butler.ingest(*datasets, transfer="copy")
        # skip_existing=True makes it a no-op to re-ingest the same datasets.
        butler.ingest(*datasets, transfer="copy", skip_existing=True)

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
        self.enterContext(butlerOut)
        self.assertIsInstance(butlerOut, Butler)
        self.assertEqual(butlerOut._config, butler._config)
        self.assertEqual(list(butlerOut.collections.defaults), list(butler.collections.defaults))
        self.assertEqual(butlerOut.run, butler.run)

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

    def testStringification(self) -> None:
        butler = Butler.from_config(self.tmpConfigFile, run=self.default_run)
        self.enterContext(butler)
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
                where="day_obs = :dayObs AND instrument = :instr",
                bind={"dayObs": dayobs, "instr": "DummyCamComp"},
            )
        )
        datasets_2 = list(
            butler.registry.queryDatasets(
                datasetType,
                collections=self.default_run,
                where="exposure.day_obs = :dayObs AND instrument = :instr",
                bind={"dayObs": dayobs, "instr": "DummyCamComp"},
            )
        )
        self.assertEqual(datasets_1, datasets_2)

    def test_transfer_dimension_records_from(self) -> None:
        source_butler = self.create_empty_butler(writeable=True)
        source_butler.import_(filename=_get_test_data_path("lsstcam-subset.yaml"))

        visit_id = 2025120200439
        exposure_id = visit_id
        target_butler = self.enterContext(create_populated_sqlite_registry())
        target_butler.transfer_dimension_records_from(
            source_butler,
            [
                # Should trigger the lookup of visit and all its associated
                # "populated_by" records (visit_detector_region,
                # visit_definition, etc.)
                DataCoordinate.standardize(
                    {"instrument": "LSSTCam", "visit": visit_id, "detector": 10},
                    universe=source_butler.dimensions,
                ),
                # Shouldn't add any records to the lookup.
                DataCoordinate.make_empty(source_butler.dimensions),
            ],
        )

        def _fetch_record(dimension: str) -> DimensionRecord:
            records = target_butler.query_dimension_records(dimension)
            self.assertEqual(len(records), 1)
            return records[0]

        visit = _fetch_record("visit")
        self.assertEqual(visit.id, visit_id)
        self.assertEqual(visit.day_obs, 20251202)
        self.assertEqual(visit.target_name, "lowdust")
        self.assertEqual(visit.seq_num, 439)
        original_visit = source_butler.query_dimension_records("visit", instrument="LSSTCam", visit=visit_id)[
            0
        ]
        self.assertEqual(visit.region, original_visit.region)
        self.assertEqual(visit.timespan, original_visit.timespan)

        visit_detector_region = _fetch_record("visit_detector_region")
        self.assertEqual(visit_detector_region.instrument, "LSSTCam")
        self.assertEqual(visit_detector_region.detector, 10)
        self.assertEqual(visit_detector_region.visit, visit_id)
        original_visit_detector_region = source_butler.query_dimension_records(
            "visit_detector_region", instrument="LSSTCam", visit=visit_id, detector=10
        )[0]
        self.assertEqual(visit_detector_region.region, original_visit_detector_region.region)

        visit_definition = _fetch_record("visit_definition")
        self.assertEqual(visit_definition.instrument, "LSSTCam")
        self.assertEqual(visit_definition.exposure, 2025120200439)
        self.assertEqual(visit_definition.visit, visit_id)

        # The matching exposure record should have been pulled in via
        # visit -> visit_definition.
        exposure = _fetch_record("exposure")
        self.assertEqual(exposure.instrument, "LSSTCam")
        self.assertEqual(exposure.id, 2025120200439)
        self.assertEqual(exposure.obs_id, "MC_O_20251202_000439")
        original_exposure = source_butler.query_dimension_records(
            "exposure", instrument="LSSTCam", exposure=exposure_id
        )[0]
        self.assertEqual(exposure.timespan, original_exposure.timespan)

        group = _fetch_record("group")
        self.assertEqual(group.instrument, "LSSTCam")
        self.assertEqual(group.name, "2025-12-03T07:58:10.858")

        visit_system_memberships = target_butler.query_dimension_records("visit_system_membership")
        visit_system_memberships.sort(key=lambda record: record.visit_system)
        self.assertEqual(len(visit_system_memberships), 2)
        self.assertEqual(visit_system_memberships[0].visit_system, 0)
        self.assertEqual(visit_system_memberships[1].visit_system, 2)
        self.assertEqual(visit_system_memberships[0].visit, visit_id)
        self.assertEqual(visit_system_memberships[1].visit, visit_id)

        visit_systems = target_butler.query_dimension_records("visit_system")
        visit_systems.sort(key=lambda record: record.id)
        visit_system_memberships.sort(key=lambda record: record.visit_system)
        self.assertEqual(visit_systems[0].id, 0)
        self.assertEqual(visit_systems[1].id, 2)
        self.assertEqual(visit_systems[0].name, "one-to-one")
        self.assertEqual(visit_systems[1].name, "by-seq-start-end")


class FileDatastoreButlerTests(ButlerTests):
    """Common tests and specialization of ButlerTests for butlers backed
    by datastores that inherit from FileDatastore.
    """

    trustModeSupported = True

    def test_butler_metrics(self):
        """Test that metrics are collected."""
        run = "test_run"
        metrics = ButlerMetrics()
        butler, datasetType = self.create_butler(
            run, "MetricsExampleModelProvenance", "prov_metric", metrics=metrics
        )
        data = MetricsExampleModel(
            summary={"AM1": 5.2, "AM2": 30.6},
            output={"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
            data=[563, 234, 456.7, 752, 8, 9, 27],
        )

        data_ref = butler.put(data, datasetType, visit=424, instrument="DummyCamComp")
        butler.get(data_ref)
        butler.get(data_ref)
        self.assertEqual(metrics.n_get, 2)
        self.assertGreater(metrics.time_in_get, 0.0)
        self.assertEqual(metrics.n_put, 1)
        self.assertGreater(metrics.time_in_put, 0.0)

        deferred = butler.getDeferred(data_ref)
        deferred.get()
        self.assertEqual(metrics.n_get, 3)

        with butler.record_metrics() as new:
            data_ref_2 = butler.put(data, datasetType, visit=425, instrument="DummyCamComp")
            butler.get(data_ref)

            butler.pruneDatasets([data_ref, data_ref_2], purge=True, unstore=True)
            with ResourcePath.temporary_uri(suffix=".json") as tmpFile:
                tmpFile.write(data.model_dump_json().encode())
                refs = [
                    DatasetRef(datasetType, data_ref_2.dataId, run),
                    DatasetRef(datasetType, data_ref.dataId, run),
                ]
                datasets = [FileDataset(path=tmpFile, refs=refs)]
                butler.ingest(*datasets, transfer="copy")

        self.assertEqual(new.n_get, 1)
        self.assertEqual(new.n_put, 1)
        self.assertEqual(new.n_ingest, 2)


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
        self.enterContext(butler)
        self.assertIsInstance(butler, Butler)

        # And again with a Path object with the butler yaml
        path = pathlib.Path(self.tmpConfigFile)
        butler = Butler.from_config(path, writeable=False)
        self.enterContext(butler)
        self.assertIsInstance(butler, Butler)

        # And again with a Path object without the butler yaml
        # (making sure we skip it if the tmp config doesn't end
        # in butler.yaml -- which is the case for a subclass)
        if self.tmpConfigFile.endswith("butler.yaml"):
            path = pathlib.Path(os.path.dirname(self.tmpConfigFile))
            butler = Butler.from_config(path, writeable=False)
            self.enterContext(butler)
            self.assertIsInstance(butler, Butler)

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

    def test_provenance(self):
        """Test that provenance is attached on put."""
        run = "test_run"
        butler, datasetType = self.create_butler(run, "MetricsExampleModelProvenance", "prov_metric")
        metric = MetricsExampleModel(
            summary={"AM1": 5.2, "AM2": 30.6},
            output={"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
            data=[563, 234, 456.7, 752, 8, 9, 27],
        )
        # Provenance can be attached to the object being put. Whether
        # it is or not is dependent on the formatter. For this test we
        # copy on adding provenance to ensure they differ.
        self.assertIsNone(metric.dataset_id)
        metric_ref = butler.put(metric, datasetType, visit=424, instrument="DummyCamComp")
        self.assertIsNone(metric.dataset_id)
        metric_2 = butler.get(metric_ref)
        self.assertEqual(metric_2.data, metric.data)
        self.assertEqual(metric_2.dataset_id, metric_ref.id)
        self.assertIsNone(metric_2.provenance)

        # Put with provenance.
        prov = DatasetProvenance(quantum_id=uuid.uuid4())
        prov.add_input(metric_ref)
        prov.add_extra_provenance(metric_ref.id, {"answer": 42})
        metric_ref2 = butler.put(metric, datasetType, visit=423, instrument="DummyCamComp", provenance=prov)
        metric_3 = butler.get(metric_ref2)
        self.assertEqual(metric_3.provenance, prov)

        # Check that we can extract provenance from dict form.
        prov_dict = prov.to_flat_dict(metric_ref2)
        prov_from_prov, ref_from_prov = DatasetProvenance.from_flat_dict(prov_dict, butler)
        self.assertEqual(ref_from_prov, metric_ref2)
        # Direct __eq__ of the provenance does not work because one side
        # includes dimension records.
        self.assertEqual({ref.id for ref in prov_from_prov.inputs}, {ref.id for ref in prov.inputs})
        self.assertEqual(prov_from_prov.quantum_id, prov.quantum_id)
        self.assertEqual(prov_from_prov.extras, prov.extras)

        # Force a bad ID into the dict.
        prov_dict["id"] = uuid.uuid4()
        with self.assertRaises(ValueError):
            DatasetProvenance.from_flat_dict(prov_dict, butler)
        del prov_dict["id"]
        prov_dict["input 0 id"] = uuid.uuid4()
        with self.assertRaises(ValueError):
            DatasetProvenance.from_flat_dict(prov_dict, butler)

        # Check that simple types can be reconstructed with non-standard
        # separators.
        prov_dict = prov.to_flat_dict(metric_ref2, prefix="XYZ", sep="😎", simple_types=True)
        prov_from_prov, ref_from_prov = DatasetProvenance.from_flat_dict(prov_dict, butler)
        self.assertEqual(ref_from_prov, metric_ref2)
        self.assertEqual({ref.id for ref in prov_from_prov.inputs}, {ref.id for ref in prov.inputs})

        with self.assertRaises(ValueError):
            DatasetProvenance.from_flat_dict({"unknown": 42}, butler)

    def test_specialized_file_datasets_functions(self):
        """Test a workflow used in Prompt Processing where we export datasets
        from one repository and write them in-place to the datastore of
        another, without immediately inserting registry entries for the
        datasets.
        """
        repo = MetricTestRepo.create_from_butler(
            self.create_empty_butler(writeable=True),
            self.tmpConfigFile,
            "StructuredCompositeReadCompNoDisassembly",
        )
        source_butler = repo.butler

        # Test writing outputs to a FileDatastore.
        with tempfile.TemporaryDirectory() as tempdir:
            target_repo_config = make_repo_for_test(tempdir)
            refs = [repo.ref1, repo.ref2]
            datasets = transfer_datasets_to_datastore(source_butler, ButlerConfig(target_repo_config), refs)
            self.assertEqual(len(datasets), 2)
            self.assertEqual({ref.id for ref in refs}, {dataset.refs[0].id for dataset in datasets})
            for dataset in datasets:
                path = ResourcePath(dataset.path, forceAbsolute=False)
                # Paths should be relative paths to the target datastore.
                self.assertFalse(path.isabs())
                # Files should have been copied into the target datastore
                self.assertTrue(ResourcePath(tempdir).join(path).exists())

            # Make sure the target Butler can ingest the datasets.
            target_butler = Butler(target_repo_config, writeable=True)
            self.enterContext(target_butler)
            target_butler.transfer_dimension_records_from(source_butler, refs)
            target_butler.ingest(*datasets, transfer=None)
            self.assertIsNotNone(target_butler.get(repo.ref1))
            self.assertIsNotNone(target_butler.get(repo.ref2))

            # Giving an empty list of files is a no-op.
            no_datasets = transfer_datasets_to_datastore(source_butler, ButlerConfig(target_repo_config), [])
            self.assertEqual(len(no_datasets), 0)

        # Test writing outputs to a ChainedDatastore.
        with tempfile.TemporaryDirectory() as tempdir:
            # Set up a second dataset type, so we can split the files across
            # multiple datastore roots.
            dt1 = repo.datasetType
            dt2 = DatasetType("other", dt1.dimensions, dt1.storageClass)
            source_butler.registry.registerDatasetType(dt2)
            other_ref = repo.addDataset(repo.ref1.dataId, datasetType=dt2)
            config = Config.fromString(
                f"""
            datastore:
                cls: lsst.daf.butler.datastores.chainedDatastore.ChainedDatastore
                datastore_constraints:
                  - constraints:
                      accept:
                        - {dt1.name}
                  - constraints:
                      accept:
                        - {dt2.name}
                datastores:
                  - datastore:
                      cls: lsst.daf.butler.datastores.fileDatastore.FileDatastore
                      root: <butlerRoot>/FileDatastore_0
                  - datastore:
                      cls: lsst.daf.butler.datastores.fileDatastore.FileDatastore
                      root: <butlerRoot>/FileDatastore_1
            """
            )
            target_repo_config = make_repo_for_test(tempdir, config)
            refs = [repo.ref1, repo.ref2, other_ref]
            datasets = transfer_datasets_to_datastore(source_butler, ButlerConfig(target_repo_config), refs)
            self.assertEqual(len(datasets), 3)
            self.assertEqual({ref.id for ref in refs}, {dataset.refs[0].id for dataset in datasets})
            for dataset in datasets:
                path = ResourcePath(dataset.path, forceAbsolute=False)
                # Paths should be relative paths to the target datastore.
                self.assertFalse(path.isabs())
                # Files should have been split up between the two datastores
                # in the chain.
                datastore_root = ResourcePath(tempdir)
                if dataset.refs[0].datasetType.name == dt1.name:
                    datastore_root = datastore_root.join("FileDatastore_0")
                else:
                    datastore_root = datastore_root.join("FileDatastore_1")
                self.assertTrue(datastore_root.join(path).exists())

            # Make sure the target Butler can ingest the datasets.
            target_butler = Butler(target_repo_config, writeable=True)
            self.enterContext(target_butler)
            target_butler.transfer_dimension_records_from(source_butler, refs)
            target_butler.ingest(*datasets, transfer=None)
            self.assertIsNotNone(target_butler.get(repo.ref1))
            self.assertIsNotNone(target_butler.get(repo.ref2))
            self.assertIsNotNone(target_butler.get(other_ref))

    def test_temporary_for_ingest(self) -> None:
        """Test the `lsst.daf.butler._rubin.ingest_from_temporary` module."""
        with self.create_empty_butler("example_run") as butler:
            dataset_type = DatasetType("example", butler.dimensions.empty, "StructuredDataDict")
            butler.registry.registerDatasetType(dataset_type)
            ref = DatasetRef(dataset_type, DataCoordinate.make_empty(butler.dimensions), "example_run")
            with TemporaryForIngest(butler, ref) as temporary:
                temporary.path.write(b"three: 3")
                found = TemporaryForIngest.find_orphaned_temporaries_by_ref(ref, butler)
                self.assertEqual(found, [temporary.path])
                self.assertIn(".tmp", temporary.ospath)
                temporary.ingest()
            loaded = butler.get(ref)
            self.assertEqual(loaded, {"three": 3})


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


class ClonedPostgresPosixDatastoreButlerTestCase(PostgresPosixDatastoreButlerTestCase, unittest.TestCase):
    """Test that Butler with a Postgres registry still works after cloning."""

    def create_butler(
        self,
        run: str,
        storageClass: StorageClass | str,
        datasetTypeName: str,
        metrics: ButlerMetrics | None = None,
    ) -> tuple[DirectButler, DatasetType]:
        butler, datasetType = super().create_butler(run, storageClass, datasetTypeName, metrics=metrics)
        return butler.clone(run=run, metrics=metrics), datasetType


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

    def test_ingest_zip(self) -> None:
        pass


class ClonedSqliteButlerTestCase(InMemoryDatastoreButlerTestCase, unittest.TestCase):
    """Test that a Butler with a Sqlite registry still works after cloning."""

    def create_butler(
        self,
        run: str,
        storageClass: StorageClass | str,
        datasetTypeName: str,
        metrics: ButlerMetrics | None = None,
    ) -> tuple[DirectButler, DatasetType]:
        butler, datasetType = super().create_butler(run, storageClass, datasetTypeName, metrics=metrics)
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
        make_repo_for_test(self.dir1, config=Config(self.configFile))

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


class RemoteTestDatastoreButlerTestCase(FileDatastoreButlerTests, unittest.TestCase):
    """Specialization of a butler using a datastore root that reports itself
    as not local; a remote file datastore + a local SqlRegistry.
    """

    configFile = os.path.join(TESTDIR, "config/basic/butler-remotetest-store.yaml")
    fullConfigKey = None
    validationCanFail = True

    registryStr = "/gen3.sqlite3"
    """Expected format of the Registry string."""

    def setUp(self) -> None:
        config = Config(self.configFile)

        self.root = makeTestTempDir(TESTDIR)
        # The space in the directory name is deliberate. It ensures the URI
        # has to be percent-encoded correctly on the way in and decoded on
        # the way out.
        root_path = os.path.join(self.root, "butler root")
        os.makedirs(root_path)
        rooturi = make_remote_test_uri(root_path)
        config.update({"datastore": {"datastore": {"root": str(rooturi)}}})

        # The registry database has to live on a real local file system.
        self.reg_dir = makeTestTempDir(TESTDIR)
        config["registry", "db"] = f"sqlite:///{self.reg_dir}/gen3.sqlite3"

        self.datastoreStr = [f"datastore='{rooturi}'"]
        self.datastoreName = [f"FileDatastore@{rooturi}"]
        make_repo_for_test(rooturi, config=config, forceConfigRoot=False)
        self.tmpConfigFile = str(rooturi.join("butler.yaml", forceDirectory=False))

    def tearDown(self) -> None:
        removeTestTempDir(self.reg_dir)
        # The base class removes self.root, which contains the datastore.
        super().tearDown()


@unittest.skipIf(not butler_server_is_available, butler_server_import_error)
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

    def create_empty_butler(
        self,
        run: str | None = None,
        writeable: bool | None = None,
        metrics: ButlerMetrics | None = None,
        cleanup: bool = True,
    ) -> Butler:
        return self.server_instance.hybrid_butler.clone(run=run, metrics=metrics)

    def testConstructor(self):
        # RemoteButler constructor is tested in test_server.py and
        # test_remote_butler.py.
        pass

    def testDafButlerRepositories(self):
        # Loading of RemoteButler via repository index is tested in
        # test_server.py.
        pass

    # Pickling not yet implemented for RemoteButler/HybridButler.
    @unittest.expectedFailure
    def testPickle(self) -> None:
        return super().testPickle()

    def testStringification(self) -> None:
        self.assertEqual(
            str(self.server_instance.remote_butler),
            "RemoteButler(https://test.example/api/butler/repo/testrepo/)",
        )

    def testTransaction(self) -> None:
        # Transactions will never be supported for RemoteButler.
        pass


@unittest.skipIf(not butler_server_is_available, butler_server_import_error)
class ButlerServerSqliteTests(ButlerServerTests, unittest.TestCase):
    """Tests for RemoteButler's registry shim, with a SQLite DB backing the
    server.
    """

    postgres = None


@unittest.skipIf(not butler_server_is_available, butler_server_import_error)
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


def _get_test_data_path(filename: str) -> ResourcePath:
    return ResourcePath(f"resource://lsst.daf.butler/tests/registry_data/{filename}")


if __name__ == "__main__":
    clean_environment()
    unittest.main()
