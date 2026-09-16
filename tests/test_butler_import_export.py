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

"""Tests for exporting datasets from a Butler and importing them back."""

from __future__ import annotations

import logging
import os
from typing import cast

import pytest
from butler_test_support import AXIS_NAMES, FILE_DATASTORE_AXES, records_from, run_put_get_test
from sqlalchemy.exc import IntegrityError

from lsst.daf.butler import (
    Butler,
    CollectionType,
    Config,
    DataCoordinate,
    DatasetExistence,
    DatasetRef,
    script,
)
from lsst.daf.butler.datastores.fileDatastore import FileDatastore
from lsst.daf.butler.registry import MissingCollectionError, OrphanedRecordError
from lsst.daf.butler.tests._repo_template_cache import make_repo_for_test
from lsst.daf.butler.tests.fixtures import (
    ButlerHarness,
    add_dataset_type,
    get_test_data_path,
    make_example_metrics,
)
from lsst.daf.butler.tests.utils import safeTestTempDir
from lsst.resources import ResourcePath

REGISTRY_LOGGER = "lsst.daf.butler.registry"
"""Logger that reports a dataset type expression matching nothing."""


def _check_file_exists(root: str | ResourcePath, relpath: str | ResourcePath) -> bool:
    """Return whether a file exists at a path relative to a root."""
    uri = ResourcePath(root, forceDirectory=True)
    return uri.join(relpath).exists()


def _run_import_export_test(
    butler_harness: ButlerHarness, storage_class_name: str, test_directory: str
) -> None:
    """Export a populated repository and import it into a fresh one.

    This exports to a temporary directory and imports back into a new
    temporary repository. It does not assume a posix datastore.

    Parameters
    ----------
    butler_harness : `~lsst.daf.butler.tests.fixtures.ButlerHarness`
        Harness supplying the Butler to export from.
    storage_class_name : `str`
        Storage class to populate the source repository with.
    test_directory : `str`
        Directory holding the Butler test configuration.
    """
    storage_class = butler_harness.storage_class_factory.getStorageClass(storage_class_name)
    export_butler = run_put_get_test(butler_harness, storage_class, "test_metric")

    # Test that we must have a file extension.
    with (
        pytest.raises(ValueError, match="Please specify a file extension"),
        export_butler.export(filename="dump", directory="."),
    ):
        pass

    # Test that unknown format is not allowed.
    with (
        pytest.raises(ValueError, match="Unknown export format"),
        export_butler.export(filename="dump.fits", directory="."),
    ):
        pass

    # Test that the repo actually has at least one dataset.
    datasets = list(export_butler.registry.queryDatasets(..., collections=...))
    assert len(datasets) > 0
    # Add a DimensionRecord that's unused by those datasets.
    skymap_record = {"name": "example_skymap", "hash": (50).to_bytes(8, byteorder="little")}
    export_butler.registry.insertDimensionData("skymap", skymap_record)
    # Export and then import datasets.
    with safeTestTempDir(test_directory) as export_dir:
        export_file = os.path.join(export_dir, "exports.yaml")
        with export_butler.export(filename=export_file, directory=export_dir, transfer="auto") as export:
            export.saveDatasets(datasets)
            # Export the same datasets again. This should quietly do nothing
            # because of internal deduplication, and it shouldn't complain
            # about being asked to export the "htm7" elements even though
            # there aren't any in these datasets or in the database.
            export.saveDatasets(datasets, elements=["htm7"])
            # Save one of the data IDs again; this should be harmless because
            # of internal deduplication.
            export.saveDataIds([datasets[0].dataId])
            # Save some dimension records directly.
            export.saveDimensionData("skymap", [skymap_record])
        assert os.path.exists(export_file)
        with safeTestTempDir(test_directory) as import_dir:
            # We always want this to be a local posix butler
            make_repo_for_test(
                import_dir, config=Config(os.path.join(test_directory, "config/basic/butler.yaml"))
            )
            # Calling script.butlerImport tests the implementation of the
            # butler command line interface "import" subcommand. Functions
            # in the script folder are generally considered protected and
            # should not be used as public api.
            with open(export_file) as f:
                script.butlerImport(
                    import_dir,
                    export_file=f,
                    directory=export_dir,
                    transfer="auto",
                    skip_dimensions=None,
                )
            with Butler.from_config(import_dir, run=butler_harness.default_run) as import_butler:
                for ref in datasets:
                    # Test for existence by passing in the DatasetType and
                    # data ID separately, to avoid lookup by dataset_id.
                    assert import_butler.exists(ref.datasetType, ref.dataId), (
                        f"dataset {ref!r} missing after import"
                    )
                assert list(import_butler.registry.queryDimensionRecords("skymap")) == [
                    import_butler.dimensions["skymap"].RecordClass(**skymap_record)
                ]


@pytest.mark.parametrize(AXIS_NAMES, FILE_DATASTORE_AXES, indirect=True)
def test_import_export(butler_harness: ButlerHarness, test_directory: str) -> None:
    _run_import_export_test(butler_harness, "StructuredDataNoComponents", test_directory)


@pytest.mark.xfail(reason="Export of a disassembled composite raises NotImplementedError from the datastore.")
@pytest.mark.parametrize(AXIS_NAMES, FILE_DATASTORE_AXES, indirect=True)
def test_import_export_virtual_composite(butler_harness: ButlerHarness, test_directory: str) -> None:
    _run_import_export_test(butler_harness, "StructuredComposite", test_directory)


@pytest.mark.parametrize(AXIS_NAMES, FILE_DATASTORE_AXES, indirect=True)
def test_remove_runs(butler_harness: ButlerHarness, caplog: pytest.LogCaptureFixture) -> None:
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    butler = butler_harness.create_empty_butler(writeable=True)
    # Load registry data with dimensions to hang datasets off of.
    butler.import_(filename=get_test_data_path("base.yaml"))
    # Add some RUN-type collection.
    run1 = "run1"
    butler.collections.register(run1)
    run2 = "run2"
    butler.collections.register(run2)
    # put a dataset in each
    metric = make_example_metrics()
    dimensions = butler.dimensions.conform(["instrument", "physical_filter"])
    dataset_type = add_dataset_type(
        "prune_collections_test_dataset", dimensions, storage_class, butler.registry
    )
    ref1 = butler.put(metric, dataset_type, {"instrument": "Cam1", "physical_filter": "Cam1-G"}, run=run1)
    ref2 = butler.put(metric, dataset_type, {"instrument": "Cam1", "physical_filter": "Cam1-G"}, run=run2)
    uri1 = butler.getURI(ref1)
    uri2 = butler.getURI(ref2)

    # Put one of the runs in a chain.
    butler.collections.register("Chain", CollectionType.CHAINED)
    butler.collections.extend_chain("Chain", run1)

    with pytest.raises(OrphanedRecordError):
        butler.registry.removeDatasetType(dataset_type.name)

    # Remove a non-run.
    with pytest.raises(TypeError):
        butler.removeRuns(["Chain"])

    # Remove without unlinking from chain should fail.
    with pytest.raises(IntegrityError):
        butler.removeRuns([run1])

    # Remove from both runs. No longer use unstore parameter since it
    # always purges.
    butler.removeRuns([run1, run2], unlink_from_chains=True)

    # Should be nothing in registry for either one, and datastore should
    # not think either exists.
    with pytest.raises(MissingCollectionError):
        butler.collections.get_info(run1)
    with pytest.raises(MissingCollectionError):
        butler.collections.get_info(run2)
    assert not butler.stored(ref1)
    assert not butler.stored(ref2)
    # We always unstore so both URIs should be gone.
    assert not uri1.exists()
    assert not uri2.exists()

    # Now that the collections have been pruned we can remove the
    # dataset type
    butler.registry.removeDatasetType(dataset_type.name)

    with caplog.at_level(logging.INFO, logger=REGISTRY_LOGGER):
        caplog.clear()
        butler.registry.removeDatasetType(("test*", "test*"))
        records = records_from(caplog, REGISTRY_LOGGER, logging.INFO)
    assert "not defined" in "\n".join(record.getMessage() for record in records)


@pytest.mark.parametrize(AXIS_NAMES, FILE_DATASTORE_AXES, indirect=True)
def test_prune_datasets(butler_harness: ButlerHarness, datastore_type: str) -> None:
    if datastore_type == "chained":
        # This test relies on manipulating files out-of-band, which is
        # impossible for this configuration because of the InMemoryDatastore
        # in the ChainedDatastore.
        return

    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    butler = butler_harness.create_empty_butler(writeable=True)
    # Load registry data with dimensions to hang datasets off of.
    butler.import_(filename=get_test_data_path("base.yaml"))
    # Add some RUN-type collections.
    run1 = "run1"
    butler.collections.register(run1)
    run2 = "run2"
    butler.collections.register(run2)
    # put some datasets.  ref1 and ref2 have the same data ID, and are in
    # different runs.  ref3 has a different data ID.
    metric = make_example_metrics()
    dimensions = butler.dimensions.conform(["instrument", "physical_filter"])
    dataset_type = add_dataset_type(
        "prune_collections_test_dataset", dimensions, storage_class, butler.registry
    )
    ref1 = butler.put(metric, dataset_type, {"instrument": "Cam1", "physical_filter": "Cam1-G"}, run=run1)
    ref2 = butler.put(metric, dataset_type, {"instrument": "Cam1", "physical_filter": "Cam1-G"}, run=run2)
    ref3 = butler.put(metric, dataset_type, {"instrument": "Cam1", "physical_filter": "Cam1-R1"}, run=run1)

    many_stored = butler.stored_many([ref1, ref2, ref3])
    for ref, stored in many_stored.items():
        assert stored, f"Ref {ref} should be stored"

    many_exists = butler._exists_many([ref1, ref2, ref3])
    for ref, exists in many_exists.items():
        assert exists, f"Checking ref {ref} exists."
        assert exists == DatasetExistence.VERIFIED, f"Ref {ref} should be stored"

    # Simple prune.
    butler.pruneDatasets([ref1, ref2, ref3], purge=True, unstore=True)
    assert not butler.exists(ref1.datasetType, ref1.dataId, collections=run1)

    many_stored = butler.stored_many([ref1, ref2, ref3])
    for ref, stored in many_stored.items():
        assert not stored, f"Ref {ref} should not be stored"

    many_exists = butler._exists_many([ref1, ref2, ref3])
    for ref, exists in many_exists.items():
        assert exists == DatasetExistence.UNRECOGNIZED, f"Ref {ref} should not be stored"

    # Put data back.
    ref1_new = butler.put(metric, ref1)
    assert ref1_new == ref1  # Reuses original ID.
    ref2 = butler.put(metric, ref2)

    many_stored = butler.stored_many([ref1, ref2, ref3])
    assert many_stored[ref1]
    assert many_stored[ref2]
    assert not many_stored[ref3]

    ref3 = butler.put(metric, ref3)

    many_exists = butler._exists_many([ref1, ref2, ref3])
    for ref, exists in many_exists.items():
        assert exists, f"Ref {ref} should not be stored"

    # Clear out the datasets from registry and start again.
    refs = [ref1, ref2, ref3]
    butler.pruneDatasets(refs, purge=True, unstore=True)
    for ref in refs:
        butler.put(metric, ref)

    # Confirm we can retrieve deferred.
    dref1 = butler.getDeferred(ref1)  # known and exists
    metric1 = dref1.get()
    assert metric1 == metric

    # Test different forms of file availability.
    # Need to be in a state where:
    # - one ref just has registry record.
    # - one ref has a missing file but a datastore record.
    # - one ref has a missing datastore record but file is there.
    # - one ref does not exist anywhere.
    # Do not need to test a ref that has everything since that is tested
    # above.
    ref0 = DatasetRef(
        dataset_type,
        DataCoordinate.standardize(
            {"instrument": "Cam1", "physical_filter": "Cam1-G"}, universe=butler.dimensions
        ),
        run=run1,
    )

    # Delete from datastore and retain in Registry.
    butler.pruneDatasets([ref1], purge=False, unstore=True, disassociate=False)

    # File has been removed.
    butler_harness.remove_dataset_out_of_band(butler, ref2)

    # Datastore has lost track.
    butler._datastore.forget([ref3])

    # First test with a standard butler.
    exists_many = butler._exists_many([ref0, ref1, ref2, ref3], full_check=True)
    assert exists_many[ref0] == DatasetExistence.UNRECOGNIZED
    assert exists_many[ref1] == DatasetExistence.RECORDED
    assert exists_many[ref2] == DatasetExistence.RECORDED | DatasetExistence.DATASTORE
    assert exists_many[ref3] == DatasetExistence.RECORDED

    exists_many = butler._exists_many([ref0, ref1, ref2, ref3], full_check=False)
    assert exists_many[ref0] == DatasetExistence.UNRECOGNIZED
    assert exists_many[ref1] == DatasetExistence.RECORDED | DatasetExistence._ASSUMED
    assert exists_many[ref2] == DatasetExistence.KNOWN
    assert exists_many[ref3] == DatasetExistence.RECORDED | DatasetExistence._ASSUMED
    assert exists_many[ref2]

    # Check that per-ref query gives the same answer as many query.
    for ref, exists in exists_many.items():
        assert butler.exists(ref, full_check=False) == exists

    # Get deferred checks for existence before it allows it to be
    # retrieved.
    with pytest.raises(LookupError):
        butler.getDeferred(ref3)  # not known, file exists
    dref2 = butler.getDeferred(ref2)  # known but file missing
    with pytest.raises(FileNotFoundError):
        dref2.get()

    # Test again with a trusting butler.
    if not butler_harness.trust_mode_supported:
        return

    # Trust mode, the trash table and the bridge are FileDatastore concepts,
    # and only file datastores reach this point.
    datastore = cast(FileDatastore, butler._datastore)
    datastore.trustGetRequest = True
    exists_many = butler._exists_many([ref0, ref1, ref2, ref3], full_check=True)
    assert exists_many[ref0] == DatasetExistence.UNRECOGNIZED
    assert exists_many[ref1] == DatasetExistence.RECORDED
    assert exists_many[ref2] == DatasetExistence.RECORDED | DatasetExistence.DATASTORE
    assert exists_many[ref3] == DatasetExistence.RECORDED | DatasetExistence._ARTIFACT

    # When trusting we can get a deferred dataset handle that is not
    # known but does exist.
    dref3 = butler.getDeferred(ref3)
    metric3 = dref3.get()
    assert metric3 == metric

    # Check that per-ref query gives the same answer as many query.
    for ref, exists in exists_many.items():
        assert butler.exists(ref, full_check=True) == exists

    # Create a ref that surprisingly has the UUID of an existing ref
    # but is not the same.
    ref_bad = DatasetRef(dataset_type, dataId=ref3.dataId, run=ref3.run, id=ref2.id)
    with pytest.raises(ValueError, match="has the same dataset ID as one in registry"):
        butler.exists(ref_bad)

    # Create a ref that has a compatible storage class.
    ref_compat = ref2.overrideStorageClass("StructuredDataDict")
    exists = butler.exists(ref_compat)
    assert exists == exists_many[ref2]

    # Remove everything and start from scratch.
    datastore.trustGetRequest = False
    butler.pruneDatasets(refs, purge=True, unstore=True)
    for ref in refs:
        butler.put(metric, ref)

    # These tests mess directly with the trash table and can leave the
    # datastore in an odd state. Do them at the end.
    # Check that in normal mode, deleting the record will lead to
    # trash not touching the file.
    uri1 = butler.getURI(ref1)
    # Update the dataset_location table
    datastore.bridge.moveToTrash([ref1], transaction=None)
    datastore.forget([ref1])
    datastore.trash(ref1)
    datastore.emptyTrash()
    assert uri1.exists()
    uri1.remove()  # Clean it up.

    # Simulate execution butler setup by deleting the datastore
    # record but keeping the file around and trusting.
    datastore.trustGetRequest = True
    uris = butler.get_many_uris([ref2, ref3])
    uri2 = uris[ref2].primaryURI
    uri3 = uris[ref3].primaryURI
    assert uri2 is not None
    assert uri3 is not None
    assert uri2.exists()
    assert uri3.exists()

    # Remove the datastore record.
    # Update the dataset_location table
    datastore.bridge.moveToTrash([ref2], transaction=None)
    datastore.forget([ref2])
    assert uri2.exists()
    datastore.trash([ref2, ref3])
    # Immediate removal for ref2 file
    assert not uri2.exists()
    # But ref3 has to wait for the empty.
    assert uri3.exists()
    datastore.emptyTrash()
    assert not uri3.exists()

    # Clear out the datasets from registry.
    butler.pruneDatasets([ref1, ref2, ref3], purge=True, unstore=True)


def test_export_transfer_copy(butler_harness: ButlerHarness, test_directory: str) -> None:
    """Test local export using all transfer modes."""
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    export_butler = run_put_get_test(butler_harness, storage_class, "test_metric")
    # Test that the repo actually has at least one dataset.
    datasets = list(export_butler.registry.queryDatasets(..., collections=...))
    assert len(datasets) > 0
    uris = [export_butler.getURI(d) for d in datasets]
    assert isinstance(export_butler._datastore, FileDatastore)
    datastore_root = export_butler.get_datastore_roots()[export_butler.get_datastore_names()[0]]
    assert datastore_root is not None

    paths_in_store = [uri.relative_to(datastore_root) for uri in uris]

    for path in paths_in_store:
        # Assume local file system
        assert path is not None
        assert _check_file_exists(datastore_root, path), f"Checking path {path}"

    for transfer in ("copy", "link", "symlink", "relsymlink"):
        with safeTestTempDir(test_directory) as export_dir:
            with export_butler.export(directory=export_dir, format="yaml", transfer=transfer) as export:
                export.saveDatasets(datasets)
                for path in paths_in_store:
                    assert path is not None
                    assert _check_file_exists(export_dir, path), f"Check that mode {transfer} exported files"
