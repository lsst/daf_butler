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

"""Tests for ingesting files that are already laid out on disk."""

from __future__ import annotations

import contextlib
import os
import tempfile
from typing import cast

import pytest
from butler_test_support import AXIS_NAMES, FILE_DATASTORE_AXES

from lsst.daf.butler import (
    Butler,
    ButlerConfig,
    Config,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    FileDataset,
)
from lsst.daf.butler._rubin.file_datasets import transfer_datasets_to_datastore
from lsst.daf.butler._rubin.temporary_for_ingest import TemporaryForIngest
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.daf.butler.tests import MultiDetectorFormatter
from lsst.daf.butler.tests._repo_template_cache import make_repo_for_test
from lsst.daf.butler.tests.fixtures import ButlerHarness, add_dataset_type, make_example_metrics
from lsst.daf.butler.tests.utils import MetricTestRepo
from lsst.resources import ResourcePath
from lsst.utils import doImportType

# An InMemoryDatastore cannot ingest files, so the two ephemeral axis values
# that BUTLER_TESTS_AXES adds are absent here rather than running empty.
INGEST_AXES = FILE_DATASTORE_AXES

LOCAL_LAYOUTS = ["in_repo", "explicit_root"]
"""Repository layouts of the two classes these posix-only tests ran under."""


@pytest.mark.parametrize(AXIS_NAMES, INGEST_AXES, indirect=True)
def test_ingest_zip(butler_harness: ButlerHarness) -> None:
    """Create butler, export data, delete data, import from Zip."""
    default_run = butler_harness.default_run
    butler, dataset_type = butler_harness.create_butler(
        run=default_run, storage_class="StructuredData", dataset_type_name="metrics"
    )

    metric = make_example_metrics()
    refs = []
    for visit in (423, 424, 425):
        ref = butler.put(metric, dataset_type, instrument="DummyCamComp", visit=visit)
        refs.append(ref)

    # Retrieve a Zip file.
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        zip = butler.retrieve_artifacts_zip(refs, destination=tmpdir)

        # Ingest will fail.
        with pytest.raises(ConflictingDefinitionError):
            butler.ingest_zip(zip)

        # Clear out the collection.
        butler.removeRuns([default_run])
        assert not butler.exists(refs[0])

        butler.ingest_zip(zip, transfer="copy")
        assert butler._metrics.time_in_ingest > 0.0
        assert butler._metrics.n_ingest == len(refs)

        # Check that it fails if we try it again.
        with pytest.raises(ConflictingDefinitionError):
            butler.ingest_zip(zip, transfer="copy")

        # This will be a no-op.
        butler.ingest_zip(zip, transfer="copy", skip_existing=True)

        # Create an entirely new local file butler in this temp directory.
        new_butler_cfg = make_repo_for_test(tmpdir)
        with Butler.from_config(new_butler_cfg, writeable=True) as new_butler:
            # This will fail since dimensions records are missing.
            with pytest.raises(ConflictingDefinitionError):
                new_butler.ingest_zip(zip, transfer="copy")

            # Dry run should work.
            new_butler.ingest_zip(zip, transfer="copy", dry_run=True)

            new_butler.ingest_zip(zip, transfer="copy", transfer_dimensions=True)
            assert butler.exists(refs[0])

    # Check that the refs can be read again.
    _ = [butler.get(ref) for ref in refs]

    uri = butler.getURI(refs[2])
    assert uri.exists()

    # Delete one dataset. The Zip file should still exist and allow
    # remaining refs to be read.
    butler.pruneDatasets([refs[0]], purge=True, unstore=True)
    assert uri.exists()

    metric2 = butler.get(refs[1])
    assert metric2 == metric, f"{metric2} != {metric}"

    butler.removeRuns([default_run])
    assert not uri.exists()
    assert not butler.exists(refs[-1])

    with pytest.raises(ValueError, match="Requested Zip file with no contents"):
        butler.retrieve_artifacts_zip([], destination=".")


@pytest.mark.parametrize(AXIS_NAMES, INGEST_AXES, indirect=True)
def test_ingest(butler_harness: ButlerHarness, test_directory: str) -> None:
    default_run = butler_harness.default_run
    butler = butler_harness.create_empty_butler(run=default_run)

    # Create and register a DatasetType
    dimensions = butler.dimensions.conform(["instrument", "visit", "detector"])

    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataDictYaml")
    dataset_type_name = "metric"

    dataset_type = add_dataset_type(dataset_type_name, dimensions, storage_class, butler.registry)

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
    data_root = os.path.join(test_directory, "data", "basic")
    datasets = []
    # Test one DatasetRef with a run that exists, and the other with a run
    # that doesn't exist, to verify that run collections are created when
    # required.
    runs = {1: default_run, 2: "a/new/run"}
    for detector in (1, 2):
        detector_name = f"detector_{detector}"
        metric_file = os.path.join(data_root, f"{detector_name}.yaml")
        data_id = butler.registry.expandDataId(
            {"instrument": "DummyCamComp", "visit": 423, "detector": detector}
        )
        # Create a DatasetRef for ingest
        ref_in = DatasetRef(dataset_type, data_id, run=runs[detector])

        datasets.append(FileDataset(path=metric_file, refs=[ref_in], formatter=formatter))

    butler.ingest(*datasets, transfer="copy")

    data_id1 = {"instrument": "DummyCamComp", "detector": 1, "visit": 423}
    data_id2 = {"instrument": "DummyCamComp", "detector": 2, "visit": 423}

    metrics1 = butler.get(dataset_type_name, data_id1)
    metrics2 = butler.get(dataset_type_name, data_id2, collections="a/new/run")
    assert metrics1 != metrics2

    # Compare URIs
    uri1 = butler.getURI(dataset_type_name, data_id1)
    uri2 = butler.getURI(dataset_type_name, data_id2, collections="a/new/run")
    assert not butler_harness.are_uris_equivalent(uri1, uri2), f"Cf. {uri1} with {uri2}"

    # Re-ingesting the same datasets raises an error with
    # skip_existing=False.
    with pytest.raises(ConflictingDefinitionError):
        butler.ingest(*datasets, transfer="copy")
    # skip_existing=True makes it a no-op to re-ingest the same datasets.
    butler.ingest(*datasets, transfer="copy", skip_existing=True)

    # Now do a multi-dataset but single file ingest
    metric_file = os.path.join(data_root, "detectors.yaml")
    refs = []
    for detector in (1, 2):
        data_id = butler.registry.expandDataId(
            {"instrument": "DummyCamComp", "visit": 424, "detector": detector}
        )
        # Create a DatasetRef for ingest
        refs.append(DatasetRef(dataset_type, data_id, run=default_run))

    # Test "move" transfer to ensure that the files themselves
    # have disappeared following ingest.
    with ResourcePath.temporary_uri(suffix=".yaml") as temp_file:
        temp_file.transfer_from(ResourcePath(metric_file), transfer="copy")

        datasets = []
        datasets.append(FileDataset(path=temp_file, refs=refs, formatter=MultiDetectorFormatter))

        # For first ingest use copy.
        butler.ingest(*datasets, transfer="copy", record_validation_info=False)

        # Now try to ingest again in "execution butler" mode where
        # the registry entries exist but the datastore does not have
        # the files. We also need to strip the dimension records to ensure
        # that they will be re-added by the ingest.
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
                assert not new_ref.dataId.hasRecords()
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
                assert ref.dataId.hasRecords()

        # Ensure that the file has disappeared.
        assert not temp_file.exists()

    # Check that the datastore recorded no file size.
    # Not all datastores can support this.
    with contextlib.suppress(AttributeError):
        infos = butler._datastore.getStoredItemsInfo(datasets[0].refs[0])  # type: ignore[attr-defined]
        assert infos[0].file_size == -1

    data_id1 = {"instrument": "DummyCamComp", "detector": 1, "visit": 424}
    data_id2 = {"instrument": "DummyCamComp", "detector": 2, "visit": 424}

    multi1 = butler.get(dataset_type_name, data_id1)
    multi2 = butler.get(dataset_type_name, data_id2)

    assert multi1 == metrics1
    assert multi2 == metrics2

    # Compare URIs
    uri1 = butler.getURI(dataset_type_name, data_id1)
    uri2 = butler.getURI(dataset_type_name, data_id2)
    assert butler_harness.are_uris_equivalent(uri1, uri2), f"Cf. {uri1} with {uri2}"

    # Test that removing one does not break the second
    # This line will issue a warning log message for a ChainedDatastore
    # that uses an InMemoryDatastore since in-memory can not ingest
    # files.
    butler.pruneDatasets([datasets[0].refs[0]], unstore=True, disassociate=False)
    assert not butler.exists(dataset_type_name, data_id1)
    assert butler.exists(dataset_type_name, data_id2)
    multi2b = butler.get(dataset_type_name, data_id2)
    assert multi2 == multi2b

    # Ensure we can ingest 0 datasets
    datasets = []
    butler.ingest(*datasets)


@pytest.mark.parametrize("repo_layout", LOCAL_LAYOUTS, indirect=True)
def test_specialized_file_datasets_functions(butler_harness: ButlerHarness) -> None:
    """Test a workflow used in Prompt Processing where we export datasets
    from one repository and write them in-place to the datastore of
    another, without immediately inserting registry entries for the
    datasets.
    """
    repo = MetricTestRepo.create_from_butler(
        butler_harness.create_empty_butler(writeable=True),
        butler_harness.config_file,
        "StructuredCompositeReadCompNoDisassembly",
    )
    source_butler = repo.butler

    # Test writing outputs to a FileDatastore.
    with tempfile.TemporaryDirectory() as tempdir:
        target_repo_config = make_repo_for_test(tempdir)
        refs = [repo.ref1, repo.ref2]
        datasets = transfer_datasets_to_datastore(source_butler, ButlerConfig(target_repo_config), refs)
        assert len(datasets) == 2
        assert {ref.id for ref in refs} == {dataset.refs[0].id for dataset in datasets}
        for dataset in datasets:
            path = ResourcePath(dataset.path, forceAbsolute=False)
            # Paths should be relative paths to the target datastore.
            assert not path.isabs()
            # Files should have been copied into the target datastore
            assert ResourcePath(tempdir).join(path).exists()

        # Make sure the target Butler can ingest the datasets.
        with Butler.from_config(target_repo_config, writeable=True) as target_butler:
            target_butler.transfer_dimension_records_from(source_butler, refs)
            target_butler.ingest(*datasets, transfer=None)
            assert target_butler.get(repo.ref1) is not None
            assert target_butler.get(repo.ref2) is not None

        # Giving an empty list of files is a no-op.
        no_datasets = transfer_datasets_to_datastore(source_butler, ButlerConfig(target_repo_config), [])
        assert len(no_datasets) == 0

    # Test writing outputs to a ChainedDatastore.
    with tempfile.TemporaryDirectory() as tempdir:
        # Set up a second dataset type, so we can split the files across
        # multiple datastore roots.
        dt1 = repo.datasetType
        dt2 = DatasetType("other", dt1.dimensions, dt1.storageClass)
        source_butler.registry.registerDatasetType(dt2)
        other_ref = repo.addDataset(dict(repo.ref1.dataId.required), datasetType=dt2)
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
        assert len(datasets) == 3
        assert {ref.id for ref in refs} == {dataset.refs[0].id for dataset in datasets}
        for dataset in datasets:
            path = ResourcePath(dataset.path, forceAbsolute=False)
            # Paths should be relative paths to the target datastore.
            assert not path.isabs()
            # Files should have been split up between the two datastores
            # in the chain.
            datastore_root = ResourcePath(tempdir)
            if dataset.refs[0].datasetType.name == dt1.name:
                datastore_root = datastore_root.join("FileDatastore_0")
            else:
                datastore_root = datastore_root.join("FileDatastore_1")
            assert datastore_root.join(path).exists()

        # Make sure the target Butler can ingest the datasets.
        with Butler.from_config(target_repo_config, writeable=True) as target_butler:
            target_butler.transfer_dimension_records_from(source_butler, refs)
            target_butler.ingest(*datasets, transfer=None)
            assert target_butler.get(repo.ref1) is not None
            assert target_butler.get(repo.ref2) is not None
            assert target_butler.get(other_ref) is not None


@pytest.mark.parametrize("repo_layout", LOCAL_LAYOUTS, indirect=True)
def test_temporary_for_ingest(butler_harness: ButlerHarness) -> None:
    """Test the `lsst.daf.butler._rubin.ingest_from_temporary` module."""
    with butler_harness.create_empty_butler("example_run") as butler:
        dataset_type = DatasetType("example", butler.dimensions.empty, "StructuredDataDict")
        butler.registry.registerDatasetType(dataset_type)
        ref = DatasetRef(dataset_type, DataCoordinate.make_empty(butler.dimensions), "example_run")
        with TemporaryForIngest(butler, ref) as temporary:
            temporary.path.write(b"three: 3")
            found = TemporaryForIngest.find_orphaned_temporaries_by_ref(ref, butler)
            assert found == [temporary.path]
            assert ".tmp" in temporary.ospath
            temporary.ingest()
        loaded = butler.get(ref)
        assert loaded == {"three": 3}
