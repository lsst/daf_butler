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


"""Tests for transferring datasets between Butlers."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import pathlib
import tempfile
import unittest.mock
import uuid
from collections.abc import Iterator

import pytest

from lsst.daf.butler import (
    Butler,
    CollectionType,
    Config,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    FileDataset,
    StorageClassFactory,
)
from lsst.daf.butler._rubin.transfer_datasets_in_place import transfer_datasets_in_place
from lsst.daf.butler.direct_butler import DirectButler
from lsst.daf.butler.registry import CollectionTypeError, ConflictingDefinitionError
from lsst.daf.butler.tests import MetricsExample
from lsst.daf.butler.tests._repo_template_cache import make_repo_for_test
from lsst.daf.butler.tests.fixtures import (
    DATASTORE_PROFILES,
    get_test_data_path,
    make_example_metrics,
)
from lsst.daf.butler.tests.server_available import butler_server_import_error, butler_server_is_available
from lsst.daf.butler.tests.utils import MetricTestRepo
from lsst.resources import ResourcePath
from lsst.resources.http import HttpResourcePath
from lsst.resources.tests import make_remote_test_uri

if butler_server_is_available:
    from lsst.daf.butler.tests.server import create_test_server


TESTDIR = os.path.abspath(os.path.dirname(__file__))

DEFAULT_MANAGER = "lsst.daf.butler.registry.datasets.byDimensions.ByDimensionsDatasetRecordStorageManagerUUID"
"""Dataset record storage manager used when a test does not name one."""


class TransferHarness:
    """State shared by the butler-to-butler transfer tests.

    This mirrors what ``DatastoreTransfers`` held on ``self``. It is local to
    this file rather than part of the shipped fixture plugin, because its
    ``create_butler`` builds a pair of repositories with a chosen dataset
    record storage manager, which is unrelated to
    `~lsst.daf.butler.tests.fixtures.ButlerHarness.create_butler`.
    """

    source_butler: Butler
    target_butler: Butler

    def __init__(
        self,
        root: str,
        config_file: str,
        storage_class_factory: StorageClassFactory,
        exit_stack: contextlib.ExitStack,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        self.root = root
        self.config_file = config_file
        self.config = Config(config_file)
        self.storage_class_factory = storage_class_factory
        self.exit_stack = exit_stack
        self.caplog = caplog

    def create_butler(self, manager: str | None, label: str, config_file: str | None = None) -> Butler:
        """Create a repository using the given dataset storage manager."""
        if manager is None:
            manager = DEFAULT_MANAGER
        config = Config(config_file if config_file is not None else self.config_file)
        config["registry", "managers", "datasets"] = manager
        butler = Butler.from_config(
            make_repo_for_test(f"{self.root}/butler{label}", config=config), writeable=True
        )
        self.exit_stack.enter_context(butler)
        return butler

    def create_butlers(
        self, manager1: str | None = None, manager2: str | None = None, source_config: str | None = None
    ) -> None:
        """Create the source and target butlers for a transfer test."""
        self.source_butler = self.create_butler(manager1, "1", config_file=source_config)
        self.target_butler = self.create_butler(manager2, "2")


@pytest.fixture
def tr(  # numpydoc ignore=PR01
    tmp_path: pathlib.Path,
    datastore_type: str,
    storage_class_factory: StorageClassFactory,
    caplog: pytest.LogCaptureFixture,
) -> Iterator[TransferHarness]:
    """Build a `TransferHarness` for the requested datastore configuration."""
    config_file = os.path.join(TESTDIR, DATASTORE_PROFILES[datastore_type].config_file)

    # Some tests cause converters to be replaced, so reset the storage class
    # factory and reload it for this configuration.
    storage_class_factory.reset()
    storage_class_factory.addFromConfig(config_file)

    with contextlib.ExitStack() as exit_stack:
        yield TransferHarness(str(tmp_path), config_file, storage_class_factory, exit_stack, caplog)


def _assert_butler_transfers(
    tr: TransferHarness,
    purge: bool = False,
    storageClassName: str = "StructuredData",
    storageClassNameTarget: str | None = None,
) -> None:
    """Test that a run can be transferred to another butler."""
    storageClass = tr.storage_class_factory.getStorageClass(storageClassName)
    if storageClassNameTarget is not None:
        storageClassTarget = tr.storage_class_factory.getStorageClass(storageClassNameTarget)
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
        tr.source_butler.collections.register(run)

    # Create dimensions in source butler.
    n_exposures = 30
    tr.source_butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
    tr.source_butler.registry.insertDimensionData(
        "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
    )
    tr.source_butler.registry.insertDimensionData(
        "detector", {"instrument": "DummyCamComp", "id": 1, "full_name": "det1"}
    )
    tr.source_butler.registry.insertDimensionData(
        "day_obs",
        {
            "instrument": "DummyCamComp",
            "id": 20250101,
        },
    )

    for i in range(n_exposures):
        tr.source_butler.registry.insertDimensionData(
            "group", {"instrument": "DummyCamComp", "name": f"group{i}"}
        )
        tr.source_butler.registry.insertDimensionData(
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
    dimensions = tr.source_butler.dimensions.conform(["instrument", "exposure"])
    for datasetTypeName in datasetTypeNames:
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        tr.source_butler.registry.registerDatasetType(datasetType)

    # Write a dataset to an unrelated run -- this will ensure that
    # we are rewriting integer dataset ids in the target if necessary.
    # Will not be relevant for UUID.
    run = "distraction"
    butler = Butler.from_config(butler=tr.source_butler, run=run)
    tr.exit_stack.enter_context(butler)
    butler.put(
        make_example_metrics(),
        datasetTypeName,
        exposure=1,
        instrument="DummyCamComp",
        physical_filter="d-r",
    )

    # Write some example metrics to the source
    butler = Butler.from_config(butler=tr.source_butler)
    tr.exit_stack.enter_context(butler)

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
            assert new_metric == metric

    # Create some bad dataset types to ensure we check for inconsistent
    # definitions.
    badStorageClass = tr.storage_class_factory.getStorageClass("StructuredDataList")
    for datasetTypeName in datasetTypeNames:
        datasetType = DatasetType(datasetTypeName, dimensions, badStorageClass)
        tr.target_butler.registry.registerDatasetType(datasetType)
    with pytest.raises(ConflictingDefinitionError) as cm:
        tr.target_butler.transfer_from(tr.source_butler, source_refs)
    assert "dataset type differs" in str(cm.value)

    # And remove the bad definitions.
    for datasetTypeName in datasetTypeNames:
        tr.target_butler.registry.removeDatasetType(datasetTypeName)

    # Transfer without creating dataset types should fail.
    with pytest.raises(KeyError):
        tr.target_butler.transfer_from(tr.source_butler, source_refs)

    # Transfer without creating dimensions should fail.
    with pytest.raises(ConflictingDefinitionError) as cm:
        tr.target_butler.transfer_from(tr.source_butler, source_refs, register_dataset_types=True)
    assert "dimension" in str(cm.value)

    # The dry run test requires dataset types to exist. If we have
    # been given distinct storage classes for the target we have
    # to redefine at least one of the dataset types in the target butler.
    if storageClass != storageClassTarget:
        tr.target_butler.registry.removeDatasetType(datasetTypeNames[0])
        datasetType = DatasetType(datasetTypeNames[0], dimensions, storageClassTarget)
        tr.target_butler.registry.registerDatasetType(datasetType)

    # The failed transfer above leaves registry in an inconsistent
    # state because the run is created but then rolled back without
    # the collection cache being cleared. For now force a refresh.
    # Can remove with DM-35498.
    tr.target_butler.registry.refresh()

    # Do a dry run -- this should not have any effect on the target butler.
    tr.target_butler.transfer_from(tr.source_butler, source_refs, dry_run=True)

    # Transfer the records for one ref to test the alternative API.
    with tr.caplog.at_level(logging.DEBUG, logger="lsst"):
        tr.target_butler.transfer_dimension_records_from(tr.source_butler, [source_refs[0]])
    assert "number of records transferred: 1" in tr.caplog.text

    # Now transfer them to the second butler, including dimensions.
    with tr.caplog.at_level(logging.DEBUG, logger="lsst"):
        transferred = tr.target_butler.transfer_from(
            tr.source_butler,
            source_refs,
            register_dataset_types=True,
            transfer_dimensions=True,
        )
    assert len(transferred) == n_expected
    log_output = tr.caplog.text

    # A ChainedDatastore will use the in-memory datastore for mexists
    # so we can not rely on the mexists log message.
    assert "Number of datastore records found in source" in log_output
    assert "Creating output run" in log_output

    # Do the transfer twice to ensure that it will do nothing extra.
    # Only do this if purge=True because it does not work for int
    # dataset_id.
    if purge:
        # This should not need to register dataset types.
        transferred = tr.target_butler.transfer_from(tr.source_butler, source_refs)
        assert len(transferred) == n_expected

        with pytest.raises((TypeError, AttributeError)):
            tr.target_butler._datastore.transfer_from(tr.source_butler, source_refs)  # type: ignore

        with pytest.raises(ValueError, match="Can not transfer from a source datastore"):
            tr.target_butler._datastore.transfer_from(
                tr.source_butler._datastore, source_refs, transfer="split"
            )

    # Now try to get the same refs from the new butler.
    for ref in source_refs:
        if ref not in deleted:
            new_metric = tr.target_butler.get(ref)
            old_metric = tr.source_butler.get(ref)
            assert new_metric == old_metric

            # Try again without implicit storage class conversion
            # triggered by using the source ref. This will do conversion
            # since the formatter will be returning the source python type.
            target_ref = tr.target_butler.get_dataset(ref.id)
            if target_ref.datasetType.storageClass != ref.datasetType.storageClass:
                new_metric = tr.target_butler.get(target_ref)
                assert type(new_metric) is not type(old_metric)

                # Remove the dataset from the target and put it again
                # as if it was the right type all along for this butler.
                tr.target_butler.pruneDatasets([target_ref], unstore=True, purge=True, disassociate=True)
                tr.target_butler.put(new_metric, target_ref)
                new_new_metric = tr.target_butler.get(target_ref)
                new_old_metric = tr.target_butler.get(target_ref, storageClass=ref.datasetType.storageClass)
                assert new_new_metric == new_metric
                assert new_old_metric == old_metric

    # Now prune run2 collection and create instead a CHAINED collection.
    # This should block the transfer.
    tr.target_butler.removeRuns(["run2"])
    tr.target_butler.collections.register("run2", CollectionType.CHAINED)
    # Re-importing the run1 datasets can be problematic if they
    # use integer IDs so filter those out.
    to_transfer = [ref for ref in source_refs if ref.run == "run2"]
    with pytest.raises(CollectionTypeError):
        tr.target_butler.transfer_from(tr.source_butler, to_transfer)


def _absolute_transfer(tr: TransferHarness, transfer: str) -> None:
    tr.create_butlers()

    storageClassName = "StructuredData"
    storageClass = tr.storage_class_factory.getStorageClass(storageClassName)
    datasetTypeName = "random_data"
    run = "run1"
    tr.source_butler.collections.register(run)

    dimensions = tr.source_butler.dimensions.conform(())
    datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
    tr.source_butler.registry.registerDatasetType(datasetType)

    metrics = make_example_metrics()
    # Ingest from a URI that reports itself as not local, so that the test
    # distinguishes "the absolute URI was preserved" from "a local path
    # happened to work".
    source_dir = os.path.join(tr.root, "source data")
    os.makedirs(source_dir)
    with ResourcePath.temporary_uri(prefix=make_remote_test_uri(source_dir), suffix=".json") as temp:
        assert not temp.isLocal
        dataId = DataCoordinate.make_empty(tr.source_butler.dimensions)
        source_refs = [DatasetRef(datasetType, dataId, run=run)]
        temp.write(json.dumps(metrics.exportAsDict()).encode())
        dataset = FileDataset(path=temp, refs=source_refs)
        tr.source_butler.ingest(dataset, transfer="direct")

        tr.target_butler.transfer_from(
            tr.source_butler, dataset.refs, register_dataset_types=True, transfer=transfer
        )

        uri = tr.target_butler.getURI(dataset.refs[0])
        if transfer == "auto" or transfer == "unsafe_direct":
            assert uri == temp
        else:
            assert uri != temp


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_transfer_uuid_to_uuid(tr: TransferHarness) -> None:
    tr.create_butlers()
    _assert_butler_transfers(tr)


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_transfer_from_chained_uuid_to_uuid(tr: TransferHarness) -> None:
    """Force the source butler to be a ChainedDatastore."""
    tr.create_butlers(source_config=os.path.join(TESTDIR, "config/basic/butler-chained.yaml"))
    _assert_butler_transfers(tr)


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_transfer_from_incompatible_uuid_to_uuid(tr: TransferHarness) -> None:
    """Force the source butler to be a incompatible datastore."""
    tr.create_butlers(source_config=os.path.join(TESTDIR, "config/basic/butler-inmemory.yaml"))
    with pytest.raises(NotImplementedError):
        _assert_butler_transfers(tr)


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_transfer_from_incompatible_chain_uuid_to_uuid(tr: TransferHarness) -> None:
    """Force the source butler to be a incompatible datastore."""
    tr.create_butlers(source_config=os.path.join(TESTDIR, "config/basic/butler-inmemory-chain.yaml"))
    with pytest.raises(TypeError):
        _assert_butler_transfers(tr)


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_transfer_from_file_uuid_to_uuid(tr: TransferHarness) -> None:
    """Force the source butler to be a FileDatastore."""
    tr.create_butlers(source_config=os.path.join(TESTDIR, "config/basic/butler.yaml"))
    _assert_butler_transfers(tr)


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_transfer_missing(tr: TransferHarness) -> None:
    """Test transfers where datastore records are missing.

    This is how execution butler works.
    """
    tr.create_butlers()

    # Configure the source butler to allow trust.
    tr.source_butler._datastore._set_trust_mode(True)

    _assert_butler_transfers(tr, purge=True)


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_transfer_missing_disassembly(tr: TransferHarness) -> None:
    """Test transfers where datastore records are missing.

    This is how execution butler works.
    """
    tr.create_butlers()

    # Configure the source butler to allow trust.
    tr.source_butler._datastore._set_trust_mode(True)

    # Test disassembly.
    _assert_butler_transfers(tr, purge=True, storageClassName="StructuredComposite")


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_transfer_differing_storage_classes(tr: TransferHarness) -> None:
    """Test transfers when the source butler dataset type has a different
    but compatible storage class.
    """
    tr.create_butlers()

    _assert_butler_transfers(tr, storageClassNameTarget="MetricsConversion")


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_transfer_differing_storage_classes_disassembly(tr: TransferHarness) -> None:
    """Test transfers when the source butler dataset type has a different
    but compatible storage class and where the source butler has
    disassembled.
    """
    tr.create_butlers()

    _assert_butler_transfers(
        tr, storageClassName="StructuredComposite", storageClassNameTarget="MetricsConversion"
    )


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_unsafe_direct_transfer(tr: TransferHarness) -> None:
    """Test that transfer='unsafe_direct' records the absolute URI of
    source files in the target datastore.
    """
    tr.create_butlers()
    dataset_type = DatasetType("dt", [], "int", universe=tr.source_butler.dimensions)
    tr.source_butler.registry.registerDatasetType(dataset_type)
    tr.source_butler.collections.register("run")
    ref = tr.source_butler.put(123, "dt", [], run="run")
    tr.target_butler.transfer_from(
        tr.source_butler, [ref], transfer="unsafe_direct", register_dataset_types=True
    )
    assert tr.target_butler.get(ref) == 123
    assert tr.source_butler.getURI(ref) == tr.target_butler.getURI(ref)


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_absolute_uri_transfer_direct(tr: TransferHarness) -> None:
    """Test transfer using an absolute URI."""
    _absolute_transfer(tr, "auto")


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_absolute_uri_transfer_unsafe_direct(tr: TransferHarness) -> None:
    """Test transfer using an absolute URI."""
    _absolute_transfer(tr, "unsafe_direct")


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_absolute_uri_transfer_copy(tr: TransferHarness) -> None:
    """Test transfer using an absolute URI."""
    _absolute_transfer(tr, "copy")


@pytest.mark.parametrize("datastore_type", ["posix", "chained"], indirect=True)
def test_shared_dimension_group(tr: TransferHarness) -> None:
    """Test internal logic that divides dataset types by dimension group
    when doing registry updates.
    """
    tr.create_butlers()
    tr.source_butler.import_(filename=get_test_data_path("base.yaml"), without_datastore=True)
    tr.source_butler.import_(filename=get_test_data_path("datasets.yaml"), without_datastore=True)

    source_butler = tr.source_butler
    target_butler = tr.target_butler

    # Create a dataset type with the same dimensions as the 'bias' dataset
    # type from base.yaml
    dataset_type = DatasetType(
        "test_type", ["instrument", "detector"], "int", universe=source_butler.dimensions
    )
    source_butler.registry.registerDatasetType(dataset_type)
    # This has the same data ID as one of the bias datasets in
    # datasets.yaml.
    test_ref = source_butler.registry.insertDatasets(
        "test_type", [{"instrument": "Cam1", "detector": 2}], run="imported_g"
    )[0]

    biases = source_butler.query_datasets("bias", ["imported_g", "imported_r"])
    flats = source_butler.query_datasets("flat", ["imported_g", "imported_r"])
    refs = [test_ref, *biases, *flats]

    # Test setup will be even more convoluted if we want the datastore to
    # actually transfer files.  For testing the dimension group behavior,
    # we really only care about the registry.
    with unittest.mock.patch.object(target_butler._datastore, "transfer_from") as mock:
        mock.return_value = (set(refs), set())
        target_butler.transfer_from(
            source_butler,
            refs,
            transfer=None,
            register_dataset_types=True,
            skip_missing=False,
            transfer_dimensions=True,
        )

    transferred_test_ref = target_butler.find_dataset(
        "test_type", {"instrument": "Cam1", "detector": 2}, collections="imported_g"
    )
    assert transferred_test_ref.id == test_ref.id

    transferred_bias = target_butler.find_dataset(
        "bias", {"instrument": "Cam1", "detector": 2}, collections="imported_g"
    )
    assert transferred_bias.id == uuid.UUID("51352db4-a47a-447c-b12d-a50b206b17cd")

    transferred_flat = target_butler.find_dataset(
        "flat",
        {"instrument": "Cam1", "detector": 2, "physical_filter": "Cam1-R1", "band": "r"},
        collections="imported_r",
    )
    assert transferred_flat.id == uuid.UUID("c1296796-56c5-4acf-9b49-40d920c6f840")


@pytest.mark.server
@pytest.mark.skipif(not butler_server_is_available, reason=butler_server_import_error)
@pytest.mark.parametrize("datastore_type", ["posix"], indirect=True)
def test_transfers_from_remote_to_direct(tr: TransferHarness) -> None:
    from lsst.daf.butler.remote_butler._remote_file_transfer_source import (
        mock_file_transfer_uris_for_unit_test,
    )

    tr.target_butler = tr.create_butler(None, "2")
    with create_test_server(TESTDIR) as server:
        tr.source_butler = server.hybrid_butler

        def _remap_transfer_url(path: HttpResourcePath) -> HttpResourcePath:
            # The Butler server returns HTTP URIs with a domain name that
            # is not resolvable because there is no actual HTTP server
            # involved in these tests.  Strip this first layer of
            # indirection, and return the target of the redirect instead.
            response = server.client.get(str(path), follow_redirects=False, headers=path._extra_headers)
            return ResourcePath(str(response.next_request.url))

        with mock_file_transfer_uris_for_unit_test(_remap_transfer_url):
            _assert_butler_transfers(tr)


def test_file_datastore() -> None:
    configFile = os.path.join(TESTDIR, "config/basic/butler.yaml")
    with (
        tempfile.TemporaryDirectory() as datastore_root,
        tempfile.TemporaryDirectory() as other_repo_root,
    ):
        config = Config(configFile)
        config["datastore", "datastore", "name"] = "file_datastore"
        make_repo_for_test(datastore_root, config=config)
        config["datastore", "datastore", "root"] = datastore_root
        make_repo_for_test(other_repo_root, config, forceConfigRoot=False)
        with (
            Butler(datastore_root, writeable=True) as source_butler,
            Butler(other_repo_root, writeable=True) as target_butler,
        ):
            _test_transfer_datasets_in_place(source_butler, target_butler)


def test_chained_datastore() -> None:
    configFile = os.path.join(TESTDIR, "config/basic/butler-chained-posix.yaml")
    with (
        tempfile.TemporaryDirectory() as datastore_root,
        tempfile.TemporaryDirectory() as other_repo_root,
    ):
        config = Config(configFile)
        config["datastore", "datastore", "datastores", 0, "datastore", "root"] = (
            f"{datastore_root}/butler_test_repository"
        )
        config["datastore", "datastore", "datastores", 1, "datastore", "root"] = (
            f"{datastore_root}/butler_test_repository2"
        )
        make_repo_for_test(datastore_root, config=config, forceConfigRoot=False)
        make_repo_for_test(other_repo_root, config=config, forceConfigRoot=False)
        with (
            Butler(datastore_root, writeable=True) as source_butler,
            Butler(other_repo_root, writeable=True) as target_butler,
        ):
            _test_transfer_datasets_in_place(source_butler, target_butler)


def _test_transfer_datasets_in_place(source_butler: DirectButler, target_butler: DirectButler) -> None:
    metric_repo = MetricTestRepo.create_from_butler(
        source_butler,
        source_butler._config,
    )
    target_butler.transfer_dimension_records_from(source_butler, [metric_repo.ref1, metric_repo.ref2])
    # Verify that the setup was correct and the two repos have
    # independent registries.
    assert target_butler.get_dataset(metric_repo.ref1.id) is None
    # Copy one dataset, and make sure we can load it from the
    # target repo.
    assert transfer_datasets_in_place(source_butler, target_butler, [metric_repo.ref1]) == [metric_repo.ref1]
    assert target_butler.get(metric_repo.ref1) == source_butler.get(metric_repo.ref1)
    assert target_butler.get_dataset(metric_repo.ref2.id) is None
    assert source_butler.getURIs(metric_repo.ref1) == target_butler.getURIs(metric_repo.ref1)
    # Trying to copy the same dataset again is a no-op.
    assert transfer_datasets_in_place(source_butler, target_butler, [metric_repo.ref1]) == []
    assert target_butler.get(metric_repo.ref1) == source_butler.get(metric_repo.ref1)
    # A mix of existing and non-existing datasets.
    assert transfer_datasets_in_place(source_butler, target_butler, [metric_repo.ref1, metric_repo.ref2]) == [
        metric_repo.ref2
    ]
    assert target_butler.get(metric_repo.ref1) == source_butler.get(metric_repo.ref1)
    assert target_butler.get(metric_repo.ref2) == source_butler.get(metric_repo.ref2)

    # For testing datastore chaining, set up a dataset that is only
    # accepted by one of the datastores.
    source_butler.registry.registerDatasetType(
        DatasetType("rejected_by_first", source_butler.dimensions.conform([]), "int")
    )
    source_butler.registry.registerRun("run")
    ref = source_butler.put(1, "rejected_by_first", dataId={}, run="run")
    assert transfer_datasets_in_place(source_butler, target_butler, [ref]) == [ref]
    assert 1 == target_butler.get(ref)
