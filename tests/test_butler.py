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

import contextlib
import json
import logging
import os
import pathlib
import pickle
import re
import tempfile
import unittest.mock
import uuid
import warnings
import weakref
from collections.abc import Callable, Iterator, Mapping
from typing import Any, cast

import pytest
from butler_test_support import (
    AXIS_NAMES,
    BUTLER_TESTS_AXES,
    FILE_DATASTORE_AXES,
    PUT_GET_AXES,
    assert_get_components,
    records_from,
    run_put_get_test,
)
from sqlalchemy.exc import IntegrityError

from lsst.daf.butler import (
    Butler,
    ButlerConfig,
    ButlerMetrics,
    ButlerRepoIndex,
    CollectionCycleError,
    CollectionType,
    Config,
    DataCoordinate,
    DatasetExistence,
    DatasetProvenance,
    DatasetRef,
    DatasetType,
    DimensionRecord,
    FileDataset,
    NoDefaultCollectionError,
    StorageClassFactory,
    ValidationError,
    script,
)
from lsst.daf.butler._rubin.file_datasets import transfer_datasets_to_datastore
from lsst.daf.butler._rubin.temporary_for_ingest import TemporaryForIngest
from lsst.daf.butler._rubin.transfer_datasets_in_place import transfer_datasets_in_place
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
from lsst.daf.butler.tests import MetricsExample, MetricsExampleModel, MultiDetectorFormatter
from lsst.daf.butler.tests._repo_template_cache import make_repo_for_test
from lsst.daf.butler.tests.dict_convertible_model import DictConvertibleModel
from lsst.daf.butler.tests.fixtures import (
    DATASTORE_PROFILES,
    ButlerHarness,
    ButlerRepo,
    ServerButlerHarness,
    add_dataset_type,
    get_test_data_path,
    make_example_metrics,
)
from lsst.daf.butler.tests.server_available import butler_server_import_error, butler_server_is_available
from lsst.daf.butler.tests.utils import MetricTestRepo, create_populated_sqlite_registry, safeTestTempDir
from lsst.resources import ResourcePath
from lsst.resources.http import HttpResourcePath
from lsst.resources.tests import make_remote_test_uri
from lsst.utils import doImportType
from lsst.utils.introspection import get_full_type_name

BUTLER_LOGGER = "lsst.daf.butler"
"""Root of the loggers the config search path test watches."""

FILE_TEMPLATE_LOGGER = "lsst.daf.butler.datastore.file_templates"
"""Logger that reports a template referring to a missing record field."""

OUTFILE_LAYOUTS = ["outfile", "outfile_dir", "outfile_uri"]
"""Repository layouts where makeRepo wrote the config outside the repo."""


def test_search_path(test_directory: str, caplog: pytest.LogCaptureFixture) -> None:
    """Test that searchPaths brings in an overriding config directory."""
    config_file = os.path.join(test_directory, "config", "basic", "butler.yaml")

    with caplog.at_level(logging.DEBUG, logger=BUTLER_LOGGER):
        caplog.clear()
        config1 = ButlerConfig(config_file)
        records = records_from(caplog, BUTLER_LOGGER, logging.DEBUG)
    assert records
    assert "testConfigs" not in "\n".join(record.getMessage() for record in records)

    override_directory = os.path.join(test_directory, "config", "testConfigs")
    with caplog.at_level(logging.DEBUG, logger=BUTLER_LOGGER):
        caplog.clear()
        config2 = ButlerConfig(config_file, searchPaths=[override_directory])
        records = records_from(caplog, BUTLER_LOGGER, logging.DEBUG)
    assert records
    assert "testConfigs" in "\n".join(record.getMessage() for record in records)

    key = ("datastore", "records", "table")
    assert config1[key] != config2[key]
    assert config2[key] == "override_record"


@pytest.mark.parametrize("repo_layout", ["explicit_root"], indirect=True)
def test_file_locations(butler_repo: ButlerRepo) -> None:
    """Test that a yaml file in one location can refer to a root in another."""
    dir1, dir2 = butler_repo.dir1, butler_repo.dir2
    assert dir1 is not None
    assert dir2 is not None
    assert dir1 != dir2
    assert os.path.exists(os.path.join(dir2, "butler2.yaml"))
    assert not os.path.exists(os.path.join(dir1, "butler.yaml"))
    assert os.path.exists(os.path.join(dir1, "gen3.sqlite3"))


@pytest.mark.parametrize("repo_layout", OUTFILE_LAYOUTS, indirect=True)
def test_config_existence(butler_repo: ButlerRepo, repo_layout: str) -> None:
    """Test that a config file makeRepo wrote outside the repo points back."""
    config_file = butler_repo.config_file
    if repo_layout == "outfile_dir":
        # Append the yaml file else the Config constructor does not know the
        # file type.
        config_file = os.path.join(config_file, "butler.yaml")

    c = Config(config_file)
    uri_config = ResourcePath(c["root"])
    uri_expected = ResourcePath(butler_repo.root, forceDirectory=True)
    assert uri_config.geturl() == uri_expected.geturl()
    assert ":" not in uri_config.path, "Check for URI concatenated with normal path"


@pytest.mark.parametrize("repo_layout", OUTFILE_LAYOUTS, indirect=True)
def test_put_get(butler_harness: ButlerHarness) -> None:
    """Test that a repository opened through such a config works normally."""
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    run_put_get_test(butler_harness, storage_class, "test_metric")


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_make_repo(
    butler_harness: ButlerHarness,
    test_directory: str,
    datastore_type: str,
    registry_backend: str,
    butler_client: str,
) -> None:
    """Test that we can write butler configuration to a new repository via
    the Butler.makeRepo interface and then instantiate a butler from the
    repo root.
    """
    if butler_client == "server":
        # Only applies to DirectButler.
        return
    if registry_backend == "postgres":
        # This test assumes that it is using sqlite and that the config file
        # on disk is acceptable to sqlite.
        pytest.skip("Postgres config is not compatible with this test.")

    full_config_key = butler_harness.profile.full_config_key
    if full_config_key is None:
        # Do not run the test if we know this datastore configuration does
        # not support a file system root.
        return

    source_config = os.path.join(test_directory, DATASTORE_PROFILES[datastore_type].config_file)
    root = butler_harness.root

    # create two separate directories
    root1 = tempfile.mkdtemp(dir=root)
    root2 = tempfile.mkdtemp(dir=root)

    with contextlib.ExitStack() as stack:
        # This test asserts on repository creation itself, so it must not go
        # through the caching helper.
        assert not Butler.has_repo_config(root1)
        butler_config = Butler.makeRepo(root1, config=Config(source_config))
        assert Butler.has_repo_config(root1)
        limited = Config(source_config)
        butler1 = stack.enter_context(Butler.from_config(butler_config))
        assert isinstance(butler1, DirectButler), "Expect DirectButler in configuration"
        butler_config = Butler.makeRepo(root2, standalone=True, config=Config(source_config))
        full = Config(butler_harness.config_file)
        butler2 = stack.enter_context(Butler.from_config(butler_config))
        assert isinstance(butler2, DirectButler), "Expect DirectButler in configuration"
        # Butlers should have the same configuration regardless of whether
        # defaults were expanded.
        assert butler1._config == butler2._config
        # Config files loaded directly should not be the same.
        assert limited != full
        # Make sure "limited" doesn't have a few keys we know it should be
        # inheriting from defaults.
        assert full_config_key in full
        assert full_config_key not in limited

        # Collections don't appear until something is put in them
        collections1 = set(butler1.registry.queryCollections())
        assert collections1 == set()
        assert set(butler2.registry.queryCollections()) == collections1

        # Check that a config with no associated file name will not
        # work properly with relocatable Butler repo
        butler_config.configFile = None
        with pytest.raises(ValueError, match="Required to replace <butlerRoot>"):
            Butler.from_config(butler_config)

        with pytest.raises(FileExistsError):
            Butler.makeRepo(root, standalone=True, config=Config(source_config), overwrite=False)


@pytest.mark.parametrize(AXIS_NAMES, FILE_DATASTORE_AXES, indirect=True)
def test_put_templates(
    butler_harness: ButlerHarness, butler_client: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that datasets land at the paths the file templates describe."""
    if butler_client == "server":
        # The Butler server instance is configured with different file naming
        # templates than this test is expecting.
        return

    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    default_run = butler_harness.default_run
    butler = butler_harness.create_empty_butler(run=default_run)

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
    metric = make_example_metrics()

    # Create two almost-identical DatasetTypes (both will use default
    # template)
    dimensions = butler.dimensions.conform(["instrument", "visit"])
    butler.registry.registerDatasetType(DatasetType("metric1", dimensions, storage_class))
    butler.registry.registerDatasetType(DatasetType("metric2", dimensions, storage_class))
    butler.registry.registerDatasetType(DatasetType("metric3", dimensions, storage_class))

    data_id1 = {"instrument": "DummyCamComp", "visit": 423}
    data_id2 = {"instrument": "DummyCamComp", "visit": 423, "physical_filter": "d-r"}

    # Put with exactly the data ID keys needed
    ref = butler.put(metric, "metric1", data_id1)
    uri = butler.getURI(ref)
    assert uri.exists()
    assert uri.unquoted_path.endswith(f"{default_run}/metric1/??#?/d-r/DummyCamComp_423.pickle")

    # Check the template based on dimensions
    if hasattr(butler._datastore, "templates"):
        butler._datastore.templates.validateTemplates([ref])

    # Put with extra data ID keys (physical_filter is an optional
    # dependency); should not change template (at least the way we're
    # defining them  to behave now; the important thing is that they
    # must be consistent).
    ref = butler.put(metric, "metric2", data_id2)
    uri = butler.getURI(ref)
    assert uri.exists()
    assert uri.unquoted_path.endswith(f"{default_run}/metric2/d-r/DummyCamComp_v423.pickle")

    # Check the template based on dimensions
    if hasattr(butler._datastore, "templates"):
        butler._datastore.templates.validateTemplates([ref])

    # Use a template that has a typo in dimension record metadata.
    # Easier to test with a butler that has a ref with records attached.
    template = FileTemplate("a/{visit.name}/{id}_{visit.namex:?}.fits")
    with caplog.at_level(logging.INFO, logger=FILE_TEMPLATE_LOGGER):
        caplog.clear()
        path = template.format(ref)
        assert records_from(caplog, FILE_TEMPLATE_LOGGER, logging.INFO)
    assert path == f"a/v423/{ref.id}_fits"

    # Without the "?" the same typo is an error rather than a warning.
    template = FileTemplate("a/{visit.name}/{id}_{visit.namex}.fits")
    with pytest.raises(KeyError):
        template.format(ref)

    # Now use a file template that will not result in unique filenames
    with pytest.raises(FileTemplateValidationError):
        butler.put(metric, "metric3", data_id1)


COMPONENT_WARNING_LOGGER = "lsst.daf.butler.datastores.file_datastore.get"
"""Logger that warns when a component has to be extracted by conversion."""


@pytest.mark.parametrize(AXIS_NAMES, PUT_GET_AXES, indirect=True)
def test_deferred_collection_passing(butler_harness: ButlerHarness) -> None:
    # Construct a butler with no run or collection, but make it writeable.
    butler = butler_harness.create_empty_butler(writeable=True)
    # Create and register a DatasetType
    dimensions = butler.dimensions.conform(["instrument", "visit"])
    dataset_type = add_dataset_type(
        "example",
        dimensions,
        butler_harness.storage_class_factory.getStorageClass("StructuredData"),
        butler.registry,
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
    data_id = {"instrument": "DummyCamComp", "visit": 423}
    # Create dataset.
    metric = make_example_metrics()
    # Register a new run and put dataset.
    run = "deferred"
    assert butler.collections.register(run)
    # Second time it will be allowed but indicate no-op
    assert not butler.collections.register(run)
    ref = butler.put(metric, dataset_type, data_id, run=run)
    # Putting with no run should fail with TypeError.
    with pytest.raises(CollectionError):
        butler.put(metric, dataset_type, data_id)
    # Dataset should exist.
    assert butler.exists(dataset_type, data_id, collections=[run])
    # We should be able to get the dataset back, but with and without
    # a deferred dataset handle.
    assert metric == butler.get(dataset_type, data_id, collections=[run])
    assert metric == butler.getDeferred(dataset_type, data_id, collections=[run]).get()
    # Trying to find the dataset without any collection is an error.
    with pytest.raises(NoDefaultCollectionError):
        butler.exists(dataset_type, data_id)
    with pytest.raises(CollectionError):
        butler.get(dataset_type, data_id)
    # Associate the dataset with a different collection.
    butler.collections.register("tagged", type=CollectionType.TAGGED)
    butler.registry.associate("tagged", [ref])
    # Deleting the dataset from the new collection should make it findable
    # in the original collection.
    butler.pruneDatasets([ref], tags=["tagged"])
    assert butler.exists(dataset_type, data_id, collections=[run])


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_basic_put_get(butler_harness: ButlerHarness) -> None:
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    run_put_get_test(butler_harness, storage_class, "test_metric")


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_composite_put_get_concrete(butler_harness: ButlerHarness) -> None:
    storage_class = butler_harness.storage_class_factory.getStorageClass(
        "StructuredCompositeReadCompNoDisassembly"
    )
    butler = run_put_get_test(butler_harness, storage_class, "test_metric")

    # Should *not* be disassembled
    datasets = list(butler.registry.queryDatasets(..., collections=butler_harness.default_run))
    assert len(datasets) == 1
    uri, components = butler.getURIs(datasets[0])
    assert isinstance(uri, ResourcePath)
    assert not components
    assert uri.fragment == "", f"Checking absence of fragment in {uri}"
    assert "423" in str(uri), f"Checking visit is in URI {uri}"

    # Predicted dataset
    if butler_harness.prediction_supported:
        data_id: dict[str, int | str] = {"instrument": "DummyCamComp", "visit": 424}
        uri, components = butler.getURIs(datasets[0].datasetType, dataId=data_id, predict=True)
        assert not components
        assert isinstance(uri, ResourcePath)
        assert "424" in str(uri), f"Checking visit is in URI {uri}"
        assert uri.fragment == "predicted", f"Checking for fragment in {uri}"
        # Repeat with a DatasetRef to test that code path.
        ref = DatasetRef(
            datasets[0].datasetType,
            dataId=DataCoordinate.standardize(data_id, universe=butler.dimensions),
            run=butler_harness.default_run,
        )
        uri2, components2 = butler.getURIs(ref, predict=True)
        assert not components2
        assert uri == uri2


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_composite_put_get_virtual(butler_harness: ButlerHarness) -> None:
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredCompositeReadComp")
    butler = run_put_get_test(butler_harness, storage_class, "test_metric_comp")

    # Should be disassembled
    datasets = list(butler.registry.queryDatasets(..., collections=butler_harness.default_run))
    assert len(datasets) == 1
    uri, components = butler.getURIs(datasets[0])

    if butler._datastore.isEphemeral:
        # Never disassemble in-memory datastore
        assert isinstance(uri, ResourcePath)
        assert not components
        assert uri.fragment == "", f"Checking absence of fragment in {uri}"
        assert "423" in str(uri), f"Checking visit is in URI {uri}"
    else:
        assert uri is None
        assert set(components) == set(storage_class.components)
        for compuri in components.values():
            assert isinstance(compuri, ResourcePath)
            assert "423" in str(compuri), f"Checking visit is in URI {compuri}"
            assert compuri.fragment == "", f"Checking absence of fragment in {compuri}"

    if butler_harness.prediction_supported:
        # Predicted dataset
        data_id = {"instrument": "DummyCamComp", "visit": 424}
        uri, components = butler.getURIs(datasets[0].datasetType, dataId=data_id, predict=True)

        if butler._datastore.isEphemeral:
            # Never disassembled
            assert isinstance(uri, ResourcePath)
            assert not components
            assert "424" in str(uri), f"Checking visit is in URI {uri}"
            assert uri.fragment == "predicted", f"Checking for fragment in {uri}"
        else:
            assert uri is None
            assert set(components) == set(storage_class.components)
            for compuri in components.values():
                assert isinstance(compuri, ResourcePath)
                assert "424" in str(compuri), f"Checking visit is in URI {compuri}"
                assert compuri.fragment == "predicted", f"Checking for fragment in {compuri}"


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_storage_class_override_get(butler_harness: ButlerHarness) -> None:
    """Test storage class conversion on get with override."""
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredData")
    dataset_type_name = "anything"
    run = butler_harness.default_run

    butler, dataset_type = butler_harness.create_butler(run, storage_class, dataset_type_name)

    # Create and store a dataset.
    metric = make_example_metrics()
    data_id = {"instrument": "DummyCamComp", "visit": 423}

    ref = butler.put(metric, dataset_type, data_id)

    # Return native type.
    retrieved = butler.get(ref)
    assert retrieved == metric

    # Specify an override.
    new_sc = butler_harness.storage_class_factory.getStorageClass("MetricsConversion")
    model = butler.get(ref, storageClass=new_sc)
    assert type(model) is not type(retrieved)
    assert type(model) is new_sc.pytype
    assert retrieved == model

    # Defer but override later.
    deferred = butler.getDeferred(ref)
    model = deferred.get(storageClass=new_sc)
    assert type(model) is new_sc.pytype
    assert retrieved == model

    # Defer but override up front.
    deferred = butler.getDeferred(ref, storageClass=new_sc)
    model = deferred.get()
    assert type(model) is new_sc.pytype
    assert retrieved == model

    # Retrieve a component. Should be a tuple.
    data = butler.get("anything.data", data_id, storageClass="StructuredDataDataTestTuple")
    assert type(data) is tuple
    assert data == tuple(retrieved.data)

    # Parameter on the write storage class should work regardless
    # of read storage class.
    data = butler.get(
        "anything.data",
        data_id,
        storageClass="StructuredDataDataTestTuple",
        parameters={"slice": slice(2, 4)},
    )
    assert len(data) == 2

    # Try a parameter that is known to the read storage class but not
    # the write storage class.
    with pytest.raises(KeyError):
        butler.get(
            "anything.data",
            data_id,
            storageClass="StructuredDataDataTestTuple",
            parameters={"xslice": slice(2, 4)},
        )


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_component_from_overridden_storage_class(butler_harness: ButlerHarness) -> None:
    """Test component get where the component is only defined by the
    read storage class and not by the storage class used to write.
    """
    # StructuredDataNoComponents defines no components at all, whereas
    # MetricsConversion (which it can be converted to) defines several.
    write_sc = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    read_sc = butler_harness.storage_class_factory.getStorageClass("MetricsConversion")
    assert not write_sc.allComponents()
    assert "summary" in read_sc.allComponents()

    butler, dataset_type = butler_harness.create_butler(butler_harness.default_run, write_sc, "unstructured")

    metric = make_example_metrics()
    data_id = {"instrument": "DummyCamComp", "visit": 423}
    ref = butler.put(metric, dataset_type, data_id)

    # The composite conversion on its own must work.
    assert type(butler.get(ref, storageClass=read_sc)) is read_sc.pytype

    # A component of the converted composite, requested via a ref.
    component_ref = ref.overrideStorageClass(read_sc).makeComponentRef("summary")
    assert butler.get(component_ref) == metric.summary

    # The same component, requested via a deferred handle that was given
    # the storage class override up front.
    deferred = butler.getDeferred(ref, storageClass=read_sc)
    assert deferred.get(component="summary") == metric.summary

    # A component whose storage class is also overridden, on top of the
    # storage class the read composite declares for it.
    converted = butler.get(component_ref, storageClass="DictConvertibleModel")
    assert isinstance(converted, DictConvertibleModel)
    assert converted.content == metric.summary

    # The handle storage class applies to the composite and so selects the
    # component, while the one given to get() applies to the component.
    converted = deferred.get(component="summary", storageClass="DictConvertibleModel")
    assert isinstance(converted, DictConvertibleModel)
    assert converted.content == metric.summary


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_pytype_put_coercion(butler_harness: ButlerHarness) -> None:
    """Test python type coercion on Butler.get and put."""
    # Store some data with the normal example storage class.
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    dataset_type_name = "test_metric"
    butler, _ = butler_harness.create_butler(butler_harness.default_run, storage_class, dataset_type_name)

    data_id = {"instrument": "DummyCamComp", "visit": 423}

    # Put a dict and this should coerce to a MetricsExample
    test_dict = {"summary": {"a": 1}, "output": {"b": 2}}
    metric_ref = butler.put(test_dict, dataset_type_name, dataId=data_id, visit=424)
    test_metric = butler.get(metric_ref)
    assert get_full_type_name(test_metric) == "lsst.daf.butler.tests.MetricsExample"
    assert test_metric.summary == test_dict["summary"]
    assert test_metric.output == test_dict["output"]

    # Check that the put still works if a DatasetType is given with
    # a definition matching this python type.
    registry_type = butler.get_dataset_type(dataset_type_name)
    this_type = DatasetType(dataset_type_name, registry_type.dimensions, "StructuredDataDictJson")
    metric2_ref = butler.put(test_dict, this_type, dataId=data_id, visit=425)
    assert metric2_ref.datasetType == registry_type

    # The get will return the type expected by registry.
    test_metric2 = butler.get(metric2_ref)
    assert get_full_type_name(test_metric2) == "lsst.daf.butler.tests.MetricsExample"

    # Make a new DatasetRef with the compatible but different DatasetType.
    # This should now return a dict.
    new_ref = DatasetRef(this_type, metric2_ref.dataId, id=metric2_ref.id, run=metric2_ref.run)
    test_dict2 = butler.get(new_ref)
    assert get_full_type_name(test_dict2) == "dict"

    # Get it again with the wrong dataset type definition using get()
    # rather than get(). This should be consistent with get()
    # behavior and return the type of the DatasetType.
    test_dict3 = butler.get(this_type, dataId=data_id, visit=425)
    assert get_full_type_name(test_dict3) == "dict"


@pytest.mark.parametrize(AXIS_NAMES, FILE_DATASTORE_AXES, indirect=True)
def test_component_from_overridden_storage_class_warns(
    butler_harness: ButlerHarness, datastore_type: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that getting a component that only the read storage class
    defines warns, since the whole dataset has to be retrieved and
    converted before the component can be extracted.
    """
    if datastore_type == "chained":
        # The InMemoryDatastore in the ChainedDatastore satisfies the get, so
        # the FileDatastore warning about having to read the whole dataset to
        # extract the component is never issued.
        return

    write_sc = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    read_sc = butler_harness.storage_class_factory.getStorageClass("MetricsConversion")
    butler, dataset_type = butler_harness.create_butler(butler_harness.default_run, write_sc, "unstructured")
    metric = make_example_metrics()
    data_id = {"instrument": "DummyCamComp", "visit": 423}
    ref = butler.put(metric, dataset_type, data_id)
    component_ref = ref.overrideStorageClass(read_sc).makeComponentRef("summary")

    with caplog.at_level(logging.WARNING, logger=COMPONENT_WARNING_LOGGER):
        caplog.clear()
        assert butler.get(component_ref) == metric.summary
        records = records_from(caplog, COMPONENT_WARNING_LOGGER, logging.WARNING)
    assert records
    message = "\n".join(record.getMessage() for record in records)
    # The message must name the component, the storage class that lacks it
    # along with the components it does have, and the storage class the
    # dataset has to be converted to.
    assert "summary" in message
    assert write_sc.name in message
    assert "components it does define: none" in message
    assert read_sc.name in message
    assert "less efficient" in message

    # Reading a component that the write storage class does define must not
    # warn.
    composite_type = add_dataset_type("composite", dataset_type.dimensions, "StructuredData", butler.registry)
    composite_ref = butler.put(metric, composite_type, data_id)
    with caplog.at_level(logging.WARNING, logger=COMPONENT_WARNING_LOGGER):
        caplog.clear()
        assert butler.get(composite_ref.makeComponentRef("summary")) == metric.summary
        assert not records_from(caplog, COMPONENT_WARNING_LOGGER, logging.WARNING)


LOCAL_LAYOUTS = ["in_repo", "explicit_root"]
"""Repository layouts of the two classes the posix-only tests ran under."""

PICKLE_AXES = [
    pytest.param(
        *param.values,
        id=param.id,
        marks=[
            *param.marks,
            pytest.mark.xfail(reason="Pickling not yet implemented for RemoteButler/HybridButler."),
        ],
    )
    if isinstance(param.id, str) and param.id.startswith("server")
    else param
    for param in BUTLER_TESTS_AXES
]
"""BUTLER_TESTS_AXES with the server axes marked as expected to fail."""


class TransactionTestError(Exception):
    """Specific error for testing transactions, to prevent misdiagnosing
    that might otherwise occur when a standard exception is used.
    """


@pytest.fixture(autouse=True, scope="module")
def _clean_environment() -> Iterator[None]:
    """Remove external environment variables that affect these tests.

    Only this file needs it: the repository index variable is read by the
    constructor and repository-alias tests here and nowhere else in the
    migrated set.
    """
    saved = os.environ.pop("DAF_BUTLER_REPOSITORY_INDEX", None)
    yield
    if saved is not None:
        os.environ["DAF_BUTLER_REPOSITORY_INDEX"] = saved


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_constructor(butler_harness: ButlerHarness, butler_client: str) -> None:
    """Independent test of constructor."""
    if butler_client == "server":
        # RemoteButler constructor is tested in test_server.py and
        # test_remote_butler.py.
        return

    config_file = butler_harness.config_file
    default_run = butler_harness.default_run
    stack = contextlib.ExitStack()
    butler = butler_harness.create_empty_butler(run=default_run)
    assert isinstance(butler, Butler)

    # Check that butler.yaml is added automatically.
    if config_file.endswith(end := "/butler.yaml"):
        config_dir = config_file[: -len(end)]
        butler = stack.enter_context(Butler.from_config(config_dir, run=default_run))
        assert isinstance(butler, Butler)

        # Even with a ResourcePath.
        butler = stack.enter_context(
            Butler.from_config(ResourcePath(config_dir, forceDirectory=True), run=default_run)
        )
        assert isinstance(butler, Butler)

    collections = set(butler.collections.query("*"))
    assert collections == {default_run}

    # Check that some special characters can be included in run name.
    special_run = "u@b.c-A"
    with Butler.from_config(butler=butler, run=special_run) as butler_special:
        collections = set(butler_special.registry.queryCollections("*@*"))
        assert collections == {special_run}

    with Butler.from_config(butler=butler, collections=["other"]) as butler2:
        assert butler2.collections.defaults == ("other",)
        assert butler2.run is None
        assert type(butler._datastore) is type(butler2._datastore)
        assert butler._datastore.config == butler2._datastore.config

    # Test that we can use an environment variable to find this
    # repository.
    butler_index = Config()
    butler_index["label"] = config_file
    for suffix in (".yaml", ".json"):
        # Ensure that the content differs so that we know that
        # we aren't reusing the cache.
        bad_label = f"file://bucket/not_real{suffix}"
        butler_index["bad_label"] = bad_label
        with ResourcePath.temporary_uri(suffix=suffix) as temp_file:
            butler_index.dumpToUri(temp_file)
            with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": str(temp_file)}):
                assert Butler.get_known_repos() == {"label", "bad_label"}
                uri = Butler.get_repo_uri("bad_label")
                assert uri == ResourcePath(bad_label)
                uri = Butler.get_repo_uri("label")
                butler = Butler.from_config(uri, writeable=False)
                assert isinstance(butler, Butler)
                butler.close()
                butler = Butler.from_config("label", writeable=False)
                assert isinstance(butler, Butler)
                butler.close()
                with pytest.raises(FileNotFoundError, match="aliases:.*bad_label"):
                    Butler.from_config("not_there", writeable=False)
                with pytest.raises(FileNotFoundError, match="resolved from alias 'bad_label'"):
                    Butler.from_config("bad_label")
                with pytest.raises(FileNotFoundError):
                    # Should ignore aliases.
                    Butler.from_config(ResourcePath("label", forceAbsolute=False))
                with pytest.raises(KeyError, match="not known to") as exc_info:
                    Butler.get_repo_uri("missing")
                assert Butler.get_repo_uri("missing", True) == ResourcePath("missing", forceAbsolute=False)
                assert "not known to" in str(exc_info.value)
                # Should report no failure.
                assert ButlerRepoIndex.get_failure_reason() == ""
    with ResourcePath.temporary_uri(suffix=suffix) as temp_file:
        # Now with empty configuration.
        butler_index = Config()
        butler_index.dumpToUri(temp_file)
        with (
            unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": str(temp_file)}),
            pytest.raises(FileNotFoundError, match="(no known aliases)"),
        ):
            Butler.from_config("label")
    with ResourcePath.temporary_uri(suffix=suffix) as temp_file:
        # Now with bad contents.
        with open(temp_file.ospath, "w") as fh:
            print("'", file=fh)
        with (
            unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": str(temp_file)}),
            pytest.raises(FileNotFoundError, match="(no known aliases:.*could not be read)"),
        ):
            Butler.from_config("label")
    with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORY_INDEX": "file://not_found/x.yaml"}):
        with pytest.raises(FileNotFoundError):
            Butler.get_repo_uri("label")
        assert Butler.get_known_repos() == set()

        with pytest.raises(FileNotFoundError, match="index file not found"):
            Butler.from_config("label")

        # Check that we can create Butler when the alias file is not found.
        butler = butler_harness.create_empty_butler(writeable=False)
        assert isinstance(butler, Butler)
    with pytest.raises(RuntimeError, match="No repository index defined") as runtime_info:
        # No environment variable set.
        Butler.get_repo_uri("label")
    assert Butler.get_repo_uri("label", True) == ResourcePath("label", forceAbsolute=False)
    assert "No repository index defined" in str(runtime_info.value)
    with pytest.raises(FileNotFoundError, match="no known aliases.*No repository index"):
        # No aliases registered.
        Butler.from_config("not_there")
    assert Butler.get_known_repos() == set()
    stack.close()


@pytest.mark.parametrize("repo_layout", LOCAL_LAYOUTS, indirect=True)
def test_path_constructor(butler_harness: ButlerHarness) -> None:
    """Independent test of constructor using PathLike."""
    config_file = butler_harness.config_file
    butler = butler_harness.create_empty_butler(run=butler_harness.default_run)
    assert isinstance(butler, Butler)

    with contextlib.ExitStack() as stack:
        # And again with a Path object with the butler yaml
        path = pathlib.Path(config_file)
        butler = stack.enter_context(Butler.from_config(path, writeable=False))
        assert isinstance(butler, Butler)

        # And again with a Path object without the butler yaml
        # (making sure we skip it if the config doesn't end in butler.yaml,
        # which is the case for the explicit-root layout)
        if config_file.endswith("butler.yaml"):
            path = pathlib.Path(os.path.dirname(config_file))
            butler = stack.enter_context(Butler.from_config(path, writeable=False))
            assert isinstance(butler, Butler)


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_close(butler_harness: ButlerHarness) -> None:
    butler = butler_harness.create_empty_butler(cleanup=False)
    # A RemoteButler has no _closed flag, so only the direct case can check it.
    direct_butler = butler if isinstance(butler, DirectButler) else None
    if direct_butler is not None:
        assert not direct_butler._closed

    with butler as butler_from_context_manager:
        assert butler is butler_from_context_manager
    if direct_butler is not None:
        assert direct_butler._closed
        with pytest.raises(RuntimeError, match="has been closed"):
            butler.get_dataset_type("raw")

    # Close may be called multiple times.
    butler.close()
    if direct_butler is not None:
        assert direct_butler._closed


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_garbage_collection(butler_harness: ButlerHarness) -> None:
    """Test that Butler does not have any circular references that prevent
    it from being garbage collected immediately when it goes out of scope.
    """
    butler = butler_harness.create_empty_butler(cleanup=False)
    is_direct_butler = isinstance(butler, DirectButler)
    butler_ref = weakref.ref(butler)
    # Narrowed with isinstance rather than the flag so that no second strong
    # reference to the butler outlives the `del` below.
    if isinstance(butler, DirectButler):
        registry_ref = weakref.ref(butler._registry)
        managers_ref = weakref.ref(butler._registry._managers)
        datastore_ref = weakref.ref(butler._datastore)
        db_ref = weakref.ref(butler._registry._db)
        engine_ref = weakref.ref(butler._registry._db._engine)

    with warnings.catch_warnings():
        # Hide warnings from unclosed database handles.
        warnings.simplefilter("ignore", ResourceWarning)
        del butler
        assert butler_ref() is None, "Butler should have been garbage collected"
        if is_direct_butler:
            assert registry_ref() is None, "SqlRegistry should have been garbage collected"
            assert managers_ref() is None, "Registry managers should have been garbage collected"
            assert datastore_ref() is None, "Datastore should have been garbage collected"
            assert db_ref() is None, "Database should have been garbage collected"
            # SQLAlchemy has internal reference cycles, so the Engine instance
            # is not cleaned up promptly even if we release our reference to
            # it.  Explicitly clean it up here to avoid file handles leaking.
            engine = engine_ref()
            if engine is not None:
                engine.dispose()


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_daf_butler_repositories(butler_harness: ButlerHarness, butler_client: str) -> None:
    # butler_harness is requested but unused: the original built a repository
    # in setUp for every one of these runs, and the axis parametrization
    # needs the whole fixture closure.
    if butler_client == "server":
        # Loading of RemoteButler via repository index is tested in
        # test_server.py.
        return

    with unittest.mock.patch.dict(
        os.environ,
        {"DAF_BUTLER_REPOSITORIES": "label: 'https://someuri.com'\notherLabel: 'https://otheruri.com'\n"},
    ):
        assert str(Butler.get_repo_uri("label")) == "https://someuri.com"

    with (
        unittest.mock.patch.dict(
            os.environ,
            {
                "DAF_BUTLER_REPOSITORIES": "label: https://someuri.com",
                "DAF_BUTLER_REPOSITORY_INDEX": "https://someuri.com",
            },
        ),
        pytest.raises(RuntimeError, match="Only one of the environment variables"),
    ):
        Butler.get_repo_uri("label")

    with (
        unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_REPOSITORIES": "invalid"}),
        pytest.raises(ValueError, match="Repository index not in expected format"),
    ):
        Butler.get_repo_uri("label")


@pytest.mark.parametrize(AXIS_NAMES, PICKLE_AXES, indirect=True)
def test_pickle(butler_harness: ButlerHarness) -> None:
    """Test pickle support."""
    butler = butler_harness.create_empty_butler(run=butler_harness.default_run)
    assert isinstance(butler, DirectButler), "Expect DirectButler in configuration"
    with pickle.loads(pickle.dumps(butler)) as butler_out:
        assert isinstance(butler_out, DirectButler)
        assert butler_out._config == butler._config
        assert list(butler_out.collections.defaults) == list(butler.collections.defaults)
        assert butler_out.run == butler.run


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_transaction(butler_harness: ButlerHarness, butler_client: str) -> None:
    if butler_client == "server":
        # Transactions will never be supported for RemoteButler.
        return

    butler = butler_harness.create_empty_butler(run=butler_harness.default_run)
    dataset_type_name = "test_metric"
    dimensions = butler.dimensions.conform(["instrument", "visit"])
    dimension_entries: tuple[tuple[str, Mapping[str, Any]], ...] = (
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
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredData")
    metric = make_example_metrics()
    data_id = {"instrument": "DummyCam", "visit": 42}
    # Create and register a DatasetType
    dataset_type = add_dataset_type(dataset_type_name, dimensions, storage_class, butler.registry)
    with pytest.raises(TransactionTestError), butler.transaction():  # noqa: PT012
        # Add needed Dimensions
        for args in dimension_entries:
            butler.registry.insertDimensionData(*args)
        # Store a dataset
        ref = butler.put(metric, dataset_type_name, data_id)
        assert isinstance(ref, DatasetRef)
        # Test get of a ref.
        metric_out = butler.get(ref)
        assert metric == metric_out
        # Test get
        metric_out = butler.get(dataset_type_name, data_id)
        assert metric == metric_out
        # Check we can get components
        assert_get_components(butler, ref, ("summary", "data", "output"), metric)
        raise TransactionTestError("This should roll back the entire transaction")

    with pytest.raises(DataIdValueError):
        butler.registry.expandDataId(data_id)
    # Should raise LookupError for missing data ID value
    with pytest.raises(LookupError):
        butler.get(dataset_type_name, data_id)
    # Also check explicitly if Dataset entry is missing
    assert butler.find_dataset(dataset_type, data_id, collections=butler.collections.defaults) is None
    # Direct retrieval should not find the file in the Datastore
    with pytest.raises(FileNotFoundError):
        butler.get(ref)


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_stringification(butler_harness: ButlerHarness, butler_client: str, registry_backend: str) -> None:
    if butler_client == "server":
        assert isinstance(butler_harness, ServerButlerHarness)
        assert (
            str(butler_harness.server_instance.remote_butler)
            == "RemoteButler(https://test.example/api/butler/repo/testrepo/)"
        )
        return

    profile = butler_harness.profile
    # The registry string is a property of the backend, not the datastore.
    registry_str = "PostgreSQL@test" if registry_backend == "postgres" else "/gen3.sqlite3"

    butler = butler_harness.create_empty_butler(run=butler_harness.default_run)
    butler_str = str(butler)

    for test_str in profile.datastore_str:
        assert test_str in butler_str
    assert registry_str in butler_str

    datastore_name = butler._datastore.name
    if profile.datastore_name is not None:
        for test_str in profile.datastore_name:
            assert test_str in datastore_name


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_butler_rewrite_data_id(butler_harness: ButlerHarness) -> None:
    """Test that dataIds can be rewritten based on dimension records."""
    default_run = butler_harness.default_run
    butler = butler_harness.create_empty_butler(run=default_run)

    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataDict")
    dataset_type_name = "random_data"

    # Create dimension records.
    butler.registry.insertDimensionData("instrument", {"name": "DummyCamComp"})
    butler.registry.insertDimensionData(
        "physical_filter", {"instrument": "DummyCamComp", "name": "d-r", "band": "R"}
    )
    butler.registry.insertDimensionData(
        "detector", {"instrument": "DummyCamComp", "id": 1, "full_name": "det1"}
    )

    dimensions = butler.dimensions.conform(["instrument", "exposure"])
    dataset_type = DatasetType(dataset_type_name, dimensions, storage_class)
    butler.registry.registerDatasetType(dataset_type)

    n_exposures = 5
    dayobs = 20210530

    # Create records for multiple day_obs but same seq_num to test that
    # we are constraining gets properly when day_obs/seq_num is used
    # for an exposure. Second day is year in future but is not used.
    for day_obs in (dayobs, dayobs + 1_00_00):
        butler.registry.insertDimensionData("day_obs", {"instrument": "DummyCamComp", "id": day_obs})

        for i in range(n_exposures):
            group_name = f"group_{day_obs}_{i}"
            butler.registry.insertDimensionData("group", {"instrument": "DummyCamComp", "name": group_name})
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
        data_id = {"seq_num": i, "day_obs": dayobs, "instrument": "DummyCamComp", "physical_filter": "d-r"}
        ref = butler.put(metric, dataset_type_name, dataId=data_id)

        # Check that the exposure is correct in the dataId
        assert ref.dataId["exposure"] == dayobs + i

        # and check that we can get the dataset back with the same dataId
        new_metric = butler.get(dataset_type_name, dataId=data_id)
        assert new_metric == metric

    # Check that we can find the datasets using the day_obs or the
    # exposure.day_obs.
    datasets_1 = list(
        butler.registry.queryDatasets(
            dataset_type,
            collections=default_run,
            where="day_obs = :dayObs AND instrument = :instr",
            bind={"dayObs": dayobs, "instr": "DummyCamComp"},
        )
    )
    datasets_2 = list(
        butler.registry.queryDatasets(
            dataset_type,
            collections=default_run,
            where="exposure.day_obs = :dayObs AND instrument = :instr",
            bind={"dayObs": dayobs, "instr": "DummyCamComp"},
        )
    )
    assert datasets_1 == datasets_2


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_transfer_dimension_records_from(butler_harness: ButlerHarness) -> None:
    source_butler = butler_harness.create_empty_butler(writeable=True)
    source_butler.import_(filename=get_test_data_path("lsstcam-subset.yaml"))

    visit_id = 2025120200439
    exposure_id = visit_id
    with create_populated_sqlite_registry() as target_butler:
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
            assert len(records) == 1
            return records[0]

        visit = _fetch_record("visit")
        assert visit.id == visit_id
        assert visit.day_obs == 20251202
        assert visit.target_name == "lowdust"
        assert visit.seq_num == 439
        original_visit = source_butler.query_dimension_records("visit", instrument="LSSTCam", visit=visit_id)[
            0
        ]
        assert visit.region == original_visit.region
        assert visit.timespan == original_visit.timespan

        visit_detector_region = _fetch_record("visit_detector_region")
        assert visit_detector_region.instrument == "LSSTCam"
        assert visit_detector_region.detector == 10
        assert visit_detector_region.visit == visit_id
        original_visit_detector_region = source_butler.query_dimension_records(
            "visit_detector_region", instrument="LSSTCam", visit=visit_id, detector=10
        )[0]
        assert visit_detector_region.region == original_visit_detector_region.region

        visit_definition = _fetch_record("visit_definition")
        assert visit_definition.instrument == "LSSTCam"
        assert visit_definition.exposure == 2025120200439
        assert visit_definition.visit == visit_id

        # The matching exposure record should have been pulled in via
        # visit -> visit_definition.
        exposure = _fetch_record("exposure")
        assert exposure.instrument == "LSSTCam"
        assert exposure.id == 2025120200439
        assert exposure.obs_id == "MC_O_20251202_000439"
        original_exposure = source_butler.query_dimension_records(
            "exposure", instrument="LSSTCam", exposure=exposure_id
        )[0]
        assert exposure.timespan == original_exposure.timespan

        group = _fetch_record("group")
        assert group.instrument == "LSSTCam"
        assert group.name == "2025-12-03T07:58:10.858"

        visit_system_memberships = target_butler.query_dimension_records("visit_system_membership")
        visit_system_memberships.sort(key=lambda record: record.visit_system)
        assert len(visit_system_memberships) == 2
        assert visit_system_memberships[0].visit_system == 0
        assert visit_system_memberships[1].visit_system == 2
        assert visit_system_memberships[0].visit == visit_id
        assert visit_system_memberships[1].visit == visit_id

        visit_systems = target_butler.query_dimension_records("visit_system")
        visit_systems.sort(key=lambda record: record.id)
        assert visit_systems[0].id == 0
        assert visit_systems[1].id == 2
        assert visit_systems[0].name == "one-to-one"
        assert visit_systems[1].name == "by-seq-start-end"


@pytest.mark.parametrize(AXIS_NAMES, FILE_DATASTORE_AXES, indirect=True)
def test_butler_metrics(butler_harness: ButlerHarness) -> None:
    """Test that metrics are collected."""
    run = "test_run"
    metrics = ButlerMetrics()
    butler, dataset_type = butler_harness.create_butler(
        run, "MetricsExampleModelProvenance", "prov_metric", metrics=metrics
    )
    data = MetricsExampleModel(
        summary={"AM1": 5.2, "AM2": 30.6},
        output={"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
        data=[563, 234, 456.7, 752, 8, 9, 27],
    )

    data_ref = butler.put(data, dataset_type, visit=424, instrument="DummyCamComp")
    butler.get(data_ref)
    butler.get(data_ref)
    assert metrics.n_get == 2
    assert metrics.time_in_get > 0.0
    assert metrics.n_put == 1
    assert metrics.time_in_put > 0.0

    deferred = butler.getDeferred(data_ref)
    deferred.get()
    assert metrics.n_get == 3

    with butler.record_metrics() as new:
        data_ref_2 = butler.put(data, dataset_type, visit=425, instrument="DummyCamComp")
        butler.get(data_ref)

        butler.pruneDatasets([data_ref, data_ref_2], purge=True, unstore=True)
        with ResourcePath.temporary_uri(suffix=".json") as tmp_file:
            tmp_file.write(data.model_dump_json().encode())
            refs = [
                DatasetRef(dataset_type, data_ref_2.dataId, run),
                DatasetRef(dataset_type, data_ref.dataId, run),
            ]
            datasets = [FileDataset(path=tmp_file, refs=refs)]
            butler.ingest(*datasets, transfer="copy")

    assert new.n_get == 1
    assert new.n_put == 1
    assert new.n_ingest == 2


@pytest.mark.parametrize("repo_layout", LOCAL_LAYOUTS, indirect=True)
def test_pytype_coercion(butler_harness: ButlerHarness) -> None:
    """Test python type coercion on Butler.get and put."""
    # Store some data with the normal example storage class.
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    dataset_type_name = "test_metric"
    butler = run_put_get_test(butler_harness, storage_class, dataset_type_name)

    data_id = {"instrument": "DummyCamComp", "visit": 423}
    metric = butler.get(dataset_type_name, dataId=data_id)
    assert get_full_type_name(metric) == "lsst.daf.butler.tests.MetricsExample"

    dataset_type_ori = butler.get_dataset_type(dataset_type_name)
    assert dataset_type_ori.storageClass.name == "StructuredDataNoComponents"

    # Now need to hack the registry dataset type definition.
    # There is no API for this.
    registry = butler._registry  # type: ignore[attr-defined]
    assert isinstance(registry, SqlRegistry)
    manager = registry._managers.datasets
    assert hasattr(manager, "_db")
    assert hasattr(manager, "_static")
    manager._db.update(
        manager._static.dataset_type,
        {"name": dataset_type_name},
        {dataset_type_name: dataset_type_name, "storage_class": "StructuredDataNoComponentsModel"},
    )

    # Force reset of dataset type cache
    butler.registry.refresh()

    dataset_type_new = butler.get_dataset_type(dataset_type_name)
    assert dataset_type_new.name == dataset_type_ori.name
    assert dataset_type_new.storageClass.name == "StructuredDataNoComponentsModel"

    metric_model = butler.get(dataset_type_name, dataId=data_id)
    assert type(metric_model) is not type(metric)
    assert get_full_type_name(metric_model) == "lsst.daf.butler.tests.MetricsExampleModel"

    # Put the model and read it back to show that everything now
    # works as normal.
    metric_ref = butler.put(metric_model, dataset_type_name, dataId=data_id, visit=424)
    metric_model_new = butler.get(metric_ref)
    assert metric_model_new == metric_model

    # Hack the storage class again to something that will fail on the
    # get with no conversion class.
    manager._db.update(
        manager._static.dataset_type,
        {"name": dataset_type_name},
        {dataset_type_name: dataset_type_name, "storage_class": "StructuredDataListYaml"},
    )
    butler.registry.refresh()

    with pytest.raises(ValueError, match="no valid converter found to convert"):
        butler.get(dataset_type_name, dataId=data_id)


@pytest.mark.parametrize("repo_layout", LOCAL_LAYOUTS, indirect=True)
def test_provenance(butler_harness: ButlerHarness) -> None:
    """Test that provenance is attached on put."""
    run = "test_run"
    butler, dataset_type = butler_harness.create_butler(run, "MetricsExampleModelProvenance", "prov_metric")
    metric = MetricsExampleModel(
        summary={"AM1": 5.2, "AM2": 30.6},
        output={"a": [1, 2, 3], "b": {"blue": 5, "red": "green"}},
        data=[563, 234, 456.7, 752, 8, 9, 27],
    )
    # Provenance can be attached to the object being put. Whether
    # it is or not is dependent on the formatter. For this test we
    # copy on adding provenance to ensure they differ.
    assert metric.dataset_id is None
    metric_ref = butler.put(metric, dataset_type, visit=424, instrument="DummyCamComp")
    assert metric.dataset_id is None
    metric_2 = butler.get(metric_ref)
    assert metric_2.data == metric.data
    assert metric_2.dataset_id == metric_ref.id
    assert metric_2.provenance is None

    # Put with provenance.
    prov = DatasetProvenance(quantum_id=uuid.uuid4())
    prov.add_input(metric_ref)
    prov.add_extra_provenance(metric_ref.id, {"answer": 42})
    metric_ref2 = butler.put(metric, dataset_type, visit=423, instrument="DummyCamComp", provenance=prov)
    metric_3 = butler.get(metric_ref2)
    assert metric_3.provenance == prov

    # Check that we can extract provenance from dict form.
    prov_dict = prov.to_flat_dict(metric_ref2)
    prov_from_prov, ref_from_prov = DatasetProvenance.from_flat_dict(prov_dict, butler)
    assert ref_from_prov == metric_ref2
    # Direct __eq__ of the provenance does not work because one side
    # includes dimension records.
    assert {ref.id for ref in prov_from_prov.inputs} == {ref.id for ref in prov.inputs}
    assert prov_from_prov.quantum_id == prov.quantum_id
    assert prov_from_prov.extras == prov.extras

    # Force a bad ID into the dict.
    prov_dict["id"] = uuid.uuid4()
    with pytest.raises(ValueError, match="Dataset associated with this provenance"):
        DatasetProvenance.from_flat_dict(prov_dict, butler)
    del prov_dict["id"]
    prov_dict["input 0 id"] = uuid.uuid4()
    # The added key separates on spaces while the rest of the header separates
    # on ".", so the separator check rejects it before the unknown input ID is
    # ever looked up. See DM-55822's mapping notes.
    with pytest.raises(ValueError, match="Inconsistent values found for separators"):
        DatasetProvenance.from_flat_dict(prov_dict, butler)

    # Check that simple types can be reconstructed with non-standard
    # separators.
    prov_dict = prov.to_flat_dict(metric_ref2, prefix="XYZ", sep="😎", simple_types=True)
    prov_from_prov, ref_from_prov = DatasetProvenance.from_flat_dict(prov_dict, butler)
    assert ref_from_prov == metric_ref2
    assert {ref.id for ref in prov_from_prov.inputs} == {ref.id for ref in prov.inputs}

    with pytest.raises(ValueError, match="No provenance information found in header"):
        DatasetProvenance.from_flat_dict({"unknown": 42}, butler)


def _setup_to_test_collection_chain(butler_harness: ButlerHarness) -> Butler:
    """Return a writeable Butler holding a chain and four runs to put in it."""
    butler = butler_harness.create_empty_butler(writeable=True)

    butler.collections.register("chain", CollectionType.CHAINED)

    runs = ["a", "b", "c", "d"]
    for run in runs:
        butler.collections.register(run)

    butler.collections.register("staticchain", CollectionType.CHAINED)
    butler.collections.redefine_chain("staticchain", ["a", "b"])

    return butler


def _check_chain(butler: Butler, expected: list[str]) -> None:
    """Assert that the test chain has exactly the expected children."""
    children = butler.collections.get_info("chain").children
    assert expected == list(children)


def _check_common_chain_functionality(
    butler: Butler,
    func: Callable[[str, str | list[str]], Any],
    *,
    skip_cycle_check: bool = False,
) -> None:
    """Assert the behavior every chain-modifying operation shares.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler`
        Butler set up by `_setup_to_test_collection_chain`.
    func : `~collections.abc.Callable`
        The chain operation under test.
    skip_cycle_check : `bool`, optional
        Whether to skip the cycle check, which does not apply to removal.
    """
    # Missing parent collection
    with pytest.raises(MissingCollectionError):
        func("doesnotexist", [])
    # Missing child collection
    with pytest.raises(MissingCollectionError):
        func("chain", ["doesnotexist"])
    # Forbid operations on non-chained collections
    with pytest.raises(CollectionTypeError):
        func("d", ["a"])

    # Prevent collection cycles
    if not skip_cycle_check:
        butler.collections.register("chain2", CollectionType.CHAINED)
        func("chain2", "chain")
        with pytest.raises(CollectionCycleError):
            func("chain", "chain2")

    # Make sure none of the earlier operations interfered with unrelated
    # chains.
    assert ["a", "b"] == list(butler.collections.get_info("staticchain").children)

    with (
        butler._caching_context(),
        pytest.raises(RuntimeError, match="Chained collection modification not permitted"),
    ):
        func("chain", "a")


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_get_dataset_types(butler_harness: ButlerHarness, butler_client: str) -> None:
    if butler_client == "server":
        # This is mostly a test of validateConfiguration, which is for
        # validating Datastore configuration and thus isn't relevant to
        # RemoteButler.
        return

    butler = butler_harness.create_empty_butler(run=butler_harness.default_run)
    dimensions = butler.dimensions.conform(["instrument", "visit", "physical_filter"])
    dimension_entries: list[tuple[str, list[Mapping[str, Any]]]] = [
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
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredData")
    # Add needed Dimensions
    for element, data in dimension_entries:
        butler.registry.insertDimensionData(element, *data)

    # When a DatasetType is added to the registry entries are not created
    # for components but querying them can return the components.
    dataset_type_names = {"metric", "metric2", "metric4", "metric33", "pvi", "paramtest"}
    components = set()
    for dataset_type_name in dataset_type_names:
        # Create and register a DatasetType
        add_dataset_type(dataset_type_name, dimensions, storage_class, butler.registry)

        for component_name in storage_class.components:
            components.add(DatasetType.nameWithComponent(dataset_type_name, component_name))

    from_registry: set[DatasetType] = set()
    for parent_dataset_type in butler.registry.queryDatasetTypes():
        from_registry.add(parent_dataset_type)
        from_registry.update(parent_dataset_type.makeAllComponentDatasetTypes())
    assert {d.name for d in from_registry} == dataset_type_names | components

    # Query with wildcard.
    dataset_types = list(butler.registry.queryDatasetTypes("metric*"))
    assert len(dataset_types) == 4, f"Got: {dataset_types}"
    # but not regex.
    with pytest.raises(DatasetTypeExpressionError):
        butler.registry.queryDatasetTypes(["pvi", re.compile("metric.*")])

    # Now that we have some dataset types registered, validate them
    ignore = [
        "test_metric_comp",
        "metric3",
        "metric5",
        "calexp",
        "DummySC",
        "datasetType.component",
        "random_data",
        "random_data_2",
    ]
    butler.validateConfiguration(ignore=ignore)

    # Add a new datasetType that will fail template validation
    add_dataset_type("test_metric_comp", dimensions, storage_class, butler.registry)
    if butler_harness.profile.validation_can_fail:
        with pytest.raises(ValidationError):
            butler.validateConfiguration()

    # Rerun validation but with a subset of dataset type names
    butler.validateConfiguration(datasetTypeNames=["metric4"])

    # Rerun validation but ignore the bad datasetType
    butler.validateConfiguration(ignore=ignore)


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_get_dataset_collection_caching(butler_harness: ButlerHarness) -> None:
    # Prior to DM-41117, there was a bug where get_dataset would throw
    # MissingCollectionError if you tried to fetch a dataset that was added
    # after the collection cache was last updated.
    reader_butler, dataset_type = butler_harness.create_butler(
        butler_harness.default_run, "int", "datasettypename"
    )
    writer_butler = butler_harness.create_empty_butler(writeable=True, run="new_run")
    data_id = {"instrument": "DummyCamComp", "visit": 423}
    put_ref = writer_butler.put(123, dataset_type, data_id)
    get_ref = reader_butler.get_dataset(put_ref.id)
    assert get_ref is not None
    assert get_ref.id == put_ref.id
    # Also works when looking up via a hexadecimal string instead of a UUID
    # instance.
    hex_ref = reader_butler.get_dataset(put_ref.id.hex)
    assert hex_ref is not None
    assert hex_ref.id == put_ref.id


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_collection_chain_redefine(butler_harness: ButlerHarness) -> None:
    butler = _setup_to_test_collection_chain(butler_harness)

    butler.collections.redefine_chain("chain", "a")
    _check_chain(butler, ["a"])

    # Duplicates are removed from the list of children
    butler.collections.redefine_chain("chain", ["c", "b", "c"])
    _check_chain(butler, ["c", "b"])

    # Empty list clears the chain
    butler.collections.redefine_chain("chain", [])
    _check_chain(butler, [])

    _check_common_chain_functionality(butler, butler.collections.redefine_chain)


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_collection_chain_prepend(butler_harness: ButlerHarness) -> None:
    butler = _setup_to_test_collection_chain(butler_harness)

    # Duplicates are removed from the list of children
    butler.collections.prepend_chain("chain", ["c", "b", "c"])
    _check_chain(butler, ["c", "b"])

    # Prepend goes on the front of existing chain
    butler.collections.prepend_chain("chain", ["a"])
    _check_chain(butler, ["a", "c", "b"])

    # Empty prepend does nothing
    butler.collections.prepend_chain("chain", [])
    _check_chain(butler, ["a", "c", "b"])

    # Prepending children that already exist in the chain removes them from
    # their current position.
    butler.collections.prepend_chain("chain", ["d", "b", "c"])
    _check_chain(butler, ["d", "b", "c", "a"])

    _check_common_chain_functionality(butler, butler.collections.prepend_chain)


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_collection_chain_extend(butler_harness: ButlerHarness) -> None:
    butler = _setup_to_test_collection_chain(butler_harness)

    # Duplicates are removed from the list of children
    butler.collections.extend_chain("chain", ["c", "b", "c"])
    _check_chain(butler, ["c", "b"])

    # Extend goes on the end of existing chain
    butler.collections.extend_chain("chain", ["a"])
    _check_chain(butler, ["c", "b", "a"])

    # Empty extend does nothing
    butler.collections.extend_chain("chain", [])
    _check_chain(butler, ["c", "b", "a"])

    # Extending children that already exist in the chain removes them from
    # their current position.
    butler.collections.extend_chain("chain", ["d", "b", "c"])
    _check_chain(butler, ["a", "d", "b", "c"])

    _check_common_chain_functionality(butler, butler.collections.extend_chain)


@pytest.mark.parametrize(AXIS_NAMES, BUTLER_TESTS_AXES, indirect=True)
def test_collection_chain_remove(butler_harness: ButlerHarness) -> None:
    butler = _setup_to_test_collection_chain(butler_harness)

    butler.collections.redefine_chain("chain", ["a", "b", "c", "d"])

    butler.collections.remove_from_chain("chain", "c")
    _check_chain(butler, ["a", "b", "d"])

    # Duplicates are allowed in the list of children
    butler.collections.remove_from_chain("chain", ["b", "b", "a"])
    _check_chain(butler, ["d"])

    # Empty remove does nothing
    butler.collections.remove_from_chain("chain", [])
    _check_chain(butler, ["d"])

    # Removing children that aren't in the chain does nothing
    butler.collections.remove_from_chain("chain", ["a", "chain"])
    _check_chain(butler, ["d"])

    _check_common_chain_functionality(butler, butler.collections.remove_from_chain, skip_cycle_check=True)


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


@pytest.mark.parametrize("repo_layout", ["in_repo", "explicit_root"], indirect=True)
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


def test_fallback(butler_repo: ButlerRepo, storage_class_factory: StorageClassFactory) -> None:
    """Test that a broken datastore config still yields a usable registry."""
    # Read the butler config and mess with the datastore section.
    config_path = os.path.join(butler_repo.root, "butler.yaml")
    bad_config = Config(config_path)
    bad_config["datastore", "cls"] = "lsst.not.a.datastore.Datastore"
    bad_config.dumpToUri(config_path)

    with pytest.raises(RuntimeError):
        Butler(butler_repo.root, without_datastore=False)

    with pytest.raises(RuntimeError):
        Butler.from_config(butler_repo.root, without_datastore=False)

    with contextlib.closing(
        Butler.from_config(butler_repo.root, writeable=True, without_datastore=True)
    ) as butler:
        assert isinstance(butler._datastore, NullDatastore)

        # Check that registry is working.
        butler.collections.register("MYRUN")
        collections = butler.collections.query("*")
        assert "MYRUN" in set(collections)

        # Create a ref.
        dimensions = butler.dimensions.conform([])
        storageClass = storage_class_factory.getStorageClass("StructuredDataDict")
        datasetTypeName = "metric"
        datasetType = DatasetType(datasetTypeName, dimensions, storageClass)
        butler.registry.registerDatasetType(datasetType)
        ref = DatasetRef(datasetType, {}, run="MYRUN")

        # Check that datastore will complain.
        with pytest.raises(FileNotFoundError):
            butler.get(ref)
        with pytest.raises(FileNotFoundError):
            butler.getURI(ref)
