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

"""Tests for putting datasets into a Butler and getting them back."""

from __future__ import annotations

import logging

import pytest
from butler_test_support import (
    AXIS_NAMES,
    BUTLER_TESTS_AXES,
    FILE_DATASTORE_AXES,
    PUT_GET_AXES,
    records_from,
    run_put_get_test,
)

from lsst.daf.butler import (
    CollectionType,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    NoDefaultCollectionError,
)
from lsst.daf.butler.registry import CollectionError
from lsst.daf.butler.tests.dict_convertible_model import DictConvertibleModel
from lsst.daf.butler.tests.fixtures import ButlerHarness, add_dataset_type, make_example_metrics
from lsst.resources import ResourcePath
from lsst.utils.introspection import get_full_type_name

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


@pytest.mark.parametrize("butler_client", ["cloned"], indirect=True)
def test_cloned_put_get(butler_harness: ButlerHarness) -> None:
    """A Butler that has been cloned is still usable for put and get.

    The cloned client was once an axis over every butler test. Its marginal
    coverage across 163 contexts was zero unique lines and zero unique arcs, so
    this stands in for all of it; `Butler.clone` itself is covered directly by
    tests/test_simpleButler.py.
    """
    storage_class = butler_harness.storage_class_factory.getStorageClass("StructuredDataNoComponents")
    butler, dataset_type = butler_harness.create_butler(
        butler_harness.default_run, storage_class, "test_metric"
    )
    metric = make_example_metrics()
    data_id = {"instrument": "DummyCamComp", "visit": 423}
    ref = butler.put(metric, dataset_type, data_id)
    assert butler.get(ref) == metric


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
