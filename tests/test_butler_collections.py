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

"""Tests for collection chains and dataset type queries."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Any

import pytest
from butler_test_support import AXIS_NAMES, BUTLER_TESTS_AXES

from lsst.daf.butler import (
    Butler,
    CollectionCycleError,
    CollectionType,
    DatasetType,
    ValidationError,
)
from lsst.daf.butler.registry import (
    CollectionTypeError,
    DatasetTypeExpressionError,
    MissingCollectionError,
)
from lsst.daf.butler.tests.fixtures import ButlerHarness, add_dataset_type


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
