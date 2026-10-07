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

"""Tests for the constraints model that lets a datastore refuse a dataset."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from typing import cast

import pytest

from lsst.daf.butler import (
    DatasetTypeNotSupportedError,
    Datastore,
    DimensionUniverse,
    FileDataset,
    StorageClassFactory,
)
from lsst.daf.butler.datastore import DatastoreConfig
from lsst.daf.butler.datastores.chainedDatastore import ChainedDatastore
from lsst.daf.butler.tests import DatasetTestHelper, DummyRegistry
from lsst.daf.butler.tests.fixtures import make_example_metrics
from lsst.utils import doImport

TESTDIR = os.path.abspath(os.path.dirname(__file__))

DATA_ID = {
    "visit": 52,
    "physical_filter": "V",
    "band": "v",
    "instrument": "DummyCamComp",
    "day_obs": 20250101,
}
"""Data ID the constraint configurations are written against."""

OTHER_DATA_ID = {
    "visit": 52,
    "physical_filter": "V",
    "band": "v",
    "instrument": "HSC",
    "day_obs": 20250101,
}
"""Data ID for a second instrument, which the per-store constraints treat
differently."""

CONSTRAINT_DATASTORES = [
    pytest.param("posixDatastoreP.yaml", True, True, id="posix"),
    pytest.param("inMemoryDatastoreP.yaml", False, False, id="in-memory"),
    pytest.param("chainedDatastoreP.yaml", True, True, id="chained"),
    pytest.param("chainedDatastorePa.yaml", True, True, id="chained-native"),
]
"""(config file, can ingest, needs a root) for each datastore configuration
that shares the same constraints.

``chained-memory`` was measured by DM-55822 as zero unique lines and zero
unique arcs and is gone. ``chained`` measured zero as well, but only because
``chained-memory`` held the same coverage: between them they were the sole
cover for `ChainedDatastore.put`'s "child rejects the ref, skip it" branch,
`chainedDatastore.py` arc 470 to 471. Removing both lost it, so the file-backed
one stays. A per-axis marginal query cannot see coverage two axes hold jointly.
"""

CONSTRAINT_CASES = [
    pytest.param("metric", "StructuredData", True, id="metric"),
    pytest.param("metric5", "StructuredData", False, id="metric5"),
    pytest.param("metric33", "StructuredData", True, id="metric33"),
    pytest.param("metric5", "StructuredDataJson", True, id="metric5-json"),
]
"""(dataset type name, storage class, whether the constraint accepts it).

These were subtests of one method; `parametrize` could not be applied while
they lived on a `unittest.TestCase`.
"""

PER_STORE_CASES = [
    pytest.param("metric", DATA_ID, "StructuredData", (False, True, False), True, id="metric"),
    pytest.param("metric5", DATA_ID, "StructuredData", (False, False, False), False, id="metric5"),
    pytest.param("metric5", OTHER_DATA_ID, "StructuredData", (True, False, False), False, id="metric5-hsc"),
    pytest.param("metric33", OTHER_DATA_ID, "StructuredDataJson", (True, True, False), True, id="metric33"),
    pytest.param("metric5", DATA_ID, "StructuredDataJson", (False, True, False), True, id="metric5-json"),
]
"""(dataset type name, data ID, storage class, per-child acceptance, whether
ingest is expected to work) for the per-store constraint chain."""


@pytest.fixture(scope="module")
def constraint_storage_class_factory() -> StorageClassFactory:
    """Storage classes for the constraint tests.

    Named distinctly from the plugin's ``storage_class_factory`` because this
    loads ``storageClasses.yaml`` rather than the Butler configs, matching what
    ``DatastoreTestsBase.setUpClass`` did. `StorageClassFactory` is a
    singleton, so the two accumulate rather than conflict.
    """
    factory = StorageClassFactory()
    factory.addFromConfig(os.path.join(TESTDIR, "config/basic/storageClasses.yaml"))
    return factory


def _make_datastore(config_file: str, root: str | None) -> Datastore:
    """Build a datastore from a test configuration, as the base class did."""
    path = os.path.join(TESTDIR, "config/basic", config_file)
    config = DatastoreConfig(path)
    datastore_type = cast(type[Datastore], doImport(config["cls"]))
    if root is not None:
        datastore_type.setConfigRoot(root, config, config.copy())
    registry = DummyRegistry()
    return Datastore.fromConfig(config=config.copy(), bridgeManager=registry.getDatastoreBridgeManager())


@pytest.fixture
def testfiles() -> Iterator[dict[str, str]]:
    """Empty JSON and YAML files, suitable for the ingest checks."""
    with (
        tempfile.NamedTemporaryFile(suffix=".yaml") as yaml_file,
        tempfile.NamedTemporaryFile(suffix=".json") as json_file,
    ):
        yield {"yaml": yaml_file.name, "json": json_file.name}


def _testfile_for(testfiles: dict[str, str], storage_class_name: str) -> str:
    """Choose the temporary file whose suffix matches the storage class."""
    return testfiles["json"] if storage_class_name.endswith("Json") else testfiles["yaml"]


@pytest.mark.parametrize(("config_file", "can_ingest", "needs_root"), CONSTRAINT_DATASTORES)
@pytest.mark.parametrize(("dataset_type_name", "storage_class_name", "accepted"), CONSTRAINT_CASES)
def test_constraints(
    config_file: str,
    can_ingest: bool,
    needs_root: bool,
    dataset_type_name: str,
    storage_class_name: str,
    accepted: bool,
    constraint_storage_class_factory: StorageClassFactory,
    testfiles: dict[str, str],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Test the constraints model shared by all these datastores."""
    helper = DatasetTestHelper()
    root = str(tmp_path_factory.mktemp("datastore")) if needs_root else None
    datastore = _make_datastore(config_file, root)

    storage_class = constraint_storage_class_factory.getStorageClass(storage_class_name)
    dimensions = DimensionUniverse().conform(("visit", "physical_filter", "instrument"))
    testfile = _testfile_for(testfiles, storage_class.name)

    metrics = make_example_metrics()
    ref = helper.makeDatasetRef(dataset_type_name, dimensions, storage_class, DATA_ID)
    if accepted:
        datastore.put(metrics, ref)
        assert datastore.exists(ref)
        datastore.remove(ref)

        # Try ingest
        if can_ingest:
            datastore.ingest(FileDataset(testfile, [ref]), transfer="link")
            assert datastore.exists(ref)
            datastore.remove(ref)
    else:
        with pytest.raises(DatasetTypeNotSupportedError):
            datastore.put(metrics, ref)
        assert not datastore.exists(ref)

        # Again with ingest
        if can_ingest:
            with pytest.raises(DatasetTypeNotSupportedError):
                datastore.ingest(FileDataset(testfile, [ref]), transfer="link")
            assert not datastore.exists(ref)


@pytest.mark.parametrize(
    ("dataset_type_name", "data_id", "storage_class_name", "accept", "ingest"), PER_STORE_CASES
)
def test_per_store_constraints(
    dataset_type_name: str,
    data_id: dict[str, object],
    storage_class_name: str,
    accept: tuple[bool, bool, bool],
    ingest: bool,
    constraint_storage_class_factory: StorageClassFactory,
    testfiles: dict[str, str],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Test that a chained datastore can control constraints per-datastore
    even if a child datastore would accept.
    """
    helper = DatasetTestHelper()
    root = str(tmp_path_factory.mktemp("datastore"))
    datastore = _make_datastore("chainedDatastorePb.yaml", root)
    assert isinstance(datastore, ChainedDatastore)

    storage_class = constraint_storage_class_factory.getStorageClass(storage_class_name)
    dimensions = DimensionUniverse().conform(("visit", "physical_filter", "instrument"))
    testfile = _testfile_for(testfiles, storage_class.name)

    metrics = make_example_metrics()
    ref = helper.makeDatasetRef(dataset_type_name, dimensions, storage_class, data_id)
    if not any(accept):
        with pytest.raises(DatasetTypeNotSupportedError):
            datastore.put(metrics, ref)
        assert not datastore.exists(ref)

        # Again with ingest
        with pytest.raises(DatasetTypeNotSupportedError):
            datastore.ingest(FileDataset(testfile, [ref]), transfer="link")
        assert not datastore.exists(ref)
        return

    datastore.put(metrics, ref)
    assert datastore.exists(ref)

    # Check each datastore inside the chained datastore
    for child_datastore, expected in zip(datastore.datastores, accept, strict=True):
        assert child_datastore.exists(ref) == expected, (
            f"Testing presence of {ref} in datastore {child_datastore.name}"
        )

    datastore.remove(ref)

    # Check that ingest works
    if not ingest:
        with pytest.raises(DatasetTypeNotSupportedError):
            datastore.ingest(FileDataset(testfile, [ref]), transfer="link")
        return

    datastore.ingest(FileDataset(testfile, [ref]), transfer="link")
    assert datastore.exists(ref)

    # Check each datastore inside the chained datastore
    for child_datastore, expected in zip(datastore.datastores, accept, strict=True):
        # Ephemeral datastores means InMemory at the moment and that does not
        # accept ingest of files.
        if child_datastore.isEphemeral:
            expected = False
        assert child_datastore.exists(ref) == expected, (
            f"Testing presence of ingested {ref} in datastore {child_datastore.name}"
        )

    datastore.remove(ref)
