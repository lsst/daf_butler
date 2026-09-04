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

"""Tests for the datastore implementations themselves."""

from __future__ import annotations

import contextlib
import dataclasses
import logging
import os
import pathlib
import pickle
import shutil
import tempfile
import time
import unittest.mock
import uuid
from collections.abc import Callable, Iterator
from typing import Any, cast

import pytest
import yaml
from butler_test_support import records_from

import lsst.daf.butler.datastores.fileDatastore
from lsst.daf.butler import (
    Config,
    DataCoordinate,
    DatasetIdGenEnum,
    DatasetRef,
    DatasetType,
    DatasetTypeNotSupportedError,
    Datastore,
    DimensionUniverse,
    FileDataset,
    StorageClass,
    StorageClassFactory,
)
from lsst.daf.butler.datastore import DatasetRefURIs, DatastoreConfig, DatastoreValidationError, NullDatastore
from lsst.daf.butler.datastore.cache_manager import (
    DatastoreCacheManager,
    DatastoreCacheManagerConfig,
    DatastoreDisabledCacheManager,
)
from lsst.daf.butler.datastore.record_data import (
    DatastoreRecordData,
    DatastoreRecordTable,
    SerializedDatastoreRecordData,
)
from lsst.daf.butler.datastore.stored_file_info import (
    StoredFileInfo,
    StoredFileInfoTable,
    make_datastore_path_relative,
)
from lsst.daf.butler.datastores.chainedDatastore import ChainedDatastore
from lsst.daf.butler.formatters.yaml import YamlFormatter
from lsst.daf.butler.tests import (
    BadNoWriteFormatter,
    BadWriteFormatter,
    DatasetTestHelper,
    DummyRegistry,
    MetricsExample,
    MetricsExampleDataclass,
    MetricsExampleModel,
)
from lsst.daf.butler.tests.dict_convertible_model import DictConvertibleModel
from lsst.daf.butler.tests.fixtures import make_example_metrics
from lsst.resources import ResourcePath
from lsst.utils import doImport
from lsst.utils.introspection import get_full_type_name

TESTDIR = os.path.dirname(__file__)

COMPOSITE_STORAGE_CLASS_NAMES = (
    "StructuredComposite",
    "StructuredCompositeTestA",
    "StructuredCompositeTestB",
    "StructuredCompositeReadComp",
    "StructuredData",  # No disassembly
    "StructuredCompositeReadCompNoDisassembly",
)
"""Composite storage classes the disassembly test covers.

Each case uses a distinct ``metric_comp_{i}`` dataset type so that a failure in
one does not cascade into the others through a file clash.
"""

INGEST_TRANSFER_MODES = ("copy", "move", "link", "hardlink", "symlink", "relsymlink", "auto")
"""Transfer modes the ingest test tries; those a datastore does not support
are expected to raise rather than being skipped."""


class TransactionTestError(Exception):
    """Specific error for transactions, to prevent misdiagnosing
    that might otherwise occur when a standard exception is used.
    """


@dataclasses.dataclass(frozen=True)
class DatastoreTestProfile:
    """Everything that varied between the concrete datastore test classes."""

    config_file: str
    """Path, relative to the test directory, of the datastore config."""

    uri_scheme: str
    """Scheme the datastore's URIs are expected to use."""

    ingest_transfer_modes: tuple[str | None, ...]
    """Transfer modes this datastore supports for ingest."""

    is_ephemeral: bool
    """Whether the datastore loses its contents when it goes away."""

    root_keys: tuple[str, ...] | None
    """Config keys holding a filesystem root, or `None` if there are none."""

    validation_can_fail: bool
    """Whether configuration validation can fail for this datastore."""

    has_unsupported_put: bool
    """Whether some storage class is rejected on put."""

    needs_root: bool = True
    """Whether the datastore has to be pointed at a temporary directory."""

    can_ingest_no_transfer_auto: bool = True
    """Whether "auto" ingest can leave the file where it is.

    Only consulted when ``"auto"`` is in `ingest_transfer_modes`, which is why
    the ephemeral profiles can leave it at the default.
    """


POSIX_MODES = (None, "copy", "move", "link", "hardlink", "symlink", "relsymlink", "auto")
CHAINED_MODES = (None, "copy", "move", "hardlink", "symlink", "relsymlink", "link", "auto")

PROFILES = {
    "posix": DatastoreTestProfile(
        config_file="butler.yaml",
        uri_scheme="file",
        ingest_transfer_modes=POSIX_MODES,
        is_ephemeral=False,
        root_keys=("root",),
        validation_can_fail=True,
        has_unsupported_put=True,
    ),
    "posix-no-checksums": DatastoreTestProfile(
        config_file="posixDatastoreNoChecksums.yaml",
        uri_scheme="file",
        ingest_transfer_modes=POSIX_MODES,
        is_ephemeral=False,
        root_keys=("root",),
        validation_can_fail=True,
        has_unsupported_put=True,
    ),
    "trash": DatastoreTestProfile(
        config_file="butler.yaml",
        uri_scheme="file",
        ingest_transfer_modes=POSIX_MODES,
        is_ephemeral=False,
        root_keys=("root",),
        validation_can_fail=True,
        has_unsupported_put=True,
    ),
    "in-memory": DatastoreTestProfile(
        config_file="inMemoryDatastore.yaml",
        uri_scheme="mem",
        ingest_transfer_modes=(),
        is_ephemeral=True,
        root_keys=None,
        validation_can_fail=False,
        has_unsupported_put=False,
        needs_root=False,
    ),
    "chained": DatastoreTestProfile(
        config_file="chainedDatastore.yaml",
        uri_scheme="file",
        ingest_transfer_modes=CHAINED_MODES,
        is_ephemeral=False,
        root_keys=(".datastores.1.root", ".datastores.2.root"),
        validation_can_fail=True,
        has_unsupported_put=False,
        can_ingest_no_transfer_auto=False,
    ),
    "chained-memory": DatastoreTestProfile(
        config_file="chainedDatastore2.yaml",
        uri_scheme="mem",
        ingest_transfer_modes=(),
        is_ephemeral=True,
        root_keys=None,
        validation_can_fail=False,
        has_unsupported_put=False,
        needs_root=False,
    ),
}
"""One entry per concrete datastore test class.

``trash`` and ``posix-no-checksums`` were subclasses of the posix case, so they
rerun every shared test.
"""

ALL_PROFILES = list(PROFILES)
"""Profiles that run the shared datastore tests."""

FILE_PROFILES = ["posix", "posix-no-checksums", "trash", "chained"]
"""Profiles backed by a FileDatastore, which run the file-specific tests."""


class DatastoreHarness:
    """A datastore configuration under test, and the pieces built from it.

    This replaces ``DatastoreTestsBase``: the same registry, config and
    datastore-class lookup, without the inheritance.

    Parameters
    ----------
    profile : `DatastoreTestProfile`
        The configuration under test.
    root : `str` or `None`
        Temporary directory the datastore should use, if it needs one.
    storage_class_factory : `~lsst.daf.butler.StorageClassFactory`
        Factory holding the datastore test storage classes.
    """

    def __init__(
        self,
        profile: DatastoreTestProfile,
        root: str | None,
        storage_class_factory: StorageClassFactory,
    ) -> None:
        self.profile = profile
        self.root = root
        self.storage_class_factory = storage_class_factory
        self.universe = DimensionUniverse()
        self.config_file = os.path.join(TESTDIR, "config/basic", profile.config_file)
        self.config = DatastoreConfig(self.config_file)
        # Do not assume the constructor name; rely on the configuration file.
        self.datastore_type = cast(type[Datastore], doImport(self.config["cls"]))
        if root is not None:
            self.datastore_type.setConfigRoot(root, self.config, self.config.copy())
        self.registry = DummyRegistry()
        self._helper = DatasetTestHelper()

    def make_datastore(self, sub: str | None = None) -> Datastore:
        """Make a new datastore of the configured type.

        Parameters
        ----------
        sub : `str`, optional
            If given, the datastore is distinct from any built with a
            different value, and gets its own registry.

        Returns
        -------
        datastore : `~lsst.daf.butler.Datastore`
            The new datastore.
        """
        config = self.config.copy()
        if sub is not None and self.root is not None:
            self.datastore_type.setConfigRoot(os.path.join(self.root, sub), config, self.config)
        registry = DummyRegistry() if sub is not None else self.registry
        return Datastore.fromConfig(config=config, bridgeManager=registry.getDatastoreBridgeManager())

    def make_dataset_ref(self, *args: Any, **kwargs: Any) -> DatasetRef:
        """Build a `DatasetRef` for a test.

        Parameters
        ----------
        *args, **kwargs
            Forwarded to
            `~lsst.daf.butler.tests.DatasetTestHelper.makeDatasetRef`.

        Returns
        -------
        ref : `~lsst.daf.butler.DatasetRef`
            The new reference.
        """
        return self._helper.makeDatasetRef(*args, **kwargs)


@pytest.fixture(scope="module")
def datastore_storage_class_factory() -> StorageClassFactory:
    """Storage classes for the datastore tests.

    Named distinctly from the plugin's ``storage_class_factory`` because this
    loads ``storageClasses.yaml`` rather than the Butler configs, matching what
    ``DatastoreTestsBase.setUpClass`` did. `StorageClassFactory` is a
    singleton, so the two accumulate rather than conflict.
    """
    factory = StorageClassFactory()
    factory.addFromConfig(os.path.join(TESTDIR, "config/basic/storageClasses.yaml"))
    return factory


@pytest.fixture
def ds(
    request: pytest.FixtureRequest,
    datastore_storage_class_factory: StorageClassFactory,
    tmp_path_factory: pytest.TempPathFactory,
) -> DatastoreHarness:  # numpydoc ignore=PR01
    """Yield a `DatastoreHarness` for the requested profile."""
    profile = PROFILES[getattr(request, "param", "posix")]
    root = None
    if profile.needs_root:
        # os.path.realpath matters for "relsymlink": on macOS a temporary file
        # can be under either /var/folders or /private/var/folders, which name
        # the same place, and a relative symlink between the two forms cannot
        # be traversed.
        root = os.path.realpath(str(tmp_path_factory.mktemp("datastore")))
    return DatastoreHarness(profile, root, datastore_storage_class_factory)


@contextlib.contextmanager
def _temp_yaml_file(data: Any) -> Iterator[str]:
    """Write data to a temporary YAML file and yield its path."""
    fh = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml")
    try:
        yaml.dump(data, stream=fh)
        fh.flush()
        yield fh.name
    finally:
        # Some tests delete the file
        with contextlib.suppress(FileNotFoundError):
            fh.close()


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_config_root(ds: DatastoreHarness) -> None:
    full = DatastoreConfig(ds.config_file)
    config = DatastoreConfig(ds.config_file, mergeDefaults=False)
    newroot = "/random/location"
    ds.datastore_type.setConfigRoot(newroot, config, full)
    if ds.profile.root_keys:
        for k in ds.profile.root_keys:
            assert newroot in config[k]


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_constructor(ds: DatastoreHarness) -> None:
    datastore = ds.make_datastore()
    assert datastore is not None
    assert datastore.isEphemeral is ds.profile.is_ephemeral


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_configuration_validation(ds: DatastoreHarness) -> None:
    datastore = ds.make_datastore()
    sc = ds.storage_class_factory.getStorageClass("ThingOne")
    datastore.validateConfiguration([sc])

    sc2 = ds.storage_class_factory.getStorageClass("ThingTwo")
    if ds.profile.validation_can_fail:
        with pytest.raises(DatastoreValidationError):
            datastore.validateConfiguration([sc2], logFailures=True)

    dimensions = ds.universe.conform(("visit", "physical_filter"))
    dataId = {
        "instrument": "dummy",
        "visit": 52,
        "physical_filter": "V",
        "band": "v",
        "day_obs": 20250101,
    }
    ref = ds.make_dataset_ref("metric", dimensions, sc, dataId)
    datastore.validateConfiguration([ref])


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_parameter_validation(ds: DatastoreHarness) -> None:
    """Check that parameters are validated"""
    sc = ds.storage_class_factory.getStorageClass("ThingOne")
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    dataId = {
        "instrument": "dummy",
        "visit": 52,
        "physical_filter": "V",
        "band": "v",
        "day_obs": 20250101,
    }
    ref = ds.make_dataset_ref("metric", dimensions, sc, dataId)
    datastore = ds.make_datastore()
    data = {1: 2, 3: 4}
    datastore.put(data, ref)
    newdata = datastore.get(ref)
    assert data == newdata
    with pytest.raises(KeyError):
        newdata = datastore.get(ref, parameters={"missing": 5})


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_basic_put_get(ds: DatastoreHarness) -> None:
    metrics = make_example_metrics()
    datastore = ds.make_datastore()

    # Create multiple storage classes for testing different formulations
    storageClasses = [
        ds.storage_class_factory.getStorageClass(sc)
        for sc in ("StructuredData", "StructuredDataJson", "StructuredDataPickle")
    ]

    dimensions = ds.universe.conform(("visit", "physical_filter"))
    dataId = {
        "instrument": "dummy",
        "visit": 52,
        "physical_filter": "V",
        "band": "v",
        "day_obs": 20250101,
    }
    dataId2 = {
        "instrument": "dummy",
        "visit": 53,
        "physical_filter": "V",
        "band": "v",
        "day_obs": 20250101,
    }

    for sc in storageClasses:
        ref = ds.make_dataset_ref("metric", dimensions, sc, dataId)
        ref2 = ds.make_dataset_ref("metric", dimensions, sc, dataId2)

        # Make sure that using getManyURIs without predicting before the
        # dataset has been put raises.
        with pytest.raises(FileNotFoundError):
            datastore.getManyURIs([ref], predict=False)

        # Make sure that using getManyURIs with predicting before the
        # dataset has been put predicts the URI.
        uris = datastore.getManyURIs([ref, ref2], predict=True)
        assert "52" in uris[ref].primaryURI.geturl()
        assert "#predicted" in uris[ref].primaryURI.geturl()
        assert "53" in uris[ref2].primaryURI.geturl()
        assert "#predicted" in uris[ref2].primaryURI.geturl()

        datastore.put(metrics, ref)

        # Does it exist?
        assert datastore.exists(ref)
        assert datastore.knows(ref)
        multi = datastore.knows_these([ref])
        assert multi[ref]
        multi = datastore.mexists([ref, ref2])
        assert multi[ref]
        assert not multi[ref2]

        # Get
        metricsOut = datastore.get(ref, parameters=None)
        assert metrics == metricsOut

        uri = datastore.getURI(ref)
        assert uri.scheme == ds.profile.uri_scheme

        uris = datastore.getManyURIs([ref])
        assert len(uris) == 1
        ref, uri = uris.popitem()
        assert uri.primaryURI.exists()
        assert not uri.componentURIs

        # Get a component -- we need to construct new refs for them
        # with derived storage classes but with parent ID
        for comp in ("data", "output"):
            compRef = ref.makeComponentRef(comp)
            output = datastore.get(compRef)
            assert output == getattr(metricsOut, comp)

            uri = datastore.getURI(compRef)
            assert uri.scheme == ds.profile.uri_scheme

            uris = datastore.getManyURIs([compRef])
            assert len(uris) == 1

    storageClass = sc

    # Check that we can put a metric with None in a component and
    # get it back as None
    metricsNone = make_example_metrics(use_none=True)
    dataIdNone = {
        "instrument": "dummy",
        "visit": 54,
        "physical_filter": "V",
        "band": "v",
        "day_obs": 20250101,
    }
    refNone = ds.make_dataset_ref("metric", dimensions, sc, dataIdNone)
    datastore.put(metricsNone, refNone)

    comp = "data"
    for comp in ("data", "output"):
        compRef = refNone.makeComponentRef(comp)
        output = datastore.get(compRef)
        assert output == getattr(metricsNone, comp)

    # Check that a put fails if the dataset type is not supported
    if ds.profile.has_unsupported_put:
        sc = StorageClass("UnsupportedSC", pytype=type(metrics))
        ref = ds.make_dataset_ref("unsupportedType", dimensions, sc, dataId)
        with pytest.raises(DatasetTypeNotSupportedError):
            datastore.put(metrics, ref)

    # These should raise
    ref = ds.make_dataset_ref("metrics", dimensions, storageClass, dataId)
    with pytest.raises(FileNotFoundError):
        # non-existing file
        datastore.get(ref)

    # Get a URI from it
    uri = datastore.getURI(ref, predict=True)
    assert uri.scheme == ds.profile.uri_scheme

    with pytest.raises(FileNotFoundError):
        datastore.getURI(ref)


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_trust_get_request(ds: DatastoreHarness) -> None:
    """Check that we can get datasets that registry knows nothing about."""
    datastore = ds.make_datastore()

    # Skip test if the attribute is not defined
    if not hasattr(datastore, "trustGetRequest"):
        return

    metrics = make_example_metrics()

    i = 0
    for sc_name in ("StructuredDataNoComponents", "StructuredData", "StructuredComposite"):
        i += 1
        datasetTypeName = f"test_metric{i}"  # Different dataset type name each time.

        if sc_name == "StructuredComposite":
            disassembled = True
        else:
            disassembled = False

        # Start datastore in default configuration of using registry
        datastore.trustGetRequest = False

        # Create multiple storage classes for testing with or without
        # disassembly
        sc = ds.storage_class_factory.getStorageClass(sc_name)
        dimensions = ds.universe.conform(("visit", "physical_filter"))

        dataId = {
            "instrument": "dummy",
            "visit": 52 + i,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }

        ref = ds.make_dataset_ref(datasetTypeName, dimensions, sc, dataId)
        datastore.put(metrics, ref)

        # Does it exist?
        assert datastore.exists(ref)
        assert datastore.knows(ref)
        multi = datastore.knows_these([ref])
        assert multi[ref]
        multi = datastore.mexists([ref])
        assert multi[ref]

        # Get
        metricsOut = datastore.get(ref)
        assert metrics == metricsOut

        # Get the URI(s)
        allURIs = datastore.getURIs(ref)
        primaryURI, componentURIs = allURIs
        if disassembled:
            assert primaryURI is None
            assert len(componentURIs) == 3
            assert list(allURIs.iter_all()) == list(componentURIs.values())
        else:
            assert datasetTypeName in primaryURI.path
            assert not componentURIs
            assert list(allURIs.iter_all()) == [primaryURI]

        # Delete registry entry so now we are trusting
        datastore.removeStoredItemInfo(ref)

        # Now stop trusting and check that things break
        datastore.trustGetRequest = False

        # Does it exist?
        assert not datastore.exists(ref)
        assert not datastore.knows(ref)
        multi = datastore.knows_these([ref])
        assert not multi[ref]
        multi = datastore.mexists([ref])
        assert not multi[ref]

        with pytest.raises(FileNotFoundError):
            datastore.get(ref)

        if sc_name != "StructuredDataNoComponents":
            with pytest.raises(FileNotFoundError):
                datastore.get(ref.makeComponentRef("data"))

        # URI should fail unless we ask for prediction
        with pytest.raises(FileNotFoundError):
            datastore.getURIs(ref)

        predicted_primary, predicted_disassembled = datastore.getURIs(ref, predict=True)
        if disassembled:
            assert predicted_primary is None
            assert len(predicted_disassembled) == 3
            for uri in predicted_disassembled.values():
                assert uri.fragment == "predicted"
                assert datasetTypeName in uri.path
        else:
            assert datasetTypeName in predicted_primary.path
            assert not predicted_disassembled
            assert predicted_primary.fragment == "predicted"

        # Now enable registry-free trusting mode
        datastore.trustGetRequest = True

        # Try again to get it
        metricsOut = datastore.get(ref)
        assert metricsOut == metrics

        # Does it exist?
        assert datastore.exists(ref)

        # Get a component
        if sc_name != "StructuredDataNoComponents":
            comp = "data"
            compRef = ref.makeComponentRef(comp)
            output = datastore.get(compRef)
            assert output == getattr(metrics, comp)

        # Get the URI -- if we trust this should work even without
        # enabling prediction.
        primaryURI2, componentURIs2 = datastore.getURIs(ref)
        assert primaryURI2 == primaryURI
        assert componentURIs2 == componentURIs

        # Check for compatible storage class.
        if sc_name in ("StructuredDataNoComponents", "StructuredData"):
            # Make new dataset ref with compatible storage class.
            ref_comp = ref.overrideStorageClass("StructuredDataDictJson")

            # Without `set_retrieve_dataset_type_method` it will fail to
            # find correct file.
            assert not datastore.exists(ref_comp)
            with pytest.raises(FileNotFoundError):
                datastore.get(ref_comp)
            with pytest.raises(FileNotFoundError):
                datastore.get(ref, storageClass="StructuredDataDictJson")

            # Need a special method to generate stored dataset type.
            def _stored_dataset_type(name: str, ref: DatasetRef = ref) -> DatasetType:
                if name == ref.datasetType.name:
                    return ref.datasetType
                raise ValueError(f"Unexpected dataset type name {ref.datasetType.name}")

            datastore.set_retrieve_dataset_type_method(_stored_dataset_type)

            # Storage class override with original dataset ref.
            metrics_as_dict = datastore.get(ref, storageClass="StructuredDataDictJson")
            assert isinstance(metrics_as_dict, dict)

            # get() should return a dict now.
            metrics_as_dict = datastore.get(ref_comp)
            assert isinstance(metrics_as_dict, dict)

            # exists() should work as well.
            assert datastore.exists(ref_comp)

            datastore.set_retrieve_dataset_type_method(None)


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
@pytest.mark.parametrize(("i", "sc_name"), list(enumerate(COMPOSITE_STORAGE_CLASS_NAMES)))
def test_disassembly(ds: DatastoreHarness, i: int, sc_name: str) -> None:
    """Test disassembly within datastore."""
    metrics = make_example_metrics()
    if ds.profile.is_ephemeral:
        # in-memory datastore does not disassemble
        return

    sc = ds.storage_class_factory.getStorageClass(sc_name)

    # Create the test datastore
    datastore = ds.make_datastore()

    # Dummy dataId
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    dataId = {"instrument": "dummy", "visit": 428, "physical_filter": "R"}

    # Create a different dataset type each time round
    # so that a test failure in this subtest does not trigger
    # a cascade of tests because of file clashes
    ref = ds.make_dataset_ref(f"metric_comp_{i}", dimensions, sc, dataId)

    disassembled = sc.name not in {"StructuredData", "StructuredCompositeReadCompNoDisassembly"}

    datastore.put(metrics, ref)

    baseURI, compURIs = datastore.getURIs(ref)
    if disassembled:
        assert baseURI is None
        assert set(compURIs) == {"data", "output", "summary"}
    else:
        assert baseURI is not None
        assert compURIs == {}

    metrics_get = datastore.get(ref)
    assert metrics_get == metrics

    # Retrieve the composite with read parameter
    stop = 4
    metrics_get = datastore.get(ref, parameters={"slice": slice(stop)})
    assert metrics_get.summary == metrics.summary
    assert metrics_get.output == metrics.output
    assert metrics_get.data == metrics.data[:stop]

    # Retrieve a component
    data = datastore.get(ref.makeComponentRef("data"))
    assert data == metrics.data

    # On supported storage classes attempt to access a read
    # only component
    if "ReadComp" in sc.name:
        cRef = ref.makeComponentRef("counter")
        counter = datastore.get(cRef)
        assert counter == len(metrics.data)

        counter = datastore.get(cRef, parameters={"slice": slice(stop)})
        assert counter == stop

    datastore.remove(ref)


def _prep_delete_test(ds: DatastoreHarness, n_refs: int = 1) -> tuple[Datastore, tuple[DatasetRef, ...]]:
    metrics = make_example_metrics()
    datastore = ds.make_datastore()
    # Put
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    sc = ds.storage_class_factory.getStorageClass("StructuredData")
    refs = []
    for i in range(n_refs):
        dataId = {
            "instrument": "dummy",
            "visit": 638 + i,
            "physical_filter": "U",
            "band": "u",
            "day_obs": 20250101,
        }
        ref = ds.make_dataset_ref("metric", dimensions, sc, dataId)
        datastore.put(metrics, ref)

        # Does it exist?
        assert datastore.exists(ref)

        # Get
        metricsOut = datastore.get(ref)
        assert metrics == metricsOut
        refs.append(ref)

    return datastore, *refs


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_remove(ds: DatastoreHarness) -> None:
    datastore, ref = _prep_delete_test(ds)

    # Remove
    datastore.remove(ref)

    # Does it exist?
    assert not datastore.exists(ref)

    # Do we now get a predicted URI?
    uri = datastore.getURI(ref, predict=True)
    assert uri.fragment == "predicted"

    # Get should now fail
    with pytest.raises(FileNotFoundError):
        datastore.get(ref)
    # Can only delete once
    with pytest.raises(FileNotFoundError):
        datastore.remove(ref)


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_forget(ds: DatastoreHarness) -> None:
    datastore, ref = _prep_delete_test(ds)

    # Remove
    datastore.forget([ref])

    # Does it exist (as far as we know)?
    assert not datastore.exists(ref)

    # Do we now get a predicted URI?
    uri = datastore.getURI(ref, predict=True)
    assert uri.fragment == "predicted"

    # Get should now fail
    with pytest.raises(FileNotFoundError):
        datastore.get(ref)

    # Forgetting again is a silent no-op
    datastore.forget([ref])

    # Predicted URI should still point to the file.
    assert uri.exists()


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_transfer(ds: DatastoreHarness) -> None:
    metrics = make_example_metrics()

    dimensions = ds.universe.conform(("visit", "physical_filter"))
    dataId = {
        "instrument": "dummy",
        "visit": 2048,
        "physical_filter": "Uprime",
        "band": "u",
        "day_obs": 20250101,
    }

    sc = ds.storage_class_factory.getStorageClass("StructuredData")
    ref = ds.make_dataset_ref("metric", dimensions, sc, dataId)

    inputDatastore = ds.make_datastore("test_input_datastore")
    outputDatastore = ds.make_datastore("test_output_datastore")

    inputDatastore.put(metrics, ref)
    outputDatastore.transfer(inputDatastore, ref)

    metricsOut = outputDatastore.get(ref)
    assert metrics == metricsOut


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_basic_transaction(ds: DatastoreHarness) -> None:
    datastore = ds.make_datastore()
    storageClass = ds.storage_class_factory.getStorageClass("StructuredData")
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    nDatasets = 6
    dataIds = [
        {"instrument": "dummy", "visit": i, "physical_filter": "V", "band": "v", "day_obs": 20250101}
        for i in range(nDatasets)
    ]
    data = [
        (
            ds.make_dataset_ref("metric", dimensions, storageClass, dataId),
            make_example_metrics(),
        )
        for dataId in dataIds
    ]
    succeed = data[: nDatasets // 2]
    fail = data[nDatasets // 2 :]
    # All datasets added in this transaction should continue to exist
    with datastore.transaction():
        for ref, metrics in succeed:
            datastore.put(metrics, ref)
    # Whereas datasets added in this transaction should not
    # The block is inherently multi-statement: the test exists to show
    # that everything inside the transaction rolls back.
    with pytest.raises(TransactionTestError), datastore.transaction():  # noqa: PT012
        for ref, metrics in fail:
            datastore.put(metrics, ref)
        raise TransactionTestError("This should propagate out of the context manager")
    # Check for datasets that should exist
    for ref, metrics in succeed:
        # Does it exist?
        assert datastore.exists(ref)
        # Get
        metricsOut = datastore.get(ref, parameters=None)
        assert metrics == metricsOut
        # URI
        uri = datastore.getURI(ref)
        assert uri.scheme == ds.profile.uri_scheme
    # Check for datasets that should not exist
    for ref, _ in fail:
        # These should raise
        with pytest.raises(FileNotFoundError):
            # non-existing file
            datastore.get(ref)
        with pytest.raises(FileNotFoundError):
            datastore.getURI(ref)


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_nested_transaction(ds: DatastoreHarness) -> None:
    datastore = ds.make_datastore()
    storageClass = ds.storage_class_factory.getStorageClass("StructuredData")
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    metrics = make_example_metrics()

    dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V", "band": "v", "day_obs": 20250101}
    refBefore = ds.make_dataset_ref("metric", dimensions, storageClass, dataId)
    datastore.put(metrics, refBefore)
    # The block is inherently multi-statement: the test exists to show
    # that everything inside the transaction rolls back.
    with pytest.raises(TransactionTestError), datastore.transaction():  # noqa: PT012
        dataId = {
            "instrument": "dummy",
            "visit": 1,
            "physical_filter": "V",
            "band": "v",
            "day_obs": 20250101,
        }
        refOuter = ds.make_dataset_ref("metric", dimensions, storageClass, dataId)
        datastore.put(metrics, refOuter)
        with datastore.transaction():
            dataId = {
                "instrument": "dummy",
                "visit": 2,
                "physical_filter": "V",
                "band": "v",
                "day_obs": 20250101,
            }
            refInner = ds.make_dataset_ref("metric", dimensions, storageClass, dataId)
            datastore.put(metrics, refInner)
        # All datasets should exist
        for ref in (refBefore, refOuter, refInner):
            metricsOut = datastore.get(ref, parameters=None)
            assert metrics == metricsOut
        raise TransactionTestError("This should roll back the transaction")
    # Dataset(s) inserted before the transaction should still exist
    metricsOut = datastore.get(refBefore, parameters=None)
    assert metrics == metricsOut
    # But all datasets inserted during the (rolled back) transaction
    # should be gone
    with pytest.raises(FileNotFoundError):
        datastore.get(refOuter)
    with pytest.raises(FileNotFoundError):
        datastore.get(refInner)


def _prepare_ingest_test(ds: DatastoreHarness) -> tuple[MetricsExample, DatasetRef]:
    storageClass = ds.storage_class_factory.getStorageClass("StructuredData")
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    metrics = make_example_metrics()
    dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V", "band": "v", "day_obs": 20250101}
    ref = ds.make_dataset_ref("metric", dimensions, storageClass, dataId)
    return metrics, ref


def _run_ingest_test(ds: DatastoreHarness, func: Callable[[MetricsExample, str, DatasetRef], None]) -> None:
    metrics, ref = _prepare_ingest_test(ds)
    # The file will be deleted after the test.
    # For symlink tests this leads to a situation where the datastore
    # points to a file that does not exist. This will make os.path.exist
    # return False but then the new symlink will fail with
    # FileExistsError later in the code so the test still passes.
    with _temp_yaml_file(metrics._asdict()) as path:
        func(metrics, path, ref)


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
@pytest.mark.parametrize("mode", [None, "auto"])
def test_ingest_no_transfer(ds: DatastoreHarness, mode: str | None) -> None:
    """Test ingesting existing files with no transfer."""
    # Some datastores have auto but can't do in place transfer
    if (
        mode == "auto"
        and "auto" in ds.profile.ingest_transfer_modes
        and not ds.profile.can_ingest_no_transfer_auto
    ):
        pytest.skip("Datastore supports auto but cannot transfer in place.")

    datastore = ds.make_datastore()

    def succeed(
        obj: MetricsExample,
        path: str,
        ref: DatasetRef,
        mode: str | None = mode,
        datastore: Datastore = datastore,
    ) -> None:
        """Ingest a file already in the datastore root."""
        # first move it into the root, and adjust the path
        # accordingly.
        # In the case of a ChainedDatastore, we have multiple
        # roots, all of which will accept the file, so we
        # have to copy it into all the roots.
        relative_path = None
        for root in datastore.roots.values():
            if root is not None:
                copied_path = shutil.copy(path, root.ospath)
                relative_path = os.path.relpath(copied_path, start=root.ospath)
        assert relative_path is not None, (
            "Running a FileDatastore test on a Datastore instance without any roots"
        )
        datastore.ingest(FileDataset(path=relative_path, refs=ref), transfer=mode)
        assert obj == datastore.get(ref)

    def failInputDoesNotExist(
        obj: MetricsExample,
        path: str,
        ref: DatasetRef,
        mode: str | None = mode,
        datastore: Datastore = datastore,
    ) -> None:
        """Can't ingest files if we're given a bad path."""
        with pytest.raises(FileNotFoundError):
            datastore.ingest(FileDataset(path="this-file-does-not-exist.yaml", refs=ref), transfer=mode)
        assert not datastore.exists(ref)

    def failOutsideRoot(
        obj: MetricsExample,
        path: str,
        ref: DatasetRef,
        mode: str | None = mode,
        datastore: Datastore = datastore,
    ) -> None:
        """Can't ingest files outside of datastore root unless
        auto.
        """
        if mode == "auto":
            datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)
            assert datastore.exists(ref)
        else:
            with pytest.raises(RuntimeError):
                datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)
            assert not datastore.exists(ref)

    def failNotImplemented(
        obj: MetricsExample,
        path: str,
        ref: DatasetRef,
        mode: str | None = mode,
        datastore: Datastore = datastore,
    ) -> None:
        with pytest.raises(NotImplementedError):
            datastore.ingest(FileDataset(path=path, refs=ref), transfer=mode)

    if mode in ds.profile.ingest_transfer_modes:
        _run_ingest_test(ds, failOutsideRoot)
        _run_ingest_test(ds, failInputDoesNotExist)
        _run_ingest_test(ds, succeed)
    else:
        _run_ingest_test(ds, failNotImplemented)


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
@pytest.mark.parametrize("mode", INGEST_TRANSFER_MODES)
def test_ingest_transfer(ds: DatastoreHarness, mode: str) -> None:
    """Test ingesting existing files after transferring them."""
    datastore = ds.make_datastore(mode)

    def succeed(
        obj: MetricsExample,
        path: str,
        ref: DatasetRef,
        mode: str | None = mode,
        datastore: Datastore = datastore,
    ) -> None:
        """Ingest a file by transferring it to the template
        location.
        """
        datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)
        assert obj == datastore.get(ref)
        file_exists = os.path.exists(path)
        if mode == "move":
            assert not file_exists
        else:
            assert file_exists

    def failInputDoesNotExist(
        obj: MetricsExample,
        path: str,
        ref: DatasetRef,
        mode: str | None = mode,
        datastore: Datastore = datastore,
    ) -> None:
        """Can't ingest files if we're given a bad path."""
        with pytest.raises(FileNotFoundError):
            # Ensure the file does not look like it is in
            # datastore for auto mode
            datastore.ingest(FileDataset(path="../this-file-does-not-exist.yaml", refs=ref), transfer=mode)
        assert not datastore.exists(ref), f"Checking not in datastore using mode {mode}"

    def failNotImplemented(
        obj: MetricsExample,
        path: str,
        ref: DatasetRef,
        mode: str | None = mode,
        datastore: Datastore = datastore,
    ) -> None:
        with pytest.raises(NotImplementedError):
            datastore.ingest(FileDataset(path=os.path.abspath(path), refs=ref), transfer=mode)

    if mode in ds.profile.ingest_transfer_modes:
        _run_ingest_test(ds, failInputDoesNotExist)
        _run_ingest_test(ds, succeed)
    else:
        _run_ingest_test(ds, failNotImplemented)


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_ingest_symlink_of_symlink(ds: DatastoreHarness) -> None:
    """Special test for symlink to a symlink ingest"""
    metrics, ref = _prepare_ingest_test(ds)
    # The aim of this test is to create a dataset on disk, then
    # create a symlink to it and finally ingest the symlink such that
    # the symlink in the datastore points to the original dataset.
    for mode in ("symlink", "relsymlink"):
        if mode not in ds.profile.ingest_transfer_modes:
            continue

        print(f"Trying mode {mode}")
        with _temp_yaml_file(metrics._asdict()) as realpath:
            with tempfile.TemporaryDirectory() as tmpdir:
                sympath = os.path.join(tmpdir, "symlink.yaml")
                os.symlink(os.path.realpath(realpath), sympath)

                datastore = ds.make_datastore()
                datastore.ingest(FileDataset(path=os.path.abspath(sympath), refs=ref), transfer=mode)

                uri = datastore.getURI(ref)
                assert uri.isLocal, f"Check {uri.scheme}"
                assert os.path.islink(uri.ospath), f"Check {uri} is a symlink"

                linkTarget = os.readlink(uri.ospath)
                if mode == "relsymlink":
                    assert not os.path.isabs(linkTarget)
                else:
                    assert os.path.samefile(linkTarget, realpath)

                # Check that we can get the dataset back regardless of mode
                metric2 = datastore.get(ref)
                assert metric2 == metrics

                # Cleanup the file for next time round loop
                # since it will get the same file name in store
                datastore.remove(ref)


def _populate_export_datastore(ds: DatastoreHarness, name: str) -> tuple[Datastore, list[DatasetRef]]:
    datastore = ds.make_datastore(name)

    # For now only the FileDatastore can be used for this test.
    # ChainedDatastore that only includes InMemoryDatastores have to be
    # skipped as well.
    for name in datastore.names:
        if not name.startswith("InMemoryDatastore"):
            break
    else:
        pytest.skip("in-memory datastore does not support record export/import")

    metrics = make_example_metrics()
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    sc = ds.storage_class_factory.getStorageClass("StructuredData")

    refs = []
    for visit in (2048, 2049, 2050):
        dataId = {
            "instrument": "dummy",
            "visit": visit,
            "physical_filter": "Uprime",
            "band": "u",
            "day_obs": 20250101,
        }
        ref = ds.make_dataset_ref("metric", dimensions, sc, dataId)
        datastore.put(metrics, ref)
        refs.append(ref)
    return datastore, refs


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_export_import_records(ds: DatastoreHarness) -> None:
    """Test for export_records and import_records methods."""
    datastore, refs = _populate_export_datastore(ds, "test_datastore")
    for exported_refs in (refs, refs[1:]):
        n_refs = len(exported_refs)
        records = datastore.export_records(exported_refs)
        assert len(records) > 0
        assert set(records.keys()) <= set(datastore.names)
        # In a ChainedDatastore each FileDatastore will have a complete set
        for datastore_name in records:
            record_data = records[datastore_name]
            assert len(record_data.records) == n_refs

            # Check that subsetting works, include non-existing dataset ID.
            dataset_ids = {exported_refs[0].id, uuid.uuid4()}
            subset = record_data.subset(dataset_ids)
            assert subset is not None
            assert len(subset.records) == 1
            subset = record_data.subset({uuid.uuid4()})
            assert subset is None

    # Use the same datastore name to import relative path.
    datastore2 = ds.make_datastore("test_datastore")

    records = datastore.export_records(refs[1:])
    datastore2.import_records(records)

    with pytest.raises(FileNotFoundError):
        data = datastore2.get(refs[0])
    data = datastore2.get(refs[1])
    assert data is not None
    data = datastore2.get(refs[2])
    assert data is not None


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_export_import_table(ds: DatastoreHarness) -> None:
    datastore, refs = _populate_export_datastore(ds, "test_datastore")
    table = datastore.export_table([ref.id for ref in refs])
    datastore2 = ds.make_datastore("test_datastore")
    datastore2.import_table(table)

    for ref in refs:
        assert datastore2.get(ref) is not None
        assert datastore.getURI(ref) == datastore2.getURI(ref)
        original_info = datastore.get_file_info_for_transfer([ref.id])[ref.id][0]
        imported_info_list = datastore.get_file_info_for_transfer([ref.id]).get(ref.id)
        assert imported_info_list is not None
        assert len(imported_info_list) == 1
        imported_info = imported_info_list[0]
        assert imported_info.file_info.formatter == original_info.file_info.formatter
        assert imported_info.file_info.storage_class_name == original_info.file_info.storage_class_name
        assert imported_info.file_info.file_size == original_info.file_info.file_size
        assert imported_info.file_info.checksum == original_info.file_info.checksum
        assert imported_info.file_info.component == original_info.file_info.component


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_export_predicted_records(ds: DatastoreHarness) -> None:
    if ds.profile.is_ephemeral:
        pytest.skip("in-memory datastore does not support record export/import")
    sc = ds.storage_class_factory.getStorageClass("ThingOne")
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    dataId = {
        "instrument": "dummy",
        "visit": 52,
        "physical_filter": "V",
        "band": "v",
        "day_obs": 20250101,
    }
    ref = ds.make_dataset_ref("metric", dimensions, sc, dataId)

    datastore = ds.make_datastore("test_datastore")
    names = {n for n in datastore.names if not n.startswith("InMemory")}
    records = datastore.export_predicted_records([ref])

    # Expect predicted records from all datastores.
    assert set(records.keys()) == names

    for record_data in records.values():
        assert len(record_data.records) == 1


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_export(ds: DatastoreHarness) -> None:
    datastore, refs = _populate_export_datastore(ds, "test_datastore")

    datasets = list(datastore.export(refs))
    assert len(datasets) == 3

    for transfer in (None, "auto"):
        # Both will default to None
        datasets = list(datastore.export(refs, transfer=transfer))
        assert len(datasets) == 3

    with pytest.raises(TypeError):
        list(datastore.export(refs, transfer="copy"))

    with pytest.raises(TypeError):
        list(datastore.export(refs, directory="exportDir", transfer="move"))

    # Create a new ref that is not known to the datastore and try to
    # export it.
    sc = ds.storage_class_factory.getStorageClass("ThingOne")
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    dataId = {
        "instrument": "dummy",
        "visit": 52,
        "physical_filter": "V",
        "band": "v",
        "day_obs": 20250101,
    }
    ref = ds.make_dataset_ref("metric", dimensions, sc, dataId)
    with pytest.raises(FileNotFoundError):
        list(datastore.export(refs + [ref], transfer=None))


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_pydantic_dict_storage_class_conversions(ds: DatastoreHarness) -> None:
    """Test converting a dataset stored as a pydantic model into a dict on
    read.
    """
    datastore = ds.make_datastore()
    store_as_model = ds.make_dataset_ref(
        "store_as_model",
        dimensions=ds.universe.empty,
        storageClass="DictConvertibleModel",
        dataId=DataCoordinate.make_empty(ds.universe),
    )
    content = {"a": "one", "b": "two"}
    model = DictConvertibleModel.from_dict(content, extra="original content")
    datastore.put(model, store_as_model)
    retrieved_model = datastore.get(store_as_model)
    assert retrieved_model == model
    loaded = datastore.get(store_as_model.overrideStorageClass("NativeDictForConvertibleModel"))
    assert type(loaded) is dict
    assert loaded == content


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_simple_class_put_get(ds: DatastoreHarness) -> None:
    """Test that we can put and get a simple class with dict()
    constructor.
    """
    datastore = ds.make_datastore()
    data = MetricsExample(summary={"a": 1}, data=[1, 2, 3], output={"b": 2})
    _assert_different_puts(ds, datastore, "MetricsExample", data)


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_dataclass_put_get(ds: DatastoreHarness) -> None:
    """Test that we can put and get a simple dataclass."""
    datastore = ds.make_datastore()
    data = MetricsExampleDataclass(summary={"a": 1}, data=[1, 2, 3], output={"b": 2})
    _assert_different_puts(ds, datastore, "MetricsExampleDataclass", data)


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_pydantic_put_get(ds: DatastoreHarness) -> None:
    """Test that we can put and get a simple Pydantic model."""
    datastore = ds.make_datastore()
    data = MetricsExampleModel(summary={"a": 1}, data=[1, 2, 3], output={"b": 2})
    _assert_different_puts(ds, datastore, "MetricsExampleModel", data)


@pytest.mark.parametrize("ds", ALL_PROFILES, indirect=True)
def test_tuple_put_get(ds: DatastoreHarness) -> None:
    """Test that we can put and get a tuple."""
    datastore = ds.make_datastore()
    data = ("a", "b", 1)
    _assert_different_puts(ds, datastore, "TupleExample", data)


def _assert_different_puts(
    ds: DatastoreHarness, datastore: Datastore, storageClass_root: str, data: Any
) -> None:
    refs = {
        x: ds.make_dataset_ref(
            f"stora_as_{x}",
            dimensions=ds.universe.empty,
            storageClass=f"{storageClass_root}{x}",
            dataId=DataCoordinate.make_empty(ds.universe),
        )
        for x in ["A", "B"]
    }

    for ref in refs.values():
        datastore.put(data, ref)

    assert datastore.get(refs["A"]) == datastore.get(refs["B"])


@pytest.mark.parametrize("ds", FILE_PROFILES, indirect=True)
def test_atomic_write(ds: DatastoreHarness, caplog: pytest.LogCaptureFixture) -> None:
    """Test that we write to a temporary and then rename"""
    datastore = ds.make_datastore()
    storageClass = ds.storage_class_factory.getStorageClass("StructuredData")
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    metrics = make_example_metrics()

    dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V", "band": "v", "day_obs": 20250101}
    ref = ds.make_dataset_ref("metric", dimensions, storageClass, dataId)

    with caplog.at_level(logging.DEBUG, logger="lsst.resources"):
        caplog.clear()
        datastore.put(metrics, ref)
        records = records_from(caplog, "lsst.resources", logging.DEBUG)
    move_logs = [record.getMessage() for record in records if "transfer=" in record.getMessage()]
    assert move_logs, "Expected a transfer log message from lsst.resources"
    assert "transfer=move" in move_logs[0]

    # And the transfer should be file to file.
    assert move_logs[0].count("file://") == 2


@pytest.mark.parametrize("ds", FILE_PROFILES, indirect=True)
def test_can_not_determine_put_formatter_location(ds: DatastoreHarness) -> None:
    """Verify that the expected exception is raised if the FileDatastore
    can not determine the put formatter location.
    """
    _ = make_example_metrics()
    datastore = ds.make_datastore()

    # Create multiple storage classes for testing different formulations
    storageClass = ds.storage_class_factory.getStorageClass("StructuredData")

    sccomp = StorageClass("Dummy")
    compositeStorageClass = StorageClass(
        "StructuredComposite", components={"dummy": sccomp, "dummy2": sccomp}
    )

    dimensions = ds.universe.conform(("visit", "physical_filter"))
    dataId = {
        "instrument": "dummy",
        "visit": 52,
        "physical_filter": "V",
        "band": "v",
        "day_obs": 20250101,
    }

    ref = ds.make_dataset_ref("metric", dimensions, storageClass, dataId)
    compRef = ds.make_dataset_ref("metric", dimensions, compositeStorageClass, dataId)

    def raiser(ref: DatasetRef) -> None:
        raise DatasetTypeNotSupportedError()

    with unittest.mock.patch.object(
        lsst.daf.butler.datastores.fileDatastore.FileDatastore,
        "_determine_put_formatter_location",
        side_effect=raiser,
    ):
        # verify the non-composite ref execution path:
        with pytest.raises(DatasetTypeNotSupportedError):
            datastore.getURIs(ref, predict=True)

        # verify the composite-ref execution path:
        with pytest.raises(DatasetTypeNotSupportedError):
            datastore.getURIs(compRef, predict=True)


@pytest.mark.parametrize("ds", FILE_PROFILES, indirect=True)
def test_roots(ds: DatastoreHarness) -> None:
    datastore = ds.make_datastore()

    assert set(datastore.names) == set(datastore.roots.keys())
    for root in datastore.roots.values():
        if root is not None:
            assert root.exists()


@pytest.mark.parametrize("ds", FILE_PROFILES, indirect=True)
def test_prepare_get_for_external_client(ds: DatastoreHarness) -> None:
    datastore = ds.make_datastore()
    storageClass = ds.storage_class_factory.getStorageClass("StructuredData")
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    dataId = {"instrument": "dummy", "visit": 52, "physical_filter": "V", "band": "v"}
    ref = ds.make_dataset_ref("metric", dimensions, storageClass, dataId)
    # Most of the coverage for this function is in test_server.py,
    # because it requires a file backend that supports URL signing.
    assert datastore.prepare_get_for_external_client(ref) is None


@pytest.mark.parametrize("ds", ["posix-no-checksums"], indirect=True)
def test_checksum(ds: DatastoreHarness) -> None:
    """Ensure that checksums have not been calculated."""
    datastore = ds.make_datastore()
    storageClass = ds.storage_class_factory.getStorageClass("StructuredData")
    dimensions = ds.universe.conform(("visit", "physical_filter"))
    metrics = make_example_metrics()

    dataId = {"instrument": "dummy", "visit": 0, "physical_filter": "V", "band": "v", "day_obs": 20250101}
    ref = ds.make_dataset_ref("metric", dimensions, storageClass, dataId)

    # Configuration should have disabled checksum calculation
    datastore.put(metrics, ref)
    infos = datastore.getStoredItemsInfo(ref)
    assert infos[0].checksum is None

    # Remove put back but with checksums enabled explicitly
    datastore.remove(ref)
    datastore.useChecksum = True
    datastore.put(metrics, ref)

    infos = datastore.getStoredItemsInfo(ref)
    assert infos[0].checksum is not None


@pytest.mark.parametrize("ds", ["posix-no-checksums"], indirect=True)
def test_repeat_ingest(ds: DatastoreHarness) -> None:
    """Test that repeatedly ingesting the same file in direct mode
    is allowed.

    Test can only run with FileDatastore since that is the only one
    supporting "direct" ingest.
    """
    metrics, v4ref = _prepare_ingest_test(ds)
    datastore = ds.make_datastore()
    v5ref = DatasetRef(
        v4ref.datasetType, v4ref.dataId, v4ref.run, id_generation_mode=DatasetIdGenEnum.DATAID_TYPE_RUN
    )

    with _temp_yaml_file(metrics._asdict()) as path:
        datastore.ingest(FileDataset(path=path, refs=v4ref), transfer="direct")

        # This will fail because the ref is using UUIDv4.
        with pytest.raises(RuntimeError):
            datastore.ingest(FileDataset(path=path, refs=v4ref), transfer="direct")

        # UUIDv5 can be repeatedly ingested in direct mode.
        datastore.ingest(FileDataset(path=path, refs=v5ref), transfer="direct")
        datastore.ingest(FileDataset(path=path, refs=v5ref), transfer="direct")

        with pytest.raises(RuntimeError):
            datastore.ingest(FileDataset(path=path, refs=v5ref), transfer="copy")


@pytest.mark.parametrize("ds", ["trash"], indirect=True)
def test_trash(ds: DatastoreHarness) -> None:
    datastore, *refs = _prep_delete_test(ds, n_refs=10)

    # Trash one of them.
    ref = refs.pop()
    uri = datastore.getURI(ref)
    datastore.trash(ref)
    assert uri.exists(), uri  # Not deleted yet
    datastore.emptyTrash()
    assert not uri.exists(), uri

    # Trash it again should be fine.
    datastore.trash(ref)

    # Trash multiple items at once.
    subset = [refs.pop(), refs.pop()]
    datastore.trash(subset)
    datastore.emptyTrash()

    # Remove a record and trash should do nothing.
    # This is execution butler scenario.
    ref = refs.pop()
    uri = datastore.getURI(ref)
    datastore._table.delete(["dataset_id"], {"dataset_id": ref.id})
    assert uri.exists()
    datastore.trash(ref)
    datastore.emptyTrash()
    assert uri.exists()

    # Switch on trust and it should delete the file.
    datastore.trustGetRequest = True
    datastore.trash([ref])
    assert not uri.exists()

    # Remove multiples at once in trust mode.
    subset = [refs.pop() for i in range(3)]
    datastore.trash(subset)
    datastore.trash(refs.pop())  # Check that a single ref can trash


@pytest.mark.parametrize("ds", ["trash"], indirect=True)
def test_empty_trash(ds: DatastoreHarness) -> None:
    """Test parameters and return value for empty trash."""
    datastore, *refs = _prep_delete_test(ds, n_refs=10)

    # Trash one of them.
    ref = refs.pop()
    uri = datastore.getURI(ref)
    datastore.trash(ref)
    assert uri.exists(), uri  # Not deleted yet

    # Empty trash but with a list of refs that does not include the
    # one in the trash table.
    removed = datastore.emptyTrash(refs=refs)
    assert len(removed) == 0
    assert uri.exists(), uri

    # Empty the entire trash but in dry_run mode.
    removed = datastore.emptyTrash(dry_run=True)
    assert len(removed) == 1
    assert removed.pop() == uri
    assert uri.exists(), uri

    # Empty the trash specifying the actual ref.
    removed = datastore.emptyTrash(refs=[ref])
    assert len(removed) == 1
    assert removed.pop() == uri
    assert not uri.exists(), uri

    # Trash everything and empty.
    datastore.trash(refs)
    removed = datastore.emptyTrash(dry_run=True)
    for u in removed:
        assert u.exists()
    removed = datastore.emptyTrash()
    for u in removed:
        assert not u.exists()


@pytest.mark.parametrize("ds", ["posix"], indirect=True)
def test_cleanup(ds: DatastoreHarness) -> None:
    """Test that a failed formatter write does cleanup a partial file."""
    metrics = make_example_metrics()
    datastore = ds.make_datastore()

    storageClass = ds.storage_class_factory.getStorageClass("StructuredData")

    dimensions = ds.universe.conform(("visit", "physical_filter"))
    dataId = {
        "instrument": "dummy",
        "visit": 52,
        "physical_filter": "V",
        "band": "v",
        "day_obs": 20250101,
    }

    ref = ds.make_dataset_ref("metric", dimensions, storageClass, dataId)

    # Determine where the file will end up (we assume Formatters use
    # the same file extension)
    expectedUri = datastore.getURI(ref, predict=True)
    assert expectedUri.fragment == "predicted"

    assert expectedUri.getExtension() == ".yaml", f"Is there a file extension in {expectedUri}"

    # Try a formatter that fails, then one that fails and leaves a file
    # behind. These stay a loop rather than becoming a parametrization: the
    # directory the second case asserts on is created by the first case's
    # failed put, so the two are not independent.
    for formatter in (BadWriteFormatter, BadNoWriteFormatter):
        # Monkey patch the formatter
        datastore.formatterFactory.registerFormatter(ref.datasetType, formatter, overwrite=True)

        # Try to put the dataset, it should fail
        with pytest.raises(RuntimeError):
            datastore.put(metrics, ref)

        # Check that there is no file on disk
        assert not expectedUri.exists(), f"Check for existence of {expectedUri}"

        # Check that there is a directory
        dir = expectedUri.dirname()
        assert dir.exists(), f"Check for existence of directory {dir}"

    # Force YamlFormatter and check that this time a file is written
    datastore.formatterFactory.registerFormatter(ref.datasetType, YamlFormatter, overwrite=True)
    datastore.put(metrics, ref)
    assert expectedUri.exists(), f"Check for existence of {expectedUri}"
    datastore.remove(ref)
    assert not expectedUri.exists(), f"Check for existence of now removed {expectedUri}"


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
    pytest.param("chainedDatastorePa.yaml", True, True, id="chained-native"),
    pytest.param("chainedDatastoreP.yaml", True, True, id="chained"),
    pytest.param("chainedDatastore2P.yaml", False, False, id="chained-memory"),
]
"""(config file, can ingest, needs a root) for each datastore configuration
that shares the same constraints."""

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


@dataclasses.dataclass
class CacheFixtures:
    """Datasets and files shared by the cache tests."""

    root: str
    """Directory the test files live in."""

    refs: list[DatasetRef]
    """Simple refs, one per file in ``files``."""

    files: list[ResourcePath]
    """Files backing ``refs``."""

    composite_refs: list[DatasetRef]
    """Composite refs, one per entry in ``comp_refs`` and ``comp_files``."""

    comp_refs: list[list[DatasetRef]]
    """Component refs for each composite."""

    comp_files: list[list[ResourcePath]]
    """Files backing ``comp_refs``."""


@pytest.fixture(scope="module")
def universe() -> DimensionUniverse:
    """Dimension universe shared by every test in this module."""
    return DimensionUniverse()


@pytest.fixture(scope="module")
def cache_storage_class_factory() -> StorageClassFactory:
    """Storage classes for the cache tests.

    Named distinctly from the plugin's ``storage_class_factory`` because this
    loads ``storageClasses.yaml`` rather than the Butler configs, matching what
    ``DatastoreCacheTestCase.setUpClass`` did. `StorageClassFactory` is a
    singleton, so the two accumulate rather than conflict.
    """
    factory = StorageClassFactory()
    factory.addFromConfig(os.path.join(TESTDIR, "config/basic/storageClasses.yaml"))
    return factory


@pytest.fixture
def cache(  # numpydoc ignore=PR01
    tmp_path: pathlib.Path,
    universe: DimensionUniverse,
    cache_storage_class_factory: StorageClassFactory,
) -> CacheFixtures:
    """Build the refs and files the cache tests operate on."""
    helper = DatasetTestHelper()
    root = str(tmp_path)

    # Create some test dataset refs and associated test files
    sc = cache_storage_class_factory.getStorageClass("StructuredDataDict")
    dimensions = universe.conform(("visit", "physical_filter"))
    dataId = {
        "instrument": "dummy",
        "visit": 52,
        "physical_filter": "V",
        "band": "v",
        "day_obs": 20250101,
    }

    # Create list of refs and list of temporary files
    n_datasets = 10
    refs = [helper.makeDatasetRef(f"metric{n}", dimensions, sc, dataId) for n in range(n_datasets)]

    root_uri = ResourcePath(root, forceDirectory=True)
    files = [root_uri.join(f"file{n}.txt") for n in range(n_datasets)]

    # Create test files.
    for uri in files:
        uri.write(b"0123456789")

    # Create some composite refs with component files.
    sc = cache_storage_class_factory.getStorageClass("StructuredData")
    composite_refs = [helper.makeDatasetRef(f"composite{n}", dimensions, sc, dataId) for n in range(3)]
    comp_files = []
    comp_refs = []
    for n, ref in enumerate(composite_refs):
        component_refs = []
        component_files = []
        for component in sc.components:
            component_ref = ref.makeComponentRef(component)
            file = root_uri.join(f"composite_file-{n}-{component}.txt")
            component_refs.append(component_ref)
            component_files.append(file)
            file.write(b"9876543210")

        comp_files.append(component_files)
        comp_refs.append(component_refs)

    return CacheFixtures(
        root=root,
        refs=refs,
        files=files,
        composite_refs=composite_refs,
        comp_refs=comp_refs,
        comp_files=comp_files,
    )


def _make_cache_manager(config_str: str, universe: DimensionUniverse) -> DatastoreCacheManager:
    """Build a cache manager from a YAML fragment."""
    config = Config.fromYaml(config_str)
    return DatastoreCacheManager(DatastoreCacheManagerConfig(config), universe=universe)


def _expiration_config(mode: str, threshold: int | str) -> str:
    """Return a cache config using the given expiry mode and threshold."""
    return f"""
cached:
  default: true
  expiry:
    mode: {mode}
    threshold: {threshold}
  cacheable:
    unused: true
    """


def _assert_cache(cache_manager: DatastoreCacheManager, cache: CacheFixtures) -> None:
    """Check the manager caches the first ref and refuses the second."""
    assert cache_manager.should_be_cached(cache.refs[0])
    assert not cache_manager.should_be_cached(cache.refs[1])

    uri = cache_manager.move_to_cache(cache.files[0], cache.refs[0])
    assert isinstance(uri, ResourcePath)
    assert cache_manager.move_to_cache(cache.files[1], cache.refs[1]) is None

    # Check presence in cache using ref and then using file extension.
    assert not cache_manager.known_to_cache(cache.refs[1])
    assert cache_manager.known_to_cache(cache.refs[0])
    assert not cache_manager.known_to_cache(cache.refs[1], cache.files[1].getExtension())
    assert cache_manager.known_to_cache(cache.refs[0], cache.files[0].getExtension())

    # Cached file should no longer exist but uncached file should be
    # unaffected.
    assert not cache.files[0].exists()
    assert cache.files[1].exists()

    # Should find this file and it should be within the cache directory.
    with cache_manager.find_in_cache(cache.refs[0], ".txt") as found:
        assert found.exists()
        assert found.relative_to(cache_manager.cache_directory) is not None

    # Should not be able to find these in cache
    with cache_manager.find_in_cache(cache.refs[0], ".fits") as found:
        assert found is None
    with cache_manager.find_in_cache(cache.refs[1], ".fits") as found:
        assert found is None


def _assert_expiration(
    cache_manager: DatastoreCacheManager,
    cache: CacheFixtures,
    n_datasets: int,
    n_retained: int,
) -> None:
    """Insert the datasets and then check the number retained."""
    for i in range(n_datasets):
        cached = cache_manager.move_to_cache(cache.files[i], cache.refs[i])
        assert cached is not None

    assert cache_manager.file_count == n_retained

    # The oldest file should not be in the cache any more.
    for i in range(n_datasets):
        with cache_manager.find_in_cache(cache.refs[i], ".txt") as found:
            if i >= n_datasets - n_retained:
                assert isinstance(found, ResourcePath)
            else:
                assert found is None


def test_no_cache_dir(cache, universe) -> None:
    """Test a cache configured with no root directory."""
    config_str = """
cached:
  root: null
  cacheable:
    metric0: true
        """
    cache_manager = _make_cache_manager(config_str, universe)

    # Look inside to check we don't have a cache directory
    assert cache_manager._cache_directory is None

    _assert_cache(cache_manager, cache)

    # Test that the cache directory is marked temporary
    assert cache_manager.cache_directory.isTemporary


def test_no_cache_dir_reversed(cache, universe) -> None:
    """Use default caching status and metric1 to false"""
    config_str = """
cached:
  root: null
  default: true
  cacheable:
    metric1: false
        """
    cache_manager = _make_cache_manager(config_str, universe)

    _assert_cache(cache_manager, cache)


def test_envvar_cache_dir(cache, universe) -> None:
    """Test that the cache directory can come from the environment."""
    config_str = f"""
cached:
  root: '{cache.root}'
  cacheable:
    metric0: true
        """

    root = ResourcePath(cache.root, forceDirectory=True)
    env_dir = root.join("somewhere", forceDirectory=True)
    elsewhere = root.join("elsewhere", forceDirectory=True)

    # Environment variable should override the config value.
    with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_CACHE_DIRECTORY": env_dir.ospath}):
        cache_manager = _make_cache_manager(config_str, universe)
    assert cache_manager.cache_directory == env_dir

    # This environment variable should not override the config value.
    with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET": env_dir.ospath}):
        cache_manager = _make_cache_manager(config_str, universe)
    assert cache_manager.cache_directory == root

    # No default setting.
    config_str = """
cached:
  root: null
  default: true
  cacheable:
    metric1: false
        """
    cache_manager = _make_cache_manager(config_str, universe)

    # This environment variable should override the config value.
    with unittest.mock.patch.dict(os.environ, {"DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET": env_dir.ospath}):
        cache_manager = _make_cache_manager(config_str, universe)
    assert cache_manager.cache_directory == env_dir

    # If both environment variables are set the main (not IF_UNSET)
    # variable should win.
    with unittest.mock.patch.dict(
        os.environ,
        {
            "DAF_BUTLER_CACHE_DIRECTORY": env_dir.ospath,
            "DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET": elsewhere.ospath,
        },
    ):
        cache_manager = _make_cache_manager(config_str, universe)
    assert cache_manager.cache_directory == env_dir

    # Use the API to set the environment variable, making sure that the
    # variable is reset on exit.
    with unittest.mock.patch.dict(
        os.environ,
        {"DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET": ""},
    ):
        defined, cache_dir = DatastoreCacheManager.set_fallback_cache_directory_if_unset()
        assert defined
        cache_manager = _make_cache_manager(config_str, universe)
        assert cache_manager.cache_directory == ResourcePath(cache_dir, forceDirectory=True)

    # Now create the cache manager ahead of time and set the fallback
    # later.
    cache_manager = _make_cache_manager(config_str, universe)
    assert cache_manager._cache_directory is None
    with unittest.mock.patch.dict(
        os.environ,
        {"DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET": ""},
    ):
        defined, cache_dir = DatastoreCacheManager.set_fallback_cache_directory_if_unset()
        assert defined
        assert cache_manager.cache_directory == ResourcePath(cache_dir, forceDirectory=True)


def test_explicit_cache_dir(cache, universe) -> None:
    """Test a cache configured with an explicit root directory."""
    config_str = f"""
cached:
  root: '{cache.root}'
  cacheable:
    metric0: true
        """
    cache_manager = _make_cache_manager(config_str, universe)

    # Look inside to check we do have a cache directory.
    assert cache_manager.cache_directory == ResourcePath(cache.root, forceDirectory=True)

    _assert_cache(cache_manager, cache)

    # Test that the cache directory is not marked temporary
    assert not cache_manager.cache_directory.isTemporary


def test_unexpected_files_in_cache_dir(cache, universe) -> None:
    """Test for regression of a bug where extraneous files in a cache
    directory would cause all cache lookups to raise an exception.
    """
    config_str = f"""
cached:
  root: '{cache.root}'
  cacheable:
    metric0: true
        """

    for filename in ["unexpected.txt", "unexpected", "un_expected", "un_expected.txt"]:
        unexpected_file = os.path.join(cache.root, filename)
        with open(unexpected_file, "w") as fh:
            fh.write("test")

    cache_manager = _make_cache_manager(config_str, universe)
    cache_manager.scan_cache()
    _assert_cache(cache_manager, cache)


def test_no_cache(cache, universe) -> None:
    """Test that the disabled cache manager caches nothing."""
    cache_manager = DatastoreDisabledCacheManager("", universe=universe)
    # unittest formatted the failure message whether or not the assertion
    # failed, so this was the only caller of the manager's __str__. A bare
    # assert only formats its message on failure, so compute it up front.
    message = f"{cache_manager}"
    for uri, ref in zip(cache.files, cache.refs, strict=True):
        assert not cache_manager.should_be_cached(ref)
        assert cache_manager.move_to_cache(uri, ref) is None
        assert not cache_manager.known_to_cache(ref)
        with cache_manager.find_in_cache(ref, ".txt") as found:
            assert found is None, message


def test_cache_expiry_files(cache, universe) -> None:
    """Test that ``files`` expiry retains the threshold number of files."""
    threshold = 2  # Keep at least 2 files.
    mode = "files"
    config_str = _expiration_config(mode, threshold)

    cache_manager = _make_cache_manager(config_str, universe)

    # Check that an empty cache returns unknown for arbitrary ref
    assert not cache_manager.known_to_cache(cache.refs[0])

    # Should end with datasets: 2, 3, 4
    _assert_expiration(cache_manager, cache, 5, threshold + 1)
    assert f"{mode}={threshold}" in str(cache_manager)

    # Check that we will not expire a file that is actively in use.
    with cache_manager.find_in_cache(cache.refs[2], ".txt") as found:
        assert found is not None

        # Trigger cache expiration that should remove the file
        # we just retrieved. Should now have: 3, 4, 5
        cached = cache_manager.move_to_cache(cache.files[5], cache.refs[5])
        assert cached is not None

        # Cache should still report the standard file count.
        assert cache_manager.file_count == threshold + 1

        # Add additional entry to cache.
        # Should now have 4, 5, 6
        cached = cache_manager.move_to_cache(cache.files[6], cache.refs[6])
        assert cached is not None

        # Is the file still there?
        assert found.exists()

        # Can we read it?
        data = found.read()
        assert len(data) > 0

    # Outside context the file should no longer exist.
    assert not found.exists()

    # File count should not have changed.
    assert cache_manager.file_count == threshold + 1

    # Dataset 2 was in the exempt directory but because hardlinks
    # are used it was deleted from the main cache during cache expiry
    # above and so should no longer be found.
    with cache_manager.find_in_cache(cache.refs[2], ".txt") as found:
        assert found is None

    # And the one stored after it is also gone.
    with cache_manager.find_in_cache(cache.refs[3], ".txt") as found:
        assert found is None

    # But dataset 4 is present.
    with cache_manager.find_in_cache(cache.refs[4], ".txt") as found:
        assert found is not None

    # Adding a new dataset to the cache should now delete it.
    cache_manager.move_to_cache(cache.files[7], cache.refs[7])

    with cache_manager.find_in_cache(cache.refs[2], ".txt") as found:
        assert found is None


def test_cache_expiry_datasets(cache, universe) -> None:
    """Test that ``datasets`` expiry retains the threshold count."""
    threshold = 2  # Keep 2 datasets.
    mode = "datasets"
    config_str = _expiration_config(mode, threshold)

    cache_manager = _make_cache_manager(config_str, universe)
    _assert_expiration(cache_manager, cache, 5, threshold + 1)
    assert f"{mode}={threshold}" in str(cache_manager)


def test_cache_expiry_datasets_from_disabled(cache, universe) -> None:
    """Test that the expiry-mode envvar enables a disabled cache."""
    threshold = 2
    mode = "datasets"
    with unittest.mock.patch.dict(
        os.environ,
        {"DAF_BUTLER_CACHE_EXPIRATION_MODE": f"{mode}={threshold}"},
    ):
        cache_manager = DatastoreCacheManager.create_disabled(universe=DimensionUniverse())
        _assert_expiration(cache_manager, cache, 5, threshold + 1)
        assert f"{mode}={threshold}" in str(cache_manager)


def test_expiration_mode_override(cache, universe, caplog) -> None:
    """Test that the envvar overrides the configured expiry mode."""
    threshold = 2  # Keep 2 datasets.
    mode = "datasets"
    config_str = _expiration_config(mode, threshold)

    mode = "size"
    threshold = 55
    with unittest.mock.patch.dict(
        os.environ,
        {"DAF_BUTLER_CACHE_EXPIRATION_MODE": f"{mode}={threshold}"},
    ):
        cache_manager = _make_cache_manager(config_str, universe)
        _assert_expiration(cache_manager, cache, 10, 6)
        assert f"{mode}={threshold}" in str(cache_manager)

    # Check we get a warning with unrecognized form.
    with unittest.mock.patch.dict(
        os.environ,
        {"DAF_BUTLER_CACHE_EXPIRATION_MODE": "something"},
    ):
        with caplog.at_level(logging.WARNING):
            _make_cache_manager(config_str, universe)
        assert "Unrecognized form (something)" in caplog.text

    with unittest.mock.patch.dict(
        os.environ,
        {"DAF_BUTLER_CACHE_EXPIRATION_MODE": "something=5"},
    ):
        with pytest.raises(ValueError, match="Unrecognized value"):
            _make_cache_manager(config_str, universe)


def test_missing_threshold(universe) -> None:
    """Test that an empty expiry threshold is rejected."""
    threshold = ""
    mode = "datasets"
    config_str = _expiration_config(mode, threshold)

    with pytest.raises(ValueError, match="Cache expiration threshold"):
        _make_cache_manager(config_str, universe)


def test_cache_expiry_datasets_composite(cache, universe) -> None:
    """Test that ``datasets`` expiry counts a composite as one dataset."""
    threshold = 2  # Keep 2 datasets.
    mode = "datasets"
    config_str = _expiration_config(mode, threshold)

    cache_manager = _make_cache_manager(config_str, universe)

    n_datasets = 3
    for i in range(n_datasets):
        for component_file, component_ref in zip(cache.comp_files[i], cache.comp_refs[i], strict=True):
            cached = cache_manager.move_to_cache(component_file, component_ref)
            assert cached is not None
            assert cache_manager.known_to_cache(component_ref)
            assert cache_manager.known_to_cache(component_ref.makeCompositeRef())
            assert cache_manager.known_to_cache(component_ref, component_file.getExtension())

    assert cache_manager.file_count == 6  # 2 datasets each of 3 files

    # Write two new non-composite and the number of files should drop.
    _assert_expiration(cache_manager, cache, 2, 5)


def test_cache_expiry_size(cache, universe) -> None:
    """Test that ``size`` expiry retains files up to the byte threshold."""
    threshold = 55  # Each file is 10 bytes
    mode = "size"
    config_str = _expiration_config(mode, threshold)

    cache_manager = _make_cache_manager(config_str, universe)
    _assert_expiration(cache_manager, cache, 10, 6)
    assert f"{mode}={threshold}" in str(cache_manager)


def test_disabled_cache(cache, universe) -> None:
    """Test that the envvar can disable a configured cache."""
    # Configure an active cache but disable via environment.
    threshold = 2
    mode = "datasets"
    config_str = _expiration_config(mode, threshold)

    with unittest.mock.patch.dict(
        os.environ,
        {"DAF_BUTLER_CACHE_EXPIRATION_MODE": "disabled"},
    ):
        env_cache_manager = _make_cache_manager(config_str, universe)

    # Configure to be disabled
    threshold = 0
    mode = "disabled"
    config_str = _expiration_config(mode, threshold)
    cfg_cache_manager = _make_cache_manager(config_str, universe)

    for cache_manager in (
        cfg_cache_manager,
        env_cache_manager,
        DatastoreCacheManager.create_disabled(universe=DimensionUniverse()),
    ):
        for uri, ref in zip(cache.files, cache.refs, strict=True):
            assert not cache_manager.should_be_cached(ref)
            assert cache_manager.move_to_cache(uri, ref) is None
            assert not cache_manager.known_to_cache(ref)
            with cache_manager.find_in_cache(ref, ".txt") as found:
                assert found is None, f"{cache_manager}"
            assert "disabled" in str(cache_manager)


def test_cache_expiry_age(cache, universe) -> None:
    """Test that ``age`` expiry removes files older than the threshold."""
    threshold = 1  # Expire older than 2 seconds
    mode = "age"
    config_str = _expiration_config(mode, threshold)

    cache_manager = _make_cache_manager(config_str, universe)
    assert f"{mode}={threshold}" in str(cache_manager)

    # Insert 3 files, then sleep, then insert more.
    for i in range(2):
        cached = cache_manager.move_to_cache(cache.files[i], cache.refs[i])
        assert cached is not None
    time.sleep(2.0)
    for j in range(4):
        i = 2 + j  # Continue the counting
        cached = cache_manager.move_to_cache(cache.files[i], cache.refs[i])
        assert cached is not None

    # Only the files written after the sleep should exist.
    assert cache_manager.file_count == 4
    with cache_manager.find_in_cache(cache.refs[1], ".txt") as found:
        assert found is None
    with cache_manager.find_in_cache(cache.refs[2], ".txt") as found:
        assert isinstance(found, ResourcePath)


def test_basics() -> None:
    helper = DatasetTestHelper()
    storage_class = StorageClassFactory().getStorageClass("StructuredDataDict")
    ref = helper.makeDatasetRef("metric", DimensionUniverse().empty, storage_class, {})

    null = NullDatastore(None, None)

    assert not null.exists(ref)
    assert not null.knows(ref)
    knows = null.knows_these([ref])
    assert not knows[ref]
    null.validateConfiguration([ref])

    with pytest.raises(FileNotFoundError):
        null.get(ref)
    with pytest.raises(NotImplementedError):
        null.put("", ref)
    with pytest.raises(FileNotFoundError):
        null.getURI(ref)
    with pytest.raises(FileNotFoundError):
        null.getURIs(ref)
    with pytest.raises(FileNotFoundError):
        null.getManyURIs([ref])
    with pytest.raises(NotImplementedError):
        null.getLookupKeys()
    with pytest.raises(NotImplementedError):
        null.import_records({})
    with pytest.raises(NotImplementedError):
        null.export_records([])
    with pytest.raises(NotImplementedError):
        null.export_predicted_records([])
    with pytest.raises(NotImplementedError):
        null.export([ref])
    with pytest.raises(NotImplementedError):
        null.transfer(null, ref)
    with pytest.raises(NotImplementedError):
        null.emptyTrash()
    with pytest.raises(NotImplementedError):
        null.trash(ref)
    with pytest.raises(NotImplementedError):
        null.forget([ref])
    with pytest.raises(NotImplementedError):
        null.remove(ref)
    with pytest.raises(NotImplementedError):
        null.retrieveArtifacts([ref], ResourcePath("."))
    with pytest.raises(NotImplementedError):
        null.transfer_from({}, [ref])
    with pytest.raises(NotImplementedError):
        null.ingest()


def test_sequence_access() -> None:
    """Verify that DatasetRefURIs can be treated like a two-item tuple."""
    uris = DatasetRefURIs()

    assert len(uris) == 2
    assert uris[0] is None
    assert uris[1] == {}

    primary_uri = ResourcePath("1/2/3")
    component_uri = ResourcePath("a/b/c")

    # affirm that DatasetRefURIs does not support MutableSequence functions.
    # The type: ignore comments are the point of the test: the assignments are
    # not allowed, statically or at run time.
    with pytest.raises(TypeError):
        uris[0] = primary_uri  # type: ignore[index]
    with pytest.raises(TypeError):
        uris[1] = {"foo": component_uri}  # type: ignore[index]

    # but DatasetRefURIs can be set by property name:
    uris.primaryURI = primary_uri
    uris.componentURIs = {"foo": component_uri}
    assert uris.primaryURI == primary_uri
    assert uris[0] == primary_uri

    primary, components = uris
    assert primary == primary_uri
    assert components == {"foo": component_uri}


def test_repr() -> None:
    """Verify __repr__ output."""
    uris = DatasetRefURIs(ResourcePath("/1/2/3"), {"comp": ResourcePath("/a/b/c")})
    assert (
        repr(uris)
        == 'DatasetRefURIs(ResourcePath("file:///1/2/3"), {\'comp\': ResourcePath("file:///a/b/c")})'
    )


def test_stored_file_info() -> None:
    helper = DatasetTestHelper()
    storage_class = StorageClassFactory().getStorageClass("StructuredDataDict")
    ref = helper.makeDatasetRef("metric", DimensionUniverse().empty, storage_class, {})

    record = dict(
        storage_class="StructuredDataDict",
        formatter="lsst.daf.butler.Formatter",
        path="a/b/c.txt",
        component="component",
        checksum=None,
        file_size=5,
    )
    info = StoredFileInfo.from_record(record)

    assert info.to_record() == record

    ref2 = helper.makeDatasetRef("metric", DimensionUniverse().empty, storage_class, {})
    rebased = info.rebase(ref2)
    assert rebased.rebase(ref) == info

    with pytest.raises(TypeError):
        rebased.update(formatter=42)

    with pytest.raises(ValueError, match="Unexpected keyword arguments"):
        rebased.update(something=42, new="42")

    # Check that pickle works on StoredFileInfo.
    pickled_info = pickle.dumps(info)
    unpickled_info = pickle.loads(pickled_info)
    assert unpickled_info == info


def test_make_datastore_path_relative() -> None:
    assert make_datastore_path_relative("a/relative/path") == "a/relative/path"
    assert make_datastore_path_relative("path/with#fragment") == "path/with#fragment"
    assert make_datastore_path_relative("http://server.com/some/path") == "some/path"
    assert make_datastore_path_relative("http://server.com/some/path#frag") == "some/path#frag"


def test_datastore_record_data_json_types() -> None:
    """Test that we don't round-trip checksums to UUIDs when deserializing
    datastore record data.
    """
    test_json = """
        {
            "dataset_ids": [
                "74478304-abf1-4a9c-9eb2-926090a84446"
            ],
            "records": {
                "lsst.daf.butler.datastore.stored_file_info.StoredFileInfo": {
                "74478304abf14a9c9eb2926090a84446": {
                    "file_datastore_records": [
                    {
                        "formatter": "lsst.daf.butler.formatters.yaml.YamlFormatter",
                        "path": "gain_factors/base-2025-158/gain_factors_spx_base-2025-158.yaml",
                        "storage_class": "GainFactors",
                        "component": "__NULL_STRING__",
                        "checksum": "cab515f6-ab67-0484-393f-aaa525dd526f",
                        "file_size": 5412
                    }
                    ]
                }
                }
            }
        }
    """
    id_str = "74478304abf14a9c9eb2926090a84446"
    s = SerializedDatastoreRecordData.model_validate_json(test_json)
    assert isinstance(
        s.records[get_full_type_name(StoredFileInfo)][id_str]["file_datastore_records"][0]["checksum"],
        str,
    )
    dataset_id = uuid.UUID(id_str)
    d = DatastoreRecordData.from_simple(s)
    stored = d.records[dataset_id]["file_datastore_records"][0]
    assert isinstance(stored, StoredFileInfo)
    assert isinstance(stored.checksum, str)


def test_empty_datastore_records_table() -> None:
    file_info_table = StoredFileInfoTable.from_records([])
    assert len(file_info_table) == 0

    assert len(DatastoreRecordTable.from_stored_file_info_table("datastore_name", file_info_table)) == 0

    assert len(DatastoreRecordTable.create_empty()) == 0
    assert len(DatastoreRecordTable.combine([])) == 0
    # Doesn't throw because there is no mismatch in datastore names.
    DatastoreRecordTable.create_empty().validate_datastore_names("arbitrary_name")


def test_stored_file_info_table_records() -> None:
    uuid1 = uuid.UUID("019e1892-7b9b-736d-8248-0e031723646c")
    uuid2 = uuid.UUID("019e1895-9ec3-7431-bed9-8ae60096103f")
    uuid3 = uuid.UUID("13d13272-454c-4bc4-94d5-3e322982eee8")
    checksum = (
        "021ced8799518305c451cde3e921515ef315ee7ba8937"
        "a92697a20c571c776afa4102744bc28d2d99d35f44e073cde80cf96e387f65f3967cca45b0d015f5a6b"
    )
    input_records = [
        {
            "dataset_id": uuid1,
            "path": "a/relative/path.fits",
            "formatter": "lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter",
            "storage_class": "ExposureF",
            "component": "__NULL_STRING__",
            "checksum": None,
            "file_size": 123,
        },
        {
            "dataset_id": uuid2,
            "path": "file:///an/absolute/path.fits",
            "formatter": "lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter",
            "storage_class": "ExposureF",
            "component": "comp",
            "checksum": checksum,
            "file_size": -1,
        },
    ]
    table = StoredFileInfoTable.from_records(input_records)
    assert len(table) == 2

    def _check_records(records: list[dict]) -> None:
        rec0 = records[0]
        assert rec0["dataset_id"] == uuid1
        assert rec0["path"] == "a/relative/path.fits"
        assert rec0["formatter"] == "lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter"
        assert rec0["storage_class"] == "ExposureF"
        assert rec0["component"] is None
        assert rec0["checksum"] is None
        assert rec0["file_size"] == 123
        rec1 = records[1]
        assert rec1["dataset_id"] == uuid2
        assert rec1["path"] == "file:///an/absolute/path.fits"
        assert rec1["formatter"] == "lsst.obs.base.formatters.fitsExposure.FitsExposureFormatter"
        assert rec1["storage_class"] == "ExposureF"
        assert rec1["component"] == "comp"
        assert rec1["checksum"] == checksum
        assert rec1["file_size"] is None

    arrow_records = table.to_arrow().to_pylist()
    assert len(arrow_records) == 2
    _check_records(arrow_records)
    assert table.to_records() == input_records

    datastore_table = DatastoreRecordTable.from_stored_file_info_table("name_of_datastore", table)
    datastore_arrow_records = datastore_table.to_arrow().to_pylist()
    assert len(datastore_arrow_records) == 2
    _check_records(datastore_arrow_records)
    assert datastore_arrow_records[0]["datastore_name"] == "name_of_datastore"
    assert datastore_arrow_records[1]["datastore_name"] == "name_of_datastore"
    _check_records(datastore_table.to_stored_file_info_table().to_arrow().to_pylist())
    # Check round-tripping to_arrow() through from_arrow()
    _check_records(
        datastore_table.from_arrow(datastore_table.to_arrow())
        .to_stored_file_info_table()
        .to_arrow()
        .to_pylist()
    )

    with pytest.raises(ValueError, match="do not match known datastores"):
        datastore_table.validate_datastore_names(["not_the_same_datastore"])
    datastore_table.validate_datastore_names(["not_the_same_datastore", "name_of_datastore"])

    second_table = DatastoreRecordTable.from_stored_file_info_table(
        "other_datastore_name",
        StoredFileInfoTable.from_records(
            [
                {
                    "dataset_id": uuid3,
                    "path": "a/relative/path2.fits",
                    "formatter": "lsst.obs.lsst.rawFormatter.LsstCamRawFormatter",
                    "storage_class": "Exposure",
                    "component": "__NULL_STRING__",
                    "checksum": None,
                    "file_size": 1000,
                },
            ]
        ),
    )
    combined_table = DatastoreRecordTable.combine([datastore_table, second_table])
    combined_records = combined_table.to_arrow().to_pylist()
    _check_records(combined_records)
    rec2 = combined_records[2]
    assert rec2["dataset_id"] == uuid3
    assert rec2["path"] == "a/relative/path2.fits"
    assert rec2["formatter"] == "lsst.obs.lsst.rawFormatter.LsstCamRawFormatter"
    assert rec2["storage_class"] == "Exposure"
    assert rec2["component"] is None
    assert rec2["checksum"] is None
    assert rec2["file_size"] == 1000

    assert len(datastore_table.filter_by_datastore_name("unknown_datastore")) == 0
    filtered_table = combined_table.filter_by_datastore_name("name_of_datastore")
    assert len(filtered_table) == 2
    _check_records(filtered_table.to_arrow().to_pylist())
    assert filtered_table.to_stored_file_info_table().to_records() == input_records
