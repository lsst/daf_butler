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
import shutil
import tempfile
import unittest.mock
import uuid
from collections.abc import Callable, Iterator
from typing import Any, cast

import pytest
import yaml
from butler_test_support import make_datastore_metrics, records_from

import lsst.daf.butler.datastores.fileDatastore
from lsst.daf.butler import (
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
from lsst.daf.butler.datastore import DatastoreConfig, DatastoreValidationError
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
from lsst.utils import doImport

TESTDIR = os.path.abspath(os.path.dirname(__file__))

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

``trash`` and ``posix-no-checksums`` are only used by their own tests; see
`ALL_PROFILES`.
"""

ALL_PROFILES = ["posix", "in-memory", "chained", "chained-memory"]
"""Profiles that run the shared datastore tests.

``trash`` and ``posix-no-checksums`` are absent. They were subclasses of the
posix case and so reran every shared test; DM-55822 measured those 50 reruns
each as zero unique lines and zero unique arcs. Both profiles survive, but only
for the tests that gave them their names, which do have unique coverage.
"""

FILE_PROFILES = ["posix", "chained"]
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
    metrics = make_datastore_metrics()
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
    metricsNone = make_datastore_metrics(use_none=True)
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

    metrics = make_datastore_metrics()

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
    metrics = make_datastore_metrics()
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
    metrics = make_datastore_metrics()
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
    metrics = make_datastore_metrics()

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
            make_datastore_metrics(),
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
    metrics = make_datastore_metrics()

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
    metrics = make_datastore_metrics()
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

    metrics = make_datastore_metrics()
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
    metrics = make_datastore_metrics()

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
    _ = make_datastore_metrics()
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
    metrics = make_datastore_metrics()

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
    metrics = make_datastore_metrics()
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
