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


"""Tests for the datastore cache manager."""

from __future__ import annotations

import dataclasses
import logging
import os
import pathlib
import time
import unittest.mock

import pytest

from lsst.daf.butler import Config, DatasetRef, DimensionUniverse, StorageClassFactory
from lsst.daf.butler.datastore.cache_manager import (
    DatastoreCacheManager,
    DatastoreCacheManagerConfig,
    DatastoreDisabledCacheManager,
)
from lsst.daf.butler.tests import DatasetTestHelper
from lsst.resources import ResourcePath

TESTDIR = os.path.dirname(__file__)


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
