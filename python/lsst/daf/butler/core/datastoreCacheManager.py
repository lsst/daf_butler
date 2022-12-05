# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

from __future__ import annotations

"""Cache management for a datastore."""

__all__ = (
    "AbstractDatastoreCacheManager",
    "DatastoreDisabledCacheManager",
    "DatastoreCacheManager",
    "DatastoreCacheManagerConfig",
)

import atexit
import contextlib
import datetime
import itertools
import logging
import os
import shutil
import tempfile
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from random import Random
from typing import (
    TYPE_CHECKING,
    Dict,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    List,
    Optional,
    Union,
    ValuesView,
)

from lsst.resources import ResourcePath
from pydantic import BaseModel, PrivateAttr

from .config import ConfigSubset
from .configSupport import processLookupConfigs
from .datasets import DatasetId, DatasetRef

if TYPE_CHECKING:
    from .configSupport import LookupKey
    from .datasets import DatasetType
    from .dimensions import DimensionUniverse
    from .storageClass import StorageClass

log = logging.getLogger(__name__)


def remove_cache_directory(directory: str) -> None:
    """Remove the specified directory and all its contents."""
    log.debug("Removing temporary cache directory %s", directory)
    shutil.rmtree(directory, ignore_errors=True)


def _construct_cache_path(root: ResourcePath, ref: DatasetRef, extension: str) -> ResourcePath:
    """Construct the full path to use for this dataset in the cache.

    Parameters
    ----------
    ref : `DatasetRef`
        The dataset to look up in or write to the cache.
    extension : `str`
        File extension to use for this file. Should include the
        leading "``.``".

    Returns
    -------
    uri : `lsst.resources.ResourcePath`
        URI to use for this dataset in the cache.
    """
    # Dataset type component is needed in the name if composite
    # disassembly is happening since the ID is shared for all components.
    component = ref.datasetType.component()
    component = f"_{component}" if component else ""
    return root.join(f"{ref.id}{component}{extension}")


def _parse_cache_name(cached_location: str) -> Dict[str, Optional[str]]:
    """For a given cache name, return its component parts.

    Changes to ``_construct_cache_path()`` should be reflected here.

    Parameters
    ----------
    cached_location : `str`
        The name of the file within the cache.

    Returns
    -------
    parsed : `dict` of `str`, `str`
        Parsed components of the file. These include:
        - "id": The dataset ID,
        - "component": The name of the component (can be `None`),
        - "extension": File extension (can be `None`).
    """
    # Assume first dot is the extension and so allow .fits.gz
    root_ext = cached_location.split(".", maxsplit=1)
    root = root_ext.pop(0)
    ext = "." + root_ext.pop(0) if root_ext else None

    parts = root.split("_")
    id_ = parts.pop(0)
    component = parts.pop(0) if parts else None
    return {"id": id_, "component": component, "extension": ext}


class CacheEntry(BaseModel):
    """Represent an entry in the cache."""

    name: str
    """Name of the file."""

    size: int
    """Size of the file in bytes."""

    ctime: datetime.datetime
    """Creation time of the file."""

    ref: DatasetId
    """ID of this dataset."""

    component: Optional[str]
    """Component for this disassembled composite (optional)."""

    @classmethod
    def from_file(cls, file: ResourcePath, root: ResourcePath) -> CacheEntry:
        """Construct an object from a file name.

        Parameters
        ----------
        file : `lsst.resources.ResourcePath`
            Path to the file.
        root : `lsst.resources.ResourcePath`
            Cache root directory.
        """
        file_in_cache = file.relative_to(root)
        if file_in_cache is None:
            raise ValueError(f"Supplied file {file} is not inside root {root}")
        parts = _parse_cache_name(file_in_cache)

        stat = os.stat(file.ospath)
        return cls(
            name=file_in_cache,
            size=stat.st_size,
            ref=parts["id"],
            component=parts["component"],
            ctime=datetime.datetime.utcfromtimestamp(stat.st_ctime),
        )


class _MarkerEntry(CacheEntry):
    pass


class CacheRegistry(BaseModel):
    """Collection of cache entries."""

    _size: int = PrivateAttr(0)
    """Size of the cache."""

    _entries: Dict[str, CacheEntry] = PrivateAttr({})
    """Internal collection of cache entries."""

    _ref_map: Dict[DatasetId, List[str]] = PrivateAttr({})
    """Mapping of DatasetID to corresponding keys in cache registry."""

    @property
    def cache_size(self) -> int:
        return self._size

    def __getitem__(self, key: str) -> CacheEntry:
        return self._entries[key]

    def __setitem__(self, key: str, entry: CacheEntry) -> None:
        self._size += entry.size
        self._entries[key] = entry

        # Update the mapping from ref to path.
        if entry.ref not in self._ref_map:
            self._ref_map[entry.ref] = []
        self._ref_map[entry.ref].append(key)

    def __delitem__(self, key: str) -> None:
        entry = self._entries.pop(key)
        self._decrement(entry)
        self._ref_map[entry.ref].remove(key)

    def _decrement(self, entry: Optional[CacheEntry]) -> None:
        if entry:
            self._size -= entry.size
            if self._size < 0:
                log.warning("Cache size has gone negative. Inconsistent cache records...")
                self._size = 0

    def __contains__(self, key: str) -> bool:
        return key in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[str]:  # type: ignore
        return iter(self._entries)

    def keys(self) -> KeysView[str]:
        return self._entries.keys()

    def values(self) -> ValuesView[CacheEntry]:
        return self._entries.values()

    def items(self) -> ItemsView[str, CacheEntry]:
        return self._entries.items()

    # An private marker to indicate that pop() should raise if no default
    # is given.
    __marker = _MarkerEntry(name="marker", size=0, ref=0, ctime=datetime.datetime.utcfromtimestamp(0))

    def pop(self, key: str, default: Optional[CacheEntry] = __marker) -> Optional[CacheEntry]:
        # The marker for dict.pop is not the same as our marker.
        if default is self.__marker:
            entry = self._entries.pop(key)
        else:
            entry = self._entries.pop(key, self.__marker)
            # Should not attempt to correct for this entry being removed
            # if we got the default value.
            if entry is self.__marker:
                return default

        self._decrement(entry)
        # The default entry given to this method may not even be in the cache.
        if entry and entry.ref in self._ref_map:
            keys = self._ref_map[entry.ref]
            if key in keys:
                keys.remove(key)
        return entry

    def get_dataset_keys(self, dataset_id: Optional[DatasetId]) -> Optional[List[str]]:
        """Retrieve all keys associated with the given dataset ID.

        Parameters
        ----------
        dataset_id : `DatasetId` or `None`
            The dataset ID to look up. Returns `None` if the ID is `None`.

        Returns
        -------
        keys : `list` [`str`]
            Keys associated with this dataset. These keys can be used to lookup
            the cache entry information in the `CacheRegistry`. Returns
            `None` if the dataset is not known to the cache.
        """
        if dataset_id not in self._ref_map:
            return None
        keys = self._ref_map[dataset_id]
        if not keys:
            return None
        return keys


class DatastoreCacheManagerConfig(ConfigSubset):
    """Configuration information for `DatastoreCacheManager`."""

    component = "cached"
    requiredKeys = ("cacheable",)


class AbstractDatastoreCacheManager(ABC):
    """An abstract base class for managing caching in a Datastore.

    Parameters
    ----------
    config : `str` or `DatastoreCacheManagerConfig`
        Configuration to control caching.
    universe : `DimensionUniverse`
        Set of all known dimensions, used to expand and validate any used
        in lookup keys.
    """

    @property
    def cache_size(self) -> int:
        """Size of the cache in bytes."""
        return 0

    @property
    def file_count(self) -> int:
        """Return number of cached files tracked by registry."""
        return 0

    def __init__(self, config: Union[str, DatastoreCacheManagerConfig], universe: DimensionUniverse):
        if not isinstance(config, DatastoreCacheManagerConfig):
            config = DatastoreCacheManagerConfig(config)
        assert isinstance(config, DatastoreCacheManagerConfig)
        self.config = config

    @abstractmethod
    def should_be_cached(self, entity: Union[DatasetRef, DatasetType, StorageClass]) -> bool:
        """Indicate whether the entity should be added to the cache.

        This is relevant when reading or writing.

        Parameters
        ----------
        entity : `StorageClass` or `DatasetType` or `DatasetRef`
            Thing to test against the configuration. The ``name`` property
            is used to determine a match.  A `DatasetType` will first check
            its name, before checking its `StorageClass`.  If there are no
            matches the default will be returned.

        Returns
        -------
        should_cache : `bool`
            Returns `True` if the dataset should be cached; `False` otherwise.
        """
        raise NotImplementedError()

    @abstractmethod
    def known_to_cache(self, ref: DatasetRef, extension: Optional[str] = None) -> bool:
        """Report if the dataset is known to the cache.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to check for in the cache.
        extension : `str`, optional
            File extension expected. Should include the leading "``.``".
            If `None` the extension is ignored and the dataset ID alone is
            used to check in the cache. The extension must be defined if
            a specific component is being checked.

        Returns
        -------
        known : `bool`
            Returns `True` if the dataset is currently known to the cache
            and `False` otherwise.

        Notes
        -----
        This method can only report if the dataset is known to the cache
        in this specific instant and does not indicate whether the file
        can be read from the cache later. `find_in_cache()` should be called
        if the cached file is to be used.
        """
        raise NotImplementedError()

    @abstractmethod
    def move_to_cache(self, uri: ResourcePath, ref: DatasetRef) -> Optional[ResourcePath]:
        """Move a file to the cache.

        Move the given file into the cache, using the supplied DatasetRef
        for naming. A call is made to `should_be_cached()` and if the
        DatasetRef should not be accepted `None` will be returned.

        Cache expiry can occur during this.

        Parameters
        ----------
        uri : `lsst.resources.ResourcePath`
            Location of the file to be relocated to the cache. Will be moved.
        ref : `DatasetRef`
            Ref associated with this file. Will be used to determine the name
            of the file within the cache.

        Returns
        -------
        new : `lsst.resources.ResourcePath` or `None`
            URI to the file within the cache, or `None` if the dataset
            was not accepted by the cache.
        """
        raise NotImplementedError()

    @abstractmethod
    @contextlib.contextmanager
    def find_in_cache(self, ref: DatasetRef, extension: str) -> Iterator[Optional[ResourcePath]]:
        """Look for a dataset in the cache and return its location.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to locate in the cache.
        extension : `str`
            File extension expected. Should include the leading "``.``".

        Yields
        ------
        uri : `lsst.resources.ResourcePath` or `None`
            The URI to the cached file, or `None` if the file has not been
            cached.

        Notes
        -----
        Should be used as a context manager in order to prevent this
        file from being removed from the cache for that context.
        """
        raise NotImplementedError()

    @abstractmethod
    def remove_from_cache(self, ref: Union[DatasetRef, Iterable[DatasetRef]]) -> None:
        """Remove the specified datasets from the cache.

        It is not an error for these datasets to be missing from the cache.

        Parameters
        ----------
        ref : `DatasetRef` or iterable of `DatasetRef`
            The datasets to remove from the cache.
        """
        raise NotImplementedError()

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()


class DatastoreCacheManager(AbstractDatastoreCacheManager):
    """A class for managing caching in a Datastore using local files.

    Parameters
    ----------
    config : `str` or `DatastoreCacheManagerConfig`
        Configuration to control caching.
    universe : `DimensionUniverse`
        Set of all known dimensions, used to expand and validate any used
        in lookup keys.

    Notes
    -----
    Two environment variables can be used to override the cache directory
    and expiration configuration:

    * ``$DAF_BUTLER_CACHE_DIRECTORY``
    * ``$DAF_BUTLER_CACHE_EXPIRATION_MODE``

    The expiration mode should take the form ``mode=threshold`` so for
    example to configure expiration to limit the cache directory to 5 datasets
    the value would be ``datasets=5``.

    Additionally the ``$DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET`` environment
    variable can be used to indicate that this directory should be used
    if no explicit directory has been specified from configuration or from
    the ``$DAF_BUTLER_CACHE_DIRECTORY`` environment variable.
    """

    _temp_exemption_prefix = "exempt/"
    _tmpdir_prefix = "butler-cache-dir-"

    def __init__(self, config: Union[str, DatastoreCacheManagerConfig], universe: DimensionUniverse):
        super().__init__(config, universe)

        # Set cache directory if it pre-exists, else defer creation until
        # requested. Allow external override from environment.
        root = os.environ.get("DAF_BUTLER_CACHE_DIRECTORY") or self.config.get("root")

        # Allow the execution environment to override the default values
        # so long as no default value has been set from the line above.
        if root is None:
            root = os.environ.get("DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET")

        self._cache_directory = (
            ResourcePath(root, forceAbsolute=True, forceDirectory=True) if root is not None else None
        )

        if self._cache_directory:
            if not self._cache_directory.isLocal:
                raise ValueError(
                    f"Cache directory must be on a local file system. Got: {self._cache_directory}"
                )
            # Ensure that the cache directory is created. We assume that
            # someone specifying a permanent cache directory will be expecting
            # it to always be there. This will also trigger an error
            # early rather than waiting until the cache is needed.
            self._cache_directory.mkdir()

        # Calculate the caching lookup table.
        self._lut = processLookupConfigs(self.config["cacheable"], universe=universe)

        # Default decision to for whether a dataset should be cached.
        self._caching_default = self.config.get("default", False)

        # Expiration mode. Read from config but allow override from
        # the environment.
        expiration_mode = self.config.get(("expiry", "mode"))
        threshold = self.config.get(("expiry", "threshold"))

        external_mode = os.environ.get("DAF_BUTLER_CACHE_EXPIRATION_MODE")
        if external_mode and "=" in external_mode:
            expiration_mode, expiration_threshold = external_mode.split("=", 1)
            threshold = int(expiration_threshold)
        if expiration_mode is None:
            # Force to None to avoid confusion.
            threshold = None

        self._expiration_mode: Optional[str] = expiration_mode
        self._expiration_threshold: Optional[int] = threshold
        if self._expiration_threshold is None and self._expiration_mode is not None:
            raise ValueError(
                f"Cache expiration threshold must be set for expiration mode {self._expiration_mode}"
            )

        log.debug(
            "Cache configuration:\n- root: %s\n- expiration mode: %s",
            self._cache_directory if self._cache_directory else "tmpdir",
            f"{self._expiration_mode}={self._expiration_threshold}" if self._expiration_mode else "disabled",
        )

        # Files in cache, indexed by path within the cache directory.
        self._cache_entries = CacheRegistry()

    @property
    def cache_directory(self) -> ResourcePath:
        if self._cache_directory is None:
            # Create on demand. Allow the override environment variable
            # to be used in case it got set after this object was created
            # but before a cache was used.
            if cache_dir := os.environ.get("DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET"):
                # Someone else will clean this up.
                isTemporary = False
                msg = "deferred fallback"
            else:
                cache_dir = tempfile.mkdtemp(prefix=self._tmpdir_prefix)
                isTemporary = True
                msg = "temporary"

            self._cache_directory = ResourcePath(cache_dir, forceDirectory=True, isTemporary=isTemporary)
            log.debug("Using %s cache directory at %s", msg, self._cache_directory)

            # Remove when we no longer need it.
            if isTemporary:
                atexit.register(remove_cache_directory, self._cache_directory.ospath)
        return self._cache_directory

    @property
    def _temp_exempt_directory(self) -> ResourcePath:
        """Return the directory in which to store temporary cache files that
        should not be expired.
        """
        return self.cache_directory.join(self._temp_exemption_prefix)

    @property
    def cache_size(self) -> int:
        return self._cache_entries.cache_size

    @property
    def file_count(self) -> int:
        return len(self._cache_entries)

    @classmethod
    def set_fallback_cache_directory_if_unset(cls) -> tuple[bool, str]:
        """Defines a fallback cache directory if a fallback not set already.

        Returns
        -------
        defined : `bool`
            `True` if the fallback directory was newly-defined in this method.
            `False` if it had already been set.
        cache_dir : `str`
            Returns the path to the cache directory that will be used if it's
            needed. This can allow the caller to run a directory cleanup
            when it's no longer needed (something that the cache manager
            can not do because forks should not clean up directories defined
            by the parent process).

        Notes
        -----
        The fallback directory will not be defined if one has already been
        defined. This method sets the ``DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET``
        environment variable only if a value has not previously been stored
        in that environment variable. Setting the environment variable allows
        this value to survive into spawned subprocesses. Calling this method
        will lead to all subsequently created cache managers sharing the same
        cache.
        """
        if cache_dir := os.environ.get("DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET"):
            # A value has already been set.
            return (False, cache_dir)

        # As a class method, we do not know at this point whether a cache
        # directory will be needed so it would be impolite to create a
        # directory that will never be used.

        # Construct our own temp name -- 16 characters should have a fairly
        # low chance of clashing when combined with the process ID.
        characters = "abcdefghijklmnopqrstuvwxyz0123456789_"
        rng = Random()
        tempchars = "".join(rng.choice(characters) for _ in range(16))

        tempname = f"{cls._tmpdir_prefix}{os.getpid()}-{tempchars}"

        cache_dir = os.path.join(tempfile.gettempdir(), tempname)
        os.environ["DAF_BUTLER_CACHE_DIRECTORY_IF_UNSET"] = cache_dir
        return (True, cache_dir)

    def should_be_cached(self, entity: Union[DatasetRef, DatasetType, StorageClass]) -> bool:
        # Docstring inherited
        matchName: Union[LookupKey, str] = "{} (via default)".format(entity)
        should_cache = self._caching_default

        for key in entity._lookupNames():
            if key in self._lut:
                should_cache = bool(self._lut[key])
                matchName = key
                break

        if not isinstance(should_cache, bool):
            raise TypeError(
                f"Got cache value {should_cache!r} for config entry {matchName!r}; expected bool."
            )

        log.debug("%s (match: %s) should%s be cached", entity, matchName, "" if should_cache else " not")
        return should_cache

    def _construct_cache_name(self, ref: DatasetRef, extension: str) -> ResourcePath:
        """Construct the name to use for this dataset in the cache.

        Parameters
        ----------
        ref : `DatasetRef`
            The dataset to look up in or write to the cache.
        extension : `str`
            File extension to use for this file. Should include the
            leading "``.``".

        Returns
        -------
        uri : `lsst.resources.ResourcePath`
            URI to use for this dataset in the cache.
        """
        return _construct_cache_path(self.cache_directory, ref, extension)

    def move_to_cache(self, uri: ResourcePath, ref: DatasetRef) -> Optional[ResourcePath]:
        # Docstring inherited
        if ref.id is None:
            raise ValueError(f"Can not cache a file associated with an unresolved reference ({ref})")

        if not self.should_be_cached(ref):
            return None

        # Write the file using the id of the dataset ref and the file
        # extension.
        cached_location = self._construct_cache_name(ref, uri.getExtension())

        # Run cache expiry to ensure that we have room for this
        # item.
        self._expire_cache()

        # The above reset the in-memory cache status. It's entirely possible
        # that another process has just cached this file (if multiple
        # processes are caching on read), so check our in-memory cache
        # before attempting to cache the dataset.
        path_in_cache = cached_location.relative_to(self.cache_directory)
        if path_in_cache and path_in_cache in self._cache_entries:
            return cached_location

        # Move into the cache. Given that multiple processes might be
        # sharing a single cache directory, and the file we need might have
        # been copied in whilst we were checking, allow overwrite without
        # complaint. Even for a private cache directory it is possible that
        # a second butler in a subprocess could be writing to it.
        cached_location.transfer_from(uri, transfer="move", overwrite=True)
        log.debug("Cached dataset %s to %s", ref, cached_location)

        self._register_cache_entry(cached_location)

        return cached_location

    @contextlib.contextmanager
    def find_in_cache(self, ref: DatasetRef, extension: str) -> Iterator[Optional[ResourcePath]]:
        # Docstring inherited
        # Short circuit this if the cache directory has not been created yet.
        if self._cache_directory is None:
            yield None
            return

        cached_location = self._construct_cache_name(ref, extension)
        if cached_location.exists():
            log.debug("Found cached file %s for dataset %s.", cached_location, ref)

            # The cached file could be removed by another process doing
            # cache expiration so we need to protect against that by making
            # a copy in a different tree. Use hardlinks to ensure that
            # we either have the cached file or we don't. This is robust
            # against race conditions that can be caused by using soft links
            # and the other end of the link being deleted just after it
            # is created.
            path_in_cache = cached_location.relative_to(self.cache_directory)
            assert path_in_cache is not None, f"Somehow {cached_location} not in cache directory"

            # Need to use a unique file name for the temporary location to
            # ensure that two different processes can read the file
            # simultaneously without one of them deleting it when it's in
            # use elsewhere. Retain the original filename for easier debugging.
            random = str(uuid.uuid4())[:8]
            basename = cached_location.basename()
            filename = f"{random}-{basename}"

            temp_location: Optional[ResourcePath] = self._temp_exempt_directory.join(filename)
            try:
                if temp_location is not None:
                    temp_location.transfer_from(cached_location, transfer="hardlink")
            except Exception as e:
                log.debug("Detected error creating hardlink for dataset %s: %s", ref, e)
                # Any failure will be treated as if the file was not
                # in the cache. Yielding the original cache location
                # is too dangerous.
                temp_location = None

            try:
                log.debug("Yielding temporary cache location %s for dataset %s", temp_location, ref)
                yield temp_location
            finally:
                try:
                    if temp_location:
                        temp_location.remove()
                except FileNotFoundError:
                    pass
            return

        log.debug("Dataset %s not found in cache.", ref)
        yield None
        return

    def remove_from_cache(self, refs: Union[DatasetRef, Iterable[DatasetRef]]) -> None:
        # Docstring inherited.

        # Stop early if there are no cache entries anyhow.
        if len(self._cache_entries) == 0:
            return

        if isinstance(refs, DatasetRef):
            refs = [refs]

        # Create a set of all the IDs
        all_ids = {ref.getCheckedId() for ref in refs}

        keys_to_remove = []
        for key, entry in self._cache_entries.items():
            if entry.ref in all_ids:
                keys_to_remove.append(key)
        self._remove_from_cache(keys_to_remove)

    def _register_cache_entry(self, cached_location: ResourcePath, can_exist: bool = False) -> Optional[str]:
        """Record the file in the cache registry.

        Parameters
        ----------
        cached_location : `lsst.resources.ResourcePath`
            Location of the file to be registered.
        can_exist : `bool`, optional
            If `True` the item being registered can already be listed.
            This can allow a cache refresh to run without checking the
            file again. If `False` it is an error for the registry to
            already know about this file.

        Returns
        -------
        cache_key : `str` or `None`
            The key used in the registry for this file. `None` if the file
            no longer exists (it could have been expired by another process).
        """
        path_in_cache = cached_location.relative_to(self.cache_directory)
        if path_in_cache is None:
            raise ValueError(
                f"Can not register cached file {cached_location} that is not within"
                f" the cache directory at {self.cache_directory}."
            )
        if path_in_cache in self._cache_entries:
            if can_exist:
                return path_in_cache
            else:
                raise ValueError(
                    f"Cached file {cached_location} is already known to the registry"
                    " but this was expected to be a new file."
                )
        try:
            details = CacheEntry.from_file(cached_location, root=self.cache_directory)
        except FileNotFoundError:
            return None
        self._cache_entries[path_in_cache] = details
        return path_in_cache

    def scan_cache(self) -> None:
        """Scan the cache directory and record information about files."""
        found = set()
        for file in ResourcePath.findFileResources([self.cache_directory]):
            assert isinstance(file, ResourcePath), "Unexpectedly did not get ResourcePath from iterator"

            # Skip any that are found in an exempt part of the hierarchy
            # since they should not be part of the registry.
            if file.relative_to(self._temp_exempt_directory) is not None:
                continue

            path_in_cache = self._register_cache_entry(file, can_exist=True)
            if path_in_cache:
                found.add(path_in_cache)

        # Find any files that were recorded in the cache but are no longer
        # on disk. (something else cleared them out?)
        known_to_cache = set(self._cache_entries)
        missing = known_to_cache - found

        if missing:
            log.debug(
                "Entries no longer on disk but thought to be in cache and so removed: %s", ",".join(missing)
            )
            for path_in_cache in missing:
                self._cache_entries.pop(path_in_cache, None)

    def known_to_cache(self, ref: DatasetRef, extension: Optional[str] = None) -> bool:
        """Report if the dataset is known to the cache.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to check for in the cache.
        extension : `str`, optional
            File extension expected. Should include the leading "``.``".
            If `None` the extension is ignored and the dataset ID alone is
            used to check in the cache. The extension must be defined if
            a specific component is being checked.

        Returns
        -------
        known : `bool`
            Returns `True` if the dataset is currently known to the cache
            and `False` otherwise. If the dataset refers to a component and
            an extension is given then only that component is checked.

        Notes
        -----
        This method can only report if the dataset is known to the cache
        in this specific instant and does not indicate whether the file
        can be read from the cache later. `find_in_cache()` should be called
        if the cached file is to be used.

        This method does not force the cache to be re-scanned and so can miss
        cached datasets that have recently been written by other processes.
        """
        if self._cache_directory is None:
            return False
        if self.file_count == 0:
            return False

        if extension is None:
            # Look solely for matching dataset ref ID and not specific
            # components.
            cached_paths = self._cache_entries.get_dataset_keys(ref.id)
            return True if cached_paths else False

        else:
            # Extension is known so we can do an explicit look up for the
            # cache entry.
            cached_location = self._construct_cache_name(ref, extension)
            path_in_cache = cached_location.relative_to(self.cache_directory)
            assert path_in_cache is not None  # For mypy
            return path_in_cache in self._cache_entries

    def _remove_from_cache(self, cache_entries: Iterable[str]) -> None:
        """Remove the specified cache entries from cache.

        Parameters
        ----------
        cache_entries : iterable of `str`
            The entries to remove from the cache. The values are the path
            within the cache.
        """
        for entry in cache_entries:
            path = self.cache_directory.join(entry)

            self._cache_entries.pop(entry, None)
            log.debug("Removing file from cache: %s", path)
            try:
                path.remove()
            except FileNotFoundError:
                pass

    def _expire_cache(self) -> None:
        """Expire the files in the cache.

        Notes
        -----
        The expiration modes are defined by the config or can be overridden.
        Available options:

        * ``files``: Number of files.
        * ``datasets``: Number of datasets
        * ``size``: Total size of files.
        * ``age``: Age of files.

        The first three would remove in reverse time order.
        Number of files is complicated by the possibility of disassembled
        composites where 10 small files can be created for each dataset.

        Additionally there is a use case for an external user to explicitly
        state the dataset refs that should be cached and then when to
        remove them. Overriding any global configuration.
        """
        if self._expiration_mode is None:
            # Expiration has been disabled.
            return

        # mypy can't be sure we have set a threshold properly
        if self._expiration_threshold is None:
            log.warning(
                "Requesting cache expiry of mode %s but no threshold set in config.", self._expiration_mode
            )
            return

        # Sync up cache. There is no file locking involved so for a shared
        # cache multiple processes may be racing to delete files. Deleting
        # a file that no longer exists is not an error.
        self.scan_cache()

        if self._expiration_mode == "files":
            n_files = len(self._cache_entries)
            n_over = n_files - self._expiration_threshold
            if n_over > 0:
                sorted_keys = self._sort_cache()
                keys_to_remove = sorted_keys[:n_over]
                self._remove_from_cache(keys_to_remove)
            return

        if self._expiration_mode == "datasets":
            # Count the datasets, in ascending timestamp order,
            # so that oldest turn up first.
            datasets = defaultdict(list)
            for key in self._sort_cache():
                entry = self._cache_entries[key]
                datasets[entry.ref].append(key)

            n_datasets = len(datasets)
            n_over = n_datasets - self._expiration_threshold
            if n_over > 0:
                # Keys will be read out in insert order which
                # will be date order so oldest ones are removed.
                ref_ids = list(datasets.keys())[:n_over]
                keys_to_remove = list(itertools.chain.from_iterable(datasets[ref_id] for ref_id in ref_ids))
                self._remove_from_cache(keys_to_remove)
            return

        if self._expiration_mode == "size":
            if self.cache_size > self._expiration_threshold:
                for key in self._sort_cache():
                    self._remove_from_cache([key])
                    if self.cache_size <= self._expiration_threshold:
                        break
            return

        if self._expiration_mode == "age":
            now = datetime.datetime.utcnow()
            for key in self._sort_cache():
                delta = now - self._cache_entries[key].ctime
                if delta.seconds > self._expiration_threshold:
                    self._remove_from_cache([key])
                else:
                    # We're already in date order.
                    break
            return

        raise ValueError(f"Unrecognized cache expiration mode of {self._expiration_mode}")

    def _sort_cache(self) -> List[str]:
        """Sort the cache entries by time and return the sorted keys.

        Returns
        -------
        sorted : `list` of `str`
            Keys into the cache, sorted by time with oldest first.
        """

        def sort_by_time(key: str) -> datetime.datetime:
            """Sorter key function using cache entry details."""
            return self._cache_entries[key].ctime

        return sorted(self._cache_entries, key=sort_by_time)

    def __str__(self) -> str:
        cachedir = self._cache_directory if self._cache_directory else "<tempdir>"
        return (
            f"{type(self).__name__}@{cachedir} ({self._expiration_mode}={self._expiration_threshold},"
            f"default={self._caching_default}) "
            f"n_files={self.file_count}, n_bytes={self.cache_size}"
        )


class DatastoreDisabledCacheManager(AbstractDatastoreCacheManager):
    """A variant of the datastore cache where no cache is enabled.

    Parameters
    ----------
    config : `str` or `DatastoreCacheManagerConfig`
        Configuration to control caching.
    universe : `DimensionUniverse`
        Set of all known dimensions, used to expand and validate any used
        in lookup keys.
    """

    def __init__(self, config: Union[str, DatastoreCacheManagerConfig], universe: DimensionUniverse):
        return

    def should_be_cached(self, entity: Union[DatasetRef, DatasetType, StorageClass]) -> bool:
        """Indicate whether the entity should be added to the cache.

        Always returns `False`.
        """
        return False

    def move_to_cache(self, uri: ResourcePath, ref: DatasetRef) -> Optional[ResourcePath]:
        """Move dataset to cache but always refuse and returns `None`."""
        return None

    @contextlib.contextmanager
    def find_in_cache(self, ref: DatasetRef, extension: str) -> Iterator[Optional[ResourcePath]]:
        """Look for a dataset in the cache and return its location.

        Never finds a file.
        """
        yield None

    def remove_from_cache(self, ref: Union[DatasetRef, Iterable[DatasetRef]]) -> None:
        """Remove datasets from cache.

        Always does nothing.
        """
        return

    def known_to_cache(self, ref: DatasetRef, extension: Optional[str] = None) -> bool:
        """Report if a dataset is known to the cache.

        Always returns `False`.
        """
        return False

    def __str__(self) -> str:
        return f"{type(self).__name__}()"
