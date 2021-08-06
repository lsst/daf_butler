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

__all__ = ("AbstractDatastoreCacheManager",
           "DatastoreDisabledCacheManager",
           "DatastoreCacheManager",
           "DatastoreCacheManagerConfig",
           )

from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    Iterator,
    ItemsView,
    KeysView,
    List,
    Optional,
    Union,
    ValuesView,
)

from abc import ABC, abstractmethod
from collections import defaultdict
import atexit
import datetime
import logging
import os
import shutil
import tempfile

from pydantic import BaseModel, PrivateAttr

from .configSupport import processLookupConfigs
from .config import ConfigSubset
from ._butlerUri import ButlerURI
from .datasets import DatasetId

if TYPE_CHECKING:
    from .dimensions import DimensionUniverse
    from .datasets import DatasetType, DatasetRef
    from .storageClass import StorageClass
    from .configSupport import LookupKey

log = logging.getLogger(__name__)


def remove_cache_directory(directory: str) -> None:
    """Remove the specified directory and all its contents.
    """
    log.debug("Removing temporary cache directory %s", directory)
    shutil.rmtree(directory, ignore_errors=True)


def _construct_cache_path(root: ButlerURI, ref: DatasetRef, extension: str) -> ButlerURI:
    """Construct the full path to use for this dataset in the cache.

    Parameters
    ----------
    ref : `DatasetRef`
        The dataset to look up in or write to the cache.
    extension : `str`
        File extension to use for this file.

    Returns
    -------
    uri : `ButlerURI`
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
    def from_file(cls, file: ButlerURI, root: ButlerURI) -> CacheEntry:
        """Construct an object from a file name.

        Parameters
        ----------
        file : `ButlerURI`
            Path to the file.
        root : `ButlerURI`
            Cache root directory.
        """
        file_in_cache = file.relative_to(root)
        if file_in_cache is None:
            raise ValueError(f"Supplied file {file} is not inside root {root}")
        parts = _parse_cache_name(file_in_cache)

        stat = os.stat(file.ospath)
        return cls(name=file_in_cache, size=stat.st_size, ref=parts["id"], component=parts["component"],
                   ctime=datetime.datetime.utcfromtimestamp(stat.st_ctime))


class CacheRegistry(BaseModel):
    """Collection of cache entries."""

    _size: int = PrivateAttr(0)
    """Size of the cache."""

    _entries: Dict[str, CacheEntry] = PrivateAttr({})
    """Internal collection of cache entries."""

    @property
    def cache_size(self) -> int:
        return self._size

    def __getitem__(self, key: str) -> CacheEntry:
        return self._entries[key]

    def __setitem__(self, key: str, entry: CacheEntry) -> None:
        self._size += entry.size
        self._entries[key] = entry

    def __delitem__(self, key: str) -> None:
        entry = self._entries.pop(key)
        self._decrement(entry)

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

    def pop(self, key: str, default: Optional[CacheEntry] = None) -> Optional[CacheEntry]:
        entry = self._entries.pop(key, default)
        self._decrement(entry)
        return entry


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

    def __init__(self, config: Union[str, DatastoreCacheManagerConfig],
                 universe: DimensionUniverse):
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
    def move_to_cache(self, uri: ButlerURI, ref: DatasetRef) -> Optional[ButlerURI]:
        """Move a file to the cache.

        Move the given file into the cache, using the supplied DatasetRef
        for naming. A call is made to `should_be_cached()` and if the
        DatasetRef should not be accepted `None` will be returned.

        Cache expiry can occur during this.

        Parameters
        ----------
        uri : `ButlerURI`
            Location of the file to be relocated to the cache. Will be moved.
        ref : `DatasetRef`
            Ref associated with this file. Will be used to determine the name
            of the file within the cache.

        Returns
        -------
        new : `ButlerURI` or `None`
            URI to the file within the cache, or `None` if the dataset
            was not accepted by the cache.
        """
        raise NotImplementedError()

    @abstractmethod
    def find_in_cache(self, ref: DatasetRef, extension: str) -> Optional[ButlerURI]:
        """Look for a dataset in the cache and return its location.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to locate in the cache.
        extension : `str`
            File extension expected.

        Returns
        -------
        uri : `ButlerURI` or `None`
            The URI to the cached file, or `None` if the file has not been
            cached.
        """
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
    """

    def __init__(self, config: Union[str, DatastoreCacheManagerConfig],
                 universe: DimensionUniverse):
        super().__init__(config, universe)

        # Set cache directory if it pre-exists, else defer creation until
        # requested.
        root = self.config.get("root")
        self._cache_directory = ButlerURI(root, forceAbsolute=True) if root is not None else None

        # Calculate the caching lookup table.
        self._lut = processLookupConfigs(self.config["cacheable"], universe=universe)

        # Expiration mode.
        self._expiration_mode: Optional[str] = self.config.get(("expiry", "mode"))
        if self._expiration_mode is None:
            threshold = None
        else:
            threshold = self.config["expiry", "threshold"]
        self._expiration_threshold: Optional[int] = threshold
        if threshold is None and self._expiration_mode is not None:
            raise ValueError("Cache expiration threshold must be set for expiration mode "
                             f"{self._expiration_mode}")

        # Files in cache, indexed by path within the cache directory.
        self._cache_entries = CacheRegistry()

    @property
    def cache_directory(self) -> ButlerURI:
        if self._cache_directory is None:
            # Create on demand.
            self._cache_directory = ButlerURI(tempfile.mkdtemp(prefix="butler-"), forceDirectory=True,
                                              isTemporary=True)
            log.debug("Creating temporary cache directory at %s", self._cache_directory)
            # Remove when we no longer need it.
            atexit.register(remove_cache_directory, self._cache_directory.ospath)
        return self._cache_directory

    def should_be_cached(self, entity: Union[DatasetRef, DatasetType, StorageClass]) -> bool:
        # Docstring inherited
        matchName: Union[LookupKey, str] = "{} (via default)".format(entity)
        should_cache = False

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

    def _construct_cache_name(self, ref: DatasetRef, extension: str) -> ButlerURI:
        """Construct the name to use for this dataset in the cache.

        Parameters
        ----------
        ref : `DatasetRef`
            The dataset to look up in or write to the cache.
        extension : `str`
            File extension to use for this file.

        Returns
        -------
        uri : `ButlerURI`
            URI to use for this dataset in the cache.
        """
        return _construct_cache_path(self.cache_directory, ref, extension)

    def move_to_cache(self, uri: ButlerURI, ref: DatasetRef) -> Optional[ButlerURI]:
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

        # Move into the cache. This will complain if something is already
        # in the cache for this file.
        cached_location.transfer_from(uri, transfer="move")
        log.debug("Cached dataset %s to %s", ref, cached_location)

        self._register_cache_entry(cached_location)

        return cached_location

    def find_in_cache(self, ref: DatasetRef, extension: str) -> Optional[ButlerURI]:
        # Docstring inherited
        # Short circuit this if the cache directory has not been created yet.
        if self._cache_directory is None:
            return None

        cached_location = self._construct_cache_name(ref, extension)
        if cached_location.exists():
            log.debug("Retrieved cached file %s for dataset %s.", cached_location, ref)
            return cached_location
        log.debug("Dataset %s not found in cache.", ref)
        return None

    def _register_cache_entry(self, cached_location: ButlerURI, can_exist: bool = False) -> str:
        """Record the file in the cache registry.

        Parameters
        ----------
        cached_location : `ButlerURI`
            Location of the file to be registered.
        can_exist : `bool`, optional
            If `True` the item being registered can already be listed.
            This can allow a cache refresh to run without checking the
            file again. If `False` it is an error for the registry to
            already know about this file.

        Returns
        -------
        cache_key : `str`
            The key used in the registry for this file.
        """
        path_in_cache = cached_location.relative_to(self.cache_directory)
        if path_in_cache is None:
            raise ValueError(f"Can not register cached file {cached_location} that is not within"
                             f" the cache directory at {self.cache_directory}.")
        if path_in_cache in self._cache_entries:
            if can_exist:
                return path_in_cache
            else:
                raise ValueError(f"Cached file {cached_location} is already known to the registry"
                                 " but this was expected to be a new file.")
        details = CacheEntry.from_file(cached_location, root=self.cache_directory)
        self._cache_entries[path_in_cache] = details
        return path_in_cache

    def scan_cache(self) -> None:
        """Scan the cache directory and record information about files.
        """
        found = set()
        for file in ButlerURI.findFileResources([self.cache_directory]):
            assert isinstance(file, ButlerURI), "Unexpectedly did not get ButlerURI from iterator"
            path_in_cache = self._register_cache_entry(file, can_exist=True)
            found.add(path_in_cache)

        # Find any files that were recorded in the cache but are no longer
        # on disk. (something else cleared them out?)
        known_to_cache = set(self._cache_entries)
        missing = known_to_cache - found

        if missing:
            log.debug("Entries no longer on disk but thought to be in cache and so removed: %s",
                      ",".join(missing))
            for path_in_cache in missing:
                self._cache_entries.pop(path_in_cache)

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
            self._cache_entries.pop(entry)
            log.debug("Removing file from cache: %s", path)
            try:
                path.remove()
            except FileNotFoundError:
                pass

    def _expire_cache(self) -> None:
        """Expire the files in the cache.

        The expiration modes are defined by the config.
        Available options:

        * Number of files.
        * Number of datasets
        * Total size of files.
        * Age of files.

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
            log.warning("Requesting cache expiry of mode %s but no threshold set in config.",
                        self._expiration_mode)
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
            # Count the datasets, using reverse date order
            # so that oldest turn up first.
            datasets = defaultdict(list)
            for key in self._sort_cache():
                entry = self._cache_entries[key]
                datasets[entry.ref].append(key)

            n_datasets = len(datasets)
            n_over = n_datasets - self._expiration_threshold
            if n_over > 0:
                keys_to_remove = []
                removed = 0
                for dataset in datasets:
                    # Keys will be read out in insert order which
                    # will be date order.
                    keys_to_remove.extend(datasets[dataset])
                    removed += 1
                    if removed >= n_over:
                        break
                self._remove_from_cache(keys_to_remove)
            return

        if self._expiration_mode == "size":
            if self._cache_entries.cache_size > self._expiration_threshold:
                for key in self._sort_cache():
                    self._remove_from_cache([key])
                    if self._cache_entries.cache_size <= self._expiration_threshold:
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

    def __init__(self, config: Union[str, DatastoreCacheManagerConfig],
                 universe: DimensionUniverse):
        return

    def should_be_cached(self, entity: Union[DatasetRef, DatasetType, StorageClass]) -> bool:
        """Indicate whether the entity should be added to the cache.

        Always returns `False`.
        """
        return False

    def move_to_cache(self, uri: ButlerURI, ref: DatasetRef) -> Optional[ButlerURI]:
        """Move dataset to cache but always refuse and returns `None`."""
        return None

    def find_in_cache(self, ref: DatasetRef, extension: str) -> Optional[ButlerURI]:
        """Look for a dataset in the cache and return its location.

        Never finds a file.
        """
        return None
