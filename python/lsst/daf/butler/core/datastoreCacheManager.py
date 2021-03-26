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
    Optional,
    Union,
)

from abc import ABC, abstractmethod
import logging
import tempfile

from .configSupport import processLookupConfigs
from .config import ConfigSubset
from ._butlerUri import ButlerURI

if TYPE_CHECKING:
    from .dimensions import DimensionUniverse
    from .datasets import DatasetType, DatasetRef
    from .storageClass import StorageClass
    from .configSupport import LookupKey

log = logging.getLogger(__name__)


class DatastoreCacheManagerConfig(ConfigSubset):
    """Configuration information for `DatastoreCacheManager`."""


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

        if (root := self.config.get("root")):
            self.cache_directory = ButlerURI(root, forceAbsolute=True)
        else:
            self.cache_directory = ButlerURI(tempfile.mkdtemp(prefix="butler-"), forceDirectory=True,
                                             isTemporary=True)

        # Calculate the caching lookup table.
        self._lut = processLookupConfigs(self.config["cacheable"], universe=universe)

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
        return self.cache_directory.join(f"{ref.id}{extension}")

    def move_to_cache(self, uri: ButlerURI, ref: DatasetRef) -> Optional[ButlerURI]:
        # Docstring inherited
        if ref.id is None:
            raise ValueError(f"Can not cache a file associated with an unresolved reference ({ref})")

        if not self.should_be_cached(ref):
            return None

        # Write the file using the id of the dataset ref and the file
        # extension.
        cached_location = self._construct_cache_name(ref, uri.getExtension())

        # Move into the cache. This will complain if something is already
        # in the cache for this file.
        cached_location.transfer_from(uri, transfer="move")
        log.debug("Cached dataset %s to %s", ref, cached_location)

        return cached_location

    def find_in_cache(self, ref: DatasetRef, extension: str) -> Optional[ButlerURI]:
        # Docstring inherited
        cached_location = self._construct_cache_name(ref, extension)
        if cached_location.exists():
            log.debug("Retrieved cached file %s for dataset %s.", cached_location, ref)
            return cached_location
        log.debug("Dataset %s not found in cache.", ref)
        return None


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
