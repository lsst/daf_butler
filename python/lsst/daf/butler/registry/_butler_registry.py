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

__all__ = ("_ButlerRegistry",)

from abc import abstractmethod
from typing import TYPE_CHECKING

from lsst.resources import ResourcePathExpression

from ..core import Config, DimensionConfig
from ._config import RegistryConfig
from ._defaults import RegistryDefaults
from ._registry import Registry

if TYPE_CHECKING:
    from .._butlerConfig import ButlerConfig
    from .interfaces import CollectionRecord, DatastoreRegistryBridgeManager


class _ButlerRegistry(Registry):
    """Registry interface extended with methods used by Butler implementation.

    Each registry implementation can have its own constructor parameters.
    The assumption is that an instance of a specific subclass will be
    constructed from configuration using `Registry.fromConfig()`.
    The base class will look for a ``cls`` entry and call that specific
    `fromConfig()` method.
    """

    defaultConfigFile: str | None = None
    """Path to configuration defaults. Accessed within the ``configs`` resource
    or relative to a search path. Can be None if no defaults specified.
    """

    @classmethod
    def forceRegistryConfig(
        cls, config: ButlerConfig | RegistryConfig | Config | str | None
    ) -> RegistryConfig:
        """Force the supplied config to a `RegistryConfig`.

        Parameters
        ----------
        config : `RegistryConfig`, `Config` or `str` or `None`
            Registry configuration, if missing then default configuration will
            be loaded from registry.yaml.

        Returns
        -------
        registry_config : `RegistryConfig`
            A registry config.
        """
        if not isinstance(config, RegistryConfig):
            if isinstance(config, str | Config) or config is None:
                config = RegistryConfig(config)
            else:
                raise ValueError(f"Incompatible Registry configuration: {config}")
        return config

    @classmethod
    @abstractmethod
    def createFromConfig(
        cls,
        config: RegistryConfig | str | None = None,
        dimensionConfig: DimensionConfig | str | None = None,
        butlerRoot: ResourcePathExpression | None = None,
    ) -> _ButlerRegistry:
        """Create registry database and return `_ButlerRegistry` instance.

        This method initializes database contents, database must be empty
        prior to calling this method.

        Parameters
        ----------
        config : `RegistryConfig` or `str`, optional
            Registry configuration, if missing then default configuration will
            be loaded from registry.yaml.
        dimensionConfig : `DimensionConfig` or `str`, optional
            Dimensions configuration, if missing then default configuration
            will be loaded from dimensions.yaml.
        butlerRoot : convertible to `lsst.resources.ResourcePath`, optional
            Path to the repository root this `Registry` will manage.

        Returns
        -------
        registry : `_ButlerRegistry`
            A new `_ButlerRegistry` instance.

        Notes
        -----
        This class will determine the concrete `_ButlerRegistry` subclass to
        use from configuration.  Each subclass should implement this method
        even if it can not create a registry.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def fromConfig(
        cls,
        config: ButlerConfig | RegistryConfig | Config | str,
        butlerRoot: ResourcePathExpression | None = None,
        writeable: bool = True,
        defaults: RegistryDefaults | None = None,
    ) -> _ButlerRegistry:
        """Create `_ButlerRegistry` subclass instance from ``config``.

        Registry database must be initialized prior to calling this method.

        Parameters
        ----------
        config : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
            Registry configuration
        butlerRoot : convertible to `lsst.resources.ResourcePath`, optional
            Path to the repository root this `Registry` will manage.
        writeable : `bool`, optional
            If `True` (default) create a read-write connection to the database.
        defaults : `~lsst.daf.butler.registry.RegistryDefaults`, optional
            Default collection search path and/or output `~CollectionType.RUN`
            collection.

        Returns
        -------
        registry : `_ButlerRegistry` (subclass)
            A new `_ButlerRegistry` subclass instance.

        Notes
        -----
        This class will determine the concrete `_ButlerRegistry` subclass to
        use from configuration.  Each subclass should implement this method.
        """
        # The base class implementation should trampoline to the correct
        # subclass. No implementation should ever use this implementation
        # directly. If no class is specified, default to the standard
        # registry.
        raise NotImplementedError()

    @abstractmethod
    def copy(self, defaults: RegistryDefaults | None = None) -> _ButlerRegistry:
        """Create a new `_ButlerRegistry` backed by the same data repository
        and connection as this one, but independent defaults.

        Parameters
        ----------
        defaults : `~lsst.daf.butler.registry.RegistryDefaults`, optional
            Default collections and data ID values for the new registry.  If
            not provided, ``self.defaults`` will be used (but future changes
            to either registry's defaults will not affect the other).

        Returns
        -------
        copy : `_ButlerRegistry`
            A new `_ButlerRegistry` instance with its own defaults.

        Notes
        -----
        Because the new registry shares a connection with the original, they
        also share transaction state (despite the fact that their `transaction`
        context manager methods do not reflect this), and must be used with
        care.
        """
        raise NotImplementedError()

    @abstractmethod
    def _get_collection_record(self, name: str) -> CollectionRecord:
        """Return the record for this collection.

        Parameters
        ----------
        name : `str`
            Name of the collection for which the record is to be retrieved.

        Returns
        -------
        record : `CollectionRecord`
            The record for this collection.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDatastoreBridgeManager(self) -> DatastoreRegistryBridgeManager:
        """Return an object that allows a new `Datastore` instance to
        communicate with this `Registry`.

        Returns
        -------
        manager : `~.interfaces.DatastoreRegistryBridgeManager`
            Object that mediates communication between this `Registry` and its
            associated datastores.
        """
        raise NotImplementedError()
