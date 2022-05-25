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

__all__ = (
    "RegistryManagerInstances",
    "RegistryManagerTypes",
)

import dataclasses
import logging
import warnings
from typing import Any, Dict, Generic, Type, TypeVar

import sqlalchemy
from lsst.utils import doImportType

from ..core import Config, DimensionConfig, DimensionUniverse
from ._config import RegistryConfig
from .interfaces import (
    ButlerAttributeManager,
    CollectionManager,
    Database,
    DatasetRecordStorageManager,
    DatastoreRegistryBridgeManager,
    DimensionRecordStorageManager,
    OpaqueTableStorageManager,
    StaticTablesContext,
)
from .versions import ButlerVersionsManager

_Attributes = TypeVar("_Attributes")
_Dimensions = TypeVar("_Dimensions")
_Collections = TypeVar("_Collections")
_Datasets = TypeVar("_Datasets")
_Opaque = TypeVar("_Opaque")
_Datastores = TypeVar("_Datastores")


_LOG = logging.getLogger(__name__)

# key for dimensions configuration in attributes table
_DIMENSIONS_ATTR = "config:dimensions.json"


@dataclasses.dataclass(frozen=True, eq=False)
class _GenericRegistryManagers(
    Generic[_Attributes, _Dimensions, _Collections, _Datasets, _Opaque, _Datastores]
):
    """Base struct used to pass around the manager instances or types that back
    a `Registry`.

    This class should only be used via its non-generic subclasses,
    `RegistryManagerInstances` and `RegistryManagerTypes`.
    """

    attributes: _Attributes
    """Manager for flat key-value pairs, including versions.
    """

    dimensions: _Dimensions
    """Manager for dimensions.
    """

    collections: _Collections
    """Manager for collections.
    """

    datasets: _Datasets
    """Manager for datasets, dataset types, and collection summaries.
    """

    opaque: _Opaque
    """Manager for opaque (to the Registry) tables.
    """

    datastores: _Datastores
    """Manager for the interface between `Registry` and `Datastore`.
    """


class RegistryManagerTypes(
    _GenericRegistryManagers[
        Type[ButlerAttributeManager],
        Type[DimensionRecordStorageManager],
        Type[CollectionManager],
        Type[DatasetRecordStorageManager],
        Type[OpaqueTableStorageManager],
        Type[DatastoreRegistryBridgeManager],
    ]
):
    """A struct used to pass around the types of the manager objects that back
    a `Registry`.
    """

    @classmethod
    def fromConfig(cls, config: RegistryConfig) -> RegistryManagerTypes:
        """Construct by extracting class names from configuration and importing
        them.

        Parameters
        ----------
        config : `RegistryConfig`
            Configuration object with a "managers" section that contains all
            fully-qualified class names for all manager types.

        Returns
        -------
        types : `RegistryManagerTypes`
            A new struct containing type objects.
        """
        return cls(**{f.name: doImportType(config["managers", f.name]) for f in dataclasses.fields(cls)})

    def makeRepo(self, database: Database, dimensionConfig: DimensionConfig) -> RegistryManagerInstances:
        """Create all persistent `Registry` state for a new, empty data
        repository, and return a new struct containing manager instances.

        Parameters
        ----------
        database : `Database`
            Object that represents a connection to the SQL database that will
            back the data repository.  Must point to an empty namespace, or at
            least one with no tables or other entities whose names might clash
            with those used by butler.
        dimensionConfig : `DimensionConfig`
            Configuration that defines a `DimensionUniverse`, to be written
            into the data repository and used to define aspects of the schema.

        Returns
        -------
        instances : `RegistryManagerInstances`
            Struct containing instances of the types contained by ``self``,
            pointing to the new repository and backed by ``database``.
        """
        universe = DimensionUniverse(dimensionConfig)
        with database.declareStaticTables(create=True) as context:
            if self.datasets.getIdColumnType() == sqlalchemy.BigInteger:
                warnings.warn(
                    "New data repositories should be created with UUID dataset IDs instead of autoincrement "
                    "integer dataset IDs; support for integers will be removed after v25.",
                    FutureWarning,
                )
            instances = RegistryManagerInstances.initialize(database, context, types=self, universe=universe)
            versions = instances.getVersions()
        # store managers and their versions in attributes table
        versions.storeManagersConfig()
        versions.storeManagersVersions()
        # dump universe config as json into attributes (faster than YAML)
        json = dimensionConfig.dump(format="json")
        if json is not None:
            instances.attributes.set(_DIMENSIONS_ATTR, json)
        else:
            raise RuntimeError("Unexpectedly failed to serialize DimensionConfig to JSON")
        return instances

    def loadRepo(self, database: Database) -> RegistryManagerInstances:
        """Construct manager instances that point to an existing data
        repository.

        Parameters
        ----------
        database : `Database`
            Object that represents a connection to the SQL database that backs
            the data repository.  Must point to a namespace that already holds
            all tables and other persistent entities used by butler.

        Returns
        -------
        instances : `RegistryManagerInstances`
            Struct containing instances of the types contained by ``self``,
            pointing to the new repository and backed by ``database``.
        """
        # Create attributes manager only first, so we can use it to load the
        # embedded dimensions configuration.
        with database.declareStaticTables(create=False) as context:
            attributes = self.attributes.initialize(database, context)
            versions = ButlerVersionsManager(attributes, dict(attributes=attributes))
            # verify that configured versions are compatible with schema
            versions.checkManagersConfig()
            versions.checkManagersVersions(database.isWriteable())
        # get serialized as a string from database
        dimensionsString = attributes.get(_DIMENSIONS_ATTR)
        if dimensionsString is not None:
            dimensionConfig = DimensionConfig(Config.fromString(dimensionsString, format="json"))
        else:
            raise LookupError(f"Registry attribute {_DIMENSIONS_ATTR} is missing from database")
        universe = DimensionUniverse(dimensionConfig)
        with database.declareStaticTables(create=False) as context:
            instances = RegistryManagerInstances.initialize(database, context, types=self, universe=universe)
            versions = instances.getVersions()
        # verify that configured versions are compatible with schema
        versions.checkManagersConfig()
        versions.checkManagersVersions(database.isWriteable())
        # Load content from database that we try to keep in-memory.
        instances.refresh()
        return instances


class RegistryManagerInstances(
    _GenericRegistryManagers[
        ButlerAttributeManager,
        DimensionRecordStorageManager,
        CollectionManager,
        DatasetRecordStorageManager,
        OpaqueTableStorageManager,
        DatastoreRegistryBridgeManager,
    ]
):
    """A struct used to pass around the manager instances that back a
    `Registry`.
    """

    @classmethod
    def initialize(
        cls,
        database: Database,
        context: StaticTablesContext,
        *,
        types: RegistryManagerTypes,
        universe: DimensionUniverse,
    ) -> RegistryManagerInstances:
        """Construct manager instances from their types and an existing
        database connection.

        Parameters
        ----------
        database : `Database`
            Object that represents a connection to the SQL database that backs
            the data repository.
        context : `StaticTablesContext`
            Object used to create tables in ``database``.
        types : `RegistryManagerTypes`
            Struct containing type objects for the manager instances to
            construct.
        universe : `DimensionUniverse`
            Object that describes all dimensions in this data repository.

        Returns
        -------
        instances : `RegistryManagerInstances`
            Struct containing manager instances.
        """
        kwargs: Dict[str, Any] = {}
        kwargs["attributes"] = types.attributes.initialize(database, context)
        kwargs["dimensions"] = types.dimensions.initialize(database, context, universe=universe)
        kwargs["collections"] = types.collections.initialize(
            database,
            context,
            dimensions=kwargs["dimensions"],
        )
        kwargs["datasets"] = types.datasets.initialize(
            database,
            context,
            collections=kwargs["collections"],
            dimensions=kwargs["dimensions"],
        )
        kwargs["opaque"] = types.opaque.initialize(database, context)
        kwargs["datastores"] = types.datastores.initialize(
            database,
            context,
            opaque=kwargs["opaque"],
            datasets=types.datasets,
            universe=universe,
        )
        return cls(**kwargs)

    def getVersions(self) -> ButlerVersionsManager:
        """Return an object that can report, check, and save the versions of
        all manager objects.

        Returns
        -------
        versions : `ButlerVersionsManager`
            Object that manages versions.
        """
        return ButlerVersionsManager(
            self.attributes,
            # Can't use dataclasses.asdict here, because it tries to do some
            # deepcopy stuff (?!) in order to find dataclasses recursively, and
            # that doesn't work on some manager objects that definitely aren't
            # supposed to be deep-copied anyway.
            {f.name: getattr(self, f.name) for f in dataclasses.fields(self)},
        )

    def refresh(self) -> None:
        """Refresh all in-memory state by querying the database."""
        self.dimensions.clearCaches()
        self.dimensions.refresh()
        self.collections.refresh()
        self.datasets.refresh()
