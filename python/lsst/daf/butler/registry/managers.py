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
from collections.abc import Mapping
from typing import Any, Dict, Generic, Optional, Type, TypeVar

import sqlalchemy
from lsst.utils import doImportType

from ..core import ColumnTypeInfo, Config, DimensionConfig, DimensionUniverse, ddl
from ._config import RegistryConfig
from .interfaces import (
    ButlerAttributeManager,
    CollectionManager,
    Database,
    DatasetRecordStorageManager,
    DatastoreRegistryBridgeManager,
    DimensionRecordStorageManager,
    ObsCoreTableManager,
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
_ObsCore = TypeVar("_ObsCore")


_LOG = logging.getLogger(__name__)

# key for dimensions configuration in attributes table
_DIMENSIONS_ATTR = "config:dimensions.json"

# key for obscore configuration in attributes table
_OBSCORE_ATTR = "config:obscore.json"


@dataclasses.dataclass(frozen=True, eq=False)
class _GenericRegistryManagers(
    Generic[_Attributes, _Dimensions, _Collections, _Datasets, _Opaque, _Datastores, _ObsCore]
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

    obscore: Optional[_ObsCore]
    """Manager for `ObsCore` table(s).
    """


@dataclasses.dataclass(frozen=True, eq=False)
class RegistryManagerTypes(
    _GenericRegistryManagers[
        Type[ButlerAttributeManager],
        Type[DimensionRecordStorageManager],
        Type[CollectionManager],
        Type[DatasetRecordStorageManager],
        Type[OpaqueTableStorageManager],
        Type[DatastoreRegistryBridgeManager],
        Type[ObsCoreTableManager],
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
        # We only check for manager names defined in class attributes.
        # TODO: Maybe we need to check keys for unknown names/typos?
        managers = {field.name for field in dataclasses.fields(cls)} - {"manager_configs"}
        # Values of "config" sub-key, if any, indexed by manager name.
        configs: Dict[str, Mapping] = {}
        manager_types: Dict[str, Type] = {}
        for manager in managers:
            manager_config = config["managers"].get(manager)
            if isinstance(manager_config, Config):
                # Expect "cls" and optional "config" sub-keys.
                manager_config_dict = manager_config.toDict()
                try:
                    class_name = manager_config_dict.pop("cls")
                except KeyError:
                    raise KeyError(f"'cls' key is not defined in {manager!r} manager configuration") from None
                if (mgr_config := manager_config_dict.pop("config", None)) is not None:
                    configs[manager] = mgr_config
                if manager_config_dict:
                    raise ValueError(
                        f"{manager!r} manager configuration has unexpected keys: {set(manager_config_dict)}"
                    )
            elif isinstance(manager_config, str):
                class_name = manager_config
            elif manager_config is None:
                # Some managers may be optional.
                continue
            else:
                raise KeyError(f"Unexpected type of {manager!r} manager configuration: {manager_config!r}")
            manager_types[manager] = doImportType(class_name)

        # obscore need special care because it's the only manager which can be
        # None, and we cannot define default value for it.
        if "obscore" in manager_types:
            return cls(**manager_types, manager_configs=configs)
        else:
            return cls(**manager_types, obscore=None, manager_configs=configs)

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
                raise RuntimeError(
                    "New data repositories should be created with UUID dataset IDs instead of autoincrement "
                    "integer dataset IDs.",
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
        if instances.obscore is not None:
            json = instances.obscore.config_json()
            instances.attributes.set(_OBSCORE_ATTR, json)
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
        if self.obscore is not None:
            # Get ObsCore configuration from attributes table, this silently
            # overrides whatever may come from config file. Idea is that we do
            # not want to carry around the whole thing, and butler config will
            # have empty obscore configuration after initialization. When
            # configuration is missing from attributes table, the obscore table
            # does not exist, and we do not instantiate obscore manager.
            obscoreString = attributes.get(_OBSCORE_ATTR)
            if obscoreString is not None:
                self.manager_configs["obscore"] = Config.fromString(obscoreString, format="json")
        with database.declareStaticTables(create=False) as context:
            instances = RegistryManagerInstances.initialize(database, context, types=self, universe=universe)
            versions = instances.getVersions()
        # verify that configured versions are compatible with schema
        versions.checkManagersConfig()
        versions.checkManagersVersions(database.isWriteable())
        # Load content from database that we try to keep in-memory.
        instances.refresh()
        return instances

    manager_configs: Dict[str, Mapping] = dataclasses.field(default_factory=dict)
    """Per-manager configuration options passed to their initialize methods.
    """


@dataclasses.dataclass(frozen=True, eq=False)
class RegistryManagerInstances(
    _GenericRegistryManagers[
        ButlerAttributeManager,
        DimensionRecordStorageManager,
        CollectionManager,
        DatasetRecordStorageManager,
        OpaqueTableStorageManager,
        DatastoreRegistryBridgeManager,
        ObsCoreTableManager,
    ]
):
    """A struct used to pass around the manager instances that back a
    `Registry`.
    """

    column_types: ColumnTypeInfo
    """Information about column types that can differ between data repositories
    and registry instances, including the dimension universe.
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
        dummy_table = ddl.TableSpec(fields=())
        kwargs: Dict[str, Any] = {}
        kwargs["column_types"] = ColumnTypeInfo(
            database.getTimespanRepresentation(),
            universe,
            dataset_id_spec=types.datasets.addDatasetForeignKey(
                dummy_table,
                primaryKey=False,
                nullable=False,
            ),
            run_key_spec=types.collections.addRunForeignKey(dummy_table, primaryKey=False, nullable=False),
        )
        kwargs["attributes"] = types.attributes.initialize(database, context)
        kwargs["dimensions"] = types.dimensions.initialize(database, context, universe=universe)
        kwargs["collections"] = types.collections.initialize(
            database,
            context,
            dimensions=kwargs["dimensions"],
        )
        kwargs["datasets"] = types.datasets.initialize(
            database, context, collections=kwargs["collections"], dimensions=kwargs["dimensions"]
        )
        kwargs["opaque"] = types.opaque.initialize(database, context)
        kwargs["datastores"] = types.datastores.initialize(
            database,
            context,
            opaque=kwargs["opaque"],
            datasets=types.datasets,
            universe=universe,
        )
        if types.obscore is not None and "obscore" in types.manager_configs:
            kwargs["obscore"] = types.obscore.initialize(
                database,
                context,
                universe=universe,
                config=types.manager_configs["obscore"],
                datasets=types.datasets,
                dimensions=kwargs["dimensions"],
            )
        else:
            kwargs["obscore"] = None
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
            {f.name: getattr(self, f.name) for f in dataclasses.fields(self) if f.name != "column_types"},
        )

    def refresh(self) -> None:
        """Refresh all in-memory state by querying the database or clearing
        caches."""
        self.dimensions.clearCaches()
        self.collections.refresh()
        self.datasets.refresh()
