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

from __future__ import annotations

from .. import ddl

__all__ = (
    "RegistryManagerInstances",
    "RegistryManagerTypes",
)

import dataclasses
import logging
from collections.abc import Mapping
from typing import Any, Generic, TypeVar

import sqlalchemy
from lsst.utils import doImportType

from .._column_type_info import ColumnTypeInfo
from .._config import Config
from ..dimensions import DimensionConfig, DimensionUniverse
from ._caching_context import CachingContext
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
    VersionedExtension,
    VersionTuple,
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

    obscore: _ObsCore | None
    """Manager for `ObsCore` table(s).
    """


@dataclasses.dataclass(frozen=True, eq=False)
class RegistryManagerTypes(
    _GenericRegistryManagers[
        type[ButlerAttributeManager],
        type[DimensionRecordStorageManager],
        type[CollectionManager],
        type[DatasetRecordStorageManager],
        type[OpaqueTableStorageManager],
        type[DatastoreRegistryBridgeManager],
        type[ObsCoreTableManager],
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
        managers = {field.name for field in dataclasses.fields(cls)} - {"manager_configs", "schema_versions"}
        # Values of "config" sub-key, if any, indexed by manager name.
        configs: dict[str, Mapping] = {}
        schema_versions: dict[str, VersionTuple] = {}
        manager_types: dict[str, type] = {}
        for manager in managers:
            manager_config = config["managers"].get(manager)
            if isinstance(manager_config, Config):
                # Expect "cls" and optional "config" and "schema_version"
                # sub-keys.
                manager_config_dict = manager_config.toDict()
                try:
                    class_name = manager_config_dict.pop("cls")
                except KeyError:
                    raise KeyError(f"'cls' key is not defined in {manager!r} manager configuration") from None
                if (mgr_config := manager_config_dict.pop("config", None)) is not None:
                    configs[manager] = mgr_config
                if (mgr_version := manager_config_dict.pop("schema_version", None)) is not None:
                    # Note that we do not check versions that come from config
                    # for compatibility, they may be overriden later by
                    # versions from registry.
                    schema_versions[manager] = VersionTuple.fromString(mgr_version)
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
            return cls(**manager_types, manager_configs=configs, schema_versions=schema_versions)
        else:
            return cls(
                **manager_types, obscore=None, manager_configs=configs, schema_versions=schema_versions
            )

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
        # If schema versions were specified in the config, check that they are
        # compatible with their managers.
        managers = self.as_dict()
        for manager_type, schema_version in self.schema_versions.items():
            manager_class = managers[manager_type]
            manager_class.checkNewSchemaVersion(schema_version)

        universe = DimensionUniverse(dimensionConfig)
        with database.declareStaticTables(create=True) as context:
            if self.datasets.getIdColumnType() is sqlalchemy.BigInteger:
                raise RuntimeError(
                    "New data repositories should be created with UUID dataset IDs instead of autoincrement "
                    "integer dataset IDs.",
                )
            instances = RegistryManagerInstances.initialize(database, context, types=self, universe=universe)

        # store managers and their versions in attributes table
        versions = ButlerVersionsManager(instances.attributes)
        versions.storeManagersConfig(instances.as_dict())

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
        # embedded dimensions configuration. Note that we do not check this
        # manager version before initializing it, it is supposed to be
        # completely backward- and forward-compatible.
        with database.declareStaticTables(create=False) as context:
            attributes = self.attributes.initialize(database, context)

        # Verify that configured classes are compatible with the ones stored
        # in registry.
        versions = ButlerVersionsManager(attributes)
        versions.checkManagersConfig(self.as_dict())

        # Read schema versions from registry and validate them.
        self.schema_versions.update(versions.managerVersions())
        for manager_type, manager_class in self.as_dict().items():
            schema_version = self.schema_versions.get(manager_type)
            if schema_version is not None:
                manager_class.checkCompatibility(schema_version, database.isWriteable())

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

        # Load content from database that we try to keep in-memory.
        instances.refresh()
        return instances

    def as_dict(self) -> Mapping[str, type[VersionedExtension]]:
        """Return contained managers as a dictionary with manager type name as
        a key.

        Returns
        -------
        extensions : `~collections.abc.Mapping` [`str`, `VersionedExtension`]
            Maps manager type name (e.g. "datasets") to its corresponding
            manager class. Only existing managers are returned.
        """
        extras = {"manager_configs", "schema_versions"}
        managers = {f.name: getattr(self, f.name) for f in dataclasses.fields(self) if f.name not in extras}
        return {key: value for key, value in managers.items() if value is not None}

    manager_configs: dict[str, Mapping] = dataclasses.field(default_factory=dict)
    """Per-manager configuration options passed to their initialize methods.
    """

    schema_versions: dict[str, VersionTuple] = dataclasses.field(default_factory=dict)
    """Per-manager schema versions defined by configuration, optional."""


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

    caching_context: CachingContext
    """Object containing caches for for various information generated by
    managers.
    """

    @classmethod
    def initialize(
        cls,
        database: Database,
        context: StaticTablesContext,
        *,
        types: RegistryManagerTypes,
        universe: DimensionUniverse,
        caching_context: CachingContext | None = None,
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
        if caching_context is None:
            caching_context = CachingContext()
        dummy_table = ddl.TableSpec(fields=())
        kwargs: dict[str, Any] = {}
        schema_versions = types.schema_versions
        kwargs["attributes"] = types.attributes.initialize(
            database, context, registry_schema_version=schema_versions.get("attributes")
        )
        kwargs["dimensions"] = types.dimensions.initialize(
            database, context, universe=universe, registry_schema_version=schema_versions.get("dimensions")
        )
        kwargs["collections"] = types.collections.initialize(
            database,
            context,
            dimensions=kwargs["dimensions"],
            caching_context=caching_context,
            registry_schema_version=schema_versions.get("collections"),
        )
        datasets = types.datasets.initialize(
            database,
            context,
            collections=kwargs["collections"],
            dimensions=kwargs["dimensions"],
            registry_schema_version=schema_versions.get("datasets"),
            caching_context=caching_context,
        )
        kwargs["datasets"] = datasets
        kwargs["opaque"] = types.opaque.initialize(
            database, context, registry_schema_version=schema_versions.get("opaque")
        )
        kwargs["datastores"] = types.datastores.initialize(
            database,
            context,
            opaque=kwargs["opaque"],
            datasets=types.datasets,
            universe=universe,
            registry_schema_version=schema_versions.get("datastores"),
        )
        if types.obscore is not None and "obscore" in types.manager_configs:
            kwargs["obscore"] = types.obscore.initialize(
                database,
                context,
                universe=universe,
                config=types.manager_configs["obscore"],
                datasets=types.datasets,
                dimensions=kwargs["dimensions"],
                registry_schema_version=schema_versions.get("obscore"),
            )
        else:
            kwargs["obscore"] = None
        kwargs["column_types"] = ColumnTypeInfo(
            database.getTimespanRepresentation(),
            universe,
            dataset_id_spec=types.datasets.addDatasetForeignKey(
                dummy_table,
                primaryKey=False,
                nullable=False,
            ),
            run_key_spec=types.collections.addRunForeignKey(dummy_table, primaryKey=False, nullable=False),
            ingest_date_dtype=datasets.ingest_date_dtype(),
        )
        kwargs["caching_context"] = caching_context
        return cls(**kwargs)

    def as_dict(self) -> Mapping[str, VersionedExtension]:
        """Return contained managers as a dictionary with manager type name as
        a key.

        Returns
        -------
        extensions : `~collections.abc.Mapping` [`str`, `VersionedExtension`]
            Maps manager type name (e.g. "datasets") to its corresponding
            manager instance. Only existing managers are returned.
        """
        instances = {
            f.name: getattr(self, f.name)
            for f in dataclasses.fields(self)
            if f.name not in ("column_types", "caching_context")
        }
        return {key: value for key, value in instances.items() if value is not None}

    def refresh(self) -> None:
        """Refresh all in-memory state by querying the database or clearing
        caches.
        """
        self.dimensions.clearCaches()
        self.collections.refresh()
        self.datasets.refresh()
