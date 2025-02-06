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

__all__ = ("SqlRegistry",)

import contextlib
import logging
import warnings
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, cast

import sqlalchemy

from lsst.daf.relation import LeafRelation, Relation
from lsst.resources import ResourcePathExpression
from lsst.utils.iteration import ensure_iterable

from .._collection_type import CollectionType
from .._column_tags import DatasetColumnTag
from .._config import Config
from .._dataset_association import DatasetAssociation
from .._dataset_ref import DatasetId, DatasetIdGenEnum, DatasetRef
from .._dataset_type import DatasetType
from .._exceptions import (
    CalibrationLookupError,
    DataIdValueError,
    DimensionNameError,
    InconsistentDataIdError,
)
from .._storage_class import StorageClassFactory
from .._timespan import Timespan
from ..dimensions import (
    DataCoordinate,
    DataId,
    DimensionConfig,
    DimensionElement,
    DimensionGroup,
    DimensionRecord,
    DimensionUniverse,
)
from ..dimensions.record_cache import DimensionRecordCache
from ..direct_query_driver import DirectQueryDriver
from ..progress import Progress
from ..queries import Query
from ..registry import (
    ArgumentError,
    CollectionExpressionError,
    CollectionSummary,
    CollectionTypeError,
    ConflictingDefinitionError,
    DatasetTypeError,
    MissingDatasetTypeError,
    NoDefaultCollectionError,
    OrphanedRecordError,
    RegistryConfig,
    RegistryConsistencyError,
    RegistryDefaults,
    queries,
)
from ..registry.interfaces import ChainedCollectionRecord, ReadOnlyDatabaseError, RunRecord
from ..registry.managers import RegistryManagerInstances, RegistryManagerTypes
from ..registry.wildcards import CollectionWildcard, DatasetTypeWildcard
from ..utils import transactional

if TYPE_CHECKING:
    from .._butler_config import ButlerConfig
    from ..datastore._datastore import DatastoreOpaqueTable
    from ..datastore.stored_file_info import StoredDatastoreItemInfo
    from ..registry._registry import CollectionArgType
    from ..registry.interfaces import (
        CollectionRecord,
        Database,
        DatastoreRegistryBridgeManager,
        ObsCoreTableManager,
    )


_LOG = logging.getLogger(__name__)


class SqlRegistry:
    """Butler Registry implementation that uses SQL database as backend.

    Parameters
    ----------
    database : `Database`
        Database instance to store Registry.
    defaults : `RegistryDefaults`
        Default collection search path and/or output `~CollectionType.RUN`
        collection.
    managers : `RegistryManagerInstances`
        All the managers required for this registry.
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
    def createFromConfig(
        cls,
        config: RegistryConfig | str | None = None,
        dimensionConfig: DimensionConfig | str | None = None,
        butlerRoot: ResourcePathExpression | None = None,
    ) -> SqlRegistry:
        """Create registry database and return `SqlRegistry` instance.

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
            Path to the repository root this `SqlRegistry` will manage.

        Returns
        -------
        registry : `SqlRegistry`
            A new `SqlRegistry` instance.
        """
        config = cls.forceRegistryConfig(config)
        config.replaceRoot(butlerRoot)

        if isinstance(dimensionConfig, str):
            dimensionConfig = DimensionConfig(dimensionConfig)
        elif dimensionConfig is None:
            dimensionConfig = DimensionConfig()
        elif not isinstance(dimensionConfig, DimensionConfig):
            raise TypeError(f"Incompatible Dimension configuration type: {type(dimensionConfig)}")

        DatabaseClass = config.getDatabaseClass()
        database = DatabaseClass.fromUri(
            config.connectionString, origin=config.get("origin", 0), namespace=config.get("namespace")
        )
        managerTypes = RegistryManagerTypes.fromConfig(config)
        managers = managerTypes.makeRepo(database, dimensionConfig)
        return cls(database, RegistryDefaults(), managers)

    @classmethod
    def fromConfig(
        cls,
        config: ButlerConfig | RegistryConfig | Config | str,
        butlerRoot: ResourcePathExpression | None = None,
        writeable: bool = True,
        defaults: RegistryDefaults | None = None,
    ) -> SqlRegistry:
        """Create `Registry` subclass instance from `config`.

        Registry database must be initialized prior to calling this method.

        Parameters
        ----------
        config : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
            Registry configuration.
        butlerRoot : `lsst.resources.ResourcePathExpression`, optional
            Path to the repository root this `Registry` will manage.
        writeable : `bool`, optional
            If `True` (default) create a read-write connection to the database.
        defaults : `RegistryDefaults`, optional
            Default collection search path and/or output `~CollectionType.RUN`
            collection.

        Returns
        -------
        registry : `SqlRegistry`
            A new `SqlRegistry` subclass instance.
        """
        config = cls.forceRegistryConfig(config)
        config.replaceRoot(butlerRoot)
        DatabaseClass = config.getDatabaseClass()
        database = DatabaseClass.fromUri(
            config.connectionString,
            origin=config.get("origin", 0),
            namespace=config.get("namespace"),
            writeable=writeable,
        )
        managerTypes = RegistryManagerTypes.fromConfig(config)
        with database.session():
            managers = managerTypes.loadRepo(database)
        if defaults is None:
            defaults = RegistryDefaults()
        return cls(database, defaults, managers)

    def __init__(
        self,
        database: Database,
        defaults: RegistryDefaults,
        managers: RegistryManagerInstances,
    ):
        self._db = database
        self._managers = managers
        self.storageClasses = StorageClassFactory()
        # This is public to SqlRegistry's internal-to-daf_butler callers, but
        # it is intentionally not part of RegistryShim.
        self.dimension_record_cache = DimensionRecordCache(
            self._managers.dimensions.universe,
            fetch=self._managers.dimensions.fetch_cache_dict,
        )
        # Intentionally invoke property setter to initialize defaults.  This
        # can only be done after most of the rest of Registry has already been
        # initialized, and must be done before the property getter is used.
        self.defaults = defaults
        # TODO: This is currently initialized by `make_datastore_tables`,
        # eventually we'll need to do it during construction.
        # The mapping is indexed by the opaque table name.
        self._datastore_record_classes: Mapping[str, type[StoredDatastoreItemInfo]] = {}

    def __str__(self) -> str:
        return str(self._db)

    def __repr__(self) -> str:
        return f"SqlRegistry({self._db!r}, {self.dimensions!r})"

    def isWriteable(self) -> bool:
        """Return `True` if this registry allows write operations, and `False`
        otherwise.
        """
        return self._db.isWriteable()

    def copy(self, defaults: RegistryDefaults | None = None) -> SqlRegistry:
        """Create a new `SqlRegistry` backed by the same data repository
        as this one and sharing a database connection pool with it, but with
        independent defaults and database sessions.

        Parameters
        ----------
        defaults : `~lsst.daf.butler.registry.RegistryDefaults`, optional
            Default collections and data ID values for the new registry.  If
            not provided, ``self.defaults`` will be used (but future changes
            to either registry's defaults will not affect the other).

        Returns
        -------
        copy : `SqlRegistry`
            A new `SqlRegistry` instance with its own defaults.
        """
        if defaults is None:
            # No need to copy, because `RegistryDefaults` is immutable; we
            # effectively copy on write.
            defaults = self.defaults
        db = self._db.clone()
        result = SqlRegistry(db, defaults, self._managers.clone(db))
        result._datastore_record_classes = dict(self._datastore_record_classes)
        result.dimension_record_cache.load_from(self.dimension_record_cache)
        return result

    @property
    def dimensions(self) -> DimensionUniverse:
        """Definitions of all dimensions recognized by this `Registry`
        (`DimensionUniverse`).
        """
        return self._managers.dimensions.universe

    @property
    def defaults(self) -> RegistryDefaults:
        """Default collection search path and/or output `~CollectionType.RUN`
        collection (`~lsst.daf.butler.registry.RegistryDefaults`).

        This is an immutable struct whose components may not be set
        individually, but the entire struct can be set by assigning to this
        property.
        """
        return self._defaults

    @defaults.setter
    def defaults(self, value: RegistryDefaults) -> None:
        if value.run is not None:
            self.registerRun(value.run)
        value.finish(self)
        self._defaults = value

    def refresh(self) -> None:
        """Refresh all in-memory state by querying the database.

        This may be necessary to enable querying for entities added by other
        registry instances after this one was constructed.
        """
        self.dimension_record_cache.reset()
        with self._db.transaction():
            self._managers.refresh()

    def refresh_collection_summaries(self) -> None:
        """Refresh content of the collection summary tables in the database.

        This only cleans dataset type summaries, we may want to add cleanup of
        governor summaries later.
        """
        for dataset_type in self.queryDatasetTypes():
            self._managers.datasets.refresh_collection_summaries(dataset_type)

    def caching_context(self) -> contextlib.AbstractContextManager[None]:
        """Return context manager that enables caching.

        Returns
        -------
        manager
            A context manager that enables client-side caching.  Entering
            the context returns `None`.
        """
        return self._managers.caching_context_manager()

    @contextlib.contextmanager
    def transaction(self, *, savepoint: bool = False) -> Iterator[None]:
        """Return a context manager that represents a transaction.

        Parameters
        ----------
        savepoint : `bool`
            Whether to issue a SAVEPOINT in the database.

        Yields
        ------
        `None`
        """
        with self._db.transaction(savepoint=savepoint):
            yield

    def resetConnectionPool(self) -> None:
        """Reset SQLAlchemy connection pool for `SqlRegistry` database.

        This operation is useful when using registry with fork-based
        multiprocessing. To use registry across fork boundary one has to make
        sure that there are no currently active connections (no session or
        transaction is in progress) and connection pool is reset using this
        method. This method should be called by the child process immediately
        after the fork.
        """
        self._db._engine.dispose()

    def registerOpaqueTable(self, tableName: str, spec: ddl.TableSpec) -> None:
        """Add an opaque (to the `Registry`) table for use by a `Datastore` or
        other data repository client.

        Opaque table records can be added via `insertOpaqueData`, retrieved via
        `fetchOpaqueData`, and removed via `deleteOpaqueData`.

        Parameters
        ----------
        tableName : `str`
            Logical name of the opaque table.  This may differ from the
            actual name used in the database by a prefix and/or suffix.
        spec : `ddl.TableSpec`
            Specification for the table to be added.
        """
        self._managers.opaque.register(tableName, spec)

    @transactional
    def insertOpaqueData(self, tableName: str, *data: dict) -> None:
        """Insert records into an opaque table.

        Parameters
        ----------
        tableName : `str`
            Logical name of the opaque table.  Must match the name used in a
            previous call to `registerOpaqueTable`.
        *data
            Each additional positional argument is a dictionary that represents
            a single row to be added.
        """
        self._managers.opaque[tableName].insert(*data)

    def fetchOpaqueData(self, tableName: str, **where: Any) -> Iterator[Mapping[str, Any]]:
        """Retrieve records from an opaque table.

        Parameters
        ----------
        tableName : `str`
            Logical name of the opaque table.  Must match the name used in a
            previous call to `registerOpaqueTable`.
        **where
            Additional keyword arguments are interpreted as equality
            constraints that restrict the returned rows (combined with AND);
            keyword arguments are column names and values are the values they
            must have.

        Yields
        ------
        row : `dict`
            A dictionary representing a single result row.
        """
        yield from self._managers.opaque[tableName].fetch(**where)

    @transactional
    def deleteOpaqueData(self, tableName: str, **where: Any) -> None:
        """Remove records from an opaque table.

        Parameters
        ----------
        tableName : `str`
            Logical name of the opaque table.  Must match the name used in a
            previous call to `registerOpaqueTable`.
        **where
            Additional keyword arguments are interpreted as equality
            constraints that restrict the deleted rows (combined with AND);
            keyword arguments are column names and values are the values they
            must have.
        """
        self._managers.opaque[tableName].delete(where.keys(), where)

    def registerCollection(
        self, name: str, type: CollectionType = CollectionType.TAGGED, doc: str | None = None
    ) -> bool:
        """Add a new collection if one with the given name does not exist.

        Parameters
        ----------
        name : `str`
            The name of the collection to create.
        type : `CollectionType`
            Enum value indicating the type of collection to create.
        doc : `str`, optional
            Documentation string for the collection.

        Returns
        -------
        registered : `bool`
            Boolean indicating whether the collection was already registered
            or was created by this call.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent.
        """
        _, registered = self._managers.collections.register(name, type, doc=doc)
        return registered

    def getCollectionType(self, name: str) -> CollectionType:
        """Return an enumeration value indicating the type of the given
        collection.

        Parameters
        ----------
        name : `str`
            The name of the collection.

        Returns
        -------
        type : `CollectionType`
            Enum value indicating the type of this collection.

        Raises
        ------
        lsst.daf.butler.registry.MissingCollectionError
            Raised if no collection with the given name exists.
        """
        return self._managers.collections.find(name).type

    def get_collection_record(self, name: str) -> CollectionRecord:
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
        return self._managers.collections.find(name)

    def registerRun(self, name: str, doc: str | None = None) -> bool:
        """Add a new run if one with the given name does not exist.

        Parameters
        ----------
        name : `str`
            The name of the run to create.
        doc : `str`, optional
            Documentation string for the collection.

        Returns
        -------
        registered : `bool`
            Boolean indicating whether a new run was registered. `False`
            if it already existed.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent.
        """
        _, registered = self._managers.collections.register(name, CollectionType.RUN, doc=doc)
        return registered

    @transactional
    def removeCollection(self, name: str) -> None:
        """Remove the given collection from the registry.

        Parameters
        ----------
        name : `str`
            The name of the collection to remove.

        Raises
        ------
        lsst.daf.butler.registry.MissingCollectionError
            Raised if no collection with the given name exists.
        sqlalchemy.exc.IntegrityError
            Raised if the database rows associated with the collection are
            still referenced by some other table, such as a dataset in a
            datastore (for `~CollectionType.RUN` collections only) or a
            `~CollectionType.CHAINED` collection of which this collection is
            a child.

        Notes
        -----
        If this is a `~CollectionType.RUN` collection, all datasets and quanta
        in it will removed from the `Registry` database.  This requires that
        those datasets be removed (or at least trashed) from any datastores
        that hold them first.

        A collection may not be deleted as long as it is referenced by a
        `~CollectionType.CHAINED` collection; the ``CHAINED`` collection must
        be deleted or redefined first.
        """
        self._managers.collections.remove(name)

    def getCollectionChain(self, parent: str) -> tuple[str, ...]:
        """Return the child collections in a `~CollectionType.CHAINED`
        collection.

        Parameters
        ----------
        parent : `str`
            Name of the chained collection.  Must have already been added via
            a call to `Registry.registerCollection`.

        Returns
        -------
        children : `~collections.abc.Sequence` [ `str` ]
            An ordered sequence of collection names that are searched when the
            given chained collection is searched.

        Raises
        ------
        lsst.daf.butler.registry.MissingCollectionError
            Raised if ``parent`` does not exist in the `Registry`.
        lsst.daf.butler.registry.CollectionTypeError
            Raised if ``parent`` does not correspond to a
            `~CollectionType.CHAINED` collection.
        """
        record = self._managers.collections.find(parent)
        if record.type is not CollectionType.CHAINED:
            raise CollectionTypeError(f"Collection '{parent}' has type {record.type.name}, not CHAINED.")
        assert isinstance(record, ChainedCollectionRecord)
        return record.children

    @transactional
    def setCollectionChain(self, parent: str, children: Any, *, flatten: bool = False) -> None:
        """Define or redefine a `~CollectionType.CHAINED` collection.

        Parameters
        ----------
        parent : `str`
            Name of the chained collection.  Must have already been added via
            a call to `Registry.registerCollection`.
        children : collection expression
            An expression defining an ordered search of child collections,
            generally an iterable of `str`; see
            :ref:`daf_butler_collection_expressions` for more information.
        flatten : `bool`, optional
            If `True` (`False` is default), recursively flatten out any nested
            `~CollectionType.CHAINED` collections in ``children`` first.

        Raises
        ------
        lsst.daf.butler.registry.MissingCollectionError
            Raised when any of the given collections do not exist in the
            `Registry`.
        lsst.daf.butler.registry.CollectionTypeError
            Raised if ``parent`` does not correspond to a
            `~CollectionType.CHAINED` collection.
        CollectionCycleError
            Raised if the given collections contains a cycle.

        Notes
        -----
        If this function is called within a call to ``Butler.transaction``, it
        will hold a lock that prevents other processes from modifying the
        parent collection until the end of the transaction.  Keep these
        transactions short.
        """
        children = CollectionWildcard.from_expression(children).require_ordered()
        if flatten:
            children = self.queryCollections(children, flattenChains=True)

        self._managers.collections.update_chain(parent, list(children), allow_use_in_caching_context=True)

    def getCollectionParentChains(self, collection: str) -> set[str]:
        """Return the CHAINED collections that directly contain the given one.

        Parameters
        ----------
        collection : `str`
            Name of the collection.

        Returns
        -------
        chains : `set` of `str`
            Set of `~CollectionType.CHAINED` collection names.
        """
        return self._managers.collections.getParentChains(self._managers.collections.find(collection).key)

    def getCollectionDocumentation(self, collection: str) -> str | None:
        """Retrieve the documentation string for a collection.

        Parameters
        ----------
        collection : `str`
            Name of the collection.

        Returns
        -------
        docs : `str` or `None`
            Docstring for the collection with the given name.
        """
        return self._managers.collections.getDocumentation(self._managers.collections.find(collection).key)

    def setCollectionDocumentation(self, collection: str, doc: str | None) -> None:
        """Set the documentation string for a collection.

        Parameters
        ----------
        collection : `str`
            Name of the collection.
        doc : `str` or `None`
            Docstring for the collection with the given name; will replace any
            existing docstring.  Passing `None` will remove any existing
            docstring.
        """
        self._managers.collections.setDocumentation(self._managers.collections.find(collection).key, doc)

    def getCollectionSummary(self, collection: str) -> CollectionSummary:
        """Return a summary for the given collection.

        Parameters
        ----------
        collection : `str`
            Name of the collection for which a summary is to be retrieved.

        Returns
        -------
        summary : `~lsst.daf.butler.registry.CollectionSummary`
            Summary of the dataset types and governor dimension values in
            this collection.
        """
        record = self._managers.collections.find(collection)
        return self._managers.datasets.getCollectionSummary(record)

    def registerDatasetType(self, datasetType: DatasetType) -> bool:
        """Add a new `DatasetType` to the Registry.

        It is not an error to register the same `DatasetType` twice.

        Parameters
        ----------
        datasetType : `DatasetType`
            The `DatasetType` to be added.

        Returns
        -------
        inserted : `bool`
            `True` if ``datasetType`` was inserted, `False` if an identical
            existing `DatasetType` was found.  Note that in either case the
            DatasetType is guaranteed to be defined in the Registry
            consistently with the given definition.

        Raises
        ------
        ValueError
            Raised if the dimensions or storage class are invalid.
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if this `DatasetType` is already registered with a different
            definition.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent.
        """
        return self._managers.datasets.register_dataset_type(datasetType)

    def removeDatasetType(self, name: str | tuple[str, ...]) -> None:
        """Remove the named `DatasetType` from the registry.

        .. warning::

            Registry implementations can cache the dataset type definitions.
            This means that deleting the dataset type definition may result in
            unexpected behavior from other butler processes that are active
            that have not seen the deletion.

        Parameters
        ----------
        name : `str` or `tuple` [`str`]
            Name of the type to be removed or tuple containing a list of type
            names to be removed. Wildcards are allowed.

        Raises
        ------
        lsst.daf.butler.registry.OrphanedRecordError
            Raised if an attempt is made to remove the dataset type definition
            when there are already datasets associated with it.

        Notes
        -----
        If the dataset type is not registered the method will return without
        action.
        """
        for datasetTypeExpression in ensure_iterable(name):
            # Catch any warnings from the caller specifying a component
            # dataset type. This will result in an error later but the
            # warning could be confusing when the caller is not querying
            # anything.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                datasetTypes = list(self.queryDatasetTypes(datasetTypeExpression))
            if not datasetTypes:
                _LOG.info("Dataset type %r not defined", datasetTypeExpression)
            else:
                for datasetType in datasetTypes:
                    self._managers.datasets.remove_dataset_type(datasetType.name)
                    _LOG.info("Removed dataset type %r", datasetType.name)

    def getDatasetType(self, name: str) -> DatasetType:
        """Get the `DatasetType`.

        Parameters
        ----------
        name : `str`
            Name of the type.

        Returns
        -------
        type : `DatasetType`
            The `DatasetType` associated with the given name.

        Raises
        ------
        lsst.daf.butler.registry.MissingDatasetTypeError
            Raised if the requested dataset type has not been registered.

        Notes
        -----
        This method handles component dataset types automatically, though most
        other registry operations do not.
        """
        parent_name, component = DatasetType.splitDatasetTypeName(name)
        parent_dataset_type = self._managers.datasets.get_dataset_type(parent_name)
        if component is None:
            return parent_dataset_type
        else:
            return parent_dataset_type.makeComponentDatasetType(component)

    def supportsIdGenerationMode(self, mode: DatasetIdGenEnum) -> bool:
        """Test whether the given dataset ID generation mode is supported by
        `insertDatasets`.

        Parameters
        ----------
        mode : `DatasetIdGenEnum`
            Enum value for the mode to test.

        Returns
        -------
        supported : `bool`
            Whether the given mode is supported.
        """
        return True

    def findDataset(
        self,
        datasetType: DatasetType | str,
        dataId: DataId | None = None,
        *,
        collections: CollectionArgType | None = None,
        timespan: Timespan | None = None,
        datastore_records: bool = False,
        **kwargs: Any,
    ) -> DatasetRef | None:
        """Find a dataset given its `DatasetType` and data ID.

        This can be used to obtain a `DatasetRef` that permits the dataset to
        be read from a `Datastore`. If the dataset is a component and can not
        be found using the provided dataset type, a dataset ref for the parent
        will be returned instead but with the correct dataset type.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.  If this is a `DatasetType`
            instance, its storage class will be respected and propagated to
            the output, even if it differs from the dataset type definition
            in the registry, as long as the storage classes are convertible.
        dataId : `dict` or `DataCoordinate`, optional
            A `dict`-like object containing the `Dimension` links that identify
            the dataset within a collection.
        collections : collection expression, optional
            An expression that fully or partially identifies the collections to
            search for the dataset; see
            :ref:`daf_butler_collection_expressions` for more information.
            Defaults to ``self.defaults.collections``.
        timespan : `Timespan`, optional
            A timespan that the validity range of the dataset must overlap.
            If not provided, any `~CollectionType.CALIBRATION` collections
            matched by the ``collections`` argument will not be searched.
        datastore_records : `bool`, optional
            Whether to attach datastore records to the `DatasetRef`.
        **kwargs
            Additional keyword arguments passed to
            `DataCoordinate.standardize` to convert ``dataId`` to a true
            `DataCoordinate` or augment an existing one.

        Returns
        -------
        ref : `DatasetRef`
            A reference to the dataset, or `None` if no matching Dataset
            was found.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``collections`` is `None` and
            ``self.defaults.collections`` is `None`.
        LookupError
            Raised if one or more data ID keys are missing.
        lsst.daf.butler.registry.MissingDatasetTypeError
            Raised if the dataset type does not exist.
        lsst.daf.butler.registry.MissingCollectionError
            Raised if any of ``collections`` does not exist in the registry.

        Notes
        -----
        This method simply returns `None` and does not raise an exception even
        when the set of collections searched is intrinsically incompatible with
        the dataset type, e.g. if ``datasetType.isCalibration() is False``, but
        only `~CollectionType.CALIBRATION` collections are being searched.
        This may make it harder to debug some lookup failures, but the behavior
        is intentional; we consider it more important that failed searches are
        reported consistently, regardless of the reason, and that adding
        additional collections that do not contain a match to the search path
        never changes the behavior.

        This method handles component dataset types automatically, though most
        other registry operations do not.
        """
        if collections is None:
            if not self.defaults.collections:
                raise NoDefaultCollectionError(
                    "No collections provided to findDataset, and no defaults from registry construction."
                )
            collections = self.defaults.collections
        backend = queries.SqlQueryBackend(self._db, self._managers, self.dimension_record_cache)
        with backend.caching_context():
            collection_wildcard = CollectionWildcard.from_expression(collections, require_ordered=True)
            if collection_wildcard.empty():
                return None
            matched_collections = backend.resolve_collection_wildcard(collection_wildcard)
            resolved_dataset_type = backend.resolve_single_dataset_type_wildcard(datasetType)
            dataId = DataCoordinate.standardize(
                dataId,
                dimensions=resolved_dataset_type.dimensions,
                universe=self.dimensions,
                defaults=self.defaults.dataId,
                **kwargs,
            )
            governor_constraints = {name: {cast(str, dataId[name])} for name in dataId.dimensions.governors}
            (filtered_collections,) = backend.filter_dataset_collections(
                [resolved_dataset_type],
                matched_collections,
                governor_constraints=governor_constraints,
            ).values()
            if not filtered_collections:
                return None
            if timespan is None:
                filtered_collections = [
                    collection_record
                    for collection_record in filtered_collections
                    if collection_record.type is not CollectionType.CALIBRATION
                ]
            if filtered_collections:
                requested_columns = {"dataset_id", "run", "collection"}
                with backend.context() as context:
                    predicate = context.make_data_coordinate_predicate(
                        dataId.subset(resolved_dataset_type.dimensions), full=False
                    )
                    if timespan is not None:
                        requested_columns.add("timespan")
                        predicate = predicate.logical_and(
                            context.make_timespan_overlap_predicate(
                                DatasetColumnTag(resolved_dataset_type.name, "timespan"), timespan
                            )
                        )
                    relation = backend.make_dataset_query_relation(
                        resolved_dataset_type, filtered_collections, requested_columns, context
                    ).with_rows_satisfying(predicate)
                    rows = list(context.fetch_iterable(relation))
            else:
                rows = []
            if not rows:
                return None
            elif len(rows) == 1:
                best_row = rows[0]
            else:
                rank_by_collection_key = {record.key: n for n, record in enumerate(filtered_collections)}
                collection_tag = DatasetColumnTag(resolved_dataset_type.name, "collection")
                row_iter = iter(rows)
                best_row = next(row_iter)
                best_rank = rank_by_collection_key[best_row[collection_tag]]
                have_tie = False
                for row in row_iter:
                    if (rank := rank_by_collection_key[row[collection_tag]]) < best_rank:
                        best_row = row
                        best_rank = rank
                        have_tie = False
                    elif rank == best_rank:
                        have_tie = True
                        assert timespan is not None, "Rank ties should be impossible given DB constraints."
                if have_tie:
                    raise CalibrationLookupError(
                        f"Ambiguous calibration lookup for {resolved_dataset_type.name} in collections "
                        f"{collection_wildcard.strings} with timespan {timespan}."
                    )
            reader = queries.DatasetRefReader(
                resolved_dataset_type,
                translate_collection=lambda k: self._managers.collections[k].name,
            )
            ref = reader.read(best_row, data_id=dataId)
            if datastore_records:
                ref = self.get_datastore_records(ref)

            return ref

    @transactional
    def insertDatasets(
        self,
        datasetType: DatasetType | str,
        dataIds: Iterable[DataId],
        run: str | None = None,
        expand: bool = True,
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
    ) -> list[DatasetRef]:
        """Insert one or more datasets into the `Registry`.

        This always adds new datasets; to associate existing datasets with
        a new collection, use ``associate``.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.
        dataIds : `~collections.abc.Iterable` of `dict` or `DataCoordinate`
            Dimension-based identifiers for the new datasets.
        run : `str`, optional
            The name of the run that produced the datasets.  Defaults to
            ``self.defaults.run``.
        expand : `bool`, optional
            If `True` (default), expand data IDs as they are inserted.  This is
            necessary in general to allow datastore to generate file templates,
            but it may be disabled if the caller can guarantee this is
            unnecessary.
        idGenerationMode : `DatasetIdGenEnum`, optional
            Specifies option for generating dataset IDs. By default unique IDs
            are generated for each inserted dataset.

        Returns
        -------
        refs : `list` of `DatasetRef`
            Resolved `DatasetRef` instances for all given data IDs (in the same
            order).

        Raises
        ------
        lsst.daf.butler.registry.DatasetTypeError
            Raised if ``datasetType`` is not known to registry.
        lsst.daf.butler.registry.CollectionTypeError
            Raised if ``run`` collection type is not `~CollectionType.RUN`.
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``run`` is `None` and ``self.defaults.run`` is `None`.
        lsst.daf.butler.registry.ConflictingDefinitionError
            If a dataset with the same dataset type and data ID as one of those
            given already exists in ``run``.
        lsst.daf.butler.registry.MissingCollectionError
            Raised if ``run`` does not exist in the registry.
        """
        datasetType = self._managers.datasets.conform_exact_dataset_type(datasetType)
        if run is None:
            if self.defaults.run is None:
                raise NoDefaultCollectionError(
                    "No run provided to insertDatasets, and no default from registry construction."
                )
            run = self.defaults.run
        runRecord = self._managers.collections.find(run)
        if runRecord.type is not CollectionType.RUN:
            raise CollectionTypeError(
                f"Given collection is of type {runRecord.type.name}; RUN collection required."
            )
        assert isinstance(runRecord, RunRecord)
        progress = Progress("daf.butler.Registry.insertDatasets", level=logging.DEBUG)
        if expand:
            expandedDataIds = [
                self.expandDataId(dataId, dimensions=datasetType.dimensions)
                for dataId in progress.wrap(dataIds, f"Expanding {datasetType.name} data IDs")
            ]
        else:
            expandedDataIds = [
                DataCoordinate.standardize(dataId, dimensions=datasetType.dimensions) for dataId in dataIds
            ]
        try:
            refs = list(
                self._managers.datasets.insert(datasetType.name, runRecord, expandedDataIds, idGenerationMode)
            )
            if self._managers.obscore:
                self._managers.obscore.add_datasets(refs)
        except sqlalchemy.exc.IntegrityError as err:
            raise ConflictingDefinitionError(
                "A database constraint failure was triggered by inserting "
                f"one or more datasets of type {datasetType} into "
                f"collection '{run}'. "
                "This probably means a dataset with the same data ID "
                "and dataset type already exists, but it may also mean a "
                "dimension row is missing."
            ) from err
        return refs

    @transactional
    def _importDatasets(
        self,
        datasets: Iterable[DatasetRef],
        expand: bool = True,
    ) -> list[DatasetRef]:
        """Import one or more datasets into the `Registry`.

        Difference from `insertDatasets` method is that this method accepts
        `DatasetRef` instances which should already be resolved and have a
        dataset ID. If registry supports globally-unique dataset IDs (e.g.
        `uuid.UUID`) then datasets which already exist in the registry will be
        ignored if imported again.

        Parameters
        ----------
        datasets : `~collections.abc.Iterable` of `DatasetRef`
            Datasets to be inserted. All `DatasetRef` instances must have
            identical ``datasetType`` and ``run`` attributes. ``run``
            attribute can be `None` and defaults to ``self.defaults.run``.
            Datasets can specify ``id`` attribute which will be used for
            inserted datasets. All dataset IDs must have the same type
            (`int` or `uuid.UUID`), if type of dataset IDs does not match
            configured backend then IDs will be ignored and new IDs will be
            generated by backend.
        expand : `bool`, optional
            If `True` (default), expand data IDs as they are inserted.  This is
            necessary in general, but it may be disabled if the caller can
            guarantee this is unnecessary.

        Returns
        -------
        refs : `list` of `DatasetRef`
            Resolved `DatasetRef` instances for all given data IDs (in the same
            order). If any of ``datasets`` has an ID which already exists in
            the database then it will not be inserted or updated, but a
            resolved `DatasetRef` will be returned for it in any case.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``run`` is `None` and ``self.defaults.run`` is `None`.
        lsst.daf.butler.registry.DatasetTypeError
            Raised if datasets correspond to more than one dataset type or
            dataset type is not known to registry.
        lsst.daf.butler.registry.ConflictingDefinitionError
            If a dataset with the same dataset type and data ID as one of those
            given already exists in ``run``.
        lsst.daf.butler.registry.MissingCollectionError
            Raised if ``run`` does not exist in the registry.

        Notes
        -----
        This method is considered package-private and internal to Butler
        implementation. Clients outside daf_butler package should not use this
        method.
        """
        datasets = list(datasets)
        if not datasets:
            # nothing to do
            return []

        # find dataset type
        datasetTypes = {dataset.datasetType for dataset in datasets}
        if len(datasetTypes) != 1:
            raise DatasetTypeError(f"Multiple dataset types in input datasets: {datasetTypes}")
        datasetType = datasetTypes.pop()

        # find run name
        runs = {dataset.run for dataset in datasets}
        if len(runs) != 1:
            raise ValueError(f"Multiple run names in input datasets: {runs}")
        run = runs.pop()

        runRecord = self._managers.collections.find(run)
        if runRecord.type is not CollectionType.RUN:
            raise CollectionTypeError(
                f"Given collection '{runRecord.name}' is of type {runRecord.type.name};"
                " RUN collection required."
            )
        assert isinstance(runRecord, RunRecord)

        progress = Progress("daf.butler.Registry.importDatasets", level=logging.DEBUG)
        if expand:
            data_ids = {
                dataset.id: self.expandDataId(dataset.dataId, dimensions=datasetType.dimensions)
                for dataset in progress.wrap(datasets, f"Expanding {datasetType.name} data IDs")
            }
        else:
            data_ids = {dataset.id: dataset.dataId for dataset in datasets}

        try:
            refs = list(self._managers.datasets.import_(datasetType, runRecord, data_ids))
            if self._managers.obscore:
                self._managers.obscore.add_datasets(refs)
        except sqlalchemy.exc.IntegrityError as err:
            raise ConflictingDefinitionError(
                "A database constraint failure was triggered by inserting "
                f"one or more datasets of type {datasetType} into "
                f"collection '{run}'. "
                "This probably means a dataset with the same data ID "
                "and dataset type already exists, but it may also mean a "
                "dimension row is missing."
            ) from err
        # Check that imported dataset IDs match the input
        for imported_ref, input_ref in zip(refs, datasets, strict=True):
            if imported_ref.id != input_ref.id:
                raise RegistryConsistencyError(
                    "Imported dataset ID differs from input dataset ID, "
                    f"input ref: {input_ref}, imported ref: {imported_ref}"
                )
        return refs

    def getDataset(self, id: DatasetId) -> DatasetRef | None:
        """Retrieve a Dataset entry.

        Parameters
        ----------
        id : `DatasetId`
            The unique identifier for the dataset.

        Returns
        -------
        ref : `DatasetRef` or `None`
            A ref to the Dataset, or `None` if no matching Dataset
            was found.
        """
        return self._managers.datasets.getDatasetRef(id)

    @transactional
    def removeDatasets(self, refs: Iterable[DatasetRef]) -> None:
        """Remove datasets from the Registry.

        The datasets will be removed unconditionally from all collections.
        `Datastore` records will *not* be deleted; the caller is responsible
        for ensuring that the dataset has already been removed from all
        Datastores.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [`DatasetRef`]
            References to the datasets to be removed.  Should be considered
            invalidated upon return.

        Raises
        ------
        lsst.daf.butler.registry.OrphanedRecordError
            Raised if any dataset is still present in any `Datastore`.
        """
        try:
            self._managers.datasets.delete(refs)
        except sqlalchemy.exc.IntegrityError as err:
            raise OrphanedRecordError(
                "One or more datasets is still present in one or more Datastores."
            ) from err

    @transactional
    def associate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        """Add existing datasets to a `~CollectionType.TAGGED` collection.

        If a DatasetRef with the same exact ID is already in a collection
        nothing is changed. If a `DatasetRef` with the same `DatasetType` and
        data ID but with different ID exists in the collection,
        `~lsst.daf.butler.registry.ConflictingDefinitionError` is raised.

        Parameters
        ----------
        collection : `str`
            Indicates the collection the datasets should be associated with.
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            An iterable of resolved `DatasetRef` instances that already exist
            in this `Registry`.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            If a Dataset with the given `DatasetRef` already exists in the
            given collection.
        lsst.daf.butler.registry.MissingCollectionError
            Raised if ``collection`` does not exist in the registry.
        lsst.daf.butler.registry.CollectionTypeError
            Raise adding new datasets to the given ``collection`` is not
            allowed.
        """
        progress = Progress("lsst.daf.butler.Registry.associate", level=logging.DEBUG)
        collectionRecord = self._managers.collections.find(collection)
        for datasetType, refsForType in progress.iter_item_chunks(
            DatasetRef.iter_by_type(refs), desc="Associating datasets by type"
        ):
            try:
                self._managers.datasets.associate(datasetType, collectionRecord, refsForType)
                if self._managers.obscore:
                    # If a TAGGED collection is being monitored by ObsCore
                    # manager then we may need to save the dataset.
                    self._managers.obscore.associate(refsForType, collectionRecord)
            except sqlalchemy.exc.IntegrityError as err:
                raise ConflictingDefinitionError(
                    f"Constraint violation while associating dataset of type {datasetType.name} with "
                    f"collection {collection}.  This probably means that one or more datasets with the same "
                    "dataset type and data ID already exist in the collection, but it may also indicate "
                    "that the datasets do not exist."
                ) from err

    @transactional
    def disassociate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        """Remove existing datasets from a `~CollectionType.TAGGED` collection.

        ``collection`` and ``ref`` combinations that are not currently
        associated are silently ignored.

        Parameters
        ----------
        collection : `str`
            The collection the datasets should no longer be associated with.
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            An iterable of resolved `DatasetRef` instances that already exist
            in this `Registry`.

        Raises
        ------
        lsst.daf.butler.AmbiguousDatasetError
            Raised if any of the given dataset references is unresolved.
        lsst.daf.butler.registry.MissingCollectionError
            Raised if ``collection`` does not exist in the registry.
        lsst.daf.butler.registry.CollectionTypeError
            Raise adding new datasets to the given ``collection`` is not
            allowed.
        """
        progress = Progress("lsst.daf.butler.Registry.disassociate", level=logging.DEBUG)
        collectionRecord = self._managers.collections.find(collection)
        for datasetType, refsForType in progress.iter_item_chunks(
            DatasetRef.iter_by_type(refs), desc="Disassociating datasets by type"
        ):
            self._managers.datasets.disassociate(datasetType, collectionRecord, refsForType)
            if self._managers.obscore:
                self._managers.obscore.disassociate(refsForType, collectionRecord)

    @transactional
    def certify(self, collection: str, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        """Associate one or more datasets with a calibration collection and a
        validity range within it.

        Parameters
        ----------
        collection : `str`
            The name of an already-registered `~CollectionType.CALIBRATION`
            collection.
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            Datasets to be associated.
        timespan : `Timespan`
            The validity range for these datasets within the collection.

        Raises
        ------
        lsst.daf.butler.AmbiguousDatasetError
            Raised if any of the given `DatasetRef` instances is unresolved.
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if the collection already contains a different dataset with
            the same `DatasetType` and data ID and an overlapping validity
            range.
        DatasetTypeError
            Raised if ``ref.datasetType.isCalibration() is False`` for any ref.
        CollectionTypeError
            Raised if
            ``collection.type is not CollectionType.CALIBRATION``.
        """
        progress = Progress("lsst.daf.butler.Registry.certify", level=logging.DEBUG)
        collectionRecord = self._managers.collections.find(collection)
        for datasetType, refsForType in progress.iter_item_chunks(
            DatasetRef.iter_by_type(refs), desc="Certifying datasets by type"
        ):
            self._managers.datasets.certify(
                datasetType,
                collectionRecord,
                refsForType,
                timespan,
                context=queries.SqlQueryContext(self._db, self._managers.column_types),
            )

    @transactional
    def decertify(
        self,
        collection: str,
        datasetType: str | DatasetType,
        timespan: Timespan,
        *,
        dataIds: Iterable[DataId] | None = None,
    ) -> None:
        """Remove or adjust datasets to clear a validity range within a
        calibration collection.

        Parameters
        ----------
        collection : `str`
            The name of an already-registered `~CollectionType.CALIBRATION`
            collection.
        datasetType : `str` or `DatasetType`
            Name or `DatasetType` instance for the datasets to be decertified.
        timespan : `Timespan`, optional
            The validity range to remove datasets from within the collection.
            Datasets that overlap this range but are not contained by it will
            have their validity ranges adjusted to not overlap it, which may
            split a single dataset validity range into two.
        dataIds : iterable [`dict` or `DataCoordinate`], optional
            Data IDs that should be decertified within the given validity range
            If `None`, all data IDs for ``self.datasetType`` will be
            decertified.

        Raises
        ------
        DatasetTypeError
            Raised if ``datasetType.isCalibration() is False``.
        CollectionTypeError
            Raised if
            ``collection.type is not CollectionType.CALIBRATION``.
        """
        collectionRecord = self._managers.collections.find(collection)
        if isinstance(datasetType, str):
            datasetType = self.getDatasetType(datasetType)
        standardizedDataIds = None
        if dataIds is not None:
            standardizedDataIds = [
                DataCoordinate.standardize(d, dimensions=datasetType.dimensions) for d in dataIds
            ]
        self._managers.datasets.decertify(
            datasetType,
            collectionRecord,
            timespan,
            data_ids=standardizedDataIds,
            context=queries.SqlQueryContext(self._db, self._managers.column_types),
        )

    def getDatastoreBridgeManager(self) -> DatastoreRegistryBridgeManager:
        """Return an object that allows a new `Datastore` instance to
        communicate with this `Registry`.

        Returns
        -------
        manager : `~.interfaces.DatastoreRegistryBridgeManager`
            Object that mediates communication between this `Registry` and its
            associated datastores.
        """
        return self._managers.datastores

    def getDatasetLocations(self, ref: DatasetRef) -> Iterable[str]:
        """Retrieve datastore locations for a given dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            A reference to the dataset for which to retrieve storage
            information.

        Returns
        -------
        datastores : `~collections.abc.Iterable` [ `str` ]
            All the matching datastores holding this dataset.

        Raises
        ------
        lsst.daf.butler.AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        """
        return self._managers.datastores.findDatastores(ref)

    def expandDataId(
        self,
        dataId: DataId | None = None,
        *,
        dimensions: Iterable[str] | DimensionGroup | None = None,
        records: Mapping[str, DimensionRecord | None] | None = None,
        withDefaults: bool = True,
        **kwargs: Any,
    ) -> DataCoordinate:
        """Expand a dimension-based data ID to include additional information.

        Parameters
        ----------
        dataId : `DataCoordinate` or `dict`, optional
            Data ID to be expanded; augmented and overridden by ``kwargs``.
        dimensions : `~collections.abc.Iterable` [ `str` ], \
                `DimensionGroup`, optional
            The dimensions to be identified by the new `DataCoordinate`.
            If not provided, will be inferred from the keys of ``dataId`` and
            ``**kwargs``, and ``universe`` must be provided unless ``dataId``
            is already a `DataCoordinate`.
        records : `~collections.abc.Mapping` [`str`, `DimensionRecord`], \
                optional
            Dimension record data to use before querying the database for that
            data, keyed by element name.
        withDefaults : `bool`, optional
            Utilize ``self.defaults.dataId`` to fill in missing governor
            dimension key-value pairs.  Defaults to `True` (i.e. defaults are
            used).
        **kwargs
            Additional keywords are treated like additional key-value pairs for
            ``dataId``, extending and overriding.

        Returns
        -------
        expanded : `DataCoordinate`
            A data ID that includes full metadata for all of the dimensions it
            identifies, i.e. guarantees that ``expanded.hasRecords()`` and
            ``expanded.hasFull()`` both return `True`.

        Raises
        ------
        lsst.daf.butler.registry.DataIdError
            Raised when ``dataId`` or keyword arguments specify unknown
            dimensions or values, or when a resulting data ID contains
            contradictory key-value pairs, according to dimension
            relationships.

        Notes
        -----
        This method cannot be relied upon to reject invalid data ID values
        for dimensions that do actually not have any record columns.  For
        efficiency reasons the records for these dimensions (which have only
        dimension key values that are given by the caller) may be constructed
        directly rather than obtained from the registry database.
        """
        if not withDefaults:
            defaults = None
        else:
            defaults = self.defaults.dataId
        standardized = DataCoordinate.standardize(
            dataId,
            dimensions=dimensions,
            universe=self.dimensions,
            defaults=defaults,
            **kwargs,
        )
        if standardized.hasRecords():
            return standardized
        if records is None:
            records = {}
        else:
            records = dict(records)
        if isinstance(dataId, DataCoordinate) and dataId.hasRecords():
            for element_name in dataId.dimensions.elements:
                records[element_name] = dataId.records[element_name]
        keys = dict(standardized.mapping)
        for element_name in standardized.dimensions.lookup_order:
            element = self.dimensions[element_name]
            record = records.get(element_name, ...)  # Use ... to mean not found; None might mean NULL
            if record is ...:
                if element_name in self.dimensions.dimensions.names and keys.get(element_name) is None:
                    if element_name in standardized.dimensions.required:
                        raise DimensionNameError(
                            f"No value or null value for required dimension {element_name}."
                        )
                    keys[element_name] = None
                    record = None
                else:
                    record = self._managers.dimensions.fetch_one(
                        element_name,
                        DataCoordinate.standardize(keys, dimensions=element.minimal_group),
                        self.dimension_record_cache,
                    )
                records[element_name] = record
            if record is not None:
                for d in element.implied:
                    value = getattr(record, d.name)
                    if keys.setdefault(d.name, value) != value:
                        raise InconsistentDataIdError(
                            f"Data ID {standardized} has {d.name}={keys[d.name]!r}, "
                            f"but {element_name} implies {d.name}={value!r}."
                        )
            else:
                if element_name in standardized.dimensions.required:
                    raise DataIdValueError(
                        f"Could not fetch record for required dimension {element.name} via keys {keys}."
                    )
                if element.defines_relationships:
                    raise InconsistentDataIdError(
                        f"Could not fetch record for element {element_name} via keys {keys}, "
                        "but it is marked as defining relationships; this means one or more dimensions are "
                        "have inconsistent values.",
                    )
                for d in element.implied:
                    keys.setdefault(d.name, None)
                    records.setdefault(d.name, None)
        return DataCoordinate.standardize(keys, dimensions=standardized.dimensions).expanded(records=records)

    def insertDimensionData(
        self,
        element: DimensionElement | str,
        *data: Mapping[str, Any] | DimensionRecord,
        conform: bool = True,
        replace: bool = False,
        skip_existing: bool = False,
    ) -> None:
        """Insert one or more dimension records into the database.

        Parameters
        ----------
        element : `DimensionElement` or `str`
            The `DimensionElement` or name thereof that identifies the table
            records will be inserted into.
        *data : `dict` or `DimensionRecord`
            One or more records to insert.
        conform : `bool`, optional
            If `False` (`True` is default) perform no checking or conversions,
            and assume that ``element`` is a `DimensionElement` instance and
            ``data`` is a one or more `DimensionRecord` instances of the
            appropriate subclass.
        replace : `bool`, optional
            If `True` (`False` is default), replace existing records in the
            database if there is a conflict.
        skip_existing : `bool`, optional
            If `True` (`False` is default), skip insertion if a record with
            the same primary key values already exists.  Unlike
            `syncDimensionData`, this will not detect when the given record
            differs from what is in the database, and should not be used when
            this is a concern.
        """
        if isinstance(element, str):
            element = self.dimensions[element]
        if conform:
            records = [
                row if isinstance(row, DimensionRecord) else element.RecordClass(**row) for row in data
            ]
        else:
            # Ignore typing since caller said to trust them with conform=False.
            records = data  # type: ignore
        if element.name in self.dimension_record_cache:
            self.dimension_record_cache.reset()
        self._managers.dimensions.insert(
            element,
            *records,
            replace=replace,
            skip_existing=skip_existing,
        )

    def syncDimensionData(
        self,
        element: DimensionElement | str,
        row: Mapping[str, Any] | DimensionRecord,
        conform: bool = True,
        update: bool = False,
    ) -> bool | dict[str, Any]:
        """Synchronize the given dimension record with the database, inserting
        if it does not already exist and comparing values if it does.

        Parameters
        ----------
        element : `DimensionElement` or `str`
            The `DimensionElement` or name thereof that identifies the table
            records will be inserted into.
        row : `dict` or `DimensionRecord`
            The record to insert.
        conform : `bool`, optional
            If `False` (`True` is default) perform no checking or conversions,
            and assume that ``element`` is a `DimensionElement` instance and
            ``data`` is a `DimensionRecord` instances of the appropriate
            subclass.
        update : `bool`, optional
            If `True` (`False` is default), update the existing record in the
            database if there is a conflict.

        Returns
        -------
        inserted_or_updated : `bool` or `dict`
            `True` if a new row was inserted, `False` if no changes were
            needed, or a `dict` mapping updated column names to their old
            values if an update was performed (only possible if
            ``update=True``).

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if the record exists in the database (according to primary
            key lookup) but is inconsistent with the given one.
        """
        if conform:
            if isinstance(element, str):
                element = self.dimensions[element]
            record = row if isinstance(row, DimensionRecord) else element.RecordClass(**row)
        else:
            # Ignore typing since caller said to trust them with conform=False.
            record = row  # type: ignore
        if record.definition.name in self.dimension_record_cache:
            self.dimension_record_cache.reset()
        return self._managers.dimensions.sync(record, update=update)

    def queryDatasetTypes(
        self,
        expression: Any = ...,
        *,
        missing: list[str] | None = None,
    ) -> Iterable[DatasetType]:
        """Iterate over the dataset types whose names match an expression.

        Parameters
        ----------
        expression : dataset type expression, optional
            An expression that fully or partially identifies the dataset types
            to return, such as a `str`, `re.Pattern`, or iterable thereof.
            ``...`` can be used to return all dataset types, and is the
            default. See :ref:`daf_butler_dataset_type_expressions` for more
            information.
        missing : `list` of `str`, optional
            String dataset type names that were explicitly given (i.e. not
            regular expression patterns) but not found will be appended to this
            list, if it is provided.

        Returns
        -------
        dataset_types : `~collections.abc.Iterable` [ `DatasetType`]
            An `~collections.abc.Iterable` of `DatasetType` instances whose
            names match ``expression``.

        Raises
        ------
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``expression`` is invalid.
        """
        wildcard = DatasetTypeWildcard.from_expression(expression)
        return self._managers.datasets.resolve_wildcard(wildcard, missing=missing)

    def queryCollections(
        self,
        expression: Any = ...,
        datasetType: DatasetType | None = None,
        collectionTypes: Iterable[CollectionType] | CollectionType = CollectionType.all(),
        flattenChains: bool = False,
        includeChains: bool | None = None,
    ) -> Sequence[str]:
        """Iterate over the collections whose names match an expression.

        Parameters
        ----------
        expression : collection expression, optional
            An expression that identifies the collections to return, such as
            a `str` (for full matches or partial matches via globs),
            `re.Pattern` (for partial matches), or iterable thereof.  ``...``
            can be used to return all collections, and is the default.
            See :ref:`daf_butler_collection_expressions` for more information.
        datasetType : `DatasetType`, optional
            If provided, only yield collections that may contain datasets of
            this type.  This is a conservative approximation in general; it may
            yield collections that do not have any such datasets.
        collectionTypes : `~collections.abc.Set` [`CollectionType`] or \
            `CollectionType`, optional
            If provided, only yield collections of these types.
        flattenChains : `bool`, optional
            If `True` (`False` is default), recursively yield the child
            collections of matching `~CollectionType.CHAINED` collections.
        includeChains : `bool`, optional
            If `True`, yield records for matching `~CollectionType.CHAINED`
            collections.  Default is the opposite of ``flattenChains``: include
            either CHAINED collections or their children, but not both.

        Returns
        -------
        collections : `~collections.abc.Sequence` [ `str` ]
            The names of collections that match ``expression``.

        Raises
        ------
        lsst.daf.butler.registry.CollectionExpressionError
            Raised when ``expression`` is invalid.

        Notes
        -----
        The order in which collections are returned is unspecified, except that
        the children of a `~CollectionType.CHAINED` collection are guaranteed
        to be in the order in which they are searched.  When multiple parent
        `~CollectionType.CHAINED` collections match the same criteria, the
        order in which the two lists appear is unspecified, and the lists of
        children may be incomplete if a child has multiple parents.
        """
        # Right now the datasetTypes argument is completely ignored, but that
        # is consistent with its [lack of] guarantees.  DM-24939 or a follow-up
        # ticket will take care of that.
        if datasetType is not None:
            warnings.warn(
                "The datasetType parameter should no longer be used. It has"
                " never had any effect. Will be removed after v28",
                FutureWarning,
            )
        try:
            wildcard = CollectionWildcard.from_expression(expression)
        except TypeError as exc:
            raise CollectionExpressionError(f"Invalid collection expression '{expression}'") from exc
        collectionTypes = ensure_iterable(collectionTypes)
        return [
            record.name
            for record in self._managers.collections.resolve_wildcard(
                wildcard,
                collection_types=frozenset(collectionTypes),
                flatten_chains=flattenChains,
                include_chains=includeChains,
            )
        ]

    def _makeQueryBuilder(
        self,
        summary: queries.QuerySummary,
        doomed_by: Iterable[str] = (),
    ) -> queries.QueryBuilder:
        """Return a `QueryBuilder` instance capable of constructing and
        managing more complex queries than those obtainable via `Registry`
        interfaces.

        This is an advanced interface; downstream code should prefer
        `Registry.queryDataIds` and `Registry.queryDatasets` whenever those
        are sufficient.

        Parameters
        ----------
        summary : `queries.QuerySummary`
            Object describing and categorizing the full set of dimensions that
            will be included in the query.
        doomed_by : `~collections.abc.Iterable` of `str`, optional
            A list of diagnostic messages that indicate why the query is going
            to yield no results and should not even be executed.  If an empty
            container (default) the query will be executed unless other code
            determines that it is doomed.

        Returns
        -------
        builder : `queries.QueryBuilder`
            Object that can be used to construct and perform advanced queries.
        """
        doomed_by = list(doomed_by)
        backend = queries.SqlQueryBackend(self._db, self._managers, self.dimension_record_cache)
        context = backend.context()
        relation: Relation | None = None
        if doomed_by:
            relation = LeafRelation.make_doomed(context.sql_engine, set(), doomed_by)
        return queries.QueryBuilder(
            summary,
            backend=backend,
            context=context,
            relation=relation,
        )

    def _standardize_query_data_id_args(
        self, data_id: DataId | None, *, doomed_by: list[str], **kwargs: Any
    ) -> DataCoordinate:
        """Preprocess the data ID arguments passed to query* methods.

        Parameters
        ----------
        data_id : `DataId` or `None`
            Data ID that constrains the query results.
        doomed_by : `list` [ `str` ]
            List to append messages indicating why the query is doomed to
            yield no results.
        **kwargs
            Additional data ID key-value pairs, extending and overriding
            ``data_id``.

        Returns
        -------
        data_id : `DataCoordinate`
            Standardized data ID.  Will be fully expanded unless expansion
            fails, in which case a message will be appended to ``doomed_by``
            on return.
        """
        try:
            return self.expandDataId(data_id, **kwargs)
        except DataIdValueError as err:
            doomed_by.append(str(err))
        return DataCoordinate.standardize(
            data_id, **kwargs, universe=self.dimensions, defaults=self.defaults.dataId
        )

    def _standardize_query_dataset_args(
        self,
        datasets: Any,
        collections: CollectionArgType | None,
        mode: Literal["find_first"] | Literal["find_all"] | Literal["constrain"] = "constrain",
        *,
        doomed_by: list[str],
    ) -> tuple[list[DatasetType], CollectionWildcard | None]:
        """Preprocess dataset arguments passed to query* methods.

        Parameters
        ----------
        datasets : `DatasetType`, `str`, `re.Pattern`, or iterable of these
            Expression identifying dataset types.  See `queryDatasetTypes` for
            details.
        collections : `str`, `re.Pattern`, or iterable of these
            Expression identifying collections to be searched.  See
            `queryCollections` for details.
        mode : `str`, optional
            The way in which datasets are being used in this query; one of:

            - "find_first": this is a query for the first dataset in an
              ordered list of collections.  Prohibits collection wildcards,
              but permits dataset type wildcards.

            - "find_all": this is a query for all datasets in all matched
              collections.  Permits collection and dataset type wildcards.

            - "constrain": this is a query for something other than datasets,
              with results constrained by dataset existence.  Permits
              collection wildcards and prohibits ``...`` as a dataset type
              wildcard.
        doomed_by : `list` [ `str` ]
            List to append messages indicating why the query is doomed to
            yield no results.

        Returns
        -------
        dataset_types : `list` [ `DatasetType` ]
            List of matched dataset types.
        collections : `CollectionWildcard`
            Processed collection expression.
        """
        dataset_types: list[DatasetType] = []
        collection_wildcard: CollectionWildcard | None = None
        if datasets is not None:
            if collections is None:
                if not self.defaults.collections:
                    raise NoDefaultCollectionError("No collections, and no registry default collections.")
                collection_wildcard = CollectionWildcard.from_expression(self.defaults.collections)
            else:
                collection_wildcard = CollectionWildcard.from_expression(collections)
                if mode == "find_first" and collection_wildcard.patterns:
                    raise TypeError(
                        f"Collection pattern(s) {collection_wildcard.patterns} not allowed in this context."
                    )
            missing: list[str] = []
            dataset_types = self._managers.datasets.resolve_wildcard(
                datasets, missing=missing, explicit_only=(mode == "constrain")
            )
            if missing and mode == "constrain":
                raise MissingDatasetTypeError(
                    f"Dataset type(s) {missing} are not registered.",
                )
            doomed_by.extend(f"Dataset type {name} is not registered." for name in missing)
        elif collections:
            # I think this check should actually be `collections is not None`,
            # but it looks like some CLI scripts use empty tuple as default.
            raise ArgumentError(f"Cannot pass 'collections' (='{collections}') without 'datasets'.")
        return dataset_types, collection_wildcard

    def queryDatasets(
        self,
        datasetType: Any,
        *,
        collections: CollectionArgType | None = None,
        dimensions: Iterable[str] | None = None,
        dataId: DataId | None = None,
        where: str = "",
        findFirst: bool = False,
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> queries.DatasetQueryResults:
        """Query for and iterate over dataset references matching user-provided
        criteria.

        Parameters
        ----------
        datasetType : dataset type expression
            An expression that fully or partially identifies the dataset types
            to be queried.  Allowed types include `DatasetType`, `str`,
            `re.Pattern`, and iterables thereof.  The special value ``...`` can
            be used to query all dataset types.  See
            :ref:`daf_butler_dataset_type_expressions` for more information.
        collections : collection expression, optional
            An expression that identifies the collections to search, such as a
            `str` (for full matches or partial matches via globs), `re.Pattern`
            (for partial matches), or iterable thereof.  ``...`` can be used to
            search all collections (actually just all `~CollectionType.RUN`
            collections, because this will still find all datasets).
            If not provided, ``self.default.collections`` is used.  See
            :ref:`daf_butler_collection_expressions` for more information.
        dimensions : `~collections.abc.Iterable` of `str`
            Dimensions to include in the query (in addition to those used
            to identify the queried dataset type(s)), either to constrain
            the resulting datasets to those for which a matching dimension
            exists, or to relate the dataset type's dimensions to dimensions
            referenced by the ``dataId`` or ``where`` arguments.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.  See
            :ref:`daf_butler_dimension_expressions` for more information.
        findFirst : `bool`, optional
            If `True` (`False` is default), for each result data ID, only
            yield one `DatasetRef` of each `DatasetType`, from the first
            collection in which a dataset of that dataset type appears
            (according to the order of ``collections`` passed in).  If `True`,
            ``collections`` must not contain regular expressions and may not
            be ``...``.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        check : `bool`, optional
            If `True` (default) check the query for consistency before
            executing it.  This may reject some valid queries that resemble
            common mistakes (e.g. queries for visits without specifying an
            instrument).
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Returns
        -------
        refs : `.queries.DatasetQueryResults`
            Dataset references matching the given query criteria.  Nested data
            IDs are guaranteed to include values for all implied dimensions
            (i.e. `DataCoordinate.hasFull` will return `True`), but will not
            include dimension records (`DataCoordinate.hasRecords` will be
            `False`) unless `~.queries.DatasetQueryResults.expanded` is
            called on the result object (which returns a new one).

        Raises
        ------
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``datasetType`` expression is invalid.
        TypeError
            Raised when the arguments are incompatible, such as when a
            collection wildcard is passed when ``findFirst`` is `True`, or
            when ``collections`` is `None` and ``self.defaults.collections`` is
            also `None`.
        lsst.daf.butler.registry.DataIdError
            Raised when ``dataId`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.

        Notes
        -----
        When multiple dataset types are queried in a single call, the
        results of this operation are equivalent to querying for each dataset
        type separately in turn, and no information about the relationships
        between datasets of different types is included.  In contexts where
        that kind of information is important, the recommended pattern is to
        use `queryDataIds` to first obtain data IDs (possibly with the
        desired dataset types and collections passed as constraints to the
        query), and then use multiple (generally much simpler) calls to
        `queryDatasets` with the returned data IDs passed as constraints.
        """
        doomed_by: list[str] = []
        data_id = self._standardize_query_data_id_args(dataId, doomed_by=doomed_by, **kwargs)
        resolved_dataset_types, collection_wildcard = self._standardize_query_dataset_args(
            datasetType,
            collections,
            mode="find_first" if findFirst else "find_all",
            doomed_by=doomed_by,
        )
        if collection_wildcard is not None and collection_wildcard.empty():
            doomed_by.append("No datasets can be found because collection list is empty.")
            return queries.ChainedDatasetQueryResults([], doomed_by=doomed_by)
        parent_results: list[queries.ParentDatasetQueryResults] = []
        for resolved_dataset_type in resolved_dataset_types:
            # The full set of dimensions in the query is the combination of
            # those needed for the DatasetType and those explicitly requested,
            # if any.
            dimension_names = set(resolved_dataset_type.dimensions.names)
            if dimensions is not None:
                dimension_names.update(self.dimensions.conform(dimensions).names)
            # Construct the summary structure needed to construct a
            # QueryBuilder.
            summary = queries.QuerySummary(
                requested=self.dimensions.conform(dimension_names),
                column_types=self._managers.column_types,
                data_id=data_id,
                expression=where,
                bind=bind,
                defaults=self.defaults.dataId,
                check=check,
                datasets=[resolved_dataset_type],
            )
            builder = self._makeQueryBuilder(summary)
            # Add the dataset subquery to the query, telling the QueryBuilder
            # to include the rank of the selected collection in the results
            # only if we need to findFirst.  Note that if any of the
            # collections are actually wildcard expressions, and
            # findFirst=True, this will raise TypeError for us.
            any_records = builder.joinDataset(
                resolved_dataset_type, collection_wildcard, isResult=True, findFirst=findFirst
            )
            if any_records:
                query = builder.finish()
                parent_results.append(queries.DatabaseParentDatasetQueryResults(query, resolved_dataset_type))
            else:
                doomed_by.append(
                    f"No datasets of type {resolved_dataset_type.name} "
                    f"in collections {collection_wildcard!r}."
                )
        if not parent_results:
            if not doomed_by:
                doomed_by.extend(
                    f"No registered dataset type matching {t!r} found, so no matching datasets can "
                    "exist in any collection."
                    for t in ensure_iterable(datasetType)
                )
            return queries.ChainedDatasetQueryResults([], doomed_by=doomed_by)
        elif len(parent_results) == 1:
            return parent_results[0]
        else:
            return queries.ChainedDatasetQueryResults(parent_results)

    def queryDataIds(
        self,
        dimensions: DimensionGroup | Iterable[str] | str,
        *,
        dataId: DataId | None = None,
        datasets: Any = None,
        collections: CollectionArgType | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> queries.DataCoordinateQueryResults:
        """Query for data IDs matching user-provided criteria.

        Parameters
        ----------
        dimensions : `DimensionGroup`, `str`, or \
                `~collections.abc.Iterable` [ `str` ]
            The dimensions of the data IDs to yield, as either `Dimension`
            instances or `str`.  Will be automatically expanded to a complete
            `DimensionGroup`.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        datasets : dataset type expression, optional
            An expression that fully or partially identifies dataset types
            that should constrain the yielded data IDs.  For example, including
            "raw" here would constrain the yielded ``instrument``,
            ``exposure``, ``detector``, and ``physical_filter`` values to only
            those for which at least one "raw" dataset exists in
            ``collections``.  Allowed types include `DatasetType`, `str`,
            and iterables thereof. See
            :ref:`daf_butler_dataset_type_expressions` for more information.
        collections : collection expression, optional
            An expression that identifies the collections to search for
            datasets, such as a `str` (for full matches or partial matches
            via globs), `re.Pattern` (for partial matches), or iterable
            thereof.  ``...`` can be used to search all collections (actually
            just all `~CollectionType.RUN` collections, because this will
            still find all datasets).  If not provided,
            ``self.default.collections`` is used.  Ignored unless ``datasets``
            is also passed.  See :ref:`daf_butler_collection_expressions` for
            more information.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.  See
            :ref:`daf_butler_dimension_expressions` for more information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        check : `bool`, optional
            If `True` (default) check the query for consistency before
            executing it.  This may reject some valid queries that resemble
            common mistakes (e.g. queries for visits without specifying an
            instrument).
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Returns
        -------
        dataIds : `.queries.DataCoordinateQueryResults`
            Data IDs matching the given query parameters.  These are guaranteed
            to identify all dimensions (`DataCoordinate.hasFull` returns
            `True`), but will not contain `DimensionRecord` objects
            (`DataCoordinate.hasRecords` returns `False`).  Call
            `~.queries.DataCoordinateQueryResults.expanded` on the
            returned object to fetch those (and consider using
            `~.queries.DataCoordinateQueryResults.materialize` on the
            returned object first if the expected number of rows is very
            large). See documentation for those methods for additional
            information.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``collections`` is `None` and
            ``self.defaults.collections`` is `None`.
        lsst.daf.butler.registry.CollectionExpressionError
            Raised when ``collections`` expression is invalid.
        lsst.daf.butler.registry.DataIdError
            Raised when ``dataId`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``datasetType`` expression is invalid.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.
        """
        requested_dimensions = self.dimensions.conform(dimensions)
        doomed_by: list[str] = []
        data_id = self._standardize_query_data_id_args(dataId, doomed_by=doomed_by, **kwargs)
        resolved_dataset_types, collection_wildcard = self._standardize_query_dataset_args(
            datasets, collections, doomed_by=doomed_by
        )
        if collection_wildcard is not None and collection_wildcard.empty():
            doomed_by.append("No data coordinates can be found because collection list is empty.")
        summary = queries.QuerySummary(
            requested=requested_dimensions,
            column_types=self._managers.column_types,
            data_id=data_id,
            expression=where,
            bind=bind,
            defaults=self.defaults.dataId,
            check=check,
            datasets=resolved_dataset_types,
        )
        builder = self._makeQueryBuilder(summary, doomed_by=doomed_by)
        for datasetType in resolved_dataset_types:
            builder.joinDataset(datasetType, collection_wildcard, isResult=False)
        query = builder.finish()

        return queries.DatabaseDataCoordinateQueryResults(query)

    def queryDimensionRecords(
        self,
        element: DimensionElement | str,
        *,
        dataId: DataId | None = None,
        datasets: Any = None,
        collections: CollectionArgType | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> queries.DimensionRecordQueryResults:
        """Query for dimension information matching user-provided criteria.

        Parameters
        ----------
        element : `DimensionElement` or `str`
            The dimension element to obtain records for.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        datasets : dataset type expression, optional
            An expression that fully or partially identifies dataset types
            that should constrain the yielded records.  See `queryDataIds` and
            :ref:`daf_butler_dataset_type_expressions` for more information.
        collections : collection expression, optional
            An expression that identifies the collections to search for
            datasets, such as a `str` (for full matches  or partial matches
            via globs), `re.Pattern` (for partial matches), or iterable
            thereof.  ``...`` can be used to search all collections (actually
            just all `~CollectionType.RUN` collections, because this will
            still find all datasets).  If not provided,
            ``self.default.collections`` is used.  Ignored unless ``datasets``
            is also passed.  See :ref:`daf_butler_collection_expressions` for
            more information.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  See
            `queryDataIds` and :ref:`daf_butler_dimension_expressions` for more
            information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        check : `bool`, optional
            If `True` (default) check the query for consistency before
            executing it.  This may reject some valid queries that resemble
            common mistakes (e.g. queries for visits without specifying an
            instrument).
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Returns
        -------
        dataIds : `.queries.DimensionRecordQueryResults`
            Data IDs matching the given query parameters.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``collections`` is `None` and
            ``self.defaults.collections`` is `None`.
        lsst.daf.butler.registry.CollectionExpressionError
            Raised when ``collections`` expression is invalid.
        lsst.daf.butler.registry.DataIdError
            Raised when ``dataId`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``datasetType`` expression is invalid.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.
        """
        if not isinstance(element, DimensionElement):
            try:
                element = self.dimensions[element]
            except KeyError as e:
                raise DimensionNameError(
                    f"No such dimension '{element}', available dimensions: " + str(self.dimensions.elements)
                ) from e
        doomed_by: list[str] = []
        data_id = self._standardize_query_data_id_args(dataId, doomed_by=doomed_by, **kwargs)
        resolved_dataset_types, collection_wildcard = self._standardize_query_dataset_args(
            datasets, collections, doomed_by=doomed_by
        )
        if collection_wildcard is not None and collection_wildcard.empty():
            doomed_by.append("No dimension records can be found because collection list is empty.")
        summary = queries.QuerySummary(
            requested=element.minimal_group,
            column_types=self._managers.column_types,
            data_id=data_id,
            expression=where,
            bind=bind,
            defaults=self.defaults.dataId,
            check=check,
            datasets=resolved_dataset_types,
        )
        builder = self._makeQueryBuilder(summary, doomed_by=doomed_by)
        for datasetType in resolved_dataset_types:
            builder.joinDataset(datasetType, collection_wildcard, isResult=False)
        query = builder.finish().with_record_columns(element.name)
        return queries.DatabaseDimensionRecordQueryResults(query, element)

    @contextlib.contextmanager
    def _query(self) -> Iterator[Query]:
        """Context manager returning a `Query` object used for construction
        and execution of complex queries.
        """
        with self._query_driver(self.defaults.collections, self.defaults.dataId) as driver:
            yield Query(driver)

    @contextlib.contextmanager
    def _query_driver(
        self,
        default_collections: Iterable[str],
        default_data_id: DataCoordinate,
    ) -> Iterator[DirectQueryDriver]:
        """Set up a `QueryDriver` instance for query execution."""
        # Query internals do repeated lookups of the same collections, so it
        # benefits from the collection record cache.
        with self._managers.caching_context.enable_collection_record_cache():
            driver = DirectQueryDriver(
                self._db,
                self.dimensions,
                self._managers,
                self.dimension_record_cache,
                default_collections=default_collections,
                default_data_id=default_data_id,
            )
            with driver:
                yield driver

    def queryDatasetAssociations(
        self,
        datasetType: str | DatasetType,
        collections: CollectionArgType | None = ...,
        *,
        collectionTypes: Iterable[CollectionType] = CollectionType.all(),
        flattenChains: bool = False,
    ) -> Iterator[DatasetAssociation]:
        """Iterate over dataset-collection combinations where the dataset is in
        the collection.

        This method is a temporary placeholder for better support for
        association results in `queryDatasets`.  It will probably be
        removed in the future, and should be avoided in production code
        whenever possible.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            A dataset type object or the name of one.
        collections : collection expression, optional
            An expression that identifies the collections to search for
            datasets, such as a `str` (for full matches  or partial matches
            via globs), `re.Pattern` (for partial matches), or iterable
            thereof.  ``...`` can be used to search all collections (actually
            just all `~CollectionType.RUN` collections, because this will still
            find all datasets).  If not provided, ``self.default.collections``
            is used.  See :ref:`daf_butler_collection_expressions` for more
            information.
        collectionTypes : `~collections.abc.Set` [ `CollectionType` ], optional
            If provided, only yield associations from collections of these
            types.
        flattenChains : `bool`, optional
            If `True`, search in the children of `~CollectionType.CHAINED`
            collections.  If `False`, ``CHAINED`` collections are ignored.

        Yields
        ------
        association : `.DatasetAssociation`
            Object representing the relationship between a single dataset and
            a single collection.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``collections`` is `None` and
            ``self.defaults.collections`` is `None`.
        lsst.daf.butler.registry.CollectionExpressionError
            Raised when ``collections`` expression is invalid.
        """
        if isinstance(datasetType, str):
            datasetType = self.getDatasetType(datasetType)
        resolved_collections = self.queryCollections(
            collections, collectionTypes=collectionTypes, flattenChains=flattenChains
        )
        with self._query() as query:
            query = query.join_dataset_search(datasetType, resolved_collections)
            result = query.general(
                datasetType.dimensions,
                dataset_fields={datasetType.name: {"dataset_id", "run", "collection", "timespan"}},
                find_first=False,
            )
            yield from DatasetAssociation.from_query_result(result, datasetType)

    def get_datastore_records(self, ref: DatasetRef) -> DatasetRef:
        """Retrieve datastore records for given ref.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset reference for which to retrieve its corresponding datastore
            records.

        Returns
        -------
        updated_ref : `DatasetRef`
            Dataset reference with filled datastore records.

        Notes
        -----
        If this method is called with the dataset ref that is not known to the
        registry then the reference with an empty set of records is returned.
        """
        datastore_records: dict[str, list[StoredDatastoreItemInfo]] = {}
        for opaque, record_class in self._datastore_record_classes.items():
            records = self.fetchOpaqueData(opaque, dataset_id=ref.id)
            datastore_records[opaque] = [record_class.from_record(record) for record in records]
        return ref.replace(datastore_records=datastore_records)

    def store_datastore_records(self, refs: Mapping[str, DatasetRef]) -> None:
        """Store datastore records for given refs.

        Parameters
        ----------
        refs : `~collections.abc.Mapping` [`str`, `DatasetRef`]
            Mapping of a datastore name to dataset reference stored in that
            datastore, reference must include datastore records.
        """
        for datastore_name, ref in refs.items():
            # Store ref IDs in the bridge table.
            bridge = self._managers.datastores.register(datastore_name)
            bridge.insert([ref])

            # store records in opaque tables
            assert ref._datastore_records is not None, "Dataset ref must have datastore records"
            for table_name, records in ref._datastore_records.items():
                opaque_table = self._managers.opaque.get(table_name)
                assert opaque_table is not None, f"Unexpected opaque table name {table_name}"
                opaque_table.insert(*(record.to_record(dataset_id=ref.id) for record in records))

    def make_datastore_tables(self, tables: Mapping[str, DatastoreOpaqueTable]) -> None:
        """Create opaque tables used by datastores.

        Parameters
        ----------
        tables : `~collections.abc.Mapping`
            Maps opaque table name to its definition.

        Notes
        -----
        This method should disappear in the future when opaque table
        definitions will be provided during `Registry` construction.
        """
        datastore_record_classes = {}
        for table_name, table_def in tables.items():
            datastore_record_classes[table_name] = table_def.record_class
            try:
                self._managers.opaque.register(table_name, table_def.table_spec)
            except ReadOnlyDatabaseError:
                # If the database is read only and we just tried and failed to
                # create a table, it means someone is trying to create a
                # read-only butler client for an empty repo.  That should be
                # okay, as long as they then try to get any datasets before
                # some other client creates the table.  Chances are they're
                # just validating configuration.
                pass
        self._datastore_record_classes = datastore_record_classes

    def preload_cache(self, *, load_dimension_record_cache: bool) -> None:
        """Immediately load caches that are used for common operations.

        Parameters
        ----------
        load_dimension_record_cache : `bool`
            If True, preload the dimension record cache.  When this cache is
            preloaded, subsequent external changes to governor dimension
            records will not be visible to this Butler.
        """
        self._managers.datasets.preload_cache()

        if load_dimension_record_cache:
            self.dimension_record_cache.preload_cache()

    @property
    def obsCoreTableManager(self) -> ObsCoreTableManager | None:
        """The ObsCore manager instance for this registry
        (`~.interfaces.ObsCoreTableManager`
        or `None`).

        ObsCore manager may not be implemented for all registry backend, or
        may not be enabled for many repositories.
        """
        return self._managers.obscore

    storageClasses: StorageClassFactory
    """All storage classes known to the registry (`StorageClassFactory`).
    """

    _defaults: RegistryDefaults
    """Default collections used for registry queries (`RegistryDefaults`)."""
