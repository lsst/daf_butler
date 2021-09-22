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
    "SqlRegistry",
)

from collections import defaultdict
import contextlib
import logging
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    TYPE_CHECKING,
    Union,
)

import sqlalchemy

from ..core import (
    ButlerURI,
    Config,
    DataCoordinate,
    DataCoordinateIterable,
    DataId,
    DatasetAssociation,
    DatasetId,
    DatasetRef,
    DatasetType,
    ddl,
    Dimension,
    DimensionConfig,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
    DimensionUniverse,
    NamedKeyMapping,
    NameLookupMapping,
    Progress,
    StorageClassFactory,
    Timespan,
)
from ..core.utils import iterable, transactional

from ..registry import (
    Registry,
    RegistryConfig,
    CollectionType,
    RegistryDefaults,
    ConflictingDefinitionError,
    InconsistentDataIdError,
    OrphanedRecordError,
    CollectionSearch,
)
from ..registry import queries
from ..registry.wildcards import CategorizedWildcard, CollectionQuery, Ellipsis
from ..registry.summaries import CollectionSummary
from ..registry.managers import RegistryManagerTypes, RegistryManagerInstances
from ..registry.interfaces import ChainedCollectionRecord, DatasetIdGenEnum, RunRecord

if TYPE_CHECKING:
    from .._butlerConfig import ButlerConfig
    from ..registry.interfaces import (
        CollectionRecord,
        Database,
        DatastoreRegistryBridgeManager,
    )


_LOG = logging.getLogger(__name__)


class SqlRegistry(Registry):
    """Registry implementation based on SQLAlchemy.

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

    defaultConfigFile: Optional[str] = None
    """Path to configuration defaults. Accessed within the ``configs`` resource
    or relative to a search path. Can be None if no defaults specified.
    """

    @classmethod
    def createFromConfig(cls, config: Optional[Union[RegistryConfig, str]] = None,
                         dimensionConfig: Optional[Union[DimensionConfig, str]] = None,
                         butlerRoot: Optional[str] = None) -> Registry:
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
        butlerRoot : `str`, optional
            Path to the repository root this `SqlRegistry` will manage.

        Returns
        -------
        registry : `SqlRegistry`
            A new `SqlRegistry` instance.
        """
        config = cls.forceRegistryConfig(config)
        config.replaceRoot(butlerRoot)

        if isinstance(dimensionConfig, str):
            dimensionConfig = DimensionConfig(config)
        elif dimensionConfig is None:
            dimensionConfig = DimensionConfig()
        elif not isinstance(dimensionConfig, DimensionConfig):
            raise TypeError(f"Incompatible Dimension configuration type: {type(dimensionConfig)}")

        DatabaseClass = config.getDatabaseClass()
        database = DatabaseClass.fromUri(str(config.connectionString), origin=config.get("origin", 0),
                                         namespace=config.get("namespace"))
        managerTypes = RegistryManagerTypes.fromConfig(config)
        managers = managerTypes.makeRepo(database, dimensionConfig)
        return cls(database, RegistryDefaults(), managers)

    @classmethod
    def fromConfig(cls, config: Union[ButlerConfig, RegistryConfig, Config, str],
                   butlerRoot: Optional[Union[str, ButlerURI]] = None, writeable: bool = True,
                   defaults: Optional[RegistryDefaults] = None) -> Registry:
        """Create `Registry` subclass instance from `config`.

        Registry database must be inbitialized prior to calling this method.

        Parameters
        ----------
        config : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
            Registry configuration
        butlerRoot : `str` or `ButlerURI`, optional
            Path to the repository root this `Registry` will manage.
        writeable : `bool`, optional
            If `True` (default) create a read-write connection to the database.
        defaults : `RegistryDefaults`, optional
            Default collection search path and/or output `~CollectionType.RUN`
            collection.

        Returns
        -------
        registry : `SqlRegistry` (subclass)
            A new `SqlRegistry` subclass instance.
        """
        config = cls.forceRegistryConfig(config)
        config.replaceRoot(butlerRoot)
        DatabaseClass = config.getDatabaseClass()
        database = DatabaseClass.fromUri(str(config.connectionString), origin=config.get("origin", 0),
                                         namespace=config.get("namespace"), writeable=writeable)
        managerTypes = RegistryManagerTypes.fromConfig(config)
        managers = managerTypes.loadRepo(database)
        if defaults is None:
            defaults = RegistryDefaults()
        return cls(database, defaults, managers)

    def __init__(self, database: Database, defaults: RegistryDefaults, managers: RegistryManagerInstances):
        self._db = database
        self._managers = managers
        self.storageClasses = StorageClassFactory()
        # Intentionally invoke property setter to initialize defaults.  This
        # can only be done after most of the rest of Registry has already been
        # initialized, and must be done before the property getter is used.
        self.defaults = defaults

    def __str__(self) -> str:
        return str(self._db)

    def __repr__(self) -> str:
        return f"SqlRegistry({self._db!r}, {self.dimensions!r})"

    def isWriteable(self) -> bool:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._db.isWriteable()

    def copy(self, defaults: Optional[RegistryDefaults] = None) -> Registry:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if defaults is None:
            # No need to copy, because `RegistryDefaults` is immutable; we
            # effectively copy on write.
            defaults = self.defaults
        return type(self)(self._db, defaults, self._managers)

    @property
    def dimensions(self) -> DimensionUniverse:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.dimensions.universe

    def refresh(self) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        self._managers.refresh()

    @contextlib.contextmanager
    def transaction(self, *, savepoint: bool = False) -> Iterator[None]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        try:
            with self._db.transaction(savepoint=savepoint):
                yield
        except BaseException:
            # TODO: this clears the caches sometimes when we wouldn't actually
            # need to.  Can we avoid that?
            self._managers.dimensions.clearCaches()
            raise

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
        data
            Each additional positional argument is a dictionary that represents
            a single row to be added.
        """
        self._managers.opaque[tableName].insert(*data)

    def fetchOpaqueData(self, tableName: str, **where: Any) -> Iterator[dict]:
        """Retrieve records from an opaque table.

        Parameters
        ----------
        tableName : `str`
            Logical name of the opaque table.  Must match the name used in a
            previous call to `registerOpaqueTable`.
        where
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
        where
            Additional keyword arguments are interpreted as equality
            constraints that restrict the deleted rows (combined with AND);
            keyword arguments are column names and values are the values they
            must have.
        """
        self._managers.opaque[tableName].delete(where.keys(), where)

    def registerCollection(self, name: str, type: CollectionType = CollectionType.TAGGED,
                           doc: Optional[str] = None) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        self._managers.collections.register(name, type, doc=doc)

    def getCollectionType(self, name: str) -> CollectionType:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.collections.find(name).type

    def _get_collection_record(self, name: str) -> CollectionRecord:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.collections.find(name)

    def registerRun(self, name: str, doc: Optional[str] = None) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        self._managers.collections.register(name, CollectionType.RUN, doc=doc)

    @transactional
    def removeCollection(self, name: str) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        self._managers.collections.remove(name)

    def getCollectionChain(self, parent: str) -> CollectionSearch:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        record = self._managers.collections.find(parent)
        if record.type is not CollectionType.CHAINED:
            raise TypeError(f"Collection '{parent}' has type {record.type.name}, not CHAINED.")
        assert isinstance(record, ChainedCollectionRecord)
        return record.children

    @transactional
    def setCollectionChain(self, parent: str, children: Any, *, flatten: bool = False) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        record = self._managers.collections.find(parent)
        if record.type is not CollectionType.CHAINED:
            raise TypeError(f"Collection '{parent}' has type {record.type.name}, not CHAINED.")
        assert isinstance(record, ChainedCollectionRecord)
        children = CollectionSearch.fromExpression(children)
        if children != record.children or flatten:
            record.update(self._managers.collections, children, flatten=flatten)

    def getCollectionDocumentation(self, collection: str) -> Optional[str]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.collections.getDocumentation(self._managers.collections.find(collection).key)

    def setCollectionDocumentation(self, collection: str, doc: Optional[str]) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        self._managers.collections.setDocumentation(self._managers.collections.find(collection).key, doc)

    def getCollectionSummary(self, collection: str) -> CollectionSummary:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        record = self._managers.collections.find(collection)
        return self._managers.datasets.getCollectionSummary(record)

    def registerDatasetType(self, datasetType: DatasetType) -> bool:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        _, inserted = self._managers.datasets.register(datasetType)
        return inserted

    def removeDatasetType(self, name: str) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        self._managers.datasets.remove(name)

    def getDatasetType(self, name: str) -> DatasetType:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.datasets[name].datasetType

    def supportsIdGenerationMode(self, mode: DatasetIdGenEnum) -> bool:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.datasets.supportsIdGenerationMode(mode)

    def findDataset(self, datasetType: Union[DatasetType, str], dataId: Optional[DataId] = None, *,
                    collections: Any = None, timespan: Optional[Timespan] = None,
                    **kwargs: Any) -> Optional[DatasetRef]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if isinstance(datasetType, DatasetType):
            storage = self._managers.datasets[datasetType.name]
        else:
            storage = self._managers.datasets[datasetType]
        dataId = DataCoordinate.standardize(dataId, graph=storage.datasetType.dimensions,
                                            universe=self.dimensions, defaults=self.defaults.dataId,
                                            **kwargs)
        if collections is None:
            if not self.defaults.collections:
                raise TypeError("No collections provided to findDataset, "
                                "and no defaults from registry construction.")
            collections = self.defaults.collections
        else:
            collections = CollectionSearch.fromExpression(collections)
        for collectionRecord in collections.iter(self._managers.collections):
            if (collectionRecord.type is CollectionType.CALIBRATION
                    and (not storage.datasetType.isCalibration() or timespan is None)):
                continue
            result = storage.find(collectionRecord, dataId, timespan=timespan)
            if result is not None:
                return result

        return None

    @transactional
    def insertDatasets(self, datasetType: Union[DatasetType, str], dataIds: Iterable[DataId],
                       run: Optional[str] = None, expand: bool = True,
                       idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE) -> List[DatasetRef]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if isinstance(datasetType, DatasetType):
            storage = self._managers.datasets.find(datasetType.name)
            if storage is None:
                raise LookupError(f"DatasetType '{datasetType}' has not been registered.")
        else:
            storage = self._managers.datasets.find(datasetType)
            if storage is None:
                raise LookupError(f"DatasetType with name '{datasetType}' has not been registered.")
        if run is None:
            if self.defaults.run is None:
                raise TypeError("No run provided to insertDatasets, "
                                "and no default from registry construction.")
            run = self.defaults.run
        runRecord = self._managers.collections.find(run)
        if runRecord.type is not CollectionType.RUN:
            raise TypeError(f"Given collection is of type {runRecord.type.name}; RUN collection required.")
        assert isinstance(runRecord, RunRecord)
        progress = Progress("daf.butler.Registry.insertDatasets", level=logging.DEBUG)
        if expand:
            expandedDataIds = [self.expandDataId(dataId, graph=storage.datasetType.dimensions)
                               for dataId in progress.wrap(dataIds,
                                                           f"Expanding {storage.datasetType.name} data IDs")]
        else:
            expandedDataIds = [DataCoordinate.standardize(dataId, graph=storage.datasetType.dimensions)
                               for dataId in dataIds]
        try:
            refs = list(storage.insert(runRecord, expandedDataIds, idGenerationMode))
        except sqlalchemy.exc.IntegrityError as err:
            raise ConflictingDefinitionError(f"A database constraint failure was triggered by inserting "
                                             f"one or more datasets of type {storage.datasetType} into "
                                             f"collection '{run}'. "
                                             f"This probably means a dataset with the same data ID "
                                             f"and dataset type already exists, but it may also mean a "
                                             f"dimension row is missing.") from err
        return refs

    @transactional
    def _importDatasets(self, datasets: Iterable[DatasetRef], expand: bool = True,
                        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
                        reuseIds: bool = False) -> List[DatasetRef]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        datasets = list(datasets)
        if not datasets:
            # nothing to do
            return []

        # find dataset type
        datasetTypes = set(dataset.datasetType for dataset in datasets)
        if len(datasetTypes) != 1:
            raise ValueError(f"Multiple dataset types in input datasets: {datasetTypes}")
        datasetType = datasetTypes.pop()

        # get storage handler for this dataset type
        storage = self._managers.datasets.find(datasetType.name)
        if storage is None:
            raise LookupError(f"DatasetType '{datasetType}' has not been registered.")

        # find run name
        runs = set(dataset.run for dataset in datasets)
        if len(runs) != 1:
            raise ValueError(f"Multiple run names in input datasets: {runs}")
        run = runs.pop()
        if run is None:
            if self.defaults.run is None:
                raise TypeError("No run provided to ingestDatasets, "
                                "and no default from registry construction.")
            run = self.defaults.run

        runRecord = self._managers.collections.find(run)
        if runRecord.type is not CollectionType.RUN:
            raise TypeError(f"Given collection '{runRecord.name}' is of type {runRecord.type.name};"
                            " RUN collection required.")
        assert isinstance(runRecord, RunRecord)

        progress = Progress("daf.butler.Registry.insertDatasets", level=logging.DEBUG)
        if expand:
            expandedDatasets = [
                dataset.expanded(self.expandDataId(dataset.dataId, graph=storage.datasetType.dimensions))
                for dataset in progress.wrap(datasets, f"Expanding {storage.datasetType.name} data IDs")]
        else:
            expandedDatasets = [
                DatasetRef(datasetType, dataset.dataId, id=dataset.id, run=dataset.run, conform=True)
                for dataset in datasets
            ]

        try:
            refs = list(storage.import_(runRecord, expandedDatasets, idGenerationMode, reuseIds))
        except sqlalchemy.exc.IntegrityError as err:
            raise ConflictingDefinitionError(f"A database constraint failure was triggered by inserting "
                                             f"one or more datasets of type {storage.datasetType} into "
                                             f"collection '{run}'. "
                                             f"This probably means a dataset with the same data ID "
                                             f"and dataset type already exists, but it may also mean a "
                                             f"dimension row is missing.") from err
        return refs

    def getDataset(self, id: DatasetId) -> Optional[DatasetRef]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.datasets.getDatasetRef(id)

    @transactional
    def removeDatasets(self, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        progress = Progress("lsst.daf.butler.Registry.removeDatasets", level=logging.DEBUG)
        for datasetType, refsForType in progress.iter_item_chunks(DatasetRef.groupByType(refs).items(),
                                                                  desc="Removing datasets by type"):
            storage = self._managers.datasets[datasetType.name]
            try:
                storage.delete(refsForType)
            except sqlalchemy.exc.IntegrityError as err:
                raise OrphanedRecordError("One or more datasets is still "
                                          "present in one or more Datastores.") from err

    @transactional
    def associate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        progress = Progress("lsst.daf.butler.Registry.associate", level=logging.DEBUG)
        collectionRecord = self._managers.collections.find(collection)
        if collectionRecord.type is not CollectionType.TAGGED:
            raise TypeError(f"Collection '{collection}' has type {collectionRecord.type.name}, not TAGGED.")
        for datasetType, refsForType in progress.iter_item_chunks(DatasetRef.groupByType(refs).items(),
                                                                  desc="Associating datasets by type"):
            storage = self._managers.datasets[datasetType.name]
            try:
                storage.associate(collectionRecord, refsForType)
            except sqlalchemy.exc.IntegrityError as err:
                raise ConflictingDefinitionError(
                    f"Constraint violation while associating dataset of type {datasetType.name} with "
                    f"collection {collection}.  This probably means that one or more datasets with the same "
                    f"dataset type and data ID already exist in the collection, but it may also indicate "
                    f"that the datasets do not exist."
                ) from err

    @transactional
    def disassociate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        progress = Progress("lsst.daf.butler.Registry.disassociate", level=logging.DEBUG)
        collectionRecord = self._managers.collections.find(collection)
        if collectionRecord.type is not CollectionType.TAGGED:
            raise TypeError(f"Collection '{collection}' has type {collectionRecord.type.name}; "
                            "expected TAGGED.")
        for datasetType, refsForType in progress.iter_item_chunks(DatasetRef.groupByType(refs).items(),
                                                                  desc="Disassociating datasets by type"):
            storage = self._managers.datasets[datasetType.name]
            storage.disassociate(collectionRecord, refsForType)

    @transactional
    def certify(self, collection: str, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        progress = Progress("lsst.daf.butler.Registry.certify", level=logging.DEBUG)
        collectionRecord = self._managers.collections.find(collection)
        for datasetType, refsForType in progress.iter_item_chunks(DatasetRef.groupByType(refs).items(),
                                                                  desc="Certifying datasets by type"):
            storage = self._managers.datasets[datasetType.name]
            storage.certify(collectionRecord, refsForType, timespan)

    @transactional
    def decertify(self, collection: str, datasetType: Union[str, DatasetType], timespan: Timespan, *,
                  dataIds: Optional[Iterable[DataId]] = None) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        collectionRecord = self._managers.collections.find(collection)
        if isinstance(datasetType, str):
            storage = self._managers.datasets[datasetType]
        else:
            storage = self._managers.datasets[datasetType.name]
        standardizedDataIds = None
        if dataIds is not None:
            standardizedDataIds = [DataCoordinate.standardize(d, graph=storage.datasetType.dimensions)
                                   for d in dataIds]
        storage.decertify(collectionRecord, timespan, dataIds=standardizedDataIds)

    def getDatastoreBridgeManager(self) -> DatastoreRegistryBridgeManager:
        """Return an object that allows a new `Datastore` instance to
        communicate with this `Registry`.

        Returns
        -------
        manager : `DatastoreRegistryBridgeManager`
            Object that mediates communication between this `Registry` and its
            associated datastores.
        """
        return self._managers.datastores

    def getDatasetLocations(self, ref: DatasetRef) -> Iterable[str]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.datastores.findDatastores(ref)

    def expandDataId(self, dataId: Optional[DataId] = None, *, graph: Optional[DimensionGraph] = None,
                     records: Optional[NameLookupMapping[DimensionElement, Optional[DimensionRecord]]] = None,
                     withDefaults: bool = True,
                     **kwargs: Any) -> DataCoordinate:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if not withDefaults:
            defaults = None
        else:
            defaults = self.defaults.dataId
        standardized = DataCoordinate.standardize(dataId, graph=graph, universe=self.dimensions,
                                                  defaults=defaults, **kwargs)
        if standardized.hasRecords():
            return standardized
        if records is None:
            records = {}
        elif isinstance(records, NamedKeyMapping):
            records = records.byName()
        else:
            records = dict(records)
        if isinstance(dataId, DataCoordinate) and dataId.hasRecords():
            records.update(dataId.records.byName())
        keys = standardized.byName()
        for element in standardized.graph.primaryKeyTraversalOrder:
            record = records.get(element.name, ...)  # Use ... to mean not found; None might mean NULL
            if record is ...:
                if isinstance(element, Dimension) and keys.get(element.name) is None:
                    if element in standardized.graph.required:
                        raise LookupError(
                            f"No value or null value for required dimension {element.name}."
                        )
                    keys[element.name] = None
                    record = None
                else:
                    storage = self._managers.dimensions[element]
                    dataIdSet = DataCoordinateIterable.fromScalar(
                        DataCoordinate.standardize(keys, graph=element.graph)
                    )
                    fetched = tuple(storage.fetch(dataIdSet))
                    try:
                        (record,) = fetched
                    except ValueError:
                        record = None
                records[element.name] = record
            if record is not None:
                for d in element.implied:
                    value = getattr(record, d.name)
                    if keys.setdefault(d.name, value) != value:
                        raise InconsistentDataIdError(
                            f"Data ID {standardized} has {d.name}={keys[d.name]!r}, "
                            f"but {element.name} implies {d.name}={value!r}."
                        )
            else:
                if element in standardized.graph.required:
                    raise LookupError(
                        f"Could not fetch record for required dimension {element.name} via keys {keys}."
                    )
                if element.alwaysJoin:
                    raise InconsistentDataIdError(
                        f"Could not fetch record for element {element.name} via keys {keys}, ",
                        "but it is marked alwaysJoin=True; this means one or more dimensions are not "
                        "related."
                    )
                for d in element.implied:
                    keys.setdefault(d.name, None)
                    records.setdefault(d.name, None)
        return DataCoordinate.standardize(keys, graph=standardized.graph).expanded(records=records)

    def insertDimensionData(self, element: Union[DimensionElement, str],
                            *data: Union[Mapping[str, Any], DimensionRecord],
                            conform: bool = True,
                            replace: bool = False) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if conform:
            if isinstance(element, str):
                element = self.dimensions[element]
            records = [row if isinstance(row, DimensionRecord) else element.RecordClass(**row)
                       for row in data]
        else:
            # Ignore typing since caller said to trust them with conform=False.
            records = data  # type: ignore
        storage = self._managers.dimensions[element]  # type: ignore
        storage.insert(*records, replace=replace)

    def syncDimensionData(self, element: Union[DimensionElement, str],
                          row: Union[Mapping[str, Any], DimensionRecord],
                          conform: bool = True,
                          update: bool = False) -> Union[bool, Dict[str, Any]]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if conform:
            if isinstance(element, str):
                element = self.dimensions[element]
            record = row if isinstance(row, DimensionRecord) else element.RecordClass(**row)
        else:
            # Ignore typing since caller said to trust them with conform=False.
            record = row  # type: ignore
        storage = self._managers.dimensions[element]  # type: ignore
        return storage.sync(record, update=update)

    def queryDatasetTypes(self, expression: Any = ..., *, components: Optional[bool] = None
                          ) -> Iterator[DatasetType]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        wildcard = CategorizedWildcard.fromExpression(expression, coerceUnrecognized=lambda d: d.name)
        if wildcard is Ellipsis:
            for datasetType in self._managers.datasets:
                # The dataset type can no longer be a component
                yield datasetType
                if components:
                    # Automatically create the component dataset types
                    try:
                        componentsForDatasetType = datasetType.makeAllComponentDatasetTypes()
                    except KeyError as err:
                        _LOG.warning(f"Could not load storage class {err} for {datasetType.name}; "
                                     "if it has components they will not be included in query results.")
                    else:
                        yield from componentsForDatasetType
            return
        done: Set[str] = set()
        for name in wildcard.strings:
            storage = self._managers.datasets.find(name)
            if storage is not None:
                done.add(storage.datasetType.name)
                yield storage.datasetType
        if wildcard.patterns:
            # If components (the argument) is None, we'll save component
            # dataset that we might want to match, but only if their parents
            # didn't get included.
            componentsForLater = []
            for registeredDatasetType in self._managers.datasets:
                # Components are not stored in registry so expand them here
                allDatasetTypes = [registeredDatasetType]
                try:
                    allDatasetTypes.extend(registeredDatasetType.makeAllComponentDatasetTypes())
                except KeyError as err:
                    _LOG.warning(f"Could not load storage class {err} for {registeredDatasetType.name}; "
                                 "if it has components they will not be included in query results.")
                for datasetType in allDatasetTypes:
                    if datasetType.name in done:
                        continue
                    parentName, componentName = datasetType.nameAndComponent()
                    if componentName is not None and not components:
                        if components is None and parentName not in done:
                            componentsForLater.append(datasetType)
                        continue
                    if any(p.fullmatch(datasetType.name) for p in wildcard.patterns):
                        done.add(datasetType.name)
                        yield datasetType
            # Go back and try to match saved components.
            for datasetType in componentsForLater:
                parentName, _ = datasetType.nameAndComponent()
                if parentName not in done and any(p.fullmatch(datasetType.name) for p in wildcard.patterns):
                    yield datasetType

    def queryCollections(self, expression: Any = ...,
                         datasetType: Optional[DatasetType] = None,
                         collectionTypes: Iterable[CollectionType] = CollectionType.all(),
                         flattenChains: bool = False,
                         includeChains: Optional[bool] = None) -> Iterator[str]:
        # Docstring inherited from lsst.daf.butler.registry.Registry

        # Right now the datasetTypes argument is completely ignored, but that
        # is consistent with its [lack of] guarantees.  DM-24939 or a follow-up
        # ticket will take care of that.
        query = CollectionQuery.fromExpression(expression)
        for record in query.iter(self._managers.collections, collectionTypes=frozenset(collectionTypes),
                                 flattenChains=flattenChains, includeChains=includeChains):
            yield record.name

    def _makeQueryBuilder(self, summary: queries.QuerySummary) -> queries.QueryBuilder:
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

        Returns
        -------
        builder : `queries.QueryBuilder`
            Object that can be used to construct and perform advanced queries.
        """
        return queries.QueryBuilder(
            summary,
            queries.RegistryManagers(
                collections=self._managers.collections,
                dimensions=self._managers.dimensions,
                datasets=self._managers.datasets,
                TimespanReprClass=self._db.getTimespanRepresentation(),
            ),
        )

    def queryDatasets(self, datasetType: Any, *,
                      collections: Any = None,
                      dimensions: Optional[Iterable[Union[Dimension, str]]] = None,
                      dataId: Optional[DataId] = None,
                      where: Optional[str] = None,
                      findFirst: bool = False,
                      components: Optional[bool] = None,
                      bind: Optional[Mapping[str, Any]] = None,
                      check: bool = True,
                      **kwargs: Any) -> queries.DatasetQueryResults:
        # Docstring inherited from lsst.daf.butler.registry.Registry

        # Standardize the collections expression.
        if collections is None:
            if not self.defaults.collections:
                raise TypeError("No collections provided to findDataset, "
                                "and no defaults from registry construction.")
            collections = self.defaults.collections
        elif findFirst:
            collections = CollectionSearch.fromExpression(collections)
        else:
            collections = CollectionQuery.fromExpression(collections)
        # Standardize and expand the data ID provided as a constraint.
        standardizedDataId = self.expandDataId(dataId, **kwargs)

        # We can only query directly if given a non-component DatasetType
        # instance.  If we were given an expression or str or a component
        # DatasetType instance, we'll populate this dict, recurse, and return.
        # If we already have a non-component DatasetType, it will remain None
        # and we'll run the query directly.
        composition: Optional[
            Dict[
                DatasetType,  # parent dataset type
                List[Optional[str]]  # component name, or None for parent
            ]
        ] = None
        if not isinstance(datasetType, DatasetType):
            # We were given a dataset type expression (which may be as simple
            # as a str).  Loop over all matching datasets, delegating handling
            # of the `components` argument to queryDatasetTypes, as we populate
            # the composition dict.
            composition = defaultdict(list)
            for trueDatasetType in self.queryDatasetTypes(datasetType, components=components):
                parentName, componentName = trueDatasetType.nameAndComponent()
                if componentName is not None:
                    parentDatasetType = self.getDatasetType(parentName)
                    composition.setdefault(parentDatasetType, []).append(componentName)
                else:
                    composition.setdefault(trueDatasetType, []).append(None)
            if not composition:
                return queries.ChainedDatasetQueryResults(
                    [],
                    doomed_by=[f"No registered dataset type matching {t!r} found."
                               for t in iterable(datasetType)],
                )
        elif datasetType.isComponent():
            # We were given a true DatasetType instance, but it's a component.
            # the composition dict will have exactly one item.
            parentName, componentName = datasetType.nameAndComponent()
            parentDatasetType = self.getDatasetType(parentName)
            composition = {parentDatasetType: [componentName]}
        if composition is not None:
            # We need to recurse.  Do that once for each parent dataset type.
            chain = []
            for parentDatasetType, componentNames in composition.items():
                parentResults = self.queryDatasets(
                    parentDatasetType,
                    collections=collections,
                    dimensions=dimensions,
                    dataId=standardizedDataId,
                    where=where,
                    bind=bind,
                    findFirst=findFirst,
                    check=check,
                )
                assert isinstance(parentResults, queries.ParentDatasetQueryResults), \
                    "Should always be true if passing in a DatasetType instance, and we are."
                chain.append(
                    parentResults.withComponents(componentNames)
                )
            return queries.ChainedDatasetQueryResults(chain)
        # If we get here, there's no need to recurse (or we are already
        # recursing; there can only ever be one level of recursion).

        # The full set of dimensions in the query is the combination of those
        # needed for the DatasetType and those explicitly requested, if any.
        requestedDimensionNames = set(datasetType.dimensions.names)
        if dimensions is not None:
            requestedDimensionNames.update(self.dimensions.extract(dimensions).names)
        # Construct the summary structure needed to construct a QueryBuilder.
        summary = queries.QuerySummary(
            requested=DimensionGraph(self.dimensions, names=requestedDimensionNames),
            dataId=standardizedDataId,
            expression=where,
            bind=bind,
            defaults=self.defaults.dataId,
            check=check,
        )
        builder = self._makeQueryBuilder(summary)
        # Add the dataset subquery to the query, telling the QueryBuilder to
        # include the rank of the selected collection in the results only if we
        # need to findFirst.  Note that if any of the collections are
        # actually wildcard expressions, and we've asked for deduplication,
        # this will raise TypeError for us.
        builder.joinDataset(datasetType, collections, isResult=True, findFirst=findFirst)
        query = builder.finish()
        return queries.ParentDatasetQueryResults(self._db, query, components=[None], datasetType=datasetType)

    def queryDataIds(self, dimensions: Union[Iterable[Union[Dimension, str]], Dimension, str], *,
                     dataId: Optional[DataId] = None,
                     datasets: Any = None,
                     collections: Any = None,
                     where: Optional[str] = None,
                     components: Optional[bool] = None,
                     bind: Optional[Mapping[str, Any]] = None,
                     check: bool = True,
                     **kwargs: Any) -> queries.DataCoordinateQueryResults:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        dimensions = iterable(dimensions)
        standardizedDataId = self.expandDataId(dataId, **kwargs)
        standardizedDatasetTypes = set()
        requestedDimensions = self.dimensions.extract(dimensions)
        queryDimensionNames = set(requestedDimensions.names)
        if datasets is not None:
            if not collections:
                if not self.defaults.collections:
                    raise TypeError(f"Cannot pass 'datasets' (='{datasets}') without 'collections'.")
                collections = self.defaults.collections
            else:
                # Preprocess collections expression in case the original
                # included single-pass iterators (we'll want to use it multiple
                # times below).
                collections = CollectionQuery.fromExpression(collections)
            for datasetType in self.queryDatasetTypes(datasets, components=components):
                queryDimensionNames.update(datasetType.dimensions.names)
                # If any matched dataset type is a component, just operate on
                # its parent instead, because Registry doesn't know anything
                # about what components exist, and here (unlike queryDatasets)
                # we don't care about returning them.
                parentDatasetTypeName, componentName = datasetType.nameAndComponent()
                if componentName is not None:
                    datasetType = self.getDatasetType(parentDatasetTypeName)
                standardizedDatasetTypes.add(datasetType)
        elif collections:
            raise TypeError(f"Cannot pass 'collections' (='{collections}') without 'datasets'.")

        summary = queries.QuerySummary(
            requested=DimensionGraph(self.dimensions, names=queryDimensionNames),
            dataId=standardizedDataId,
            expression=where,
            bind=bind,
            defaults=self.defaults.dataId,
            check=check,
        )
        builder = self._makeQueryBuilder(summary)
        for datasetType in standardizedDatasetTypes:
            builder.joinDataset(datasetType, collections, isResult=False)
        query = builder.finish()
        return queries.DataCoordinateQueryResults(self._db, query)

    def queryDimensionRecords(self, element: Union[DimensionElement, str], *,
                              dataId: Optional[DataId] = None,
                              datasets: Any = None,
                              collections: Any = None,
                              where: Optional[str] = None,
                              components: Optional[bool] = None,
                              bind: Optional[Mapping[str, Any]] = None,
                              check: bool = True,
                              **kwargs: Any) -> Iterator[DimensionRecord]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if not isinstance(element, DimensionElement):
            try:
                element = self.dimensions[element]
            except KeyError as e:
                raise KeyError(f"No such dimension '{element}', available dimensions: "
                               + str(self.dimensions.getStaticElements())) from e
        dataIds = self.queryDataIds(element.graph, dataId=dataId, datasets=datasets, collections=collections,
                                    where=where, components=components, bind=bind, check=check, **kwargs)
        return iter(self._managers.dimensions[element].fetch(dataIds))

    def queryDatasetAssociations(
        self,
        datasetType: Union[str, DatasetType],
        collections: Any = ...,
        *,
        collectionTypes: Iterable[CollectionType] = CollectionType.all(),
        flattenChains: bool = False,
    ) -> Iterator[DatasetAssociation]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if collections is None:
            if not self.defaults.collections:
                raise TypeError("No collections provided to findDataset, "
                                "and no defaults from registry construction.")
            collections = self.defaults.collections
        else:
            collections = CollectionQuery.fromExpression(collections)
        TimespanReprClass = self._db.getTimespanRepresentation()
        if isinstance(datasetType, str):
            storage = self._managers.datasets[datasetType]
        else:
            storage = self._managers.datasets[datasetType.name]
        for collectionRecord in collections.iter(self._managers.collections,
                                                 collectionTypes=frozenset(collectionTypes),
                                                 flattenChains=flattenChains):
            query = storage.select(collectionRecord)
            for row in self._db.query(query.combine()).mappings():
                dataId = DataCoordinate.fromRequiredValues(
                    storage.datasetType.dimensions,
                    tuple(row[name] for name in storage.datasetType.dimensions.required.names)
                )
                runRecord = self._managers.collections[row[self._managers.collections.getRunForeignKeyName()]]
                ref = DatasetRef(storage.datasetType, dataId, id=row["id"], run=runRecord.name,
                                 conform=False)
                if collectionRecord.type is CollectionType.CALIBRATION:
                    timespan = TimespanReprClass.extract(row)
                else:
                    timespan = None
                yield DatasetAssociation(ref=ref, collection=collectionRecord.name, timespan=timespan)

    storageClasses: StorageClassFactory
    """All storage classes known to the registry (`StorageClassFactory`).
    """
