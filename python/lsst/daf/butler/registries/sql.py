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

__all__ = ("SqlRegistry",)

import contextlib
import logging
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Set,
    Union,
    cast,
)

import sqlalchemy
from lsst.daf.relation import LeafRelation, Relation
from lsst.resources import ResourcePathExpression
from lsst.utils.iteration import ensure_iterable

from ..core import (
    Config,
    DataCoordinate,
    DataId,
    DatasetAssociation,
    DatasetColumnTag,
    DatasetId,
    DatasetRef,
    DatasetType,
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
    ddl,
)
from ..core.utils import transactional
from ..registry import (
    ArgumentError,
    CollectionExpressionError,
    CollectionSummary,
    CollectionType,
    CollectionTypeError,
    ConflictingDefinitionError,
    DataIdValueError,
    DatasetTypeError,
    DimensionNameError,
    InconsistentDataIdError,
    NoDefaultCollectionError,
    OrphanedRecordError,
    Registry,
    RegistryConfig,
    RegistryDefaults,
    queries,
)
from ..registry.interfaces import ChainedCollectionRecord, DatasetIdFactory, DatasetIdGenEnum, RunRecord
from ..registry.managers import RegistryManagerInstances, RegistryManagerTypes
from ..registry.wildcards import CollectionWildcard, DatasetTypeWildcard

if TYPE_CHECKING:
    from .._butlerConfig import ButlerConfig
    from ..registry.interfaces import CollectionRecord, Database, DatastoreRegistryBridgeManager


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
    def createFromConfig(
        cls,
        config: Optional[Union[RegistryConfig, str]] = None,
        dimensionConfig: Optional[Union[DimensionConfig, str]] = None,
        butlerRoot: Optional[ResourcePathExpression] = None,
    ) -> Registry:
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
            str(config.connectionString), origin=config.get("origin", 0), namespace=config.get("namespace")
        )
        managerTypes = RegistryManagerTypes.fromConfig(config)
        managers = managerTypes.makeRepo(database, dimensionConfig)
        return cls(database, RegistryDefaults(), managers)

    @classmethod
    def fromConfig(
        cls,
        config: Union[ButlerConfig, RegistryConfig, Config, str],
        butlerRoot: Optional[ResourcePathExpression] = None,
        writeable: bool = True,
        defaults: Optional[RegistryDefaults] = None,
    ) -> Registry:
        """Create `Registry` subclass instance from `config`.

        Registry database must be initialized prior to calling this method.

        Parameters
        ----------
        config : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
            Registry configuration
        butlerRoot : `lsst.resources.ResourcePathExpression`, optional
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
        database = DatabaseClass.fromUri(
            config.connectionString.render_as_string(hide_password=False),
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

    def __init__(self, database: Database, defaults: RegistryDefaults, managers: RegistryManagerInstances):
        self._db = database
        self._managers = managers
        self.storageClasses = StorageClassFactory()
        # Intentionally invoke property setter to initialize defaults.  This
        # can only be done after most of the rest of Registry has already been
        # initialized, and must be done before the property getter is used.
        self.defaults = defaults
        # In the future DatasetIdFactory may become configurable and this
        # instance will need to be shared with datasets manager.
        self.datasetIdFactory = DatasetIdFactory()

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
        with self._db.transaction():
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

    def fetchOpaqueData(self, tableName: str, **where: Any) -> Iterator[Mapping[str, Any]]:
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

    def registerCollection(
        self, name: str, type: CollectionType = CollectionType.TAGGED, doc: Optional[str] = None
    ) -> bool:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        _, registered = self._managers.collections.register(name, type, doc=doc)
        return registered

    def getCollectionType(self, name: str) -> CollectionType:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.collections.find(name).type

    def _get_collection_record(self, name: str) -> CollectionRecord:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.collections.find(name)

    def registerRun(self, name: str, doc: Optional[str] = None) -> bool:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        _, registered = self._managers.collections.register(name, CollectionType.RUN, doc=doc)
        return registered

    @transactional
    def removeCollection(self, name: str) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        self._managers.collections.remove(name)

    def getCollectionChain(self, parent: str) -> tuple[str, ...]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        record = self._managers.collections.find(parent)
        if record.type is not CollectionType.CHAINED:
            raise CollectionTypeError(f"Collection '{parent}' has type {record.type.name}, not CHAINED.")
        assert isinstance(record, ChainedCollectionRecord)
        return record.children

    @transactional
    def setCollectionChain(self, parent: str, children: Any, *, flatten: bool = False) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        record = self._managers.collections.find(parent)
        if record.type is not CollectionType.CHAINED:
            raise CollectionTypeError(f"Collection '{parent}' has type {record.type.name}, not CHAINED.")
        assert isinstance(record, ChainedCollectionRecord)
        children = CollectionWildcard.from_expression(children).require_ordered()
        if children != record.children or flatten:
            record.update(self._managers.collections, children, flatten=flatten)

    def getCollectionParentChains(self, collection: str) -> Set[str]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return {
            record.name
            for record in self._managers.collections.getParentChains(
                self._managers.collections.find(collection).key
            )
        }

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
        parent_name, component = DatasetType.splitDatasetTypeName(name)
        storage = self._managers.datasets[parent_name]
        if component is None:
            return storage.datasetType
        else:
            return storage.datasetType.makeComponentDatasetType(component)

    def supportsIdGenerationMode(self, mode: DatasetIdGenEnum) -> bool:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.datasets.supportsIdGenerationMode(mode)

    def findDataset(
        self,
        datasetType: Union[DatasetType, str],
        dataId: Optional[DataId] = None,
        *,
        collections: Any = None,
        timespan: Optional[Timespan] = None,
        **kwargs: Any,
    ) -> Optional[DatasetRef]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if collections is None:
            if not self.defaults.collections:
                raise NoDefaultCollectionError(
                    "No collections provided to findDataset, and no defaults from registry construction."
                )
            collections = self.defaults.collections
        backend = queries.SqlQueryBackend(self._db, self._managers)
        collection_wildcard = CollectionWildcard.from_expression(collections, require_ordered=True)
        matched_collections = backend.resolve_collection_wildcard(collection_wildcard)
        parent_dataset_type, components = backend.resolve_single_dataset_type_wildcard(
            datasetType, components_deprecated=False
        )
        if len(components) > 1:
            raise DatasetTypeError(
                f"findDataset requires exactly one dataset type; got multiple components {components} "
                f"for parent dataset type {parent_dataset_type.name}."
            )
        component = components[0]
        dataId = DataCoordinate.standardize(
            dataId,
            graph=parent_dataset_type.dimensions,
            universe=self.dimensions,
            defaults=self.defaults.dataId,
            **kwargs,
        )
        governor_constraints = {name: {cast(str, dataId[name])} for name in dataId.graph.governors.names}
        (filtered_collections,) = backend.filter_dataset_collections(
            [parent_dataset_type],
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
                    dataId.subset(parent_dataset_type.dimensions), full=False
                )
                if timespan is not None:
                    requested_columns.add("timespan")
                    predicate = predicate.logical_and(
                        context.make_timespan_overlap_predicate(
                            DatasetColumnTag(parent_dataset_type.name, "timespan"), timespan
                        )
                    )
                relation = backend.make_dataset_query_relation(
                    parent_dataset_type, filtered_collections, requested_columns, context
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
            collection_tag = DatasetColumnTag(parent_dataset_type.name, "collection")
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
                raise LookupError(
                    f"Ambiguous calibration lookup for {parent_dataset_type.name} in collections "
                    f"{collection_wildcard.strings} with timespan {timespan}."
                )
        reader = queries.DatasetRefReader(
            parent_dataset_type,
            translate_collection=lambda k: self._managers.collections[k].name,
        )
        ref = reader.read(best_row, data_id=dataId)
        if component is not None:
            ref = ref.makeComponentRef(component)
        return ref

    @transactional
    def insertDatasets(
        self,
        datasetType: Union[DatasetType, str],
        dataIds: Iterable[DataId],
        run: Optional[str] = None,
        expand: bool = True,
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
    ) -> List[DatasetRef]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if isinstance(datasetType, DatasetType):
            storage = self._managers.datasets.find(datasetType.name)
            if storage is None:
                raise DatasetTypeError(f"DatasetType '{datasetType}' has not been registered.")
        else:
            storage = self._managers.datasets.find(datasetType)
            if storage is None:
                raise DatasetTypeError(f"DatasetType with name '{datasetType}' has not been registered.")
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
                self.expandDataId(dataId, graph=storage.datasetType.dimensions)
                for dataId in progress.wrap(dataIds, f"Expanding {storage.datasetType.name} data IDs")
            ]
        else:
            expandedDataIds = [
                DataCoordinate.standardize(dataId, graph=storage.datasetType.dimensions) for dataId in dataIds
            ]
        try:
            refs = list(storage.insert(runRecord, expandedDataIds, idGenerationMode))
            if self._managers.obscore:
                context = queries.SqlQueryContext(self._db, self._managers.column_types)
                self._managers.obscore.add_datasets(refs, context)
        except sqlalchemy.exc.IntegrityError as err:
            raise ConflictingDefinitionError(
                "A database constraint failure was triggered by inserting "
                f"one or more datasets of type {storage.datasetType} into "
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
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
        reuseIds: bool = False,
    ) -> List[DatasetRef]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        datasets = list(datasets)
        if not datasets:
            # nothing to do
            return []

        # find dataset type
        datasetTypes = set(dataset.datasetType for dataset in datasets)
        if len(datasetTypes) != 1:
            raise DatasetTypeError(f"Multiple dataset types in input datasets: {datasetTypes}")
        datasetType = datasetTypes.pop()

        # get storage handler for this dataset type
        storage = self._managers.datasets.find(datasetType.name)
        if storage is None:
            raise DatasetTypeError(f"DatasetType '{datasetType}' has not been registered.")

        # find run name
        runs = set(dataset.run for dataset in datasets)
        if len(runs) != 1:
            raise ValueError(f"Multiple run names in input datasets: {runs}")
        run = runs.pop()
        if run is None:
            if self.defaults.run is None:
                raise NoDefaultCollectionError(
                    "No run provided to ingestDatasets, and no default from registry construction."
                )
            run = self.defaults.run

        runRecord = self._managers.collections.find(run)
        if runRecord.type is not CollectionType.RUN:
            raise CollectionTypeError(
                f"Given collection '{runRecord.name}' is of type {runRecord.type.name};"
                " RUN collection required."
            )
        assert isinstance(runRecord, RunRecord)

        progress = Progress("daf.butler.Registry.insertDatasets", level=logging.DEBUG)
        if expand:
            expandedDatasets = [
                dataset.expanded(self.expandDataId(dataset.dataId, graph=storage.datasetType.dimensions))
                for dataset in progress.wrap(datasets, f"Expanding {storage.datasetType.name} data IDs")
            ]
        else:
            expandedDatasets = [
                DatasetRef(datasetType, dataset.dataId, id=dataset.id, run=dataset.run, conform=True)
                for dataset in datasets
            ]

        try:
            refs = list(storage.import_(runRecord, expandedDatasets, idGenerationMode, reuseIds))
            if self._managers.obscore:
                context = queries.SqlQueryContext(self._db, self._managers.column_types)
                self._managers.obscore.add_datasets(refs, context)
        except sqlalchemy.exc.IntegrityError as err:
            raise ConflictingDefinitionError(
                "A database constraint failure was triggered by inserting "
                f"one or more datasets of type {storage.datasetType} into "
                f"collection '{run}'. "
                "This probably means a dataset with the same data ID "
                "and dataset type already exists, but it may also mean a "
                "dimension row is missing."
            ) from err
        return refs

    def getDataset(self, id: DatasetId) -> Optional[DatasetRef]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        return self._managers.datasets.getDatasetRef(id)

    @transactional
    def removeDatasets(self, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        progress = Progress("lsst.daf.butler.Registry.removeDatasets", level=logging.DEBUG)
        for datasetType, refsForType in progress.iter_item_chunks(
            DatasetRef.groupByType(refs).items(), desc="Removing datasets by type"
        ):
            storage = self._managers.datasets[datasetType.name]
            try:
                storage.delete(refsForType)
            except sqlalchemy.exc.IntegrityError as err:
                raise OrphanedRecordError(
                    "One or more datasets is still present in one or more Datastores."
                ) from err

    @transactional
    def associate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        progress = Progress("lsst.daf.butler.Registry.associate", level=logging.DEBUG)
        collectionRecord = self._managers.collections.find(collection)
        if collectionRecord.type is not CollectionType.TAGGED:
            raise CollectionTypeError(
                f"Collection '{collection}' has type {collectionRecord.type.name}, not TAGGED."
            )
        for datasetType, refsForType in progress.iter_item_chunks(
            DatasetRef.groupByType(refs).items(), desc="Associating datasets by type"
        ):
            storage = self._managers.datasets[datasetType.name]
            try:
                storage.associate(collectionRecord, refsForType)
                if self._managers.obscore:
                    # If a TAGGED collection is being monitored by ObsCore
                    # manager then we may need to save the dataset.
                    context = queries.SqlQueryContext(self._db, self._managers.column_types)
                    self._managers.obscore.associate(refsForType, collectionRecord, context)
            except sqlalchemy.exc.IntegrityError as err:
                raise ConflictingDefinitionError(
                    f"Constraint violation while associating dataset of type {datasetType.name} with "
                    f"collection {collection}.  This probably means that one or more datasets with the same "
                    "dataset type and data ID already exist in the collection, but it may also indicate "
                    "that the datasets do not exist."
                ) from err

    @transactional
    def disassociate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        progress = Progress("lsst.daf.butler.Registry.disassociate", level=logging.DEBUG)
        collectionRecord = self._managers.collections.find(collection)
        if collectionRecord.type is not CollectionType.TAGGED:
            raise CollectionTypeError(
                f"Collection '{collection}' has type {collectionRecord.type.name}; expected TAGGED."
            )
        for datasetType, refsForType in progress.iter_item_chunks(
            DatasetRef.groupByType(refs).items(), desc="Disassociating datasets by type"
        ):
            storage = self._managers.datasets[datasetType.name]
            storage.disassociate(collectionRecord, refsForType)
            if self._managers.obscore:
                self._managers.obscore.disassociate(refsForType, collectionRecord)

    @transactional
    def certify(self, collection: str, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        progress = Progress("lsst.daf.butler.Registry.certify", level=logging.DEBUG)
        collectionRecord = self._managers.collections.find(collection)
        for datasetType, refsForType in progress.iter_item_chunks(
            DatasetRef.groupByType(refs).items(), desc="Certifying datasets by type"
        ):
            storage = self._managers.datasets[datasetType.name]
            storage.certify(
                collectionRecord,
                refsForType,
                timespan,
                context=queries.SqlQueryContext(self._db, self._managers.column_types),
            )

    @transactional
    def decertify(
        self,
        collection: str,
        datasetType: Union[str, DatasetType],
        timespan: Timespan,
        *,
        dataIds: Optional[Iterable[DataId]] = None,
    ) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        collectionRecord = self._managers.collections.find(collection)
        if isinstance(datasetType, str):
            storage = self._managers.datasets[datasetType]
        else:
            storage = self._managers.datasets[datasetType.name]
        standardizedDataIds = None
        if dataIds is not None:
            standardizedDataIds = [
                DataCoordinate.standardize(d, graph=storage.datasetType.dimensions) for d in dataIds
            ]
        storage.decertify(
            collectionRecord,
            timespan,
            dataIds=standardizedDataIds,
            context=queries.SqlQueryContext(self._db, self._managers.column_types),
        )

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

    def expandDataId(
        self,
        dataId: Optional[DataId] = None,
        *,
        graph: Optional[DimensionGraph] = None,
        records: Optional[NameLookupMapping[DimensionElement, Optional[DimensionRecord]]] = None,
        withDefaults: bool = True,
        **kwargs: Any,
    ) -> DataCoordinate:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if not withDefaults:
            defaults = None
        else:
            defaults = self.defaults.dataId
        try:
            standardized = DataCoordinate.standardize(
                dataId, graph=graph, universe=self.dimensions, defaults=defaults, **kwargs
            )
        except KeyError as exc:
            # This means either kwargs have some odd name or required
            # dimension is missing.
            raise DimensionNameError(str(exc)) from exc
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
        context = queries.SqlQueryContext(self._db, self._managers.column_types)
        for element in standardized.graph.primaryKeyTraversalOrder:
            record = records.get(element.name, ...)  # Use ... to mean not found; None might mean NULL
            if record is ...:
                if isinstance(element, Dimension) and keys.get(element.name) is None:
                    if element in standardized.graph.required:
                        raise DimensionNameError(
                            f"No value or null value for required dimension {element.name}."
                        )
                    keys[element.name] = None
                    record = None
                else:
                    storage = self._managers.dimensions[element]
                    record = storage.fetch_one(DataCoordinate.standardize(keys, graph=element.graph), context)
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
                    raise DataIdValueError(
                        f"Could not fetch record for required dimension {element.name} via keys {keys}."
                    )
                if element.alwaysJoin:
                    raise InconsistentDataIdError(
                        f"Could not fetch record for element {element.name} via keys {keys}, ",
                        "but it is marked alwaysJoin=True; this means one or more dimensions are not "
                        "related.",
                    )
                for d in element.implied:
                    keys.setdefault(d.name, None)
                    records.setdefault(d.name, None)
        return DataCoordinate.standardize(keys, graph=standardized.graph).expanded(records=records)

    def insertDimensionData(
        self,
        element: Union[DimensionElement, str],
        *data: Union[Mapping[str, Any], DimensionRecord],
        conform: bool = True,
        replace: bool = False,
        skip_existing: bool = False,
    ) -> None:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if conform:
            if isinstance(element, str):
                element = self.dimensions[element]
            records = [
                row if isinstance(row, DimensionRecord) else element.RecordClass(**row) for row in data
            ]
        else:
            # Ignore typing since caller said to trust them with conform=False.
            records = data  # type: ignore
        storage = self._managers.dimensions[element]
        storage.insert(*records, replace=replace, skip_existing=skip_existing)

    def syncDimensionData(
        self,
        element: Union[DimensionElement, str],
        row: Union[Mapping[str, Any], DimensionRecord],
        conform: bool = True,
        update: bool = False,
    ) -> Union[bool, Dict[str, Any]]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if conform:
            if isinstance(element, str):
                element = self.dimensions[element]
            record = row if isinstance(row, DimensionRecord) else element.RecordClass(**row)
        else:
            # Ignore typing since caller said to trust them with conform=False.
            record = row  # type: ignore
        storage = self._managers.dimensions[element]
        return storage.sync(record, update=update)

    def queryDatasetTypes(
        self,
        expression: Any = ...,
        *,
        components: Optional[bool] = None,
        missing: Optional[List[str]] = None,
    ) -> Iterable[DatasetType]:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        wildcard = DatasetTypeWildcard.from_expression(expression)
        composition_dict = self._managers.datasets.resolve_wildcard(
            wildcard,
            components=components,
            missing=missing,
        )
        result: list[DatasetType] = []
        for parent_dataset_type, components_for_parent in composition_dict.items():
            result.extend(
                parent_dataset_type.makeComponentDatasetType(c) if c is not None else parent_dataset_type
                for c in components_for_parent
            )
        return result

    def queryCollections(
        self,
        expression: Any = ...,
        datasetType: Optional[DatasetType] = None,
        collectionTypes: Union[Iterable[CollectionType], CollectionType] = CollectionType.all(),
        flattenChains: bool = False,
        includeChains: Optional[bool] = None,
    ) -> Sequence[str]:
        # Docstring inherited from lsst.daf.butler.registry.Registry

        # Right now the datasetTypes argument is completely ignored, but that
        # is consistent with its [lack of] guarantees.  DM-24939 or a follow-up
        # ticket will take care of that.
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
        doomed_by : `Iterable` of `str`, optional
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
        backend = queries.SqlQueryBackend(self._db, self._managers)
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
        collections: Any,
        components: bool | None,
        mode: Literal["find_first"] | Literal["find_all"] | Literal["constrain"] = "constrain",
        *,
        doomed_by: list[str],
    ) -> tuple[dict[DatasetType, list[str | None]], CollectionWildcard | None]:
        """Preprocess dataset arguments passed to query* methods.

        Parameters
        ----------
        datasets : `DatasetType`, `str`, `re.Pattern`, or iterable of these
            Expression identifying dataset types.  See `queryDatasetTypes` for
            details.
        collections : `str`, `re.Pattern`, or iterable of these
            Expression identifying collections to be searched.  See
            `queryCollections` for details.
        components : `bool`, optional
            If `True`, apply all expression patterns to component dataset type
            names as well.  If `False`, never apply patterns to components.
            If `None` (default), apply patterns to components only if their
            parent datasets were not matched by the expression.
            Fully-specified component datasets (`str` or `DatasetType`
            instances) are always included.

            Values other than `False` are deprecated, and only `False` will be
            supported after v26.  After v27 this argument will be removed
            entirely.
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
        composition : `defaultdict` [ `DatasetType`, `list` [ `str` ] ]
            Dictionary mapping parent dataset type to `list` of components
            matched for that dataset type (or `None` for the parent itself).
        collections : `CollectionWildcard`
            Processed collection expression.
        """
        composition: dict[DatasetType, list[str | None]] = {}
        if datasets is not None:
            if not collections:
                if not self.defaults.collections:
                    raise NoDefaultCollectionError("No collections, and no registry default collections.")
                collections = self.defaults.collections
            else:
                collections = CollectionWildcard.from_expression(collections)
                if mode == "find_first" and collections.patterns:
                    raise TypeError(
                        f"Collection pattern(s) {collections.patterns} not allowed in this context."
                    )
            missing: list[str] = []
            composition = self._managers.datasets.resolve_wildcard(
                datasets, components=components, missing=missing, explicit_only=(mode == "constrain")
            )
            if missing and mode == "constrain":
                # After v26 this should raise MissingDatasetTypeError, to be
                # implemented on DM-36303.
                warnings.warn(
                    f"Dataset type(s) {missing} are not registered; this will be an error after v26.",
                    FutureWarning,
                )
            doomed_by.extend(f"Dataset type {name} is not registered." for name in missing)
        elif collections:
            raise ArgumentError(f"Cannot pass 'collections' (='{collections}') without 'datasets'.")
        return composition, collections

    def queryDatasets(
        self,
        datasetType: Any,
        *,
        collections: Any = None,
        dimensions: Optional[Iterable[Union[Dimension, str]]] = None,
        dataId: Optional[DataId] = None,
        where: str = "",
        findFirst: bool = False,
        components: Optional[bool] = None,
        bind: Optional[Mapping[str, Any]] = None,
        check: bool = True,
        **kwargs: Any,
    ) -> queries.DatasetQueryResults:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        doomed_by: list[str] = []
        data_id = self._standardize_query_data_id_args(dataId, doomed_by=doomed_by, **kwargs)
        dataset_composition, collections = self._standardize_query_dataset_args(
            datasetType,
            collections,
            components,
            mode="find_first" if findFirst else "find_all",
            doomed_by=doomed_by,
        )
        parent_results: list[queries.ParentDatasetQueryResults] = []
        for parent_dataset_type, components_for_parent in dataset_composition.items():
            # The full set of dimensions in the query is the combination of
            # those needed for the DatasetType and those explicitly requested,
            # if any.
            dimension_names = set(parent_dataset_type.dimensions.names)
            if dimensions is not None:
                dimension_names.update(self.dimensions.extract(dimensions).names)
            # Construct the summary structure needed to construct a
            # QueryBuilder.
            summary = queries.QuerySummary(
                requested=DimensionGraph(self.dimensions, names=dimension_names),
                data_id=data_id,
                expression=where,
                bind=bind,
                defaults=self.defaults.dataId,
                check=check,
                datasets=[parent_dataset_type],
            )
            builder = self._makeQueryBuilder(summary)
            # Add the dataset subquery to the query, telling the QueryBuilder
            # to include the rank of the selected collection in the results
            # only if we need to findFirst.  Note that if any of the
            # collections are actually wildcard expressions, and
            # findFirst=True, this will raise TypeError for us.
            builder.joinDataset(parent_dataset_type, collections, isResult=True, findFirst=findFirst)
            query = builder.finish()
            parent_results.append(
                queries.ParentDatasetQueryResults(
                    query, parent_dataset_type, components=components_for_parent
                )
            )
        if not parent_results:
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
        dimensions: Union[Iterable[Union[Dimension, str]], Dimension, str],
        *,
        dataId: Optional[DataId] = None,
        datasets: Any = None,
        collections: Any = None,
        where: str = "",
        components: Optional[bool] = None,
        bind: Optional[Mapping[str, Any]] = None,
        check: bool = True,
        **kwargs: Any,
    ) -> queries.DataCoordinateQueryResults:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        dimensions = ensure_iterable(dimensions)
        requestedDimensions = self.dimensions.extract(dimensions)
        doomed_by: list[str] = []
        data_id = self._standardize_query_data_id_args(dataId, doomed_by=doomed_by, **kwargs)
        dataset_composition, collections = self._standardize_query_dataset_args(
            datasets, collections, components, doomed_by=doomed_by
        )
        summary = queries.QuerySummary(
            requested=requestedDimensions,
            data_id=data_id,
            expression=where,
            bind=bind,
            defaults=self.defaults.dataId,
            check=check,
            datasets=dataset_composition.keys(),
        )
        builder = self._makeQueryBuilder(summary, doomed_by=doomed_by)
        for datasetType in dataset_composition.keys():
            builder.joinDataset(datasetType, collections, isResult=False)
        query = builder.finish()

        return queries.DataCoordinateQueryResults(query)

    def queryDimensionRecords(
        self,
        element: Union[DimensionElement, str],
        *,
        dataId: Optional[DataId] = None,
        datasets: Any = None,
        collections: Any = None,
        where: str = "",
        components: Optional[bool] = None,
        bind: Optional[Mapping[str, Any]] = None,
        check: bool = True,
        **kwargs: Any,
    ) -> queries.DimensionRecordQueryResults:
        # Docstring inherited from lsst.daf.butler.registry.Registry
        if not isinstance(element, DimensionElement):
            try:
                element = self.dimensions[element]
            except KeyError as e:
                raise DimensionNameError(
                    f"No such dimension '{element}', available dimensions: "
                    + str(self.dimensions.getStaticElements())
                ) from e
        doomed_by: list[str] = []
        data_id = self._standardize_query_data_id_args(dataId, doomed_by=doomed_by, **kwargs)
        dataset_composition, collections = self._standardize_query_dataset_args(
            datasets, collections, components, doomed_by=doomed_by
        )
        summary = queries.QuerySummary(
            requested=element.graph,
            data_id=data_id,
            expression=where,
            bind=bind,
            defaults=self.defaults.dataId,
            check=check,
            datasets=dataset_composition.keys(),
        )
        builder = self._makeQueryBuilder(summary, doomed_by=doomed_by)
        for datasetType in dataset_composition.keys():
            builder.joinDataset(datasetType, collections, isResult=False)
        query = builder.finish().with_record_columns(element)
        return queries.DatabaseDimensionRecordQueryResults(query, element)

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
                raise NoDefaultCollectionError(
                    "No collections provided to queryDatasetAssociations, "
                    "and no defaults from registry construction."
                )
            collections = self.defaults.collections
        collections = CollectionWildcard.from_expression(collections)
        backend = queries.SqlQueryBackend(self._db, self._managers)
        parent_dataset_type, _ = backend.resolve_single_dataset_type_wildcard(datasetType, components=False)
        timespan_tag = DatasetColumnTag(parent_dataset_type.name, "timespan")
        collection_tag = DatasetColumnTag(parent_dataset_type.name, "collection")
        for parent_collection_record in backend.resolve_collection_wildcard(
            collections,
            collection_types=frozenset(collectionTypes),
            flatten_chains=flattenChains,
        ):
            # Resolve this possibly-chained collection into a list of
            # non-CHAINED collections that actually hold datasets of this
            # type.
            candidate_collection_records = backend.resolve_dataset_collections(
                parent_dataset_type,
                CollectionWildcard.from_names([parent_collection_record.name]),
                allow_calibration_collections=True,
                governor_constraints={},
            )
            if not candidate_collection_records:
                continue
            with backend.context() as context:
                relation = backend.make_dataset_query_relation(
                    parent_dataset_type,
                    candidate_collection_records,
                    columns={"dataset_id", "run", "timespan", "collection"},
                    context=context,
                )
                reader = queries.DatasetRefReader(
                    parent_dataset_type,
                    translate_collection=lambda k: self._managers.collections[k].name,
                    full=False,
                )
                for row in context.fetch_iterable(relation):
                    ref = reader.read(row)
                    collection_record = self._managers.collections[row[collection_tag]]
                    if collection_record.type is CollectionType.CALIBRATION:
                        timespan = row[timespan_tag]
                    else:
                        # For backwards compatibility and (possibly?) user
                        # convenience we continue to define the timespan of a
                        # DatasetAssociation row for a non-CALIBRATION
                        # collection to be None rather than a fully unbounded
                        # timespan.
                        timespan = None
                    yield DatasetAssociation(ref=ref, collection=collection_record.name, timespan=timespan)

    storageClasses: StorageClassFactory
    """All storage classes known to the registry (`StorageClassFactory`).
    """
