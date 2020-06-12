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
    "ConsistentDataIds",
    "Registry",
)

from collections import defaultdict
import contextlib
from dataclasses import dataclass
import sys
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Type,
    TYPE_CHECKING,
    Union,
)

import astropy.time
import sqlalchemy

import lsst.sphgeom
from ..core import (
    Config,
    DataCoordinate,
    DataId,
    DatasetRef,
    DatasetType,
    ddl,
    Dimension,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
    DimensionUniverse,
    ExpandedDataCoordinate,
    NamedKeyDict,
    Timespan,
    StorageClassFactory,
)
from ..core.utils import doImport, iterable, transactional
from ._config import RegistryConfig
from .queries import (
    QueryBuilder,
    QuerySummary,
)
from ._collectionType import CollectionType
from ._exceptions import ConflictingDefinitionError, InconsistentDataIdError, OrphanedRecordError
from .wildcards import CategorizedWildcard, CollectionQuery, CollectionSearch, Ellipsis
from .interfaces import ChainedCollectionRecord, RunRecord

if TYPE_CHECKING:
    from ..butlerConfig import ButlerConfig
    from .interfaces import (
        ButlerAttributeManager,
        CollectionManager,
        Database,
        OpaqueTableStorageManager,
        DimensionRecordStorageManager,
        DatasetRecordStorageManager,
        DatastoreRegistryBridgeManager,
    )


@dataclass
class ConsistentDataIds:
    """A struct used to report relationships between data IDs by
    `Registry.relateDataIds`.

    If an instance of this class is returned (instead of `None`), the data IDs
    are "not inconsistent" - any keys they have in common have the same value,
    and any spatial or temporal relationships they have at least might involve
    an overlap.  To capture this, any instance of `ConsistentDataIds` coerces
    to `True` in boolean contexts.
    """

    overlaps: bool
    """If `True`, the data IDs have at least one key in common, associated with
    the same value.

    Note that data IDs are not inconsistent even if overlaps is `False` - they
    may simply have no keys in common, which means they cannot have
    inconsistent values for any keys.  They may even be equal, in the case that
    both data IDs are empty.

    This field does _not_ indicate whether a spatial or temporal overlap
    relationship exists.
    """

    contains: bool
    """If `True`, all keys in the first data ID are in the second, and are
    associated with the same values.

    This includes case where the first data ID is empty.
    """

    within: bool
    """If `True`, all keys in the second data ID are in the first, and are
    associated with the same values.

    This includes case where the second data ID is empty.
    """

    @property
    def equal(self) -> bool:
        """If `True`, the two data IDs are the same.

        Data IDs are equal if they have both a `contains` and a `within`
        relationship.
        """
        return self.contains and self.within

    @property
    def disjoint(self) -> bool:
        """If `True`, the two data IDs have no keys in common.

        This is simply the oppose of `overlaps`.  Disjoint datasets are by
        definition not inconsistent.
        """
        return not self.overlaps

    def __bool__(self) -> bool:
        return True


class Registry:
    """Registry interface.

    Parameters
    ----------
    config : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
        Registry configuration
    """

    defaultConfigFile = None
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    @classmethod
    def fromConfig(cls, config: Union[ButlerConfig, RegistryConfig, Config, str], create: bool = False,
                   butlerRoot: Optional[str] = None, writeable: bool = True) -> Registry:
        """Create `Registry` subclass instance from `config`.

        Uses ``registry.cls`` from `config` to determine which subclass to
        instantiate.

        Parameters
        ----------
        config : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
            Registry configuration
        create : `bool`, optional
            Assume empty Registry and create a new one.
        butlerRoot : `str`, optional
            Path to the repository root this `Registry` will manage.
        writeable : `bool`, optional
            If `True` (default) create a read-write connection to the database.

        Returns
        -------
        registry : `Registry` (subclass)
            A new `Registry` subclass instance.
        """
        if not isinstance(config, RegistryConfig):
            if isinstance(config, str) or isinstance(config, Config):
                config = RegistryConfig(config)
            else:
                raise ValueError("Incompatible Registry configuration: {}".format(config))
        config.replaceRoot(butlerRoot)
        DatabaseClass = config.getDatabaseClass()
        database = DatabaseClass.fromUri(str(config.connectionString), origin=config.get("origin", 0),
                                         namespace=config.get("namespace"), writeable=writeable)
        universe = DimensionUniverse(config)
        attributes = doImport(config["managers", "attributes"])
        opaque = doImport(config["managers", "opaque"])
        dimensions = doImport(config["managers", "dimensions"])
        collections = doImport(config["managers", "collections"])
        datasets = doImport(config["managers", "datasets"])
        datastoreBridges = doImport(config["managers", "datastores"])
        return cls(database, universe, dimensions=dimensions, attributes=attributes, opaque=opaque,
                   collections=collections, datasets=datasets, datastoreBridges=datastoreBridges,
                   create=create)

    def __init__(self, database: Database, universe: DimensionUniverse, *,
                 attributes: Type[ButlerAttributeManager],
                 opaque: Type[OpaqueTableStorageManager],
                 dimensions: Type[DimensionRecordStorageManager],
                 collections: Type[CollectionManager],
                 datasets: Type[DatasetRecordStorageManager],
                 datastoreBridges: Type[DatastoreRegistryBridgeManager],
                 create: bool = False):
        self._db = database
        self.storageClasses = StorageClassFactory()
        with self._db.declareStaticTables(create=create) as context:
            self._attributes = attributes.initialize(self._db, context)
            self._dimensions = dimensions.initialize(self._db, context, universe=universe)
            self._collections = collections.initialize(self._db, context)
            self._datasets = datasets.initialize(self._db, context,
                                                 collections=self._collections,
                                                 universe=self.dimensions)
            self._opaque = opaque.initialize(self._db, context)
            self._datastoreBridges = datastoreBridges.initialize(self._db, context,
                                                                 opaque=self._opaque,
                                                                 datasets=datasets,
                                                                 universe=self.dimensions)
        self._collections.refresh()
        self._datasets.refresh(universe=self._dimensions.universe)

    def __str__(self) -> str:
        return str(self._db)

    def __repr__(self) -> str:
        return f"Registry({self._db!r}, {self.dimensions!r})"

    def isWriteable(self) -> bool:
        """Return `True` if this registry allows write operations, and `False`
        otherwise.
        """
        return self._db.isWriteable()

    @property
    def dimensions(self) -> DimensionUniverse:
        """All dimensions recognized by this `Registry` (`DimensionUniverse`).
        """
        return self._dimensions.universe

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Return a context manager that represents a transaction.
        """
        # TODO make savepoint=False the default.
        try:
            with self._db.transaction():
                yield
        except BaseException:
            # TODO: this clears the caches sometimes when we wouldn't actually
            # need to.  Can we avoid that?
            self._dimensions.clearCaches()
            raise

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
        self._opaque.register(tableName, spec)

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
        self._opaque[tableName].insert(*data)

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
        yield from self._opaque[tableName].fetch(**where)

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
        self._opaque[tableName].delete(**where)

    def registerCollection(self, name: str, type: CollectionType = CollectionType.TAGGED) -> None:
        """Add a new collection if one with the given name does not exist.

        Parameters
        ----------
        name : `str`
            The name of the collection to create.
        type : `CollectionType`
            Enum value indicating the type of collection to create.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent.
        """
        self._collections.register(name, type)

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
        MissingCollectionError
            Raised if no collection with the given name exists.
        """
        return self._collections.find(name).type

    def registerRun(self, name: str) -> None:
        """Add a new run if one with the given name does not exist.

        Parameters
        ----------
        name : `str`
            The name of the run to create.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent.
        """
        self._collections.register(name, CollectionType.RUN)

    @transactional
    def removeCollection(self, name: str) -> None:
        """Completely remove the given collection.

        Parameters
        ----------
        name : `str`
            The name of the collection to remove.

        Raises
        ------
        MissingCollectionError
            Raised if no collection with the given name exists.

        Notes
        -----
        If this is a `~CollectionType.RUN` collection, all datasets and quanta
        in it are also fully removed.  This requires that those datasets be
        removed (or at least trashed) from any datastores that hold them first.

        A collection may not be deleted as long as it is referenced by a
        `~CollectionType.CHAINED` collection; the ``CHAINED`` collection must
        be deleted or redefined first.
        """
        self._collections.remove(name)

    def getCollectionChain(self, parent: str) -> CollectionSearch:
        """Return the child collections in a `~CollectionType.CHAINED`
        collection.

        Parameters
        ----------
        parent : `str`
            Name of the chained collection.  Must have already been added via
            a call to `Registry.registerCollection`.

        Returns
        -------
        children : `CollectionSearch`
            An object that defines the search path of the collection.
            See :ref:`daf_butler_collection_expressions` for more information.

        Raises
        ------
        MissingCollectionError
            Raised if ``parent`` does not exist in the `Registry`.
        TypeError
            Raised if ``parent`` does not correspond to a
            `~CollectionType.CHAINED` collection.
        """
        record = self._collections.find(parent)
        if record.type is not CollectionType.CHAINED:
            raise TypeError(f"Collection '{parent}' has type {record.type.name}, not CHAINED.")
        assert isinstance(record, ChainedCollectionRecord)
        return record.children

    @transactional
    def setCollectionChain(self, parent: str, children: Any) -> None:
        """Define or redefine a `~CollectionType.CHAINED` collection.

        Parameters
        ----------
        parent : `str`
            Name of the chained collection.  Must have already been added via
            a call to `Registry.registerCollection`.
        children : `Any`
            An expression defining an ordered search of child collections,
            generally an iterable of `str`.  Restrictions on the dataset types
            to be searched can also be included, by passing mapping or an
            iterable containing tuples; see
            :ref:`daf_butler_collection_expressions` for more information.

        Raises
        ------
        MissingCollectionError
            Raised when any of the given collections do not exist in the
            `Registry`.
        TypeError
            Raised if ``parent`` does not correspond to a
            `~CollectionType.CHAINED` collection.
        ValueError
            Raised if the given collections contains a cycle.
        """
        record = self._collections.find(parent)
        if record.type is not CollectionType.CHAINED:
            raise TypeError(f"Collection '{parent}' has type {record.type.name}, not CHAINED.")
        assert isinstance(record, ChainedCollectionRecord)
        children = CollectionSearch.fromExpression(children)
        if children != record.children:
            record.update(self._collections, children)

    def registerDatasetType(self, datasetType: DatasetType) -> bool:
        """
        Add a new `DatasetType` to the Registry.

        It is not an error to register the same `DatasetType` twice.

        Parameters
        ----------
        datasetType : `DatasetType`
            The `DatasetType` to be added.

        Returns
        -------
        inserted : `bool`
            `True` if ``datasetType`` was inserted, `False` if an identical
            existing `DatsetType` was found.  Note that in either case the
            DatasetType is guaranteed to be defined in the Registry
            consistently with the given definition.

        Raises
        ------
        ValueError
            Raised if the dimensions or storage class are invalid.
        ConflictingDefinitionError
            Raised if this DatasetType is already registered with a different
            definition.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent.
        """
        _, inserted = self._datasets.register(datasetType)
        return inserted

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
        KeyError
            Requested named DatasetType could not be found in registry.
        """
        storage = self._datasets.find(name)
        if storage is None:
            raise KeyError(f"DatasetType '{name}' could not be found.")
        return storage.datasetType

    def findDataset(self, datasetType: Union[DatasetType, str], dataId: Optional[DataId] = None, *,
                    collections: Any, **kwargs: Any) -> Optional[DatasetRef]:
        """Find a dataset given its `DatasetType` and data ID.

        This can be used to obtain a `DatasetRef` that permits the dataset to
        be read from a `Datastore`. If the dataset is a component and can not
        be found using the provided dataset type, a dataset ref for the parent
        will be returned instead but with the correct dataset type.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.
        dataId : `dict` or `DataCoordinate`, optional
            A `dict`-like object containing the `Dimension` links that identify
            the dataset within a collection.
        collections
            An expression that fully or partially identifies the collections
            to search for the dataset, such as a `str`, `re.Pattern`, or
            iterable  thereof.  `...` can be used to return all collections.
            See :ref:`daf_butler_collection_expressions` for more information.
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
        LookupError
            Raised if one or more data ID keys are missing or the dataset type
            does not exist.
        MissingCollectionError
            Raised if any of ``collections`` does not exist in the registry.
        """
        if isinstance(datasetType, DatasetType):
            storage = self._datasets.find(datasetType.name)
            if storage is None:
                raise LookupError(f"DatasetType '{datasetType}' has not been registered.")
        else:
            storage = self._datasets.find(datasetType)
            if storage is None:
                raise LookupError(f"DatasetType with name '{datasetType}' has not been registered.")
        dataId = DataCoordinate.standardize(dataId, graph=storage.datasetType.dimensions,
                                            universe=self.dimensions, **kwargs)
        collections = CollectionSearch.fromExpression(collections)
        for collectionRecord in collections.iter(self._collections, datasetType=storage.datasetType):
            result = storage.find(collectionRecord, dataId)
            if result is not None:
                return result

        # fallback to the parent if we got nothing and this was a component
        if storage.datasetType.isComponent():
            parentType, _ = storage.datasetType.nameAndComponent()
            parentRef = self.findDataset(parentType, dataId, collections=collections, **kwargs)
            if parentRef is not None:
                # Should already conform and we know no components
                return DatasetRef(storage.datasetType, parentRef.dataId, id=parentRef.id,
                                  run=parentRef.run, conform=False, hasParentId=True)

        return None

    @transactional
    def insertDatasets(self, datasetType: Union[DatasetType, str], dataIds: Iterable[DataId],
                       run: str) -> List[DatasetRef]:
        """Insert one or more datasets into the `Registry`

        This always adds new datasets; to associate existing datasets with
        a new collection, use ``associate``.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.
        dataIds :  `~collections.abc.Iterable` of `dict` or `DataCoordinate`
            Dimension-based identifiers for the new datasets.
        run : `str`
            The name of the run that produced the datasets.

        Returns
        -------
        refs : `list` of `DatasetRef`
            Resolved `DatasetRef` instances for all given data IDs (in the same
            order).

        Raises
        ------
        ConflictingDefinitionError
            If a dataset with the same dataset type and data ID as one of those
            given already exists in ``run``.
        MissingCollectionError
            Raised if ``run`` does not exist in the registry.
        """
        if isinstance(datasetType, DatasetType):
            storage = self._datasets.find(datasetType.name)
            if storage is None:
                raise LookupError(f"DatasetType '{datasetType}' has not been registered.")
        else:
            storage = self._datasets.find(datasetType)
            if storage is None:
                raise LookupError(f"DatasetType with name '{datasetType}' has not been registered.")
        runRecord = self._collections.find(run)
        if runRecord.type is not CollectionType.RUN:
            raise TypeError("Given collection is of type {runRecord.type.name}; RUN collection required.")
        assert isinstance(runRecord, RunRecord)
        expandedDataIds = [self.expandDataId(dataId, graph=storage.datasetType.dimensions)
                           for dataId in dataIds]
        try:
            refs = list(storage.insert(runRecord, expandedDataIds))
        except sqlalchemy.exc.IntegrityError as err:
            raise ConflictingDefinitionError(f"A database constraint failure was triggered by inserting "
                                             f"one or more datasets of type {storage.datasetType} into "
                                             f"collection '{run}'. "
                                             f"This probably means a dataset with the same data ID "
                                             f"and dataset type already exists, but it may also mean a "
                                             f"dimension row is missing.") from err
        return refs

    def getDataset(self, id: int) -> Optional[DatasetRef]:
        """Retrieve a Dataset entry.

        Parameters
        ----------
        id : `int`
            The unique identifier for the dataset.

        Returns
        -------
        ref : `DatasetRef` or `None`
            A ref to the Dataset, or `None` if no matching Dataset
            was found.
        """
        ref = self._datasets.getDatasetRef(id, universe=self.dimensions)
        if ref is None:
            return None
        return ref

    @transactional
    def removeDatasets(self, refs: Iterable[DatasetRef]) -> None:
        """Remove datasets from the Registry.

        The datasets will be removed unconditionally from all collections, and
        any `Quantum` that consumed this dataset will instead be marked with
        having a NULL input.  `Datastore` records will *not* be deleted; the
        caller is responsible for ensuring that the dataset has already been
        removed from all Datastores.

        Parameters
        ----------
        refs : `Iterable` of `DatasetRef`
            References to the datasets to be removed.  Must include a valid
            ``id`` attribute, and should be considered invalidated upon return.

        Raises
        ------
        AmbiguousDatasetError
            Raised if any ``ref.id`` is `None`.
        OrphanedRecordError
            Raised if any dataset is still present in any `Datastore`.
        """
        for datasetType, refsForType in DatasetRef.groupByType(refs).items():
            storage = self._datasets.find(datasetType.name)
            assert storage is not None
            try:
                storage.delete(refsForType)
            except sqlalchemy.exc.IntegrityError as err:
                raise OrphanedRecordError("One or more datasets is still "
                                          "present in one or more Datastores.") from err

    @transactional
    def associate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        """Add existing datasets to a `~CollectionType.TAGGED` collection.

        If a DatasetRef with the same exact integer ID is already in a
        collection nothing is changed. If a `DatasetRef` with the same
        `DatasetType` and data ID but with different integer ID
        exists in the collection, `ConflictingDefinitionError` is raised.

        Parameters
        ----------
        collection : `str`
            Indicates the collection the datasets should be associated with.
        refs : `Iterable` [ `DatasetRef` ]
            An iterable of resolved `DatasetRef` instances that already exist
            in this `Registry`.

        Raises
        ------
        ConflictingDefinitionError
            If a Dataset with the given `DatasetRef` already exists in the
            given collection.
        AmbiguousDatasetError
            Raised if ``any(ref.id is None for ref in refs)``.
        MissingCollectionError
            Raised if ``collection`` does not exist in the registry.
        TypeError
            Raise adding new datasets to the given ``collection`` is not
            allowed.
        """
        collectionRecord = self._collections.find(collection)
        if collectionRecord.type is not CollectionType.TAGGED:
            raise TypeError(f"Collection '{collection}' has type {collectionRecord.type.name}, not TAGGED.")
        for datasetType, refsForType in DatasetRef.groupByType(refs).items():
            storage = self._datasets.find(datasetType.name)
            assert storage is not None
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
        """Remove existing datasets from a `~CollectionType.TAGGED` collection.

        ``collection`` and ``ref`` combinations that are not currently
        associated are silently ignored.

        Parameters
        ----------
        collection : `str`
            The collection the datasets should no longer be associated with.
        refs : `Iterable` [ `DatasetRef` ]
            An iterable of resolved `DatasetRef` instances that already exist
            in this `Registry`.

        Raises
        ------
        AmbiguousDatasetError
            Raised if any of the given dataset references is unresolved.
        MissingCollectionError
            Raised if ``collection`` does not exist in the registry.
        TypeError
            Raise adding new datasets to the given ``collection`` is not
            allowed.
        """
        collectionRecord = self._collections.find(collection)
        if collectionRecord.type is not CollectionType.TAGGED:
            raise TypeError(f"Collection '{collection}' has type {collectionRecord.type.name}; "
                            "expected TAGGED.")
        for datasetType, refsForType in DatasetRef.groupByType(refs).items():
            storage = self._datasets.find(datasetType.name)
            assert storage is not None
            storage.disassociate(collectionRecord, refsForType)

    def getDatastoreBridgeManager(self) -> DatastoreRegistryBridgeManager:
        """Return an object that allows a new `Datastore` instance to
        communicate with this `Registry`.

        Returns
        -------
        manager : `DatastoreRegistryBridgeManager`
            Object that mediates communication between this `Registry` and its
            associated datastores.
        """
        return self._datastoreBridges

    def getDatasetLocations(self, ref: DatasetRef) -> Iterable[str]:
        """Retrieve datastore locations for a given dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            A reference to the dataset for which to retrieve storage
            information.

        Returns
        -------
        datastores : `Iterable` [ `str` ]
            All the matching datastores holding this dataset.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        """
        return self._datastoreBridges.findDatastores(ref)

    def expandDataId(self, dataId: Optional[DataId] = None, *, graph: Optional[DimensionGraph] = None,
                     records: Optional[Mapping[DimensionElement, Optional[DimensionRecord]]] = None,
                     **kwargs: Any) -> ExpandedDataCoordinate:
        """Expand a dimension-based data ID to include additional information.

        Parameters
        ----------
        dataId : `DataCoordinate` or `dict`, optional
            Data ID to be expanded; augmented and overridden by ``kwds``.
        graph : `DimensionGraph`, optional
            Set of dimensions for the expanded ID.  If `None`, the dimensions
            will be inferred from the keys of ``dataId`` and ``kwds``.
            Dimensions that are in ``dataId`` or ``kwds`` but not in ``graph``
            are silently ignored, providing a way to extract and expand a
            subset of a data ID.
        records : `Mapping` [`DimensionElement`, `DimensionRecord`], optional
            Dimension record data to use before querying the database for that
            data.
        **kwargs
            Additional keywords are treated like additional key-value pairs for
            ``dataId``, extending and overriding

        Returns
        -------
        expanded : `ExpandedDataCoordinate`
            A data ID that includes full metadata for all of the dimensions it
            identifieds.
        """
        standardized = DataCoordinate.standardize(dataId, graph=graph, universe=self.dimensions, **kwargs)
        if isinstance(standardized, ExpandedDataCoordinate):
            return standardized
        elif isinstance(dataId, ExpandedDataCoordinate):
            records = NamedKeyDict(records) if records is not None else NamedKeyDict()
            records.update(dataId.records)
        else:
            records = NamedKeyDict(records) if records is not None else NamedKeyDict()
        keys = dict(standardized.byName())
        regions: List[lsst.sphgeom.ConvexPolygon] = []
        timespans: List[Timespan[astropy.time.Time]] = []
        for element in standardized.graph.primaryKeyTraversalOrder:
            record = records.get(element.name, ...)  # Use ... to mean not found; None might mean NULL
            if record is ...:
                storage = self._dimensions[element]
                record = storage.fetch(keys)
                records[element] = record
            if record is not None:
                for d in element.implied:
                    value = getattr(record, d.name)
                    if keys.setdefault(d.name, value) != value:
                        raise InconsistentDataIdError(
                            f"Data ID {standardized} has {d.name}={keys[d.name]!r}, "
                            f"but {element.name} implies {d.name}={value!r}."
                        )
                if element in standardized.graph.spatial and record.region is not None:
                    if any(record.region.relate(r) & lsst.sphgeom.DISJOINT for r in regions):
                        raise InconsistentDataIdError(f"Data ID {standardized}'s region for {element.name} "
                                                      f"is disjoint with those for other elements.")
                    regions.append(record.region)
                if element in standardized.graph.temporal:
                    if any(not record.timespan.overlaps(t) for t in timespans):
                        raise InconsistentDataIdError(f"Data ID {standardized}'s timespan for {element.name}"
                                                      f" is disjoint with those for other elements.")
                    timespans.append(record.timespan)
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
                records.update((d, None) for d in element.implied)
        return ExpandedDataCoordinate(standardized.graph, standardized.values(), records=records)

    def relateDataIds(self, a: DataId, b: DataId) -> Optional[ConsistentDataIds]:
        """Compare the keys and values of a pair of data IDs for consistency.

        See `ConsistentDataIds` for more information.

        Parameters
        ----------
        a : `dict` or `DataCoordinate`
            First data ID to be compared.
        b : `dict` or `DataCoordinate`
            Second data ID to be compared.

        Returns
        -------
        relationship : `ConsistentDataIds` or `None`
            Relationship information.  This is not `None` and coerces to
            `True` in boolean contexts if and only if the data IDs are
            consistent in terms of all common key-value pairs, all many-to-many
            join tables, and all spatial andtemporal relationships.
        """
        a = DataCoordinate.standardize(a, universe=self.dimensions)
        b = DataCoordinate.standardize(b, universe=self.dimensions)
        aFull = getattr(a, "full", None)
        bFull = getattr(b, "full", None)
        aBest = aFull if aFull is not None else a
        bBest = bFull if bFull is not None else b
        jointKeys = aBest.keys() & bBest.keys()
        # If any common values are not equal, we know they are inconsistent.
        if any(aBest[k] != bBest[k] for k in jointKeys):
            return None
        # If the graphs are equal, we know the data IDs are.
        if a.graph == b.graph:
            return ConsistentDataIds(contains=True, within=True, overlaps=bool(jointKeys))
        # Result is still inconclusive.  Try to expand a data ID containing
        # keys from both; that will fail if they are inconsistent.
        # First, if either input was already an ExpandedDataCoordinate, extract
        # its records so we don't have to query for them.
        records: NamedKeyDict[DimensionElement, Optional[DimensionRecord]] = NamedKeyDict()
        if isinstance(a, ExpandedDataCoordinate):
            records.update(a.records)
        if isinstance(b, ExpandedDataCoordinate):
            records.update(b.records)
        try:
            self.expandDataId({**a.byName(), **b.byName()}, graph=(a.graph | b.graph), records=records)
        except InconsistentDataIdError:
            return None
        # We know the answer is not `None`; time to figure out what it is.
        return ConsistentDataIds(
            contains=(a.graph >= b.graph),
            within=(a.graph <= b.graph),
            overlaps=bool(a.graph & b.graph),
        )

    def insertDimensionData(self, element: Union[DimensionElement, str],
                            *data: Union[Mapping[str, Any], DimensionRecord],
                            conform: bool = True) -> None:
        """Insert one or more dimension records into the database.

        Parameters
        ----------
        element : `DimensionElement` or `str`
            The `DimensionElement` or name thereof that identifies the table
            records will be inserted into.
        data : `dict` or `DimensionRecord` (variadic)
            One or more records to insert.
        conform : `bool`, optional
            If `False` (`True` is default) perform no checking or conversions,
            and assume that ``element`` is a `DimensionElement` instance and
            ``data`` is a one or more `DimensionRecord` instances of the
            appropriate subclass.
        """
        if conform:
            if isinstance(element, str):
                element = self.dimensions[element]
            records = [row if isinstance(row, DimensionRecord) else element.RecordClass.fromDict(row)
                       for row in data]
        else:
            # Ignore typing since caller said to trust them with conform=False.
            records = data  # type: ignore
        storage = self._dimensions[element]  # type: ignore
        storage.insert(*records)

    def syncDimensionData(self, element: Union[DimensionElement, str],
                          row: Union[Mapping[str, Any], DimensionRecord],
                          conform: bool = True) -> bool:
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
            ``data`` is a one or more `DimensionRecord` instances of the
            appropriate subclass.

        Returns
        -------
        inserted : `bool`
            `True` if a new row was inserted, `False` otherwise.

        Raises
        ------
        ConflictingDefinitionError
            Raised if the record exists in the database (according to primary
            key lookup) but is inconsistent with the given one.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent.
        """
        if conform:
            if isinstance(element, str):
                element = self.dimensions[element]
            record = row if isinstance(row, DimensionRecord) else element.RecordClass.fromDict(row)
        else:
            # Ignore typing since caller said to trust them with conform=False.
            record = row  # type: ignore
        storage = self._dimensions[element]  # type: ignore
        return storage.sync(record)

    def queryDatasetTypes(self, expression: Any = ..., *, components: Optional[bool] = None
                          ) -> Iterator[DatasetType]:
        """Iterate over the dataset types whose names match an expression.

        Parameters
        ----------
        expression : `Any`, optional
            An expression that fully or partially identifies the dataset types
            to return, such as a `str`, `re.Pattern`, or iterable thereof.
            `...` can be used to return all dataset types, and is the default.
            See :ref:`daf_butler_dataset_type_expressions` for more
            information.
        components : `bool`, optional
            If `True`, apply all expression patterns to component dataset type
            names as well.  If `False`, never apply patterns to components.
            If `None` (default), apply patterns to components only if their
            parent datasets were not matched by the expression.
            Fully-specified component datasets (`str` or `DatasetType`
            instances) are always included.

        Yields
        ------
        datasetType : `DatasetType`
            A `DatasetType` instance whose name matches ``expression``.
        """
        wildcard = CategorizedWildcard.fromExpression(expression, coerceUnrecognized=lambda d: d.name)
        if wildcard is Ellipsis:
            for datasetType in self._datasets:
                if components or not datasetType.isComponent():
                    yield datasetType
            return
        done: Set[str] = set()
        for name in wildcard.strings:
            storage = self._datasets.find(name)
            if storage is not None:
                done.add(storage.datasetType.name)
                yield storage.datasetType
        if wildcard.patterns:
            # If components (the argument) is None, we'll save component
            # dataset that we might want to match, but only if their parents
            # didn't get included.
            componentsForLater = []
            for datasetType in self._datasets:
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
                         collectionType: Optional[CollectionType] = None,
                         flattenChains: bool = False,
                         includeChains: Optional[bool] = None) -> Iterator[str]:
        """Iterate over the collections whose names match an expression.

        Parameters
        ----------
        expression : `Any`, optional
            An expression that fully or partially identifies the collections
            to return, such as a `str`, `re.Pattern`, or iterable thereof.
            `...` can be used to return all collections, and is the default.
            See :ref:`daf_butler_collection_expressions` for more
            information.
        datasetType : `DatasetType`, optional
            If provided, only yield collections that should be searched for
            this dataset type according to ``expression``.  If this is
            not provided, any dataset type restrictions in ``expression`` are
            ignored.
        collectionType : `CollectionType`, optional
            If provided, only yield collections of this type.
        flattenChains : `bool`, optional
            If `True` (`False` is default), recursively yield the child
            collections of matching `~CollectionType.CHAINED` collections.
        includeChains : `bool`, optional
            If `True`, yield records for matching `~CollectionType.CHAINED`
            collections.  Default is the opposite of ``flattenChains``: include
            either CHAINED collections or their children, but not both.

        Yields
        ------
        collection : `str`
            The name of a collection that matches ``expression``.
        """
        query = CollectionQuery.fromExpression(expression)
        for record in query.iter(self._collections, datasetType=datasetType, collectionType=collectionType,
                                 flattenChains=flattenChains, includeChains=includeChains):
            yield record.name

    def makeQueryBuilder(self, summary: QuerySummary) -> QueryBuilder:
        """Return a `QueryBuilder` instance capable of constructing and
        managing more complex queries than those obtainable via `Registry`
        interfaces.

        This is an advanced interface; downstream code should prefer
        `Registry.queryDimensions` and `Registry.queryDatasets` whenever those
        are sufficient.

        Parameters
        ----------
        summary : `QuerySummary`
            Object describing and categorizing the full set of dimensions that
            will be included in the query.

        Returns
        -------
        builder : `QueryBuilder`
            Object that can be used to construct and perform advanced queries.
        """
        return QueryBuilder(summary=summary,
                            collections=self._collections,
                            dimensions=self._dimensions,
                            datasets=self._datasets)

    def queryDimensions(self, dimensions: Union[Iterable[Union[Dimension, str]], Dimension, str], *,
                        dataId: Optional[DataId] = None,
                        datasets: Any = None,
                        collections: Any = None,
                        where: Optional[str] = None,
                        expand: bool = True,
                        components: Optional[bool] = None,
                        **kwargs: Any) -> Iterator[DataCoordinate]:
        """Query for and iterate over data IDs matching user-provided criteria.

        Parameters
        ----------
        dimensions : `Dimension` or `str`, or iterable thereof
            The dimensions of the data IDs to yield, as either `Dimension`
            instances or `str`.  Will be automatically expanded to a complete
            `DimensionGraph`.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        datasets : `Any`, optional
            An expression that fully or partially identifies dataset types
            that should constrain the yielded data IDs.  For example, including
            "raw" here would constrain the yielded ``instrument``,
            ``exposure``, ``detector``, and ``physical_filter`` values to only
            those for which at least one "raw" dataset exists in
            ``collections``.  Allowed types include `DatasetType`, `str`,
            `re.Pattern`, and iterables thereof.  Unlike other dataset type
            expressions, `...` is not permitted - it doesn't make sense to
            constrain data IDs on the existence of *all* datasets.
            See :ref:`daf_butler_dataset_type_expressions` for more
            information.
        collections: `Any`, optional
            An expression that fully or partially identifies the collections
            to search for datasets, such as a `str`, `re.Pattern`, or iterable
            thereof.  `...` can be used to return all collections.  Must be
            provided if ``datasets`` is, and is ignored if it is not.  See
            :ref:`daf_butler_collection_expressions` for more information.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.  See
            :ref:`daf_butler_dimension_expressions` for more information.
        expand : `bool`, optional
            If `True` (default) yield `ExpandedDataCoordinate` instead of
            minimal `DataCoordinate` base-class instances.
        components : `bool`, optional
            If `True`, apply all dataset expression patterns to component
            dataset type names as well.  If `False`, never apply patterns to
            components.  If `None` (default), apply patterns to components only
            if their parent datasets were not matched by the expression.
            Fully-specified component datasets (`str` or `DatasetType`
            instances) are always included.
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Yields
        ------
        dataId : `DataCoordinate`
            Data IDs matching the given query parameters.  Order is
            unspecified.
        """
        dimensions = iterable(dimensions)
        standardizedDataId = self.expandDataId(dataId, **kwargs)
        standardizedDatasetTypes = set()
        requestedDimensionNames = set(self.dimensions.extract(dimensions).names)
        if datasets is not None:
            if collections is None:
                raise TypeError("Cannot pass 'datasets' without 'collections'.")
            for datasetType in self.queryDatasetTypes(datasets, components=components):
                requestedDimensionNames.update(datasetType.dimensions.names)
                # If any matched dataset type is a component, just operate on
                # its parent instead, because Registry doesn't know anything
                # about what components exist, and here (unlike queryDatasets)
                # we don't care about returning them.
                parentDatasetTypeName, componentName = datasetType.nameAndComponent()
                if componentName is not None:
                    datasetType = self.getDatasetType(parentDatasetTypeName)
                standardizedDatasetTypes.add(datasetType)
            # Preprocess collections expression in case the original included
            # single-pass iterators (we'll want to use it multiple times
            # below).
            collections = CollectionQuery.fromExpression(collections)

        summary = QuerySummary(
            requested=DimensionGraph(self.dimensions, names=requestedDimensionNames),
            dataId=standardizedDataId,
            expression=where,
        )
        builder = self.makeQueryBuilder(summary)
        for datasetType in standardizedDatasetTypes:
            builder.joinDataset(datasetType, collections, isResult=False)
        query = builder.finish()
        predicate = query.predicate()
        for row in self._db.query(query.sql):
            if predicate(row):
                result = query.extractDataId(row)
                if expand:
                    yield self.expandDataId(result, records=standardizedDataId.records)
                else:
                    yield result

    def queryDatasets(self, datasetType: Any, *,
                      collections: Any,
                      dimensions: Optional[Iterable[Union[Dimension, str]]] = None,
                      dataId: Optional[DataId] = None,
                      where: Optional[str] = None,
                      deduplicate: bool = False,
                      expand: bool = True,
                      components: Optional[bool] = None,
                      **kwargs: Any) -> Iterator[DatasetRef]:
        """Query for and iterate over dataset references matching user-provided
        criteria.

        Parameters
        ----------
        datasetType
            An expression that fully or partially identifies the dataset types
            to be queried.  Allowed types include `DatasetType`, `str`,
            `re.Pattern`, and iterables thereof.  The special value `...` can
            be used to query all dataset types.  See
            :ref:`daf_butler_dataset_type_expressions` for more information.
        collections
            An expression that fully or partially identifies the collections
            to search for datasets, such as a `str`, `re.Pattern`, or iterable
            thereof.  `...` can be used to return all collections.  See
            :ref:`daf_butler_collection_expressions` for more information.
        dimensions : `~collections.abc.Iterable` of `Dimension` or `str`
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
        deduplicate : `bool`, optional
            If `True` (`False` is default), for each result data ID, only
            yield one `DatasetRef` of each `DatasetType`, from the first
            collection in which a dataset of that dataset type appears
            (according to the order of ``collections`` passed in).  If `True`,
            ``collections`` must not contain regular expressions and may not
            be `...`.
        expand : `bool`, optional
            If `True` (default) attach `ExpandedDataCoordinate` instead of
            minimal `DataCoordinate` base-class instances.
        components : `bool`, optional
            If `True`, apply all dataset expression patterns to component
            dataset type names as well.  If `False`, never apply patterns to
            components.  If `None` (default), apply patterns to components only
            if their parent datasets were not matched by the expression.
            Fully-specified component datasets (`str` or `DatasetType`
            instances) are always included.
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Yields
        ------
        ref : `DatasetRef`
            Dataset references matching the given query criteria.  These
            are grouped by `DatasetType` if the query evaluates to multiple
            dataset types, but order is otherwise unspecified.

        Raises
        ------
        TypeError
            Raised when the arguments are incompatible, such as when a
            collection wildcard is passed when ``deduplicate`` is `True`.

        Notes
        -----
        When multiple dataset types are queried in a single call, the
        results of this operation are equivalent to querying for each dataset
        type separately in turn, and no information about the relationships
        between datasets of different types is included.  In contexts where
        that kind of information is important, the recommended pattern is to
        use `queryDimensions` to first obtain data IDs (possibly with the
        desired dataset types and collections passed as constraints to the
        query), and then use multiple (generally much simpler) calls to
        `queryDatasets` with the returned data IDs passed as constraints.
        """
        # Standardize the collections expression.
        if deduplicate:
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
        elif datasetType.isComponent():
            # We were given a true DatasetType instance, but it's a component.
            # the composition dict will have exactly one item.
            parentName, componentName = datasetType.nameAndComponent()
            parentDatasetType = self.getDatasetType(parentName)
            composition = {parentDatasetType: [componentName]}
        if composition is not None:
            # We need to recurse.  Do that once for each parent dataset type.
            for parentDatasetType, componentNames in composition.items():
                for parentRef in self.queryDatasets(parentDatasetType, collections=collections,
                                                    dimensions=dimensions, dataId=standardizedDataId,
                                                    where=where, deduplicate=deduplicate):
                    # Loop over components, yielding one for each one for each
                    # one requested.
                    for componentName in componentNames:
                        if componentName is None:
                            yield parentRef
                        else:
                            yield parentRef.makeComponentRef(componentName)
            return
        # If we get here, there's no need to recurse (or we are already
        # recursing; there can only ever be one level of recursion).

        # The full set of dimensions in the query is the combination of those
        # needed for the DatasetType and those explicitly requested, if any.
        requestedDimensionNames = set(datasetType.dimensions.names)
        if dimensions is not None:
            requestedDimensionNames.update(self.dimensions.extract(dimensions).names)
        # Construct the summary structure needed to construct a QueryBuilder.
        summary = QuerySummary(
            requested=DimensionGraph(self.dimensions, names=requestedDimensionNames),
            dataId=standardizedDataId,
            expression=where,
        )
        builder = self.makeQueryBuilder(summary)
        # Add the dataset subquery to the query, telling the QueryBuilder to
        # include the rank of the selected collection in the results only if we
        # need to deduplicate.  Note that if any of the collections are
        # actually wildcard expressions, and we've asked for deduplication,
        # this will raise TypeError for us.
        if not builder.joinDataset(datasetType, collections, isResult=True, addRank=deduplicate):
            return
        query = builder.finish()
        predicate = query.predicate()
        if not deduplicate:
            # No need to de-duplicate across collections.
            for row in self._db.query(query.sql):
                if predicate(row):
                    dataId = query.extractDataId(row, graph=datasetType.dimensions)
                    if expand:
                        dataId = self.expandDataId(dataId, records=standardizedDataId.records)
                    yield query.extractDatasetRef(row, datasetType, dataId)[0]
        else:
            # For each data ID, yield only the DatasetRef with the lowest
            # collection rank.
            bestRefs = {}
            bestRanks: Dict[DataCoordinate, int] = {}
            for row in self._db.query(query.sql):
                if predicate(row):
                    ref, rank = query.extractDatasetRef(row, datasetType)
                    bestRank = bestRanks.get(ref.dataId, sys.maxsize)
                    assert rank is not None
                    if rank < bestRank:
                        bestRefs[ref.dataId] = ref
                        bestRanks[ref.dataId] = rank
            # If caller requested expanded data IDs, we defer that until here
            # so we do as little expansion as possible.
            if expand:
                for ref in bestRefs.values():
                    dataId = self.expandDataId(ref.dataId, records=standardizedDataId.records)
                    yield ref.expanded(dataId)
            else:
                yield from bestRefs.values()

    storageClasses: StorageClassFactory
    """All storage classes known to the registry (`StorageClassFactory`).
    """
