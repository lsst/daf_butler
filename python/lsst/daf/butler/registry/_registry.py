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
    "ConflictingDefinitionError",
    "ConsistentDataIds",
    "InconsistentDataIdError",
    "OrphanedRecordError",
    "Registry",
)

import contextlib
from dataclasses import dataclass
import sys
from typing import (
    Any,
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

import sqlalchemy

import lsst.sphgeom
from ..core import (
    Config,
    DataCoordinate,
    DataId,
    DatasetRef,
    DatasetType,
    Dimension,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
    DimensionUniverse,
    ExpandedDataCoordinate,
    FakeDatasetRef,
    StorageClassFactory,
)
from ..core import ddl
from ..core.utils import doImport, iterable, transactional
from ._config import RegistryConfig
from .queries import (
    DatasetRegistryStorage,
    QueryBuilder,
    QuerySummary,
)
from .tables import makeRegistryTableSpecs
from ._collectionType import CollectionType
from .wildcards import CollectionQuery, CollectionSearch
from .interfaces import DatabaseConflictError

if TYPE_CHECKING:
    from ..butlerConfig import ButlerConfig
    from ..core import (
        Quantum
    )
    from .interfaces import (
        CollectionManager,
        Database,
        OpaqueTableStorageManager,
        DimensionRecordStorageManager,
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


class InconsistentDataIdError(ValueError):
    """Exception raised when a data ID contains contradictory key-value pairs,
    according to dimension relationships.

    This can include the case where the data ID identifies mulitple spatial
    regions or timspans that are disjoint.
    """


class ConflictingDefinitionError(Exception):
    """Exception raised when trying to insert a database record when a
    conflicting record already exists.
    """


class OrphanedRecordError(Exception):
    """Exception raised when trying to remove or modify a database record
    that is still being used in some other table.
    """


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
        opaque = doImport(config["managers", "opaque"])
        dimensions = doImport(config["managers", "dimensions"])
        collections = doImport(config["managers", "collections"])
        return cls(database, universe, dimensions=dimensions, opaque=opaque, collections=collections,
                   create=create)

    def __init__(self, database: Database, universe: DimensionUniverse, *,
                 opaque: Type[OpaqueTableStorageManager],
                 dimensions: Type[DimensionRecordStorageManager],
                 collections: Type[CollectionManager],
                 create: bool = False):
        self._db = database
        self.storageClasses = StorageClassFactory()
        with self._db.declareStaticTables(create=create) as context:
            self._dimensions = dimensions.initialize(self._db, context, universe=universe)
            self._collections = collections.initialize(self._db, context)
            self._tables = context.addTableTuple(makeRegistryTableSpecs(self.dimensions, self._collections))
            self._opaque = opaque.initialize(self._db, context)
        self._collections.refresh()
        # TODO: we shouldn't be grabbing the private connection from the
        # Database instance like this, but it's a reasonable way to proceed
        # while we transition to using the Database API more.
        self._connection = self._db._connection
        self._datasetStorage = DatasetRegistryStorage(connection=self._connection,
                                                      universe=self.dimensions,
                                                      tables=self._tables._asdict(),
                                                      collections=self._collections)
        self._datasetTypes = {}

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
    def transaction(self):
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
            self._datasetTypes.clear()
            raise

    def registerOpaqueTable(self, tableName: str, spec: ddl.TableSpec):
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
    def insertOpaqueData(self, tableName: str, *data: dict):
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
    def deleteOpaqueData(self, tableName: str, **where: Any):
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

    def registerCollection(self, name: str, type: CollectionType = CollectionType.TAGGED):
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

    def registerRun(self, name: str):
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
    def removeCollection(self, name: str):
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
        return record.children

    @transactional
    def setCollectionChain(self, parent: str, children: Any):
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
        children = CollectionSearch.fromExpression(children)
        if children != record.children:
            record.update(self._collections, children)

    @transactional
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
        """
        # TODO: this implementation isn't concurrent, except *maybe* in SQLite
        # with aggressive locking (where starting a transaction is essentially
        # the same as grabbing a full-database lock).  Should be reimplemented
        # with Database.sync to fix this, but that may require schema changes
        # as well so we only have to synchronize one row to know if we have
        # inconsistent definitions.

        # If the DatasetType is already in the cache, we assume it's already in
        # the DB (note that we don't actually provide a way to remove them from
        # the DB).
        existingDatasetType = self._datasetTypes.get(datasetType.name)
        # If it's not in the cache, try to insert it.
        if existingDatasetType is None:
            try:
                with self._db.transaction():
                    self._db.insert(
                        self._tables.dataset_type,
                        {
                            "dataset_type_name": datasetType.name,
                            "storage_class": datasetType.storageClass.name,
                        }
                    )
            except sqlalchemy.exc.IntegrityError:
                # Insert failed on the only unique constraint on this table:
                # dataset_type_name.  So now the question is whether the one in
                # there is the same as the one we tried to insert.
                existingDatasetType = self.getDatasetType(datasetType.name)
            else:
                # If adding the DatasetType record itself succeeded, add its
                # dimensions (if any).  We don't guard this in a try block
                # because a problem with this insert means the database
                # content must be corrupted.
                if datasetType.dimensions:
                    self._db.insert(
                        self._tables.dataset_type_dimensions,
                        *[{"dataset_type_name": datasetType.name,
                           "dimension_name": dimensionName}
                          for dimensionName in datasetType.dimensions.names]
                    )
                # Update the cache.
                self._datasetTypes[datasetType.name] = datasetType
                # Also register component DatasetTypes (if any).
                for compName, compStorageClass in datasetType.storageClass.components.items():
                    compType = DatasetType(datasetType.componentTypeName(compName),
                                           dimensions=datasetType.dimensions,
                                           storageClass=compStorageClass)
                    self.registerDatasetType(compType)
                # Inserts succeeded, nothing left to do here.
                return True
        # A DatasetType with this name exists, check if is equal
        if datasetType == existingDatasetType:
            return False
        else:
            raise ConflictingDefinitionError(f"DatasetType: {datasetType} != existing {existingDatasetType}")

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
        datasetType = self._datasetTypes.get(name)
        if datasetType is None:
            # Get StorageClass from DatasetType table
            result = self._db.query(
                sqlalchemy.sql.select(
                    [self._tables.dataset_type.c.storage_class]
                ).where(
                    self._tables.dataset_type.columns.dataset_type_name == name
                )
            ).fetchone()

            if result is None:
                raise KeyError("Could not find entry for datasetType {}".format(name))

            storageClass = self.storageClasses.getStorageClass(result["storage_class"])
            # Get Dimensions (if any) from DatasetTypeDimensions table
            result = self._db.query(
                sqlalchemy.sql.select(
                    [self._tables.dataset_type_dimensions.columns.dimension_name]
                ).where(
                    self._tables.dataset_type_dimensions.columns.dataset_type_name == name
                )
            ).fetchall()
            dimensions = DimensionGraph(self.dimensions, names=(r[0] for r in result) if result else ())
            datasetType = DatasetType(name=name,
                                      storageClass=storageClass,
                                      dimensions=dimensions)
            self._datasetTypes[name] = datasetType
        return datasetType

    def _makeDatasetRefFromRow(self, row: sqlalchemy.engine.RowProxy,
                               datasetType: Optional[DatasetType] = None,
                               dataId: Optional[DataCoordinate] = None):
        """Construct a DatasetRef from the result of a query on the Dataset
        table.

        Parameters
        ----------
        row : `sqlalchemy.engine.RowProxy`.
            Row of a query that contains all columns from the `Dataset` table.
            May include additional fields (which will be ignored).
        datasetType : `DatasetType`, optional
            `DatasetType` associated with this dataset.  Will be retrieved
            if not provided.  If provided, the caller guarantees that it is
            already consistent with what would have been retrieved from the
            database.
        dataId : `DataCoordinate`, optional
            Dimensions associated with this dataset.  Will be retrieved if not
            provided.  If provided, the caller guarantees that it is already
            consistent with what would have been retrieved from the database.

        Returns
        -------
        ref : `DatasetRef`.
            A new `DatasetRef` instance.
        """
        if datasetType is None:
            datasetType = self.getDatasetType(row["dataset_type_name"])
        runRecord = self._collections[row[self._collections.getRunForeignKeyName()]]
        assert runRecord is not None, "Should be guaranteed by foreign key constraints."
        run = runRecord.name
        datasetRefHash = row["dataset_ref_hash"]
        if dataId is None:
            # TODO: should we expand here?
            dataId = DataCoordinate.standardize(
                row,
                graph=datasetType.dimensions,
                universe=self.dimensions
            )
        # Get components (if present)
        components = {}
        if datasetType.storageClass.isComposite():
            t = self._tables
            columns = list(t.dataset.columns)
            columns.append(t.dataset_composition.columns.component_name)
            results = self._db.query(
                sqlalchemy.sql.select(
                    columns
                ).select_from(
                    t.dataset.join(
                        t.dataset_composition,
                        (t.dataset.columns.dataset_id == t.dataset_composition.columns.component_dataset_id)
                    )
                ).where(
                    t.dataset_composition.columns.parent_dataset_id == row["dataset_id"]
                )
            ).fetchall()
            for result in results:
                componentName = result["component_name"]
                componentDatasetType = DatasetType(
                    DatasetType.nameWithComponent(datasetType.name, componentName),
                    dimensions=datasetType.dimensions,
                    storageClass=datasetType.storageClass.components[componentName]
                )
                components[componentName] = self._makeDatasetRefFromRow(result, dataId=dataId,
                                                                        datasetType=componentDatasetType)
            if not components.keys() <= datasetType.storageClass.components.keys():
                raise RuntimeError(
                    f"Inconsistency detected between dataset and storage class definitions: "
                    f"{datasetType.storageClass.name} has components "
                    f"{set(datasetType.storageClass.components.keys())}, "
                    f"but dataset has components {set(components.keys())}"
                )
        return DatasetRef(datasetType=datasetType, dataId=dataId, id=row["dataset_id"], run=run,
                          hash=datasetRefHash, components=components)

    def findDataset(self, datasetType: Union[DatasetType, str], dataId: Optional[DataId] = None, *,
                    collections: Any, **kwds: Any) -> Optional[DatasetRef]:
        """Find a dataset given its `DatasetType` and data ID.

        This can be used to obtain a `DatasetRef` that permits the dataset to
        be read from a `Datastore`.

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
        **kwds
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
            Raised if one or more data ID keys are missing.
        MissingCollectionError
            Raised if any of ``collections`` does not exist in the registry.
        """
        if not isinstance(datasetType, DatasetType):
            datasetType = self.getDatasetType(datasetType)
        dataId = DataCoordinate.standardize(dataId, graph=datasetType.dimensions,
                                            universe=self.dimensions, **kwds)
        collections = CollectionSearch.fromExpression(collections)
        for collectionRecord in collections.iter(self._collections, datasetType=datasetType):
            if collectionRecord.type is CollectionType.TAGGED:
                collectionColumn = \
                    self._tables.dataset_collection.columns[self._collections.getCollectionForeignKeyName()]
                fromClause = self._tables.dataset.join(self._tables.dataset_collection)
            elif collectionRecord.type is CollectionType.RUN:
                collectionColumn = self._tables.dataset.columns[self._collections.getRunForeignKeyName()]
                fromClause = self._tables.dataset
            else:
                raise NotImplementedError(f"Unrecognized CollectionType: '{collectionRecord.type}'.")
            whereTerms = [
                self._tables.dataset.columns.dataset_type_name == datasetType.name,
                collectionColumn == collectionRecord.key,
            ]
            whereTerms.extend(self._tables.dataset.columns[name] == dataId[name] for name in dataId.keys())
            query = self._tables.dataset.select().select_from(
                fromClause
            ).where(
                sqlalchemy.sql.and_(*whereTerms)
            )
            result = self._db.query(query).fetchone()
            if result is not None:
                return self._makeDatasetRefFromRow(result, datasetType=datasetType, dataId=dataId)
        return None

    @transactional
    def insertDatasets(self, datasetType: Union[DatasetType, str], dataIds: Iterable[DataId],
                       run: str, *, producer: Optional[Quantum] = None, recursive: bool = False
                       ) -> List[DatasetRef]:
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
        producer : `Quantum`
            Unit of work that produced the datasets.  May be `None` to store
            no provenance information, but if present the `Quantum` must
            already have been added to the Registry.
        recursive : `bool`
            If True, recursively add datasets and attach entries for component
            datasets as well.

        Returns
        -------
        refs : `list` of `DatasetRef`
            Resolved `DatasetRef` instances for all given data IDs (in the same
            order).

        Raises
        ------
        ConflictingDefinitionError
            If a dataset with the same dataset type and data ID as one of those
            given already exists in the given collection.
        MissingCollectionError
            Raised if ``run`` does not exist in the registry.
        """
        if not isinstance(datasetType, DatasetType):
            datasetType = self.getDatasetType(datasetType)
        rows = []
        refs = []
        runRecord = self._collections.find(run)
        base = {
            "dataset_type_name": datasetType.name,
            self._collections.getRunForeignKeyName(): runRecord.key,
            "quantum_id": producer.id if producer is not None else None,
        }
        # Expand data IDs and build both a list of unresolved DatasetRefs
        # and a list of dictionary rows for the dataset table.
        for dataId in dataIds:
            ref = DatasetRef(datasetType, self.expandDataId(dataId, graph=datasetType.dimensions))
            refs.append(ref)
            row = dict(base, dataset_ref_hash=ref.hash)
            for dimension, value in ref.dataId.full.items():
                row[dimension.name] = value
            rows.append(row)
        # Actually insert into the dataset table.
        try:
            datasetIds = self._db.insert(self._tables.dataset, *rows, returnIds=True)
        except sqlalchemy.exc.IntegrityError as err:
            raise ConflictingDefinitionError(
                f"Constraint violation while inserting datasets into run {run}. "
                f"This usually means that one or more datasets with the same dataset type and data ID "
                f"already exist in the collection, but it may be a foreign key violation."
            ) from err
        # Resolve the DatasetRefs with the autoincrement IDs we generated.
        refs = [ref.resolved(id=datasetId, run=run) for datasetId, ref in zip(datasetIds, refs)]
        if recursive and datasetType.isComposite():
            # Insert component rows by recursing, and gather a single big list
            # of rows to insert into the dataset_composition table.
            compositionRows = []
            for componentName in datasetType.storageClass.components:
                componentDatasetType = datasetType.makeComponentDatasetType(componentName)
                componentRefs = self.insertDatasets(componentDatasetType,
                                                    dataIds=(ref.dataId for ref in refs),
                                                    run=run,
                                                    producer=producer,
                                                    recursive=True)
                for parentRef, componentRef in zip(refs, componentRefs):
                    parentRef._components[componentName] = componentRef
                    compositionRows.append({
                        "parent_dataset_id": parentRef.id,
                        "component_dataset_id": componentRef.id,
                        "component_name": componentName,
                    })
            if compositionRows:
                self._db.insert(self._tables.dataset_composition, *compositionRows)
        return refs

    def getDataset(self, id: int, datasetType: Optional[DatasetType] = None,
                   dataId: Optional[DataCoordinate] = None) -> Optional[DatasetRef]:
        """Retrieve a Dataset entry.

        Parameters
        ----------
        id : `int`
            The unique identifier for the Dataset.
        datasetType : `DatasetType`, optional
            The `DatasetType` of the dataset to retrieve.  This is used to
            short-circuit retrieving the `DatasetType`, so if provided, the
            caller is guaranteeing that it is what would have been retrieved.
        dataId : `DataCoordinate`, optional
            A `Dimension`-based identifier for the dataset within a
            collection, possibly containing additional metadata. This is used
            to short-circuit retrieving the dataId, so if provided, the
            caller is guaranteeing that it is what would have been retrieved.

        Returns
        -------
        ref : `DatasetRef`
            A ref to the Dataset, or `None` if no matching Dataset
            was found.
        """
        result = self._db.query(
            self._tables.dataset.select().where(
                self._tables.dataset.columns.dataset_id == id
            )
        ).fetchone()
        if result is None:
            return None
        return self._makeDatasetRefFromRow(result, datasetType=datasetType, dataId=dataId)

    @transactional
    def removeDatasets(self, refs: Iterable[DatasetRef], *, recursive: bool = True):
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
        recursive : `bool`, optional
            If `True`, remove all component datasets as well.  Note that
            this only removes components that are actually included in the
            given `DatasetRef` instances, which may not be the same as those in
            the database (especially if they were obtained from
            `queryDatasets`, which does not populate `DatasetRef.components`).

        Raises
        ------
        AmbiguousDatasetError
            Raised if any ``ref.id`` is `None`.
        OrphanedRecordError
            Raised if any dataset is still present in any `Datastore`.
        """
        if recursive:
            refs = DatasetRef.flatten(refs)
        rows = [{"dataset_id": ref.getCheckedId()} for ref in refs]
        # Remove the dataset records.  We rely on ON DELETE clauses to
        # take care of other dependencies:
        #  - ON DELETE CASCADE will remove dataset_composition rows.
        #  - ON DELETE CASCADE will remove dataset_collection rows.
        #  - ON DELETE SET NULL will apply to dataset_consumer rows, making it
        #    clear that the provenance of any quanta that used this dataset as
        #    an input is now incomplete.
        try:
            self._db.delete(self._tables.dataset, ["dataset_id"], *rows)
        except sqlalchemy.exc.IntegrityError as err:
            raise OrphanedRecordError("One or more datasets is still "
                                      "present in one or more Datastores.") from err

    @transactional
    def attachComponent(self, name: str, parent: DatasetRef, component: DatasetRef):
        """Attach a component to a dataset.

        Parameters
        ----------
        name : `str`
            Name of the component.
        parent : `DatasetRef`
            A reference to the parent dataset. Will be updated to reference
            the component.
        component : `DatasetRef`
            A reference to the component dataset.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``parent.id`` or ``component.id`` is `None`.
        """
        # TODO Insert check for component name and type against
        # parent.storageClass specified components
        values = dict(component_name=name,
                      parent_dataset_id=parent.getCheckedId(),
                      component_dataset_id=component.getCheckedId())
        self._db.insert(self._tables.dataset_composition, values)
        parent._components[name] = component

    @transactional
    def associate(self, collection: str, refs: Iterable[DatasetRef], *, recursive: bool = True):
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
        recursive : `bool`, optional
            If `True`, associate all component datasets as well.  Note that
            this only associates components that are actually included in the
            given `DatasetRef` instances, which may not be the same as those in
            the database (especially if they were obtained from
            `queryDatasets`, which does not populate `DatasetRef.components`).

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
        if recursive:
            refs = DatasetRef.flatten(refs)
        rows = [{"dataset_id": ref.getCheckedId(),
                 "dataset_ref_hash": ref.hash,
                 self._collections.getCollectionForeignKeyName(): collectionRecord.key}
                for ref in refs]
        try:
            self._db.replace(self._tables.dataset_collection, *rows)
        except sqlalchemy.exc.IntegrityError as err:
            raise ConflictingDefinitionError(
                f"Constraint violation while associating datasets with collection {collection}. "
                f"This probably means that one or more datasets with the same dataset type and data ID "
                f"already exist in the collection, but it may also indicate that the datasets do not exist."
            ) from err

    @transactional
    def disassociate(self, collection: str, refs: Iterable[DatasetRef], *, recursive: bool = True):
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
        recursive : `bool`, optional
            If `True`, disassociate all component datasets as well.  Note that
            this only disassociates components that are actually included in
            the given `DatasetRef` instances, which may not be the same as
            those in the database (especially if they were obtained from
            `queryDatasets`, which does not populate `DatasetRef.components`).

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``any(ref.id is None for ref in refs)``.
        MissingCollectionError
            Raised if ``collection`` does not exist in the registry.
        TypeError
            Raise adding new datasets to the given ``collection`` is not
            allowed.
        """
        collectionFieldName = self._collections.getCollectionForeignKeyName()
        collectionRecord = self._collections.find(collection)
        if collectionRecord.type is not CollectionType.TAGGED:
            raise TypeError(f"Collection '{collection}' has type {collectionRecord.type.name}; "
                            "expected TAGGED.")
        if recursive:
            refs = DatasetRef.flatten(refs)
        rows = [{"dataset_id": ref.getCheckedId(), collectionFieldName: collectionRecord.key}
                for ref in refs]
        self._db.delete(self._tables.dataset_collection, ["dataset_id", collectionFieldName], *rows)

    @transactional
    def insertDatasetLocations(self, datastoreName: str, refs: Iterable[DatasetRef]):
        """Record that a datastore holds the given datasets.

        Typically used by `Datastore`.

        Parameters
        ----------
        datastoreName : `str`
            Name of the datastore holding these datasets.
        refs : `~collections.abc.Iterable` of `DatasetRef`
            References to the datasets.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``any(ref.id is None for ref in refs)``.
        """
        self._db.insert(
            self._tables.dataset_location,
            *[{"datastore_name": datastoreName, "dataset_id": ref.getCheckedId()} for ref in refs]
        )

    @transactional
    def moveDatasetLocationToTrash(self, datastoreName: str, refs: Iterable[DatasetRef]):
        """Move the dataset location information to trash.

        Parameters
        ----------
        datastoreName : `str`
            Name of the datastore holding these datasets.
        refs : `~collections.abc.Iterable` of `DatasetRef`
            References to the datasets.
        """
        # We only want to move rows that already exist in the main table
        filtered = self.checkDatasetLocations(datastoreName, refs)
        self.canDeleteDatasetLocations(datastoreName, filtered)
        self.removeDatasetLocation(datastoreName, filtered)

    @transactional
    def canDeleteDatasetLocations(self, datastoreName: str, refs: Iterable[DatasetRef]):
        """Record that a datastore can delete this dataset

        Parameters
        ----------
        datastoreName : `str`
            Name of the datastore holding these datasets.
        refs : `~collections.abc.Iterable` of `DatasetRef`
            References to the datasets.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``any(ref.id is None for ref in refs)``.
        """
        self._db.insert(
            self._tables.dataset_location_trash,
            *[{"datastore_name": datastoreName, "dataset_id": ref.getCheckedId()} for ref in refs]
        )

    def checkDatasetLocations(self, datastoreName: str, refs: Iterable[DatasetRef]) -> List[DatasetRef]:
        """Check which refs are listed for this datastore.

        Parameters
        ----------
        datastoreName : `str`
            Name of the datastore holding these datasets.
        refs : `~collections.abc.Iterable` of `DatasetRef`
            References to the datasets.

        Returns
        -------
        present : `list` of `DatasetRef`
            All the `DatasetRef` that are listed.
        """

        table = self._tables.dataset_location
        result = self._db.query(
            sqlalchemy.sql.select(
                [table.columns.datastore_name, table.columns.dataset_id]
            ).where(
                sqlalchemy.sql.and_(table.columns.dataset_id.in_([ref.id for ref in refs]),
                                    table.columns.datastore_name == datastoreName)
            )
        ).fetchall()

        matched_ids = {r["dataset_id"] for r in result}
        return [ref for ref in refs if ref.id in matched_ids]

    def getDatasetLocations(self, ref: DatasetRef) -> Set[str]:
        """Retrieve datastore locations for a given dataset.

        Typically used by `Datastore`.

        Parameters
        ----------
        ref : `DatasetRef`
            A reference to the dataset for which to retrieve storage
            information.

        Returns
        -------
        datastores : `set` of `str`
            All the matching datastores holding this dataset. Empty set
            if the dataset does not exist anywhere.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        """
        table = self._tables.dataset_location
        result = self._db.query(
            sqlalchemy.sql.select(
                [table.columns.datastore_name]
            ).where(
                table.columns.dataset_id == ref.id
            )
        ).fetchall()
        return {r["datastore_name"] for r in result}

    @transactional
    def getTrashedDatasets(self, datastoreName: str) -> Set[FakeDatasetRef]:
        """Retrieve all the dataset ref IDs that are in the trash
        associated with the specified datastore.

        Parameters
        ----------
        datastoreName : `str`
            The relevant datastore name to use.

        Returns
        -------
        ids : `set` of `FakeDatasetRef`
            The IDs of datasets that can be safely removed from this datastore.
            Can be empty.
        """
        table = self._tables.dataset_location_trash
        result = self._db.query(
            sqlalchemy.sql.select(
                [table.columns.dataset_id]
            ).where(
                table.columns.datastore_name == datastoreName
            )
        ).fetchall()
        return {FakeDatasetRef(r["dataset_id"]) for r in result}

    @transactional
    def emptyDatasetLocationsTrash(self, datastoreName: str, refs: Iterable[FakeDatasetRef]) -> None:
        """Remove datastore location associated with these datasets from trash.

        Typically used by `Datastore` when a dataset is removed.

        Parameters
        ----------
        datastoreName : `str`
            Name of this `Datastore`.
        refs : iterable of `FakeDatasetRef`
            The dataset IDs to be removed.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        """
        if not refs:
            return
        self._db.delete(
            self._tables.dataset_location_trash,
            ["dataset_id", "datastore_name"],
            *[{"dataset_id": ref.id, "datastore_name": datastoreName} for ref in refs]
        )

    @transactional
    def removeDatasetLocation(self, datastoreName: str, refs: Iterable[DatasetRef]) -> None:
        """Remove datastore location associated with this dataset.

        Typically used by `Datastore` when a dataset is removed.

        Parameters
        ----------
        datastoreName : `str`
            Name of this `Datastore`.
        refs : iterable of `DatasetRef`
            A reference to the dataset for which information is to be removed.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        """
        if not refs:
            return
        self._db.delete(
            self._tables.dataset_location,
            ["dataset_id", "datastore_name"],
            *[{"dataset_id": ref.getCheckedId(), "datastore_name": datastoreName} for ref in refs]
        )

    def expandDataId(self, dataId: Optional[DataId] = None, *, graph: Optional[DimensionGraph] = None,
                     records: Optional[Mapping[DimensionElement, DimensionRecord]] = None, **kwds):
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
        records : mapping [`DimensionElement`, `DimensionRecord`], optional
            Dimension record data to use before querying the database for that
            data.
        **kwds
            Additional keywords are treated like additional key-value pairs for
            ``dataId``, extending and overriding

        Returns
        -------
        expanded : `ExpandedDataCoordinate`
            A data ID that includes full metadata for all of the dimensions it
            identifieds.
        """
        standardized = DataCoordinate.standardize(dataId, graph=graph, universe=self.dimensions, **kwds)
        if isinstance(standardized, ExpandedDataCoordinate):
            return standardized
        elif isinstance(dataId, ExpandedDataCoordinate):
            records = dict(records) if records is not None else {}
            records.update(dataId.records)
        else:
            records = dict(records) if records is not None else {}
        keys = dict(standardized)
        regions = []
        timespans = []
        for element in standardized.graph.primaryKeyTraversalOrder:
            record = records.get(element.name, ...)  # Use ... to mean not found; None might mean NULL
            if record is ...:
                storage = self._dimensions[element]
                record = storage.fetch(keys)
                records[element] = record
            if record is not None:
                for d in element.implied:
                    value = getattr(record, d.name)
                    if keys.setdefault(d, value) != value:
                        raise InconsistentDataIdError(f"Data ID {standardized} has {d.name}={keys[d]!r}, "
                                                      f"but {element.name} implies {d.name}={value!r}.")
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
                        f"but it is marked alwaysJoin=True; this means one or more dimensions are not "
                        f"related."
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
        records = {}
        if hasattr(a, "records"):
            records.update(a.records)
        if hasattr(b, "records"):
            records.update(b.records)
        try:
            self.expandDataId({**a, **b}, graph=(a.graph | b.graph), records=records)
        except InconsistentDataIdError:
            return None
        # We know the answer is not `None`; time to figure out what it is.
        return ConsistentDataIds(
            contains=(a.graph >= b.graph),
            within=(a.graph <= b.graph),
            overlaps=bool(a.graph & b.graph),
        )

    def insertDimensionData(self, element: Union[DimensionElement, str],
                            *data: Union[dict, DimensionRecord],
                            conform: bool = True):
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
            element = self.dimensions[element]  # if this is a name, convert it to a true DimensionElement.
            records = [element.RecordClass.fromDict(row) if not type(row) is element.RecordClass else row
                       for row in data]
        else:
            records = data
        storage = self._dimensions[element]
        storage.insert(*records)

    def syncDimensionData(self, element: Union[DimensionElement, str],
                          row: Union[dict, DimensionRecord],
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
            element = self.dimensions[element]  # if this is a name, convert it to a true DimensionElement.
            record = element.RecordClass.fromDict(row) if not type(row) is element.RecordClass else row
        else:
            record = row
        storage = self._dimensions[element]
        try:
            return storage.sync(record)
        except DatabaseConflictError as err:
            raise ConflictingDefinitionError(str(err)) from err

    def queryDatasetTypes(self, expression: Any = ...) -> Iterator[DatasetType]:
        """Iterate over the dataset types whose names match an expression.

        Parameters
        ----------
        expression : `Any`, optional
            An expression that fully or partially identifies the dataset types
            to return, such as a `str`, `re.Pattern`, or iterable thereof.
            `...` can be used to return all dataset types, and is the default.
            See :ref:`daf_butler_dataset_type_expressions` for more
            information.

        Yields
        ------
        datasetType : `DatasetType`
            A `DatasetType` instance whose name matches ``expression``.
        """
        yield from self._datasetStorage.fetchDatasetTypes(expression)

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
        return QueryBuilder(connection=self._connection, summary=summary,
                            dimensionStorage=self._dimensions,
                            datasetStorage=self._datasetStorage)

    def queryDimensions(self, dimensions: Union[Iterable[Union[Dimension, str]], Dimension, str], *,
                        dataId: Optional[DataId] = None,
                        datasets: Any = None,
                        collections: Any = None,
                        where: Optional[str] = None,
                        expand: bool = True,
                        **kwds) -> Iterator[DataCoordinate]:
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
        kwds
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
        standardizedDataId = self.expandDataId(dataId, **kwds)
        standardizedDatasetTypes = []
        requestedDimensionNames = set(self.dimensions.extract(dimensions).names)
        if datasets is not None:
            if collections is None:
                raise TypeError("Cannot pass 'datasets' without 'collections'.")
            for datasetType in self._datasetStorage.fetchDatasetTypes(datasets):
                requestedDimensionNames.update(datasetType.dimensions.names)
                standardizedDatasetTypes.append(datasetType)
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
        for row in query.execute():
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
                      **kwds) -> Iterator[DatasetRef]:
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
        kwds
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
        standardizedDataId = self.expandDataId(dataId, **kwds)
        # If the datasetType passed isn't actually a DatasetType, expand it
        # (it could be an expression that yields multiple DatasetTypes) and
        # recurse.
        if not isinstance(datasetType, DatasetType):
            for trueDatasetType in self._datasetStorage.fetchDatasetTypes(datasetType):
                yield from self.queryDatasets(trueDatasetType, collections=collections,
                                              dimensions=dimensions, dataId=standardizedDataId,
                                              where=where, deduplicate=deduplicate)
            return
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
            for row in query.execute():
                if predicate(row):
                    dataId = query.extractDataId(row, graph=datasetType.dimensions)
                    if expand:
                        dataId = self.expandDataId(dataId, records=standardizedDataId.records)
                    yield query.extractDatasetRef(row, datasetType, dataId)[0]
        else:
            # For each data ID, yield only the DatasetRef with the lowest
            # collection rank.
            bestRefs = {}
            bestRanks = {}
            for row in query.execute():
                if predicate(row):
                    ref, rank = query.extractDatasetRef(row, datasetType)
                    bestRank = bestRanks.get(ref.dataId, sys.maxsize)
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

    dimensions: DimensionUniverse
    """The universe of all dimensions known to the registry
    (`DimensionUniverse`).
    """

    storageClasses: StorageClassFactory
    """All storage classes known to the registry (`StorageClassFactory`).
    """
