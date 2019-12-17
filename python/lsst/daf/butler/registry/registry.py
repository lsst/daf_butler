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

__all__ = ["Registry", "AmbiguousDatasetError", "ConflictingDefinitionError", "OrphanedRecordError"]

import contextlib
import sys
from typing import (
    Any,
    FrozenSet,
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
    StorageClassFactory,
)
from ..core.dimensions.storage import setupDimensionStorage
from ..core.queries import (
    CollectionsExpression,
    DatasetRegistryStorage,
    DatasetTypeExpression,
    QueryBuilder,
    QuerySummary,
)
from ..core import ddl
from ..core.utils import iterable, transactional, NamedKeyDict
from .config import RegistryConfig

from .tables import makeRegistryTableSpecs

if TYPE_CHECKING:
    from ..butlerConfig import ButlerConfig
    from ..core import (
        Quantum
    )
    from .interfaces import Database


class AmbiguousDatasetError(Exception):
    """Exception raised when a `DatasetRef` has no ID and a `Registry`
    operation requires one.
    """


class ConflictingDefinitionError(Exception):
    """Exception raised when trying to insert a database record when a
    conflicting record already exists.
    """


class OrphanedRecordError(Exception):
    """Exception raised when trying to remove or modify a database record
    that is still being used in some other table.
    """


def _expandComponents(refs: Iterable[DatasetRef]) -> Iterator[DatasetRef]:
    """Expand an iterable of datasets to include its components.

    Parameters
    ----------
    refs : iterable of `DatasetRef`
        An iterable of `DatasetRef` instances.

    Yields
    ------
    refs : `DatasetRef`
        Recursively expanded datasets.
    """
    for ref in refs:
        yield ref
        yield from _expandComponents(ref.components.values())


def _checkAndGetId(ref: DatasetRef) -> int:
    """Return the ID of the given `DatasetRef`, or raise if it is `None`.

    This trivial function exists to allow operations that would otherwise be
    natural list comprehensions to check that the ID is not `None` as well.

    Parameters
    ----------
    ref : `DatasetRef`
        Dataset reference.

    Returns
    -------
    id : `int`
        ``ref.id``

    Raises
    ------
    AmbiguousDatasetError
        Raised if ``ref.id`` is `None`.
    """
    if ref.id is None:
        raise AmbiguousDatasetError("Dataset ID must not be `None`.")
    return ref.id


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
                   butlerRoot: Optional[str] = None) -> Registry:
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
                                         namespace=config.get("namespace"))
        dimensions = DimensionUniverse(config)
        return cls(database=database, dimensions=dimensions, create=create)

    def __init__(self, database: Database, dimensions: DimensionUniverse, *, create: bool = False):
        self._db = database
        self.dimensions = dimensions
        self.storageClasses = StorageClassFactory()
        dimensionTables = {}
        with self._db.declareStaticTables(create=create) as context:
            for name, spec in self.dimensions.makeSchemaSpec().items():
                dimensionTables[name] = context.addTable(name, spec)
            self._tables = context.addTableTuple(makeRegistryTableSpecs(self.dimensions))
        # TODO: we shouldn't be grabbing the private connection from the
        # Database instance like this, but it's a reasonable way to proceed
        # while we transition to using the Database API more.
        self._connection = self._db._connection
        self._dimensionStorage = setupDimensionStorage(self._connection, dimensions, dimensionTables)
        self._datasetStorage = DatasetRegistryStorage(connection=self._connection,
                                                      universe=self.dimensions,
                                                      tables=self._tables._asdict())
        self._opaqueTables = {}
        self._datasetTypes = {}
        self._runIdsByName = {}   # key = name, value = id
        self._runNamesById = {}   # key = id, value = name

    def __str__(self) -> str:
        return str(self._db)

    def __repr__(self) -> str:
        return f"Registry({self._db!r}, {self.dimensions!r})"

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
            for storage in self._dimensionStorage.values():
                storage.clearCaches()
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
        self._opaqueTables[tableName] = self._db.ensureTableExists(tableName, spec)

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
        self._db.insert(self._opaqueTables[tableName], *data)

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
        table = self._opaqueTables[tableName]
        sql = table.select()
        whereTerms = [table.columns[k] == v for k, v in where.items()]
        if whereTerms:
            sql = sql.where(sqlalchemy.sql.and_(*whereTerms))
        for row in self._db.query(sql):
            yield dict(row)

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
        self._db.delete(self._opaqueTables[tableName], where.keys(), where)

    def getAllCollections(self):
        """Get names of all the collections found in this repository.

        Returns
        -------
        collections : `set` of `str`
            The collections.
        """
        table = self._tables.dataset_collection
        result = self._db.query(sqlalchemy.sql.select([table.c.collection]).distinct()).fetchall()
        if result is None:
            return set()
        return {r[0] for r in result}

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
        id = self._runIdsByName.get(name)
        if id is None:
            (id,), _ = self._db.sync(self._tables.run, keys={"name": name}, returning=["id"])
            self._runIdsByName[name] = id
            self._runNamesById[id] = name
        # Assume that if the run is in the cache, it's in the database, because
        # right now there's no way to delete them.

    def _getRunNameFromId(self, id: int) -> str:
        """Return the name of the run associated with the given integer ID.
        """
        assert isinstance(id, int)
        name = self._runNamesById.get(id)
        if name is None:
            table = self._tables.run
            name = self._db.query(
                sqlalchemy.sql.select(
                    [table.columns.name]
                ).select_from(
                    table
                ).where(
                    table.columns.id == id
                )
            ).scalar()
            self._runNamesById[id] = name
            self._runIdsByName[name] = id
        return name

    def _getRunIdFromName(self, name: str) -> id:
        """Return the integer ID of the run associated with the given name.
        """
        assert isinstance(name, str)
        id = self._runIdsByName.get(name)
        if id is None:
            table = self._tables.run
            id = self._db.query(
                sqlalchemy.sql.select(
                    [table.columns.id]
                ).select_from(
                    table
                ).where(
                    table.columns.name == name
                )
            ).scalar()
            self._runNamesById[id] = name
            self._runIdsByName[name] = id
        return id

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

    def getAllDatasetTypes(self) -> FrozenSet[DatasetType]:
        """Get every registered `DatasetType`.

        Returns
        -------
        types : `frozenset` of `DatasetType`
            Every `DatasetType` in the registry.
        """
        # Get all the registered names
        result = self._db.query(
            sqlalchemy.sql.select(
                [self._tables.dataset_type.columns.dataset_type_name]
            )
        ).fetchall()
        if result is None:
            return frozenset()
        datasetTypeNames = [r[0] for r in result]
        return frozenset(self.getDatasetType(name) for name in datasetTypeNames)

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
        run = self._getRunNameFromId(row["run_id"])
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

    def find(self, collection: str, datasetType: Union[DatasetType, str], dataId: Optional[DataId] = None,
             **kwds: Any) -> Optional[DatasetRef]:
        """Lookup a dataset.

        This can be used to obtain a `DatasetRef` that permits the dataset to
        be read from a `Datastore`.

        Parameters
        ----------
        collection : `str`
            Identifies the collection to search.
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.
        dataId : `dict` or `DataCoordinate`, optional
            A `dict`-like object containing the `Dimension` links that identify
            the dataset within a collection.
        **kwds
            Additional keyword arguments passed to
            `DataCoordinate.standardize` to convert ``dataId`` to a true
            `DataCoordinate` or augment an existing one.

        Returns
        -------
        ref : `DatasetRef`
            A ref to the Dataset, or `None` if no matching Dataset
            was found.

        Raises
        ------
        LookupError
            If one or more data ID keys are missing.
        """
        if not isinstance(datasetType, DatasetType):
            datasetType = self.getDatasetType(datasetType)
        dataId = DataCoordinate.standardize(dataId, graph=datasetType.dimensions,
                                            universe=self.dimensions, **kwds)
        whereTerms = [
            self._tables.dataset.columns.dataset_type_name == datasetType.name,
            self._tables.dataset_collection.columns.collection == collection,
        ]
        whereTerms.extend(self._tables.dataset.columns[name] == dataId[name] for name in dataId.keys())
        result = self._db.query(
            self._tables.dataset.select().select_from(
                self._tables.dataset.join(self._tables.dataset_collection)
            ).where(
                sqlalchemy.sql.and_(*whereTerms)
            )
        ).fetchone()
        if result is None:
            return None
        return self._makeDatasetRefFromRow(result, datasetType=datasetType, dataId=dataId)

    @transactional
    def addDataset(self, datasetType: Union[DatasetType, str],
                   dataId: DataId, run: str, producer: Optional[Quantum] = None,
                   recursive: bool = False, **kwds: Any) -> DatasetRef:
        """Adds a Dataset entry to the `Registry`

        This always adds a new Dataset; to associate an existing Dataset with
        a new collection, use ``associate``.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.
        dataId : `dict` or `DataCoordinate`
            A `dict`-like object containing the `Dimension` links that identify
            the dataset within a collection.
        run : `str`
            The name of the run that produced the dataset.
        producer : `Quantum`
            Unit of work that produced the Dataset.  May be `None` to store
            no provenance information, but if present the `Quantum` must
            already have been added to the Registry.
        recursive : `bool`
            If True, recursively add Dataset and attach entries for component
            Datasets as well.
        **kwds
            Additional keyword arguments passed to
            `DataCoordinate.standardize` to convert ``dataId`` to a
            true `DataCoordinate` or augment an existing
            one.

        Returns
        -------
        ref : `DatasetRef`
            A newly-created `DatasetRef` instance.

        Raises
        ------
        ConflictingDefinitionError
            If a Dataset with the given `DatasetRef` already exists in the
            given collection.

        Exception
            Raised if ``dataId`` contains unknown or invalid `Dimension`
            entries.
        """
        if not isinstance(datasetType, DatasetType):
            datasetType = self.getDatasetType(datasetType)
        # Make an expanded, standardized data ID up front, so we don't do that
        # multiple times in calls below.  Note that calling expandDataId with a
        # full ExpandedDataCoordinate is basically a no-op.
        dataId = self.expandDataId(dataId, graph=datasetType.dimensions, **kwds)
        # Retrieve the ID for the given run.
        runId = self._getRunIdFromName(run)
        # Add the dataset table entry itself.  Note that this will get rolled
        # back if the subsequent call to associate raises, which is what we
        # want.
        datasetRef = DatasetRef(datasetType=datasetType, dataId=dataId)
        # TODO add producer
        row = {k.name: v for k, v in dataId.full.items()}
        row.update(
            dataset_type_name=datasetType.name,
            run_id=runId,
            dataset_ref_hash=datasetRef.hash,
            quantum_id=None
        )
        datasetId, = self._db.insert(self._tables.dataset, row, returnIds=True)
        datasetRef = datasetRef.resolved(id=datasetId, run=run)
        # A dataset is always initially associated with its run as a
        # collection.
        self.associate(run, [datasetRef, ])
        if recursive:
            for component in datasetType.storageClass.components:
                compTypeName = datasetType.componentTypeName(component)
                compDatasetType = self.getDatasetType(compTypeName)
                compRef = self.addDataset(compDatasetType, dataId, run=run, producer=producer,
                                          recursive=True)
                self.attachComponent(component, datasetRef, compRef)
        return datasetRef

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
    def removeDataset(self, ref: DatasetRef):
        """Remove a dataset from the Registry.

        The dataset and all components will be removed unconditionally from
        all collections, and any associated `Quantum` records will also be
        removed.  `Datastore` records will *not* be deleted; the caller is
        responsible for ensuring that the dataset has already been removed
        from all Datastores.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the dataset to be removed.  Must include a valid
            ``id`` attribute, and should be considered invalidated upon return.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        OrphanedRecordError
            Raised if the dataset is still present in any `Datastore`.
        """
        if not ref.id:
            raise AmbiguousDatasetError(f"Cannot remove dataset {ref} without ID.")
        # Remove component datasets.  We assume ``ref.components`` is already
        # correctly populated, and rely on ON DELETE CASCADE to remove entries
        # from DatasetComposition.
        for componentRef in ref.components.values():
            self.removeDataset(componentRef)

        # Remove related quanta.  We rely on ON DELETE CASCADE to remove any
        # related records in dataset_consumers.  Note that we permit a Quantum
        # to be deleted without removing the datasets it refers to, but do not
        # allow a dataset to be deleted without removing the Quanta that refer
        # to them.  A dataset is still quite usable without provenance, but
        # provenance is worthless if it's inaccurate.
        t = self._tables
        selectProducer = sqlalchemy.sql.select(
            [t.dataset.columns.quantum_id]
        ).where(
            t.dataset.columns.dataset_id == ref.id
        )
        selectConsumers = sqlalchemy.sql.select(
            [t.dataset_consumers.columns.quantum_id]
        ).where(
            t.dataset_consumers.columns.dataset_id == ref.id
        )
        # TODO: we'd like to use Database.delete here, but it doesn't general
        # queries yet.
        self._connection.execute(
            t.quantum.delete().where(
                t.quantum.columns.id.in_(sqlalchemy.sql.union(selectProducer, selectConsumers))
            )
        )
        # Remove the Dataset record itself.  We rely on ON DELETE CASCADE to
        # remove from DatasetCollection, and assume foreign key violations
        # come from DatasetLocation (everything else should have an ON DELETE).
        try:
            self._connection.execute(
                t.dataset.delete().where(t.dataset.c.dataset_id == ref.id)
            )
        except sqlalchemy.exc.IntegrityError as err:
            raise OrphanedRecordError(f"Dataset {ref} is still present in one or more Datastores.") from err

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
        if parent.id is None:
            raise AmbiguousDatasetError(f"Cannot attach component to dataset {parent} without ID.")
        if component.id is None:
            raise AmbiguousDatasetError(f"Cannot attach component {component} without ID.")
        values = dict(component_name=name,
                      parent_dataset_id=parent.id,
                      component_dataset_id=component.id)
        self._db.insert(self._tables.dataset_composition, values)
        parent._components[name] = component

    @transactional
    def associate(self, collection: str, refs: List[DatasetRef]):
        """Add existing Datasets to a collection, implicitly creating the
        collection if it does not already exist.

        If a DatasetRef with the same exact ``dataset_id`` is already in a
        collection nothing is changed. If a `DatasetRef` with the same
        `DatasetType1` and dimension values but with different ``dataset_id``
        exists in the collection, `ValueError` is raised.

        Parameters
        ----------
        collection : `str`
            Indicates the collection the Datasets should be associated with.
        refs : iterable of `DatasetRef`
            An iterable of `DatasetRef` instances that already exist in this
            `Registry`.  All component datasets will be associated with the
            collection as well.

        Raises
        ------
        ConflictingDefinitionError
            If a Dataset with the given `DatasetRef` already exists in the
            given collection.
        AmbiguousDatasetError
            Raised if ``any(ref.id is None for ref in refs)``.
        """
        rows = [{"dataset_id": _checkAndGetId(ref),
                 "dataset_ref_hash": ref.hash,
                 "collection": collection}
                for ref in _expandComponents(refs)]
        try:
            self._db.replace(self._tables.dataset_collection, *rows)
        except sqlalchemy.exc.IntegrityError as err:
            raise ConflictingDefinitionError(
                f"Constraint violation while associating datasets with collection {collection}. "
                f"This probably means that one or more datasets with the same dataset type and data ID "
                f"already exist in the collection, but it may also indicate that the datasets do not exist."
            ) from err

    @transactional
    def disassociate(self, collection: str, refs: List[DatasetRef]):
        """Remove existing Datasets from a collection.

        ``collection`` and ``ref`` combinations that are not currently
        associated are silently ignored.

        Parameters
        ----------
        collection : `str`
            The collection the Datasets should no longer be associated with.
        refs : `list` of `DatasetRef`
            A `list` of `DatasetRef` instances that already exist in this
            `Registry`.  All component datasets will also be removed.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``any(ref.id is None for ref in refs)``.
        """
        rows = [{"dataset_id": _checkAndGetId(ref), "collection": collection}
                for ref in _expandComponents(refs)]
        self._db.delete(self._tables.dataset_collection, ["dataset_id", "collection"], *rows)

    @transactional
    def addDatasetLocation(self, ref: DatasetRef, datastoreName: str):
        """Add datastore name locating a given dataset.

        Typically used by `Datastore`.

        Parameters
        ----------
        ref : `DatasetRef`
            A reference to the dataset for which to add storage information.
        datastoreName : `str`
            Name of the datastore holding this dataset.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        """
        self._db.insert(self._tables.dataset_storage,
                        {"dataset_id": _checkAndGetId(ref),
                         "datastore_name": datastoreName})

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
        table = self._tables.dataset_storage
        result = self._db.query(
            sqlalchemy.sql.select(
                [table.columns.datastore_name]
            ).where(
                table.columns.dataset_id == ref.id
            )
        ).fetchall()
        return {r["datastore_name"] for r in result}

    @transactional
    def removeDatasetLocation(self, datastoreName, ref):
        """Remove datastore location associated with this dataset.

        Typically used by `Datastore` when a dataset is removed.

        Parameters
        ----------
        datastoreName : `str`
            Name of this `Datastore`.
        ref : `DatasetRef`
            A reference to the dataset for which information is to be removed.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        """
        self._db.delete(
            self._tables.dataset_storage,
            ["dataset_id", "datastore_name"],
            {"dataset_id": _checkAndGetId(ref), "datastore_name": datastoreName}
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
        for element in standardized.graph._primaryKeyTraversalOrder:
            record = records.get(element.name, ...)  # Use ... to mean not found; None might mean NULL
            if record is ...:
                storage = self._dimensionStorage[element]
                record = storage.fetch(keys)
                records[element] = record
            if record is not None:
                keys.update((d, getattr(record, d.name)) for d in element.implied)
            else:
                if element in standardized.graph.required:
                    raise LookupError(
                        f"Could not fetch record for required dimension {element.name} via keys {keys}."
                    )
                records.update((d, None) for d in element.implied)
        return ExpandedDataCoordinate(standardized.graph, standardized.values(), records=records)

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
        storage = self._dimensionStorage[element]
        storage.insert(*records)

    def makeQueryBuilder(self, summary: QuerySummary) -> QueryBuilder:
        """Return a `QueryBuilder` instance capable of constructing and
        managing more complex queries than those obtainable via `Registry`
        interfaces.

        This is an advanced `SqlRegistry`-only interface; downstream code
        should prefer `Registry.queryDimensions` and `Registry.queryDatasets`
        whenever those are sufficient.

        Parameters
        ----------
        summary: `QuerySummary`
            Object describing and categorizing the full set of dimensions that
            will be included in the query.

        Returns
        -------
        builder : `QueryBuilder`
            Object that can be used to construct and perform advanced queries.
        """
        return QueryBuilder(connection=self._connection, summary=summary,
                            dimensionStorage=self._dimensionStorage,
                            datasetStorage=self._datasetStorage)

    def queryDimensions(self, dimensions: Union[Iterable[Union[Dimension, str]], Dimension, str], *,
                        dataId: Optional[DataId] = None,
                        datasets: Optional[Mapping[DatasetTypeExpression, CollectionsExpression]] = None,
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
        datasets : `~collections.abc.Mapping`, optional
            Datasets whose existence in the registry constrain the set of data
            IDs returned.  This is a mapping from a dataset type expression
            (a `str` name, a true `DatasetType` instance, a `Like` pattern
            for the name, or ``...`` for all DatasetTypes) to a collections
            expression (a sequence of `str` or `Like` patterns, or `...` for
            all collections).
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.
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
        standardizedDatasets = NamedKeyDict()
        requestedDimensionNames = set(self.dimensions.extract(dimensions).names)
        if datasets is not None:
            for datasetTypeExpr, collectionsExpr in datasets.items():
                for trueDatasetType in self._datasetStorage.fetchDatasetTypes(datasetTypeExpr,
                                                                              collections=collectionsExpr,
                                                                              dataId=standardizedDataId):
                    requestedDimensionNames.update(trueDatasetType.dimensions.names)
                    standardizedDatasets[trueDatasetType] = collectionsExpr
        summary = QuerySummary(
            requested=DimensionGraph(self.dimensions, names=requestedDimensionNames),
            dataId=standardizedDataId,
            expression=where,
        )
        builder = self.makeQueryBuilder(summary)
        for datasetType, collections in standardizedDatasets.items():
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

    def queryDatasets(self, datasetType: DatasetTypeExpression, *,
                      collections: CollectionsExpression,
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
        datasetType : `DatasetType`, `str`, `Like`, or ``...``
            An expression indicating type(s) of datasets to query for.
            ``...`` may be used to query for all known DatasetTypes.
            Multiple explicitly-provided dataset types cannot be queried in a
            single call to `queryDatasets` even though wildcard expressions
            can, because the results would be identical to chaining the
            iterators produced by multiple calls to `queryDatasets`.
        collections: `~collections.abc.Sequence` of `str` or `Like`, or ``...``
            An expression indicating the collections to be searched for
            datasets.  ``...`` may be passed to search all collections.
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
            key column of a dimension table) dimension name.
        deduplicate : `bool`, optional
            If `True` (`False` is default), for each result data ID, only
            yield one `DatasetRef` of each `DatasetType`, from the first
            collection in which a dataset of that dataset type appears
            (according to the order of ``collections`` passed in).  Cannot be
            used if any element in ``collections`` is an expression.
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
            collection wildcard is pass when ``deduplicate`` is `True`.

        Notes
        -----
        When multiple dataset types are queried via a wildcard expression, the
        results of this operation are equivalent to querying for each dataset
        type separately in turn, and no information about the relationships
        between datasets of different types is included.  In contexts where
        that kind of information is important, the recommended pattern is to
        use `queryDimensions` to first obtain data IDs (possibly with the
        desired dataset types and collections passed as constraints to the
        query), and then use multiple (generally much simpler) calls to
        `queryDatasets` with the returned data IDs passed as constraints.
        """
        # Standardize and expand the data ID provided as a constraint.
        standardizedDataId = self.expandDataId(dataId, **kwds)
        # If the datasetType passed isn't actually a DatasetType, expand it
        # (it could be an expression that yields multiple DatasetTypes) and
        # recurse.
        if not isinstance(datasetType, DatasetType):
            for trueDatasetType in self._datasetStorage.fetchDatasetTypes(datasetType,
                                                                          collections=collections,
                                                                          dataId=standardizedDataId):
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
        builder.joinDataset(datasetType, collections, isResult=True, addRank=deduplicate)
        query = builder.finish()
        predicate = query.predicate()
        if not deduplicate or len(collections) == 1:
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
