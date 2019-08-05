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

__all__ = ("SqlRegistryConfig", "SqlRegistry")

import sys
import contextlib
import warnings
from typing import Union, Iterable, Optional, Mapping, Iterator

from sqlalchemy import create_engine, text, func
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select, and_, bindparam, union
from sqlalchemy.exc import IntegrityError, SADeprecationWarning

from ..core.utils import transactional, NamedKeyDict

from ..core.datasets import DatasetType, DatasetRef
from ..core.registryConfig import RegistryConfig
from ..core.registry import (Registry, ConflictingDefinitionError,
                             AmbiguousDatasetError, OrphanedRecordError)
from ..core.schema import Schema
from ..core.execution import Execution
from ..core.run import Run
from ..core.storageClass import StorageClassFactory
from ..core.config import Config
from ..core.dimensions import (DataCoordinate, DimensionGraph, ExpandedDataCoordinate, DimensionElement,
                               DataId, DimensionRecord, Dimension)
from ..core.dimensions.storage import setupDimensionStorage
from ..core.dimensions.schema import addDimensionForeignKey
from ..core.queries import (DatasetRegistryStorage, QuerySummary, QueryBuilder,
                            DatasetTypeExpression, CollectionsExpression)
from .sqlRegistryDatabaseDict import SqlRegistryDatabaseDict


class SqlRegistryConfig(RegistryConfig):
    pass


class SqlRegistry(Registry):
    """Registry backed by a SQL database.

    Parameters
    ----------
    registryConfig : `SqlRegistryConfig` or `str`
        Load configuration
    schemaConfig : `SchemaConfig` or `str`
        Definition of the schema to use.
    dimensionConfig : `DimensionConfig` or `Config` or
        `DimensionGraph` configuration.
    create : `bool`
        Assume registry is empty and create a new one.
    """

    defaultConfigFile = None
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    def __init__(self, registryConfig, schemaConfig, dimensionConfig, create=False, butlerRoot=None):
        registryConfig = SqlRegistryConfig(registryConfig)
        super().__init__(registryConfig, dimensionConfig=dimensionConfig)
        self.storageClasses = StorageClassFactory()
        # Build schema for dimensions.
        schemaSpec = self.dimensions.makeSchemaSpec()
        # Update with schema directly loaded from config.
        schemaSpec.update(schemaConfig.toSpec())
        # Add dimension columns and foreign keys to the dataset table.
        datasetTableSpec = schemaSpec["dataset"]
        for dimension in self.dimensions.dimensions:
            addDimensionForeignKey(datasetTableSpec, dimension, primaryKey=False, nullable=True)
        # Translate the schema specification to SQLALchemy, allowing subclasses
        # to specialize.
        self._schema = self._createSchema(schemaSpec)
        self._datasetTypes = {}
        self._engine = self._createEngine()
        self._connection = self._createConnection(self._engine)
        self._cachedRuns = {}   # Run objects, keyed by id or collection
        self._dimensionStorage = setupDimensionStorage(connection=self._connection,
                                                       universe=self.dimensions,
                                                       tables=self._schema.tables)
        self._datasetStorage = DatasetRegistryStorage(connection=self._connection,
                                                      universe=self.dimensions,
                                                      tables=self._schema.tables)
        if create:
            # In our tables we have columns that make use of sqlalchemy
            # Sequence objects. There is currently a bug in sqlalchmey
            # that causes a deprecation warning to be thrown on a
            # property of the Sequence object when the repr for the
            # sequence is created. Here a filter is used to catch these
            # deprecation warnings when tables are created.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=SADeprecationWarning)
                self._createTables(self._schema, self._connection)

    def __str__(self):
        return self.config["db"]

    @contextlib.contextmanager
    def transaction(self):
        """Context manager that implements SQL transactions.

        Will roll back any changes to the `SqlRegistry` database
        in case an exception is raised in the enclosed block.

        This context manager may be nested.
        """
        trans = self._connection.begin_nested()
        try:
            yield
            trans.commit()
        except BaseException:
            trans.rollback()
            for storage in self._dimensionStorage.values():
                storage.clearCaches()
            raise

    def _createSchema(self, spec):
        """Create and return an `lsst.daf.butler.Schema` object containing
        SQLAlchemy table definitions.

        This is a hook provided for customization by subclasses, but it is
        known to be insufficient for that purpose and is expected to change in
        the future.

        Note that this method should not actually create any tables or views
        in the database - it is called even when an existing database is used
        in order to construct the SQLAlchemy representation of the expected
        schema.

        Parameters
        ----------
        spec : `dict` mapping `str` to `TableSpec`
            Specification of the logical tables to be created.

        Returns
        -------
        schema : `Schema`
            Structure containing SQLAlchemy objects representing the tables
            and views in the registry schema.
        """
        return Schema(spec=spec)

    def _createEngine(self):
        """Create and return a `sqlalchemy.Engine` for this `Registry`.

        This is a hook provided for customization by subclasses.

        SQLAlchemy generally expects engines to be created at module scope,
        with a pool of connections used by different parts of an application.
        Because our `Registry` instances don't know what database they'll
        connect to until they are constructed, that is impossible for us, so
        the engine is connected with the `Registry` instance.  In addition,
        we do not expect concurrent usage of the same `Registry`, and hence
        don't gain anything from connection pooling.  As a result, the default
        implementation of this function uses `sqlalchemy.pool.NullPool` to
        associate just a single connection with the engine.  Unless they
        have a very good reason not to, subclasses that override this method
        should do the same.
        """
        return create_engine(self.config.connectionString, poolclass=NullPool)

    def _createConnection(self, engine):
        """Create and return a `sqlalchemy.Connection` for this `Registry`.

        This is a hook provided for customization by subclasses.
        """
        return engine.connect()

    def _createTables(self, schema, connection):
        """Create all tables in the given schema, using the given connection.

        This is a hook provided for customization by subclasses.
        """
        schema.metadata.create_all(connection)

    def _isValidDatasetType(self, datasetType):
        """Check if given `DatasetType` instance is valid for this `Registry`.

        .. todo::

            Insert checks for `storageClass`, `dimensions` and `template`.
        """
        return isinstance(datasetType, DatasetType)

    def makeDatabaseDict(self, table, key, value):
        """Construct a DatabaseDict backed by a table in the same database as
        this Registry.

        Parameters
        ----------
        table : `table`
            Name of the table that backs the returned DatabaseDict.  If this
            table already exists, its schema must include at least everything
            in `types`.
        key : `str`
            The name of the field to be used as the dictionary key.  Must not
            be present in ``value._fields``.
        value : `type`
            The type used for the dictionary's values, typically a
            `DatabaseDictRecordBase`.  Must have a ``fields`` class method
            that is a tuple of field names; these field names must also appear
            in the return value of the ``types()`` class method, and it must be
            possible to construct it from a sequence of values. Lengths of
            string fields must be obtainable as a `dict` from using the
            ``lengths`` property.

        Returns
        -------
        databaseDict : `DatabaseDict`
            `DatabaseDict` backed by this registry.
        """
        # We need to construct a temporary config for the table value because
        # SqlRegistryDatabaseDict.__init__ is required to take a config so it
        # can be called by DatabaseDict.fromConfig as well.
        # I suppose we could have Registry.makeDatabaseDict take a config as
        # well, since it"ll also usually be called by DatabaseDict.fromConfig,
        # but I strongly believe in having signatures that only take what they
        # really need.
        config = Config()
        config["table"] = table
        return SqlRegistryDatabaseDict(config, key=key, value=value,
                                       registry=self)

    def _makeDatasetRefFromRow(self, row, datasetType=None, dataId=None):
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
        run = self.getRun(id=row.run_id)
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
            datasetCompositionTable = self._schema.tables["dataset_composition"]
            datasetTable = self._schema.tables["dataset"]
            columns = list(datasetTable.c)
            columns.append(datasetCompositionTable.c.component_name)
            results = self._connection.execute(
                select(
                    columns
                ).select_from(
                    datasetTable.join(
                        datasetCompositionTable,
                        datasetTable.c.dataset_id == datasetCompositionTable.c.component_dataset_id
                    )
                ).where(
                    datasetCompositionTable.c.parent_dataset_id == row["dataset_id"]
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

    def getAllCollections(self):
        # Docstring inherited from Registry.getAllCollections
        datasetCollectionTable = self._schema.tables["dataset_collection"]
        result = self._connection.execute(select([datasetCollectionTable.c.collection]).distinct()).fetchall()
        if result is None:
            return set()
        return {r[0] for r in result}

    def find(self, collection, datasetType, dataId=None, **kwds):
        # Docstring inherited from Registry.find
        if not isinstance(datasetType, DatasetType):
            datasetType = self.getDatasetType(datasetType)
        dataId = DataCoordinate.standardize(dataId, graph=datasetType.dimensions,
                                            universe=self.dimensions, **kwds)
        datasetTable = self._schema.tables["dataset"]
        datasetCollectionTable = self._schema.tables["dataset_collection"]
        dataIdExpression = and_(self._schema.tables["dataset"].c[name] == dataId[name]
                                for name in dataId.keys())
        result = self._connection.execute(
            datasetTable.select().select_from(
                datasetTable.join(datasetCollectionTable)
            ).where(
                and_(
                    datasetTable.c.dataset_type_name == datasetType.name,
                    datasetCollectionTable.c.collection == collection,
                    dataIdExpression
                )
            )
        ).fetchone()
        # TODO update dimension values and add Run, Quantum and assembler?
        if result is None:
            return None
        return self._makeDatasetRefFromRow(result, datasetType=datasetType, dataId=dataId)

    def query(self, sql, **params):
        """Execute a SQL SELECT statement directly.

        Named parameters are specified in the SQL query string by preceeding
        them with a colon.  Parameter values are provided as additional
        keyword arguments.  For example:

          registry.query("SELECT * FROM instrument WHERE instrument=:name",
                         name="HSC")

        Parameters
        ----------
        sql : `str`
            SQL query string.  Must be a SELECT statement.
        **params
            Parameter name-value pairs to insert into the query.

        Yields
        -------
        row : `dict`
            The next row result from executing the query.

        """
        # TODO: make this guard against non-SELECT queries.
        t = text(sql)
        for row in self._connection.execute(t, **params):
            yield dict(row)

    @transactional
    def registerDatasetType(self, datasetType):
        # Docstring inherited from Registry.getDatasetType.
        # If the DatasetType is already in the cache, we assume it's already in
        # the DB (note that we don't actually provide a way to remove them from
        # the DB).
        existingDatasetType = self._datasetTypes.get(datasetType.name, None)
        # If it's not in the cache, try to insert it.
        if existingDatasetType is None:
            try:
                with self.transaction():
                    self._connection.execute(
                        self._schema.tables["dataset_type"].insert().values(
                            dataset_type_name=datasetType.name,
                            storage_class=datasetType.storageClass.name
                        )
                    )
            except IntegrityError:
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
                    self._connection.execute(
                        self._schema.tables["dataset_type_dimensions"].insert(),
                        [{"dataset_type_name": datasetType.name,
                          "dimension_name": dimensionName}
                         for dimensionName in datasetType.dimensions.names]
                    )
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

    def getAllDatasetTypes(self):
        # Docstring inherited from Registry.getAllDatasetTypes.
        datasetTypeTable = self._schema.tables["dataset_type"]

        # Get all the registered names
        result = self._connection.execute(select([datasetTypeTable.c.dataset_type_name])).fetchall()
        if result is None:
            return frozenset()

        datasetTypeNames = [r[0] for r in result]
        return frozenset(self.getDatasetType(name) for name in datasetTypeNames)

    def getDatasetType(self, name):
        # Docstring inherited from Registry.getDatasetType.
        datasetTypeTable = self._schema.tables["dataset_type"]
        datasetTypeDimensionsTable = self._schema.tables["dataset_type_dimensions"]
        # Get StorageClass from DatasetType table
        result = self._connection.execute(select([datasetTypeTable.c.storage_class]).where(
            datasetTypeTable.c.dataset_type_name == name)).fetchone()

        if result is None:
            raise KeyError("Could not find entry for datasetType {}".format(name))

        storageClass = self.storageClasses.getStorageClass(result["storage_class"])
        # Get Dimensions (if any) from DatasetTypeDimensions table
        result = self._connection.execute(select([datasetTypeDimensionsTable.c.dimension_name]).where(
            datasetTypeDimensionsTable.c.dataset_type_name == name)).fetchall()
        dimensions = DimensionGraph(self.dimensions, names=(r[0] for r in result) if result else ())
        datasetType = DatasetType(name=name,
                                  storageClass=storageClass,
                                  dimensions=dimensions)
        return datasetType

    @transactional
    def addDataset(self, datasetType, dataId, run, producer=None, recursive=False, **kwds):
        # Docstring inherited from Registry.addDataset

        if not isinstance(datasetType, DatasetType):
            datasetType = self.getDatasetType(datasetType)

        # Make an expanded, standardized data ID up front, so we don't do that
        # multiple times in calls below.  Note that calling expandDataId with a
        # full ExpandedDataCoordinate is basically a no-op.
        dataId = self.expandDataId(dataId, graph=datasetType.dimensions, **kwds)

        # Add the Dataset table entry itself.  Note that this will get rolled
        # back if the subsequent call to associate raises, which is what we
        # want.
        datasetTable = self._schema.tables["dataset"]
        datasetRef = DatasetRef(datasetType=datasetType, dataId=dataId, run=run)
        # TODO add producer
        row = {k.name: v for k, v in dataId.full.items()}
        row.update(
            dataset_type_name=datasetType.name,
            run_id=run.id,
            dataset_ref_hash=datasetRef.hash,
            quantum_id=None
        )
        result = self._connection.execute(datasetTable.insert(), row)
        datasetRef._id = result.inserted_primary_key[0]
        # If the result is reported as a list of a number, unpack the list
        if isinstance(datasetRef._id, list):
            datasetRef._id = datasetRef._id[0]

        # A dataset is always initially associated with its Run collection.
        self.associate(run.collection, [datasetRef, ])

        if recursive:
            for component in datasetType.storageClass.components:
                compTypeName = datasetType.componentTypeName(component)
                compDatasetType = self.getDatasetType(compTypeName)
                compRef = self.addDataset(compDatasetType, dataId, run=run, producer=producer,
                                          recursive=True)
                self.attachComponent(component, datasetRef, compRef)
        return datasetRef

    def getDataset(self, id, datasetType=None, dataId=None):
        # Docstring inherited from Registry.getDataset
        datasetTable = self._schema.tables["dataset"]
        result = self._connection.execute(
            select([datasetTable]).where(datasetTable.c.dataset_id == id)).fetchone()
        if result is None:
            return None
        return self._makeDatasetRefFromRow(result, datasetType=datasetType, dataId=dataId)

    @transactional
    def removeDataset(self, ref):
        # Docstring inherited from Registry.removeDataset.
        if not ref.id:
            raise AmbiguousDatasetError(f"Cannot remove dataset {ref} without ID.")

        # Remove component datasets.  We assume ``ref.components`` is already
        # correctly populated, and rely on ON DELETE CASCADE to remove entries
        # from DatasetComposition.
        for componentRef in ref.components.values():
            self.removeDataset(componentRef)

        datasetTable = self._schema.tables["dataset"]

        # Remove related quanta.  We actually delete from Execution, because
        # Quantum's primary key (quantum_id) is also a foreign key to
        # Execution.execution_id.  We then rely on ON DELETE CASCADE to remove
        # the Quantum record as well as any related records in
        # DatasetConsumers.  Note that we permit a Quantum to be deleted
        # without removing the Datasets it refers to, but do not allow a
        # Dataset to be deleting without removing the Quanta that refer to
        # them.  A Dataset is still quite usable without provenance, but
        # provenance is worthless if it's inaccurate.
        executionTable = self._schema.tables["execution"]
        datasetConsumersTable = self._schema.tables["dataset_consumers"]
        selectProducer = select(
            [datasetTable.c.quantum_id]
        ).where(
            datasetTable.c.dataset_id == ref.id
        )
        selectConsumers = select(
            [datasetConsumersTable.c.quantum_id]
        ).where(
            datasetConsumersTable.c.dataset_id == ref.id
        )
        self._connection.execute(
            executionTable.delete().where(
                executionTable.c.execution_id.in_(union(selectProducer, selectConsumers))
            )
        )

        # Remove the Dataset record itself.  We rely on ON DELETE CASCADE to
        # remove from DatasetCollection, and assume foreign key violations
        # come from DatasetLocation (everything else should have an ON DELETE).
        try:
            self._connection.execute(
                datasetTable.delete().where(datasetTable.c.dataset_id == ref.id)
            )
        except IntegrityError as err:
            raise OrphanedRecordError(f"Dataset {ref} is still present in one or more Datastores.") from err

    @transactional
    def attachComponent(self, name, parent, component):
        # Docstring inherited from Registry.attachComponent.
        # TODO Insert check for component name and type against
        # parent.storageClass specified components
        if parent.id is None:
            raise AmbiguousDatasetError(f"Cannot attach component to dataset {parent} without ID.")
        if component.id is None:
            raise AmbiguousDatasetError(f"Cannot attach component {component} without ID.")
        datasetCompositionTable = self._schema.tables["dataset_composition"]
        values = dict(component_name=name,
                      parent_dataset_id=parent.id,
                      component_dataset_id=component.id)
        self._connection.execute(datasetCompositionTable.insert().values(**values))
        parent._components[name] = component

    @transactional
    def associate(self, collection, refs):
        # Docstring inherited from Registry.associate.

        # Most SqlRegistry subclass implementations should replace this
        # implementation with special "UPSERT" or "MERGE" syntax.  This
        # implementation is only concurrency-safe for databases that implement
        # transactions with database- or table-wide locks (e.g. SQLite).

        datasetCollectionTable = self._schema.tables["dataset_collection"]
        insertQuery = datasetCollectionTable.insert()
        checkQuery = select([datasetCollectionTable.c.dataset_id], whereclause=and_(
            datasetCollectionTable.c.collection == collection,
            datasetCollectionTable.c.dataset_ref_hash == bindparam("hash")))

        for ref in refs:
            if ref.id is None:
                raise AmbiguousDatasetError(f"Cannot associate dataset {ref} without ID.")

            try:
                with self.transaction():
                    self._connection.execute(insertQuery, {"dataset_id": ref.id, "dataset_ref_hash": ref.hash,
                                                           "collection": collection})
            except IntegrityError as exc:
                # Did we clash with a completely duplicate entry (because this
                # dataset is already in this collection)?  Or is there already
                # a different dataset with the same DatasetType and data ID in
                # this collection?  Only the latter is an error.
                row = self._connection.execute(checkQuery, hash=ref.hash).fetchone()
                if row.dataset_id != ref.id:
                    raise ConflictingDefinitionError(
                        "A dataset of type {} with id: {} already exists in collection {}".format(
                            ref.datasetType, ref.dataId, collection
                        )
                    ) from exc
            self.associate(collection, ref.components.values())

    @transactional
    def disassociate(self, collection, refs):
        # Docstring inherited from Registry.disassociate.
        datasetCollectionTable = self._schema.tables["dataset_collection"]
        for ref in refs:
            if ref.id is None:
                raise AmbiguousDatasetError(f"Cannot disassociate dataset {ref} without ID.")
            self.disassociate(collection, ref.components.values())
            self._connection.execute(datasetCollectionTable.delete().where(
                and_(datasetCollectionTable.c.dataset_id == ref.id,
                     datasetCollectionTable.c.collection == collection)))

    @transactional
    def addDatasetLocation(self, ref, datastoreName):
        # Docstring inherited from Registry.addDatasetLocation.
        if ref.id is None:
            raise AmbiguousDatasetError(f"Cannot add location for dataset {ref} without ID.")
        datasetStorageTable = self._schema.tables["dataset_storage"]
        values = dict(dataset_id=ref.id,
                      datastore_name=datastoreName)
        self._connection.execute(datasetStorageTable.insert().values(**values))

    def getDatasetLocations(self, ref):
        # Docstring inherited from Registry.getDatasetLocation.
        if ref.id is None:
            raise AmbiguousDatasetError(f"Cannot add location for dataset {ref} without ID.")
        datasetStorageTable = self._schema.tables["dataset_storage"]
        result = self._connection.execute(
            select([datasetStorageTable.c.datastore_name]).where(
                and_(datasetStorageTable.c.dataset_id == ref.id))).fetchall()

        return {r["datastore_name"] for r in result}

    @transactional
    def removeDatasetLocation(self, datastoreName, ref):
        # Docstring inherited from Registry.getDatasetLocation.
        datasetStorageTable = self._schema.tables["dataset_storage"]
        self._connection.execute(datasetStorageTable.delete().where(
            and_(datasetStorageTable.c.dataset_id == ref.id,
                 datasetStorageTable.c.datastore_name == datastoreName)))

    @transactional
    def addExecution(self, execution):
        # Docstring inherited from Registry.addExecution
        executionTable = self._schema.tables["execution"]
        kwargs = {}
        # Only pass in the execution_id to the insert statement if it is not
        # None. Otherwise, some databases attempt to insert a null and fail.
        # The Column is an auto increment primary key, so it will automatically
        # be inserted if absent.
        if execution.id is not None:
            kwargs["execution_id"] = execution.id
        kwargs["start_time"] = execution.startTime
        kwargs["end_time"] = execution.endTime
        kwargs["host"] = execution.host
        result = self._connection.execute(executionTable.insert().values(**kwargs))
        # Reassign id, may have been `None`
        execution._id = result.inserted_primary_key[0]
        # If the result is reported as a list of a number, unpack the list
        if isinstance(execution._id, list):
            execution._id = execution._id[0]

    def getExecution(self, id):
        # Docstring inherited from Registry.getExecution
        executionTable = self._schema.tables["execution"]
        result = self._connection.execute(
            select([executionTable.c.start_time,
                    executionTable.c.end_time,
                    executionTable.c.host]).where(executionTable.c.execution_id == id)).fetchone()
        if result is not None:
            return Execution(startTime=result["start_time"],
                             endTime=result["end_time"],
                             host=result["host"],
                             id=id)
        else:
            return None

    @transactional
    def makeRun(self, collection):
        # Docstring inherited from Registry.makeRun
        run = Run(collection=collection)
        self.addRun(run)
        return run

    @transactional
    def ensureRun(self, run):
        # Docstring inherited from Registry.ensureRun
        if run.id is not None:
            existingRun = self.getRun(id=run.id)
            if run != existingRun:
                raise ConflictingDefinitionError(f"{run} != existing: {existingRun}")
            return
        self.addRun(run)

    @transactional
    def addRun(self, run):
        # Docstring inherited from Registry.addRun
        runTable = self._schema.tables["run"]
        # TODO: this check is probably undesirable, as we may want to have
        # multiple Runs output to the same collection.  Fixing this requires
        # (at least) modifying getRun() accordingly.
        selection = select([func.count()]).select_from(runTable).where(runTable.c.collection ==
                                                                       run.collection)
        if self._connection.execute(selection).scalar() > 0:
            raise ConflictingDefinitionError(f"A run already exists with this collection: {run.collection}")
        # First add the Execution part
        self.addExecution(run)
        # Then the Run specific part
        self._connection.execute(runTable.insert().values(execution_id=run.id,
                                                          collection=run.collection,
                                                          environment_id=None,  # TODO add environment
                                                          pipeline_id=None))    # TODO add pipeline
        # TODO: set given Run's "id" attribute, add to self,_cachedRuns.

    def getRun(self, id=None, collection=None):
        # Docstring inherited from Registry.getRun
        executionTable = self._schema.tables["execution"]
        runTable = self._schema.tables["run"]
        run = None
        # Retrieve by id
        if (id is not None) and (collection is None):
            run = self._cachedRuns.get(id)
            if run is not None:
                return run
            result = self._connection.execute(select([executionTable.c.execution_id,
                                                      executionTable.c.start_time,
                                                      executionTable.c.end_time,
                                                      executionTable.c.host,
                                                      runTable.c.collection,
                                                      runTable.c.environment_id,
                                                      runTable.c.pipeline_id]).select_from(
                runTable.join(executionTable)).where(
                runTable.c.execution_id == id)).fetchone()
        # Retrieve by collection
        elif (collection is not None) and (id is None):
            run = self._cachedRuns.get(collection, None)
            if run is not None:
                return run
            result = self._connection.execute(select([executionTable.c.execution_id,
                                                      executionTable.c.start_time,
                                                      executionTable.c.end_time,
                                                      executionTable.c.host,
                                                      runTable.c.collection,
                                                      runTable.c.environment_id,
                                                      runTable.c.pipeline_id]).select_from(
                runTable.join(executionTable)).where(
                runTable.c.collection == collection)).fetchone()
        else:
            raise ValueError("Either collection or id must be given")
        if result is not None:
            run = Run(id=result["execution_id"],
                      startTime=result["start_time"],
                      endTime=result["end_time"],
                      host=result["host"],
                      collection=result["collection"],
                      environment=None,  # TODO add environment
                      pipeline=None)     # TODO add pipeline
            self._cachedRuns[run.id] = run
            self._cachedRuns[run.collection] = run
        return run

    def expandDataId(self, dataId: Optional[DataId] = None, *, graph: Optional[DimensionGraph] = None,
                     records: Optional[Mapping[DimensionElement, DimensionRecord]] = None, **kwds):
        # Docstring inherited from Registry.expandDataId.
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

    @transactional
    def insertDimensionData(self, element: Union[DimensionElement, str],
                            *data: Union[dict, DimensionRecord],
                            conform: bool = True):
        # Docstring inherited from Registry.insertDimensionData.
        if conform:
            element = self.dimensions[element]  # if this is a name, convert it to a true DimensionElement.
            records = [element.RecordClass.fromDict(row) if not type(row) is element.RecordClass else row
                       for row in data]
        else:
            records = data
        storage = self._dimensionStorage[element]
        try:
            storage.insert(*records)
        except IntegrityError as err:
            # TODO: this maintains previous behavior, but we're not actually
            # checking that this is a UNIQUE violation rather than, say, NOT
            # NULL or FOREIGN KEY.
            raise ConflictingDefinitionError(f"Existing definition for {element.name} record.") from err

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

    def queryDimensions(self, dimensions: Iterable[Union[Dimension, str]], *,
                        dataId: Optional[DataId] = None,
                        datasets: Optional[Mapping[DatasetTypeExpression, CollectionsExpression]] = None,
                        where: Optional[str] = None,
                        expand: bool = True,
                        **kwds) -> Iterator[DataCoordinate]:
        # Docstring inherited from Registry.queryDimensions.
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
        # Docstring inherited from Registry.queryDatasets.
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
                    ref._dataId = dataId  # TODO: add semi-public API for this?
                    yield ref
            else:
                yield from bestRefs.values()
