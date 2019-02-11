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

import itertools
import contextlib

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select, and_, exists, bindparam, union
from sqlalchemy.exc import IntegrityError

from ..core.utils import transactional

from ..core.datasets import DatasetType, DatasetRef
from ..core.registry import (RegistryConfig, Registry, disableWhenLimited,
                             ConflictingDefinitionError, AmbiguousDatasetError,
                             OrphanedRecordError)
from ..core.schema import Schema
from ..core.execution import Execution
from ..core.run import Run
from ..core.quantum import Quantum
from ..core.storageClass import StorageClassFactory
from ..core.config import Config
from ..core.dimensions import DataId
from .sqlRegistryDatabaseDict import SqlRegistryDatabaseDict
from ..sql import MultipleDatasetQueryBuilder


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

    def __init__(self, registryConfig, schemaConfig, dimensionConfig, create=False):
        registryConfig = SqlRegistryConfig(registryConfig)
        super().__init__(registryConfig, dimensionConfig=dimensionConfig)
        self.storageClasses = StorageClassFactory()
        self._schema = self._createSchema(schemaConfig)
        self._datasetTypes = {}
        self._engine = self._createEngine()
        self._connection = self._createConnection(self._engine)
        if create:
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
            raise

    def _createSchema(self, schemaConfig):
        """Create and return an `lsst.daf.butler.Schema` object containing
        SQLAlchemy table definitions.

        This is a hook provided for customization by subclasses, but it is
        known to be insufficient for that purpose and is expected to change in
        the future.

        Note that this method should not actually create any tables or views
        in the database - it is called even when an existing database is used
        in order to construct the SQLAlchemy representation of the expected
        schema.
        """
        return Schema(config=schemaConfig, limited=self.limited)

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
        return create_engine(self.config["db"], poolclass=NullPool)

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

    def makeDatabaseDict(self, table, types, key, value):
        """Construct a DatabaseDict backed by a table in the same database as
        this Registry.

        Parameters
        ----------
        table : `table`
            Name of the table that backs the returned DatabaseDict.  If this
            table already exists, its schema must include at least everything
            in `types`.
        types : `dict`
            A dictionary mapping `str` field names to type objects, containing
            all fields to be held in the database.
        key : `str`
            The name of the field to be used as the dictionary key.  Must not
            be present in ``value._fields``.
        value : `type`
            The type used for the dictionary's values, typically a
            `~collections.namedtuple`.  Must have a ``_fields`` class
            attribute that is a tuple of field names (i.e. as defined by
            `~collections.namedtuple`); these field names must also appear
            in the ``types`` arg, and a `_make` attribute to construct it
            from a sequence of values (again, as defined by
            `~collections.namedtuple`).
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
        return SqlRegistryDatabaseDict(config, types=types, key=key, value=value, registry=self)

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
        dataId : `DataId`, optional
            `DataId` associated with this datasets.  Will be retrieved if not
            provided.  If provided, the caller guarantees that it is already
            consistent with what would have been retrieved from the database.

        Returns
        -------
        ref : `DatasetRef`.
            A new `DatasetRef` instance.
        """
        if datasetType is None:
            datasetType = self.getDatasetType(row["dataset_type_name"])
        else:
            datasetType.normalize(universe=self.dimensions)
        run = self.getRun(id=row.run_id)
        datasetRefHash = row["dataset_ref_hash"]
        if dataId is None:
            dataId = DataId({link: row[self._schema.datasetTable.c[link]]
                             for link in datasetType.dimensions.links()},
                            dimensions=datasetType.dimensions,
                            universe=self.dimensions)
        # Get components (if present)
        components = {}
        if datasetType.storageClass.isComposite():
            datasetCompositionTable = self._schema.tables["DatasetComposition"]
            datasetTable = self._schema.tables["Dataset"]
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

    def find(self, collection, datasetType, dataId=None, **kwds):
        # Docstring inherited from Registry.find
        if not isinstance(datasetType, DatasetType):
            datasetType = self.getDatasetType(datasetType)
        else:
            datasetType.normalize(universe=self.dimensions)
        dataId = DataId(dataId, dimensions=datasetType.dimensions, universe=self.dimensions, **kwds)
        datasetTable = self._schema.tables["Dataset"]
        datasetCollectionTable = self._schema.tables["DatasetCollection"]
        dataIdExpression = and_(self._schema.datasetTable.c[name] == dataId[name]
                                for name in dataId.dimensions().links())
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

          registry.query("SELECT * FROM Instrument WHERE instrument=:name",
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
        datasetType.normalize(universe=self.dimensions)
        # If the DatasetType is already in the cache, we assume it's already in
        # the DB (note that we don't actually provide a way to remove them from
        # the DB).
        existingDatasetType = self._datasetTypes.get(datasetType.name, None)
        # If it's not in the cache, try to insert it.
        if existingDatasetType is None:
            try:
                self._connection.execute(
                    self._schema.tables["DatasetType"].insert().values(
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
                        self._schema.tables["DatasetTypeDimensions"].insert(),
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

    def getDatasetType(self, name):
        # Docstring inherited from Registry.getDatasetType.
        datasetTypeTable = self._schema.tables["DatasetType"]
        datasetTypeDimensionsTable = self._schema.tables["DatasetTypeDimensions"]
        # Get StorageClass from DatasetType table
        result = self._connection.execute(select([datasetTypeTable.c.storage_class]).where(
            datasetTypeTable.c.dataset_type_name == name)).fetchone()

        if result is None:
            raise KeyError("Could not find entry for datasetType {}".format(name))

        storageClass = self.storageClasses.getStorageClass(result["storage_class"])
        # Get Dimensions (if any) from DatasetTypeDimensions table
        result = self._connection.execute(select([datasetTypeDimensionsTable.c.dimension_name]).where(
            datasetTypeDimensionsTable.c.dataset_type_name == name)).fetchall()
        dimensions = self.dimensions.extract((r[0] for r in result) if result else ())
        datasetType = DatasetType(name=name,
                                  storageClass=storageClass,
                                  dimensions=dimensions)
        return datasetType

    @transactional
    def addDataset(self, datasetType, dataId, run, producer=None, recursive=False, **kwds):
        # Docstring inherited from Registry.addDataset

        if not isinstance(datasetType, DatasetType):
            datasetType = self.getDatasetType(datasetType)
        else:
            datasetType.normalize(universe=self.dimensions)

        # Make a full DataId up front, so we don't do multiple times
        # in calls below.  Note that calling DataId with a full DataId
        # is basically a no-op.
        dataId = DataId(dataId, dimensions=datasetType.dimensions, universe=self.dimensions, **kwds)

        # Expand Dimension links to insert into the table to include implied
        # dependencies.
        if not self.limited:
            self.expandDataId(dataId)
        links = dataId.implied()

        # Add the Dataset table entry itself.  Note that this will get rolled
        # back if the subsequent call to associate raises, which is what we
        # want.
        datasetTable = self._schema.tables["Dataset"]
        datasetRef = DatasetRef(datasetType=datasetType, dataId=dataId, run=run)
        # TODO add producer
        result = self._connection.execute(datasetTable.insert().values(dataset_type_name=datasetType.name,
                                                                       run_id=run.id,
                                                                       dataset_ref_hash=datasetRef.hash,
                                                                       quantum_id=None,
                                                                       **links))
        datasetRef._id = result.inserted_primary_key[0]

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
        datasetTable = self._schema.tables["Dataset"]
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

        datasetTable = self._schema.tables["Dataset"]

        # Remove related quanta.  We actually delete from Execution, because
        # Quantum's primary key (quantum_id) is also a foreign key to
        # Execution.execution_id.  We then rely on ON DELETE CASCADE to remove
        # the Quantum record as well as any related records in
        # DatasetConsumers.  Note that we permit a Quantum to be deleted
        # without removing the Datasets it refers to, but do not allow a
        # Dataset to be deleting without removing the Quanta that refer to
        # them.  A Dataset is still quite usable without provenance, but
        # provenance is worthless if it's inaccurate.
        executionTable = self._schema.tables["Execution"]
        datasetConsumersTable = self._schema.tables["DatasetConsumers"]
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
        datasetCompositionTable = self._schema.tables["DatasetComposition"]
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

        datasetCollectionTable = self._schema.tables["DatasetCollection"]
        insertQuery = datasetCollectionTable.insert()
        checkQuery = datasetCollectionTable.select(datasetCollectionTable.c.dataset_id).where(
            and_(datasetCollectionTable.c.collection == collection,
                 datasetCollectionTable.c.dataset_ref_hash == bindparam("hash"))
        )

        for ref in refs:

            ref.datasetType.normalize(universe=self.dimensions)

            if ref.id is None:
                raise AmbiguousDatasetError(f"Cannot associate dataset {ref} without ID.")

            try:
                self._connection.execute(insertQuery, {"dataset_id": ref.id, "dataset_ref_hash": ref.hash,
                                                       "collection": collection})
            except IntegrityError:
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
                    )
            self.associate(collection, ref.components.values())

    @transactional
    def disassociate(self, collection, refs):
        # Docstring inherited from Registry.disassociate.
        datasetCollectionTable = self._schema.tables["DatasetCollection"]
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
        datasetStorageTable = self._schema.tables["DatasetStorage"]
        values = dict(dataset_id=ref.id,
                      datastore_name=datastoreName)
        self._connection.execute(datasetStorageTable.insert().values(**values))

    def getDatasetLocations(self, ref):
        # Docstring inherited from Registry.getDatasetLocation.
        if ref.id is None:
            raise AmbiguousDatasetError(f"Cannot add location for dataset {ref} without ID.")
        datasetStorageTable = self._schema.tables["DatasetStorage"]
        result = self._connection.execute(
            select([datasetStorageTable.c.datastore_name]).where(
                and_(datasetStorageTable.c.dataset_id == ref.id))).fetchall()

        return {r["datastore_name"] for r in result}

    @transactional
    def removeDatasetLocation(self, datastoreName, ref):
        # Docstring inherited from Registry.getDatasetLocation.
        datasetStorageTable = self._schema.tables["DatasetStorage"]
        self._connection.execute(datasetStorageTable.delete().where(
            and_(datasetStorageTable.c.dataset_id == ref.id,
                 datasetStorageTable.c.datastore_name == datastoreName)))

    @transactional
    def addExecution(self, execution):
        # Docstring inherited from Registry.addExecution
        executionTable = self._schema.tables["Execution"]
        result = self._connection.execute(executionTable.insert().values(execution_id=execution.id,
                                                                         start_time=execution.startTime,
                                                                         end_time=execution.endTime,
                                                                         host=execution.host))
        # Reassign id, may have been `None`
        execution._id = result.inserted_primary_key[0]

    def getExecution(self, id):
        # Docstring inherited from Registry.getExecution
        executionTable = self._schema.tables["Execution"]
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
        runTable = self._schema.tables["Run"]
        # TODO: this check is probably undesirable, as we may want to have
        # multiple Runs output to the same collection.  Fixing this requires
        # (at least) modifying getRun() accordingly.
        selection = select([exists().where(runTable.c.collection == run.collection)])
        if self._connection.execute(selection).scalar():
            raise ConflictingDefinitionError(f"A run already exists with this collection: {run.collection}")
        # First add the Execution part
        self.addExecution(run)
        # Then the Run specific part
        self._connection.execute(runTable.insert().values(execution_id=run.id,
                                                          collection=run.collection,
                                                          environment_id=None,  # TODO add environment
                                                          pipeline_id=None))    # TODO add pipeline
        # TODO: set given Run's "id" attribute.

    def getRun(self, id=None, collection=None):
        # Docstring inherited from Registry.getRun
        executionTable = self._schema.tables["Execution"]
        runTable = self._schema.tables["Run"]
        run = None
        # Retrieve by id
        if (id is not None) and (collection is None):
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
        return run

    @transactional
    def addQuantum(self, quantum):
        # Docstring inherited from Registry.addQuantum.
        quantumTable = self._schema.tables["Quantum"]
        datasetConsumersTable = self._schema.tables["DatasetConsumers"]
        # First add the Execution part
        self.addExecution(quantum)
        # Then the Quantum specific part
        self._connection.execute(quantumTable.insert().values(execution_id=quantum.id,
                                                              task=quantum.task,
                                                              run_id=quantum.run.id))
        # Attach dataset consumers
        # We use itertools.chain here because quantum.predictedInputs is a
        # dict of ``name : [DatasetRef, ...]`` and we need to flatten it
        # for inserting.
        flatInputs = itertools.chain.from_iterable(quantum.predictedInputs.values())
        self._connection.execute(datasetConsumersTable.insert(),
                                 [{"quantum_id": quantum.id, "dataset_id": ref.id, "actual": False}
                                     for ref in flatInputs])

    def getQuantum(self, id):
        # Docstring inherited from Registry.getQuantum.
        executionTable = self._schema.tables["Execution"]
        quantumTable = self._schema.tables["Quantum"]
        result = self._connection.execute(
            select([quantumTable.c.task,
                    quantumTable.c.run_id,
                    executionTable.c.start_time,
                    executionTable.c.end_time,
                    executionTable.c.host]).select_from(quantumTable.join(executionTable)).where(
                quantumTable.c.execution_id == id)).fetchone()
        if result is not None:
            run = self.getRun(id=result["run_id"])
            quantum = Quantum(task=result["task"],
                              run=run,
                              startTime=result["start_time"],
                              endTime=result["end_time"],
                              host=result["host"],
                              id=id)
            # Add predicted and actual inputs to quantum
            datasetConsumersTable = self._schema.tables["DatasetConsumers"]
            for result in self._connection.execute(select([datasetConsumersTable.c.dataset_id,
                                                           datasetConsumersTable.c.actual]).where(
                    datasetConsumersTable.c.quantum_id == id)):
                ref = self.getDataset(result["dataset_id"])
                quantum.addPredictedInput(ref)
                if result["actual"]:
                    quantum._markInputUsed(ref)
            return quantum
        else:
            return None

    @transactional
    def markInputUsed(self, quantum, ref):
        # Docstring inherited from Registry.markInputUsed.
        datasetConsumersTable = self._schema.tables["DatasetConsumers"]
        result = self._connection.execute(datasetConsumersTable.update().where(and_(
            datasetConsumersTable.c.quantum_id == quantum.id,
            datasetConsumersTable.c.dataset_id == ref.id)).values(actual=True))
        if result.rowcount != 1:
            raise KeyError("{} is not a predicted consumer for {}".format(ref, quantum))
        quantum._markInputUsed(ref)

    @disableWhenLimited
    @transactional
    def addDimensionEntry(self, dimension, dataId=None, entry=None, **kwds):
        # Docstring inherited from Registry.addDimensionEntry.
        dataId = DataId(dataId, dimension=dimension, universe=self.dimensions, **kwds)
        # The given dimension should be the only leaf dimension of the graph,
        # and this should ensure it's a true `Dimension`, not a `str` name.
        dimension, = dataId.dimensions().leaves
        if entry is not None:
            dataId.entries[dimension].update(entry)
        table = self._schema.tables[dimension.name]
        if table is None:
            raise TypeError(f"Dimension '{dimension.name}' has no table.")
        try:
            self._connection.execute(table.insert().values(**dataId.fields(dimension, region=False)))
        except IntegrityError:
            # TODO check for conflict, not just existence.
            raise ConflictingDefinitionError(f"Existing definition for {dimension.name} entry with {dataId}.")
        if dataId.region is not None:
            self.setDimensionRegion(dataId)
        return dataId

    @disableWhenLimited
    def findDimensionEntry(self, dimension, dataId=None, **kwds):
        # Docstring inherited from Registry.findDimensionEntry
        dataId = DataId(dataId, dimension=dimension, universe=self.dimensions)
        # The given dimension should be the only leaf dimension of the graph,
        # and this should ensure it's a true `Dimension`, not a `str` name.
        dimension, = dataId.dimensions().leaves
        table = self._schema.tables[dimension.name]
        result = self._connection.execute(select([table]).where(
            and_(table.c[name] == value for name, value in dataId.items()))).fetchone()
        if result is not None:
            return dict(result.items())
        else:
            return None

    @disableWhenLimited
    @transactional
    def setDimensionRegion(self, dataId=None, *, update=True, region=None, **kwds):
        # Docstring inherited from Registry.setDimensionRegion
        dataId = DataId(dataId, universe=self.dimensions, region=region, **kwds)
        if dataId.region is None:
            raise ValueError("No region provided.")
        holder = dataId.dimensions().getRegionHolder()
        if holder.links() != dataId.dimensions().links():
            raise ValueError(
                f"Data ID contains superfluous keys: {dataId.dimensions().links() - holder.links()}"
            )
        table = self._schema.tables[holder.name]
        # Update the region for an existing entry
        if update:
            result = self._connection.execute(
                table.update().where(
                    and_((table.columns[name] == dataId[name] for name in holder.links()))
                ).values(
                    region=dataId.region.encode()
                )
            )
            if result.rowcount == 0:
                raise ValueError("No records were updated when setting region, did you forget update=False?")
        else:  # Insert rather than update.
            self._connection.execute(
                table.insert().values(
                    region=dataId.region.encode(),
                    **dataId
                )
            )
        # Update the join table between this Dimension and SkyPix, if it isn't
        # itself a view.
        join = dataId.dimensions().union(["SkyPix"]).joins().findIf(
            lambda join: join != holder and join.name not in self._schema.views
        )
        if join is None:
            return
        if update:
            # Delete any old SkyPix join entries for this Dimension
            self._connection.execute(
                self._schema.tables[join.name].delete().where(
                    and_((self._schema.tables[join.name].c[name] == dataId[name]
                          for name in holder.links()))
                )
            )
        parameters = []
        for begin, end in self.pixelization.envelope(dataId.region).ranges():
            for skypix in range(begin, end):
                parameters.append(dict(dataId, skypix=skypix))
        self._connection.execute(self._schema.tables[join.name].insert(), parameters)
        return dataId

    @disableWhenLimited
    def selectDimensions(self, originInfo, expression=None, neededDatasetTypes=(), futureDatasetTypes=(),
                         expandDataIds=True):
        # Docstring inherited from Registry.selectDimensions
        def standardize(dsType):
            if not isinstance(dsType, DatasetType):
                dsType = self.getDatasetType(dsType)
            else:
                dsType.normalize(universe=self.dimensions)
            return dsType

        needed = [standardize(t) for t in neededDatasetTypes]
        future = [standardize(t) for t in futureDatasetTypes]

        builder = MultipleDatasetQueryBuilder.fromDatasetTypes(
            self,
            originInfo,
            required=needed,
            optional=future,
            deferOptionalDatasetQueries=self.config["deferOutputIdQueries"]
        )

        if expression is not None:
            builder.whereParsedExpression(expression)

        return builder.execute(expandDataIds=expandDataIds)

    @disableWhenLimited
    def _queryMetadata(self, element, dataId, columns):
        # Docstring inherited from Registry._queryMetadata.
        table = self._schema.tables[element.name]
        cols = [table.c[col] for col in columns]
        row = self._connection.execute(
            select(cols)
            .where(
                and_(table.c[name] == value for name, value in dataId.items()
                     if name in element.links())
            )
        ).fetchone()
        if row is None:
            raise LookupError(f"{element.name} entry for {dataId} not found.")
        return {c.name: row[c.name] for c in cols}
