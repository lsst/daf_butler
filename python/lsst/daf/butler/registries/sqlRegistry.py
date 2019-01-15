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

import itertools
import contextlib

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select, and_, exists
from sqlalchemy.exc import IntegrityError

from ..core.utils import transactional

from ..core.datasets import DatasetType, DatasetRef
from ..core.registry import RegistryConfig, Registry, disableWhenLimited
from ..core.schema import Schema
from ..core.execution import Execution
from ..core.run import Run
from ..core.quantum import Quantum
from ..core.storageClass import StorageClassFactory
from ..core.config import Config
from ..core.dimensions import DataId, DimensionGraph
from .sqlRegistryDatabaseDict import SqlRegistryDatabaseDict
from .sqlPreFlight import SqlPreFlight

__all__ = ("SqlRegistryConfig", "SqlRegistry")


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

    def _findDatasetId(self, collection, datasetType, dataId, **kwds):
        """Lookup a dataset ID.

        This can be used to obtain a ``dataset_id`` that permits the dataset
        to be read from a `Datastore`.

        Parameters
        ----------
        collection : `str`
            Identifies the collection to search.
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.
        dataId : `dict` or `DataId`
            A `dict` of `Dimension` link fields that label a Dataset
            within a Collection.
        kwds
            Additional keyword arguments passed to the `DataId` constructor
            to convert ``dataId`` to a true `DataId` or augment an existing
            one.

        Returns
        -------
        dataset_id : `int` or `None`
            ``dataset_id`` value, or `None` if no matching Dataset was found.

        Raises
        ------
        ValueError
            If dataId is invalid.
        """
        if not isinstance(datasetType, DatasetType):
            datasetType = self.getDatasetType(datasetType)
        dataId = DataId(dataId, dimensions=datasetType.dimensions, universe=self.dimensions, **kwds)
        datasetTable = self._schema.tables["Dataset"]
        datasetCollectionTable = self._schema.tables["DatasetCollection"]
        dataIdExpression = and_(self._schema.datasetTable.c[name] == dataId[name]
                                for name in dataId.dimensions().links())
        result = self._connection.execute(select([datasetTable.c.dataset_id]).select_from(
            datasetTable.join(datasetCollectionTable)).where(and_(
                datasetTable.c.dataset_type_name == datasetType.name,
                datasetCollectionTable.c.collection == collection,
                dataIdExpression))).fetchone()
        # TODO update dimension values and add Run, Quantum and assembler?
        if result is not None:
            return result.dataset_id
        else:
            return None

    def find(self, collection, datasetType, dataId=None, **kwds):
        # Docstring inherited from Registry.find
        dataset_id = self._findDatasetId(collection, datasetType, dataId, **kwds)
        # TODO update dimension values and add Run, Quantum and assembler?
        if dataset_id is not None:
            return self.getDataset(dataset_id)
        else:
            return None

    def query(self, sql, **params):
        """Execute a SQL SELECT statement directly.

        Named parameters are specified in the SQL query string by preceeding
        them with a colon.  Parameter values are provided as additional
        keyword arguments.  For example:

          registry.query("SELECT * FROM Instrument WHERE instrument=:name", name="HSC")

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
        """
        Add a new `DatasetType` to the SqlRegistry.

        It is not an error to register the same `DatasetType` twice.

        Parameters
        ----------
        datasetType : `DatasetType`
            The `DatasetType` to be added.

        Raises
        ------
        ValueError
            DatasetType is not valid for this registry or is already registered
            but not identical.

        Returns
        -------
        inserted : `bool`
            `True` if ``datasetType`` was inserted, `False` if an identical
            existing `DatsetType` was found.
        """
        if not self._isValidDatasetType(datasetType):
            raise ValueError("DatasetType is not valid for this registry")
        # If a DatasetType is already registered it must be identical
        try:
            # A DatasetType entry with this name may exist, get it first.
            # Note that we can't just look in the cache, because it may not be there yet.
            existingDatasetType = self.getDatasetType(datasetType.name)
        except KeyError:
            # No registered DatasetType with this name exists, move on to inserting it
            pass
        else:
            # A DatasetType with this name exists, check if is equal
            if datasetType == existingDatasetType:
                return False
            else:
                raise ValueError("DatasetType: {} != existing {}".format(datasetType, existingDatasetType))
        # Insert it
        datasetTypeTable = self._schema.tables["DatasetType"]
        datasetTypeDimensionsTable = self._schema.tables["DatasetTypeDimensions"]
        values = {"dataset_type_name": datasetType.name,
                  "storage_class": datasetType.storageClass.name}
        # If the DatasetType only knows about the names of dimensions, upgrade
        # that to a full DimensionGraph now.  Doing that before any database
        # is good because should validate those names.  It's also a desirable
        # side-effect.
        if not isinstance(datasetType.dimensions, DimensionGraph):
            datasetType._dimensions = self.dimensions.extract(datasetType._dimensions)
        self._connection.execute(datasetTypeTable.insert().values(**values))
        if datasetType.dimensions:
            self._connection.execute(datasetTypeDimensionsTable.insert(),
                                     [{"dataset_type_name": datasetType.name,
                                       "dimension_name": dimensionName}
                                      for dimensionName in datasetType.dimensions.names])
        self._datasetTypes[datasetType.name] = datasetType
        # Also register component DatasetTypes (if any)
        for compName, compStorageClass in datasetType.storageClass.components.items():
            compType = DatasetType(datasetType.componentTypeName(compName),
                                   datasetType.dimensions,
                                   compStorageClass)
            self.registerDatasetType(compType)
        return True

    def getDatasetType(self, name):
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

        # Make a full DataId up front, so we don't do multiple times
        # in calls below.  Note that calling DataId with a full DataId
        # is basically a no-op.
        dataId = DataId(dataId, dimensions=datasetType.dimensions, universe=self.dimensions, **kwds)

        # Expand Dimension links to insert into the table to include implied
        # dependencies.
        if not self.limited:
            self.expandDataId(dataId)
        links = dataId.implied()

        # Collection cannot have more than one unique DataId of the same
        # DatasetType, this constraint is checked in `associate` method
        # which raises.
        # NOTE: Client code (e.g. `lsst.obs.base.ingest`) has some assumptions
        # about behavior of this code, in particular that it should not modify
        # database contents if exception is raised. This is why we have to
        # make additional check for uniqueness before we add a row to Dataset
        # table.
        # TODO also note that this check is not safe
        # in the presence of concurrent calls to addDataset.
        # Then again, it is undoubtedly not the only place where
        # this problem occurs. Needs some serious thought.
        if self._findDatasetId(run.collection, datasetType, dataId) is not None:
            raise ValueError("A dataset of type {} with id: {} already exists in collection {}".format(
                datasetType, dataId, run.collection))

        datasetTable = self._schema.tables["Dataset"]
        datasetRef = DatasetRef(datasetType=datasetType, dataId=dataId, run=run)
        # TODO add producer
        result = self._connection.execute(datasetTable.insert().values(dataset_type_name=datasetType.name,
                                                                       run_id=run.id,
                                                                       dataset_ref_hash=datasetRef.hash,
                                                                       quantum_id=None,
                                                                       **links))
        datasetRef._id = result.inserted_primary_key[0]
        # A dataset is always associated with its Run collection
        self.associate(run.collection, [datasetRef, ])

        if recursive:
            for component in datasetType.storageClass.components:
                compTypeName = datasetType.componentTypeName(component)
                compDatasetType = self.getDatasetType(compTypeName)
                compRef = self.addDataset(compDatasetType, dataId, run=run, producer=producer,
                                          recursive=True)
                self.attachComponent(component, datasetRef, compRef)
        return datasetRef

    def getDataset(self, id):
        """Retrieve a Dataset entry.

        Parameters
        ----------
        id : `int`
            The unique identifier for the Dataset.

        Returns
        -------
        ref : `DatasetRef`
            A ref to the Dataset, or `None` if no matching Dataset
            was found.
        """
        datasetTable = self._schema.tables["Dataset"]
        with self._connection.begin():
            result = self._connection.execute(
                select([datasetTable]).where(datasetTable.c.dataset_id == id)).fetchone()
        if result is not None:
            datasetType = self.getDatasetType(result["dataset_type_name"])
            run = self.getRun(id=result.run_id)
            datasetRefHash = result["dataset_ref_hash"]
            dataId = DataId({link: result[self._schema.datasetTable.c[link]]
                             for link in datasetType.dimensions.links()},
                            dimensions=datasetType.dimensions,
                            universe=self.dimensions)
            # Get components (if present)
            # TODO check against expected components
            components = {}
            datasetCompositionTable = self._schema.tables["DatasetComposition"]
            results = self._connection.execute(
                select([datasetCompositionTable.c.component_name,
                        datasetCompositionTable.c.component_dataset_id]).where(
                            datasetCompositionTable.c.parent_dataset_id == id)).fetchall()
            if results is not None:
                for result in results:
                    components[result["component_name"]] = self.getDataset(result["component_dataset_id"])
            ref = DatasetRef(datasetType=datasetType, dataId=dataId, id=id, run=run, hash=datasetRefHash)
            ref._components = components
            return ref
        else:
            return None

    @transactional
    def attachComponent(self, name, parent, component):
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
        """
        # TODO Insert check for component name and type against parent.storageClass specified components
        datasetCompositionTable = self._schema.tables["DatasetComposition"]
        values = dict(component_name=name,
                      parent_dataset_id=parent.id,
                      component_dataset_id=component.id)
        self._connection.execute(datasetCompositionTable.insert().values(**values))
        parent._components[name] = component

    @transactional
    def associate(self, collection, refs):
        """Add existing Datasets to a collection, possibly creating the
        collection in the process.

        If a DatasetRef with the same exact `dataset_id`` is already in a
        collection nothing is changed. If a DatasetRef with the same
        DatasetType and dimension values but with different ``dataset_id``
        exists in a collection then exception is raised.

        Parameters
        ----------
        collection : `str`
            Indicates the collection the Datasets should be associated with.
        refs : `list` of `DatasetRef`
            A `list` of `DatasetRef` instances that already exist in this
            `SqlRegistry`.

        Raises
        ------
        ValueError
            If a Dataset with the given `DatasetRef` already exists in the
            given collection.
        """
        # A collection cannot contain more than one Dataset with the same
        # DatasetRef. Our SQL schema does not enforce this constraint yet so
        # checks have to be done in the code:
        # - read existing collection and try to match its contents with
        #   new DatasetRefs using dimensions
        # - if there is a match and dataset_id is different then constraint
        #   check fails
        # TODO: This implementation has a race which can violate the
        # constraint if multiple clients update registry concurrently. Proper
        # constraint checks have to be implmented in schema.

        def _matchRef(row, ref):
            """Compare Dataset table row with a DatasetRef.

            Parameters
            ----------
            row : `sqlalchemy.RowProxy`
                Single row from Dataset table.
            ref : `DatasetRef`

            Returns
            -------
            match : `bool`
                True if Dataset row is identical to ``ref`` (their IDs match),
                False otherwise.

            Raises
            ------
            ValueError
                If DatasetRef dimension values match row data but their IDs differ.
            """
            if row.dataset_id == ref.id:
                return True

            if row.dataset_type_name != ref.datasetType.name:
                return False

            # TODO: factor this operation out, fix use of private member
            if not isinstance(ref.datasetType.dimensions, DimensionGraph):
                ref.datasetType._dimensions = self.dimensions.extract(ref.datasetType.dimensions)

            dataId = ref.dataId
            if all(row[col] == dataId[col] for col in ref.datasetType.dimensions.links()):
                raise ValueError("A dataset of type {} with id: {} already exists in collection {}".format(
                    ref.datasetType, dataId, collection))
            return False

        if len(refs) == 1:
            # small optimization for a single ref
            ref = refs[0]
            dataset_id = self._findDatasetId(collection, ref.datasetType, ref.dataId)
            if dataset_id == ref.id:
                # already there
                return
            elif dataset_id is not None:
                raise ValueError("A dataset of type {} with id: {} already exists in collection {}".format(
                    ref.datasetType, ref.dataId, collection))
        else:
            # full scan of a collection to compare DatasetRef dimensions
            datasetTable = self._schema.tables["Dataset"]
            datasetCollectionTable = self._schema.tables["DatasetCollection"]
            query = datasetTable.select()
            query = query.where(and_(datasetTable.c.dataset_id == datasetCollectionTable.c.dataset_id,
                                     datasetCollectionTable.c.collection == collection))
            result = self._connection.execute(query)
            for row in result:
                # skip DatasetRefs that are already there
                refs = [ref for ref in refs if not _matchRef(row, ref)]

        # if any ref is not there yet add it
        if refs:
            datasetCollectionTable = self._schema.tables["DatasetCollection"]
            self._connection.execute(datasetCollectionTable.insert(),
                                     [{"dataset_id": ref.id, "dataset_ref_hash": ref.hash,
                                       "collection": collection} for ref in refs])

    @transactional
    def disassociate(self, collection, refs, remove=True):
        r"""Remove existing Datasets from a collection.

        ``collection`` and ``ref`` combinations that are not currently
        associated are silently ignored.

        Parameters
        ----------
        collection : `str`
            The collection the Datasets should no longer be associated with.
        refs : `list` of `DatasetRef`
            A `list` of `DatasetRef` instances that already exist in this
            `SqlRegistry`.
        remove : `bool`
            If `True`, remove Datasets from the `SqlRegistry` if they are not
            associated with any collection (including via any composites).

        Returns
        -------
        removed : `list` of `DatasetRef`
            If `remove` is `True`, the `list` of `DatasetRef`\ s that were
            removed.
        """
        if remove:
            raise NotImplementedError("Cleanup of datasets not yet implemented")
        datasetCollectionTable = self._schema.tables["DatasetCollection"]
        for ref in refs:
            self._connection.execute(datasetCollectionTable.delete().where(
                and_(datasetCollectionTable.c.dataset_id == ref.id,
                     datasetCollectionTable.c.collection == collection)))
        return []

    @transactional
    def addDatasetLocation(self, ref, datastoreName):
        """Add datastore name locating a given dataset.

        Typically used by `Datastore`.

        Parameters
        ----------
        ref : `DatasetRef`
            A reference to the dataset for which to add storage information.
        datastoreName : `str`
            Name of the datastore holding this dataset.
        """
        datasetStorageTable = self._schema.tables["DatasetStorage"]
        values = dict(dataset_id=ref.id,
                      datastore_name=datastoreName)
        self._connection.execute(datasetStorageTable.insert().values(**values))

    def getDatasetLocations(self, ref):
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
        """
        datasetStorageTable = self._schema.tables["DatasetStorage"]
        result = self._connection.execute(
            select([datasetStorageTable.c.datastore_name]).where(
                and_(datasetStorageTable.c.dataset_id == ref.id))).fetchall()

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
        """
        datasetStorageTable = self._schema.tables["DatasetStorage"]
        self._connection.execute(datasetStorageTable.delete().where(
            and_(datasetStorageTable.c.dataset_id == ref.id,
                 datasetStorageTable.c.datastore_name == datastoreName)))

    @transactional
    def addExecution(self, execution):
        """Add a new `Execution` to the `SqlRegistry`.

        If ``execution.id`` is `None` the `SqlRegistry` will update it to
        that of the newly inserted entry.

        Parameters
        ----------
        execution : `Execution`
            Instance to add to the `SqlRegistry`.
            The given `Execution` must not already be present in the
            `SqlRegistry`.

        Raises
        ------
        Exception
            If `Execution` is already present in the `SqlRegistry`.
        """
        executionTable = self._schema.tables["Execution"]
        result = self._connection.execute(executionTable.insert().values(execution_id=execution.id,
                                                                         start_time=execution.startTime,
                                                                         end_time=execution.endTime,
                                                                         host=execution.host))
        # Reassign id, may have been `None`
        execution._id = result.inserted_primary_key[0]

    def getExecution(self, id):
        """Retrieve an Execution.

        Parameters
        ----------
        id : `int`
            The unique identifier for the Execution.
        """
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
        """Create a new `Run` in the `SqlRegistry` and return it.

        If a run with this collection already exists, return that instead.

        Parameters
        ----------
        collection : `str`
            The collection used to identify all inputs and outputs
            of the `Run`.

        Returns
        -------
        run : `Run`
            A new `Run` instance.
        """
        run = Run(collection=collection)
        self.addRun(run)
        return run

    @transactional
    def ensureRun(self, run):
        """Conditionally add a new `Run` to the `SqlRegistry`.

        If the ``run.id`` is `None` or a `Run` with this `id` doesn't exist
        in the `Registry` yet, add it.  Otherwise, ensure the provided run is
        identical to the one already in the registry.

        Parameters
        ----------
        run : `Run`
            Instance to add to the `SqlRegistry`.

        Raises
        ------
        ValueError
            If ``run`` already exists, but is not identical.
        """
        if run.id is not None:
            existingRun = self.getRun(id=run.id)
            if run != existingRun:
                raise ValueError("{} != existing: {}".format(run, existingRun))
            return
        self.addRun(run)

    @transactional
    def addRun(self, run):
        """Add a new `Run` to the `SqlRegistry`.

        Parameters
        ----------
        run : `Run`
            Instance to add to the `SqlRegistry`.
            The given `Run` must not already be present in the `SqlRegistry`
            (or any other).  Therefore its `id` must be `None` and its
            `collection` must not be associated with any existing `Run`.

        Raises
        ------
        ValueError
            If a run already exists with this collection.
        """
        runTable = self._schema.tables["Run"]
        # TODO: this check is probably undesirable, as we may want to have multiple Runs output
        # to the same collection.  Fixing this requires (at least) modifying getRun() accordingly.
        selection = select([exists().where(runTable.c.collection == run.collection)])
        if self._connection.execute(selection).scalar():
            raise ValueError("A run already exists with this collection: {}".format(run.collection))
        # First add the Execution part
        self.addExecution(run)
        # Then the Run specific part
        self._connection.execute(runTable.insert().values(execution_id=run.id,
                                                          collection=run.collection,
                                                          environment_id=None,  # TODO add environment
                                                          pipeline_id=None))    # TODO add pipeline
        # TODO: set given Run's "id" attribute.

    def getRun(self, id=None, collection=None):
        """
        Get a `Run` corresponding to its collection or id

        Parameters
        ----------
        id : `int`, optional
            Lookup by run `id`, or:
        collection : `str`
            If given, lookup by `collection` name instead.

        Returns
        -------
        run : `Run`
            The `Run` instance.

        Raises
        ------
        ValueError
            Must supply one of ``collection`` or ``id``.
        """
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
        r"""Add a new `Quantum` to the `SqlRegistry`.

        Parameters
        ----------
        quantum : `Quantum`
            Instance to add to the `SqlRegistry`.
            The given `Quantum` must not already be present in the
            `SqlRegistry` (or any other), therefore its:

            - `run` attribute must be set to an existing `Run`.
            - `predictedInputs` attribute must be fully populated with
              `DatasetRef`\ s, and its.
            - `actualInputs` and `outputs` will be ignored.
        """
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
        """Retrieve an Quantum.

        Parameters
        ----------
        id : `int`
            The unique identifier for the Quantum.
        """
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
        """Record the given `DatasetRef` as an actual (not just predicted)
        input of the given `Quantum`.

        This updates both the `SqlRegistry`"s `Quantum` table and the Python
        `Quantum.actualInputs` attribute.

        Parameters
        ----------
        quantum : `Quantum`
            Producer to update.
            Will be updated in this call.
        ref : `DatasetRef`
            To set as actually used input.

        Raises
        ------
        KeyError
            If ``quantum`` is not a predicted consumer for ``ref``.
        """
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
        except IntegrityError as err:
            raise ValueError(str(err))  # TODO this should do an explicit validity check instead
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
    def selectDimensions(self, originInfo, expression, neededDatasetTypes, futureDatasetTypes,
                         expandDataIds=True):
        # Docstring inherited from Registry.selectDimensions
        def standardize(dsType):
            if not isinstance(dsType, DatasetType):
                dsType = self.getDatasetType(dsType)
            elif not isinstance(dsType.dimensions, DimensionGraph):
                dsType._dimensions = self.dimensions.extract(dsType.dimensions)
            return dsType
        needed = [standardize(t) for t in neededDatasetTypes]
        future = [standardize(t) for t in futureDatasetTypes]
        preFlight = SqlPreFlight(self)
        return preFlight.selectDimensions(originInfo, expression, needed, future,
                                          expandDataIds=expandDataIds)

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
