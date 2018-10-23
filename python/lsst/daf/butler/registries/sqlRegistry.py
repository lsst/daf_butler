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
from sqlalchemy.sql import select, and_, exists
from sqlalchemy.exc import IntegrityError

from lsst.sphgeom import ConvexPolygon

from ..core.utils import transactional

from ..core.datasets import DatasetType, DatasetRef
from ..core.registry import RegistryConfig, Registry, disableWhenLimited
from ..core.schema import Schema
from ..core.execution import Execution
from ..core.run import Run
from ..core.quantum import Quantum
from ..core.storageClass import StorageClassFactory
from ..core.config import Config
from ..core.sqlRegistryDatabaseDict import SqlRegistryDatabaseDict
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
    dataUnitConfig : `DataUnitConfig` or `str`
        Definition of the DataUnitRegistry to use.
    create : `bool`
        Assume registry is empty and create a new one.
    """

    defaultConfigFile = None
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    def __init__(self, registryConfig, schemaConfig, dataUnitConfig, create=False):
        registryConfig = SqlRegistryConfig(registryConfig)
        super().__init__(registryConfig, dataUnitConfig=dataUnitConfig)
        self.storageClasses = StorageClassFactory()
        self._schema = Schema(config=schemaConfig, limited=self.limited)
        self._engine = create_engine(self.config["db"])
        self._datasetTypes = {}
        self._connection = self._engine.connect()
        if not self.limited:
            self._preFlight = SqlPreFlight(self._schema, self._dataUnits, self._connection)
        if create:
            self._createTables()

    def __str__(self):
        return self.config["db"]

    @contextlib.contextmanager
    def transaction(self):
        """Context manager that implements SQL transactions.

        Will roll back any changes to the `SqlRegistry` database
        in case an exception is raised in the enclosed block.

        This context manager may be nested.
        """
        trans = self._connection.begin()
        try:
            yield
            trans.commit()
        except BaseException:
            trans.rollback()
            raise

    def _createTables(self):
        self._schema.metadata.create_all(self._engine)

    def _isValidDatasetType(self, datasetType):
        """Check if given `DatasetType` instance is valid for this `Registry`.

        .. todo::

            Insert checks for `storageClass`, `dataUnits` and `template`.
        """
        return isinstance(datasetType, DatasetType)

    def _validateDataId(self, datasetType, dataId):
        """Check if a dataId is valid for a particular `DatasetType`.

        .. todo::

            Move this function to some other place once DataUnit relations are
            implemented.

        datasetType : `DatasetType`
            The `DatasetType`.
        dataId : `dict`
            A `dict` of `DataUnit` link name, value pairs that label the
            `DatasetRef` within a collection.

        Raises
        ------
        ValueError
            If the dataId is invalid for the given datasetType.
        """
        for name in datasetType.dataUnits:
            try:
                self._dataUnits[name].validateId(dataId)
            except ValueError as err:
                raise ValueError("Error validating {}".format(datasetType.name)) from err

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

    def _findDatasetId(self, collection, datasetType, dataId):
        """Lookup a dataset ID.

        This can be used to obtain a ``dataset_id`` that permits the dataset
        to be read from a `Datastore`.

        Parameters
        ----------
        collection : `str`
            Identifies the collection to search.
        datasetType : `DatasetType`
            The `DatasetType`.
        dataId : `dict`
            A `dict` of `DataUnit` link name, value pairs that label the
            `DatasetRef` within a collection.

        Returns
        -------
        dataset_id : `int` or `None`
            ``dataset_id`` value, or `None` if no matching Dataset was found.

        Raises
        ------
        ValueError
            If dataId is invalid.
        """
        self._validateDataId(datasetType, dataId)
        datasetTable = self._schema.tables["Dataset"]
        datasetCollectionTable = self._schema.tables["DatasetCollection"]
        dataIdExpression = and_((self._schema.datasetTable.c[name] == dataId[name]
                                 for name in self._dataUnits.getPrimaryKeyNames(
                                     datasetType.dataUnits)))
        result = self._connection.execute(select([datasetTable.c.dataset_id]).select_from(
            datasetTable.join(datasetCollectionTable)).where(and_(
                datasetTable.c.dataset_type_name == datasetType.name,
                datasetCollectionTable.c.collection == collection,
                dataIdExpression))).fetchone()
        # TODO update unit values and add Run, Quantum and assembler?
        if result is not None:
            return result.dataset_id
        else:
            return None

    def find(self, collection, datasetType, dataId):
        """Lookup a dataset.

        This can be used to obtain a `DatasetRef` that permits the dataset to
        be read from a `Datastore`.

        Parameters
        ----------
        collection : `str`
            Identifies the collection to search.
        datasetType : `DatasetType`
            The `DatasetType`.
        dataId : `dict`
            A `dict` of `DataUnit` link name, value pairs that label the
            `DatasetRef` within a collection.

        Returns
        -------
        ref : `DatasetRef`
            A ref to the Dataset, or `None` if no matching Dataset
            was found.

        Raises
        ------
        ValueError
            If dataId is invalid.
        """
        dataset_id = self._findDatasetId(collection, datasetType, dataId)
        # TODO update unit values and add Run, Quantum and assembler?
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
        datasetTypeUnitsTable = self._schema.tables["DatasetTypeUnits"]
        values = {"dataset_type_name": datasetType.name,
                  "storage_class": datasetType.storageClass.name}
        self._connection.execute(datasetTypeTable.insert().values(**values))
        if datasetType.dataUnits:
            self._connection.execute(datasetTypeUnitsTable.insert(),
                                     [{"dataset_type_name": datasetType.name, "unit_name": dataUnitName}
                                      for dataUnitName in datasetType.dataUnits])
        self._datasetTypes[datasetType.name] = datasetType
        # Also register component DatasetTypes (if any)
        for compName, compStorageClass in datasetType.storageClass.components.items():
            compType = DatasetType(datasetType.componentTypeName(compName),
                                   datasetType.dataUnits,
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
        datasetTypeUnitsTable = self._schema.tables["DatasetTypeUnits"]
        # Get StorageClass from DatasetType table
        result = self._connection.execute(select([datasetTypeTable.c.storage_class]).where(
            datasetTypeTable.c.dataset_type_name == name)).fetchone()

        if result is None:
            raise KeyError("Could not find entry for datasetType {}".format(name))

        storageClass = self.storageClasses.getStorageClass(result["storage_class"])
        # Get DataUnits (if any) from DatasetTypeUnits table
        result = self._connection.execute(select([datasetTypeUnitsTable.c.unit_name]).where(
            datasetTypeUnitsTable.c.dataset_type_name == name)).fetchall()
        dataUnits = (r[0] for r in result) if result else ()
        datasetType = DatasetType(name=name,
                                  storageClass=storageClass,
                                  dataUnits=dataUnits)
        return datasetType

    @transactional
    def addDataset(self, datasetType, dataId, run, producer=None, recursive=False):
        """Adds a Dataset entry to the `Registry`

        This always adds a new Dataset; to associate an existing Dataset with
        a new collection, use ``associate``.

        Parameters
        ----------
        datasetType : `DatasetType`
            Type of the Dataset.
        dataId : `dict`
            A `dict` of `DataUnit` link name, value pairs that label the
            `DatasetRef` within a collection.
        run : `Run`
            The `Run` instance that produced the Dataset.  Ignored if
            ``producer`` is passed (`producer.run` is then used instead).
            A Run must be provided by one of the two arguments.
        producer : `Quantum`
            Unit of work that produced the Dataset.  May be `None` to store
            no provenance information, but if present the `Quantum` must
            already have been added to the SqlRegistry.
        recursive : `bool`
            If True, recursively add Dataset and attach entries for component
            Datasets as well.

        Returns
        -------
        ref : `DatasetRef`
            A newly-created `DatasetRef` instance.

        Raises
        ------
        ValueError
            If a Dataset with the given `DatasetRef` already exists in the
            given collection.

        Exception
            If ``dataId`` contains unknown or invalid `DataUnit` entries.
        """
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
        datasetRef = None
        # TODO add producer
        result = self._connection.execute(datasetTable.insert().values(dataset_type_name=datasetType.name,
                                                                       run_id=run.id,
                                                                       quantum_id=None,
                                                                       **dataId))
        datasetRef = DatasetRef(datasetType=datasetType, dataId=dataId, id=result.inserted_primary_key[0],
                                run=run)
        # A dataset is always associated with its Run collection
        self.associate(run.collection, [datasetRef, ], transactional=False)

        if recursive:
            for component in datasetType.storageClass.components:
                compTypeName = datasetType.componentTypeName(component)
                compDatasetType = self.getDatasetType(compTypeName)
                compRef = self.addDataset(compDatasetType, dataId, run=run, producer=producer,
                                          recursive=True, transactional=False)
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
            # dataUnitName gives a `str` key which which is used to lookup
            # the corresponding sqlalchemy.core.Column entry to index the result
            # because the name of the key may not be the name of the name of the
            # DataUnit link.
            dataId = {dataUnitName: result[self._schema.datasetTable.c[dataUnitName]]
                      for dataUnitName in self._dataUnits.getPrimaryKeyNames(datasetType.dataUnits)}
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
            ref = DatasetRef(datasetType=datasetType, dataId=dataId, id=id, run=run)
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
        DatasetType and unit values but with different ``dataset_id`` exists
        in a collection then exception is raised.

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
        #   new DatasetRefs using units
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
                If DatasetRef unit values match row data but their IDs differ.
            """
            if row.dataset_id == ref.id:
                return True

            if row.dataset_type_name != ref.datasetType.name:
                return False

            columns = self._dataUnits.getPrimaryKeyNames(ref.datasetType.dataUnits)
            dataId = ref.dataId
            if all(row[col] == dataId[col] for col in columns):
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
            # full scan of a collection to compare DatasetRef units
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
                                     [{"dataset_id": ref.id, "collection": collection} for ref in refs])

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

    def getDataUnitDefinition(self, dataUnitName):
        """Return the definition of a DataUnit (an actual `DataUnit` object).

        Parameters
        ----------
        dataUnitName : `str`
            Name of the DataUnit, e.g. "Instrument", "Tract", etc.
        """
        # TODO: remove this when DataUnitRegistry is a singleton
        return self._dataUnits[dataUnitName]

    @disableWhenLimited
    @transactional
    def addDataUnitEntry(self, dataUnitName, values):
        """Add a new `DataUnit` entry.

        dataUnitName : `str`
            Name of the `DataUnit` (e.g. ``"Instrument"``).
        values : `dict`
            Dictionary of ``columnName, columnValue`` pairs.

        If ``values`` includes a "region" key, `setDataUnitRegion` will
        automatically be called to set it any associated spatial join
        tables.
        Region fields associated with a combination of DataUnits must be
        explicitly set separately.

        Raises
        ------
        TypeError
            If the given `DataUnit` does not have explicit entries in the
            registry.
        ValueError
            If an entry with the primary-key defined in `values` is already
            present.
        NotImplementedError
            Raised if `limited` is `True`.
        """
        dataUnit = self._dataUnits[dataUnitName]
        dataUnit.validateId(values)
        dataUnitTable = self._schema.tables[dataUnitName]
        v = values.copy()
        region = v.pop("region", None)
        if dataUnitTable is None:
            raise TypeError("DataUnit '{}' has no table.".format(dataUnitName))
        try:
            self._connection.execute(dataUnitTable.insert().values(**v))
        except IntegrityError as err:
            raise ValueError(str(err))  # TODO this should do an explicit validity check instead
        if region is not None:
            self.setDataUnitRegion((dataUnitName,), v, region)

    @disableWhenLimited
    def findDataUnitEntry(self, dataUnitName, value):
        """Return a `DataUnit` entry corresponding to a `value`.

        Parameters
        ----------
        dataUnitName : `str`
            Name of a `DataUnit`
        value : `dict`
            A dictionary of values that uniquely identify the `DataUnit`.

        Returns
        -------
        dataUnitEntry : `dict`
            Dictionary with all `DataUnit` values, or `None` if no matching
            entry is found.
        NotImplementedError
            Raised if `limited` is `True`.
        """
        dataUnit = self._dataUnits[dataUnitName]
        dataUnit.validateId(value)
        dataUnitTable = self._schema.tables[dataUnitName]
        primaryKeyColumns = {k: self._schema.tables[dataUnit.name].c[k] for k in dataUnit.primaryKey}
        result = self._connection.execute(select([dataUnitTable]).where(
            and_((primaryKeyColumns[name] == value[name] for name in primaryKeyColumns)))).fetchone()
        if result is not None:
            return dict(result.items())
        else:
            return None

    @disableWhenLimited
    @transactional
    def setDataUnitRegion(self, dataUnitNames, value, region, update=True):
        """Set the region field for a DataUnit instance or a combination
        thereof and update associated spatial join tables.

        Parameters
        ----------
        dataUnitNames : sequence
            A sequence of DataUnit names whose instances are jointly associated
            with a region on the sky. This must not include dependencies that
            are implied, e.g. "Patch" must not include "Tract", but "Detector"
            needs to add "Visit".
        value : `dict`
            A dictionary of values that uniquely identify the DataUnits.
        region : `sphgeom.ConvexPolygon`
            Region on the sky.
        update : `bool`
            If True, existing region information for these DataUnits is being
            replaced.  This is usually required because DataUnit entries are
            assumed to be pre-inserted prior to calling this function.

        Raises
        ------
        NotImplementedError
            Raised if `limited` is `True`.
        """
        primaryKey = set()
        regionUnitNames = []
        for dataUnitName in dataUnitNames:
            dataUnit = self._dataUnits[dataUnitName]
            dataUnit.validateId(value)
            primaryKey.update(dataUnit.primaryKey)
            regionUnitNames.append(dataUnitName)
            regionUnitNames += [d.name for d in dataUnit.requiredDependencies]
        regionDataUnit = self._dataUnits.getRegionHolder(*dataUnitNames)
        table = self._schema.tables[regionDataUnit.name]
        if table is None:
            raise TypeError("No region table found for '{}'.".format(dataUnitNames))
        # Update the region for an existing entry
        if update:
            result = self._connection.execute(
                table.update().where(
                    and_((table.columns[name] == value[name] for name in primaryKey))
                ).values(
                    region=region.encode()
                )
            )
            if result.rowcount == 0:
                raise ValueError("No records were updated when setting region, did you forget update=False?")
        else:  # Insert rather than update.
            self._connection.execute(
                table.insert().values(
                    region=region.encode(),
                    **value
                )
            )
        assert "SkyPix" not in dataUnitNames
        join = self._dataUnits.getJoin(dataUnitNames, "SkyPix")
        if join is None or join.name in self._schema.views:
            return
        if update:
            # Delete any old SkyPix join entries for this DataUnit
            self._connection.execute(
                self._schema.tables[join.name].delete().where(
                    and_((self._schema.tables[join.name].c[name] == value[name] for name in primaryKey))
                )
            )
        parameters = []
        for begin, end in self.pixelization.envelope(region).ranges():
            for skypix in range(begin, end):
                parameters.append(dict(value, skypix=skypix))
        self._connection.execute(self._schema.tables[join.name].insert(), parameters)

    @disableWhenLimited
    def getRegion(self, dataId):
        """Get region associated with a dataId.

        Parameters
        ----------
        dataId : `dict`
            A `dict` of `DataUnit` link name, value pairs that label the
            `DatasetRef` within a collection.

        Returns
        -------
        region : `lsst.sphgeom.ConvexPolygon`
            The region associated with a ``dataId`` or `None` if not present.

        Raises
        ------
        KeyError
            If the set of dataunits for the ``dataId`` does not correspond to
            a unique spatial lookup.
        """
        dataUnitNames = (self._dataUnits.getByLinkName(linkName).name for linkName in dataId)
        regionHolder = self._dataUnits.getRegionHolder(*tuple(dataUnitNames))
        # Skypix does not have a table to lookup the region in, instead generate it
        if regionHolder == self._dataUnits["SkyPix"]:
            return self.pixelization.pixel(dataId["skypix"])
        # Lookup region
        primaryKeyColumns = {k: self._schema.tables[regionHolder.name].c[k] for k in regionHolder.primaryKey}
        result = self._connection.execute(select([self._schema.tables[regionHolder.name].c["region"]]).where(
            and_((primaryKeyColumns[name] == dataId[name] for name in primaryKeyColumns)))).fetchone()
        if result is not None:
            return ConvexPolygon.decode(result[0])
        else:
            return None

    @disableWhenLimited
    def selectDataUnits(self, originInfo, expression, neededDatasetTypes, futureDatasetTypes):
        """Evaluate a filter expression and lists of
        `DatasetTypes <DatasetType>` and return a set of data unit values.

        Returned set consists of combinations of units participating in data
        transformation from ``neededDatasetTypes`` to ``futureDatasetTypes``,
        restricted by existing data and filter expression.

        Parameters
        ----------
        originInfo : `DatasetOriginInfo`
            Object which provides names of the input/output collections.
        expression : `str`
            An expression that limits the `DataUnits <DataUnit>` and
            (indirectly) the Datasets returned.
        neededDatasetTypes : `list` of `DatasetType`
            The `list` of `DatasetTypes <DatasetType>` whose DataUnits will
            be included in the returned column set. Output is limited to the
            the Datasets of these DatasetTypes which already exist in the
            registry.
        futureDatasetTypes : `list` of `DatasetType`
            The `list` of `DatasetTypes <DatasetType>` whose DataUnits will
            be included in the returned column set. It is expected that
            Datasets for these DatasetTypes do not exist in the registry,
            but presently this is not checked.

        Yields
        ------
        row : `PreFlightUnitsRow`
            Single row is a unique combination of units in a transform.

        Raises
        ------
        NotImplementedError
            Raised if `limited` is `True`.
        """
        return self._preFlight.selectDataUnits(originInfo,
                                               expression,
                                               neededDatasetTypes,
                                               futureDatasetTypes)
