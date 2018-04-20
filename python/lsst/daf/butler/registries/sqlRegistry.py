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

from sqlalchemy import create_engine
from sqlalchemy.sql import select, and_, exists
from sqlalchemy.exc import IntegrityError

from ..core.datasets import DatasetType, DatasetRef
from ..core.registry import RegistryConfig, Registry
from ..core.schema import Schema
from ..core.execution import Execution
from ..core.run import Run
from ..core.quantum import Quantum
from ..core.storageInfo import StorageInfo
from ..core.storageClass import StorageClassFactory

__all__ = ("SqlRegistryConfig", "SqlRegistry")


class SqlRegistryConfig(RegistryConfig):
    pass


class SqlRegistry(Registry):
    """Registry backed by a SQL database.

    Parameters
    ----------
    config : `SqlRegistryConfig` or `str`
        Load configuration
    """

    def __init__(self, config):
        super().__init__(config)

        self.config = SqlRegistryConfig(config)
        self.storageClasses = StorageClassFactory()
        self._schema = Schema(self.config['schema'])
        self._engine = create_engine(self.config['db'])
        self._schema.metadata.create_all(self._engine)
        self._datasetTypes = {}

    def _isValidDatasetType(self, datasetType):
        """Check if given `DatasetType` instance is valid for this `Registry`.

        .. todo::

            Insert checks for `storageClass`, `dataUnits` and `template`.
        """
        return isinstance(datasetType, DatasetType)

    def registerDatasetType(self, datasetType):
        """
        Add a new `DatasetType` to the SqlRegistry.

        Parameters
        ----------
        datasetType : `DatasetType`
            The `DatasetType` to be added.

        Raises
        ------
        KeyError
            Dataset is already registered.
        ValueError
            DatasetType is not valid for this registry.
        """
        if not self._isValidDatasetType(datasetType):
            raise ValueError("DatasetType is not valid for this registry")
        if datasetType.name in self._datasetTypes:
            raise KeyError("DatasetType: {} already registered".format(datasetType.name))
        datasetTypeTable = self._schema.metadata.tables['DatasetType']
        datasetTypeUnitsTable = self._schema.metadata.tables['DatasetTypeUnits']
        with self._engine.begin() as connection:
            connection.execute(datasetTypeTable.insert().values(dataset_type_name=datasetType.name,
                                                                storage_class=datasetType.storageClass.name))
            if datasetType.dataUnits:
                connection.execute(datasetTypeUnitsTable.insert(),
                                   [{'dataset_type_name': datasetType.name, 'unit_name': dataUnitName}
                                    for dataUnitName in datasetType.dataUnits])
            self._datasetTypes[datasetType.name] = datasetType

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
        datasetType = None
        if name in self._datasetTypes:
            datasetType = self._datasetTypes[name]
        else:
            datasetTypeTable = self._schema.metadata.tables['DatasetType']
            datasetTypeUnitsTable = self._schema.metadata.tables['DatasetTypeUnits']
            with self._engine.begin() as connection:
                # Get StorageClass from DatasetType table
                result = connection.execute(select([datasetTypeTable.c.storage_class]).where(
                    datasetTypeTable.c.dataset_type_name == name)).fetchone()

                if result is None:
                    raise KeyError("Could not find entry for datasetType {}".format(name))

                storageClass = self.storageClasses.getStorageClass(result['storage_class'])
                # Get DataUnits (if any) from DatasetTypeUnits table
                result = connection.execute(select([datasetTypeUnitsTable.c.unit_name]).where(
                    datasetTypeUnitsTable.c.dataset_type_name == name)).fetchall()
                dataUnits = (r[0] for r in result) if result else ()
                datasetType = DatasetType(name=name,
                                          storageClass=storageClass,
                                          dataUnits=dataUnits)
        return datasetType

    def addDataset(self, datasetType, dataId, run, producer=None):
        """Add a Dataset to a Collection.

        This always adds a new Dataset; to associate an existing Dataset with
        a new `Collection`, use ``associate``.

        Parameters
        ----------
        datasetType : `str`
            Name of a `DatasetType`.
        dataId : `dict`
            A `dict` of `DataUnit` name, value pairs that label the `DatasetRef`
            within a Collection.
        run : `Run`
            The `Run` instance that produced the Dataset.  Ignored if
            ``producer`` is passed (`producer.run` is then used instead).
            A Run must be provided by one of the two arguments.
        producer : `Quantum`
            Unit of work that produced the Dataset.  May be ``None`` to store
            no provenance information, but if present the `Quantum` must
            already have been added to the SqlRegistry.

        Returns
        -------
        ref : `DatasetRef`
            A newly-created `DatasetRef` instance.

        Raises
        ------
        ValueError
            If a Dataset with the given `DatasetRef` already exists in the
            given Collection.
        """
        # TODO this is obviously not the most efficient way to check
        # for existence.
        # TODO also note that this check is not safe
        # in the presence of concurrent calls to addDataset.
        # Then again, it is undoubtedly not the only place where
        # this problem occurs. Needs some serious thought.
        if self.find(run.collection, datasetType, dataId) is not None:
            raise ValueError("A dataset with id: {} already exists in collection {}".format(
                dataId, run.collection))
        datasetTable = self._schema.metadata.tables['Dataset']
        datasetRef = None
        with self._engine.begin() as connection:
            result = connection.execute(datasetTable.insert().values(dataset_type_name=datasetType.name,
                                                                     run_id=run.id,
                                                                     quantum_id=None,  # TODO add producer
                                                                     **dataId))
            datasetRef = DatasetRef(datasetType=datasetType, dataId=dataId, id=result.inserted_primary_key[0])
            # A dataset is always associated with its Run collection
            self.associate(run.collection, [datasetRef])
        return datasetRef

    def getDataset(self, id):
        """Retrieve an Dataset.

        Parameters
        ----------
        id : `int`
            The unique identifier for the Dataset.
        """
        datasetTable = self._schema.metadata.tables['Dataset']
        with self._engine.begin() as connection:
            result = connection.execute(
                select([datasetTable]).where(datasetTable.c.dataset_id == id)).fetchone()
        if result is not None:
            datasetType = self.getDatasetType(result['dataset_type_name'])
            # dataUnitName gives a `str` key which which is used to lookup
            # the corresponding sqlalchemy.core.Column entry to index the result
            # because the name of the key may not be the name of the name of the
            # DataUnit link.
            dataId = {dataUnitName: result[self._schema.dataUnits.links[dataUnitName]]
                      for dataUnitName in self._schema.dataUnits.getPrimaryKeyNames(datasetType.dataUnits)}
            # Get components (if present)
            # TODO check against expected components
            components = {}
            datasetCompositionTable = self._schema.metadata.tables['DatasetComposition']
            with self._engine.begin() as connection:
                results = connection.execute(
                    select([datasetCompositionTable.c.component_name,
                            datasetCompositionTable.c.component_dataset_id]).where(
                                datasetCompositionTable.c.parent_dataset_id == id)).fetchall()
                if results is not None:
                    for result in results:
                        components[result['component_name']] = self.getDataset(result['component_dataset_id'])
            ref = DatasetRef(datasetType=datasetType, dataId=dataId, id=id)
            ref._components = components
            return ref
        else:
            return None

    def setAssembler(self, ref, assembler):
        """Set the assembler to use for a composite dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the dataset for which to set the assembler.
        assembler : `str`
            Fully qualified name of the assembler.
        """
        datasetTable = self._schema.metadata.tables['Dataset']
        with self._engine.begin() as connection:
            connection.execute(datasetTable.update().where(
                datasetTable.c.dataset_id == ref.id).values(assembler=assembler))
            ref._assembler = assembler

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
        datasetCompositionTable = self._schema.metadata.tables['DatasetComposition']
        with self._engine.begin() as connection:
            connection.execute(datasetCompositionTable.insert().values(component_name=name,
                                                                       parent_dataset_id=parent.id,
                                                                       component_dataset_id=component.id))
            parent._components[name] = component

    def associate(self, collection, refs):
        r"""Add existing Datasets to a Collection, possibly creating the
        Collection in the process.

        Parameters
        ----------
        collection : `str`
            Indicates the Collection the Datasets should be associated with.
        refs : `list` of `DatasetRef`
            A `list` of `DatasetRef` instances that already exist in this
            `SqlRegistry`.
        """
        datasetCollectionTable = self._schema.metadata.tables['DatasetCollection']
        with self._engine.begin() as connection:
            connection.execute(datasetCollectionTable.insert(),
                               [{'dataset_id': ref.id, 'collection': collection} for ref in refs])

    def disassociate(self, collection, refs, remove=True):
        r"""Remove existing Datasets from a Collection.

        ``collection`` and ``ref`` combinations that are not currently
        associated are silently ignored.

        Parameters
        ----------
        collection : `str`
            The Collection the Datasets should no longer be associated with.
        refs : `list` of `DatasetRef`
            A `list` of `DatasetRef` instances that already exist in this
            `SqlRegistry`.
        remove : `bool`
            If `True`, remove Datasets from the `SqlRegistry` if they are not
            associated with any Collection (including via any composites).

        Returns
        -------
        removed : `list` of `DatasetRef`
            If `remove` is `True`, the `list` of `DatasetRef`\ s that were
            removed.
        """
        if remove:
            raise NotImplementedError("Cleanup of datasets not yet implemented")
        datasetCollectionTable = self._schema.metadata.tables['DatasetCollection']
        with self._engine.begin() as connection:
            for ref in refs:
                connection.execute(datasetCollectionTable.delete().where(
                    and_(datasetCollectionTable.c.dataset_id == ref.id,
                         datasetCollectionTable.c.collection == collection)))
        return []

    def addStorageInfo(self, ref, storageInfo):
        """Add storage information for a given dataset.

        Typically used by `Datastore`.

        Parameters
        ----------
        ref : `DatasetRef`
            A reference to the dataset for which to add storage information.
        storageInfo : `StorageInfo`
            Storage information about the dataset.
        """
        datasetStorageTable = self._schema.metadata.tables['DatasetStorage']
        with self._engine.begin() as connection:
            connection.execute(datasetStorageTable.insert().values(dataset_id=ref.id,
                                                                   datastore_name=storageInfo.datastoreName,
                                                                   checksum=storageInfo.checksum,
                                                                   size=storageInfo.size))

    def updateStorageInfo(self, ref, datastoreName, storageInfo):
        """Update storage information for a given dataset.

        Typically used by `Datastore`.

        Parameters
        ----------
        ref : `DatasetRef`
            A reference to the dataset for which to add storage information.
        datastoreName : `str`
            What datastore association to update.
        storageInfo : `StorageInfo`
            Storage information about the dataset.
        """
        datasetStorageTable = self._schema.metadata.tables['DatasetStorage']
        with self._engine.begin() as connection:
            connection.execute(datasetStorageTable.update().where(and_(
                datasetStorageTable.c.dataset_id == ref.id,
                datasetStorageTable.c.datastore_name == datastoreName)).values(
                    datastore_name=storageInfo.datastoreName,
                    checksum=storageInfo.checksum,
                    size=storageInfo.size))

    def getStorageInfo(self, ref, datastoreName):
        """Retrieve storage information for a given dataset.

        Typically used by `Datastore`.

        Parameters
        ----------
        ref : `DatasetRef`
            A reference to the dataset for which to add storage information.
        datastoreName : `str`
            What datastore association to update.

        Returns
        -------
        info : `StorageInfo`
            Storage information about the dataset.

        Raises
        ------
        KeyError
            The requested Dataset does not exist.
        """
        datasetStorageTable = self._schema.metadata.tables['DatasetStorage']
        storageInfo = None
        with self._engine.begin() as connection:
            result = connection.execute(
                select([datasetStorageTable.c.datastore_name,
                        datasetStorageTable.c.checksum,
                        datasetStorageTable.c.size]).where(
                            and_(datasetStorageTable.c.dataset_id == ref.id,
                                 datasetStorageTable.c.datastore_name == datastoreName))).fetchone()

        if result is None:
            raise KeyError("Unable to retrieve information associated with "
                           "Dataset {} in datastore {}".format(ref.id, datastoreName))

        storageInfo = StorageInfo(datastoreName=result["datastore_name"],
                                  checksum=result["checksum"],
                                  size=result["size"])
        return storageInfo

    def removeStorageInfo(self, datastoreName, ref):
        """Remove storage information associated with this dataset.

        Typically used by `Datastore` when a dataset is removed.

        Parameters
        ----------
        datastoreName : `str`
            Name of this `Datastore`.
        ref : `DatasetRef`
            A reference to the dataset for which information is to be removed.
        """
        datasetStorageTable = self._schema.metadata.tables['DatasetStorage']
        with self._engine.begin() as connection:
            connection.execute(datasetStorageTable.delete().where(
                               and_(datasetStorageTable.c.dataset_id == ref.id,
                                    datasetStorageTable.c.datastore_name == datastoreName)))

    def addExecution(self, execution):
        """Add a new `Execution` to the `SqlRegistry`.

        Parameters
        ----------
        execution : `Execution`
            Instance to add to the `SqlRegistry`.
            The given `Execution` must not already be present in the `SqlRegistry`
            (or any other), therefore its `id` attribute must be `None`.
        """
        assert execution.id is None  # Must not be preexisting
        executionTable = self._schema.metadata.tables['Execution']
        with self._engine.begin() as connection:
            result = connection.execute(executionTable.insert().values(start_time=execution.startTime,
                                                                       end_time=execution.endTime,
                                                                       host=execution.host))
            execution._id = result.inserted_primary_key[0]

    def getExecution(self, id):
        """Retrieve an Execution.

        Parameters
        ----------
        id : `int`
            The unique identifier for the Execution.
        """
        executionTable = self._schema.metadata.tables['Execution']
        with self._engine.begin() as connection:
            result = connection.execute(
                select([executionTable.c.start_time,
                        executionTable.c.end_time,
                        executionTable.c.host]).where(executionTable.c.execution_id == id)).fetchone()
        if result is not None:
            return Execution(startTime=result['start_time'],
                             endTime=result['end_time'],
                             host=result['host'],
                             id=id)
        else:
            return None

    def makeRun(self, collection):
        """Create a new `Run` in the `SqlRegistry` and return it.

        If a run with this collection already exists, return that instead.

        Parameters
        ----------
        collection : `str`
            The Collection collection used to identify all inputs and outputs
            of the `Run`.

        Returns
        -------
        run : `Run`
            A new `Run` instance.
        """
        run = Run(collection=collection)
        self.addRun(run)
        return run

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
        runTable = self._schema.metadata.tables['Run']
        with self._engine.begin() as connection:
            if connection.execute(select([exists().where(runTable.c.collection == run.collection)])).scalar():
                raise ValueError("A run already exists with this collection: {}".format(run.collection))
            # First add the Execution part
            self.addExecution(run)
            # Then the Run specific part
            connection.execute(runTable.insert().values(execution_id=run.id,
                                                        collection=run.collection,
                                                        environment_id=None,  # TODO add environment
                                                        pipeline_id=None))    # TODO add pipeline

    def getRun(self, id=None, collection=None):
        """
        Get a `Run` corresponding to it's collection or id

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
        executionTable = self._schema.metadata.tables['Execution']
        runTable = self._schema.metadata.tables['Run']
        run = None
        with self._engine.begin() as connection:
            # Retrieve by id
            if (id is not None) and (collection is None):
                result = connection.execute(select([executionTable.c.execution_id,
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
                result = connection.execute(select([executionTable.c.execution_id,
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
                run = Run(id=result['execution_id'],
                          startTime=result['start_time'],
                          endTime=result['end_time'],
                          host=result['host'],
                          collection=result['collection'],
                          environment=None,  # TODO add environment
                          pipeline=None)     # TODO add pipeline
        return run

    def addQuantum(self, quantum):
        r"""Add a new `Quantum` to the `SqlRegistry`.

        Parameters
        ----------
        quantum : `Quantum`
            Instance to add to the `SqlRegistry`.
            The given `Quantum` must not already be present in the `SqlRegistry`
            (or any other), therefore its:

            - `execution` attribute must be set to an existing `Execution`.
            - `run` attribute must be set to an existing `Run`.
            - `predictedInputs` attribute must be fully populated with
              `DatasetRef`\ s, and its.
            - `actualInputs` and `outputs` will be ignored.
        """
        quantumTable = self._schema.metadata.tables['Quantum']
        datasetConsumersTable = self._schema.metadata.tables['DatasetConsumers']
        with self._engine.begin() as connection:
            # First add the Execution part
            self.addExecution(quantum)
            # Then the Quantum specific part
            connection.execute(quantumTable.insert().values(execution_id=quantum.id,
                                                            task=quantum.task,
                                                            run_id=quantum.run.id))
            # Attach dataset consumers
            # We use itertools.chain here because quantum.predictedInputs is a
            # dict of ``name : [DatasetRef, ...]`` and we need to flatten it
            # for inserting.
            connection.execute(datasetConsumersTable.insert(),
                               [{'quantum_id': quantum.id, 'dataset_id': ref.id, 'actual': False}
                                for ref in itertools.chain.from_iterable(quantum.predictedInputs.values())])

    def getQuantum(self, id):
        """Retrieve an Quantum.

        Parameters
        ----------
        id : `int`
            The unique identifier for the Quantum.
        """
        executionTable = self._schema.metadata.tables['Execution']
        quantumTable = self._schema.metadata.tables['Quantum']
        with self._engine.begin() as connection:
            result = connection.execute(
                select([quantumTable.c.task,
                        quantumTable.c.run_id,
                        executionTable.c.start_time,
                        executionTable.c.end_time,
                        executionTable.c.host]).select_from(quantumTable.join(executionTable)).where(
                    quantumTable.c.execution_id == id)).fetchone()
        if result is not None:
            run = self.getRun(id=result['run_id'])
            quantum = Quantum(task=result['task'],
                              run=run,
                              startTime=result['start_time'],
                              endTime=result['end_time'],
                              host=result['host'],
                              id=id)
            # Add predicted and actual inputs to quantum
            datasetConsumersTable = self._schema.metadata.tables['DatasetConsumers']
            with self._engine.begin() as connection:
                for result in connection.execute(select([datasetConsumersTable.c.dataset_id,
                                                         datasetConsumersTable.c.actual]).where(
                        datasetConsumersTable.c.quantum_id == id)):
                    ref = self.getDataset(result['dataset_id'])
                    quantum.addPredictedInput(ref)
                    if result['actual']:
                        quantum._markInputUsed(ref)
            return quantum
        else:
            return None

    def markInputUsed(self, quantum, ref):
        """Record the given `DatasetRef` as an actual (not just predicted)
        input of the given `Quantum`.

        This updates both the `SqlRegistry`'s `Quantum` table and the Python
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
        ValueError
            If ``ref`` is not already in the predicted inputs list.
        KeyError
            If ``ref`` is not a predicted consumer for ``quantum``.
        """
        datasetConsumersTable = self._schema.metadata.tables['DatasetConsumers']
        with self._engine.begin() as connection:
            result = connection.execute(datasetConsumersTable.update().where(and_(
                datasetConsumersTable.c.quantum_id == quantum.id,
                datasetConsumersTable.c.dataset_id == ref.id)).values(actual=True))
            if result.rowcount != 1:
                raise KeyError("{} is not a predicted consumer for {}".format(ref, quantum))
            quantum._markInputUsed(ref)

    def addDataUnitEntry(self, dataUnitName, values):
        """Add a new `DataUnit` entry.

        dataUnitName : `str`
            Name of the `DataUnit` (e.g. ``"Camera"``).
        values : `dict`
            Dictionary of ``columnName, columnValue`` pairs.

        Raises
        ------
        ValueError
            If an entry with the primary-key defined in `values` is already
            present.
        """
        dataUnit = self._schema.dataUnits[dataUnitName]
        dataUnit.validateId(values)
        dataUnitTable = dataUnit.table
        with self._engine.begin() as connection:
            try:
                connection.execute(dataUnitTable.insert().values(**values))
            except IntegrityError as err:
                raise ValueError(str(err))  # TODO this should do an explicit validity check instead

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
            Dictionary with all `DataUnit` values, or `None` if no matching entry is found.
        """
        dataUnit = self._schema.dataUnits[dataUnitName]
        dataUnit.validateId(value)
        dataUnitTable = dataUnit.table
        primaryKeyColumns = dataUnit.primaryKeyColumns
        with self._engine.begin() as connection:
            result = connection.execute(select([dataUnitTable]).where(
                and_((primaryKeyColumns[name] == value[name] for name in primaryKeyColumns)))).fetchone()
            if result is not None:
                return dict(result.items())
            else:
                return None

    def expand(self, ref):
        """Expand a `DatasetRef`.

        Parameters
        ----------
        ref : `DatasetRef`
            The `DatasetRef` to expand.

        Returns
        -------
        ref : `DatasetRef`
            The expanded reference.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def _validateDataId(self, datasetType, dataId):
        """Check if a dataId is valid for a particular `DatasetType`.

        TODO move this function to some other place once DataUnit relations
        are implemented.

        datasetType : `DatasetType`
            The `DatasetType`.
        dataId : `dict`
            A `dict` of `DataUnit` name, value pairs that label the `DatasetRef`
            within a Collection.

        Raises
        ------
        ValueError
            If the dataId is invalid for the given datasetType.
        """
        for name in datasetType.dataUnits:
            self._schema.dataUnits[name].validateId(dataId)

    def find(self, collection, datasetType, dataId):
        """Lookup a dataset.

        This can be used to obtain a `DatasetRef` that permits the dataset to
        be read from a `Datastore`.

        Parameters
        ----------
        collection : `str`
            Identifies the Collection to search.
        datasetType : `DatasetType`
            The `DatasetType`.
        dataId : `dict`
            A `dict` of `DataUnit` name, value pairs that label the `DatasetRef`
            within a Collection.

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
        self._validateDataId(datasetType, dataId)
        datasetTable = self._schema.metadata.tables['Dataset']
        datasetCollectionTable = self._schema.metadata.tables['DatasetCollection']
        dataIdExpression = and_((self._schema.dataUnits.links[name] == dataId[name]
                                 for name in self._schema.dataUnits.getPrimaryKeyNames(
                                     datasetType.dataUnits)))
        with self._engine.begin() as connection:
            result = connection.execute(select([datasetTable.c.dataset_id]).select_from(
                datasetTable.join(datasetCollectionTable)).where(and_(
                    datasetTable.c.dataset_type_name == datasetType.name,
                    datasetCollectionTable.c.collection == collection,
                    dataIdExpression))).fetchone()
        # TODO update unit values and add Run, Quantum and assembler?
        if result is not None:
            return self.getDataset(result['dataset_id'])
        else:
            return None

    def subset(self, collection, expr, datasetTypes):
        r"""Create a new `Collection` by subsetting an existing one.

        Parameters
        ----------
        collection : `str`
            Indicates the input Collection to subset.
        expr : `str`
            An expression that limits the `DataUnit`\ s and (indirectly)
            Datasets in the subset.
        datasetTypes : `list` of `DatasetType`
            The `list` of `DatasetType`\ s whose instances should be included
            in the subset.

        Returns
        -------
        collection : `str`
            The newly created collection.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def merge(self, outputCollection, inputCollections):
        r"""Create a new Collection from a series of existing ones.

        Entries earlier in the list will be used in preference to later
        entries when both contain Datasets with the same `DatasetRef`.

        Parameters
        ----------
        outputCollection : `str`
            collection to use for the new Collection.
        inputCollections : `list` of `str`
            A `list` of Collections to combine.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def makeDataGraph(self, collections, expr, neededDatasetTypes, futureDatasetTypes):
        r"""Evaluate a filter expression and lists of `DatasetType`\ s and
        return a `QuantumGraph`.

        Parameters
        ----------
        collections : `list` of `str`
            An ordered `list` of collections indicating the Collections to
            search for Datasets.
        expr : `str`
            An expression that limits the `DataUnit`\ s and (indirectly) the
            Datasets returned.
        neededDatasetTypes : `list` of `DatasetType`
            The `list` of `DatasetType`\ s whose instances should be included
            in the graph and limit its extent.
        futureDatasetTypes : `list` of `DatasetType`
            The `list` of `DatasetType`\ s whose instances may be added to the
            graph later, which requires that their `DataUnit` types must be
            present in the graph.

        Returns
        -------
        graph : `QuantumGraph`
            A `QuantumGraph` instance with a `QuantumGraph.units` attribute
            that is not `None`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def makeProvenanceGraph(self, expr, types=None):
        """Make a `QuantumGraph` that contains the full provenance of all
        Datasets matching an expression.

        Parameters
        ----------
        expr : `str`
            An expression (SQL query that evaluates to a list of Dataset
            primary keys) that selects the Datasets.

        Returns
        -------
        graph : `QuantumGraph`
            Instance (with `units` set to `None`).
        """
        raise NotImplementedError("Must be implemented by subclass")

    def export(self, expr):
        """Export contents of the `SqlRegistry`, limited to those reachable from
        the Datasets identified by the expression `expr`, into a `TableSet`
        format such that it can be imported into a different database.

        Parameters
        ----------
        expr : `str`
            An expression (SQL query that evaluates to a list of Dataset
            primary keys) that selects the `Datasets, or a `QuantumGraph`
            that can be similarly interpreted.

        Returns
        -------
        ts : `TableSet`
            Containing all rows, from all tables in the `SqlRegistry` that
            are reachable from the selected Datasets.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def import_(self, tables, collection):
        """Import (previously exported) contents into the (possibly empty)
        `SqlRegistry`.

        Parameters
        ----------
        ts : `TableSet`
            Contains the previously exported content.
        collection : `str`
            An additional Collection collection assigned to the newly
            imported Datasets.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def transfer(self, src, expr, collection):
        r"""Transfer contents from a source `SqlRegistry`, limited to those
        reachable from the Datasets identified by the expression `expr`,
        into this `SqlRegistry` and collection them with a Collection.

        Parameters
        ----------
        src : `SqlRegistry`
            The source `SqlRegistry`.
        expr : `str`
            An expression that limits the `DataUnit`\ s and (indirectly)
            the Datasets transferred.
        collection : `str`
            An additional Collection collection assigned to the newly
            imported Datasets.
        """
        self.import_(src.export(expr), collection)
