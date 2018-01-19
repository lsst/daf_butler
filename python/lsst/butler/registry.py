#
# LSST Data Management System
#
# Copyright 2008-2017  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

from sqlalchemy import create_engine
from sqlalchemy.sql import select, and_, exists

from lsst.daf.persistence import doImport

from .schema import metadata, DatasetTypeTable, RunTable, QuantumTable, DatasetTable, DatasetCollectionsTable, DatasetConsumersTable, DatasetTypeUnitsTable
from .datasets import DatasetType, DatasetHandle, DatasetRef, DatasetLabel
from .run import Run
from .quantum import Quantum
from .units import DataUnit, DataUnitTypeSet
from .storageClass import StorageClass
from .config import Config

class RegistryConfig(Config):
    pass

class Registry:
    """Basic SQL backed registry.

    Attributes
    ----------
    id: `int`
        Unique identifier for this `Registry`.
    engine: `sqlalchemy.Enigine`
        Connects to a backend database.
    """

    def __init__(self, config):
        self.config = RegistryConfig(config)['registry']
        self.engine = create_engine(self.config['dbname'])
        self.id = self.config['id']

        metadata.create_all(self.engine)

    @staticmethod
    def fromConfig(config):
        
        cls = doImport(config['registry.cls'])
        return cls(config=config)

    def registerDatasetType(self, datasetType):
        """
        Add a new :ref:`DatasetType` to the Registry.

        Parameters
        ----------
        datasetType: `DatasetType`
            The `DatasetType` to be added.
        """
        assert isinstance(datasetType, DatasetType)

        with self.engine.begin() as connection:
            insert = DatasetTypeTable.insert().values(
                dataset_type_name = datasetType.name,
                template = datasetType.template,
                storage_class = datasetType.storageClass.name
            )
            connection.execute(insert)

            connection.execute(DatasetTypeUnitsTable.insert(), [
                               {'dataset_type_name': datasetType.name, 'unit_name': unit.__name__} for unit in datasetType.units])

    def getDatasetType(self, name):
        """Get the `DatasetType`.

        Parameters
        ----------
        name: `str`
            Name of the type.

        Returns
        -------
        type: `DatasetType`
            The `DatasetType` associated with the given name.
        """
        assert isinstance(name, str)

        with self.engine.begin() as connection:
            # DataUnits
            units = []
            for result in connection.execute(select([DatasetTypeUnitsTable]).where(DatasetTypeUnitsTable.c.dataset_type_name == name)).fetchall():
                units.append(DataUnit.getType(result[DatasetTypeUnitsTable.c.unit_name]))

            # DatasetType
            result = connection.execute(select([DatasetTypeTable]).where(
                DatasetTypeTable.c.dataset_type_name == name)).fetchone()
            if result:
                return DatasetType(
                    name = result[DatasetTypeTable.c.dataset_type_name],
                    template = result[DatasetTypeTable.c.template],
                    units = DataUnitTypeSet(units),
                    storageClass = StorageClass.subclasses[result[DatasetTypeTable.c.storage_class]]
                )
            else:
                return None

    def addDataset(self, ref, uri, components, run, producer=None):
        """Add a `Dataset` to a Collection.

        This always adds a new `Dataset`; to associate an existing `Dataset` with
        a new `Collection`, use `associate`.

        Parameters
        ----------
        ref: `DatasetRef`
            Identifies the `Dataset` and contains its `DatasetType`.
        uri: `str`
            The URI that has been associated with the `Dataset` by a `Datastore`.
        components: `dict`
            If the `Dataset` is a composite, a ``{name : URI}`` dictionary of its named
            components and storage locations.
        run: `Run`
            The `Run` instance that produced the Dataset.  Ignored if ``producer`` is passed
            (`producer.run` is then used instead).  A Run must be provided by one of the two arguments.
        producer: `Quantum`
            Unit of work that produced the Dataset.  May be ``None`` to store no
            provenance information, but if present the `Quantum` must already have
            been added to the Registry.

        Returns
        -------
        handle: `DatasetHandle`
            A newly-created `DatasetHandle` instance.

        Raises
        ------
        e: `Exception`
            If a `Dataset` with the given `DatasetRef` already exists in the given Collection.
        """
        assert isinstance(ref, DatasetRef)
        assert isinstance(uri, str)
        assert isinstance(run, Run)
        assert producer is None or isinstance(producer, Quantum)

        if self.find(run.collection, ref) is not None:
            raise ValueError("dupplicate dataset {0}".format(str(ref)))

        datasetId = DatasetRef.getNewId()

        datasetHandle = DatasetHandle(
            datasetId=DatasetRef.getNewId(),
            registryId=self.id,
            ref=ref,
            uri=uri,
            components=components,
            run=run,
        )

        unitHash = datasetHandle.type.units.invariantHash(datasetHandle.units)

        with self.engine.begin() as connection:
            insert = DatasetTable.insert().values(
                dataset_id = datasetHandle.datasetId,
                registry_id = datasetHandle.registryId,
                dataset_type_name = datasetHandle.type.name,
                unit_hash = unitHash,
                uri = datasetHandle.uri,
                run_id = datasetHandle.run.runId,
                producer_id = datasetHandle.producer.quantumId if datasetHandle.producer else None
            )
            connection.execute(insert)

            if components:
                raise NotImplementedError

        self.associate(run.collection, datasetHandle)

        return datasetHandle

    def associate(self, collection, handles):
        """Add existing `Dataset`s to a Collection, possibly creating the Collection in the process.

        Parameters
        ----------
        collection: `str`
            Indicates the Collection the `Dataset`s should be associated with.
        handles: `[DatasetHandle]`
            A `list` of `DatasetHandle` instances that already exist in this `Registry`.
        """
        if isinstance(handles, DatasetHandle):
            handles = (handles, )
        with self.engine.begin() as connection:
            connection.execute(DatasetCollectionsTable.insert(),
                               [{'collection': collection, 'dataset_id': handle.datasetId, 'registry_id': handle.registryId}
                                   for handle in handles]
                               )

    def disassociate(self, collection, handles, remove=True):
        """Remove existing `Dataset`s from a Collection.

        ``collection`` and ``handle`` combinations that are not currently associated are silently ignored.

        Parameters
        ----------
        collection: `str`
            The Collection the `Dataset`s should no longer be associated with.
        handles: `[DatasetHandle]`
            A `list` of `DatasetHandle` instances that already exist in this `Registry`.
        remove: `bool`
            If `True`, remove `Dataset`s from the `Registry` if they are not associated with
            any Collection (including via any composites).

        Returns
        -------
        removed: `[DatasetHandle]`
            If `remove` is `True`, the `list` of `DatasetHandle`s that were removed.
        """
        deletedDatasets = []
        with self.engine.begin() as connection:
            for handle in handles:
                connection.execute(DatasetCollectionsTable.delete().where(and_(
                    DatasetCollectionsTable.c.collection == collection,
                    DatasetCollectionsTable.c.dataset_id == handle.datasetId,
                    DatasetCollectionsTable.c.registry_id == handle.registryId
                )))

                if remove:
                    if not connection.execute(select([exists().where(and_(
                            DatasetCollectionsTable.c.dataset_id == handle.datasetId,
                            DatasetCollectionsTable.c.registry_id == handle.registryId
                        ))])).scalar():

                        connection.execute(DatasetTable.delete().where(and_(
                            DatasetTable.c.dataset_id == handle.datasetId,
                            DatasetTable.c.registry_id == handle.registryId
                        )))

                        deletedDatasets.append(handle)
                    else:
                        # Dataset is still in use
                        pass
        return deletedDatasets

    def makeRun(self, collection):
        """Create a new `Run` in the `Registry` and return it.

        Parameters
        ----------
        collection: `str`
            The Collection collection used to identify all inputs and outputs of the `Run`.

        Returns
        -------
        run: `Run`
            A new `Run` instance.
        """
        run = Run(runId=Run.getNewId(), registryId=self.id, collection=collection, environment=None, pipeline=None)

        with self.engine.begin() as connection:
            insert = RunTable.insert().values(
                run_id=run.runId,
                registry_id=run.registryId,
                collection=run.collection,
                environment_id=run.environment,
                pipeline_id=run.pipeline
            )
            connection.execute(insert)

        return run

    def updateRun(self, run):
        """Update the `environment` and/or `pipeline` of the given `Run` in the database,
        given the `DatasetHandle` attributes of the input `Run`.

        Parameters
        ----------
        run: `Run`
            The `Run` to update with the new values filled in.
        """
        with self.engine.begin() as connection:
            # TODO: should it also update the collection?
            connection.execute(RunTable.update().where(and_(RunTable.c.run_id == run.runId, RunTable.c.registry_id == run.registryId)).values(
                environment_id = run.environment, pipeline_id = run.pipeline))

    def getRun(self, collection=None, id=None):
        """
        Get a :ref:`Run` corresponding to it's collection or id

        Parameters
        ----------
        collection : `str`
            Collection collection
        id : `int`, optional
            If given, lookup by id instead and ignore `collection`.
        """
        if collection is None and id is None:
            raise ValueError("Either collection or id needs to be given")
        with self.engine.begin() as connection:
            if id is not None:
                result = connection.execute(RunTable.select().where(
                    and_(RunTable.c.run_id == id, RunTable.c.registry_id == self.id))).fetchone()
            else:
                result = connection.execute(RunTable.select().where(
                    and_(RunTable.c.collection == collection, RunTable.c.registry_id == self.id))).fetchone()

            if result:
                runId = result[RunTable.c.run_id]
                registryId = result[RunTable.c.registry_id]
                collection = result[RunTable.c.collection]
                environment = result[RunTable.c.environment_id]
                pipeline = result[RunTable.c.pipeline_id]

                return Run(runId, self.id, collection, environment, pipeline)
            else:
                return None

    def addQuantum(self, quantum):
        """Add a new `Quantum` to the `Registry`.

        Parameters
        ----------
        quantum: `Quantum`
            Instance to add to the `Registry`.
            The given `Quantum` must not already be present in the `Registry` (or any other), therefore its:
            - `pkey` attribute must be `None`.
            - `predictedInputs` attribute must be fully populated with `DatasetHandle`s, and its.
            - `actualInputs` and `outputs` will be ignored.
        """
        assert isinstance(quantum, Quantum)
        assert quantum.pkey is None

        quantum._quantumId = Quantum.getNewId()
        quantum._registryId = self.id

        with self.engine.begin() as connection:
            insert = QuantumTable.insert().values(
                quantum_id = quantum.quantumId,
                registry_id = quantum.registryId,
                run_id = quantum.run.runId,
                task = quantum.task
            )
            connection.execute(insert)

            for inputs in quantum.predictedInputs.values():
                connection.execute(DatasetConsumersTable.insert(), [
                    {'quantum_id': quantum.quantumId,
                     'quantum_registry_id': quantum.registryId,
                     'dataset_id': handle.datasetId,
                     'dataset_registry_id': handle.registryId,
                     'actual': False} for handle in inputs])

    def markInputUsed(self, quantum, handle):
        """Record the given `DatasetHandle` as an actual (not just predicted) input of the given `Quantum`.

        This updates both the `Registry`'s `Quantum` table and the Python `Quantum.actualInputs` attribute.

        Parameters
        ----------
        quantum: `Quantum`
            Producer to update.
            Will be updated in this call.
        handle: `DatasetHandle`
            To set as actually used input.

        Raises
        ------
        e: `Exception`
            If `handle` is not already in the predicted inputs list.
        """
        assert isinstance(quantum, Quantum)
        assert isinstance(handle, DatasetHandle)

        with self.engine.begin() as connection:
            update = DatasetConsumersTable.update().where(and_(
                DatasetConsumersTable.c.quantum_id == quantum.quantumId,
                DatasetConsumersTable.c.quantum_registry_id == quantum.registryId,
                DatasetConsumersTable.c.dataset_id == handle.datasetId,
                DatasetConsumersTable.c.dataset_registry_id == handle.registryId
            )).values(actual=True)
            connection.execute(update)

    def addDataUnit(self, unit, replace=False):
        """Add a new `DataUnit`, optionally replacing an existing one (for updates).

        unit: `DataUnit`
            The `DataUnit` to add or replace.
        replace: `bool`
            If `True`, replace any matching `DataUnit` that already exists
            (updating its non-unique fields) instead of raising an exception.
        """
        assert isinstance(unit, DataUnit)

        if replace:
            raise NotImplementedError

        with self.engine.begin() as connection:
            unit.insert(connection)

    def findDataUnit(self, cls, values):
        """Return a `DataUnit` given a dictionary of values.

        Parameters
        ----------
        cls: `type`
            A class that inherits from `DataUnit`.
        values: `dict`
            A dictionary of values that uniquely identify the `DataUnit`.

        Returns
        -------
        unit: `DataUnit`
            Instance of type `cls`, or `None` if no matching unit is found.

        See Also
        --------
        `DataUnitMap.findDataUnit` : Find a `DataUnit` in a `DataUnitTypeMap`.
        """
        with self.engine.begin() as connection:
            return cls.find(values, connection)

    def expand(self, label):
        """Expand a `DatasetLabel`, returning an equivalent `DatasetRef`.

        Is a simple pass-through if `label` is already a `DatasetRef`.

        Parameters
        ----------
        label: `DatasetLabel`
            The `DatasetLabel` to expand.

        Returns
        -------
        ref: `DatasetRef`
            The label expanded as a reference.
        """
        assert isinstance(label, DatasetLabel)
        if isinstance(label, DatasetRef):
            return label

        datasetType = self.getDatasetType(label.name)
        dataUnits = datasetType.units.expand(self.findDataUnit, label.units)

        return DatasetRef(datasetType, dataUnits)

    def find(self, collection, label):
        """Look up the location of the `Dataset` associated with the given `DatasetLabel`.

        This can be used to obtain the URI that permits the `Dataset` to be read from a `Datastore`.
        Is a simple pass-through if `label` is already a `DatasetHandle`.

        Parameters
        ----------
        collection: `str`
            Identifies the Collection to search.
        label: `DatasetLabel`
            Identifies the `Dataset`.

        Returns
        -------
        handle: `DatasetHandle`
            A handle to the `Dataset`, or `None` if no matching `Dataset` was found.
        """
        datasetRef = self.expand(label)

        unitHash = datasetRef.type.units.invariantHash(datasetRef.units)

        with self.engine.begin() as connection:
            s = select([DatasetTable]).select_from(DatasetTable.join(DatasetCollectionsTable)).where(
                and_(DatasetTable.c.unit_hash == unitHash, DatasetCollectionsTable.c.collection == collection))
            result = connection.execute(s).fetchall()

            if len(result) == 1:
                row = result[0]
                datasetId = row[DatasetTable.c.dataset_id]
                registryId = row[DatasetTable.c.registry_id]
                uri = row[DatasetTable.c.uri]
                runId = row[DatasetTable.c.run_id]
                components = None
                run = self.getRun(id=runId)
                return DatasetHandle(datasetId, registryId, datasetRef, uri, components, run)
            elif len(result) == 0:
                return None
            else:
                raise NotImplementedError("Cannot handle collisions")

    def subset(self, collection, expr, datasetTypes):
        """Create a new `Collection` by subsetting an existing one.

        Parameters
        ----------
        collection: `str`
            Indicates the input Collection to subset.
        expr: `str`
            An expression that limits the `DataUnit`s and (indirectly) `Dataset`s in the subset.
        datasetTypes: `[DatasetType]`
            The `list` of `DatasetType`s whose instances should be included in the subset.

        Returns
        -------
        collection: `str`
            The newly created collection.
        """
        raise NotImplementedError

    def merge(self, outputCollection, inputCollections):
        """Create a new Collection from a series of existing ones.

        Entries earlier in the list will be used in preference to later entries when both contain
        `Dataset`s with the same `DatasetRef`.

        Parameters
        ----------
        outputCollection: `str`
            collection to use for the new Collection.
        inputCollections: `[str]`
            A `list` of Collections to combine.
        """
        raise NotImplementedError

    def makeDataGraph(self, collections, expr, neededDatasetTypes, futureDatasetTypes):
        """Evaluate a filter expression and lists of `DatasetType`s and return a `QuantumGraph`.

        Parameters
        ----------
        collections: `[str]`
            An ordered `list` of collections indicating the Collections to search for `Dataset`s.
        expr: `str`
            An expression that limits the `DataUnit`s and (indirectly) the `Dataset`s returned.
        neededDatasetTypes: `[DatasetType]`
            The `list` of `DatasetType`s whose instances should be included in the graph and limit its extent.
        futureDatasetTypes: `[DatasetType]`
            The `list` of `DatasetType`s whose instances may be added to the graph later,
            which requires that their `DataUnit` types must be present in the graph.

        Returns
        -------
        graph: `QuantumGraph`
            A `QuantumGraph` instance with a `QuantumGraph.units` attribute that is not `None`.
        """
        raise NotImplementedError

    def makeProvenanceGraph(self, expr, types=None):
        """Make a `QuantumGraph` that contains the full provenance of all `Dataset`s matching an expression.

        Parameters
        ----------
        expr: `str`
            An expression (SQL query that evaluates to a list of `Dataset` primary keys) that selects the `Dataset`s.

        Returns
        -------
        graph: `QuantumGraph`
            Instance (with `units` set to `None`).
        """
        raise NotImplementedError

    def export(self, expr):
        """Export contents of the `Registry`, limited to those reachable from the `Dataset`s identified
        by the expression `expr`, into a `TableSet` format such that it can be imported into a different database.

        Parameters
        ----------
        expr: `str`
            An expression (SQL query that evaluates to a list of `Dataset` primary keys) that selects the `Datasets,
            or a `QuantumGraph` that can be similarly interpreted.

        Returns
        -------
        ts: `TableSet`
            Containing all rows, from all tables in the `Registry` that are reachable from
            the selected `Dataset`s.
        """
        raise NotImplementedError

    def import_(self, tables, collection):
        """Import (previously exported) contents into the (possibly empty) `Registry`.

        Parameters
        ----------
        ts: `TableSet`
            Contains the previously exported content.
        collection: `str`
            An additional Collection collection assigned to the newly imported `Dataset`s.
        """
        raise NotImplementedError

    def transfer(self, src, expr, collection):
        """Transfer contents from a source `Registry`, limited to those reachable from the `Dataset`s
        identified by the expression `expr`, into this `Registry` and collection them with a Collection.

        Parameters
        ----------
        src: `Registry`
            The source `Registry`.
        expr: `str`
            An expression that limits the `DataUnit`s and (indirectly) the `Dataset`s transferred.
        collection: `str`
            An additional Collection collection assigned to the newly imported `Dataset`s.
        """
        self.import_(src.export(expr), collection)
