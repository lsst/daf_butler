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

from .schema import metadata, DatasetTypeTable, RunTable, QuantumTable, DatasetTable, DatasetCollectionsTable, DatasetConsumersTable, DatasetTypeUnitsTable
from .datasets import DatasetType, DatasetHandle, DatasetRef, DatasetLabel
from .run import Run
from .quantum import Quantum
from .units import DataUnit, DataUnitTypeSet
from .storageClass import StorageClass


class Registry:
    """Basic SQL backed registry.
    """

    def __init__(self, dbname='sqlite:///:memory:', id=0):
        self.engine = create_engine(dbname)
        self.id = id

        metadata.create_all(self.engine)

    def registerDatasetType(self, datasetType):
        """
        Add a new :ref:`DatasetType` to the Registry.

        :param DatasetType datasetType: the :ref:`DatasetType` to be added

        :return: None
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
        """
        Return the :py:class:`DatasetType` associated with the given name.
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
        """
        Add a :ref:`Dataset` to a :ref:`Collection`.

        This always adds a new :ref:`Dataset`; to associate an existing :ref:`Dataset` with a new :ref:`Collection`, use :py:meth:`associate`.

        The :ref:`Quantum` that generated the :ref:`Dataset` can optionally be provided to add provenance information.

        :param ref: a :ref:`DatasetRef` that identifies the :ref:`Dataset` and contains its :ref:`DatasetType`.

        :param str uri: the :ref:`URI` that has been associated with the :ref:`Dataset` by a :ref:`Datastore`.

        :param dict components: if the :ref:`Dataset` is a composite, a ``{name : URI}`` dictionary of its named components and storage locations.

        :param Run run: the :ref:`Run` instance that produced the Dataset.  Ignored if ``producer`` is passed (:py:attr:`producer.run <Quantum.run>` is then used instead).  A Run must be provided by one of the two arguments.

        :param Quantum producer: the Quantum instance that produced the Dataset.  May be ``None`` to store no provenance information, but if present the :py:class:`Quantum` must already have been added to the Registry.

        :return: a newly-created :py:class:`DatasetHandle` instance.

        :raises: an exception if a :ref:`Dataset` with the given :ref:`DatasetRef` already exists in the given :ref:`Collection`.
        """
        assert isinstance(ref, DatasetRef)
        assert isinstance(uri, str)
        assert isinstance(run, Run)
        assert producer is None or isinstance(producer, Quantum)

        if self.find(run.tag, ref) is not None:
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

        self.associate(run.tag, datasetHandle)

        return datasetHandle

    def associate(self, tag, handles):
        """
        Add existing :ref:`Datasets <Dataset>` to a :ref:`Collection`, possibly creating the :ref:`Collection` in the process.

        :param str tag: a :ref:`CollectionTag <Collection>` indicating the Collection the :ref:`Datasets <Dataset>` should be associated with.

        :param list[DatasetHandle] handles: a list of :py:class:`DatasetHandle` instances that already exist in this :ref:`Registry`.

        :return: None
        """
        if isinstance(handles, DatasetHandle):
            handles = (handles, )
        with self.engine.begin() as connection:
            connection.execute(DatasetCollectionsTable.insert(),
                               [{'tag': tag, 'dataset_id': handle.datasetId, 'registry_id': handle.registryId}
                                   for handle in handles]
                               )

    def disassociate(self, tag, handles, remove=True):
        """
        Remove existing :ref:`Datasets <Dataset>` from a :ref:`Collection`.

        :param str tag: a :ref:`CollectionTag <Collection>` indicating the Collection the :ref:`Datasets <Dataset>` should no longer be associated with.

        :param list[DatasetHandle] handles: a list of :py:class:`DatasetHandle` instances that already exist in this :ref:`Registry`.

        :param bool remove: if True, remove Datasets from the Registry if they are not associated with any :ref:`Collection` (including via any composites).

        :returns: If ``remove`` is True, the list of :py:class:`DatasetHandles <DatasetHandle>` that were removed.

        ``tag`` and ``handle`` combinations that are not currently associated are silently ignored.
        """
        deletedDatasets = []
        with self.engine.begin() as connection:
            for handle in handles:
                connection.execute(DatasetCollectionsTable.delete().where(and_(
                    DatasetCollectionsTable.c.tag == tag,
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

    def makeRun(self, tag):
        """
        Create a new :ref:`Run` in the :ref:`Registry` and return it.

        :param str tag: the :ref:`CollectionTag <Collection>` used to identify all inputs and outputs of the :ref:`Run`.

        :returns: a :py:class:`Run` instance.
        """
        run = Run(runId=Run.getNewId(), registryId=self.id, tag=tag, environmentId=None, pipelineId=None)

        with self.engine.begin() as connection:
            insert = RunTable.insert().values(
                run_id=run.runId,
                registry_id=run.registryId,
                tag=run.tag,
                environment_id=run.environmentId,
                pipeline_id=run.pipelineId
            )
            connection.execute(insert)

        return run

    def updateRun(self, run):
        """
        Update the ``environment`` and/or ``pipeline`` of the given Run in the database, given the :py:class:`DatasetHandles <DatasetHandle>` attributes of the given :py:class:`Run`.
        """
        with self.engine.begin() as connection:
            # TODO: should it also update the tag?
            connection.execute(RunTable.update().where(and_(RunTable.c.run_id == run.runId, RunTable.c.registry_id == run.registryId)).values(
                environment_id = run.environmentId, pipeline_id = run.pipelineId))

    def getRun(self, pkey):
        """
        Get a :ref:`Run` corresponding to it's primary key
        """
        runId, registryId = pkey
        with self.engine.begin() as connection:
            result = connection.execute(RunTable.select().where(
                and_(RunTable.c.run_id == runId, RunTable.c.registry_id == registryId))).fetchone()

            if result:
                runId = result[RunTable.c.run_id]
                registryId = result[RunTable.c.registry_id]
                tag = result[RunTable.c.tag]
                environmentId = result[RunTable.c.environment_id]
                pipelineId = result[RunTable.c.pipeline_id]

                return Run(runId, registryId, tag, environmentId, pipelineId)
            else:
                return None

    def addQuantum(self, quantum):
        """
        Add a new :ref:`Quantum` to the :ref:`Registry`.

        :param Quantum quantum: a :py:class:`Quantum` instance to add to the :ref:`Registry`.

        The given Quantum must not already be present in the Registry (or any other); its :py:attr:`pkey <Quantum.pkey>` attribute must be ``None``.

        The :py:attr:`predictedInputs <Quantum.predictedInputs>` attribute must be fully populated with :py:class:`DatasetHandles <DatasetHandle>`.
        The :py:attr:`actualInputs <Quantum.actualInputs>` and :py:attr:`outputs <Quantum.outputs>` will be ignored.
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
        """
        Record the given :py:class:`DatasetHandle` as an actual (not just predicted) input of the given :ref:`Quantum`.

        This updates both the Registry's :ref:`Quantum <sql_Quantum>` table and the Python :py:attr:`Quantum.actualInputs` attribute.

        Raises an exception if ``handle`` is not already in the predicted inputs list.
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
        """
        Add a new :ref:`DataUnit`, optionally replacing an existing one (for updates).

        :param DataUnit unit: the :py:class:`DataUnit` to add or replace.

        :param bool replace: if True, replace any matching :ref:`DataUnit` that already exists (updating its non-unique fields) instead of raising an exception.
        """
        assert isinstance(unit, DataUnit)

        if replace:
            raise NotImplementedError

        with self.engine.begin() as connection:
            unit.insert(connection)

    def findDataUnit(self, cls, values):
        """
        Return a :ref:`DataUnit` given a dictionary of values.

        :param type cls: a class that inherits from :py:class:`DataUnit`.

        :param dict values: a dictionary of values that uniquely identify the :ref:`DataUnit`.

        :returns: a :py:class:`DataUnit` instance of type ``cls``, or ``None`` if no matching unit is found.

        See also :py:meth:`DataUnitMap.findDataUnit`.
        """
        with self.engine.begin() as connection:
            return cls.find(values, connection)

    def expand(self, label):
        """
        Expand a :py:class:`DatasetLabel`, returning an equivalent :py:class:`DatasetRef`.

        Must be a simple pass-through if ``label`` is already a :ref:`DatasetRef`.

        *For limited Registries,* ``label`` *must be a* :py:class:`DatasetRef` *, making this a guaranteed no-op (but still callable, for interface compatibility).*
        """
        assert isinstance(label, DatasetLabel)
        if isinstance(label, DatasetRef):
            return label

        datasetType = self.getDatasetType(label.name)
        dataUnits = datasetType.units.expand(self.findDataUnit, label.units)

        return DatasetRef(datasetType, dataUnits)

    def find(self, tag, label):
        """
        Look up the location of the :ref:`Dataset` associated with the given :py:class:`DatasetLabel`.

        This can be used to obtain the :ref:`URI` that permits the :ref:`Dataset` to be read from a :ref:`Datastore`.

        Must be a simple pass-through if ``label`` is already a :py:class:`DatasetHandle`.

        :param str tag: a :ref:`CollectionTag <Collection>` indicating the :ref:`Collection` to search.

        :param DatasetLabel label: a :py:class:`DatasetLabel` that identifies the :ref:`Dataset`.  *For limited Registries, must be a* :py:class:`DatasetRef`.

        :returns: a :py:class:`DatasetHandle` instance
        """
        datasetRef = self.expand(label)

        unitHash = datasetRef.type.units.invariantHash(datasetRef.units)

        with self.engine.begin() as connection:
            s = select([DatasetTable]).select_from(DatasetTable.join(DatasetCollectionsTable)).where(
                and_(DatasetTable.c.unit_hash == unitHash, DatasetCollectionsTable.c.tag == tag))
            result = connection.execute(s).fetchall()

            if len(result) == 1:
                row = result[0]
                datasetId = row[DatasetTable.c.dataset_id]
                registryId = row[DatasetTable.c.registry_id]
                uri = row[DatasetTable.c.uri]
                runId = row[DatasetTable.c.run_id]
                components = {}
                run = self.getRun((runId, registryId))
                return DatasetHandle(datasetId, registryId, datasetRef, uri, components, run)
            elif len(result) == 0:
                return None
            else:
                raise NotImplementedError("Cannot handle collisions")

    def subset(self, tag, expr, datasetTypes):
        """
        Create a new :ref:`Collection` by subsetting an existing one.

        :param str tag: a :ref:`CollectionTag <Collection>` indicating the input :ref:`Collection` to subset.

        :param str expr: an expression that limits the :ref:`DataUnits <DataUnit>` and (indirectly) the :ref:`Datasets <Dataset>` in the subset.

        :param list[DatasetType] datasetTypes: the list of :ref:`DatasetTypes <DatasetType>` whose instances should be included in the subset.

        :returns: a str :ref:`CollectionTag <Collection>`
        """
        raise NotImplementedError

    def merge(self, outputTag, inputTags):
        """
        Create a new :ref:`Collection` from a series of existing ones.

        Entries earlier in the list will be used in preference to later entries when both contain :ref:`Datasets <Dataset>` with the same :ref:`DatasetRef`.

        :param outputTag: a str :ref:`CollectionTag <Collection>` to use for the new :ref:`Collection`.

        :param list[str] inputTags: a list of :ref:`CollectionTags <Collection>` to combine.
        """
        raise NotImplementedError

    def makeDataGraph(self, tags, expr, neededDatasetTypes, futureDatasetTypes):
        """
        Evaluate a filter expression and lists of :ref:`DatasetTypes <DatasetType>` and return a :ref:`QuantumGraph`.

        :param list[str] tags: an ordered list of tags indicating the :ref:`Collections <Collection>` to search for :ref:`Datasets <Dataset>`.

        :param str expr: an expression that limits the :ref:`DataUnits <DataUnit>` and (indirectly) the :ref:`Datasets <Dataset>` returned.

        :param list[DatasetType] neededDatasetTypes: the list of :ref:`DatasetTypes <DatasetType>` whose instances should be included in the graph and limit its extent.

        :param list[DatasetType] futureDatasetTypes: the list of :ref:`DatasetTypes <DatasetType>` whose instances may be added to the graph later, which requires that their :ref:`DataUnit` types must be present in the graph.

        :returns: a :ref:`QuantumGraph` instance with a :py:attr:`QuantumGraph.units` attribute that is not ``None``.
        """
        raise NotImplementedError

    def makeProvenanceGraph(self, expr, types=None):
        """
        Return a :ref:`QuantumGraph` that contains the full provenance of all :ref:`Datasets <Dataset>` matching an expression.

        :param str expr: an expression (SQL query that evaluates to a list of ``datasetId``) that selects the :ref:`Datasets <Dataset>`.

        :return: a :py:class:`QuantumGraph` instance (with :py:attr:`units <QuantumGraph.units>` set to None).
        """
        raise NotImplementedError

    def export(self, expr):
        """
        Export contents of the :ref:`Registry`, limited to those reachable from the :ref:`Datasets <Dataset>` identified
        by the expression ``expr``, into a :ref:`TableSet` format such that it can be imported into a different database.

        :param str expr: an expression (SQL query that evaluates to a list of ``datasetId``) that selects the :ref:`Datasets <Dataset>`, or a :ref:`QuantumGraph` that can be similarly interpreted.

        :returns: a :ref:`TableSet` containing all rows, from all tables in the :ref:`Registry` that are reachable from the selected :ref:`Datasets <Dataset>`.
        """
        raise NotImplementedError

    def import_(self, tables, tag):
        """
        Import (previously exported) contents into the (possibly empty) :ref:`Registry`.

        :param TableSet tables: a :ref:`TableSet` containing the exported content.

        :param str tag: an additional CollectionTag assigned to the newly imported :ref:`Datasets <Dataset>`.
        """
        raise NotImplementedError

    def transfer(self, src, expr, tag):
        """
        Transfer contents from a source :ref:`Registry`, limited to those reachable from the :ref:`Datasets <Dataset>` identified
        by the expression ``expr``, into this :ref:`Registry` and tag them with a :ref:`Collection`.

        :param Registry src: the source :ref:`Registry`.

        :param str expr: an expression that limits the :ref:`DataUnits <DataUnit>` and (indirectly) the :ref:`Datasets <Dataset>` transferred.

        :param str tag: an additional CollectionTag assigned to the newly imported :ref:`Datasets <Dataset>`.
        """
        self.import_(src.export(expr), tag)
