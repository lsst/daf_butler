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

from sqlalchemy import create_engine
from sqlalchemy.sql import select

from ..core.datasets import DatasetType, DatasetRef
from ..core.registry import RegistryConfig, Registry
from ..core.schema import Schema
from ..core.run import Run

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
        """
        if not self._isValidDatasetType(datasetType):
            raise ValueError("DatasetType is not valid for this registry")
        if datasetType.name in self._datasetTypes:
            raise KeyError("DatasetType: {} already registered".format(datasetType.name))
        datasetTypeTable = self._schema.metadata.tables['DatasetType']
        datasetTypeUnitsTable = self._schema.metadata.tables['DatasetTypeUnits']
        with self._engine.begin() as connection:
            connection.execute(datasetTypeTable.insert().values(dataset_type_name=datasetType.name,
                                                                storage_class=datasetType.storageClass))
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
                storageClass = result['storage_class']
                # Get DataUnits (if any) from DatasetTypeUnits table
                result = connection.execute(select([datasetTypeUnitsTable.c.unit_name]).where(
                    datasetTypeUnitsTable.c.dataset_type_name == name)).fetchall()
                dataUnits = (r[0] for r in result) if result else ()
                datasetType = DatasetType(name=name,
                                          storageClass=storageClass,
                                          dataUnits=dataUnits)
        return datasetType

    def addDataset(self, datasetType, dataId, run, producer=None):
        """Add a `Dataset` to a Collection.

        This always adds a new `Dataset`; to associate an existing `Dataset` with
        a new `Collection`, use `associate`.

        Parameters
        ----------
        datasetType : `str`
            Name of a `DatasetType`.
        dataId : `dict`
            An identifier with `DataUnit` names and values.
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
        `DatasetRef`
            A newly-created `DatasetRef` instance.

        Raises
        ------
        Exception
            If a `Dataset` with the given `DatasetRef` already exists in the
            given Collection.
        """
        datasetTable = self._schema.metadata.tables['Dataset']
        datasetRef = None
        with self._engine.begin() as connection:
            result = connection.execute(datasetTable.insert().values(dataset_type_name=datasetType.name,
                                                                     run_id=run.execution,
                                                                     quantum_id=None))  # TODO add producer
            datasetRef = DatasetRef(datasetType, dataId, result.inserted_primary_key[0])
        return datasetRef

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
                datasetTable.c.dataset_id==ref.id).values(assembler=assembler))
            ref._assembler = assembler

    def attachComponent(self, name, parent, component):
        """Attach a component to a dataset.

        Parameters
        ----------
        name : `str`
            Name of the component.
        parent : `DatasetRef`
            A reference to the parent dataset.
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
        """Add existing `Dataset`\ s to a Collection, possibly creating the
        Collection in the process.

        Parameters
        ----------
        collection : `str`
            Indicates the Collection the `Dataset`\ s should be associated with.
        refs : `list` of `DatasetRef`
            A `list` of `DatasetRef` instances that already exist in this
            `SqlRegistry`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def disassociate(self, collection, refs, remove=True):
        """Remove existing `Dataset`\ s from a Collection.

        ``collection`` and ``ref`` combinations that are not currently
        associated are silently ignored.

        Parameters
        ----------
        collection : `str`
            The Collection the `Dataset`\ s should no longer be associated with.
        refs : `list` of `DatasetRef`
            A `list` of `DatasetRef` instances that already exist in this
            `SqlRegistry`.
        remove : `bool`
            If `True`, remove `Dataset`\ s from the `SqlRegistry` if they are not
            associated with any Collection (including via any composites).

        Returns
        -------
        removed : `list` of `DatasetRef`
            If `remove` is `True`, the `list` of `DatasetRef`\ s that were
            removed.
        """
        raise NotImplementedError("Must be implemented by subclass")

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
        # First see if a run with this collection already exists
        run = self.getRun(collection)
        if run is None:
            execution = None  # Automatically generate one
            environment = None
            pipeline = None
            runTable = self._schema.metadata.tables['Run']
            with self._engine.begin() as connection:
                connection.execute(runTable.insert().values(execution_id=execution,
                                                            collection=collection,
                                                            environment_id=environment,
                                                            pipeline_id=pipeline))
            run = self.getRun(collection)  # Needed because execution_id is autoincrement
        return run

    def updateRun(self, run):
        """Update the `environment` and/or `pipeline` of the given `Run`
        in the database, given the `DatasetRef` attributes of the input
        `Run`.

        Parameters
        ----------
        run : `Run`
            The `Run` to update with the new values filled in.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def getRun(self, collection=None, id=None):
        """
        Get a `Run` corresponding to it's collection or id

        Parameters
        ----------
        collection : `str`
            Collection collection
        id : `int`, optional
            If given, lookup by id instead and ignore `collection`.
        """
        runTable = self._schema.metadata.tables['Run']
        run = None
        with self._engine.begin() as connection:
            # Retrieve by id
            if (id is not None) and (collection is None):
                result = connection.execute(select([runTable.c.execution_id,
                                                    runTable.c.collection,
                                                    runTable.c.environment_id,
                                                    runTable.c.pipeline_id]).where(
                                                        runTable.c.execution_id == id)).fetchone()
            # Retrieve by collection
            elif (collection is not None) and (id is None):
                result = connection.execute(select([runTable.c.execution_id,
                                                    runTable.c.collection,
                                                    runTable.c.environment_id,
                                                    runTable.c.pipeline_id]).where(
                                                        runTable.c.collection == collection)).fetchone()
            else:
                raise ValueError("Either collection or id must be given")
            if result is not None:
                run = Run(execution=result['execution_id'],
                          collection=result['collection'],
                          environment=result['environment_id'],
                          pipeline=result['pipeline_id'])
        return run

    def addQuantum(self, quantum):
        """Add a new `Quantum` to the `SqlRegistry`.

        Parameters
        ----------
        quantum : `Quantum`
            Instance to add to the `SqlRegistry`.
            The given `Quantum` must not already be present in the `SqlRegistry`
            (or any other), therefore its:

            - `pkey` attribute must be `None`.
            - `predictedInputs` attribute must be fully populated with
              `DatasetRef`\ s, and its.
            - `actualInputs` and `outputs` will be ignored.
        """
        raise NotImplementedError("Must be implemented by subclass")

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
        Exception
            If `ref` is not already in the predicted inputs list.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def addDataUnit(self, unit, replace=False):
        """Add a new `DataUnit`, optionally replacing an existing one
        (for updates).

        unit : `DataUnit`
            The `DataUnit` to add or replace.
        replace : `bool`
            If `True`, replace any matching `DataUnit` that already exists
            (updating its non-unique fields) instead of raising an exception.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def findDataUnit(self, cls, values):
        """Return a `DataUnit` given a dictionary of values.

        Parameters
        ----------
        cls : `type`
            A class that inherits from `DataUnit`.
        values : `dict`
            A dictionary of values that uniquely identify the `DataUnit`.

        Returns
        -------
        unit : `DataUnit`
            Instance of type `cls`, or `None` if no matching unit is found.
        """
        raise NotImplementedError("Must be implemented by subclass")

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

    def find(self, collection, ref):
        """Look up the location of the `Dataset` associated with the given
        `DatasetRef`.

        This can be used to obtain the URI that permits the `Dataset` to be
        read from a `Datastore`.

        Parameters
        ----------
        collection : `str`
            Identifies the Collection to search.
        ref : `DatasetRef`
            Identifies the `Dataset`.

        Returns
        -------
        ref : `DatasetRef`
            A ref to the `Dataset`, or `None` if no matching `Dataset`
            was found.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def subset(self, collection, expr, datasetTypes):
        """Create a new `Collection` by subsetting an existing one.

        Parameters
        ----------
        collection : `str`
            Indicates the input Collection to subset.
        expr : `str`
            An expression that limits the `DataUnit`\ s and (indirectly)
            `Dataset`\ s in the subset.
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
        """Create a new Collection from a series of existing ones.

        Entries earlier in the list will be used in preference to later
        entries when both contain
        `Dataset`\ s with the same `DatasetRef`.

        Parameters
        ----------
        outputCollection : `str`
            collection to use for the new Collection.
        inputCollections : `list` of `str`
            A `list` of Collections to combine.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def makeDataGraph(self, collections, expr, neededDatasetTypes, futureDatasetTypes):
        """Evaluate a filter expression and lists of `DatasetType`\ s and
        return a `QuantumGraph`.

        Parameters
        ----------
        collections : `list` of `str`
            An ordered `list` of collections indicating the Collections to
            search for `Dataset`\ s.
        expr : `str`
            An expression that limits the `DataUnit`\ s and (indirectly) the
            `Dataset`\ s returned.
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
        `Dataset`\ s matching an expression.

        Parameters
        ----------
        expr : `str`
            An expression (SQL query that evaluates to a list of `Dataset`
            primary keys) that selects the `Dataset`\ s.

        Returns
        -------
        graph : `QuantumGraph`
            Instance (with `units` set to `None`).
        """
        raise NotImplementedError("Must be implemented by subclass")

    def export(self, expr):
        """Export contents of the `SqlRegistry`, limited to those reachable from
        the `Dataset`\ s identified by the expression `expr`, into a `TableSet`
        format such that it can be imported into a different database.

        Parameters
        ----------
        expr : `str`
            An expression (SQL query that evaluates to a list of `Dataset`
            primary keys) that selects the `Datasets, or a `QuantumGraph`
            that can be similarly interpreted.

        Returns
        -------
        ts : `TableSet`
            Containing all rows, from all tables in the `SqlRegistry` that
            are reachable from the selected `Dataset`\ s.
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
            imported `Dataset`\ s.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def transfer(self, src, expr, collection):
        """Transfer contents from a source `SqlRegistry`, limited to those
        reachable from the `Dataset`\ s identified by the expression `expr`,
        into this `SqlRegistry` and collection them with a Collection.

        Parameters
        ----------
        src : `SqlRegistry`
            The source `SqlRegistry`.
        expr : `str`
            An expression that limits the `DataUnit`\ s and (indirectly)
            the `Dataset`\ s transferred.
        collection : `str`
            An additional Collection collection assigned to the newly
            imported `Dataset`\ s.
        """
        self.import_(src.export(expr), collection)
