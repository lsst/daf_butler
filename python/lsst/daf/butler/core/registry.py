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

from abc import ABCMeta, abstractmethod

from lsst.daf.butler.core.utils import doImport

from .config import Config


class RegistryConfig(Config):
    pass


class Registry(metaclass=ABCMeta):
    """Registry interface.

    Parameters
    ----------
    config : `RegistryConfig`
    """
    @staticmethod
    def fromConfig(config):
        """Create `Registry` subclass instance from `config`.

        Uses ``registry.cls`` from `config` to determine which subclass to instantiate.

        Parameters
        ----------
        config : `RegistryConfig`, `Config` or `str`
            Registry configuration

        Returns
        -------
        registry : `Registry` (subclass)
            A new `Registry` subclass instance.
        """
        if not isinstance(config, RegistryConfig):
            if isinstance(config, str):
                config = Config(config)
            if isinstance(config, Config):
                config = RegistryConfig(config['registry'])
            else:
                raise ValueError("Incompatible Registry configuration: {}".format(config))
        cls = doImport(config['cls'])
        return cls(config=config)

    def __init__(self, config):
        assert isinstance(config, RegistryConfig)
        self.config = config

    @abstractmethod
    def registerDatasetType(self, datasetType):
        """
        Add a new :ref:`DatasetType` to the Registry.

        Parameters
        ----------
        datasetType: `DatasetType`
            The `DatasetType` to be added.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def getDatasetType(self, name):
        """Get the `DatasetType`.

        Parameters
        ----------
        name: `str`
            Name of the type.

        Returns
        -------
        datasetType: `DatasetType`
            The `DatasetType` associated with the given name.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def addDataset(self, ref, uri, components, run, producer=None):
        """Add a `Dataset` to a Collection.

        This always adds a new `Dataset`; to associate an existing `Dataset` with
        a new `Collection`, use `associate`.

        Parameters
        ----------
        ref: `DatasetRef`
            Identifies the `Dataset` and contains its `DatasetType`.
        uri: `str`
            The URI that has been associated with the `Dataset` by a
            `Datastore`.
        components: `dict`
            If the `Dataset` is a composite, a ``{name : URI}`` dictionary of
            its named components and storage locations.
        run: `Run`
            The `Run` instance that produced the Dataset.  Ignored if
            ``producer`` is passed (`producer.run` is then used instead).
            A Run must be provided by one of the two arguments.
        producer: `Quantum`
            Unit of work that produced the Dataset.  May be ``None`` to store
            no provenance information, but if present the `Quantum` must
            already have been added to the Registry.

        Returns
        -------
        ref: `DatasetRef`
            A newly-created `DatasetRef` instance.

        Raises
        ------
        e: `Exception`
            If a `Dataset` with the given `DatasetRef` already exists in the
            given Collection.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def associate(self, collection, refs):
        """Add existing `Dataset`s to a Collection, possibly creating the
        Collection in the process.

        Parameters
        ----------
        collection: `str`
            Indicates the Collection the `Dataset`s should be associated with.
        refs: `[DatasetRef]`
            A `list` of `DatasetRef` instances that already exist in this
            `Registry`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def disassociate(self, collection, refs, remove=True):
        """Remove existing `Dataset`s from a Collection.

        ``collection`` and ``ref`` combinations that are not currently
        associated are silently ignored.

        Parameters
        ----------
        collection: `str`
            The Collection the `Dataset`s should no longer be associated with.
        refs: `[DatasetRef]`
            A `list` of `DatasetRef` instances that already exist in this
            `Registry`.
        remove: `bool`
            If `True`, remove `Dataset`s from the `Registry` if they are not
            associated with any Collection (including via any composites).

        Returns
        -------
        removed: `[DatasetRef]`
            If `remove` is `True`, the `list` of `DatasetRef`s that were
            removed.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def makeRun(self, collection):
        """Create a new `Run` in the `Registry` and return it.

        Parameters
        ----------
        collection: `str`
            The Collection collection used to identify all inputs and outputs
            of the `Run`.

        Returns
        -------
        run: `Run`
            A new `Run` instance.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def updateRun(self, run):
        """Update the `environment` and/or `pipeline` of the given `Run`
        in the database, given the `DatasetRef` attributes of the input
        `Run`.

        Parameters
        ----------
        run: `Run`
            The `Run` to update with the new values filled in.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def addQuantum(self, quantum):
        """Add a new `Quantum` to the `Registry`.

        Parameters
        ----------
        quantum: `Quantum`
            Instance to add to the `Registry`.
            The given `Quantum` must not already be present in the `Registry`
            (or any other), therefore its:
            - `pkey` attribute must be `None`.
            - `predictedInputs` attribute must be fully populated with
               `DatasetRef`s, and its.
            - `actualInputs` and `outputs` will be ignored.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def markInputUsed(self, quantum, ref):
        """Record the given `DatasetRef` as an actual (not just predicted)
        input of the given `Quantum`.

        This updates both the `Registry`'s `Quantum` table and the Python `Quantum.actualInputs` attribute.

        Parameters
        ----------
        quantum: `Quantum`
            Producer to update.
            Will be updated in this call.
        ref: `DatasetRef`
            To set as actually used input.

        Raises
        ------
        e: `Exception`
            If `ref` is not already in the predicted inputs list.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def addDataUnit(self, unit, replace=False):
        """Add a new `DataUnit`, optionally replacing an existing one
        (for updates).

        unit: `DataUnit`
            The `DataUnit` to add or replace.
        replace: `bool`
            If `True`, replace any matching `DataUnit` that already exists
            (updating its non-unique fields) instead of raising an exception.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def expand(self, ref):
        """Expand a `DatasetRef`.

        Parameters
        ----------
        ref: `DatasetRef`
            The `DatasetRef` to expand.

        Returns
        -------
        ref: `DatasetRef`
            The expanded reference.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def find(self, collection, ref):
        """Look up the location of the `Dataset` associated with the given
        `DatasetRef`.

        This can be used to obtain the URI that permits the `Dataset` to be
        read from a `Datastore`.

        Parameters
        ----------
        collection: `str`
            Identifies the Collection to search.
        ref: `DatasetRef`
            Identifies the `Dataset`.

        Returns
        -------
        ref: `DatasetRef`
            A ref to the `Dataset`, or `None` if no matching `Dataset`
            was found.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def subset(self, collection, expr, datasetTypes):
        """Create a new `Collection` by subsetting an existing one.

        Parameters
        ----------
        collection: `str`
            Indicates the input Collection to subset.
        expr: `str`
            An expression that limits the `DataUnit`s and (indirectly)
            `Dataset`s in the subset.
        datasetTypes: `[DatasetType]`
            The `list` of `DatasetType`s whose instances should be included
            in the subset.

        Returns
        -------
        collection: `str`
            The newly created collection.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def merge(self, outputCollection, inputCollections):
        """Create a new Collection from a series of existing ones.

        Entries earlier in the list will be used in preference to later
        entries when both contain
        `Dataset`s with the same `DatasetRef`.

        Parameters
        ----------
        outputCollection: `str`
            collection to use for the new Collection.
        inputCollections: `[str]`
            A `list` of Collections to combine.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def makeDataGraph(self, collections, expr, neededDatasetTypes, futureDatasetTypes):
        """Evaluate a filter expression and lists of `DatasetType`s and
        return a `QuantumGraph`.

        Parameters
        ----------
        collections: `[str]`
            An ordered `list` of collections indicating the Collections to
            search for `Dataset`s.
        expr: `str`
            An expression that limits the `DataUnit`s and (indirectly) the
            `Dataset`s returned.
        neededDatasetTypes: `[DatasetType]`
            The `list` of `DatasetType`s whose instances should be included
            in the graph and limit its extent.
        futureDatasetTypes: `[DatasetType]`
            The `list` of `DatasetType`s whose instances may be added to the
            graph later, which requires that their `DataUnit` types must be
            present in the graph.

        Returns
        -------
        graph: `QuantumGraph`
            A `QuantumGraph` instance with a `QuantumGraph.units` attribute
            that is not `None`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def makeProvenanceGraph(self, expr, types=None):
        """Make a `QuantumGraph` that contains the full provenance of all
        `Dataset`s matching an expression.

        Parameters
        ----------
        expr: `str`
            An expression (SQL query that evaluates to a list of `Dataset`
            primary keys) that selects the `Dataset`s.

        Returns
        -------
        graph: `QuantumGraph`
            Instance (with `units` set to `None`).
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def export(self, expr):
        """Export contents of the `Registry`, limited to those reachable from
        the `Dataset`s identified by the expression `expr`, into a `TableSet`
        format such that it can be imported into a different database.

        Parameters
        ----------
        expr: `str`
            An expression (SQL query that evaluates to a list of `Dataset`
            primary keys) that selects the `Datasets, or a `QuantumGraph`
            that can be similarly interpreted.

        Returns
        -------
        ts: `TableSet`
            Containing all rows, from all tables in the `Registry` that
            are reachable from the selected `Dataset`s.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def import_(self, tables, collection):
        """Import (previously exported) contents into the (possibly empty)
        `Registry`.

        Parameters
        ----------
        ts: `TableSet`
            Contains the previously exported content.
        collection: `str`
            An additional Collection collection assigned to the newly
            imported `Dataset`s.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def transfer(self, src, expr, collection):
        """Transfer contents from a source `Registry`, limited to those
        reachable from the `Dataset`s identified by the expression `expr`,
        into this `Registry` and collection them with a Collection.

        Parameters
        ----------
        src: `Registry`
            The source `Registry`.
        expr: `str`
            An expression that limits the `DataUnit`s and (indirectly)
            the `Dataset`s transferred.
        collection: `str`
            An additional Collection collection assigned to the newly
            imported `Dataset`s.
        """
        self.import_(src.export(expr), collection)
