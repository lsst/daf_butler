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
import contextlib

from .utils import doImport
from .config import Config, ConfigSubset
from .schema import SchemaConfig
from .utils import transactional

__all__ = ("RegistryConfig", "Registry")


class RegistryConfig(ConfigSubset):
    component = "registry"
    requiredKeys = ("cls",)
    defaultConfigFile = "registry.yaml"


class Registry(metaclass=ABCMeta):
    """Registry interface.

    Parameters
    ----------
    registryConfig : `RegistryConfig`
        Registry configuration.
    schemaConfig : `SchemaConfig`, optional
        Schema configuration.
    """

    defaultConfigFile = None
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    @classmethod
    @abstractmethod
    def setConfigRoot(cls, root, config, full):
        """Set any filesystem-dependent config options for this Registry to
        be appropriate for a new empty repository with the given root.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the data repository.
        config : `Config`
            A `Config` to update. Only the subset understood by
            this component will be updated. Will not expand
            defaults.
        full : `Config`
            A complete config with all defaults expanded that can be
            converted to a `RegistryConfig`. Read-only and will not be
            modified by this method.
            Repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from `full` to `Config`.
        """
        Config.overrideParameters(RegistryConfig, config, full,
                                  toCopy=("skypix.cls", "skypix.level"))

    @staticmethod
    def fromConfig(registryConfig, schemaConfig=None, create=False):
        """Create `Registry` subclass instance from `config`.

        Uses ``registry.cls`` from `config` to determine which subclass to
        instantiate.

        Parameters
        ----------
        registryConfig : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
            Registry configuration
        schemaConfig : `SchemaConfig`, `Config` or `str`, optional.
            Schema configuration. Can be read from supplied registryConfig
            if the relevant component is defined and ``schemaConfig`` is
            `None`.
        create : `bool`
            Assume empty Registry and create a new one.

        Returns
        -------
        registry : `Registry` (subclass)
            A new `Registry` subclass instance.
        """
        if schemaConfig is None:
            # Try to instantiate a schema configuration from the supplied
            # registry configuration.
            schemaConfig = SchemaConfig(registryConfig)
        elif not isinstance(schemaConfig, SchemaConfig):
            if isinstance(schemaConfig, str) or isinstance(schemaConfig, Config):
                schemaConfig = SchemaConfig(schemaConfig)
            else:
                raise ValueError("Incompatible Schema configuration: {}".format(schemaConfig))

        if not isinstance(registryConfig, RegistryConfig):
            if isinstance(registryConfig, str) or isinstance(registryConfig, Config):
                registryConfig = RegistryConfig(registryConfig)
            else:
                raise ValueError("Incompatible Registry configuration: {}".format(registryConfig))

        cls = doImport(registryConfig["cls"])
        return cls(registryConfig, schemaConfig, create=create)

    def __init__(self, registryConfig, schemaConfig=None, create=False):
        assert isinstance(registryConfig, RegistryConfig)
        self.config = registryConfig
        self._pixelization = None

    def __str__(self):
        return "None"

    @contextlib.contextmanager
    def transaction(self):
        """Optionally implemented in `Registry` subclasses to provide exception
        safety guarantees in case an exception is raised in the enclosed block.

        This context manager may be nested (e.g. any implementation by a
        `Registry` subclass must nest properly).

        .. warning::

            The level of exception safety is not guaranteed by this API.
            It may implement stong exception safety and roll back any changes
            leaving the state unchanged, or it may do nothing leaving the
            underlying `Registry` corrupted.  Depending on the implementation
            in the subclass.

        .. todo::

            Investigate if we may want to provide a `TransactionalRegistry`
            subclass that guarantees a particular level of exception safety.
        """
        yield

    @property
    def pixelization(self):
        """Object that interprets SkyPix DataUnit values (`sphgeom.Pixelization`)."""
        if self._pixelization is None:
            pixelizationCls = doImport(self.config["skypix.cls"])
            self._pixelization = pixelizationCls(level=self.config["skypix.level"])
        return self._pixelization

    @abstractmethod
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def registerDatasetType(self, datasetType):
        """
        Add a new `DatasetType` to the Registry.

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
            ``True`` if ``datasetType`` was inserted, ``False`` if an identical
            existing `DatsetType` was found.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def addDataset(self, datasetType, dataId, run, producer=None, recursive=False):
        """Adds a Dataset entry to the `Registry`

        This always adds a new Dataset; to associate an existing Dataset with
        a new collection, use ``associate``.

        Parameters
        ----------
        datasetType : `str`
            Name of a `DatasetType`.
        dataId : `dict`
            A `dict` of `DataUnit` link name, value pairs that label the
            `DatasetRef` within a collection.
        run : `Run`
            The `Run` instance that produced the Dataset.  Ignored if
            ``producer`` is passed (`producer.run` is then used instead).
            A Run must be provided by one of the two arguments.
        producer : `Quantum`
            Unit of work that produced the Dataset.  May be ``None`` to store
            no provenance information, but if present the `Quantum` must
            already have been added to the Registry.
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def associate(self, collection, refs):
        """Add existing Datasets to a collection, possibly creating the
        collection in the process.

        Parameters
        ----------
        collection : `str`
            Indicates the collection the Datasets should be associated with.
        refs : `list` of `DatasetRef`
            A `list` of `DatasetRef` instances that already exist in this
            `Registry`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
            `Registry`.
        remove : `bool`
            If `True`, remove Datasets from the `Registry` if they are not
            associated with any collection (including via any composites).

        Returns
        -------
        removed : `list` of `DatasetRef`
            If `remove` is `True`, the `list` of `DatasetRef`\ s that were
            removed.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def addExecution(self, execution):
        """Add a new `Execution` to the `Registry`.

        If ``execution.id`` is `None` the `Registry` will update it to
        that of the newly inserted entry.

        Parameters
        ----------
        execution : `Execution`
            Instance to add to the `Registry`.
            The given `Execution` must not already be present in the
            `Registry`.

        Raises
        ------
        Exception
            If `Execution` is already present in the `Registry`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def getExecution(self, id):
        """Retrieve an Execution.

        Parameters
        ----------
        id : `int`
            The unique identifier for the Execution.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def makeRun(self, collection):
        """Create a new `Run` in the `Registry` and return it.

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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def ensureRun(self, run):
        """Conditionally add a new `Run` to the `Registry`.

        If the ``run.id`` is ``None`` or a `Run` with this `id` doesn't exist
        in the `Registry` yet, add it.  Otherwise, ensure the provided run is
        identical to the one already in the registry.

        Parameters
        ----------
        run : `Run`
            Instance to add to the `Registry`.

        Raises
        ------
        ValueError
            If ``run`` already exists, but is not identical.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def addRun(self, run):
        """Add a new `Run` to the `Registry`.

        Parameters
        ----------
        run : `Run`
            Instance to add to the `Registry`.
            The given `Run` must not already be present in the `Registry`
            (or any other).  Therefore its `id` must be `None` and its
            `collection` must not be associated with any existing `Run`.

        Raises
        ------
        ValueError
            If a run already exists with this collection.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def addQuantum(self, quantum):
        r"""Add a new `Quantum` to the `Registry`.

        Parameters
        ----------
        quantum : `Quantum`
            Instance to add to the `Registry`.
            The given `Quantum` must not already be present in the
            `Registry` (or any other), therefore its:

            - `run` attribute must be set to an existing `Run`.
            - `predictedInputs` attribute must be fully populated with
              `DatasetRef`\ s, and its.
            - `actualInputs` and `outputs` will be ignored.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def getQuantum(self, id):
        """Retrieve an Quantum.

        Parameters
        ----------
        id : `int`
            The unique identifier for the Quantum.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def markInputUsed(self, quantum, ref):
        """Record the given `DatasetRef` as an actual (not just predicted)
        input of the given `Quantum`.

        This updates both the `Registry`"s `Quantum` table and the Python
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
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def addDataUnitEntry(self, dataUnitName, values):
        """Add a new `DataUnit` entry.

        dataUnitName : `str`
            Name of the `DataUnit` (e.g. ``"Camera"``).
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
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def setDataUnitRegion(self, dataUnitNames, value, region, update=True):
        """Set the region field for a DataUnit instance or a combination
        thereof and update associated spatial join tables.

        Parameters
        ----------
        dataUnitNames : sequence
            A sequence of DataUnit names whose instances are jointly associated
            with a region on the sky.
        value : `dict`
            A dictionary of values that uniquely identify the DataUnits.
        region : `sphgeom.ConvexPolygon`
            Region on the sky.
        update : `bool`
            If True, existing region information for these DataUnits is being
            replaced.  This is usually required because DataUnit entries are
            assumed to be pre-inserted prior to calling this function.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
            The region associated with a ``dataId`` or ``None`` if not present.

        Raises
        ------
        KeyError
            If the set of dataunits for the ``dataId`` does not correspond to
            a unique spatial lookup.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def selectDataUnits(self, collections, expression, neededDatasetTypes, futureDatasetTypes):
        """Evaluate a filter expression and lists of
        `DatasetTypes <DatasetType>` and return a set of data unit values.

        Returned set consists of combinations of units participating in data
        transformation from ``neededDatasetTypes`` to ``futureDatasetTypes``,
        restricted by existing data and filter expression.

        Parameters
        ----------
        collections : `PreFlightCollections`
            Object which provides names of the input/output collections.
        expression : `str`
            An expression that limits the `DataUnits <DataUnit>` and
            (indirectly) the Datasets returned.
        neededDatasetTypes : `list` of `DatasetType`
            The `list` of `DatasetTypes <DatasetType>` whose DataUnits will
            be included in the returned column set. Output is limited by the
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
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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

    @abstractmethod
    def export(self, expr):
        """Export contents of the `Registry`, limited to those reachable
        from the Datasets identified by the expression `expr`, into a
        `TableSet` format such that it can be imported into a different
        database.

        Parameters
        ----------
        expr : `str`
            An expression (SQL query that evaluates to a list of Dataset
            primary keys) that selects the `Datasets, or a `QuantumGraph`
            that can be similarly interpreted.

        Returns
        -------
        ts : `TableSet`
            Containing all rows, from all tables in the `Registry` that
            are reachable from the selected Datasets.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def import_(self, tables, collection):
        """Import (previously exported) contents into the (possibly empty)
        `Registry`.

        Parameters
        ----------
        ts : `TableSet`
            Contains the previously exported content.
        collection : `str`
            An additional collection assigned to the newly
            imported Datasets.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @transactional
    def transfer(self, src, expr, collection):
        r"""Transfer contents from a source `Registry`, limited to those
        reachable from the Datasets identified by the expression `expr`,
        into this `Registry` and associate them with a collection.

        Parameters
        ----------
        src : `Registry`
            The source `Registry`.
        expr : `str`
            An expression that limits the `DataUnit`\ s and (indirectly)
            the Datasets transferred.
        collection : `str`
            An additional collection assigned to the newly
            imported Datasets.
        """
        self.import_(src.export(expr), collection)

    @abstractmethod
    @transactional
    def subset(self, collection, expr, datasetTypes):
        r"""Create a new collection by subsetting an existing one.

        Parameters
        ----------
        collection : `str`
            Indicates the input collection to subset.
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

    @abstractmethod
    @transactional
    def merge(self, outputCollection, inputCollections):
        """Create a new collection from a series of existing ones.

        Entries earlier in the list will be used in preference to later
        entries when both contain Datasets with the same `DatasetRef`.

        Parameters
        ----------
        outputCollection : `str`
            collection to use for the new collection.
        inputCollections : `list` of `str`
            A `list` of collections to combine.
        """
        raise NotImplementedError("Must be implemented by subclass")
