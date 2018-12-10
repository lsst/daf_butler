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
from collections.abc import Mapping
import contextlib
import functools

from lsst.utils import doImport
from lsst.sphgeom import ConvexPolygon
from .config import Config, ConfigSubset
from .dimensions import DimensionConfig, DimensionGraph, DataId
from .schema import SchemaConfig
from .utils import transactional

__all__ = ("RegistryConfig", "Registry", "disableWhenLimited")


def disableWhenLimited(func):
    """Decorator that indicates that a method should raise NotImplementedError
    on Registries whose ``limited`` attribute is `True`.

    This implements that check and raise for all subclasses.
    """
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        if self.limited:
            raise NotImplementedError("Not implemented for limited Registry.")
        return func(self, *args, **kwargs)
    return inner


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
    dimensionConfig : `DimensionConfig` or `Config` or
        `DimensionGraph` configuration.
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
                                  toCopy=(("skypix", "cls"), ("skypix", "level")))

    @staticmethod
    def fromConfig(registryConfig, schemaConfig=None, dimensionConfig=None, create=False):
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
        dimensionConfig : `DimensionConfig` or `Config` or
            `str`, optional. `DimensionGraph` configuration. Can be read
            from supplied registryConfig if the relevant component is
            defined and ``dimensionConfig`` is `None`.
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

        if dimensionConfig is None:
            # Try to instantiate a schema configuration from the supplied
            # registry configuration.
            dimensionConfig = DimensionConfig(registryConfig)
        elif not isinstance(dimensionConfig, DimensionConfig):
            if isinstance(dimensionConfig, str) or isinstance(dimensionConfig, Config):
                dimensionConfig = DimensionConfig(dimensionConfig)
            else:
                raise ValueError("Incompatible Dimension configuration: {}".format(dimensionConfig))

        if not isinstance(registryConfig, RegistryConfig):
            if isinstance(registryConfig, str) or isinstance(registryConfig, Config):
                registryConfig = RegistryConfig(registryConfig)
            else:
                raise ValueError("Incompatible Registry configuration: {}".format(registryConfig))

        cls = doImport(registryConfig["cls"])
        return cls(registryConfig, schemaConfig, dimensionConfig, create=create)

    def __init__(self, registryConfig, schemaConfig=None, dimensionConfig=None, create=False):
        assert isinstance(registryConfig, RegistryConfig)
        self.config = registryConfig
        self._pixelization = None
        self.dimensions = DimensionGraph.fromConfig(dimensionConfig)

    def __str__(self):
        return "None"

    @property
    def limited(self):
        """If True, this Registry does not maintain Dimension metadata or
        relationships (`bool`)."""
        return self.config.get("limited", False)

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
        """Object that interprets SkyPix Dimension values (`lsst.sphgeom.Pixelization`).

        `None` for limited registries.
        """
        if self.limited:
            return None
        if self._pixelization is None:
            pixelizationCls = doImport(self.config["skypix", "cls"])
            self._pixelization = pixelizationCls(level=self.config["skypix", "level"])
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
    def find(self, collection, datasetType, dataId=None, **kwds):
        """Lookup a dataset.

        This can be used to obtain a `DatasetRef` that permits the dataset to
        be read from a `Datastore`.

        Parameters
        ----------
        collection : `str`
            Identifies the collection to search.
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.
        dataId : `dict` or `DataId`, optional
            A `dict`-like object containing the `Dimension` links that identify
            the dataset within a collection.
        kwds
            Additional keyword arguments passed to the `DataId` constructor
            to convert ``dataId`` to a true `DataId` or augment an existing
            one.

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
            `True` if ``datasetType`` was inserted, `False` if an identical
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
    def addDataset(self, datasetType, dataId, run, producer=None, recursive=False, **kwds):
        """Adds a Dataset entry to the `Registry`

        This always adds a new Dataset; to associate an existing Dataset with
        a new collection, use ``associate``.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            A `DatasetType` or the name of one.
        dataId : `dict` or `DataId`
            A `dict`-like object containing the `Dimension` links that identify
            the dataset within a collection.
        run : `Run`
            The `Run` instance that produced the Dataset.  Ignored if
            ``producer`` is passed (`producer.run` is then used instead).
            A Run must be provided by one of the two arguments.
        producer : `Quantum`
            Unit of work that produced the Dataset.  May be `None` to store
            no provenance information, but if present the `Quantum` must
            already have been added to the Registry.
        recursive : `bool`
            If True, recursively add Dataset and attach entries for component
            Datasets as well.
        kwds
            Additional keyword arguments passed to the `DataId` constructor
            to convert ``dataId`` to a true `DataId` or augment an existing
            one.

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
            If ``dataId`` contains unknown or invalid `Dimension` entries.
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
        # TODO: this requires `ref.dataset_id` to be not None, and probably
        # doesn't use anything else from `ref`.  Should it just take a
        # `dataset_id`?
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
        # TODO: this requires `ref.dataset_id` to be not None, and probably
        # doesn't use anything else from `ref`.  Should it just take a
        # `dataset_id`?
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
        # TODO: this requires `ref.dataset_id` to be not None, and probably
        # doesn't use anything else from `ref`.  Should it just take a
        # `dataset_id`?
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

        If the ``run.id`` is `None` or a `Run` with this `id` doesn't exist
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
    @disableWhenLimited
    @transactional
    def addDimensionEntry(self, dimension, dataId=None, entry=None, **kwds):
        """Add a new `Dimension` entry.

        dimension : `str` or `Dimension`
            Either a `Dimension` object or the name of one.
        dataId : `dict` or `DataId`, optional
            A `dict`-like object containing the `Dimension` links that form
            the primary key of the row to insert.  If this is a full `DataId`
            object, ``dataId.entries[dimension]`` will be updated with
            ``entry`` and then inserted into the `Registry`.
        entry : `dict`
            Dictionary that maps column name to column value.
        kwds
            Additional keyword arguments passed to the `DataId` constructor
            to convert ``dataId`` to a true `DataId` or augment an existing
            one.

        If ``values`` includes a "region" key, `setDimensionRegion` will
        automatically be called to set it any associated spatial join
        tables.
        Region fields associated with a combination of Dimensions must be
        explicitly set separately.

        Returns
        -------
        dataId : `DataId`
            A Data ID for exactly the given dimension that includes the added
            entry.

        Raises
        ------
        TypeError
            If the given `Dimension` does not have explicit entries in the
            registry.
        ValueError
            If an entry with the primary-key defined in `values` is already
            present.
        NotImplementedError
            Raised if `limited` is `True`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @disableWhenLimited
    def findDimensionEntry(self, dimension, dataId=None, **kwds):
        """Return a `Dimension` entry corresponding to a `DataId`.

        Parameters
        ----------
        dimension : `str` or `Dimension`
            Either a `Dimension` object or the name of one.
        dataId : `dict` or `DataId`, optional
            A `dict`-like object containing the `Dimension` links that form
            the primary key of the row to retreive.  If this is a full `DataId`
            object, ``dataId.entries[dimension]`` will be updated with the
            entry obtained from the `Registry`.
        kwds
            Additional keyword arguments passed to the `DataId` constructor
            to convert ``dataId`` to a true `DataId` or augment an existing
            one.

        Returns
        -------
        entry : `dict`
            Dictionary with all `Dimension` values, or `None` if no matching
            entry is found.  `None` if there is no entry for the given
            `DataId`.

        Raises
        ------
        NotImplementedError
            Raised if `limited` is `True`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @disableWhenLimited
    @transactional
    def setDimensionRegion(self, dataId=None, *, update=True, region=None, **kwds):
        """Set the region field for a Dimension instance or a combination
        thereof and update associated spatial join tables.

        Parameters
        ----------
        dataId : `dict` or `DataId`
            A `dict`-like object containing the `Dimension` links that form
            the primary key of the row to insert or update.  If this is a full
            `DataId`, ``dataId.region`` will be set to ``region`` (if
            ``region`` is not `None`) and then used to update or insert into
            the `Registry`.
        update : `bool`
            If True, existing region information for these Dimensions is being
            replaced.  This is usually required because Dimension entries are
            assumed to be pre-inserted prior to calling this function.
        region : `lsst.sphgeom.ConvexPolygon`, optional
            The region to update or insert into the `Registry`.  If not present
            ``dataId.region`` must not be `None`.
        kwds
            Additional keyword arguments passed to the `DataId` constructor
            to convert ``dataId`` to a true `DataId` or augment an existing
            one.

        Returns
        -------
        dataId : `DataId`
            A Data ID with its ``region`` attribute set.

        Raises
        ------
        NotImplementedError
            Raised if `limited` is `True`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @disableWhenLimited
    def selectDimensions(self, originInfo, expression, neededDatasetTypes, futureDatasetTypes):
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
            An expression that limits the `Dimensions <Dimension>` and
            (indirectly) the Datasets returned.
        neededDatasetTypes : `list` of `DatasetType` or `str`
            The `list` of `DatasetTypes <DatasetType>` whose Dimensions will
            be included in the returned column set. Output is limited to the
            the Datasets of these DatasetTypes which already exist in the
            registry.
        futureDatasetTypes : `list` of `DatasetType` or `str`
            The `list` of `DatasetTypes <DatasetType>` whose Dimensions will
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
        raise NotImplementedError("Must be implemented by subclass")

    @disableWhenLimited
    def queryDataId(self, dataId=None, *, dimension=None, metadata=None, region=False, **kwds):
        """Expand a data ID to include additional information.

        Parameters
        ----------
        dataId : `dict` or `DataId`
            A `dict`-like object containing the `Dimension` links that include
            the primary keys of the rows to query.  If this is a true `DataId`,
            the object will be updated in-place.
        dimension : `Dimension` or `str`
            A dimension passed to the `DataId` constructor to create a true
            `DataId` or augment an existing one.
        metadata : `collections.abc.Mapping`, optional
            A mapping from `Dimension` or `str` name to column name, indicating
            fields to read into ``dataId.entries``.
            If ``dimension`` is provided, may instead be a sequence of column
            names for that dimension.
        region : `bool`
            If `True` and the given `DataId` is uniquely associated with a
            region on the sky, obtain that region from the `Registry` and
            attach it as ``dataId.region``.
        kwds
            Additional keyword arguments passed to the `DataId` constructor
            to convert ``dataId`` to a true `DataId` or augment an existing
            one.

        Returns
        -------
        dataId : `DataId`
            A Data ID with its ``region`` attribute set.

        Raises
        ------
        NotImplementedError
            Raised if `limited` is `True`.
        """
        dataId = DataId(dataId, dimension=dimension, universe=self.dimensions, **kwds)

        if region:
            holder = dataId.dimensions.getRegionHolder()
            if holder is not None:
                dataId.region = self._queryRegion(holder, DataId(dataId, dimensions=holder.graph()))

        if metadata is not None:
            # By the time we get here 'dimension' and 'dataId.dimension'
            # correspond to the same thing, but:
            #  - 'dimension' might be a string or a `DimensionElement`
            #  - 'dataId.dimension' is definitely a `Dimension`
            #  - 'dataId.dimension' may not be None even if 'dimension' is
            if dimension is not None and not isinstance(metadata, Mapping):
                # If a single dimension was passed explicitly, permit 'metadata' to
                # be a sequence corresponding to just that dimension.
                result = self._queryMetadata(dataId.dimension, dataId, metadata)
                dataId.entries[dataId.dimension].update(result)
            else:
                for element, columns in metadata.items():
                    subDataId = DataId(dataId, dimension=element)
                    result = self._queryMetadata(element, subDataId, columns)
                    dataId.entries[element].update(result)

        return dataId

    @disableWhenLimited
    def _queryRegion(self, dimension, dataId):
        """Get the region associated with a dataId.

        This is conceptually a "protected" method that may be overridden by
        subclasses but should not be called directly by users, who should use
        ``queryDataId(..., region=True)`` instead.

        Parameters
        ----------
        dimension : `DimensionElement`
            The `Dimension` or `DimensionJoin` that holds the region.  Must
            be equal to ``dataId.dimensions.getRegionHolder()``.
        dataId : `DataId`
            A dictionary of primary key name-value pairs that uniquely identify
            a row in the table for ``dimension``.  Note that this must be a
            true `DataId` instance, not a `dict`, unlike more flexible
            public `Registry` methods.

        Returns
        -------
        region : `lsst.sphgeom.ConvexPolygon`
            The region associated with a ``dataId`` or `None` if not present.

        Raises
        ------
        LookupError
            Raised if no entry for the given data ID exists.
        NotImplementedError
            Raised if `limited` is `True`.
        """
        if dimension.name == "SkyPix":
            # SkyPix is special; we always obtain those regions from
            # self.pixelization
            return self.pixelization.pixel(dataId["skypix"])
        metadata = self._queryMetadata(dimension, dataId, ["region"])
        encoded = metadata["region"]
        if encoded is None:
            return None
        return ConvexPolygon.decode(encoded)

    @abstractmethod
    @disableWhenLimited
    def _queryMetadata(self, element, dataId, columns):
        """Get metadata associated with a dataId.

        This is conceptually a "protected" method that must be overridden by
        subclasses but should not be called directly by users, who should use
        ``queryDataId(..., columns={...})`` instead.

        Parameters
        ----------
        element : `DimensionElement`
            The `Dimension` or `DimensionJoin` to query for column values.
        dataId : `DataId`
            A dictionary of primary key name-value pairs that uniquely identify
            a row in the table for ``element``.  Note that this must be a
            true `DataId` instance, not a `dict`, unlike more flexible
            public `Registry` methods.
        columns : iterable of `str`
            String column names to query values for.

        Returns
        -------
        metadata : `dict`
            Dictionary that maps column name to value, or `None` if there is
            no row for the given `DataId`.

        Raises
        ------
        LookupError
            Raised if no entry for the given data ID exists.
        NotImplementedError
            Raised if `limited` is `True`.
        """
        raise NotImplementedError("Must be implemented by subclass")
