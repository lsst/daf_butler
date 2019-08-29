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

__all__ = ("RegistryConfig", "Registry", "disableWhenLimited",
           "AmbiguousDatasetError", "ConflictingDefinitionError", "OrphanedRecordError")

from abc import ABCMeta, abstractmethod
from collections.abc import Mapping
import contextlib
import functools


from lsst.utils import doImport
from .config import Config, ConfigSubset
from .dimensions import DimensionConfig, DimensionUniverse, DataId, DimensionKeyDict
from .schema import SchemaConfig
from .utils import transactional
from .dataIdPacker import DataIdPackerFactory


class AmbiguousDatasetError(Exception):
    """Exception raised when a `DatasetRef` has no ID and a `Registry`
    operation requires one.
    """


class ConflictingDefinitionError(Exception):
    """Exception raised when trying to insert a database record when a
    conflicting record already exists.
    """


class OrphanedRecordError(Exception):
    """Exception raised when trying to remove or modify a database record
    that is still being used in some other table.
    """


def disableWhenLimited(func):
    """Decorator that indicates that a method should raise NotImplementedError
    on Registries whose ``limited`` attribute is `True`.

    This implements that check and raise for all subclasses.
    """
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        if self.limited:
            raise NotImplementedError(
                "Operation not implemented for limited Registry; note that data IDs may need to be expanded "
                "by a full Registry before being used for some operations on a limited Registry."
            )
        return func(self, *args, **kwargs)
    return inner


class RegistryConfig(ConfigSubset):
    component = "registry"
    requiredKeys = ("db",)
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
    def setConfigRoot(cls, root, config, full, overwrite=True):
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
            should be copied from ``full`` to ``config``.
        overwrite : `bool`, optional
            If `False`, do not modify a value in ``config`` if the value
            already exists.  Default is always to overwrite with the provided
            ``root``.

        Notes
        -----
        If a keyword is explicitly defined in the supplied ``config`` it
        will not be overridden by this method if ``overwrite`` is `False`.
        This allows explicit values set in external configs to be retained.
        """
        Config.updateParameters(RegistryConfig, config, full,
                                toCopy=(("skypix", "cls"), ("skypix", "level")), overwrite=overwrite)

    @staticmethod
    def getDialect(registryConfig):
        """Parses the `db` key of the config and returns the database dialect.

        Parameters
        ----------
        registryConfig : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
            Registry configuration

        Returns
        -------
        dialect : `str`
            Dialect found in the connection string.
        """
        # this import can not live at the top due to circular import issue
        from .connectionString import ConnectionStringFactory
        conStr = ConnectionStringFactory.fromConfig(registryConfig)
        return conStr.get_backend_name()

    @staticmethod
    def fromConfig(registryConfig, schemaConfig=None, dimensionConfig=None, create=False, butlerRoot=None):
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

        cls = Registry.getRegistryClass(registryConfig)

        return cls(registryConfig, schemaConfig, dimensionConfig, create=create,
                   butlerRoot=butlerRoot)

    @classmethod
    def getRegistryClass(cls, registryConfig):
        """Returns registry class targeted by configuration values.

        The appropriate class is determined from the `cls` key, if it exists.
        Otherwise the `db` key is parsed and the correct class is determined
        from a list of aliases found under `clsMap` key of the registry config.

        Parameters
        ----------
        registryConfig : `ButlerConfig`, `RegistryConfig`, `Config` or `str`
            Registry configuration

        Returns
        -------
        registry : `type`
           Type object of the registry subclass targeted by the registry
           configuration.
        """
        regConfig = RegistryConfig(registryConfig)
        if regConfig.get("cls") is not None:
            registryClass = regConfig.get("cls")
        else:
            dialect = cls.getDialect(regConfig)
            if dialect not in regConfig["clsMap"]:
                raise ValueError(f"Connection string dialect has no known aliases. Received: {dialect}")
            registryClass = regConfig.get(("clsMap", dialect))

        return doImport(registryClass)

    def __init__(self, registryConfig, schemaConfig=None, dimensionConfig=None, create=False,
                 butlerRoot=None):
        assert isinstance(registryConfig, RegistryConfig)
        self.config = registryConfig
        self._pixelization = None
        self.dimensions = DimensionUniverse.fromConfig(dimensionConfig)
        self._dataIdPackerFactories = {
            name: DataIdPackerFactory.fromConfig(self.dimensions, subconfig)
            for name, subconfig in registryConfig.get("dataIdPackers", {}).items()
        }
        self._fieldsToAlwaysGet = DimensionKeyDict(keys=self.dimensions.elements, factory=set)
        for packerFactory in self._dataIdPackerFactories.values():
            packerFactory.updateFieldsToGet(self._fieldsToAlwaysGet)

    def __str__(self):
        return "None"

    @property
    def connectionString(self):
        """Return the connection string to the underlying database
        (`sqlalchemy.engine.url.URL`).
        """
        # this import can not live at the top due to circular import issue
        from .connectionString import ConnectionStringFactory
        return ConnectionStringFactory.fromConfig(self.config)

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
        """Object that interprets skypix Dimension values
        (`lsst.sphgeom.Pixelization`).

        `None` for limited registries.
        """
        if self.limited:
            return None
        if self._pixelization is None:
            pixelizationCls = doImport(self.config["skypix", "cls"])
            self._pixelization = pixelizationCls(level=self.config["skypix", "level"])
        return self._pixelization

    @abstractmethod
    def makeDatabaseDict(self, table, key, value):
        """Construct a `DatabaseDict` backed by a table in the same database as
        this Registry.

        Parameters
        ----------
        table : `table`
            Name of the table that backs the returned `DatabaseDict`.  If this
            table already exists, its schema must include at least everything
            in ``valuetypes``.
        key : `str`
            The name of the field to be used as the dictionary key.  Must not
            be present in ``value._fields``.
        value : `type`
            The type used for the dictionary's values, typically a
            `DatabaseDictRecordBase`.  Must have a ``fields`` class method
            that is a tuple of field names; these field names must also appear
            in the return value of the ``types()`` class method, and it must be
            possible to construct it from a sequence of values. Lengths of
            string fields must be obtainable as a `dict` from using the
            ``lengths`` property.

        Returns
        -------
        databaseDict : `DatabaseDict`
            `DatabaseDict` backed by this registry.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def getAllCollections(self):
        """Get names of all the collections found in this repository.

        Returns
        -------
        collections : `set` of `str`
            The collections.
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
        LookupError
            If one or more data ID keys are missing.
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
            Raised if the dimensions or storage class are invalid.
        ConflictingDefinitionError
            Raised if this DatasetType is already registered with a different
            definition.

        Returns
        -------
        inserted : `bool`
            `True` if ``datasetType`` was inserted, `False` if an identical
            existing `DatsetType` was found.  Note that in either case the
            DatasetType is guaranteed to be defined in the Registry
            consistently with the given definition.
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
    def getAllDatasetTypes(self):
        r"""Get every registered `DatasetType`.

        Returns
        -------
        types : `frozenset` of `DatasetType`
            Every `DatasetType` in the registry.
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
        ConflictingDefinitionError
            If a Dataset with the given `DatasetRef` already exists in the
            given collection.

        Exception
            If ``dataId`` contains unknown or invalid `Dimension` entries.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def getDataset(self, id, datasetType=None, dataId=None):
        """Retrieve a Dataset entry.

        Parameters
        ----------
        id : `int`
            The unique identifier for the Dataset.
        datasetType : `DatasetType`, optional
            The `DatasetType` of the dataset to retrieve.  This is used to
            short-circuit retrieving the `DatasetType`, so if provided, the
            caller is guaranteeing that it is what would have been retrieved.
        dataId : `DataId`, optional
            A `Dimension`-based identifier for the dataset within a
            collection, possibly containing additional metadata. This is used
            to short-circuit retrieving the `DataId`, so if provided, the
            caller is guaranteeing that it is what would have been retrieved.

        Returns
        -------
        ref : `DatasetRef`
            A ref to the Dataset, or `None` if no matching Dataset
            was found.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def removeDataset(self, ref):
        """Remove a dataset from the Registry.

        The dataset and all components will be removed unconditionally from
        all collections, and any associated `Quantum` records will also be
        removed.  `Datastore` records will *not* be deleted; the caller is
        responsible for ensuring that the dataset has already been removed
        from all Datastores.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the dataset to be removed.  Must include a valid
            ``id`` attribute, and should be considered invalidated upon return.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        OrphanedRecordError
            Raised if the dataset is still present in any `Datastore`.
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

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``parent.id`` or ``component.id`` is `None`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def associate(self, collection, refs):
        """Add existing Datasets to a collection, implicitly creating the
        collection if it does not already exist.

        If a DatasetRef with the same exact ``dataset_id`` is already in a
        collection nothing is changed. If a `DatasetRef` with the same
        `DatasetType1` and dimension values but with different ``dataset_id``
        exists in the collection, `ValueError` is raised.

        Parameters
        ----------
        collection : `str`
            Indicates the collection the Datasets should be associated with.
        refs : iterable of `DatasetRef`
            An iterable of `DatasetRef` instances that already exist in this
            `Registry`.  All component datasets will be associated with the
            collection as well.

        Raises
        ------
        ConflictingDefinitionError
            If a Dataset with the given `DatasetRef` already exists in the
            given collection.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @transactional
    def disassociate(self, collection, refs):
        r"""Remove existing Datasets from a collection.

        ``collection`` and ``ref`` combinations that are not currently
        associated are silently ignored.

        Parameters
        ----------
        collection : `str`
            The collection the Datasets should no longer be associated with.
        refs : `list` of `DatasetRef`
            A `list` of `DatasetRef` instances that already exist in this
            `Registry`.  All component datasets will also be removed.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``any(ref.id is None for ref in refs)``.
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

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
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

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
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

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
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
        ConflictingDefinitionError
            If ``execution`` is already present in the `Registry`.
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
        ConflictingDefinitionError
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
        ConflictingDefinitionError
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
        ConflictingDefinitionError
            If an entry with the primary-key defined in `values` is already
            present.
        NotImplementedError
            Raised if `limited` is `True`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @disableWhenLimited
    @transactional
    def addDimensionEntryList(self, dimension, dataIdList, entry=None, **kwds):
        """Add a new `Dimension` entry.

        dimension : `str` or `Dimension`
            Either a `Dimension` object or the name of one.
        dataId : `list` of `dict` or `DataId`
            A list of `dict`-like objects containing the `Dimension` links that
            form the primary key of the rows to insert.  If these are a full
            `DataId` object, ``dataId.entries[dimension]`` will be updated with
            ``entry`` and then inserted into the `Registry`.
        entry : `dict`
            Dictionary that maps column name to column value.
        kwds
            Additional keyword arguments passed to the `DataId` constructor
            to convert ``dataId`` to a true `DataId` or augment an existing
            one.

        If ``values`` includes a "region" key, regions will automatically be
        added to set it any associated spatial join tables.
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
        ConflictingDefinitionError
            If an entry with the primary-key defined in `values` is already
            present.
        NotImplementedError
            Raised if `limited` is `True`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    @disableWhenLimited
    def findDimensionEntries(self, dimension):
        """Return all `Dimension` entries corresponding to the named dimension.

        Parameters
        ----------
        dimension : `str` or `Dimension`
            Either a `Dimension` object or the name of one.

        Returns
        -------
        entries : `list` of `dict`
            List with `dict` containing the `Dimension` values for each variant
            of the `Dimension`.  Returns empty list if no entries have been
            added for this dimension.

        Raises
        ------
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

    @disableWhenLimited
    def expandDataId(self, dataId=None, *, dimension=None, metadata=None, region=False, update=False,
                     **kwds):
        """Expand a data ID to include additional information.

        `expandDataId` always returns a true `DataId` and ensures that its
        `~DataId.entries` dict contains (at least) values for all implied
        dependencies.

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
        update : `bool`
            If `True`, assume existing entries and regions in the given
            `DataId` are out-of-date and should be updated by values in the
            database.  If `False`, existing values will be assumed to be
            correct and database queries will only be executed if they are
            missing.
        kwds
            Additional keyword arguments passed to the `DataId` constructor
            to convert ``dataId`` to a true `DataId` or augment an existing
            one.

        Returns
        -------
        dataId : `DataId`
            A Data ID with all requested data populated.

        Raises
        ------
        NotImplementedError
            Raised if `limited` is `True`.
        """
        dataId = DataId(dataId, dimension=dimension, universe=self.dimensions, **kwds)

        fieldsToGet = DimensionKeyDict(keys=dataId.dimensions(implied=True).elements, factory=set)
        fieldsToGet.updateValues(self._fieldsToAlwaysGet)

        # Interpret the 'metadata' argument and initialize the 'fieldsToGet'
        # dict, which maps each DimensionElement instance to a set of field
        # names.
        if metadata is not None:
            if dimension is not None and not isinstance(metadata, Mapping):
                # If a single dimension was passed explicitly, permit
                # 'metadata' to be a sequence corresponding to just that
                # dimension by updating our mapping-of-sets for that dimension.
                fieldsToGet[dimension].update(metadata)
            else:
                fieldsToGet.updateValues(metadata)

        # If 'region' was passed, add a query for that to fieldsToGet as well.
        if region and (update or dataId.region is None):
            holder = dataId.dimensions().getRegionHolder()
            if holder is not None:
                if holder.name == "skypix":
                    # skypix is special; we always obtain those regions from
                    # self.pixelization
                    dataId.region = self.pixelization.pixel(dataId["skypix"])
                else:
                    fieldsToGet[holder].add("region")

        # We now process fieldsToGet with calls to _queryMetadata via a
        # depth-first traversal of the dependency graph.  As we traverse, we
        # update...

        # A dictionary containing all link values.  This starts with the given
        # dataId, but we'll update it to include links for optional
        # dependencies.
        allLinks = dict(dataId)

        # A set of DimensionElement names recording the vertices we've
        # processed:
        visited = set()

        def visit(element):
            if element.name in visited:
                return
            assert element.links() <= allLinks.keys()
            entries = dataId.entries[element]
            dependencies = element.dependencies(implied=True)
            # Get the set of fields we want to retrieve.
            fieldsToGetNow = fieldsToGet[element]
            # Note which links to dependencies we need to query for and which
            # we already know.  Make sure the ones we know are in the entries
            # dict for this element.
            linksWeKnow = dependencies.links().intersection(allLinks.keys())
            linksWeNeed = dependencies.links() - linksWeKnow
            fieldsToGetNow |= linksWeNeed
            entries.update((link, allLinks[link]) for link in linksWeKnow)
            # Remove fields that are already present in the dataId.
            if not update:
                fieldsToGetNow -= entries.keys()
            # Remove fields that are part of the primary key of this element;
            # we have to already know those if the query is going to work.
            # (and we asserted that we do know them up at the top).
            fieldsToGetNow -= element.links()
            # Actually do the query - only if there's actually anything left
            # to query.  Put the results in the entries dict.
            if fieldsToGetNow:
                result = self._queryMetadata(element, allLinks, fieldsToGetNow)
                if "region" in result:
                    dataId.region = result.pop("region")
                entries.update(result)
            # Update the running dictionary of link values and the marker set.
            allLinks.update((link, entries[link]) for link in dependencies.links())
            visited.add(element.name)
            # Recurse to dependencies.  Note that at this point we know that
            # allLinks has all of the links for any element we're recursing to,
            # either because we started with them in the data ID, they were
            # already in the entries dict, or we queried for them above.
            for other in dependencies:
                visit(other)

        # Kick off the traversal with joins, which are never dependencies of
        # any other elements.
        for join in dataId.dimensions().joins():
            visit(join)

        # Now traverse over the dimensions that are not dependencies of any
        # other dependencies in this particular graph.
        for dim in dataId.dimensions().leaves:
            visit(dim)

        return dataId

    @abstractmethod
    @disableWhenLimited
    def _queryMetadata(self, element, dataId, columns):
        """Get metadata associated with a dataId.

        This is conceptually a "protected" method that must be overridden by
        subclasses but should not be called directly by users, who should use
        ``expandDataId(dataId, dimension=element, metadata={...})`` instead.

        Parameters
        ----------
        element : `DimensionElement`
            The `Dimension` or `DimensionJoin` to query for column values.
        dataId : `dict` or `DataId`
            A `dict`-like object containing the `Dimension` links that include
            the primary keys of the rows to query.  May include link fields
            beyond those required to identify ``element``.
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

    def makeDataIdPacker(self, name, dataId=None, **kwds):
        """Create an object that can pack certain data IDs into integers.

        Parameters
        ----------
        name : `str`
            Name of the packer, as given in the `Registry` configuration.
        dataId : `dict` or `DataId`, optional
            Data ID that identifies at least the "given" dimensions of the
            packer.
        kwds
            Addition keyword arguments used to augment or override the given
            data ID.

        Returns
        -------
        packer : `DataIdPacker`
            Instance of a subclass of `DataIdPacker`.
        """
        factory = self._dataIdPackerFactories[name]
        givenDataId = self.expandDataId(dataId, dimensions=factory.dimensions.given, **kwds)
        return factory.makePacker(givenDataId)

    def packDataId(self, name, dataId=None, *, returnMaxBits=False, **kwds):
        """Pack the given `DataId` into an integer.

        Parameters
        ----------
        name : `str`
            Name of the packer, as given in the `Registry` configuration.
        dataId : `dict` or `DataId`, optional
            Data ID that identifies at least the "required" dimensions of the
            packer.
        returnMaxBits : `bool`
            If `True`, return a tuple of ``(packed, self.maxBits)``.
        kwds
            Addition keyword arguments used to augment or override the given
            data ID.

        Returns
        -------
        packed : `int`
            Packed integer ID.
        maxBits : `int`, optional
            Maximum number of nonzero bits in ``packed``.  Not returned unless
            ``returnMaxBits`` is `True`.
        """
        packer = self.makeDataIdPacker(name, dataId, **kwds)
        return packer.pack(dataId, returnMaxBits=returnMaxBits, **kwds)
