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

__all__ = ("Registry", "AmbiguousDatasetError", "ConflictingDefinitionError", "OrphanedRecordError")

from abc import ABCMeta, abstractmethod
import contextlib
from typing import Optional, Mapping, Union, Iterable, Iterator

from .config import Config
from .dimensions import (DimensionConfig, DimensionUniverse, DimensionGraph, DataId, DimensionElement,
                         DimensionRecord)
from .datasets import DatasetRef
from .schema import SchemaConfig
from .utils import transactional
from .dimensions import Dimension, DataCoordinate
from .queries import DatasetTypeExpression, CollectionsExpression
from .registryConfig import RegistryConfig


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
        Config.updateParameters(RegistryConfig, config, full, toCopy=(), overwrite=overwrite)

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
            # Try to instantiate a dimension configuration from the supplied
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

        cls = registryConfig.getRegistryClass()

        return cls(registryConfig, schemaConfig, dimensionConfig, create=create,
                   butlerRoot=butlerRoot)

    def __init__(self, registryConfig, schemaConfig=None, dimensionConfig=None, create=False,
                 butlerRoot=None):
        assert isinstance(registryConfig, RegistryConfig)
        self.config = registryConfig
        self.dimensions = DimensionUniverse(dimensionConfig)

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
        dataId : `dict` or `DataCoordinate`, optional
            A `dict`-like object containing the `Dimension` links that identify
            the dataset within a collection.
        kwds
            Additional keyword arguments passed to
            `DataCoordinate.standardize` to convert ``dataId`` to a true
            `DataCoordinate` or augment an existing one.

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
        dataId : `dict` or `DataCoordinate`
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
            Additional keyword arguments passed to
            `DataCoordinate.standardize` to convert ``dataId`` to a
            true `DataCoordinate` or augment an existing
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
        dataId : `DataCoordinate`, optional
            A `Dimension`-based identifier for the dataset within a
            collection, possibly containing additional metadata. This is used
            to short-circuit retrieving the dataId, so if provided, the
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
            Retrieve the run with the given integer ``id``.
        collection : `str`
            If given, lookup by ``collection`` name instead.

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
    def expandDataId(self, dataId: Optional[DataId] = None, *, graph: Optional[DimensionGraph] = None,
                     records: Optional[Mapping[DimensionElement, DimensionRecord]] = None, **kwds):
        """Expand a dimension-based data ID to include additional
        information.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def insertDimensionData(self, element: Union[DimensionElement, str],
                            *data: Union[dict, DimensionRecord],
                            conform: bool = True):
        """Insert one or more dimension records into the database.

        Parameters
        ----------
        element : `DimensionElement` or `str`
            The `DimensionElement` or name thereof that identifies the table
            records will be inserted into.
        data : `dict` or `DimensionRecord` (variadic)
            One or more records to insert.
        conform : `bool`, optional
            If `False` (`True` is default) perform no checking or conversions,
            and assume that ``element`` is a `DimensionElement` instance and
            ``data`` is a one or more `DimensionRecord` instances of the
            appropriate subclass.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def queryDimensions(self, dimensions: Iterable[Union[Dimension, str]], *,
                        dataId: Optional[DataId] = None,
                        datasets: Optional[Mapping[DatasetTypeExpression, CollectionsExpression]] = None,
                        where: Optional[str] = None,
                        expand: bool = True,
                        **kwds) -> Iterator[DataCoordinate]:
        """Query for and iterate over data IDs matching user-provided criteria.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` of `Dimension` or `str`
            The dimensions of the data IDs to yield, as either `Dimension`
            instances or `str`.  Will be automatically expanded to a complete
            `DimensionGraph`.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        datasets : `~collections.abc.Mapping`, optional
            Datasets whose existence in the registry constrain the set of data
            IDs returned.  This is a mapping from a dataset type expression
            (a `str` name, a true `DatasetType` instance, a `Like` pattern
            for the name, or ``...`` for all DatasetTypes) to a collections
            expression (a sequence of `str` or `Like` patterns, or `...` for
            all collections).
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.
        expand : `bool`, optional
            If `True` (default) yield `ExpandedDataCoordinate` instead of
            minimal `DataCoordinate` base-class instances.
        kwds
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Yields
        ------
        dataId : `DataCoordinate`
            Data IDs matching the given query parameters.  Order is
            unspecified.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def queryDatasets(self, datasetType: DatasetTypeExpression, *,
                      collections: CollectionsExpression,
                      dimensions: Optional[Iterable[Union[Dimension, str]]] = None,
                      dataId: Optional[DataId] = None,
                      where: Optional[str] = None,
                      deduplicate: bool = False,
                      expand: bool = True,
                      **kwds) -> Iterator[DatasetRef]:
        """Query for and iterate over dataset references matching user-provided
        criteria.

        Parameters
        ----------
        datasetType : `DatasetType`, `str`, `Like`, or ``...``
            An expression indicating type(s) of datasets to query for.
            ``...`` may be used to query for all known DatasetTypes.
            Multiple explicitly-provided dataset types cannot be queried in a
            single call to `queryDatasets` even though wildcard expressions
            can, because the results would be identical to chaining the
            iterators produced by multiple calls to `queryDatasets`.
        collections: `~collections.abc.Sequence` of `str` or `Like`, or ``...``
            An expression indicating the collections to be searched for
            datasets.  ``...`` may be passed to search all collections.
        dimensions : `~collections.abc.Iterable` of `Dimension` or `str`
            Dimensions to include in the query (in addition to those used
            to identify the queried dataset type(s)), either to constrain
            the resulting datasets to those for which a matching dimension
            exists, or to relate the dataset type's dimensions to dimensions
            referenced by the ``dataId`` or ``where`` arguments.
        dataId : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.
        deduplicate : `bool`, optional
            If `True` (`False` is default), for each result data ID, only
            yield one `DatasetRef` of each `DatasetType`, from the first
            collection in which a dataset of that dataset type appears
            (according to the order of ``collections`` passed in).  Cannot be
            used if any element in ``collections`` is an expression.
        expand : `bool`, optional
            If `True` (default) attach `ExpandedDataCoordinate` instead of
            minimal `DataCoordinate` base-class instances.
        kwds
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``dataId``
            argument (and may be used to provide a constraining data ID even
            when the ``dataId`` argument is `None`).

        Yields
        ------
        ref : `DatasetRef`
            Dataset references matching the given query criteria.  These
            are grouped by `DatasetType` if the query evaluates to multiple
            dataset types, but order is otherwise unspecified.

        Raises
        ------
        TypeError
            Raised when the arguments are incompatible, such as when a
            collection wildcard is pass when ``deduplicate`` is `True`.

        Notes
        -----
        When multiple dataset types are queried via a wildcard expression, the
        results of this operation are equivalent to querying for each dataset
        type separately in turn, and no information about the relationships
        between datasets of different types is included.  In contexts where
        that kind of information is important, the recommended pattern is to
        use `queryDimensions` to first obtain data IDs (possibly with the
        desired dataset types and collections passed as constraints to the
        query), and then use multiple (generally much simpler) calls to
        `queryDatasets` with the returned data IDs passed as constraints.
        """
        raise NotImplementedError("Must be implemented by subclass")
