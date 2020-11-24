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

"""
Butler top level classes.
"""
from __future__ import annotations

__all__ = (
    "Butler",
    "ButlerValidationError",
    "PruneCollectionsArgsError",
    "PurgeWithoutUnstorePruneCollectionsError",
    "RunWithoutPurgePruneCollectionsError",
    "PurgeUnsupportedPruneCollectionsError",
)


from collections import defaultdict
import contextlib
import logging
import numbers
import os
from typing import (
    Any,
    ClassVar,
    Counter,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    TextIO,
    Tuple,
    Type,
    Union,
)

try:
    import boto3
except ImportError:
    boto3 = None

from lsst.utils import doImport
from .core import (
    AmbiguousDatasetError,
    ButlerURI,
    Config,
    ConfigSubset,
    DataCoordinate,
    DataId,
    DataIdValue,
    DatasetRef,
    DatasetType,
    Datastore,
    Dimension,
    DimensionConfig,
    FileDataset,
    StorageClassFactory,
    Timespan,
    ValidationError,
)
from .core.repoRelocation import BUTLER_ROOT_TAG
from .core.utils import transactional, getClassOf
from ._deferredDatasetHandle import DeferredDatasetHandle
from ._butlerConfig import ButlerConfig
from .registry import Registry, RegistryConfig, CollectionType
from .registry.wildcards import CollectionSearch
from .transfers import RepoExportContext

log = logging.getLogger(__name__)


class ButlerValidationError(ValidationError):
    """There is a problem with the Butler configuration."""
    pass


class PruneCollectionsArgsError(TypeError):
    """Base class for errors relating to Butler.pruneCollections input
    arguments.
    """
    pass


class PurgeWithoutUnstorePruneCollectionsError(PruneCollectionsArgsError):
    """Raised when purge and unstore are both required to be True, and
    purge is True but unstore is False.
    """

    def __init__(self) -> None:
        super().__init__("Cannot pass purge=True without unstore=True.")


class RunWithoutPurgePruneCollectionsError(PruneCollectionsArgsError):
    """Raised when pruning a RUN collection but purge is False."""

    def __init__(self, collectionType: CollectionType):
        self.collectionType = collectionType
        super().__init__(f"Cannot prune RUN collection {self.collectionType.name} without purge=True.")


class PurgeUnsupportedPruneCollectionsError(PruneCollectionsArgsError):
    """Raised when purge is True but is not supported for the given
    collection."""

    def __init__(self, collectionType: CollectionType):
        self.collectionType = collectionType
        super().__init__(
            f"Cannot prune {self.collectionType} collection {self.collectionType.name} with purge=True.")


class Butler:
    """Main entry point for the data access system.

    Parameters
    ----------
    config : `ButlerConfig`, `Config` or `str`, optional.
        Configuration. Anything acceptable to the
        `ButlerConfig` constructor.  If a directory path
        is given the configuration will be read from a ``butler.yaml`` file in
        that location.  If `None` is given default values will be used.
    butler : `Butler`, optional.
        If provided, construct a new Butler that uses the same registry and
        datastore as the given one, but with the given collection and run.
        Incompatible with the ``config``, ``searchPaths``, and ``writeable``
        arguments.
    collections : `Any`, optional
        An expression specifying the collections to be searched (in order) when
        reading datasets, and optionally dataset type restrictions on them.
        This may be:
        - a `str` collection name;
        - a tuple of (collection name, *dataset type restriction*);
        - an iterable of either of the above;
        - a mapping from `str` to *dataset type restriction*.

        See :ref:`daf_butler_collection_expressions` for more information,
        including the definition of a *dataset type restriction*.  All
        collections must either already exist or be specified to be created
        by other arguments.
    run : `str`, optional
        Name of the run datasets should be output to.  If the run
        does not exist, it will be created.  If ``collections`` is `None`, it
        will be set to ``[run]``.  If this is not set (and ``writeable`` is
        not set either), a read-only butler will be created.
    tags : `Iterable` [ `str` ], optional
        A list of `~CollectionType.TAGGED` collections that datasets should be
        associated with in `put` or `ingest` and disassociated from in
        `pruneDatasets`.  If any of these collections does not exist, it will
        be created.
    chains : `Mapping` [ `str`, `Iterable` [ `str` ] ], optional
        A mapping from the names of new `~CollectionType.CHAINED` collections
        to an expression identifying their child collections (which takes the
        same form as the ``collections`` argument.  Chains may be nested only
        if children precede their parents in this mapping.
    searchPaths : `list` of `str`, optional
        Directory paths to search when calculating the full Butler
        configuration.  Not used if the supplied config is already a
        `ButlerConfig`.
    writeable : `bool`, optional
        Explicitly sets whether the butler supports write operations.  If not
        provided, a read-write butler is created if any of ``run``, ``tags``,
        or ``chains`` is non-empty.

    Examples
    --------
    While there are many ways to control exactly how a `Butler` interacts with
    the collections in its `Registry`, the most common cases are still simple.

    For a read-only `Butler` that searches one collection, do::

        butler = Butler("/path/to/repo", collections=["u/alice/DM-50000"])

    For a read-write `Butler` that writes to and reads from a
    `~CollectionType.RUN` collection::

        butler = Butler("/path/to/repo", run="u/alice/DM-50000/a")

    The `Butler` passed to a ``PipelineTask`` is often much more complex,
    because we want to write to one `~CollectionType.RUN` collection but read
    from several others (as well), while defining a new
    `~CollectionType.CHAINED` collection that combines them all::

        butler = Butler("/path/to/repo", run="u/alice/DM-50000/a",
                        collections=["u/alice/DM-50000"],
                        chains={
                            "u/alice/DM-50000": ["u/alice/DM-50000/a",
                                                 "u/bob/DM-49998",
                                                 "raw/hsc"]
                        })

    This butler will `put` new datasets to the run ``u/alice/DM-50000/a``, but
    they'll also be available from the chained collection ``u/alice/DM-50000``.
    Datasets will be read first from that run (since it appears first in the
    chain), and then from ``u/bob/DM-49998`` and finally ``raw/hsc``.
    If ``u/alice/DM-50000`` had already been defined, the ``chain`` argument
    would be unnecessary.  We could also construct a butler that performs
    exactly the same `put` and `get` operations without actually creating a
    chained collection, just by passing multiple items is ``collections``::

        butler = Butler("/path/to/repo", run="u/alice/DM-50000/a",
                        collections=["u/alice/DM-50000/a",
                                     "u/bob/DM-49998",
                                     "raw/hsc"])

    Finally, one can always create a `Butler` with no collections::

        butler = Butler("/path/to/repo", writeable=True)

    This can be extremely useful when you just want to use ``butler.registry``,
    e.g. for inserting dimension data or managing collections, or when the
    collections you want to use with the butler are not consistent.
    Passing ``writeable`` explicitly here is only necessary if you want to be
    able to make changes to the repo - usually the value for ``writeable`` is
    can be guessed from the collection arguments provided, but it defaults to
    `False` when there are not collection arguments.
    """
    def __init__(self, config: Union[Config, str, None] = None, *,
                 butler: Optional[Butler] = None,
                 collections: Any = None,
                 run: Optional[str] = None,
                 tags: Iterable[str] = (),
                 chains: Optional[Mapping[str, Any]] = None,
                 searchPaths: Optional[List[str]] = None,
                 writeable: Optional[bool] = None,
                 ):
        # Transform any single-pass iterator into an actual sequence so we
        # can see if its empty
        self.tags = tuple(tags)
        # Load registry, datastore, etc. from config or existing butler.
        if butler is not None:
            if config is not None or searchPaths is not None or writeable is not None:
                raise TypeError("Cannot pass 'config', 'searchPaths', or 'writeable' "
                                "arguments with 'butler' argument.")
            self.registry = butler.registry
            self.datastore = butler.datastore
            self.storageClasses = butler.storageClasses
            self._config: ButlerConfig = butler._config
        else:
            self._config = ButlerConfig(config, searchPaths=searchPaths)
            if "root" in self._config:
                butlerRoot = self._config["root"]
            else:
                butlerRoot = self._config.configDir
            if writeable is None:
                writeable = run is not None or chains is not None or bool(self.tags)
            self.registry = Registry.fromConfig(self._config, butlerRoot=butlerRoot, writeable=writeable)
            self.datastore = Datastore.fromConfig(self._config, self.registry.getDatastoreBridgeManager(),
                                                  butlerRoot=butlerRoot)
            self.storageClasses = StorageClassFactory()
            self.storageClasses.addFromConfig(self._config)
        # Check the many collection arguments for consistency and create any
        # needed collections that don't exist.
        if collections is None:
            if run is not None:
                collections = (run,)
            else:
                collections = ()
        self.collections = CollectionSearch.fromExpression(collections)
        if chains is None:
            chains = {}
        self.run = run
        if "run" in self._config or "collection" in self._config:
            raise ValueError("Passing a run or collection via configuration is no longer supported.")
        if self.run is not None:
            self.registry.registerCollection(self.run, type=CollectionType.RUN)
        for tag in self.tags:
            self.registry.registerCollection(tag, type=CollectionType.TAGGED)
        for parent, children in chains.items():
            self.registry.registerCollection(parent, type=CollectionType.CHAINED)
            self.registry.setCollectionChain(parent, children)

    GENERATION: ClassVar[int] = 3
    """This is a Generation 3 Butler.

    This attribute may be removed in the future, once the Generation 2 Butler
    interface has been fully retired; it should only be used in transitional
    code.
    """

    @staticmethod
    def makeRepo(root: str, config: Union[Config, str, None] = None,
                 dimensionConfig: Union[Config, str, None] = None, standalone: bool = False,
                 searchPaths: Optional[List[str]] = None, forceConfigRoot: bool = True,
                 outfile: Optional[str] = None, overwrite: bool = False) -> Config:
        """Create an empty data repository by adding a butler.yaml config
        to a repository root directory.

        Parameters
        ----------
        root : `str` or `ButlerURI`
            Path or URI to the root location of the new repository. Will be
            created if it does not exist.
        config : `Config` or `str`, optional
            Configuration to write to the repository, after setting any
            root-dependent Registry or Datastore config options.  Can not
            be a `ButlerConfig` or a `ConfigSubset`.  If `None`, default
            configuration will be used.  Root-dependent config options
            specified in this config are overwritten if ``forceConfigRoot``
            is `True`.
        dimensionConfig : `Config` or `str`, optional
            Configuration for dimensions, will be used to initialize registry
            database.
        standalone : `bool`
            If True, write all expanded defaults, not just customized or
            repository-specific settings.
            This (mostly) decouples the repository from the default
            configuration, insulating it from changes to the defaults (which
            may be good or bad, depending on the nature of the changes).
            Future *additions* to the defaults will still be picked up when
            initializing `Butlers` to repos created with ``standalone=True``.
        searchPaths : `list` of `str`, optional
            Directory paths to search when calculating the full butler
            configuration.
        forceConfigRoot : `bool`, optional
            If `False`, any values present in the supplied ``config`` that
            would normally be reset are not overridden and will appear
            directly in the output config.  This allows non-standard overrides
            of the root directory for a datastore or registry to be given.
            If this parameter is `True` the values for ``root`` will be
            forced into the resulting config if appropriate.
        outfile : `str`, optional
            If not-`None`, the output configuration will be written to this
            location rather than into the repository itself. Can be a URI
            string.  Can refer to a directory that will be used to write
            ``butler.yaml``.
        overwrite : `bool`, optional
            Create a new configuration file even if one already exists
            in the specified output location. Default is to raise
            an exception.

        Returns
        -------
        config : `Config`
            The updated `Config` instance written to the repo.

        Raises
        ------
        ValueError
            Raised if a ButlerConfig or ConfigSubset is passed instead of a
            regular Config (as these subclasses would make it impossible to
            support ``standalone=False``).
        FileExistsError
            Raised if the output config file already exists.
        os.error
            Raised if the directory does not exist, exists but is not a
            directory, or cannot be created.

        Notes
        -----
        Note that when ``standalone=False`` (the default), the configuration
        search path (see `ConfigSubset.defaultSearchPaths`) that was used to
        construct the repository should also be used to construct any Butlers
        to avoid configuration inconsistencies.
        """
        if isinstance(config, (ButlerConfig, ConfigSubset)):
            raise ValueError("makeRepo must be passed a regular Config without defaults applied.")

        # Ensure that the root of the repository exists or can be made
        uri = ButlerURI(root, forceDirectory=True)
        uri.mkdir()

        config = Config(config)

        # If we are creating a new repo from scratch with relative roots,
        # do not propagate an explicit root from the config file
        if "root" in config:
            del config["root"]

        full = ButlerConfig(config, searchPaths=searchPaths)  # this applies defaults
        datastoreClass: Type[Datastore] = doImport(full["datastore", "cls"])
        datastoreClass.setConfigRoot(BUTLER_ROOT_TAG, config, full, overwrite=forceConfigRoot)

        # if key exists in given config, parse it, otherwise parse the defaults
        # in the expanded config
        if config.get(("registry", "db")):
            registryConfig = RegistryConfig(config)
        else:
            registryConfig = RegistryConfig(full)
        defaultDatabaseUri = registryConfig.makeDefaultDatabaseUri(BUTLER_ROOT_TAG)
        if defaultDatabaseUri is not None:
            Config.updateParameters(RegistryConfig, config, full,
                                    toUpdate={"db": defaultDatabaseUri},
                                    overwrite=forceConfigRoot)
        else:
            Config.updateParameters(RegistryConfig, config, full, toCopy=("db",),
                                    overwrite=forceConfigRoot)

        if standalone:
            config.merge(full)
        else:
            # Always expand the registry.managers section into the per-repo
            # config, because after the database schema is created, it's not
            # allowed to change anymore.  Note that in the standalone=True
            # branch, _everything_ in the config is expanded, so there's no
            # need to special case this.
            Config.updateParameters(RegistryConfig, config, full, toCopy=("managers",), overwrite=False)
        configURI: Union[str, ButlerURI]
        if outfile is not None:
            # When writing to a separate location we must include
            # the root of the butler repo in the config else it won't know
            # where to look.
            config["root"] = uri.geturl()
            configURI = outfile
        else:
            configURI = uri
        config.dumpToUri(configURI, overwrite=overwrite)

        # Create Registry and populate tables
        registryConfig = RegistryConfig(config.get("registry"))
        dimensionConfig = DimensionConfig(dimensionConfig)
        Registry.createFromConfig(registryConfig, dimensionConfig=dimensionConfig, butlerRoot=root)

        return config

    @classmethod
    def _unpickle(cls, config: ButlerConfig, collections: Optional[CollectionSearch], run: Optional[str],
                  tags: Tuple[str, ...], writeable: bool) -> Butler:
        """Callable used to unpickle a Butler.

        We prefer not to use ``Butler.__init__`` directly so we can force some
        of its many arguments to be keyword-only (note that ``__reduce__``
        can only invoke callables with positional arguments).

        Parameters
        ----------
        config : `ButlerConfig`
            Butler configuration, already coerced into a true `ButlerConfig`
            instance (and hence after any search paths for overrides have been
            utilized).
        collections : `CollectionSearch`
            Names of collections to read from.
        run : `str`, optional
            Name of `~CollectionType.RUN` collection to write to.
        tags : `tuple` [`str`]
            Names of `~CollectionType.TAGGED` collections to associate with.
        writeable : `bool`
            Whether the Butler should support write operations.

        Returns
        -------
        butler : `Butler`
            A new `Butler` instance.
        """
        return cls(config=config, collections=collections, run=run, tags=tags, writeable=writeable)

    def __reduce__(self) -> tuple:
        """Support pickling.
        """
        return (Butler._unpickle, (self._config, self.collections, self.run, self.tags,
                                   self.registry.isWriteable()))

    def __str__(self) -> str:
        return "Butler(collections={}, run={}, tags={}, datastore='{}', registry='{}')".format(
            self.collections, self.run, self.tags, self.datastore, self.registry)

    def isWriteable(self) -> bool:
        """Return `True` if this `Butler` supports write operations.
        """
        return self.registry.isWriteable()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Context manager supporting `Butler` transactions.

        Transactions can be nested.
        """
        with self.registry.transaction():
            with self.datastore.transaction():
                yield

    def _standardizeArgs(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
                         dataId: Optional[DataId] = None, **kwds: Any
                         ) -> Tuple[DatasetType, Optional[DataId]]:
        """Standardize the arguments passed to several Butler APIs.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the second argument.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        datasetType : `DatasetType`
            A `DatasetType` instance extracted from ``datasetRefOrType``.
        dataId : `dict` or `DataId`, optional
            Argument that can be used (along with ``kwds``) to construct a
            `DataId`.

        Notes
        -----
        Butler APIs that conceptually need a DatasetRef also allow passing a
        `DatasetType` (or the name of one) and a `DataId` (or a dict and
        keyword arguments that can be used to construct one) separately. This
        method accepts those arguments and always returns a true `DatasetType`
        and a `DataId` or `dict`.

        Standardization of `dict` vs `DataId` is best handled by passing the
        returned ``dataId`` (and ``kwds``) to `Registry` APIs, which are
        generally similarly flexible.
        """
        externalDatasetType: Optional[DatasetType] = None
        internalDatasetType: Optional[DatasetType] = None
        if isinstance(datasetRefOrType, DatasetRef):
            if dataId is not None or kwds:
                raise ValueError("DatasetRef given, cannot use dataId as well")
            externalDatasetType = datasetRefOrType.datasetType
            dataId = datasetRefOrType.dataId
        else:
            # Don't check whether DataId is provided, because Registry APIs
            # can usually construct a better error message when it wasn't.
            if isinstance(datasetRefOrType, DatasetType):
                externalDatasetType = datasetRefOrType
            else:
                internalDatasetType = self.registry.getDatasetType(datasetRefOrType)

        # Check that they are self-consistent
        if externalDatasetType is not None:
            internalDatasetType = self.registry.getDatasetType(externalDatasetType.name)
            if externalDatasetType != internalDatasetType:
                raise ValueError(f"Supplied dataset type ({externalDatasetType}) inconsistent with "
                                 f"registry definition ({internalDatasetType})")

        assert internalDatasetType is not None
        return internalDatasetType, dataId

    def _findDatasetRef(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
                        dataId: Optional[DataId] = None, *,
                        collections: Any = None,
                        allowUnresolved: bool = False,
                        **kwds: Any) -> DatasetRef:
        """Shared logic for methods that start with a search for a dataset in
        the registry.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`, optional
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        allowUnresolved : `bool`, optional
            If `True`, return an unresolved `DatasetRef` if finding a resolved
            one in the `Registry` fails.  Defaults to `False`.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Returns
        -------
        ref : `DatasetRef`
            A reference to the dataset identified by the given arguments.

        Raises
        ------
        LookupError
            Raised if no matching dataset exists in the `Registry` (and
            ``allowUnresolved is False``).
        ValueError
            Raised if a resolved `DatasetRef` was passed as an input, but it
            differs from the one found in the registry.
        TypeError
            Raised if no collections were provided.
        """
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        if isinstance(datasetRefOrType, DatasetRef):
            idNumber = datasetRefOrType.id
        else:
            idNumber = None
        timespan: Optional[Timespan] = None

        # Process dimension records that are using record information
        # rather than ids
        newDataId: Dict[str, DataIdValue] = {}
        byRecord: Dict[str, Dict[str, Any]] = defaultdict(dict)

        # if all the dataId comes from keyword parameters we do not need
        # to do anything here because they can't be of the form
        # exposure.obs_id because a "." is not allowed in a keyword parameter.
        if dataId:
            for k, v in dataId.items():
                # If we have a Dimension we do not need to do anything
                # because it cannot be a compound key.
                if isinstance(k, str) and "." in k:
                    # Someone is using a more human-readable dataId
                    dimensionName, record = k.split(".", 1)
                    byRecord[dimensionName][record] = v
                elif isinstance(k, Dimension):
                    newDataId[k.name] = v
                else:
                    newDataId[k] = v

        # Go through the updated dataId and check the type in case someone is
        # using an alternate key.  We have already filtered out the compound
        # keys dimensions.record format.
        not_dimensions = {}

        # Will need to look in the dataId and the keyword arguments
        # and will remove them if they need to be fixed or are unrecognized.
        for dataIdDict in (newDataId, kwds):
            # Use a list so we can adjust the dict safely in the loop
            for dimensionName in list(dataIdDict):
                value = dataIdDict[dimensionName]
                try:
                    dimension = self.registry.dimensions.getStaticDimensions()[dimensionName]
                except KeyError:
                    # This is not a real dimension
                    not_dimensions[dimensionName] = value
                    del dataIdDict[dimensionName]
                    continue

                # Convert an integral type to an explicit int to simplify
                # comparisons here
                if isinstance(value, numbers.Integral):
                    value = int(value)

                if not isinstance(value, dimension.primaryKey.getPythonType()):
                    for alternate in dimension.alternateKeys:
                        if isinstance(value, alternate.getPythonType()):
                            byRecord[dimensionName][alternate.name] = value
                            del dataIdDict[dimensionName]
                            log.debug("Converting dimension %s to %s.%s=%s",
                                      dimensionName, dimensionName, alternate.name, value)
                            break
                    else:
                        log.warning("Type mismatch found for value '%r' provided for dimension %s. "
                                    "Could not find matching alternative (primary key has type %s) "
                                    "so attempting to use as-is.",
                                    value, dimensionName, dimension.primaryKey.getPythonType())

        # If we have some unrecognized dimensions we have to try to connect
        # them to records in other dimensions.  This is made more complicated
        # by some dimensions having records with clashing names.  A mitigation
        # is that we can tell by this point which dimensions are missing
        # for the DatasetType but this does not work for calibrations
        # where additional dimensions can be used to constrain the temporal
        # axis.
        if not_dimensions:
            # Calculate missing dimensions
            provided = set(newDataId) | set(kwds) | set(byRecord)
            missingDimensions = datasetType.dimensions.names - provided

            # For calibrations we may well be needing temporal dimensions
            # so rather than always including all dimensions in the scan
            # restrict things a little. It is still possible for there
            # to be confusion over day_obs in visit vs exposure for example.
            # If we are not searching calibration collections things may
            # fail but they are going to fail anyway because of the
            # ambiguousness of the dataId...
            candidateDimensions: Set[str] = set()
            candidateDimensions.update(missingDimensions)
            if datasetType.isCalibration():
                for dim in self.registry.dimensions.getStaticDimensions():
                    if dim.temporal:
                        candidateDimensions.add(str(dim))

            # Look up table for the first association with a dimension
            guessedAssociation: Dict[str, Dict[str, Any]] = defaultdict(dict)

            # Keep track of whether an item is associated with multiple
            # dimensions.
            counter: Counter[str] = Counter()
            assigned: Dict[str, Set[str]] = defaultdict(set)

            # Go through the missing dimensions and associate the
            # given names with records within those dimensions
            for dimensionName in candidateDimensions:
                dimension = self.registry.dimensions.getStaticDimensions()[dimensionName]
                fields = dimension.metadata.names | dimension.uniqueKeys.names
                for field in not_dimensions:
                    if field in fields:
                        guessedAssociation[dimensionName][field] = not_dimensions[field]
                        counter[dimensionName] += 1
                        assigned[field].add(dimensionName)

            # There is a chance we have allocated a single dataId item
            # to multiple dimensions. Need to decide which should be retained.
            # For now assume that the most popular alternative wins.
            # This means that day_obs with seq_num will result in
            # exposure.day_obs and not visit.day_obs
            # Also prefer an explicitly missing dimension over an inferred
            # temporal dimension.
            for fieldName, assignedDimensions in assigned.items():
                if len(assignedDimensions) > 1:
                    # Pick the most popular (preferring mandatory dimensions)
                    requiredButMissing = assignedDimensions.intersection(missingDimensions)
                    if requiredButMissing:
                        candidateDimensions = requiredButMissing
                    else:
                        candidateDimensions = assignedDimensions

                    # Select the relevant items and get a new restricted
                    # counter.
                    theseCounts = {k: v for k, v in counter.items() if k in candidateDimensions}
                    duplicatesCounter: Counter[str] = Counter()
                    duplicatesCounter.update(theseCounts)

                    # Choose the most common. If they are equally common
                    # we will pick the one that was found first.
                    # Returns a list of tuples
                    selected = duplicatesCounter.most_common(1)[0][0]

                    log.debug("Ambiguous dataId entry '%s' associated with multiple dimensions: %s."
                              " Removed ambiguity by choosing dimension %s.",
                              fieldName, ", ".join(assignedDimensions), selected)

                    for candidateDimension in assignedDimensions:
                        if candidateDimension != selected:
                            del guessedAssociation[candidateDimension][fieldName]

            # Update the record look up dict with the new associations
            for dimensionName, values in guessedAssociation.items():
                if values:  # A dict might now be empty
                    log.debug("Assigned non-dimension dataId keys to dimension %s: %s",
                              dimensionName, values)
                    byRecord[dimensionName].update(values)

        if byRecord:
            # Some record specifiers were found so we need to convert
            # them to the Id form
            for dimensionName, values in byRecord.items():
                if dimensionName in newDataId:
                    log.warning("DataId specified explicit %s dimension value of %s in addition to"
                                " general record specifiers for it of %s.  Ignoring record information.",
                                dimensionName, newDataId[dimensionName], str(values))
                    continue

                # Build up a WHERE expression -- use single quotes
                def quote(s: Any) -> str:
                    if isinstance(s, str):
                        return f"'{s}'"
                    else:
                        return s

                where = " AND ".join(f"{dimensionName}.{k} = {quote(v)}"
                                     for k, v in values.items())

                # Hopefully we get a single record that matches
                records = set(self.registry.queryDimensionRecords(dimensionName, dataId=newDataId,
                                                                  where=where, **kwds))

                if len(records) != 1:
                    if len(records) > 1:
                        log.debug("Received %d records from constraints of %s", len(records), str(values))
                        for r in records:
                            log.debug("- %s", str(r))
                        raise RuntimeError(f"DataId specification for dimension {dimensionName} is not"
                                           f" uniquely constrained to a single dataset by {values}."
                                           f" Got {len(records)} results.")
                    raise RuntimeError(f"DataId specification for dimension {dimensionName} matched no"
                                       f" records when constrained by {values}")

                # Get the primary key from the real dimension object
                dimension = self.registry.dimensions.getStaticDimensions()[dimensionName]
                if not isinstance(dimension, Dimension):
                    raise RuntimeError(
                        f"{dimension.name} is not a true dimension, and cannot be used in data IDs."
                    )
                newDataId[dimensionName] = getattr(records.pop(), dimension.primaryKey.name)

            # We have modified the dataId so need to switch to it
            dataId = newDataId

        if datasetType.isCalibration():
            # Because this is a calibration dataset, first try to make a
            # standardize the data ID without restricting the dimensions to
            # those of the dataset type requested, because there may be extra
            # dimensions that provide temporal information for a validity-range
            # lookup.
            dataId = DataCoordinate.standardize(dataId, universe=self.registry.dimensions, **kwds)
            if dataId.graph.temporal:
                dataId = self.registry.expandDataId(dataId)
                timespan = dataId.timespan
        else:
            # Standardize the data ID to just the dimensions of the dataset
            # type instead of letting registry.findDataset do it, so we get the
            # result even if no dataset is found.
            dataId = DataCoordinate.standardize(dataId, graph=datasetType.dimensions, **kwds)
        if collections is None:
            collections = self.collections
            if not collections:
                raise TypeError("No input collections provided.")
        else:
            collections = CollectionSearch.fromExpression(collections)
        # Always lookup the DatasetRef, even if one is given, to ensure it is
        # present in the current collection.
        ref = self.registry.findDataset(datasetType, dataId, collections=collections, timespan=timespan)
        if ref is None:
            if allowUnresolved:
                return DatasetRef(datasetType, dataId)
            else:
                raise LookupError(f"Dataset {datasetType.name} with data ID {dataId} "
                                  f"could not be found in collections {collections}.")
        if idNumber is not None and idNumber != ref.id:
            raise ValueError(f"DatasetRef.id provided ({idNumber}) does not match "
                             f"id ({ref.id}) in registry in collections {collections}.")
        return ref

    @transactional
    def put(self, obj: Any, datasetRefOrType: Union[DatasetRef, DatasetType, str],
            dataId: Optional[DataId] = None, *,
            run: Optional[str] = None,
            tags: Optional[Iterable[str]] = None,
            **kwds: Any) -> DatasetRef:
        """Store and register a dataset.

        Parameters
        ----------
        obj : `object`
            The dataset.
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` is provided, ``dataId`` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the second argument.
        run : `str`, optional
            The name of the run the dataset should be added to, overriding
            ``self.run``.
        tags : `Iterable` [ `str` ], optional
            The names of a `~CollectionType.TAGGED` collections to associate
            the dataset with, overriding ``self.tags``.  These collections
            must have already been added to the `Registry`.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        ref : `DatasetRef`
            A reference to the stored dataset, updated with the correct id if
            given.

        Raises
        ------
        TypeError
            Raised if the butler is read-only or if no run has been provided.
        """
        log.debug("Butler put: %s, dataId=%s, run=%s", datasetRefOrType, dataId, run)
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        if isinstance(datasetRefOrType, DatasetRef) and datasetRefOrType.id is not None:
            raise ValueError("DatasetRef must not be in registry, must have None id")

        if run is None:
            if self.run is None:
                raise TypeError("No run provided.")
            run = self.run
            # No need to check type for run; first thing we do is
            # insertDatasets, and that will check for us.

        if tags is None:
            tags = self.tags
        else:
            tags = tuple(tags)
        for tag in tags:
            # Check that these are tagged collections up front, because we want
            # to avoid relying on Datastore transactionality to avoid modifying
            # the repo if there's an error later.
            collectionType = self.registry.getCollectionType(tag)
            if collectionType is not CollectionType.TAGGED:
                raise TypeError(f"Cannot associate into collection '{tag}' of non-TAGGED type "
                                f"{collectionType.name}.")

        # Add Registry Dataset entry.
        dataId = self.registry.expandDataId(dataId, graph=datasetType.dimensions, **kwds)
        ref, = self.registry.insertDatasets(datasetType, run=run, dataIds=[dataId])

        # Add Datastore entry.
        self.datastore.put(obj, ref)

        for tag in tags:
            self.registry.associate(tag, [ref])

        return ref

    def getDirect(self, ref: DatasetRef, *, parameters: Optional[Dict[str, Any]] = None) -> Any:
        """Retrieve a stored dataset.

        Unlike `Butler.get`, this method allows datasets outside the Butler's
        collection to be read as long as the `DatasetRef` that identifies them
        can be obtained separately.

        Parameters
        ----------
        ref : `DatasetRef`
            Resolved reference to an already stored dataset.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.

        Returns
        -------
        obj : `object`
            The dataset.
        """
        return self.datastore.get(ref, parameters=parameters)

    def getDirectDeferred(self, ref: DatasetRef, *,
                          parameters: Union[dict, None] = None) -> DeferredDatasetHandle:
        """Create a `DeferredDatasetHandle` which can later retrieve a dataset,
        from a resolved `DatasetRef`.

        Parameters
        ----------
        ref : `DatasetRef`
            Resolved reference to an already stored dataset.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.

        Returns
        -------
        obj : `DeferredDatasetHandle`
            A handle which can be used to retrieve a dataset at a later time.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id is None``, i.e. the reference is unresolved.
        """
        if ref.id is None:
            raise AmbiguousDatasetError(
                f"Dataset of type {ref.datasetType.name} with data ID {ref.dataId} is not resolved."
            )
        return DeferredDatasetHandle(butler=self, ref=ref, parameters=parameters)

    def getDeferred(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
                    dataId: Optional[DataId] = None, *,
                    parameters: Union[dict, None] = None,
                    collections: Any = None,
                    **kwds: Any) -> DeferredDatasetHandle:
        """Create a `DeferredDatasetHandle` which can later retrieve a dataset,
        after an immediate registry lookup.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`, optional
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Returns
        -------
        obj : `DeferredDatasetHandle`
            A handle which can be used to retrieve a dataset at a later time.

        Raises
        ------
        LookupError
            Raised if no matching dataset exists in the `Registry` (and
            ``allowUnresolved is False``).
        ValueError
            Raised if a resolved `DatasetRef` was passed as an input, but it
            differs from the one found in the registry.
        TypeError
            Raised if no collections were provided.
        """
        ref = self._findDatasetRef(datasetRefOrType, dataId, collections=collections, **kwds)
        return DeferredDatasetHandle(butler=self, ref=ref, parameters=parameters)

    def get(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
            dataId: Optional[DataId] = None, *,
            parameters: Optional[Dict[str, Any]] = None,
            collections: Any = None,
            **kwds: Any) -> Any:
        """Retrieve a stored dataset.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        obj : `object`
            The dataset.

        Raises
        ------
        ValueError
            Raised if a resolved `DatasetRef` was passed as an input, but it
            differs from the one found in the registry.
        LookupError
            Raised if no matching dataset exists in the `Registry`.
        TypeError
            Raised if no collections were provided.

        Notes
        -----
        When looking up datasets in a `~CollectionType.CALIBRATION` collection,
        this method requires that the given data ID include temporal dimensions
        beyond the dimensions of the dataset type itself, in order to find the
        dataset with the appropriate validity range.  For example, a "bias"
        dataset with native dimensions ``{instrument, detector}`` could be
        fetched with a ``{instrument, detector, exposure}`` data ID, because
        ``exposure`` is a temporal dimension.
        """
        log.debug("Butler get: %s, dataId=%s, parameters=%s", datasetRefOrType, dataId, parameters)
        ref = self._findDatasetRef(datasetRefOrType, dataId, collections=collections, **kwds)
        return self.getDirect(ref, parameters=parameters)

    def getURIs(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
                dataId: Optional[DataId] = None, *,
                predict: bool = False,
                collections: Any = None,
                run: Optional[str] = None,
                **kwds: Any) -> Tuple[Optional[ButlerURI], Dict[str, ButlerURI]]:
        """Returns the URIs associated with the dataset.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        predict : `bool`
            If `True`, allow URIs to be returned of datasets that have not
            been written.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        run : `str`, optional
            Run to use for predictions, overriding ``self.run``.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        primary : `ButlerURI`
            The URI to the primary artifact associated with this dataset.
            If the dataset was disassembled within the datastore this
            may be `None`.
        components : `dict`
            URIs to any components associated with the dataset artifact.
            Can be empty if there are no components.
        """
        ref = self._findDatasetRef(datasetRefOrType, dataId, allowUnresolved=predict,
                                   collections=collections, **kwds)
        if ref.id is None:  # only possible if predict is True
            if run is None:
                run = self.run
                if run is None:
                    raise TypeError("Cannot predict location with run=None.")
            # Lie about ID, because we can't guess it, and only
            # Datastore.getURIs() will ever see it (and it doesn't use it).
            ref = ref.resolved(id=0, run=run)
        return self.datastore.getURIs(ref, predict)

    def getURI(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
               dataId: Optional[DataId] = None, *,
               predict: bool = False,
               collections: Any = None,
               run: Optional[str] = None,
               **kwds: Any) -> ButlerURI:
        """Return the URI to the Dataset.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        predict : `bool`
            If `True`, allow URIs to be returned of datasets that have not
            been written.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        run : `str`, optional
            Run to use for predictions, overriding ``self.run``.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        uri : `ButlerURI`
            URI pointing to the Dataset within the datastore. If the
            Dataset does not exist in the datastore, and if ``predict`` is
            `True`, the URI will be a prediction and will include a URI
            fragment "#predicted".
            If the datastore does not have entities that relate well
            to the concept of a URI the returned URI string will be
            descriptive. The returned URI is not guaranteed to be obtainable.

        Raises
        ------
        LookupError
            A URI has been requested for a dataset that does not exist and
            guessing is not allowed.
        ValueError
            Raised if a resolved `DatasetRef` was passed as an input, but it
            differs from the one found in the registry.
        TypeError
            Raised if no collections were provided.
        RuntimeError
            Raised if a URI is requested for a dataset that consists of
            multiple artifacts.
        """
        primary, components = self.getURIs(datasetRefOrType, dataId=dataId, predict=predict,
                                           collections=collections, run=run, **kwds)

        if primary is None or components:
            raise RuntimeError(f"Dataset ({datasetRefOrType}) includes distinct URIs for components. "
                               "Use Butler.getURIs() instead.")
        return primary

    def datasetExists(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
                      dataId: Optional[DataId] = None, *,
                      collections: Any = None,
                      **kwds: Any) -> bool:
        """Return True if the Dataset is actually present in the Datastore.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Raises
        ------
        LookupError
            Raised if the dataset is not even present in the Registry.
        ValueError
            Raised if a resolved `DatasetRef` was passed as an input, but it
            differs from the one found in the registry.
        TypeError
            Raised if no collections were provided.
        """
        ref = self._findDatasetRef(datasetRefOrType, dataId, collections=collections, **kwds)
        return self.datastore.exists(ref)

    def pruneCollection(self, name: str, purge: bool = False, unstore: bool = False) -> None:
        """Remove a collection and possibly prune datasets within it.

        Parameters
        ----------
        name : `str`
            Name of the collection to remove.  If this is a
            `~CollectionType.TAGGED` or `~CollectionType.CHAINED` collection,
            datasets within the collection are not modified unless ``unstore``
            is `True`.  If this is a `~CollectionType.RUN` collection,
            ``purge`` and ``unstore`` must be `True`, and all datasets in it
            are fully removed from the data repository.
        purge : `bool`, optional
            If `True`, permit `~CollectionType.RUN` collections to be removed,
            fully removing datasets within them.  Requires ``unstore=True`` as
            well as an added precaution against accidental deletion.  Must be
            `False` (default) if the collection is not a ``RUN``.
        unstore: `bool`, optional
            If `True`, remove all datasets in the collection from all
            datastores in which they appear.

        Raises
        ------
        TypeError
            Raised if the butler is read-only or arguments are mutually
            inconsistent.
        """

        # See pruneDatasets comments for more information about the logic here;
        # the cases are almost the same, but here we can rely on Registry to
        # take care everything but Datastore deletion when we remove the
        # collection.
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        collectionType = self.registry.getCollectionType(name)
        if purge and not unstore:
            raise PurgeWithoutUnstorePruneCollectionsError()
        if collectionType is CollectionType.RUN and not purge:
            raise RunWithoutPurgePruneCollectionsError(collectionType)
        if collectionType is not CollectionType.RUN and purge:
            raise PurgeUnsupportedPruneCollectionsError(collectionType)

        with self.registry.transaction():
            if unstore:
                for ref in self.registry.queryDatasets(..., collections=name, findFirst=True):
                    if self.datastore.exists(ref):
                        self.datastore.trash(ref)
            self.registry.removeCollection(name)
        if unstore:
            # Point of no return for removing artifacts
            self.datastore.emptyTrash()

    def pruneDatasets(self, refs: Iterable[DatasetRef], *,
                      disassociate: bool = True,
                      unstore: bool = False,
                      tags: Optional[Iterable[str]] = None,
                      purge: bool = False,
                      run: Optional[str] = None) -> None:
        """Remove one or more datasets from a collection and/or storage.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` of `DatasetRef`
            Datasets to prune.  These must be "resolved" references (not just
            a `DatasetType` and data ID).
        disassociate : `bool`, optional
            Disassociate pruned datasets from ``self.tags`` (or the collections
            given via the ``tags`` argument).
        unstore : `bool`, optional
            If `True` (`False` is default) remove these datasets from all
            datastores known to this butler.  Note that this will make it
            impossible to retrieve these datasets even via other collections.
            Datasets that are already not stored are ignored by this option.
        tags : `Iterable` [ `str` ], optional
            `~CollectionType.TAGGED` collections to disassociate the datasets
            from, overriding ``self.tags``.  Ignored if ``disassociate`` is
            `False` or ``purge`` is `True`.
        purge : `bool`, optional
            If `True` (`False` is default), completely remove the dataset from
            the `Registry`.  To prevent accidental deletions, ``purge`` may
            only be `True` if all of the following conditions are met:

             - All given datasets are in the given run.
             - ``disassociate`` is `True`;
             - ``unstore`` is `True`.

            This mode may remove provenance information from datasets other
            than those provided, and should be used with extreme care.
        run : `str`, optional
            `~CollectionType.RUN` collection to purge from, overriding
            ``self.run``.  Ignored unless ``purge`` is `True`.

        Raises
        ------
        TypeError
            Raised if the butler is read-only, if no collection was provided,
            or the conditions for ``purge=True`` were not met.
        """
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        if purge:
            if not disassociate:
                raise TypeError("Cannot pass purge=True without disassociate=True.")
            if not unstore:
                raise TypeError("Cannot pass purge=True without unstore=True.")
            if run is None:
                run = self.run
                if run is None:
                    raise TypeError("No run provided but purge=True.")
            collectionType = self.registry.getCollectionType(run)
            if collectionType is not CollectionType.RUN:
                raise TypeError(f"Cannot purge from collection '{run}' "
                                f"of non-RUN type {collectionType.name}.")
        elif disassociate:
            if tags is None:
                tags = self.tags
            else:
                tags = tuple(tags)
            if not tags:
                raise TypeError("No tags provided but disassociate=True.")
            for tag in tags:
                collectionType = self.registry.getCollectionType(tag)
                if collectionType is not CollectionType.TAGGED:
                    raise TypeError(f"Cannot disassociate from collection '{tag}' "
                                    f"of non-TAGGED type {collectionType.name}.")
        # Transform possibly-single-pass iterable into something we can iterate
        # over multiple times.
        refs = list(refs)
        # Pruning a component of a DatasetRef makes no sense since registry
        # doesn't know about components and datastore might not store
        # components in a separate file
        for ref in refs:
            if ref.datasetType.component():
                raise ValueError(f"Can not prune a component of a dataset (ref={ref})")
        # We don't need an unreliable Datastore transaction for this, because
        # we've been extra careful to ensure that Datastore.trash only involves
        # mutating the Registry (it can _look_ at Datastore-specific things,
        # but shouldn't change them), and hence all operations here are
        # Registry operations.
        with self.registry.transaction():
            if unstore:
                for ref in refs:
                    # There is a difference between a concrete composite
                    # and virtual composite. In a virtual composite the
                    # datastore is never given the top level DatasetRef. In
                    # the concrete composite the datastore knows all the
                    # refs and will clean up itself if asked to remove the
                    # parent ref.  We can not check configuration for this
                    # since we can not trust that the configuration is the
                    # same. We therefore have to ask if the ref exists or
                    # not.  This is consistent with the fact that we want
                    # to ignore already-removed-from-datastore datasets
                    # anyway.
                    if self.datastore.exists(ref):
                        self.datastore.trash(ref)
            if purge:
                self.registry.removeDatasets(refs)
            elif disassociate:
                assert tags, "Guaranteed by earlier logic in this function."
                for tag in tags:
                    self.registry.disassociate(tag, refs)
        # We've exited the Registry transaction, and apparently committed.
        # (if there was an exception, everything rolled back, and it's as if
        # nothing happened - and we never get here).
        # Datastore artifacts are not yet gone, but they're clearly marked
        # as trash, so if we fail to delete now because of (e.g.) filesystem
        # problems we can try again later, and if manual administrative
        # intervention is required, it's pretty clear what that should entail:
        # deleting everything on disk and in private Datastore tables that is
        # in the dataset_location_trash table.
        if unstore:
            # Point of no return for removing artifacts
            self.datastore.emptyTrash()

    @transactional
    def ingest(self, *datasets: FileDataset, transfer: Optional[str] = "auto", run: Optional[str] = None,
               tags: Optional[Iterable[str]] = None,) -> None:
        """Store and register one or more datasets that already exist on disk.

        Parameters
        ----------
        datasets : `FileDataset`
            Each positional argument is a struct containing information about
            a file to be ingested, including its path (either absolute or
            relative to the datastore root, if applicable), a `DatasetRef`,
            and optionally a formatter class or its fully-qualified string
            name.  If a formatter is not provided, the formatter that would be
            used for `put` is assumed.  On successful return, all
            `FileDataset.ref` attributes will have their `DatasetRef.id`
            attribute populated and all `FileDataset.formatter` attributes will
            be set to the formatter class used.  `FileDataset.path` attributes
            may be modified to put paths in whatever the datastore considers a
            standardized form.
        transfer : `str`, optional
            If not `None`, must be one of 'auto', 'move', 'copy', 'direct',
            'hardlink', 'relsymlink' or 'symlink', indicating how to transfer
            the file.
        run : `str`, optional
            The name of the run ingested datasets should be added to,
            overriding ``self.run``.
        tags : `Iterable` [ `str` ], optional
            The names of a `~CollectionType.TAGGED` collections to associate
            the dataset with, overriding ``self.tags``.  These collections
            must have already been added to the `Registry`.

        Raises
        ------
        TypeError
            Raised if the butler is read-only or if no run was provided.
        NotImplementedError
            Raised if the `Datastore` does not support the given transfer mode.
        DatasetTypeNotSupportedError
            Raised if one or more files to be ingested have a dataset type that
            is not supported by the `Datastore`..
        FileNotFoundError
            Raised if one of the given files does not exist.
        FileExistsError
            Raised if transfer is not `None` but the (internal) location the
            file would be moved to is already occupied.

        Notes
        -----
        This operation is not fully exception safe: if a database operation
        fails, the given `FileDataset` instances may be only partially updated.

        It is atomic in terms of database operations (they will either all
        succeed or all fail) providing the database engine implements
        transactions correctly.  It will attempt to be atomic in terms of
        filesystem operations as well, but this cannot be implemented
        rigorously for most datastores.
        """
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        if run is None:
            if self.run is None:
                raise TypeError("No run provided.")
            run = self.run
            # No need to check run type, since insertDatasets will do that
            # (safely) for us.
        if tags is None:
            tags = self.tags
        else:
            tags = tuple(tags)
        for tag in tags:
            # Check that these are tagged collections up front, because we want
            # to avoid relying on Datastore transactionality to avoid modifying
            # the repo if there's an error later.
            collectionType = self.registry.getCollectionType(tag)
            if collectionType is not CollectionType.TAGGED:
                raise TypeError(f"Cannot associate into collection '{tag}' of non-TAGGED type "
                                f"{collectionType.name}.")
        # Reorganize the inputs so they're grouped by DatasetType and then
        # data ID.  We also include a list of DatasetRefs for each FileDataset
        # to hold the resolved DatasetRefs returned by the Registry, before
        # it's safe to swap them into FileDataset.refs.
        # Some type annotation aliases to make that clearer:
        GroupForType = Dict[DataCoordinate, Tuple[FileDataset, List[DatasetRef]]]
        GroupedData = MutableMapping[DatasetType, GroupForType]
        # The actual data structure:
        groupedData: GroupedData = defaultdict(dict)
        # And the nested loop that populates it:
        for dataset in datasets:
            # This list intentionally shared across the inner loop, since it's
            # associated with `dataset`.
            resolvedRefs: List[DatasetRef] = []
            for ref in dataset.refs:
                groupedData[ref.datasetType][ref.dataId] = (dataset, resolvedRefs)

        # Now we can bulk-insert into Registry for each DatasetType.
        allResolvedRefs: List[DatasetRef] = []
        for datasetType, groupForType in groupedData.items():
            refs = self.registry.insertDatasets(datasetType,
                                                dataIds=groupForType.keys(),
                                                run=run)
            # Append those resolved DatasetRefs to the new lists we set up for
            # them.
            for ref, (_, resolvedRefs) in zip(refs, groupForType.values()):
                resolvedRefs.append(ref)

        # Go back to the original FileDatasets to replace their refs with the
        # new resolved ones, and also build a big list of all refs.
        allResolvedRefs = []
        for groupForType in groupedData.values():
            for dataset, resolvedRefs in groupForType.values():
                dataset.refs = resolvedRefs
                allResolvedRefs.extend(resolvedRefs)

        # Bulk-associate everything with any tagged collections.
        for tag in tags:
            self.registry.associate(tag, allResolvedRefs)

        # Bulk-insert everything into Datastore.
        self.datastore.ingest(*datasets, transfer=transfer)

    @contextlib.contextmanager
    def export(self, *, directory: Optional[str] = None,
               filename: Optional[str] = None,
               format: Optional[str] = None,
               transfer: Optional[str] = None) -> Iterator[RepoExportContext]:
        """Export datasets from the repository represented by this `Butler`.

        This method is a context manager that returns a helper object
        (`RepoExportContext`) that is used to indicate what information from
        the repository should be exported.

        Parameters
        ----------
        directory : `str`, optional
            Directory dataset files should be written to if ``transfer`` is not
            `None`.
        filename : `str`, optional
            Name for the file that will include database information associated
            with the exported datasets.  If this is not an absolute path and
            ``directory`` is not `None`, it will be written to ``directory``
            instead of the current working directory.  Defaults to
            "export.{format}".
        format : `str`, optional
            File format for the database information file.  If `None`, the
            extension of ``filename`` will be used.
        transfer : `str`, optional
            Transfer mode passed to `Datastore.export`.

        Raises
        ------
        TypeError
            Raised if the set of arguments passed is inconsistent.

        Examples
        --------
        Typically the `Registry.queryDataIds` and `Registry.queryDatasets`
        methods are used to provide the iterables over data IDs and/or datasets
        to be exported::

            with butler.export("exports.yaml") as export:
                # Export all flats, but none of the dimension element rows
                # (i.e. data ID information) associated with them.
                export.saveDatasets(butler.registry.queryDatasets("flat"),
                                    elements=())
                # Export all datasets that start with "deepCoadd_" and all of
                # their associated data ID information.
                export.saveDatasets(butler.registry.queryDatasets("deepCoadd_*"))
        """
        if directory is None and transfer is not None:
            raise TypeError("Cannot transfer without providing a directory.")
        if transfer == "move":
            raise TypeError("Transfer may not be 'move': export is read-only")
        if format is None:
            if filename is None:
                raise TypeError("At least one of 'filename' or 'format' must be provided.")
            else:
                _, format = os.path.splitext(filename)
        elif filename is None:
            filename = f"export.{format}"
        if directory is not None:
            filename = os.path.join(directory, filename)
        BackendClass = getClassOf(self._config["repo_transfer_formats"][format]["export"])
        with open(filename, 'w') as stream:
            backend = BackendClass(stream)
            try:
                helper = RepoExportContext(self.registry, self.datastore, backend=backend,
                                           directory=directory, transfer=transfer)
                yield helper
            except BaseException:
                raise
            else:
                helper._finish()

    def import_(self, *, directory: Optional[str] = None,
                filename: Union[str, TextIO, None] = None,
                format: Optional[str] = None,
                transfer: Optional[str] = None,
                skip_dimensions: Optional[Set] = None) -> None:
        """Import datasets exported from a different butler repository.

        Parameters
        ----------
        directory : `str`, optional
            Directory containing dataset files.  If `None`, all file paths
            must be absolute.
        filename : `str` or `TextIO`, optional
            A stream or name of file that contains database information
            associated with the exported datasets.  If this a string (name) and
            is not an absolute path, does not exist in the current working
            directory, and ``directory`` is not `None`, it is assumed to be in
            ``directory``. Defaults to "export.{format}".
        format : `str`, optional
            File format for the database information file.  If `None`, the
            extension of ``filename`` will be used.
        transfer : `str`, optional
            Transfer mode passed to `Datastore.ingest`.
        skip_dimensions : `set`, optional
            Names of dimensions that should be skipped and not imported.

        Raises
        ------
        TypeError
            Raised if the set of arguments passed is inconsistent, or if the
            butler is read-only.
        """
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        if format is None:
            if filename is None:
                raise TypeError("At least one of 'filename' or 'format' must be provided.")
            else:
                _, format = os.path.splitext(filename)  # type: ignore
        elif filename is None:
            filename = f"export.{format}"
        if isinstance(filename, str) and directory is not None and not os.path.exists(filename):
            filename = os.path.join(directory, filename)
        BackendClass = getClassOf(self._config["repo_transfer_formats"][format]["import"])

        def doImport(importStream: TextIO) -> None:
            backend = BackendClass(importStream, self.registry)
            backend.register()
            with self.transaction():
                backend.load(self.datastore, directory=directory, transfer=transfer,
                             skip_dimensions=skip_dimensions)

        if isinstance(filename, str):
            with open(filename, "r") as stream:
                doImport(stream)
        else:
            doImport(filename)

    def validateConfiguration(self, logFailures: bool = False,
                              datasetTypeNames: Optional[Iterable[str]] = None,
                              ignore: Iterable[str] = None) -> None:
        """Validate butler configuration.

        Checks that each `DatasetType` can be stored in the `Datastore`.

        Parameters
        ----------
        logFailures : `bool`, optional
            If `True`, output a log message for every validation error
            detected.
        datasetTypeNames : iterable of `str`, optional
            The `DatasetType` names that should be checked.  This allows
            only a subset to be selected.
        ignore : iterable of `str`, optional
            Names of DatasetTypes to skip over.  This can be used to skip
            known problems. If a named `DatasetType` corresponds to a
            composite, all components of that `DatasetType` will also be
            ignored.

        Raises
        ------
        ButlerValidationError
            Raised if there is some inconsistency with how this Butler
            is configured.
        """
        if datasetTypeNames:
            datasetTypes = [self.registry.getDatasetType(name) for name in datasetTypeNames]
        else:
            datasetTypes = list(self.registry.queryDatasetTypes())

        # filter out anything from the ignore list
        if ignore:
            ignore = set(ignore)
            datasetTypes = [e for e in datasetTypes
                            if e.name not in ignore and e.nameAndComponent()[0] not in ignore]
        else:
            ignore = set()

        # Find all the registered instruments
        instruments = set(
            record.name for record in self.registry.queryDimensionRecords("instrument")
        )

        # For each datasetType that has an instrument dimension, create
        # a DatasetRef for each defined instrument
        datasetRefs = []

        for datasetType in datasetTypes:
            if "instrument" in datasetType.dimensions:
                for instrument in instruments:
                    datasetRef = DatasetRef(datasetType, {"instrument": instrument},  # type: ignore
                                            conform=False)
                    datasetRefs.append(datasetRef)

        entities: List[Union[DatasetType, DatasetRef]] = []
        entities.extend(datasetTypes)
        entities.extend(datasetRefs)

        datastoreErrorStr = None
        try:
            self.datastore.validateConfiguration(entities, logFailures=logFailures)
        except ValidationError as e:
            datastoreErrorStr = str(e)

        # Also check that the LookupKeys used by the datastores match
        # registry and storage class definitions
        keys = self.datastore.getLookupKeys()

        failedNames = set()
        failedDataId = set()
        for key in keys:
            if key.name is not None:
                if key.name in ignore:
                    continue

                # skip if specific datasetType names were requested and this
                # name does not match
                if datasetTypeNames and key.name not in datasetTypeNames:
                    continue

                # See if it is a StorageClass or a DatasetType
                if key.name in self.storageClasses:
                    pass
                else:
                    try:
                        self.registry.getDatasetType(key.name)
                    except KeyError:
                        if logFailures:
                            log.fatal("Key '%s' does not correspond to a DatasetType or StorageClass", key)
                        failedNames.add(key)
            else:
                # Dimensions are checked for consistency when the Butler
                # is created and rendezvoused with a universe.
                pass

            # Check that the instrument is a valid instrument
            # Currently only support instrument so check for that
            if key.dataId:
                dataIdKeys = set(key.dataId)
                if set(["instrument"]) != dataIdKeys:
                    if logFailures:
                        log.fatal("Key '%s' has unsupported DataId override", key)
                    failedDataId.add(key)
                elif key.dataId["instrument"] not in instruments:
                    if logFailures:
                        log.fatal("Key '%s' has unknown instrument", key)
                    failedDataId.add(key)

        messages = []

        if datastoreErrorStr:
            messages.append(datastoreErrorStr)

        for failed, msg in ((failedNames, "Keys without corresponding DatasetType or StorageClass entry: "),
                            (failedDataId, "Keys with bad DataId entries: ")):
            if failed:
                msg += ", ".join(str(k) for k in failed)
                messages.append(msg)

        if messages:
            raise ValidationError(";\n".join(messages))

    registry: Registry
    """The object that manages dataset metadata and relationships (`Registry`).

    Most operations that don't involve reading or writing butler datasets are
    accessible only via `Registry` methods.
    """

    datastore: Datastore
    """The object that manages actual dataset storage (`Datastore`).

    Direct user access to the datastore should rarely be necessary; the primary
    exception is the case where a `Datastore` implementation provides extra
    functionality beyond what the base class defines.
    """

    storageClasses: StorageClassFactory
    """An object that maps known storage class names to objects that fully
    describe them (`StorageClassFactory`).
    """

    collections: Optional[CollectionSearch]
    """The collections to search and any restrictions on the dataset types to
    search for within them, in order (`CollectionSearch`).
    """

    run: Optional[str]
    """Name of the run this butler writes outputs to (`str` or `None`).
    """

    tags: Tuple[str, ...]
    """Names of `~CollectionType.TAGGED` collections this butler associates
    with in `put` and `ingest`, and disassociates from in `pruneDatasets`
    (`tuple` [ `str` ]).
    """
