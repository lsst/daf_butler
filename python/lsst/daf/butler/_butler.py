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

import collections.abc
import contextlib
import logging
import numbers
import os
from collections import defaultdict
from typing import (
    Any,
    ClassVar,
    Counter,
    Dict,
    Iterable,
    Iterator,
    List,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    TextIO,
    Tuple,
    Type,
    Union,
)

from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils import doImportType
from lsst.utils.introspection import get_class_of
from lsst.utils.logging import VERBOSE, getLogger

from ._butlerConfig import ButlerConfig
from ._butlerRepoIndex import ButlerRepoIndex
from ._deferredDatasetHandle import DeferredDatasetHandle
from ._limited_butler import LimitedButler
from .core import (
    AmbiguousDatasetError,
    Config,
    ConfigSubset,
    DataCoordinate,
    DataId,
    DataIdValue,
    DatasetRef,
    DatasetRefURIs,
    DatasetType,
    Datastore,
    Dimension,
    DimensionConfig,
    DimensionElement,
    DimensionRecord,
    DimensionUniverse,
    FileDataset,
    Progress,
    StorageClass,
    StorageClassFactory,
    Timespan,
    ValidationError,
)
from .core.repoRelocation import BUTLER_ROOT_TAG
from .core.utils import transactional
from .registry import (
    CollectionType,
    ConflictingDefinitionError,
    DataIdError,
    DatasetIdGenEnum,
    Registry,
    RegistryConfig,
    RegistryDefaults,
)
from .transfers import RepoExportContext

log = getLogger(__name__)


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
            f"Cannot prune {self.collectionType} collection {self.collectionType.name} with purge=True."
        )


class Butler(LimitedButler):
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
    collections : `str` or `Iterable` [ `str` ], optional
        An expression specifying the collections to be searched (in order) when
        reading datasets.
        This may be a `str` collection name or an iterable thereof.
        See :ref:`daf_butler_collection_expressions` for more information.
        These collections are not registered automatically and must be
        manually registered before they are used by any method, but they may be
        manually registered after the `Butler` is initialized.
    run : `str`, optional
        Name of the `~CollectionType.RUN` collection new datasets should be
        inserted into.  If ``collections`` is `None` and ``run`` is not `None`,
        ``collections`` will be set to ``[run]``.  If not `None`, this
        collection will automatically be registered.  If this is not set (and
        ``writeable`` is not set either), a read-only butler will be created.
    searchPaths : `list` of `str`, optional
        Directory paths to search when calculating the full Butler
        configuration.  Not used if the supplied config is already a
        `ButlerConfig`.
    writeable : `bool`, optional
        Explicitly sets whether the butler supports write operations.  If not
        provided, a read-write butler is created if any of ``run``, ``tags``,
        or ``chains`` is non-empty.
    inferDefaults : `bool`, optional
        If `True` (default) infer default data ID values from the values
        present in the datasets in ``collections``: if all collections have the
        same value (or no value) for a governor dimension, that value will be
        the default for that dimension.  Nonexistent collections are ignored.
        If a default value is provided explicitly for a governor dimension via
        ``**kwargs``, no default will be inferred for that dimension.
    **kwargs : `str`
        Default data ID key-value pairs.  These may only identify "governor"
        dimensions like ``instrument`` and ``skymap``.

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
    from several others (as well)::

        butler = Butler("/path/to/repo", run="u/alice/DM-50000/a",
                        collections=["u/alice/DM-50000/a",
                                     "u/bob/DM-49998",
                                     "HSC/defaults"])

    This butler will `put` new datasets to the run ``u/alice/DM-50000/a``.
    Datasets will be read first from that run (since it appears first in the
    chain), and then from ``u/bob/DM-49998`` and finally ``HSC/defaults``.

    Finally, one can always create a `Butler` with no collections::

        butler = Butler("/path/to/repo", writeable=True)

    This can be extremely useful when you just want to use ``butler.registry``,
    e.g. for inserting dimension data or managing collections, or when the
    collections you want to use with the butler are not consistent.
    Passing ``writeable`` explicitly here is only necessary if you want to be
    able to make changes to the repo - usually the value for ``writeable`` can
    be guessed from the collection arguments provided, but it defaults to
    `False` when there are not collection arguments.
    """

    def __init__(
        self,
        config: Union[Config, str, None] = None,
        *,
        butler: Optional[Butler] = None,
        collections: Any = None,
        run: Optional[str] = None,
        searchPaths: Optional[List[str]] = None,
        writeable: Optional[bool] = None,
        inferDefaults: bool = True,
        **kwargs: str,
    ):
        defaults = RegistryDefaults(collections=collections, run=run, infer=inferDefaults, **kwargs)
        # Load registry, datastore, etc. from config or existing butler.
        if butler is not None:
            if config is not None or searchPaths is not None or writeable is not None:
                raise TypeError(
                    "Cannot pass 'config', 'searchPaths', or 'writeable' arguments with 'butler' argument."
                )
            self.registry = butler.registry.copy(defaults)
            self.datastore = butler.datastore
            self.storageClasses = butler.storageClasses
            self._config: ButlerConfig = butler._config
            self._allow_put_of_predefined_dataset = butler._allow_put_of_predefined_dataset
        else:
            # Can only look for strings in the known repos list.
            if isinstance(config, str) and config in self.get_known_repos():
                config = str(self.get_repo_uri(config))
            try:
                self._config = ButlerConfig(config, searchPaths=searchPaths)
            except FileNotFoundError as e:
                if known := self.get_known_repos():
                    aliases = f"(known aliases: {', '.join(known)})"
                else:
                    aliases = "(no known aliases)"
                raise FileNotFoundError(f"{e} {aliases}") from e
            self._config = ButlerConfig(config, searchPaths=searchPaths)
            try:
                if "root" in self._config:
                    butlerRoot = self._config["root"]
                else:
                    butlerRoot = self._config.configDir
                if writeable is None:
                    writeable = run is not None
                self.registry = Registry.fromConfig(
                    self._config, butlerRoot=butlerRoot, writeable=writeable, defaults=defaults
                )
                self.datastore = Datastore.fromConfig(
                    self._config, self.registry.getDatastoreBridgeManager(), butlerRoot=butlerRoot
                )
                self.storageClasses = StorageClassFactory()
                self.storageClasses.addFromConfig(self._config)
                self._allow_put_of_predefined_dataset = self._config.get(
                    "allow_put_of_predefined_dataset", False
                )
            except Exception:
                # Failures here usually mean that configuration is incomplete,
                # just issue an error message which includes config file URI.
                log.error(f"Failed to instantiate Butler from config {self._config.configFile}.")
                raise

        if "run" in self._config or "collection" in self._config:
            raise ValueError("Passing a run or collection via configuration is no longer supported.")

    GENERATION: ClassVar[int] = 3
    """This is a Generation 3 Butler.

    This attribute may be removed in the future, once the Generation 2 Butler
    interface has been fully retired; it should only be used in transitional
    code.
    """

    @classmethod
    def get_repo_uri(cls, label: str) -> ResourcePath:
        """Look up the label in a butler repository index.

        Parameters
        ----------
        label : `str`
            Label of the Butler repository to look up.

        Returns
        -------
        uri : `lsst.resources.ResourcePath`
            URI to the Butler repository associated with the given label.

        Raises
        ------
        KeyError
            Raised if the label is not found in the index, or if an index
            can not be found at all.

        Notes
        -----
        See `~lsst.daf.butler.ButlerRepoIndex` for details on how the
        information is discovered.
        """
        return ButlerRepoIndex.get_repo_uri(label)

    @classmethod
    def get_known_repos(cls) -> Set[str]:
        """Retrieve the list of known repository labels.

        Returns
        -------
        repos : `set` of `str`
            All the known labels. Can be empty if no index can be found.

        Notes
        -----
        See `~lsst.daf.butler.ButlerRepoIndex` for details on how the
        information is discovered.
        """
        return ButlerRepoIndex.get_known_repos()

    @staticmethod
    def makeRepo(
        root: ResourcePathExpression,
        config: Union[Config, str, None] = None,
        dimensionConfig: Union[Config, str, None] = None,
        standalone: bool = False,
        searchPaths: Optional[List[str]] = None,
        forceConfigRoot: bool = True,
        outfile: Optional[ResourcePathExpression] = None,
        overwrite: bool = False,
    ) -> Config:
        """Create an empty data repository by adding a butler.yaml config
        to a repository root directory.

        Parameters
        ----------
        root : `lsst.resources.ResourcePathExpression`
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
        outfile : `lss.resources.ResourcePathExpression`, optional
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
        root_uri = ResourcePath(root, forceDirectory=True)
        root_uri.mkdir()

        config = Config(config)

        # If we are creating a new repo from scratch with relative roots,
        # do not propagate an explicit root from the config file
        if "root" in config:
            del config["root"]

        full = ButlerConfig(config, searchPaths=searchPaths)  # this applies defaults
        imported_class = doImportType(full["datastore", "cls"])
        if not issubclass(imported_class, Datastore):
            raise TypeError(f"Imported datastore class {full['datastore', 'cls']} is not a Datastore")
        datastoreClass: Type[Datastore] = imported_class
        datastoreClass.setConfigRoot(BUTLER_ROOT_TAG, config, full, overwrite=forceConfigRoot)

        # if key exists in given config, parse it, otherwise parse the defaults
        # in the expanded config
        if config.get(("registry", "db")):
            registryConfig = RegistryConfig(config)
        else:
            registryConfig = RegistryConfig(full)
        defaultDatabaseUri = registryConfig.makeDefaultDatabaseUri(BUTLER_ROOT_TAG)
        if defaultDatabaseUri is not None:
            Config.updateParameters(
                RegistryConfig, config, full, toUpdate={"db": defaultDatabaseUri}, overwrite=forceConfigRoot
            )
        else:
            Config.updateParameters(RegistryConfig, config, full, toCopy=("db",), overwrite=forceConfigRoot)

        if standalone:
            config.merge(full)
        else:
            # Always expand the registry.managers section into the per-repo
            # config, because after the database schema is created, it's not
            # allowed to change anymore.  Note that in the standalone=True
            # branch, _everything_ in the config is expanded, so there's no
            # need to special case this.
            Config.updateParameters(RegistryConfig, config, full, toMerge=("managers",), overwrite=False)
        configURI: ResourcePathExpression
        if outfile is not None:
            # When writing to a separate location we must include
            # the root of the butler repo in the config else it won't know
            # where to look.
            config["root"] = root_uri.geturl()
            configURI = outfile
        else:
            configURI = root_uri
        # Strip obscore configuration, if it is present, before writing config
        # to a file, obscore config will be stored in registry.
        config_to_write = config
        if ("registry", "managers", "obscore") in config:
            config_to_write = config.copy()
            del config_to_write["registry", "managers", "obscore", "config"]
        config_to_write.dumpToUri(configURI, overwrite=overwrite)

        # Create Registry and populate tables
        registryConfig = RegistryConfig(config.get("registry"))
        dimensionConfig = DimensionConfig(dimensionConfig)
        Registry.createFromConfig(registryConfig, dimensionConfig=dimensionConfig, butlerRoot=root_uri)

        log.verbose("Wrote new Butler configuration file to %s", configURI)

        return config

    @classmethod
    def _unpickle(
        cls,
        config: ButlerConfig,
        collections: Optional[tuple[str, ...]],
        run: Optional[str],
        defaultDataId: Dict[str, str],
        writeable: bool,
    ) -> Butler:
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
        collections : `tuple` [ `str` ]
            Names of the default collections to read from.
        run : `str`, optional
            Name of the default `~CollectionType.RUN` collection to write to.
        defaultDataId : `dict` [ `str`, `str` ]
            Default data ID values.
        writeable : `bool`
            Whether the Butler should support write operations.

        Returns
        -------
        butler : `Butler`
            A new `Butler` instance.
        """
        # MyPy doesn't recognize that the kwargs below are totally valid; it
        # seems to think '**defaultDataId* is a _positional_ argument!
        return cls(
            config=config,
            collections=collections,
            run=run,
            writeable=writeable,
            **defaultDataId,  # type: ignore
        )

    def __reduce__(self) -> tuple:
        """Support pickling."""
        return (
            Butler._unpickle,
            (
                self._config,
                self.collections,
                self.run,
                self.registry.defaults.dataId.byName(),
                self.registry.isWriteable(),
            ),
        )

    def __str__(self) -> str:
        return "Butler(collections={}, run={}, datastore='{}', registry='{}')".format(
            self.collections, self.run, self.datastore, self.registry
        )

    def isWriteable(self) -> bool:
        """Return `True` if this `Butler` supports write operations."""
        return self.registry.isWriteable()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Context manager supporting `Butler` transactions.

        Transactions can be nested.
        """
        with self.registry.transaction():
            with self.datastore.transaction():
                yield

    def _standardizeArgs(
        self,
        datasetRefOrType: Union[DatasetRef, DatasetType, str],
        dataId: Optional[DataId] = None,
        for_put: bool = True,
        **kwargs: Any,
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
        for_put : `bool`, optional
            If `True` this call is invoked as part of a `Butler.put()`.
            Otherwise it is assumed to be part of a `Butler.get()`. This
            parameter is only relevant if there is dataset type
            inconsistency.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        datasetType : `DatasetType`
            A `DatasetType` instance extracted from ``datasetRefOrType``.
        dataId : `dict` or `DataId`, optional
            Argument that can be used (along with ``kwargs``) to construct a
            `DataId`.

        Notes
        -----
        Butler APIs that conceptually need a DatasetRef also allow passing a
        `DatasetType` (or the name of one) and a `DataId` (or a dict and
        keyword arguments that can be used to construct one) separately. This
        method accepts those arguments and always returns a true `DatasetType`
        and a `DataId` or `dict`.

        Standardization of `dict` vs `DataId` is best handled by passing the
        returned ``dataId`` (and ``kwargs``) to `Registry` APIs, which are
        generally similarly flexible.
        """
        externalDatasetType: Optional[DatasetType] = None
        internalDatasetType: Optional[DatasetType] = None
        if isinstance(datasetRefOrType, DatasetRef):
            if dataId is not None or kwargs:
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
                # We can allow differences if they are compatible, depending
                # on whether this is a get or a put. A get requires that
                # the python type associated with the datastore can be
                # converted to the user type. A put requires that the user
                # supplied python type can be converted to the internal
                # type expected by registry.
                relevantDatasetType = internalDatasetType
                if for_put:
                    is_compatible = internalDatasetType.is_compatible_with(externalDatasetType)
                else:
                    is_compatible = externalDatasetType.is_compatible_with(internalDatasetType)
                    relevantDatasetType = externalDatasetType
                if not is_compatible:
                    raise ValueError(
                        f"Supplied dataset type ({externalDatasetType}) inconsistent with "
                        f"registry definition ({internalDatasetType})"
                    )
                # Override the internal definition.
                internalDatasetType = relevantDatasetType

        assert internalDatasetType is not None
        return internalDatasetType, dataId

    def _rewrite_data_id(
        self, dataId: Optional[DataId], datasetType: DatasetType, **kwargs: Any
    ) -> Tuple[Optional[DataId], Dict[str, Any]]:
        """Rewrite a data ID taking into account dimension records.

        Take a Data ID and keyword args and rewrite it if necessary to
        allow the user to specify dimension records rather than dimension
        primary values.

        This allows a user to include a dataId dict with keys of
        ``exposure.day_obs`` and ``exposure.seq_num`` instead of giving
        the integer exposure ID.  It also allows a string to be given
        for a dimension value rather than the integer ID if that is more
        convenient. For example, rather than having to specifyin the
        detector with ``detector.full_name``, a string given for ``detector``
        will be interpreted as the full name and converted to the integer
        value.

        Keyword arguments can also use strings for dimensions like detector
        and exposure but python does not allow them to include ``.`` and
        so the ``exposure.day_obs`` syntax can not be used in a keyword
        argument.

        Parameters
        ----------
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that will label the
            `DatasetRef` within a Collection.
        datasetType : `DatasetType`
            The dataset type associated with this dataId. Required to
            determine the relevant dimensions.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Returns
        -------
        dataId : `dict` or `DataCoordinate`
            The, possibly rewritten, dataId. If given a `DataCoordinate` and
            no keyword arguments, the original dataId will be returned
            unchanged.
        **kwargs : `dict`
            Any unused keyword arguments (would normally be empty dict).
        """
        # Do nothing if we have a standalone DataCoordinate.
        if isinstance(dataId, DataCoordinate) and not kwargs:
            return dataId, kwargs

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
        for dataIdDict in (newDataId, kwargs):
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
                            log.debug(
                                "Converting dimension %s to %s.%s=%s",
                                dimensionName,
                                dimensionName,
                                alternate.name,
                                value,
                            )
                            break
                    else:
                        log.warning(
                            "Type mismatch found for value '%r' provided for dimension %s. "
                            "Could not find matching alternative (primary key has type %s) "
                            "so attempting to use as-is.",
                            value,
                            dimensionName,
                            dimension.primaryKey.getPythonType(),
                        )

        # By this point kwargs and newDataId should only include valid
        # dimensions. Merge kwargs in to the new dataId and log if there
        # are dimensions in both (rather than calling update).
        for k, v in kwargs.items():
            if k in newDataId and newDataId[k] != v:
                log.debug(
                    "Keyword arg %s overriding explicit value in dataId of %s with %s", k, newDataId[k], v
                )
            newDataId[k] = v
        # No need to retain any values in kwargs now.
        kwargs = {}

        # If we have some unrecognized dimensions we have to try to connect
        # them to records in other dimensions.  This is made more complicated
        # by some dimensions having records with clashing names.  A mitigation
        # is that we can tell by this point which dimensions are missing
        # for the DatasetType but this does not work for calibrations
        # where additional dimensions can be used to constrain the temporal
        # axis.
        if not_dimensions:
            # Search for all dimensions even if we have been given a value
            # explicitly. In some cases records are given as well as the
            # actually dimension and this should not be an error if they
            # match.
            mandatoryDimensions = datasetType.dimensions.names  # - provided

            candidateDimensions: Set[str] = set()
            candidateDimensions.update(mandatoryDimensions)

            # For calibrations we may well be needing temporal dimensions
            # so rather than always including all dimensions in the scan
            # restrict things a little. It is still possible for there
            # to be confusion over day_obs in visit vs exposure for example.
            # If we are not searching calibration collections things may
            # fail but they are going to fail anyway because of the
            # ambiguousness of the dataId...
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
            matched_dims = set()
            for dimensionName in candidateDimensions:
                dimension = self.registry.dimensions.getStaticDimensions()[dimensionName]
                fields = dimension.metadata.names | dimension.uniqueKeys.names
                for field in not_dimensions:
                    if field in fields:
                        guessedAssociation[dimensionName][field] = not_dimensions[field]
                        counter[dimensionName] += 1
                        assigned[field].add(dimensionName)
                        matched_dims.add(field)

            # Calculate the fields that matched nothing.
            never_found = set(not_dimensions) - matched_dims

            if never_found:
                raise ValueError(f"Unrecognized keyword args given: {never_found}")

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
                    requiredButMissing = assignedDimensions.intersection(mandatoryDimensions)
                    if requiredButMissing:
                        candidateDimensions = requiredButMissing
                    else:
                        candidateDimensions = assignedDimensions

                        # If this is a choice between visit and exposure and
                        # neither was a required part of the dataset type,
                        # (hence in this branch) always prefer exposure over
                        # visit since exposures are always defined and visits
                        # are defined from exposures.
                        if candidateDimensions == {"exposure", "visit"}:
                            candidateDimensions = {"exposure"}

                    # Select the relevant items and get a new restricted
                    # counter.
                    theseCounts = {k: v for k, v in counter.items() if k in candidateDimensions}
                    duplicatesCounter: Counter[str] = Counter()
                    duplicatesCounter.update(theseCounts)

                    # Choose the most common. If they are equally common
                    # we will pick the one that was found first.
                    # Returns a list of tuples
                    selected = duplicatesCounter.most_common(1)[0][0]

                    log.debug(
                        "Ambiguous dataId entry '%s' associated with multiple dimensions: %s."
                        " Removed ambiguity by choosing dimension %s.",
                        fieldName,
                        ", ".join(assignedDimensions),
                        selected,
                    )

                    for candidateDimension in assignedDimensions:
                        if candidateDimension != selected:
                            del guessedAssociation[candidateDimension][fieldName]

            # Update the record look up dict with the new associations
            for dimensionName, values in guessedAssociation.items():
                if values:  # A dict might now be empty
                    log.debug("Assigned non-dimension dataId keys to dimension %s: %s", dimensionName, values)
                    byRecord[dimensionName].update(values)

        if byRecord:
            # Some record specifiers were found so we need to convert
            # them to the Id form
            for dimensionName, values in byRecord.items():
                if dimensionName in newDataId:
                    log.debug(
                        "DataId specified explicit %s dimension value of %s in addition to"
                        " general record specifiers for it of %s.  Ignoring record information.",
                        dimensionName,
                        newDataId[dimensionName],
                        str(values),
                    )
                    # Get the actual record and compare with these values.
                    try:
                        recs = list(self.registry.queryDimensionRecords(dimensionName, dataId=newDataId))
                    except DataIdError:
                        raise ValueError(
                            f"Could not find dimension '{dimensionName}'"
                            f" with dataId {newDataId} as part of comparing with"
                            f" record values {byRecord[dimensionName]}"
                        ) from None
                    if len(recs) == 1:
                        errmsg: List[str] = []
                        for k, v in values.items():
                            if (recval := getattr(recs[0], k)) != v:
                                errmsg.append(f"{k}({recval} != {v})")
                        if errmsg:
                            raise ValueError(
                                f"Dimension {dimensionName} in dataId has explicit value"
                                " inconsistent with records: " + ", ".join(errmsg)
                            )
                    else:
                        # Multiple matches for an explicit dimension
                        # should never happen but let downstream complain.
                        pass
                    continue

                # Build up a WHERE expression
                bind = {k: v for k, v in values.items()}
                where = " AND ".join(f"{dimensionName}.{k} = {k}" for k in bind)

                # Hopefully we get a single record that matches
                records = set(
                    self.registry.queryDimensionRecords(
                        dimensionName, dataId=newDataId, where=where, bind=bind, **kwargs
                    )
                )

                if len(records) != 1:
                    if len(records) > 1:
                        # visit can have an ambiguous answer without involving
                        # visit_system. The default visit_system is defined
                        # by the instrument.
                        if (
                            dimensionName == "visit"
                            and "visit_system_membership" in self.registry.dimensions
                            and "visit_system"
                            in self.registry.dimensions["instrument"].metadata  # type: ignore
                        ):
                            instrument_records = list(
                                self.registry.queryDimensionRecords(
                                    "instrument",
                                    dataId=newDataId,
                                    **kwargs,
                                )
                            )
                            if len(instrument_records) == 1:
                                visit_system = instrument_records[0].visit_system
                                if visit_system is None:
                                    # Set to a value that will never match.
                                    visit_system = -1

                                # Look up each visit in the
                                # visit_system_membership records.
                                for rec in records:
                                    membership = list(
                                        self.registry.queryDimensionRecords(
                                            # Use bind to allow zero results.
                                            # This is a fully-specified query.
                                            "visit_system_membership",
                                            where="instrument = inst AND visit_system = system AND visit = v",
                                            bind=dict(
                                                inst=instrument_records[0].name, system=visit_system, v=rec.id
                                            ),
                                        )
                                    )
                                    if membership:
                                        # This record is the right answer.
                                        records = set([rec])
                                        break

                        # The ambiguity may have been resolved so check again.
                        if len(records) > 1:
                            log.debug("Received %d records from constraints of %s", len(records), str(values))
                            for r in records:
                                log.debug("- %s", str(r))
                            raise ValueError(
                                f"DataId specification for dimension {dimensionName} is not"
                                f" uniquely constrained to a single dataset by {values}."
                                f" Got {len(records)} results."
                            )
                    else:
                        raise ValueError(
                            f"DataId specification for dimension {dimensionName} matched no"
                            f" records when constrained by {values}"
                        )

                # Get the primary key from the real dimension object
                dimension = self.registry.dimensions.getStaticDimensions()[dimensionName]
                if not isinstance(dimension, Dimension):
                    raise RuntimeError(
                        f"{dimension.name} is not a true dimension, and cannot be used in data IDs."
                    )
                newDataId[dimensionName] = getattr(records.pop(), dimension.primaryKey.name)

        return newDataId, kwargs

    def _findDatasetRef(
        self,
        datasetRefOrType: Union[DatasetRef, DatasetType, str],
        dataId: Optional[DataId] = None,
        *,
        collections: Any = None,
        allowUnresolved: bool = False,
        **kwargs: Any,
    ) -> DatasetRef:
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
        **kwargs
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
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, for_put=False, **kwargs)
        if isinstance(datasetRefOrType, DatasetRef):
            idNumber = datasetRefOrType.id
        else:
            idNumber = None
        timespan: Optional[Timespan] = None

        dataId, kwargs = self._rewrite_data_id(dataId, datasetType, **kwargs)

        if datasetType.isCalibration():
            # Because this is a calibration dataset, first try to make a
            # standardize the data ID without restricting the dimensions to
            # those of the dataset type requested, because there may be extra
            # dimensions that provide temporal information for a validity-range
            # lookup.
            dataId = DataCoordinate.standardize(
                dataId, universe=self.registry.dimensions, defaults=self.registry.defaults.dataId, **kwargs
            )
            if dataId.graph.temporal:
                dataId = self.registry.expandDataId(dataId)
                timespan = dataId.timespan
        else:
            # Standardize the data ID to just the dimensions of the dataset
            # type instead of letting registry.findDataset do it, so we get the
            # result even if no dataset is found.
            dataId = DataCoordinate.standardize(
                dataId, graph=datasetType.dimensions, defaults=self.registry.defaults.dataId, **kwargs
            )
        # Always lookup the DatasetRef, even if one is given, to ensure it is
        # present in the current collection.
        ref = self.registry.findDataset(datasetType, dataId, collections=collections, timespan=timespan)
        if ref is None:
            if allowUnresolved:
                return DatasetRef(datasetType, dataId)
            else:
                if collections is None:
                    collections = self.registry.defaults.collections
                raise LookupError(
                    f"Dataset {datasetType.name} with data ID {dataId} "
                    f"could not be found in collections {collections}."
                )
        if idNumber is not None and idNumber != ref.id:
            if collections is None:
                collections = self.registry.defaults.collections
            raise ValueError(
                f"DatasetRef.id provided ({idNumber}) does not match "
                f"id ({ref.id}) in registry in collections {collections}."
            )
        if datasetType != ref.datasetType:
            # If they differ it is because the user explicitly specified
            # a compatible dataset type to this call rather than using the
            # registry definition. The DatasetRef must therefore be recreated
            # using the user definition such that the expected type is
            # returned.
            ref = DatasetRef(datasetType, ref.dataId, run=ref.run, id=ref.id)

        return ref

    @transactional
    def putDirect(self, obj: Any, ref: DatasetRef) -> DatasetRef:
        # Docstring inherited.
        (imported_ref,) = self.registry._importDatasets(
            [ref],
            expand=True,
        )
        if imported_ref.id != ref.getCheckedId():
            raise RuntimeError("This registry configuration does not support putDirect.")
        self.datastore.put(obj, ref)
        return ref

    @transactional
    def put(
        self,
        obj: Any,
        datasetRefOrType: Union[DatasetRef, DatasetType, str],
        dataId: Optional[DataId] = None,
        *,
        run: Optional[str] = None,
        **kwargs: Any,
    ) -> DatasetRef:
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
        **kwargs
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
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwargs)
        if isinstance(datasetRefOrType, DatasetRef) and datasetRefOrType.id is not None:
            raise ValueError("DatasetRef must not be in registry, must have None id")

        # Handle dimension records in dataId
        dataId, kwargs = self._rewrite_data_id(dataId, datasetType, **kwargs)

        # Add Registry Dataset entry.
        dataId = self.registry.expandDataId(dataId, graph=datasetType.dimensions, **kwargs)

        # For an execution butler the datasets will be pre-defined.
        # If the butler is configured that way datasets should only be inserted
        # if they do not already exist in registry. Trying and catching
        # ConflictingDefinitionError will not work because the transaction
        # will be corrupted. Instead, in this mode always check first.
        ref = None
        ref_is_predefined = False
        if self._allow_put_of_predefined_dataset:
            # Get the matching ref for this run.
            ref = self.registry.findDataset(datasetType, collections=run, dataId=dataId)

            if ref:
                # Must be expanded form for datastore templating
                dataId = self.registry.expandDataId(dataId, graph=datasetType.dimensions)
                ref = ref.expanded(dataId)
                ref_is_predefined = True

        if not ref:
            (ref,) = self.registry.insertDatasets(datasetType, run=run, dataIds=[dataId])

        # If the ref is predefined it is possible that the datastore also
        # has the record. Asking datastore to put it again will result in
        # the artifact being recreated, overwriting previous, then will cause
        # a failure in writing the record which will cause the artifact
        # to be removed. Much safer to ask first before attempting to
        # overwrite. Race conditions should not be an issue for the
        # execution butler environment.
        if ref_is_predefined:
            if self.datastore.knows(ref):
                raise ConflictingDefinitionError(f"Dataset associated {ref} already exists.")

        self.datastore.put(obj, ref)

        return ref

    def getDirect(
        self,
        ref: DatasetRef,
        *,
        parameters: Optional[Dict[str, Any]] = None,
        storageClass: Optional[Union[StorageClass, str]] = None,
    ) -> Any:
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
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type.

        Returns
        -------
        obj : `object`
            The dataset.
        """
        return self.datastore.get(ref, parameters=parameters, storageClass=storageClass)

    def getDirectDeferred(
        self,
        ref: DatasetRef,
        *,
        parameters: Union[dict, None] = None,
        storageClass: str | StorageClass | None = None,
    ) -> DeferredDatasetHandle:
        """Create a `DeferredDatasetHandle` which can later retrieve a dataset,
        from a resolved `DatasetRef`.

        Parameters
        ----------
        ref : `DatasetRef`
            Resolved reference to an already stored dataset.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type.

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
        return DeferredDatasetHandle(butler=self, ref=ref, parameters=parameters, storageClass=storageClass)

    def getDeferred(
        self,
        datasetRefOrType: Union[DatasetRef, DatasetType, str],
        dataId: Optional[DataId] = None,
        *,
        parameters: Union[dict, None] = None,
        collections: Any = None,
        storageClass: str | StorageClass | None = None,
        **kwargs: Any,
    ) -> DeferredDatasetHandle:
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
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type.
        **kwargs
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
        ref = self._findDatasetRef(datasetRefOrType, dataId, collections=collections, **kwargs)
        return DeferredDatasetHandle(butler=self, ref=ref, parameters=parameters, storageClass=storageClass)

    def get(
        self,
        datasetRefOrType: Union[DatasetRef, DatasetType, str],
        dataId: Optional[DataId] = None,
        *,
        parameters: Optional[Dict[str, Any]] = None,
        collections: Any = None,
        storageClass: Optional[Union[StorageClass, str]] = None,
        **kwargs: Any,
    ) -> Any:
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
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type.
        **kwargs
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
        ref = self._findDatasetRef(datasetRefOrType, dataId, collections=collections, **kwargs)
        return self.getDirect(ref, parameters=parameters, storageClass=storageClass)

    def getURIs(
        self,
        datasetRefOrType: Union[DatasetRef, DatasetType, str],
        dataId: Optional[DataId] = None,
        *,
        predict: bool = False,
        collections: Any = None,
        run: Optional[str] = None,
        **kwargs: Any,
    ) -> DatasetRefURIs:
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
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        uris : `DatasetRefURIs`
            The URI to the primary artifact associated with this dataset (if
            the dataset was disassembled within the datastore this may be
            `None`), and the URIs to any components associated with the dataset
            artifact. (can be empty if there are no components).
        """
        ref = self._findDatasetRef(
            datasetRefOrType, dataId, allowUnresolved=predict, collections=collections, **kwargs
        )
        if ref.id is None:  # only possible if predict is True
            if run is None:
                run = self.run
                if run is None:
                    raise TypeError("Cannot predict location with run=None.")
            # Lie about ID, because we can't guess it, and only
            # Datastore.getURIs() will ever see it (and it doesn't use it).
            ref = ref.resolved(id=0, run=run)
        return self.datastore.getURIs(ref, predict)

    def getURI(
        self,
        datasetRefOrType: Union[DatasetRef, DatasetType, str],
        dataId: Optional[DataId] = None,
        *,
        predict: bool = False,
        collections: Any = None,
        run: Optional[str] = None,
        **kwargs: Any,
    ) -> ResourcePath:
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
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        uri : `lsst.resources.ResourcePath`
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
        primary, components = self.getURIs(
            datasetRefOrType, dataId=dataId, predict=predict, collections=collections, run=run, **kwargs
        )

        if primary is None or components:
            raise RuntimeError(
                f"Dataset ({datasetRefOrType}) includes distinct URIs for components. "
                "Use Butler.getURIs() instead."
            )
        return primary

    def retrieveArtifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool = False,
    ) -> List[ResourcePath]:
        """Retrieve the artifacts associated with the supplied refs.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets for which artifacts are to be retrieved.
            A single ref can result in multiple artifacts. The refs must
            be resolved.
        destination : `lsst.resources.ResourcePath` or `str`
            Location to write the artifacts.
        transfer : `str`, optional
            Method to use to transfer the artifacts. Must be one of the options
            supported by `~lsst.resources.ResourcePath.transfer_from()`.
            "move" is not allowed.
        preserve_path : `bool`, optional
            If `True` the full path of the artifact within the datastore
            is preserved. If `False` the final file component of the path
            is used.
        overwrite : `bool`, optional
            If `True` allow transfers to overwrite existing files at the
            destination.

        Returns
        -------
        targets : `list` of `lsst.resources.ResourcePath`
            URIs of file artifacts in destination location. Order is not
            preserved.

        Notes
        -----
        For non-file datastores the artifacts written to the destination
        may not match the representation inside the datastore. For example
        a hierarchical data structure in a NoSQL database may well be stored
        as a JSON file.
        """
        return self.datastore.retrieveArtifacts(
            refs,
            ResourcePath(destination),
            transfer=transfer,
            preserve_path=preserve_path,
            overwrite=overwrite,
        )

    def datasetExists(
        self,
        datasetRefOrType: Union[DatasetRef, DatasetType, str],
        dataId: Optional[DataId] = None,
        *,
        collections: Any = None,
        **kwargs: Any,
    ) -> bool:
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
        **kwargs
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
        ref = self._findDatasetRef(datasetRefOrType, dataId, collections=collections, **kwargs)
        return self.datastore.exists(ref)

    def removeRuns(self, names: Iterable[str], unstore: bool = True) -> None:
        """Remove one or more `~CollectionType.RUN` collections and the
        datasets within them.

        Parameters
        ----------
        names : `Iterable` [ `str` ]
            The names of the collections to remove.
        unstore : `bool`, optional
            If `True` (default), delete datasets from all datastores in which
            they are present, and attempt to rollback the registry deletions if
            datastore deletions fail (which may not always be possible).  If
            `False`, datastore records for these datasets are still removed,
            but any artifacts (e.g. files) will not be.

        Raises
        ------
        TypeError
            Raised if one or more collections are not of type
            `~CollectionType.RUN`.
        """
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        names = list(names)
        refs: List[DatasetRef] = []
        for name in names:
            collectionType = self.registry.getCollectionType(name)
            if collectionType is not CollectionType.RUN:
                raise TypeError(f"The collection type of '{name}' is {collectionType.name}, not RUN.")
            refs.extend(self.registry.queryDatasets(..., collections=name, findFirst=True))
        with self.datastore.transaction():
            with self.registry.transaction():
                if unstore:
                    self.datastore.trash(refs)
                else:
                    self.datastore.forget(refs)
                for name in names:
                    self.registry.removeCollection(name)
        if unstore:
            # Point of no return for removing artifacts
            self.datastore.emptyTrash()

    def pruneCollection(
        self, name: str, purge: bool = False, unstore: bool = False, unlink: Optional[List[str]] = None
    ) -> None:
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
        unlink: `list` [`str`], optional
            Before removing the given `collection` unlink it from from these
            parent collections.

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

        def remove(child: str, parent: str) -> None:
            """Remove a child collection from a parent collection."""
            # Remove child from parent.
            chain = list(self.registry.getCollectionChain(parent))
            try:
                chain.remove(name)
            except ValueError as e:
                raise RuntimeError(f"{name} is not a child of {parent}") from e
            self.registry.setCollectionChain(parent, chain)

        with self.datastore.transaction():
            with self.registry.transaction():
                if unlink:
                    for parent in unlink:
                        remove(name, parent)
                if unstore:
                    refs = self.registry.queryDatasets(..., collections=name, findFirst=True)
                    self.datastore.trash(refs)
                self.registry.removeCollection(name)

        if unstore:
            # Point of no return for removing artifacts
            self.datastore.emptyTrash()

    def pruneDatasets(
        self,
        refs: Iterable[DatasetRef],
        *,
        disassociate: bool = True,
        unstore: bool = False,
        tags: Iterable[str] = (),
        purge: bool = False,
    ) -> None:
        # docstring inherited from LimitedButler

        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        if purge:
            if not disassociate:
                raise TypeError("Cannot pass purge=True without disassociate=True.")
            if not unstore:
                raise TypeError("Cannot pass purge=True without unstore=True.")
        elif disassociate:
            tags = tuple(tags)
            if not tags:
                raise TypeError("No tags provided but disassociate=True.")
            for tag in tags:
                collectionType = self.registry.getCollectionType(tag)
                if collectionType is not CollectionType.TAGGED:
                    raise TypeError(
                        f"Cannot disassociate from collection '{tag}' "
                        f"of non-TAGGED type {collectionType.name}."
                    )
        # For an execution butler we want to keep existing UUIDs for the
        # datasets, for that we need to keep them in the collections but
        # remove from datastore.
        if self._allow_put_of_predefined_dataset and purge:
            purge = False
            disassociate = False
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
        with self.datastore.transaction():
            with self.registry.transaction():
                if unstore:
                    self.datastore.trash(refs)
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
    def ingest(
        self,
        *datasets: FileDataset,
        transfer: Optional[str] = "auto",
        run: Optional[str] = None,
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
        record_validation_info: bool = True,
    ) -> None:
        """Store and register one or more datasets that already exist on disk.

        Parameters
        ----------
        datasets : `FileDataset`
            Each positional argument is a struct containing information about
            a file to be ingested, including its URI (either absolute or
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
            'split', 'hardlink', 'relsymlink' or 'symlink', indicating how to
            transfer the file.
        run : `str`, optional
            The name of the run ingested datasets should be added to,
            overriding ``self.run``.
        idGenerationMode : `DatasetIdGenEnum`, optional
            Specifies option for generating dataset IDs. By default unique IDs
            are generated for each inserted dataset.
        record_validation_info : `bool`, optional
            If `True`, the default, the datastore can record validation
            information associated with the file. If `False` the datastore
            will not attempt to track any information such as checksums
            or file sizes. This can be useful if such information is tracked
            in an external system or if the file is to be compressed in place.
            It is up to the datastore whether this parameter is relevant.

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
        progress = Progress("lsst.daf.butler.Butler.ingest", level=logging.DEBUG)
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
        for dataset in progress.wrap(datasets, desc="Grouping by dataset type"):
            # This list intentionally shared across the inner loop, since it's
            # associated with `dataset`.
            resolvedRefs: List[DatasetRef] = []

            # Somewhere to store pre-existing refs if we have an
            # execution butler.
            existingRefs: List[DatasetRef] = []

            for ref in dataset.refs:
                if ref.dataId in groupedData[ref.datasetType]:
                    raise ConflictingDefinitionError(
                        f"Ingest conflict. Dataset {dataset.path} has same"
                        " DataId as other ingest dataset"
                        f" {groupedData[ref.datasetType][ref.dataId][0].path} "
                        f" ({ref.dataId})"
                    )
                if self._allow_put_of_predefined_dataset:
                    existing_ref = self.registry.findDataset(
                        ref.datasetType, dataId=ref.dataId, collections=run
                    )
                    if existing_ref:
                        if self.datastore.knows(existing_ref):
                            raise ConflictingDefinitionError(
                                f"Dataset associated with path {dataset.path}"
                                f" already exists as {existing_ref}."
                            )
                        # Store this ref elsewhere since it already exists
                        # and we do not want to remake it but we do want
                        # to store it in the datastore.
                        existingRefs.append(existing_ref)

                        # Nothing else to do until we have finished
                        # iterating.
                        continue

                groupedData[ref.datasetType][ref.dataId] = (dataset, resolvedRefs)

            if existingRefs:

                if len(dataset.refs) != len(existingRefs):
                    # Keeping track of partially pre-existing datasets is hard
                    # and should generally never happen. For now don't allow
                    # it.
                    raise ConflictingDefinitionError(
                        f"For dataset {dataset.path} some dataIds already exist"
                        " in registry but others do not. This is not supported."
                    )

                # Attach the resolved refs if we found them.
                dataset.refs = existingRefs

        # Now we can bulk-insert into Registry for each DatasetType.
        for datasetType, groupForType in progress.iter_item_chunks(
            groupedData.items(), desc="Bulk-inserting datasets by type"
        ):
            refs = self.registry.insertDatasets(
                datasetType,
                dataIds=groupForType.keys(),
                run=run,
                expand=self.datastore.needs_expanded_data_ids(transfer, datasetType),
                idGenerationMode=idGenerationMode,
            )
            # Append those resolved DatasetRefs to the new lists we set up for
            # them.
            for ref, (_, resolvedRefs) in zip(refs, groupForType.values()):
                resolvedRefs.append(ref)

        # Go back to the original FileDatasets to replace their refs with the
        # new resolved ones.
        for groupForType in progress.iter_chunks(
            groupedData.values(), desc="Reassociating resolved dataset refs with files"
        ):
            for dataset, resolvedRefs in groupForType.values():
                dataset.refs = resolvedRefs

        # Bulk-insert everything into Datastore.
        self.datastore.ingest(*datasets, transfer=transfer, record_validation_info=record_validation_info)

    @contextlib.contextmanager
    def export(
        self,
        *,
        directory: Optional[str] = None,
        filename: Optional[str] = None,
        format: Optional[str] = None,
        transfer: Optional[str] = None,
    ) -> Iterator[RepoExportContext]:
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
        BackendClass = get_class_of(self._config["repo_transfer_formats"][format]["export"])
        with open(filename, "w") as stream:
            backend = BackendClass(stream, universe=self.registry.dimensions)
            try:
                helper = RepoExportContext(
                    self.registry, self.datastore, backend=backend, directory=directory, transfer=transfer
                )
                yield helper
            except BaseException:
                raise
            else:
                helper._finish()

    def import_(
        self,
        *,
        directory: Optional[str] = None,
        filename: Union[str, TextIO, None] = None,
        format: Optional[str] = None,
        transfer: Optional[str] = None,
        skip_dimensions: Optional[Set] = None,
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
        reuseIds: bool = False,
    ) -> None:
        """Import datasets into this repository that were exported from a
        different butler repository via `~lsst.daf.butler.Butler.export`.

        Parameters
        ----------
        directory : `str`, optional
            Directory containing dataset files to import from. If `None`,
            ``filename`` and all dataset file paths specified therein must
            be absolute.
        filename : `str` or `TextIO`, optional
            A stream or name of file that contains database information
            associated with the exported datasets, typically generated by
            `~lsst.daf.butler.Butler.export`.  If this a string (name) and
            is not an absolute path, does not exist in the current working
            directory, and ``directory`` is not `None`, it is assumed to be in
            ``directory``. Defaults to "export.{format}".
        format : `str`, optional
            File format for ``filename``.  If `None`, the extension of
            ``filename`` will be used.
        transfer : `str`, optional
            Transfer mode passed to `~lsst.daf.butler.Datastore.ingest`.
        skip_dimensions : `set`, optional
            Names of dimensions that should be skipped and not imported.
        idGenerationMode : `DatasetIdGenEnum`, optional
            Specifies option for generating dataset IDs when IDs are not
            provided or their type does not match backend type. By default
            unique IDs are generated for each inserted dataset.
        reuseIds : `bool`, optional
            If `True` then forces re-use of imported dataset IDs for integer
            IDs which are normally generated as auto-incremented; exception
            will be raised if imported IDs clash with existing ones. This
            option has no effect on the use of globally-unique IDs which are
            always re-used (or generated if integer IDs are being imported).

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
        BackendClass = get_class_of(self._config["repo_transfer_formats"][format]["import"])

        def doImport(importStream: TextIO) -> None:
            backend = BackendClass(importStream, self.registry)
            backend.register()
            with self.transaction():
                backend.load(
                    self.datastore,
                    directory=directory,
                    transfer=transfer,
                    skip_dimensions=skip_dimensions,
                    idGenerationMode=idGenerationMode,
                    reuseIds=reuseIds,
                )

        if isinstance(filename, str):
            with open(filename, "r") as stream:
                doImport(stream)
        else:
            doImport(filename)

    def transfer_from(
        self,
        source_butler: Butler,
        source_refs: Iterable[DatasetRef],
        transfer: str = "auto",
        id_gen_map: Dict[str, DatasetIdGenEnum] | None = None,
        skip_missing: bool = True,
        register_dataset_types: bool = False,
        transfer_dimensions: bool = False,
    ) -> List[DatasetRef]:
        """Transfer datasets to this Butler from a run in another Butler.

        Parameters
        ----------
        source_butler : `Butler`
            Butler from which the datasets are to be transferred.
        source_refs : iterable of `DatasetRef`
            Datasets defined in the source butler that should be transferred to
            this butler.
        transfer : `str`, optional
            Transfer mode passed to `~lsst.daf.butler.Datastore.transfer_from`.
        id_gen_map : `dict` of [`str`, `DatasetIdGenEnum`], optional
            A mapping of dataset type to ID generation mode. Only used if
            the source butler is using integer IDs. Should not be used
            if this receiving butler uses integer IDs. Without this dataset
            import always uses unique.
        skip_missing : `bool`
            If `True`, datasets with no datastore artifact associated with
            them are not transferred. If `False` a registry entry will be
            created even if no datastore record is created (and so will
            look equivalent to the dataset being unstored).
        register_dataset_types : `bool`
            If `True` any missing dataset types are registered. Otherwise
            an exception is raised.
        transfer_dimensions : `bool`, optional
            If `True`, dimension record data associated with the new datasets
            will be transferred.

        Returns
        -------
        refs : `list` of `DatasetRef`
            The refs added to this Butler.

        Notes
        -----
        Requires that any dimension definitions are already present in the
        receiving Butler. The datastore artifact has to exist for a transfer
        to be made but non-existence is not an error.

        Datasets that already exist in this run will be skipped.

        The datasets are imported as part of a transaction, although
        dataset types are registered before the transaction is started.
        This means that it is possible for a dataset type to be registered
        even though transfer has failed.
        """
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        progress = Progress("lsst.daf.butler.Butler.transfer_from", level=VERBOSE)

        # Will iterate through the refs multiple times so need to convert
        # to a list if this isn't a collection.
        if not isinstance(source_refs, collections.abc.Collection):
            source_refs = list(source_refs)

        original_count = len(source_refs)
        log.info("Transferring %d datasets into %s", original_count, str(self))

        if id_gen_map is None:
            id_gen_map = {}

        # In some situations the datastore artifact may be missing
        # and we do not want that registry entry to be imported.
        # Asking datastore is not sufficient, the records may have been
        # purged, we have to ask for the (predicted) URI and check
        # existence explicitly. Execution butler is set up exactly like
        # this with no datastore records.
        artifact_existence: Dict[ResourcePath, bool] = {}
        if skip_missing:
            dataset_existence = source_butler.datastore.mexists(
                source_refs, artifact_existence=artifact_existence
            )
            source_refs = [ref for ref, exists in dataset_existence.items() if exists]
            filtered_count = len(source_refs)
            log.verbose(
                "%d datasets removed because the artifact does not exist. Now have %d.",
                original_count - filtered_count,
                filtered_count,
            )

        # Importing requires that we group the refs by dataset type and run
        # before doing the import.
        source_dataset_types = set()
        grouped_refs = defaultdict(list)
        grouped_indices = defaultdict(list)
        for i, ref in enumerate(source_refs):
            grouped_refs[ref.datasetType, ref.run].append(ref)
            grouped_indices[ref.datasetType, ref.run].append(i)
            source_dataset_types.add(ref.datasetType)

        # Check to see if the dataset type in the source butler has
        # the same definition in the target butler and register missing
        # ones if requested. Registration must happen outside a transaction.
        newly_registered_dataset_types = set()
        for datasetType in source_dataset_types:
            if register_dataset_types:
                # Let this raise immediately if inconsistent. Continuing
                # on to find additional inconsistent dataset types
                # might result in additional unwanted dataset types being
                # registered.
                if self.registry.registerDatasetType(datasetType):
                    newly_registered_dataset_types.add(datasetType)
            else:
                # If the dataset type is missing, let it fail immediately.
                target_dataset_type = self.registry.getDatasetType(datasetType.name)
                if target_dataset_type != datasetType:
                    raise ConflictingDefinitionError(
                        "Source butler dataset type differs from definition"
                        f" in target butler: {datasetType} !="
                        f" {target_dataset_type}"
                    )
        if newly_registered_dataset_types:
            # We may have registered some even if there were inconsistencies
            # but should let people know (or else remove them again).
            log.log(
                VERBOSE,
                "Registered the following dataset types in the target Butler: %s",
                ", ".join(d.name for d in newly_registered_dataset_types),
            )
        else:
            log.log(VERBOSE, "All required dataset types are known to the target Butler")

        dimension_records: Dict[DimensionElement, Dict[DataCoordinate, DimensionRecord]] = defaultdict(dict)
        if transfer_dimensions:
            # Collect all the dimension records for these refs.
            # All dimensions are to be copied but the list of valid dimensions
            # come from this butler's universe.
            elements = frozenset(
                element
                for element in self.registry.dimensions.getStaticElements()
                if element.hasTable() and element.viewOf is None
            )
            dataIds = set(ref.dataId for ref in source_refs)
            # This logic comes from saveDataIds.
            for dataId in dataIds:
                # Should be a no-op if the ref has already been expanded.
                dataId = source_butler.registry.expandDataId(dataId)
                # If this butler doesn't know about a dimension in the source
                # butler things will break later.
                for record in dataId.records.values():
                    if record is not None and record.definition in elements:
                        dimension_records[record.definition].setdefault(record.dataId, record)

        # The returned refs should be identical for UUIDs.
        # For now must also support integers and so need to retain the
        # newly-created refs from this registry.
        # Pre-size it so we can assign refs into the correct slots
        transferred_refs_tmp: List[Optional[DatasetRef]] = [None] * len(source_refs)
        default_id_gen = DatasetIdGenEnum.UNIQUE

        handled_collections: Set[str] = set()

        # Do all the importing in a single transaction.
        with self.transaction():
            if dimension_records:
                log.verbose("Ensuring that dimension records exist for transferred datasets.")
                for element, r in dimension_records.items():
                    records = [r[dataId] for dataId in r]
                    # Assume that if the record is already present that we can
                    # use it without having to check that the record metadata
                    # is consistent.
                    self.registry.insertDimensionData(element, *records, skip_existing=True)

            for (datasetType, run), refs_to_import in progress.iter_item_chunks(
                grouped_refs.items(), desc="Importing to registry by run and dataset type"
            ):
                if run not in handled_collections:
                    run_doc = source_butler.registry.getCollectionDocumentation(run)
                    registered = self.registry.registerRun(run, doc=run_doc)
                    handled_collections.add(run)
                    if registered:
                        log.log(VERBOSE, "Creating output run %s", run)

                id_generation_mode = default_id_gen
                if isinstance(refs_to_import[0].id, int):
                    # ID generation mode might need to be overridden when
                    # targetting UUID
                    id_generation_mode = id_gen_map.get(datasetType.name, default_id_gen)

                n_refs = len(refs_to_import)
                log.verbose(
                    "Importing %d ref%s of dataset type %s into run %s",
                    n_refs,
                    "" if n_refs == 1 else "s",
                    datasetType.name,
                    run,
                )

                # No way to know if this butler's registry uses UUID.
                # We have to trust the caller on this. If it fails they will
                # have to change their approach. We can't catch the exception
                # and retry with unique because that will mess up the
                # transaction handling. We aren't allowed to ask the registry
                # manager what type of ID it is using.
                imported_refs = self.registry._importDatasets(
                    refs_to_import, idGenerationMode=id_generation_mode, expand=False
                )

                # Map them into the correct slots to match the initial order
                for i, ref in zip(grouped_indices[datasetType, run], imported_refs):
                    transferred_refs_tmp[i] = ref

            # Mypy insists that we might have None in here so we have to make
            # that explicit by assigning to a new variable and filtering out
            # something that won't be there.
            transferred_refs = [ref for ref in transferred_refs_tmp if ref is not None]

            # Check consistency
            assert len(source_refs) == len(transferred_refs), "Different number of refs imported than given"

            log.verbose("Imported %d datasets into destination butler", len(transferred_refs))

            # The transferred refs need to be reordered to match the original
            # ordering given by the caller. Without this the datastore transfer
            # will be broken.

            # Ask the datastore to transfer. The datastore has to check that
            # the source datastore is compatible with the target datastore.
            self.datastore.transfer_from(
                source_butler.datastore,
                source_refs,
                local_refs=transferred_refs,
                transfer=transfer,
                artifact_existence=artifact_existence,
            )

        return transferred_refs

    def validateConfiguration(
        self,
        logFailures: bool = False,
        datasetTypeNames: Optional[Iterable[str]] = None,
        ignore: Iterable[str] | None = None,
    ) -> None:
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
            datasetTypes = [
                e for e in datasetTypes if e.name not in ignore and e.nameAndComponent()[0] not in ignore
            ]
        else:
            ignore = set()

        # Find all the registered instruments
        instruments = set(record.name for record in self.registry.queryDimensionRecords("instrument"))

        # For each datasetType that has an instrument dimension, create
        # a DatasetRef for each defined instrument
        datasetRefs = []

        for datasetType in datasetTypes:
            if "instrument" in datasetType.dimensions:
                for instrument in instruments:
                    datasetRef = DatasetRef(
                        datasetType, {"instrument": instrument}, conform=False  # type: ignore
                    )
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
                            log.critical("Key '%s' does not correspond to a DatasetType or StorageClass", key)
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
                        log.critical("Key '%s' has unsupported DataId override", key)
                    failedDataId.add(key)
                elif key.dataId["instrument"] not in instruments:
                    if logFailures:
                        log.critical("Key '%s' has unknown instrument", key)
                    failedDataId.add(key)

        messages = []

        if datastoreErrorStr:
            messages.append(datastoreErrorStr)

        for failed, msg in (
            (failedNames, "Keys without corresponding DatasetType or StorageClass entry: "),
            (failedDataId, "Keys with bad DataId entries: "),
        ):
            if failed:
                msg += ", ".join(str(k) for k in failed)
                messages.append(msg)

        if messages:
            raise ValidationError(";\n".join(messages))

    @property
    def collections(self) -> Sequence[str]:
        """The collections to search by default, in order
        (`Sequence` [ `str` ]).

        This is an alias for ``self.registry.defaults.collections``.  It cannot
        be set directly in isolation, but all defaults may be changed together
        by assigning a new `RegistryDefaults` instance to
        ``self.registry.defaults``.
        """
        return self.registry.defaults.collections

    @property
    def run(self) -> Optional[str]:
        """Name of the run this butler writes outputs to by default (`str` or
        `None`).

        This is an alias for ``self.registry.defaults.run``.  It cannot be set
        directly in isolation, but all defaults may be changed together by
        assigning a new `RegistryDefaults` instance to
        ``self.registry.defaults``.
        """
        return self.registry.defaults.run

    @property
    def dimensions(self) -> DimensionUniverse:
        # Docstring inherited.
        return self.registry.dimensions

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

    _allow_put_of_predefined_dataset: bool
    """Allow a put to succeed even if there is already a registry entry for it
    but not a datastore record. (`bool`)."""
