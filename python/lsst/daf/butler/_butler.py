# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from __future__ import annotations

__all__ = ["Butler"]

from abc import abstractmethod
from collections.abc import Collection, Iterable, Sequence
from contextlib import AbstractContextManager
from typing import Any, TextIO

from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils import doImportType
from lsst.utils.logging import getLogger

from ._butler_config import ButlerConfig
from ._butler_repo_index import ButlerRepoIndex
from ._config import Config, ConfigSubset
from ._dataset_existence import DatasetExistence
from ._dataset_ref import DatasetIdGenEnum, DatasetRef
from ._dataset_type import DatasetType
from ._deferredDatasetHandle import DeferredDatasetHandle
from ._file_dataset import FileDataset
from ._limited_butler import LimitedButler
from ._storage_class import StorageClass
from .datastore import DatasetRefURIs, Datastore
from .dimensions import DataId, DimensionConfig
from .registry import Registry, RegistryConfig, _RegistryFactory
from .repo_relocation import BUTLER_ROOT_TAG
from .transfers import RepoExportContext

log = getLogger(__name__)


class Butler(LimitedButler):
    """Interface for data butler and factory for Butler instances.

    Parameters
    ----------
    config : `ButlerConfig`, `Config` or `str`, optional.
        Configuration. Anything acceptable to the `ButlerConfig` constructor.
        If a directory path is given the configuration will be read from a
        ``butler.yaml`` file in that location. If `None` is given default
        values will be used. If ``config`` contains "cls" key then its value is
        used as a name of butler class and it must be a sub-class of this
        class, otherwise `DirectButler` is instantiated.
    collections : `str` or `~collections.abc.Iterable` [ `str` ], optional
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
    **kwargs : `Any`
        Additional keyword arguments passed to a constructor of actual butler
        class.

    Notes
    -----
    The preferred way to instantiate Butler is via the `from_config` method.
    The call to ``Butler(...)`` is equivalent to ``Butler.from_config(...)``,
    but ``mypy`` will complain about the former.
    """

    def __new__(
        cls,
        config: Config | ResourcePathExpression | None = None,
        *,
        collections: Any = None,
        run: str | None = None,
        searchPaths: Sequence[ResourcePathExpression] | None = None,
        writeable: bool | None = None,
        inferDefaults: bool = True,
        **kwargs: Any,
    ) -> Butler:
        if cls is Butler:
            cls = cls._find_butler_class(config, searchPaths)
        # Note: we do not pass any parameters to __new__, Python will pass them
        # to __init__ after __new__ returns sub-class instance.
        return super().__new__(cls)

    @staticmethod
    def _find_butler_class(
        config: Config | ResourcePathExpression | None = None,
        searchPaths: Sequence[ResourcePathExpression] | None = None,
    ) -> type[Butler]:
        """Find actual class to instantiate."""
        butler_class_name: str | None = None
        if config is not None:
            # Check for optional "cls" key in config.
            if not isinstance(config, Config):
                config = ButlerConfig(config, searchPaths=searchPaths)
            butler_class_name = config.get("cls")

        # Make DirectButler if class is not specified.
        butler_class: type[Butler]
        if butler_class_name is None:
            from .direct_butler import DirectButler

            butler_class = DirectButler
        else:
            butler_class = doImportType(butler_class_name)
            if not issubclass(butler_class, Butler):
                raise TypeError(f"{butler_class_name} is not a subclass of Butler")
        return butler_class

    @classmethod
    def from_config(
        cls,
        config: Config | ResourcePathExpression | None = None,
        *,
        collections: Any = None,
        run: str | None = None,
        searchPaths: Sequence[ResourcePathExpression] | None = None,
        writeable: bool | None = None,
        inferDefaults: bool = True,
        **kwargs: Any,
    ) -> Butler:
        """Create butler instance from configuration.

        Parameters
        ----------
        config : `ButlerConfig`, `Config` or `str`, optional.
            Configuration. Anything acceptable to the `ButlerConfig`
            constructor. If a directory path is given the configuration will be
            read from a ``butler.yaml`` file in that location. If `None` is
            given default values will be used. If ``config`` contains "cls" key
            then its value is used as a name of butler class and it must be a
            sub-class of this class, otherwise `DirectButler` is instantiated.
        collections : `str` or `~collections.abc.Iterable` [ `str` ], optional
            An expression specifying the collections to be searched (in order)
            when reading datasets.
            This may be a `str` collection name or an iterable thereof.
            See :ref:`daf_butler_collection_expressions` for more information.
            These collections are not registered automatically and must be
            manually registered before they are used by any method, but they
            may be manually registered after the `Butler` is initialized.
        run : `str`, optional
            Name of the `~CollectionType.RUN` collection new datasets should be
            inserted into.  If ``collections`` is `None` and ``run`` is not
            `None`, ``collections`` will be set to ``[run]``.  If not `None`,
            this collection will automatically be registered.  If this is not
            set (and ``writeable`` is not set either), a read-only butler will
            be created.
        searchPaths : `list` of `str`, optional
            Directory paths to search when calculating the full Butler
            configuration.  Not used if the supplied config is already a
            `ButlerConfig`.
        writeable : `bool`, optional
            Explicitly sets whether the butler supports write operations.  If
            not provided, a read-write butler is created if any of ``run``,
            ``tags``, or ``chains`` is non-empty.
        inferDefaults : `bool`, optional
            If `True` (default) infer default data ID values from the values
            present in the datasets in ``collections``: if all collections have
            the same value (or no value) for a governor dimension, that value
            will be the default for that dimension.  Nonexistent collections
            are ignored.  If a default value is provided explicitly for a
            governor dimension via ``**kwargs``, no default will be inferred
            for that dimension.
        **kwargs : `Any`
            Additional keyword arguments passed to a constructor of actual
            butler class.

        Notes
        -----
        Calling this factory method is identical to calling
        ``Butler(config, ...)``. Its only raison d'être is that ``mypy``
        complains about ``Butler()`` call.

        Examples
        --------
        While there are many ways to control exactly how a `Butler` interacts
        with the collections in its `Registry`, the most common cases are still
        simple.

        For a read-only `Butler` that searches one collection, do::

            butler = Butler.from_config(
                "/path/to/repo", collections=["u/alice/DM-50000"]
            )

        For a read-write `Butler` that writes to and reads from a
        `~CollectionType.RUN` collection::

            butler = Butler.from_config(
                "/path/to/repo", run="u/alice/DM-50000/a"
            )

        The `Butler` passed to a ``PipelineTask`` is often much more complex,
        because we want to write to one `~CollectionType.RUN` collection but
        read from several others (as well)::

            butler = Butler.from_config(
                "/path/to/repo",
                run="u/alice/DM-50000/a",
                collections=[
                    "u/alice/DM-50000/a", "u/bob/DM-49998", "HSC/defaults"
                ]
            )

        This butler will `put` new datasets to the run ``u/alice/DM-50000/a``.
        Datasets will be read first from that run (since it appears first in
        the chain), and then from ``u/bob/DM-49998`` and finally
        ``HSC/defaults``.

        Finally, one can always create a `Butler` with no collections::

            butler = Butler.from_config("/path/to/repo", writeable=True)

        This can be extremely useful when you just want to use
        ``butler.registry``, e.g. for inserting dimension data or managing
        collections, or when the collections you want to use with the butler
        are not consistent. Passing ``writeable`` explicitly here is only
        necessary if you want to be able to make changes to the repo - usually
        the value for ``writeable`` can be guessed from the collection
        arguments provided, but it defaults to `False` when there are not
        collection arguments.
        """
        cls = cls._find_butler_class(config, searchPaths)
        return cls(
            config,
            collections=collections,
            run=run,
            searchPaths=searchPaths,
            writeable=writeable,
            inferDefaults=inferDefaults,
            **kwargs,
        )

    @staticmethod
    def makeRepo(
        root: ResourcePathExpression,
        config: Config | str | None = None,
        dimensionConfig: Config | str | None = None,
        standalone: bool = False,
        searchPaths: list[str] | None = None,
        forceConfigRoot: bool = True,
        outfile: ResourcePathExpression | None = None,
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
        if isinstance(config, ButlerConfig | ConfigSubset):
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
        datastoreClass: type[Datastore] = imported_class
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
        if (obscore_config_key := ("registry", "managers", "obscore", "config")) in config:
            config_to_write = config.copy()
            del config_to_write[obscore_config_key]
            config_to_write.dumpToUri(configURI, overwrite=overwrite)
            # configFile attribute is updated, need to copy it to original.
            config.configFile = config_to_write.configFile
        else:
            config.dumpToUri(configURI, overwrite=overwrite)

        # Create Registry and populate tables
        registryConfig = RegistryConfig(config.get("registry"))
        dimensionConfig = DimensionConfig(dimensionConfig)
        _RegistryFactory(registryConfig).create_from_config(
            dimensionConfig=dimensionConfig, butlerRoot=root_uri
        )

        log.verbose("Wrote new Butler configuration file to %s", configURI)

        return config

    @classmethod
    def get_repo_uri(cls, label: str, return_label: bool = False) -> ResourcePath:
        """Look up the label in a butler repository index.

        Parameters
        ----------
        label : `str`
            Label of the Butler repository to look up.
        return_label : `bool`, optional
            If ``label`` cannot be found in the repository index (either
            because index is not defined or ``label`` is not in the index) and
            ``return_label`` is `True` then return ``ResourcePath(label)``.
            If ``return_label`` is `False` (default) then an exception will be
            raised instead.

        Returns
        -------
        uri : `lsst.resources.ResourcePath`
            URI to the Butler repository associated with the given label or
            default value if it is provided.

        Raises
        ------
        KeyError
            Raised if the label is not found in the index, or if an index
            is not defined, and ``return_label`` is `False`.

        Notes
        -----
        See `~lsst.daf.butler.ButlerRepoIndex` for details on how the
        information is discovered.
        """
        return ButlerRepoIndex.get_repo_uri(label, return_label)

    @classmethod
    def get_known_repos(cls) -> set[str]:
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

    @abstractmethod
    def transaction(self) -> AbstractContextManager[None]:
        """Context manager supporting `Butler` transactions.

        Transactions can be nested.
        """
        raise NotImplementedError()

    @abstractmethod
    def put(
        self,
        obj: Any,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        run: str | None = None,
        **kwargs: Any,
    ) -> DatasetRef:
        """Store and register a dataset.

        Parameters
        ----------
        obj : `object`
            The dataset.
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` is provided, ``dataId`` should be `None`.
            Otherwise the `DatasetType` or name thereof. If a fully resolved
            `DatasetRef` is given the run and ID are used directly.
        dataId : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the second argument.
        run : `str`, optional
            The name of the run the dataset should be added to, overriding
            ``self.run``. Not used if a resolved `DatasetRef` is provided.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters. Not used if a resolve `DatasetRef` is provided.

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
        raise NotImplementedError()

    @abstractmethod
    def getDeferred(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        parameters: dict | None = None,
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
            Raised if no matching dataset exists in the `Registry` or
            datastore.
        ValueError
            Raised if a resolved `DatasetRef` was passed as an input, but it
            differs from the one found in the registry.
        TypeError
            Raised if no collections were provided.
        """
        raise NotImplementedError()

    @abstractmethod
    def get(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        parameters: dict[str, Any] | None = None,
        collections: Any = None,
        storageClass: StorageClass | str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Retrieve a stored dataset.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
            If a resolved `DatasetRef`, the associated dataset
            is returned directly without additional querying.
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
        raise NotImplementedError()

    @abstractmethod
    def getURIs(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        predict: bool = False,
        collections: Any = None,
        run: str | None = None,
        **kwargs: Any,
    ) -> DatasetRefURIs:
        """Return the URIs associated with the dataset.

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
        raise NotImplementedError()

    @abstractmethod
    def getURI(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        predict: bool = False,
        collections: Any = None,
        run: str | None = None,
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
        raise NotImplementedError()

    @abstractmethod
    def retrieveArtifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool = False,
    ) -> list[ResourcePath]:
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
        raise NotImplementedError()

    @abstractmethod
    def exists(
        self,
        dataset_ref_or_type: DatasetRef | DatasetType | str,
        /,
        data_id: DataId | None = None,
        *,
        full_check: bool = True,
        collections: Any = None,
        **kwargs: Any,
    ) -> DatasetExistence:
        """Indicate whether a dataset is known to Butler registry and
        datastore.

        Parameters
        ----------
        dataset_ref_or_type : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        data_id : `dict` or `DataCoordinate`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        full_check : `bool`, optional
            If `True`, an additional check will be made for dataset artifact
            existence. This will involve additional overhead due to the need
            to query an external system. If `False` registry and datastore
            will solely be asked if they know about the dataset but no
            check for the artifact will be performed.
        collections : Any, optional
            Collections to be searched, overriding ``self.collections``.
            Can be any of the types supported by the ``collections`` argument
            to butler construction.
        **kwargs
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        existence : `DatasetExistence`
            Object indicating whether the dataset is known to registry and
            datastore. Evaluates to `True` if the dataset is present and known
            to both.
        """
        raise NotImplementedError()

    @abstractmethod
    def _exists_many(
        self,
        refs: Iterable[DatasetRef],
        /,
        *,
        full_check: bool = True,
    ) -> dict[DatasetRef, DatasetExistence]:
        """Indicate whether multiple datasets are known to Butler registry and
        datastore.

        This is an experimental API that may change at any moment.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets to be checked.
        full_check : `bool`, optional
            If `True`, an additional check will be made for dataset artifact
            existence. This will involve additional overhead due to the need
            to query an external system. If `False` registry and datastore
            will solely be asked if they know about the dataset but no
            check for the artifact will be performed.

        Returns
        -------
        existence : dict of [`DatasetRef`, `DatasetExistence`]
            Mapping from the given dataset refs to an enum indicating the
            status of the dataset in registry and datastore.
            Each value evaluates to `True` if the dataset is present and known
            to both.
        """
        raise NotImplementedError()

    @abstractmethod
    def removeRuns(self, names: Iterable[str], unstore: bool = True) -> None:
        """Remove one or more `~CollectionType.RUN` collections and the
        datasets within them.

        Parameters
        ----------
        names : `~collections.abc.Iterable` [ `str` ]
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
        raise NotImplementedError()

    @abstractmethod
    def ingest(
        self,
        *datasets: FileDataset,
        transfer: str | None = "auto",
        run: str | None = None,
        idGenerationMode: DatasetIdGenEnum | None = None,
        record_validation_info: bool = True,
    ) -> None:
        """Store and register one or more datasets that already exist on disk.

        Parameters
        ----------
        datasets : `FileDataset`
            Each positional argument is a struct containing information about
            a file to be ingested, including its URI (either absolute or
            relative to the datastore root, if applicable), a resolved
            `DatasetRef`, and optionally a formatter class or its
            fully-qualified string name.  If a formatter is not provided, the
            formatter that would be used for `put` is assumed.  On successful
            ingest all `FileDataset.formatter` attributes will be set to the
            formatter class used. `FileDataset.path` attributes may be modified
            to put paths in whatever the datastore considers a standardized
            form.
        transfer : `str`, optional
            If not `None`, must be one of 'auto', 'move', 'copy', 'direct',
            'split', 'hardlink', 'relsymlink' or 'symlink', indicating how to
            transfer the file.
        run : `str`, optional
            The name of the run ingested datasets should be added to,
            overriding ``self.run``. This parameter is now deprecated since
            the run is encoded in the ``FileDataset``.
        idGenerationMode : `DatasetIdGenEnum`, optional
            Specifies option for generating dataset IDs. Parameter is
            deprecated.
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
        raise NotImplementedError()

    @abstractmethod
    def export(
        self,
        *,
        directory: str | None = None,
        filename: str | None = None,
        format: str | None = None,
        transfer: str | None = None,
    ) -> AbstractContextManager[RepoExportContext]:
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
        raise NotImplementedError()

    @abstractmethod
    def import_(
        self,
        *,
        directory: ResourcePathExpression | None = None,
        filename: ResourcePathExpression | TextIO | None = None,
        format: str | None = None,
        transfer: str | None = None,
        skip_dimensions: set | None = None,
    ) -> None:
        """Import datasets into this repository that were exported from a
        different butler repository via `~lsst.daf.butler.Butler.export`.

        Parameters
        ----------
        directory : `~lsst.resources.ResourcePathExpression`, optional
            Directory containing dataset files to import from. If `None`,
            ``filename`` and all dataset file paths specified therein must
            be absolute.
        filename : `~lsst.resources.ResourcePathExpression` or `TextIO`
            A stream or name of file that contains database information
            associated with the exported datasets, typically generated by
            `~lsst.daf.butler.Butler.export`.  If this a string (name) or
            `~lsst.resources.ResourcePath` and is not an absolute path,
            it will first be looked for relative to  ``directory`` and if not
            found there it will be looked for in the current working
            directory. Defaults to "export.{format}".
        format : `str`, optional
            File format for ``filename``.  If `None`, the extension of
            ``filename`` will be used.
        transfer : `str`, optional
            Transfer mode passed to `~lsst.daf.butler.Datastore.ingest`.
        skip_dimensions : `set`, optional
            Names of dimensions that should be skipped and not imported.

        Raises
        ------
        TypeError
            Raised if the set of arguments passed is inconsistent, or if the
            butler is read-only.
        """
        raise NotImplementedError()

    @abstractmethod
    def transfer_from(
        self,
        source_butler: LimitedButler,
        source_refs: Iterable[DatasetRef],
        transfer: str = "auto",
        skip_missing: bool = True,
        register_dataset_types: bool = False,
        transfer_dimensions: bool = False,
    ) -> Collection[DatasetRef]:
        """Transfer datasets to this Butler from a run in another Butler.

        Parameters
        ----------
        source_butler : `LimitedButler`
            Butler from which the datasets are to be transferred. If data IDs
            in ``source_refs`` are not expanded then this has to be a full
            `Butler` whose registry will be used to expand data IDs.
        source_refs : iterable of `DatasetRef`
            Datasets defined in the source butler that should be transferred to
            this butler.
        transfer : `str`, optional
            Transfer mode passed to `~lsst.daf.butler.Datastore.transfer_from`.
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
        The datastore artifact has to exist for a transfer
        to be made but non-existence is not an error.

        Datasets that already exist in this run will be skipped.

        The datasets are imported as part of a transaction, although
        dataset types are registered before the transaction is started.
        This means that it is possible for a dataset type to be registered
        even though transfer has failed.
        """
        raise NotImplementedError()

    @abstractmethod
    def validateConfiguration(
        self,
        logFailures: bool = False,
        datasetTypeNames: Iterable[str] | None = None,
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
        raise NotImplementedError()

    @property
    @abstractmethod
    def collections(self) -> Sequence[str]:
        """The collections to search by default, in order
        (`~collections.abc.Sequence` [ `str` ]).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def run(self) -> str | None:
        """Name of the run this butler writes outputs to by default (`str` or
        `None`).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def registry(self) -> Registry:
        """The object that manages dataset metadata and relationships
        (`Registry`).

        Many operations that don't involve reading or writing butler datasets
        are accessible only via `Registry` methods. Eventually these methods
        will be replaced by equivalent `Butler` methods.
        """
        raise NotImplementedError()
