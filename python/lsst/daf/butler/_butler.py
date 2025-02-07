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

import dataclasses
import urllib.parse
import uuid
from abc import abstractmethod
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager
from types import EllipsisType
from typing import TYPE_CHECKING, Any, TextIO

from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils import doImportType
from lsst.utils.iteration import ensure_iterable
from lsst.utils.logging import getLogger

from ._butler_collections import ButlerCollections
from ._butler_config import ButlerConfig, ButlerType
from ._butler_instance_options import ButlerInstanceOptions
from ._butler_repo_index import ButlerRepoIndex
from ._config import Config, ConfigSubset
from ._exceptions import EmptyQueryResultError, InvalidQueryError
from ._limited_butler import LimitedButler
from ._query_all_datasets import QueryAllDatasetsParameters
from .datastore import Datastore
from .dimensions import DataCoordinate, DimensionConfig
from .registry import RegistryConfig, _RegistryFactory
from .repo_relocation import BUTLER_ROOT_TAG
from .utils import has_globs

if TYPE_CHECKING:
    from ._dataset_existence import DatasetExistence
    from ._dataset_provenance import DatasetProvenance
    from ._dataset_ref import DatasetId, DatasetRef
    from ._dataset_type import DatasetType
    from ._deferredDatasetHandle import DeferredDatasetHandle
    from ._file_dataset import FileDataset
    from ._labeled_butler_factory import LabeledButlerFactoryProtocol
    from ._storage_class import StorageClass
    from ._timespan import Timespan
    from .datastore import DatasetRefURIs
    from .dimensions import DataId, DimensionGroup, DimensionRecord
    from .queries import Query
    from .registry import CollectionArgType, Registry
    from .transfers import RepoExportContext

_LOG = getLogger(__name__)


@dataclasses.dataclass
class ParsedButlerDatasetURI:
    label: str
    dataset_id: uuid.UUID
    uri: str


@dataclasses.dataclass
class SpecificButlerDataset:
    butler: Butler
    dataset: DatasetRef | None


class Butler(LimitedButler):  # numpydoc ignore=PR02
    """Interface for data butler and factory for Butler instances.

    Parameters
    ----------
    config : `ButlerConfig`, `Config` or `str`, optional
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
    without_datastore : `bool`, optional
        If `True` do not attach a datastore to this butler. Any attempts
        to use a datastore will fail.
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
        without_datastore: bool = False,
        **kwargs: Any,
    ) -> Butler:
        if cls is Butler:
            return Butler.from_config(
                config=config,
                collections=collections,
                run=run,
                searchPaths=searchPaths,
                writeable=writeable,
                inferDefaults=inferDefaults,
                without_datastore=without_datastore,
                **kwargs,
            )

        # Note: we do not pass any parameters to __new__, Python will pass them
        # to __init__ after __new__ returns sub-class instance.
        return super().__new__(cls)

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
        without_datastore: bool = False,
        **kwargs: Any,
    ) -> Butler:
        """Create butler instance from configuration.

        Parameters
        ----------
        config : `ButlerConfig`, `Config` or `str`, optional
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
        without_datastore : `bool`, optional
            If `True` do not attach a datastore to this butler. Any attempts
            to use a datastore will fail.
        **kwargs : `Any`
            Default data ID key-value pairs.  These may only identify
            "governor" dimensions like ``instrument`` and ``skymap``.

        Returns
        -------
        butler : `Butler`
            A `Butler` constructed from the given configuration.

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
                    "u/alice/DM-50000/a",
                    "u/bob/DM-49998",
                    "HSC/defaults",
                ],
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
        # DirectButler used to have a way to specify a "copy constructor" by
        # passing the "butler" parameter to its constructor.  This has
        # been moved out of the constructor into Butler.clone().
        butler = kwargs.pop("butler", None)
        if butler is not None:
            if not isinstance(butler, Butler):
                raise TypeError("'butler' parameter must be a Butler instance")
            if config is not None or searchPaths is not None or writeable is not None:
                raise TypeError(
                    "Cannot pass 'config', 'searchPaths', or 'writeable' arguments with 'butler' argument."
                )
            return butler.clone(collections=collections, run=run, inferDefaults=inferDefaults, dataId=kwargs)

        options = ButlerInstanceOptions(
            collections=collections, run=run, writeable=writeable, inferDefaults=inferDefaults, kwargs=kwargs
        )

        # Load the Butler configuration.  This may involve searching the
        # environment to locate a configuration file.
        butler_config = ButlerConfig(config, searchPaths=searchPaths, without_datastore=without_datastore)
        butler_type = butler_config.get_butler_type()

        # Make DirectButler if class is not specified.
        match butler_type:
            case ButlerType.DIRECT:
                from .direct_butler import DirectButler

                return DirectButler.create_from_config(
                    butler_config,
                    options=options,
                    without_datastore=without_datastore,
                )
            case ButlerType.REMOTE:
                from .remote_butler import RemoteButlerFactory

                # Assume this is being created by a client who would like
                # default caching of remote datasets.
                factory = RemoteButlerFactory.create_factory_from_config(butler_config)
                return factory.create_butler_with_credentials_from_environment(
                    butler_options=options, use_disabled_datastore_cache=False
                )
            case _:
                raise TypeError(f"Unknown Butler type '{butler_type}'")

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

        _LOG.verbose("Wrote new Butler configuration file to %s", configURI)

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

    @classmethod
    def parse_dataset_uri(cls, uri: str) -> ParsedButlerDatasetURI:
        """Extract the butler label and dataset ID from a dataset URI.

        Parameters
        ----------
        uri : `str`
            The dataset URI to parse.

        Returns
        -------
        parsed : `ParsedButlerDatasetURI`
            The label associated with the butler repository from which this
            dataset originates and the ID of the dataset.

        Notes
        -----
        Supports dataset URIs of the forms
        ``ivo://org.rubinobs/usdac/dr1?repo=butler_label&id=UUID`` (see
        DMTN-302) and ``butler://butler_label/UUID``. The ``butler`` URI is
        deprecated and can not include ``/`` in the label string. ``ivo`` URIs
        can include anything supported by the `Butler` constructor, including
        paths to repositories and alias labels.

            ivo://org.rubinobs/dr1?repo=/repo/main&id=UUID

        will return a label of ``/repo/main``.

        This method does not attempt to check that the dataset exists in the
        labeled butler.

        Since the IVOID can be issued by any publisher to represent a Butler
        dataset there is no validation of the path or netloc component of the
        URI. The only requirement is that there are ``id`` and ``repo`` keys
        in the ``ivo`` URI query component.
        """
        parsed = urllib.parse.urlparse(uri)
        parsed_scheme = parsed.scheme.lower()
        if parsed_scheme == "ivo":
            # Do not validate the netloc or the path values.
            qs = urllib.parse.parse_qs(parsed.query)
            if "repo" not in qs or "id" not in qs:
                raise ValueError(f"Missing 'repo' and/or 'id' query parameters in IVOID {uri}.")
            if len(qs["repo"]) != 1 or len(qs["id"]) != 1:
                raise ValueError(f"Butler IVOID only supports a single value of repo and id, got {uri}")
            label = qs["repo"][0]
            id_ = qs["id"][0]
        elif parsed_scheme == "butler":
            label = parsed.netloc  # Butler label is case sensitive.
            # Need to strip the leading /.
            id_ = parsed.path[1:]
        else:
            raise ValueError(f"Unrecognized URI scheme: {uri!r}")
        # Strip trailing/leading whitespace from label.
        label = label.strip()
        if not label:
            raise ValueError(f"No butler repository label found in uri {uri!r}")
        try:
            dataset_id = uuid.UUID(hex=id_)
        except Exception as e:
            e.add_note(f"Error extracting dataset ID from uri {uri!r} with dataset ID string {id_!r}")
            raise

        return ParsedButlerDatasetURI(label=label, dataset_id=dataset_id, uri=uri)

    @classmethod
    def get_dataset_from_uri(
        cls, uri: str, factory: LabeledButlerFactoryProtocol | None = None
    ) -> SpecificButlerDataset:
        """Get the dataset associated with the given dataset URI.

        Parameters
        ----------
        uri : `str`
            The URI associated with a dataset.
        factory : `LabeledButlerFactoryProtocol` or `None`, optional
            Bound factory function that will be given the butler label
            and receive a `Butler`. If this is not provided the label
            will be tried directly.

        Returns
        -------
        result : `SpecificButlerDataset`
            The butler associated with this URI and the dataset itself.
            The dataset can be `None` if the UUID is valid but the dataset
            is not known to this butler.
        """
        parsed = cls.parse_dataset_uri(uri)
        butler: Butler | None = None
        if factory is not None:
            # If the label is not recognized, it might be a path.
            try:
                butler = factory(parsed.label)
            except KeyError:
                pass
        if butler is None:
            butler = cls.from_config(parsed.label)
        return SpecificButlerDataset(butler=butler, dataset=butler.get_dataset(parsed.dataset_id))

    @abstractmethod
    def _caching_context(self) -> AbstractContextManager[None]:
        """Context manager that enables caching."""
        raise NotImplementedError()

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
        provenance: DatasetProvenance | None = None,
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
        provenance : `DatasetProvenance` or `None`, optional
            Any provenance that should be attached to the serialized dataset.
            Not supported by all serialization mechanisms.
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
        timespan: Timespan | None = None,
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
        timespan : `Timespan` or `None`, optional
            A timespan that the validity range of the dataset must overlap.
            If not provided and this is a calibration dataset type, an attempt
            will be made to find the timespan from any temporal coordinate
            in the data ID.
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
        timespan: Timespan | None = None,
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
        timespan : `Timespan` or `None`, optional
            A timespan that the validity range of the dataset must overlap.
            If not provided and this is a calibration dataset type, an attempt
            will be made to find the timespan from any temporal coordinate
            in the data ID.
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
        primary, components = self.getURIs(
            datasetRefOrType, dataId=dataId, predict=predict, collections=collections, run=run, **kwargs
        )

        if primary is None or components:
            raise RuntimeError(
                f"Dataset ({datasetRefOrType}) includes distinct URIs for components. "
                "Use Butler.getURIs() instead."
            )
        return primary

    @abstractmethod
    def get_dataset_type(self, name: str) -> DatasetType:
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
        lsst.daf.butler.MissingDatasetTypeError
            Raised if the requested dataset type has not been registered.

        Notes
        -----
        This method handles component dataset types automatically, though most
        other operations do not.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_dataset(
        self,
        id: DatasetId,
        *,
        storage_class: str | StorageClass | None = None,
        dimension_records: bool = False,
        datastore_records: bool = False,
    ) -> DatasetRef | None:
        """Retrieve a Dataset entry.

        Parameters
        ----------
        id : `DatasetId`
            The unique identifier for the dataset.
        storage_class : `str` or `StorageClass` or `None`
            A storage class to use when creating the returned entry. If given
            it must be compatible with the default storage class.
        dimension_records : `bool`, optional
            If `True` the ref will be expanded and contain dimension records.
        datastore_records : `bool`, optional
            If `True` the ref will contain associated datastore records.

        Returns
        -------
        ref : `DatasetRef` or `None`
            A ref to the Dataset, or `None` if no matching Dataset
            was found.
        """
        raise NotImplementedError()

    @abstractmethod
    def find_dataset(
        self,
        dataset_type: DatasetType | str,
        data_id: DataId | None = None,
        *,
        collections: str | Sequence[str] | None = None,
        timespan: Timespan | None = None,
        storage_class: str | StorageClass | None = None,
        dimension_records: bool = False,
        datastore_records: bool = False,
        **kwargs: Any,
    ) -> DatasetRef | None:
        """Find a dataset given its `DatasetType` and data ID.

        This can be used to obtain a `DatasetRef` that permits the dataset to
        be read from a `Datastore`. If the dataset is a component and can not
        be found using the provided dataset type, a dataset ref for the parent
        will be returned instead but with the correct dataset type.

        Parameters
        ----------
        dataset_type : `DatasetType` or `str`
            A `DatasetType` or the name of one.  If this is a `DatasetType`
            instance, its storage class will be respected and propagated to
            the output, even if it differs from the dataset type definition
            in the registry, as long as the storage classes are convertible.
        data_id : `dict` or `DataCoordinate`, optional
            A `dict`-like object containing the `Dimension` links that identify
            the dataset within a collection. If it is a `dict` the dataId
            can include dimension record values such as ``day_obs`` and
            ``seq_num`` or ``full_name`` that can be used to derive the
            primary dimension.
        collections : `str` or `list` [`str`], optional
            A an ordered list of collections to search for the dataset.
            Defaults to ``self.defaults.collections``.
        timespan : `Timespan`, optional
            A timespan that the validity range of the dataset must overlap.
            If not provided, any `~CollectionType.CALIBRATION` collections
            matched by the ``collections`` argument will not be searched.
        storage_class : `str` or `StorageClass` or `None`
            A storage class to use when creating the returned entry. If given
            it must be compatible with the default storage class.
        dimension_records : `bool`, optional
            If `True` the ref will be expanded and contain dimension records.
        datastore_records : `bool`, optional
            If `True` the ref will contain associated datastore records.
        **kwargs
            Additional keyword arguments passed to
            `DataCoordinate.standardize` to convert ``dataId`` to a true
            `DataCoordinate` or augment an existing one. This can also include
            dimension record metadata that can be used to derive a primary
            dimension value.

        Returns
        -------
        ref : `DatasetRef`
            A reference to the dataset, or `None` if no matching Dataset
            was found.

        Raises
        ------
        lsst.daf.butler.NoDefaultCollectionError
            Raised if ``collections`` is `None` and
            ``self.collections`` is `None`.
        LookupError
            Raised if one or more data ID keys are missing.
        lsst.daf.butler.MissingDatasetTypeError
            Raised if the dataset type does not exist.
        lsst.daf.butler.MissingCollectionError
            Raised if any of ``collections`` does not exist in the registry.

        Notes
        -----
        This method simply returns `None` and does not raise an exception even
        when the set of collections searched is intrinsically incompatible with
        the dataset type, e.g. if ``datasetType.isCalibration() is False``, but
        only `~CollectionType.CALIBRATION` collections are being searched.
        This may make it harder to debug some lookup failures, but the behavior
        is intentional; we consider it more important that failed searches are
        reported consistently, regardless of the reason, and that adding
        additional collections that do not contain a match to the search path
        never changes the behavior.

        This method handles component dataset types automatically, though most
        other query operations do not.
        """
        raise NotImplementedError()

    @abstractmethod
    def retrieve_artifacts_zip(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        overwrite: bool = True,
    ) -> ResourcePath:
        """Retrieve artifacts from a Butler and place in ZIP file.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            The datasets to be included in the zip file.
        destination : `lsst.resources.ResourcePathExpression`
            Directory to write the new ZIP file. This directory will
            also be used as a staging area for the datasets being downloaded
            from the datastore.
        overwrite : `bool`, optional
            If `False` the output Zip will not be written if a file of the
            same name is already present in ``destination``.

        Returns
        -------
        zip_file : `lsst.resources.ResourcePath`
            The path to the new ZIP file.

        Raises
        ------
        ValueError
            Raised if there are no refs to retrieve.
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
            If `True`, a check will be made for the actual existence of a
            dataset artifact. This will involve additional overhead due to
            the need to query an external system. If `False`, this check will
            be omitted, and the registry and datastore will solely be asked
            if they know about the dataset but no direct check for the
            artifact will be performed.
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
            If `True`, a check will be made for the actual existence of each
            dataset artifact. This will involve additional overhead due to
            the need to query an external system. If `False`, this check will
            be omitted, and the registry and datastore will solely be asked
            if they know about the dataset(s) but no direct check for the
            artifact(s) will be performed.

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
        record_validation_info: bool = True,
    ) -> None:
        """Store and register one or more datasets that already exist on disk.

        Parameters
        ----------
        *datasets : `FileDataset`
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
    def ingest_zip(self, zip_file: ResourcePathExpression, transfer: str = "auto") -> None:
        """Ingest a Zip file into this butler.

        The Zip file must have been created by `retrieve_artifacts_zip`.

        Parameters
        ----------
        zip_file : `lsst.resources.ResourcePathExpression`
            Path to the Zip file.
        transfer : `str`, optional
            Method to use to transfer the Zip into the datastore.

        Notes
        -----
        Run collections are created as needed.
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
                export.saveDatasets(
                    butler.registry.queryDatasets("flat"), elements=()
                )
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
        record_validation_info: bool = True,
        without_datastore: bool = False,
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
        record_validation_info : `bool`, optional
            If `True`, the default, the datastore can record validation
            information associated with the file. If `False` the datastore
            will not attempt to track any information such as checksums
            or file sizes. This can be useful if such information is tracked
            in an external system or if the file is to be compressed in place.
            It is up to the datastore whether this parameter is relevant.
        without_datastore : `bool`, optional
            If `True` only registry records will be imported and the datastore
            will be ignored.

        Raises
        ------
        TypeError
            Raised if the set of arguments passed is inconsistent, or if the
            butler is read-only.
        """
        raise NotImplementedError()

    @abstractmethod
    def transfer_dimension_records_from(
        self, source_butler: LimitedButler | Butler, source_refs: Iterable[DatasetRef]
    ) -> None:
        """Transfer dimension records to this Butler from another Butler.

        Parameters
        ----------
        source_butler : `LimitedButler` or `Butler`
            Butler from which the records are to be transferred. If data IDs
            in ``source_refs`` are not expanded then this has to be a full
            `Butler` whose registry will be used to expand data IDs. If the
            source refs contain coordinates that are used to populate other
            records then this will also need to be a full `Butler`.
        source_refs : iterable of `DatasetRef`
            Datasets defined in the source butler whose dimension records
            should be transferred to this butler. In most circumstances.
            transfer is faster if the dataset refs are expanded.
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
        dry_run: bool = False,
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
            this butler. In most circumstances, ``transfer_from`` is faster if
            the dataset refs are expanded.
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
        dry_run : `bool`, optional
            If `True` the transfer will be processed without any modifications
            made to the target butler and as if the target butler did not
            have any of the datasets.

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
    def collection_chains(self) -> ButlerCollections:
        """Object with methods for modifying collection chains
        (`~lsst.daf.butler.ButlerCollections`).

        Deprecated. Replaced with ``collections`` property.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def collections(self) -> ButlerCollections:
        """Object with methods for modifying and querying collections
        (`~lsst.daf.butler.ButlerCollections`).

        Use of this object is preferred over `registry` wherever possible.
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

    @abstractmethod
    def query(self) -> AbstractContextManager[Query]:
        """Context manager returning a `Query` object used for construction
        and execution of complex queries.
        """
        raise NotImplementedError()

    def query_data_ids(
        self,
        dimensions: DimensionGroup | Iterable[str] | str,
        *,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        with_dimension_records: bool = False,
        order_by: Iterable[str] | str | None = None,
        limit: int | None = -20_000,
        explain: bool = True,
        **kwargs: Any,
    ) -> list[DataCoordinate]:
        """Query for data IDs matching user-provided criteria.

        Parameters
        ----------
        dimensions : `DimensionGroup`, `str`, or \
                `~collections.abc.Iterable` [`str`]
            The dimensions of the data IDs to yield, as either `DimensionGroup`
            instances or `str`.  Will be automatically expanded to a complete
            `DimensionGroup`.
        data_id : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.  See
            :ref:`daf_butler_dimension_expressions` for more information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        with_dimension_records : `bool`, optional
            If `True` (default is `False`) then returned data IDs will have
            dimension records.
        order_by : `~collections.abc.Iterable` [`str`] or `str`, optional
            Names of the columns/dimensions to use for ordering returned data
            IDs. Column name can be prefixed with minus (``-``) to use
            descending ordering.
        limit : `int` or `None`, optional
            Upper limit on the number of returned records. `None` can be used
            if no limit is wanted. A limit of ``0`` means that the query will
            be executed and validated but no results will be returned. In this
            case there will be no exception even if ``explain`` is `True`.
            If a negative value is given a warning will be issued if the number
            of results is capped by that limit.
        explain : `bool`, optional
            If `True` (default) then `EmptyQueryResultError` exception is
            raised when resulting list is empty. The exception contains
            non-empty list of strings explaining possible causes for empty
            result.
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``data_id``
            argument (and may be used to provide a constraining data ID even
            when the ``data_id`` argument is `None`).

        Returns
        -------
        dataIds : `list` [`DataCoordinate`]
            Data IDs matching the given query parameters.  These are always
            guaranteed to identify all dimensions (`DataCoordinate.hasFull`
            returns `True`).

        Raises
        ------
        lsst.daf.butler.registry.DataIdError
            Raised when ``data_id`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.
        lsst.daf.butler.EmptyQueryResultError
            Raised when query generates empty result and ``explain`` is set to
            `True`.
        TypeError
            Raised when the arguments are incompatible.
        """
        if data_id is None:
            data_id = DataCoordinate.make_empty(self.dimensions)
        if order_by is None:
            order_by = []
        query_limit = limit
        warn_limit = False
        if limit is not None and limit < 0:
            query_limit = abs(limit) + 1
            warn_limit = True
        with self.query() as query:
            result = (
                query.where(data_id, where, bind=bind, **kwargs)
                .data_ids(dimensions)
                .order_by(*ensure_iterable(order_by))
                .limit(query_limit)
            )
            if with_dimension_records:
                result = result.with_dimension_records()
            data_ids = list(result)
            if warn_limit and len(data_ids) == query_limit:
                # We asked for one too many so must remove that from the list.
                data_ids.pop(-1)
                assert limit is not None  # For mypy.
                _LOG.warning("More data IDs are available than the requested limit of %d.", abs(limit))
        if explain and (limit is None or limit != 0) and not data_ids:
            raise EmptyQueryResultError(list(result.explain_no_results()))
        return data_ids

    def query_datasets(
        self,
        dataset_type: str | DatasetType,
        collections: str | Iterable[str] | None = None,
        *,
        find_first: bool = True,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        with_dimension_records: bool = False,
        order_by: Iterable[str] | str | None = None,
        limit: int | None = -20_000,
        explain: bool = True,
        **kwargs: Any,
    ) -> list[DatasetRef]:
        """Query for dataset references matching user-provided criteria.

        Parameters
        ----------
        dataset_type : `str` or `DatasetType`
            Dataset type object or name to search for.
        collections : collection expression, optional
            A collection name or iterable of collection names to search. If not
            provided, the default collections are used. Can be a wildcard if
            ``find_first`` is `False` (if find first is requested the order
            of collections matters and wildcards make the order indeterminate).
             See :ref:`daf_butler_collection_expressions` for more information.
        find_first : `bool`, optional
            If `True` (default), for each result data ID, only yield one
            `DatasetRef` of each `DatasetType`, from the first collection in
            which a dataset of that dataset type appears (according to the
            order of ``collections`` passed in).  If `True`, ``collections``
            must not contain wildcards.
        data_id : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints in
            the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve any
            column of a dimension table or (as a shortcut for the primary key
            column of a dimension table) dimension name.  See
            :ref:`daf_butler_dimension_expressions` for more information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace. Values
            of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        with_dimension_records : `bool`, optional
            If `True` (default is `False`) then returned data IDs will have
            dimension records.
        order_by : `~collections.abc.Iterable` [`str`] or `str`, optional
            Names of the columns/dimensions to use for ordering returned data
            IDs. Column name can be prefixed with minus (``-``) to use
            descending ordering.
        limit : `int` or `None`, optional
            Upper limit on the number of returned records. `None` can be used
            if no limit is wanted. A limit of ``0`` means that the query will
            be executed and validated but no results will be returned. In this
            case there will be no exception even if ``explain`` is `True`.
            If a negative value is given a warning will be issued if the number
            of results is capped by that limit.
        explain : `bool`, optional
            If `True` (default) then `EmptyQueryResultError` exception is
            raised when resulting list is empty. The exception contains
            non-empty list of strings explaining possible causes for empty
            result.
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``data_id``
            argument (and may be used to provide a constraining data ID even
            when the ``data_id`` argument is `None`).

        Returns
        -------
        refs : `.queries.DatasetRefQueryResults`
            Dataset references matching the given query criteria.  Nested data
            IDs are guaranteed to include values for all implied dimensions
            (i.e. `DataCoordinate.hasFull` will return `True`).

        Raises
        ------
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``dataset_type`` expression is invalid.
        lsst.daf.butler.registry.DataIdError
            Raised when ``data_id`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.
        lsst.daf.butler.EmptyQueryResultError
            Raised when query generates empty result and ``explain`` is set to
            `True`.
        TypeError
            Raised when the arguments are incompatible, such as when a
            collection wildcard is passed when ``find_first`` is `True`, or
            when ``collections`` is `None` and default butler collections are
            not defined.
        """
        if data_id is None:
            data_id = DataCoordinate.make_empty(self.dimensions)
        if order_by is None:
            order_by = []
        if collections and has_globs(collections):
            # Wild cards need to be expanded but can only be allowed if
            # find_first=False because expanding wildcards does not return
            # a guaranteed ordering. Querying collection registry to expand
            # collections when we do not have wildcards is expensive so only
            # do it if we need it.
            if find_first:
                raise InvalidQueryError(
                    f"Can not use wildcards in collections when find_first=True (given {collections})"
                )
            collections = self.collections.query(collections)
        query_limit = limit
        warn_limit = False
        if limit is not None and limit < 0:
            query_limit = abs(limit) + 1
            warn_limit = True
        with self.query() as query:
            result = (
                query.datasets(dataset_type, collections=collections, find_first=find_first)
                .where(data_id, where, bind=bind, **kwargs)
                .order_by(*ensure_iterable(order_by))
                .limit(query_limit)
            )
            if with_dimension_records:
                result = result.with_dimension_records()
            refs = list(result)
            if warn_limit and len(refs) == query_limit:
                # We asked for one too many so must remove that from the list.
                refs.pop(-1)
                assert limit is not None  # For mypy.
                _LOG.warning("More datasets are available than the requested limit of %d.", abs(limit))
        if explain and (limit is None or limit != 0) and not refs:
            raise EmptyQueryResultError(list(result.explain_no_results()))
        return refs

    def query_dimension_records(
        self,
        element: str,
        *,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        order_by: Iterable[str] | str | None = None,
        limit: int | None = -20_000,
        explain: bool = True,
        **kwargs: Any,
    ) -> list[DimensionRecord]:
        """Query for dimension information matching user-provided criteria.

        Parameters
        ----------
        element : `str`
            The name of a dimension element to obtain records for.
        data_id : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  See
            `queryDataIds` and :ref:`daf_butler_dimension_expressions` for more
            information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        order_by : `~collections.abc.Iterable` [`str`] or `str`, optional
            Names of the columns/dimensions to use for ordering returned data
            IDs. Column name can be prefixed with minus (``-``) to use
            descending ordering.
        limit : `int` or `None`, optional
            Upper limit on the number of returned records. `None` can be used
            if no limit is wanted. A limit of ``0`` means that the query will
            be executed and validated but no results will be returned. In this
            case there will be no exception even if ``explain`` is `True`.
            If a negative value is given a warning will be issued if the number
            of results is capped by that limit.
        explain : `bool`, optional
            If `True` (default) then `EmptyQueryResultError` exception is
            raised when resulting list is empty. The exception contains
            non-empty list of strings explaining possible causes for empty
            result.
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``data_id``
            argument (and may be used to provide a constraining data ID even
            when the ``data_id`` argument is `None`).

        Returns
        -------
        records : `list`[`DimensionRecord`]
            Dimension records matching the given query parameters.

        Raises
        ------
        lsst.daf.butler.registry.DataIdError
            Raised when ``data_id`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.
        lsst.daf.butler.EmptyQueryResultError
            Raised when query generates empty result and ``explain`` is set to
            `True`.
        TypeError
            Raised when the arguments are incompatible, such as when a
            collection wildcard is passed when ``find_first`` is `True`, or
            when ``collections`` is `None` and default butler collections are
            not defined.
        """
        if data_id is None:
            data_id = DataCoordinate.make_empty(self.dimensions)
        if order_by is None:
            order_by = []
        query_limit = limit
        warn_limit = False
        if limit is not None and limit < 0:
            query_limit = abs(limit) + 1
            warn_limit = True
        with self.query() as query:
            result = (
                query.where(data_id, where, bind=bind, **kwargs)
                .dimension_records(element)
                .order_by(*ensure_iterable(order_by))
                .limit(query_limit)
            )
            dimension_records = list(result)
            if warn_limit and len(dimension_records) == query_limit:
                # We asked for one too many so must remove that from the list.
                dimension_records.pop(-1)
                assert limit is not None  # For mypy.
                _LOG.warning(
                    "More dimension records are available than the requested limit of %d.", abs(limit)
                )
        if explain and (limit is None or limit != 0) and not dimension_records:
            raise EmptyQueryResultError(list(result.explain_no_results()))
        return dimension_records

    def _query_all_datasets(
        self,
        collections: str | Iterable[str] | None = None,
        *,
        name: str | Iterable[str] = "*",
        find_first: bool = True,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        limit: int | None = -20_000,
        **kwargs: Any,
    ) -> list[DatasetRef]:
        """Query for datasets of potentially multiple types.

        Parameters
        ----------
        collections : `str` or `~collections.abc.Iterable` [ `str` ], optional
            The collection or collections to search, in order.  If not provided
            or `None`, the default collection search path for this butler is
            used.
        name : `str` or `~collections.abc.Iterable` [ `str` ], optional
            Names or name patterns (glob-style) that returned dataset type
            names must match.  If an iterable, items are OR'd together.  The
            default is to include all dataset types in the given collections.
        find_first : `bool`, optional
            If `True` (default), for each result data ID, only yield one
            `DatasetRef` of each `DatasetType`, from the first collection in
            which a dataset of that dataset type appears (according to the
            order of ``collections`` passed in).
        data_id : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints in
            the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve any
            column of a dimension table or (as a shortcut for the primary key
            column of a dimension table) dimension name.  See
            :ref:`daf_butler_dimension_expressions` for more information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace. Values
            of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        limit : `int` or `None`, optional
            Upper limit on the number of returned records. `None` can be used
            if no limit is wanted. A limit of ``0`` means that the query will
            be executed and validated but no results will be returned.
            If a negative value is given a warning will be issued if the number
            of results is capped by that limit. If no limit is provided, by
            default a maximum of 20,000 records will be returned.
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``data_id``
            argument (and may be used to provide a constraining data ID even
            when the ``data_id`` argument is `None`).

        Raises
        ------
        MissingDatasetTypeError
            When no dataset types match ``name``, or an explicit (non-glob)
            dataset type in ``name`` does not exist.
        InvalidQueryError
            If the parameters to the query are inconsistent or malformed.
        MissingCollectionError
            If a given collection is not found.

        Returns
        -------
        refs : `list` [ `DatasetRef` ]
            Dataset references matching the given query criteria.  Nested data
            IDs are guaranteed to include values for all implied dimensions
            (i.e. `DataCoordinate.hasFull` will return `True`), but will not
            include dimension records (`DataCoordinate.hasRecords` will be
            `False`).
        """
        if collections is None:
            collections = list(self.collections.defaults)
        else:
            collections = list(ensure_iterable(collections))

        if bind is None:
            bind = {}
        if data_id is None:
            data_id = {}

        warn_limit = False
        if limit is not None and limit < 0:
            # Add one to the limit so we can detect if we have exceeded it.
            limit = abs(limit) + 1
            warn_limit = True

        args = QueryAllDatasetsParameters(
            collections=collections,
            name=list(ensure_iterable(name)),
            find_first=find_first,
            data_id=data_id,
            where=where,
            limit=limit,
            bind=bind,
            kwargs=kwargs,
            with_dimension_records=False,
        )
        with self._query_all_datasets_by_page(args) as pages:
            result = []
            for page in pages:
                result.extend(page)

        if warn_limit and limit is not None and len(result) >= limit:
            # Remove the extra dataset we added for the limit check.
            result.pop()
            _LOG.warning("More datasets are available than the requested limit of %d.", limit - 1)

        return result

    @abstractmethod
    def _query_all_datasets_by_page(
        self, args: QueryAllDatasetsParameters
    ) -> AbstractContextManager[Iterator[list[DatasetRef]]]:
        raise NotImplementedError()

    def clone(
        self,
        *,
        collections: CollectionArgType | None | EllipsisType = ...,
        run: str | None | EllipsisType = ...,
        inferDefaults: bool | EllipsisType = ...,
        dataId: dict[str, str] | EllipsisType = ...,
    ) -> Butler:
        """Return a new Butler instance connected to the same repository
        as this one, optionally overriding ``collections``, ``run``,
        ``inferDefaults``, and default data ID.

        Parameters
        ----------
        collections : `~lsst.daf.butler.registry.CollectionArgType` or `None`,\
            optional
            Same as constructor.  If omitted, uses value from original object.
        run : `str` or `None`, optional
            Same as constructor.  If `None`, no default run is used.  If
            omitted, copies value from original object.
        inferDefaults : `bool`, optional
            Same as constructor.  If omitted, copies value from original
            object.
        dataId : `str`
            Same as ``kwargs`` passed to the constructor.  If omitted, copies
            values from original object.
        """
        raise NotImplementedError()
