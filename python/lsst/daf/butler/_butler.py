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

__all__ = ("Butler", "ButlerValidationError")

import os
from collections import defaultdict
import contextlib
import logging
from typing import (
    Any,
    ClassVar,
    ContextManager,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Tuple,
    Union,
)

try:
    import boto3
except ImportError:
    boto3 = None

from lsst.utils import doImport
from .core import (
    ButlerURI,
    CompositesMap,
    Config,
    ConfigSubset,
    DataCoordinate,
    DataId,
    DatasetRef,
    DatasetType,
    Datastore,
    FileDataset,
    Quantum,
    RepoExport,
    StorageClassFactory,
    ValidationError,
)
from .core.repoRelocation import BUTLER_ROOT_TAG
from .core.safeFileIo import safeMakeDir
from .core.utils import transactional, getClassOf
from ._deferredDatasetHandle import DeferredDatasetHandle
from ._butlerConfig import ButlerConfig
from .registry import Registry, RegistryConfig

log = logging.getLogger(__name__)


class ButlerValidationError(ValidationError):
    """There is a problem with the Butler configuration."""
    pass


class Butler:
    """Main entry point for the data access system.

    Attributes
    ----------
    config : `str`, `ButlerConfig` or `Config`, optional
        (filename to) configuration. If this is not a `ButlerConfig`, defaults
        will be read.  If a `str`, may be the path to a directory containing
        a "butler.yaml" file.
    datastore : `Datastore`
        Datastore to use for storage.
    registry : `Registry`
        Registry to use for lookups.

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
    collection : `str`, optional
        Collection to use for all input lookups.  May be `None` to either use
        the value passed to ``run``, or to defer passing a collection until
        the methods that require one are called.
    run : `str`, optional
        Name of the run datasets should be output to; also used as a tagged
        collection name these dataset will be associated with.  If the run
        does not exist, it will be created.  If ``collection`` is `None`, this
        collection will be used for input lookups as well; if not, it must have
        the same value as ``run``.
    searchPaths : `list` of `str`, optional
        Directory paths to search when calculating the full Butler
        configuration.  Not used if the supplied config is already a
        `ButlerConfig`.
    writeable : `bool`, optional
        Explicitly sets whether the butler supports write operations.  If not
        provided, a read-only butler is created unless ``run`` is passed.

    Raises
    ------
    ValueError
        Raised if neither "collection" nor "run" are provided by argument or
        config, or if both are provided and are inconsistent.
    """
    def __init__(self, config: Union[Config, str, None] = None, *,
                 butler: Optional[Butler] = None,
                 collection: Optional[str] = None,
                 run: Optional[str] = None,
                 searchPaths: Optional[List[str]] = None,
                 writeable: Optional[bool] = None):
        if butler is not None:
            if config is not None or searchPaths is not None or writeable is not None:
                raise TypeError("Cannot pass 'config', 'searchPaths', or 'writeable' "
                                "arguments with 'butler' argument.")
            self.registry = butler.registry
            self.datastore = butler.datastore
            self.storageClasses = butler.storageClasses
            self._composites = butler._composites
            self._config = butler._config
        else:
            self._config = ButlerConfig(config, searchPaths=searchPaths)
            if "root" in self._config:
                butlerRoot = self._config["root"]
            else:
                butlerRoot = self._config.configDir
            if writeable is None:
                writeable = run is not None
            self.registry = Registry.fromConfig(self._config, butlerRoot=butlerRoot, writeable=writeable)
            self.datastore = Datastore.fromConfig(self._config, self.registry, butlerRoot=butlerRoot)
            self.storageClasses = StorageClassFactory()
            self.storageClasses.addFromConfig(self._config)
            self._composites = CompositesMap(self._config, universe=self.registry.dimensions)
        if "run" in self._config or "collection" in self._config:
            raise ValueError("Passing a run or collection via configuration is no longer supported.")
        if run is not None and writeable is False:
            raise ValueError(f"Butler initialized with run='{run}', "
                             f"but is read-only; use collection='{run}' instead.")
        self.run = run
        if collection is None and run is not None:
            collection = run
        if self.run is not None and collection != self.run:
            raise ValueError(
                "Run ({}) and collection ({}) are inconsistent.".format(self.run, collection)
            )
        self.collection = collection
        if self.run is not None:
            self.registry.registerRun(self.run)

    GENERATION: ClassVar[int] = 3
    """This is a Generation 3 Butler.

    This attribute may be removed in the future, once the Generation 2 Butler
    interface has been fully retired; it should only be used in transitional
    code.
    """

    @staticmethod
    def makeRepo(root: str, config: Union[Config, str, None] = None, standalone: bool = False,
                 createRegistry: bool = True, searchPaths: Optional[List[str]] = None,
                 forceConfigRoot: bool = True, outfile: Optional[str] = None) -> Config:
        """Create an empty data repository by adding a butler.yaml config
        to a repository root directory.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the new repository.  Will be created
            if it does not exist.
        config : `Config` or `str`, optional
            Configuration to write to the repository, after setting any
            root-dependent Registry or Datastore config options.  Can not
            be a `ButlerConfig` or a `ConfigSubset`.  If `None`, default
            configuration will be used.  Root-dependent config options
            specified in this config are overwritten if ``forceConfigRoot``
            is `True`.
        standalone : `bool`
            If True, write all expanded defaults, not just customized or
            repository-specific settings.
            This (mostly) decouples the repository from the default
            configuration, insulating it from changes to the defaults (which
            may be good or bad, depending on the nature of the changes).
            Future *additions* to the defaults will still be picked up when
            initializing `Butlers` to repos created with ``standalone=True``.
        createRegistry : `bool`, optional
            If `True` create a new Registry.
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
            location rather than into the repository itself.

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

        # for "file" schemes we are assuming POSIX semantics for paths, for
        # schemeless URIs we are assuming os.path semantics.
        uri = ButlerURI(root)
        if uri.scheme == "file" or not uri.scheme:
            if not os.path.isdir(uri.ospath):
                safeMakeDir(uri.ospath)
        elif uri.scheme == "s3":
            s3 = boto3.resource("s3")
            # implies bucket exists, if not another level of checks
            bucket = s3.Bucket(uri.netloc)
            bucket.put_object(Bucket=uri.netloc, Key=uri.relativeToPathRoot)
        else:
            raise ValueError(f"Unrecognized scheme: {uri.scheme}")
        config = Config(config)

        # If we are creating a new repo from scratch with relative roots,
        # do not propagate an explicit root from the config file
        if "root" in config:
            del config["root"]

        full = ButlerConfig(config, searchPaths=searchPaths)  # this applies defaults
        datastoreClass = doImport(full["datastore", "cls"])
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
        config.dumpToUri(uri)

        # Create Registry and populate tables
        Registry.fromConfig(config, create=createRegistry, butlerRoot=root)
        return config

    @classmethod
    def _unpickle(cls, config: ButlerConfig, collection: str, run: Optional[str], writeable: bool) -> Butler:
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
        collection : `str`
            String name of a collection to use for read operations.
        run : `str`, optional
            String name of a run to use for write operations, or `None` for a
            read-only butler.

        Returns
        -------
        butler : `Butler`
            A new `Butler` instance.
        """
        return cls(config=config, collection=collection, run=run, writeable=writeable)

    def __reduce__(self):
        """Support pickling.
        """
        return (Butler._unpickle, (self._config, self.collection, self.run, self.registry.isWriteable()))

    def __str__(self):
        return "Butler(collection='{}', datastore='{}', registry='{}')".format(
            self.collection, self.datastore, self.registry)

    def isWriteable(self) -> bool:
        """Return `True` if this `Butler` supports write operations.
        """
        return self.registry.isWriteable()

    @contextlib.contextmanager
    def transaction(self):
        """Context manager supporting `Butler` transactions.

        Transactions can be nested.
        """
        with self.registry.transaction():
            with self.datastore.transaction():
                yield

    def _standardizeArgs(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
                         dataId: Optional[DataId] = None, **kwds: Any) -> Tuple[DatasetType, DataId]:
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
        if isinstance(datasetRefOrType, DatasetRef):
            if dataId is not None or kwds:
                raise ValueError("DatasetRef given, cannot use dataId as well")
            datasetType = datasetRefOrType.datasetType
            dataId = datasetRefOrType.dataId
        else:
            # Don't check whether DataId is provided, because Registry APIs
            # can usually construct a better error message when it wasn't.
            if isinstance(datasetRefOrType, DatasetType):
                datasetType = datasetRefOrType
            else:
                datasetType = self.registry.getDatasetType(datasetRefOrType)
        return datasetType, dataId

    def _findDatasetRef(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
                        dataId: Optional[DataId] = None, *,
                        collection: Optional[str] = None,
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
        collection : `str`, optional
            Name of the collection to search, overriding ``self.collection``.
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
            differs from the one found in the registry in this collection.
        TypeError
            Raised if ``collection`` and ``self.collection`` are both `None`.
        """
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        if isinstance(datasetRefOrType, DatasetRef):
            idNumber = datasetRefOrType.id
        else:
            idNumber = None
        # Expand the data ID first instead of letting registry.find do it, so
        # we get the result even if it returns None.
        dataId = self.registry.expandDataId(dataId, graph=datasetType.dimensions, **kwds)
        if collection is None:
            collection = self.collection
            if collection is None:
                raise TypeError("No collection provided.")
        # Always lookup the DatasetRef, even if one is given, to ensure it is
        # present in the current collection.
        ref = self.registry.find(collection, datasetType, dataId)
        if ref is None:
            if allowUnresolved:
                return DatasetRef(datasetType, dataId)
            else:
                raise LookupError(f"Dataset {datasetType.name} with data ID {dataId} "
                                  f"could not be found in collection '{collection}'.")
        if idNumber is not None and idNumber != ref.id:
            raise ValueError(f"DatasetRef.id provided ({idNumber}) does not match "
                             f"id ({ref.id}) in registry in collection '{collection}'.")
        return ref

    @transactional
    def put(self, obj: Any, datasetRefOrType: Union[DatasetRef, DatasetType, str],
            dataId: Optional[DataId] = None, *,
            producer: Optional[Quantum] = None,
            run: Optional[str] = None,
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
        producer : `Quantum`, optional
            The producer.
        run : `str`, optional
            The name of the run the dataset should be added to, overriding
            ``self.run``.
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
        log.debug("Butler put: %s, dataId=%s, producer=%s, run=%s", datasetRefOrType, dataId, producer, run)
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        if isinstance(datasetRefOrType, DatasetRef) and datasetRefOrType.id is not None:
            raise ValueError("DatasetRef must not be in registry, must have None id")

        if run is None:
            if self.run is None:
                raise TypeError("No run provided.")
            run = self.run

        isVirtualComposite = self._composites.shouldBeDisassembled(datasetType)

        # Add Registry Dataset entry.  If not a virtual composite, add
        # and attach components at the same time.
        dataId = self.registry.expandDataId(dataId, graph=datasetType.dimensions, **kwds)
        ref, = self.registry.insertDatasets(datasetType, run=run, dataIds=[dataId],
                                            producer=producer, recursive=not isVirtualComposite)

        # Check to see if this datasetType requires disassembly
        if isVirtualComposite:
            components = datasetType.storageClass.assembler().disassemble(obj)
            for component, info in components.items():
                compTypeName = datasetType.componentTypeName(component)
                compRef = self.put(info.component, compTypeName, dataId, producer=producer, run=run)
                self.registry.attachComponent(component, ref, compRef)
        else:
            # This is an entity without a disassembler.
            self.datastore.put(obj, ref)

        return ref

    def getDirect(self, ref: DatasetRef, *, parameters: Optional[Dict[str, Any]] = None):
        """Retrieve a stored dataset.

        Unlike `Butler.get`, this method allows datasets outside the Butler's
        collection to be read as long as the `DatasetRef` that identifies them
        can be obtained separately.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to an already stored dataset.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.

        Returns
        -------
        obj : `object`
            The dataset.
        """
        # if the ref exists in the store we return it directly
        if self.datastore.exists(ref):
            return self.datastore.get(ref, parameters=parameters)
        elif ref.isComposite():
            # Check that we haven't got any unknown parameters
            ref.datasetType.storageClass.validateParameters(parameters)
            # Reconstruct the composite
            usedParams = set()
            components = {}
            for compName, compRef in ref.components.items():
                # make a dictionary of parameters containing only the subset
                # supported by the StorageClass of the components
                compParams = compRef.datasetType.storageClass.filterParameters(parameters)
                usedParams.update(set(compParams))
                components[compName] = self.datastore.get(compRef, parameters=compParams)

            # Any unused parameters will have to be passed to the assembler
            if parameters:
                unusedParams = {k: v for k, v in parameters.items() if k not in usedParams}
            else:
                unusedParams = {}

            # Assemble the components
            inMemoryDataset = ref.datasetType.storageClass.assembler().assemble(components)
            return ref.datasetType.storageClass.assembler().handleParameters(inMemoryDataset,
                                                                             parameters=unusedParams)
        else:
            # single entity in datastore
            raise FileNotFoundError(f"Unable to locate dataset '{ref}' in datastore {self.datastore.name}")

    def getDeferred(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
                    dataId: Optional[DataId] = None, *,
                    parameters: Union[dict, None] = None,
                    collection: Optional[str] = None,
                    **kwds: Any) -> DeferredDatasetHandle:
        """Create a `DeferredDatasetHandle` which can later retrieve a dataset

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataCoordinate`, optional
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        collection : `str`, optional
            Name of the collection to search, overriding ``self.collection``.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.
        collection : `str`, optional
            Collection to search, overriding ``self.collection``.
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
            differs from the one found in the registry in this collection.
        TypeError
            Raised if ``collection`` and ``self.collection`` are both `None`.
        """
        ref = self._findDatasetRef(datasetRefOrType, dataId, collection=collection, **kwds)
        return DeferredDatasetHandle(butler=self, ref=ref, parameters=parameters)

    def get(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
            dataId: Optional[DataId] = None, *,
            parameters: Optional[Dict[str, Any]] = None,
            collection: Optional[str] = None,
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
        collection : `str`, optional
            Collection to search, overriding ``self.collection``.
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
            differs from the one found in the registry in this collection.
        LookupError
            Raised if no matching dataset exists in the `Registry`.
        TypeError
            Raised if ``collection`` and ``self.collection`` are both `None`.
        """
        log.debug("Butler get: %s, dataId=%s, parameters=%s", datasetRefOrType, dataId, parameters)
        ref = self._findDatasetRef(datasetRefOrType, dataId, collection=collection, **kwds)
        return self.getDirect(ref, parameters=parameters)

    def getUri(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
               dataId: Optional[DataId] = None, *,
               predict: bool = False,
               collection: Optional[str] = None,
               run: Optional[str] = None,
               **kwds: Any) -> str:
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
        collection : `str`, optional
            Collection to search, overriding ``self.collection``.
        run : `str`, optional
            Run to use for predictions, overriding ``self.run``.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        uri : `str`
            URI string pointing to the Dataset within the datastore. If the
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
            differs from the one found in the registry in this collection.
        TypeError
            Raised if ``collection`` and ``self.collection`` are both `None`.
        """
        ref = self._findDatasetRef(datasetRefOrType, dataId, allowUnresolved=predict, collection=collection,
                                   **kwds)
        if ref.id is None:  # only possible if predict is True
            if run is None:
                run = self.run
                if run is None:
                    raise TypeError("Cannot predict location with run=None.")
            # Lie about ID, because we can't guess it, and only
            # Datastore.getUri() will ever see it (and it doesn't use it).
            ref = ref.resolved(id=0, run=self.run)
        return self.datastore.getUri(ref, predict)

    def datasetExists(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
                      dataId: Optional[DataId] = None, *,
                      collection: Optional[str] = None,
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
        collection : `str`, optional
            Collection to search, overriding ``self.collection``.
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
            differs from the one found in the registry in this collection.
        TypeError
            Raised if ``collection`` and ``self.collection`` are both `None`.
        """
        ref = self._findDatasetRef(datasetRefOrType, dataId, collection=collection, **kwds)
        return self.datastore.exists(ref)

    def remove(self, datasetRefOrType: Union[DatasetRef, DatasetType, str],
               dataId: Optional[DataId] = None, *,
               delete: bool = True, remember: bool = True, collection: Optional[str] = None, **kwds: Any):
        """Remove a dataset from the collection and possibly the repository.

        The identified dataset is always at least removed from the Butler's
        collection.  By default it is also deleted from the Datastore (e.g.
        files are actually deleted), but the dataset is "remembered" by
        retaining its row in the dataset and provenance tables in the registry.

        If the dataset is a composite, all components will also be removed.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataId`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        delete : `bool`
            If `True` (default) actually delete the dataset from the
            Datastore (i.e. actually remove files).
        remember : `bool`
            If `True` (default), retain dataset and provenance records in
            the `Registry` for this dataset.
        collection : `str`, optional
            Collection to search, overriding ``self.collection``.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Raises
        ------
        TypeError
            Raised if the butler is read-only, if no collection was provided,
            or if ``delete`` and ``remember`` are both `False`; a dataset
            cannot remain in a `Datastore` if its `Registry` entries is
            removed.
        OrphanedRecordError
            Raised if ``remember`` is `False` but the dataset is still present
            in a `Datastore` not recognized by this `Butler` client.
        ValueError
            Raised if a resolved `DatasetRef` was passed as an input, but it
            differs from the one found in the registry in this collection.
        """
        if not self.isWriteable():
            raise TypeError("Butler is read-only.")
        ref = self._findDatasetRef(datasetRefOrType, dataId, collection=collection, **kwds)
        if delete:
            # There is a difference between a concrete composite and virtual
            # composite. In a virtual composite the datastore is never
            # given the top level DatasetRef. In the concrete composite
            # the datastore knows all the refs and will clean up itself
            # if asked to remove the parent ref.
            # We can not check configuration for this since we can not trust
            # that the configuration is the same. We therefore have to ask
            # if the ref exists or not
            if self.datastore.exists(ref):
                self.datastore.remove(ref)
            elif ref.isComposite():
                datastoreNames = set(self.datastore.names)
                for r in ref.components.values():
                    # If a dataset was removed previously but remembered
                    # in registry, skip the removal in the datastore.
                    datastoreLocations = self.registry.getDatasetLocations(r)
                    if datastoreLocations & datastoreNames:
                        self.datastore.remove(r)
            else:
                raise FileNotFoundError(f"Dataset {ref} not known to datastore")
        elif not remember:
            raise ValueError("Cannot retain dataset in Datastore without keeping Registry dataset record.")
        if remember:
            self.registry.disassociate(self.collection, [ref])
        else:
            # This also implicitly disassociates.
            self.registry.removeDataset(ref)

    @transactional
    def ingest(self, *datasets: FileDataset, transfer: Optional[str] = None, run: Optional[str] = None):
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
            If not `None`, must be one of 'move', 'copy', 'hardlink', or
            'symlink', indicating how to transfer the file.
        run : `str`, optional
            The name of the run ingested datasets should be added to,
            overriding ``self.run``.

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
            resolvedRefs = []
            for ref in dataset.refs:
                groupedData[ref.datasetType][ref.dataId] = (dataset, resolvedRefs)

        # Now we can bulk-insert into Registry for each DatasetType.
        for datasetType, groupForType in groupedData.items():
            refs = self.registry.insertDatasets(datasetType,
                                                dataIds=groupForType.keys(),
                                                run=run,
                                                recursive=True)
            # Append those resolved DatasetRefs to the new lists we set up for
            # them.
            for ref, (_, resolvedRefs) in zip(refs, groupForType.values()):
                resolvedRefs.append(ref)

        # Go back to the original FileDatasets to replace their refs with the
        # new resolved ones.
        for groupForType in groupedData.values():
            for dataset, resolvedRefs in groupForType.values():
                dataset.refs = resolvedRefs

        # Bulk-insert everything into Datastore.
        self.datastore.ingest(*datasets, transfer=transfer)

    @contextlib.contextmanager
    def export(self, *, directory: Optional[str] = None,
               filename: Optional[str] = None,
               format: Optional[str] = None,
               transfer: Optional[str] = None) -> ContextManager[RepoExport]:
        """Export datasets from the repository represented by this `Butler`.

        This method is a context manager that returns a helper object
        (`RepoExport`) that is used to indicate what information from the
        repository should be exported.

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
        Typically the `Registry.queryDimensions` and `Registry.queryDatasets`
        methods are used to provide the iterables over data IDs and/or datasets
        to be exported::

            with butler.export("exports.yaml") as export:
                # Export all flats, and the calibration_label dimensions
                # associated with them.
                export.saveDatasets(butler.registry.queryDatasets("flat"),
                                    elements=[butler.registry.dimensions["calibration_label"]])
                # Export all datasets that start with "deepCoadd_" and all of
                # their associated data ID information.
                export.saveDatasets(butler.registry.queryDatasets("deepCoadd_*"))
        """
        if directory is None and transfer is not None:
            raise TypeError("Cannot transfer without providing a directory.")
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
            with self.transaction():
                try:
                    helper = RepoExport(self.registry, self.datastore, backend=backend,
                                        directory=directory, transfer=transfer)
                    yield helper
                except BaseException:
                    raise
                else:
                    helper._finish()

    def import_(self, *, directory: Optional[str] = None,
                filename: Optional[str] = None,
                format: Optional[str] = None,
                transfer: Optional[str] = None):
        """Import datasets exported from a different butler repository.

        Parameters
        ----------
        directory : `str`, optional
            Directory containing dataset files.  If `None`, all file paths
            must be absolute.
        filename : `str`, optional
            Name for the file that containing database information associated
            with the exported datasets.  If this is not an absolute path, does
            not exist in the current working directory, and ``directory`` is
            not `None`, it is assumed to be in ``directory``.  Defaults to
            "export.{format}".
        format : `str`, optional
            File format for the database information file.  If `None`, the
            extension of ``filename`` will be used.
        transfer : `str`, optional
            Transfer mode passed to `Datastore.export`.

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
                _, format = os.path.splitext(filename)
        elif filename is None:
            filename = f"export.{format}"
        if directory is not None and not os.path.exists(filename):
            filename = os.path.join(directory, filename)
        BackendClass = getClassOf(self._config["repo_transfer_formats"][format]["import"])
        with open(filename, 'r') as stream:
            backend = BackendClass(stream, self.registry)
            backend.register()
            with self.transaction():
                backend.load(self.datastore, directory=directory, transfer=transfer)

    def validateConfiguration(self, logFailures: bool = False,
                              datasetTypeNames: Optional[Iterable[str]] = None,
                              ignore: Iterable[str] = None):
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
            composite, all component of that `DatasetType` will also be
            ignored.

        Raises
        ------
        ButlerValidationError
            Raised if there is some inconsistency with how this Butler
            is configured.
        """
        if datasetTypeNames:
            entities = [self.registry.getDatasetType(name) for name in datasetTypeNames]
        else:
            entities = list(self.registry.getAllDatasetTypes())

        # filter out anything from the ignore list
        if ignore:
            ignore = set(ignore)
            entities = [e for e in entities if e.name not in ignore and e.nameAndComponent()[0] not in ignore]
        else:
            ignore = set()

        # Find all the registered instruments
        instruments = set(
            dataId["instrument"] for dataId in self.registry.queryDimensions(["instrument"])
        )

        # For each datasetType that has an instrument dimension, create
        # a DatasetRef for each defined instrument
        datasetRefs = []

        for datasetType in entities:
            if "instrument" in datasetType.dimensions:
                for instrument in instruments:
                    datasetRef = DatasetRef(datasetType, {"instrument": instrument}, conform=False)
                    datasetRefs.append(datasetRef)

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
            datasetType = None
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

    run: Optional[str]
    """Name of the run this butler writes outputs to (`str` or `None`).
    """

    collection: Optional[str]
    """Name of the collection this butler searches for datasets (`str` or
    `None`).
    """
