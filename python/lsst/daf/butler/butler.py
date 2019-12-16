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

__all__ = ("Butler", "ButlerValidationError")

import os
import contextlib
import logging
import itertools
import typing

try:
    import boto3
except ImportError:
    boto3 = None

from lsst.utils import doImport
from .core.utils import transactional, getClassOf
from .core.datasets import DatasetRef, DatasetType
from .core import deferredDatasetHandle as dDH
from .core.datastore import Datastore
from .registry import Registry
from .core.registryConfig import RegistryConfig
from .core.storageClass import StorageClassFactory
from .core.config import Config, ConfigSubset
from .core.butlerConfig import ButlerConfig
from .core.composites import CompositesMap
from .core.dimensions import DataCoordinate, DataId
from .core.exceptions import ValidationError
from .core.repoRelocation import BUTLER_ROOT_TAG
from .core.safeFileIo import safeMakeDir
from .core.location import ButlerURI
from .core.repoTransfers import RepoExport, FileDataset

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
        Incompatible with the ``config`` and ``searchPaths`` arguments.
    collection : `str`, optional
        Collection to use for all input lookups, overriding
        config["collection"] if provided.
    run : `str`, optional
        Name of the run datasets should be output to; also used as a tagged
        collection name these dataset will be associated with.  If the run
        does not exist, it will be created.  If "collection" is None, this
        collection will be used for input lookups as well; if not, it must have
        the same value as "run".
    searchPaths : `list` of `str`, optional
        Directory paths to search when calculating the full Butler
        configuration.  Not used if the supplied config is already a
        `ButlerConfig`.

    Raises
    ------
    ValueError
        Raised if neither "collection" nor "run" are provided by argument or
        config, or if both are provided and are inconsistent.
    """

    GENERATION = 3
    """This is a Generation 3 Butler.

    This attribute may be removed in the future, once the Generation 2 Butler
    interface has been fully retired; it should only be used in transitional
    code.
    """

    @staticmethod
    def makeRepo(root, config=None, standalone=False, createRegistry=True, searchPaths=None,
                 forceConfigRoot=True, outfile=None):
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

    def __init__(self, config=None, butler=None, collection=None, run=None, searchPaths=None):
        # save arguments for pickling
        self._args = (config, butler, collection, run, searchPaths)
        if butler is not None:
            if config is not None or searchPaths is not None:
                raise TypeError("Cannot pass config or searchPaths arguments with butler argument.")
            self.registry = butler.registry
            self.datastore = butler.datastore
            self.storageClasses = butler.storageClasses
            self.composites = butler.composites
            self.config = butler.config
        else:
            # save arguments for pickling
            self.config = ButlerConfig(config, searchPaths=searchPaths)
            if "root" in self.config:
                butlerRoot = self.config["root"]
            else:
                butlerRoot = self.config.configDir
            self.registry = Registry.fromConfig(self.config, butlerRoot=butlerRoot)
            self.datastore = Datastore.fromConfig(self.config, self.registry, butlerRoot=butlerRoot)
            self.storageClasses = StorageClassFactory()
            self.storageClasses.addFromConfig(self.config)
            self.composites = CompositesMap(self.config, universe=self.registry.dimensions)
        if run is None:
            self.run = self.config.get("run", None)
        else:
            self.run = run
            # if run *arg* is not None and collection arg is, use run for
            # collection.
            if collection is None:
                collection = run
        if collection is None:  # didn't get a collection from collection or run *args*
            collection = self.config.get("collection", None)
            if collection is None:  # didn't get a collection from config["collection"]
                collection = self.run    # get collection from run found in config
        if collection is None:
            raise ValueError("No run or collection provided.")
        if self.run is not None and collection != self.run:
            raise ValueError(
                "Run ({}) and collection ({}) are inconsistent.".format(self.run, collection)
            )
        self.collection = collection
        if self.run is not None:
            self.registry.registerRun(self.run)

    def __reduce__(self):
        """Support pickling.
        """
        return (Butler, self._args)

    def __str__(self):
        return "Butler(collection='{}', datastore='{}', registry='{}')".format(
            self.collection, self.datastore, self.registry)

    @contextlib.contextmanager
    def transaction(self):
        """Context manager supporting `Butler` transactions.

        Transactions can be nested.
        """
        with self.registry.transaction():
            with self.datastore.transaction():
                yield

    def _standardizeArgs(self, datasetRefOrType, dataId=None, **kwds):
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

    @transactional
    def put(self, obj, datasetRefOrType, dataId=None, producer=None, **kwds):
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
            Raised if the butler was not constructed with a run, and is hence
            read-only.
        """
        log.debug("Butler put: %s, dataId=%s, producer=%s", datasetRefOrType, dataId, producer)
        if self.run is None:
            raise TypeError("Butler is read-only.")
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        if isinstance(datasetRefOrType, DatasetRef) and datasetRefOrType.id is not None:
            raise ValueError("DatasetRef must not be in registry, must have None id")

        isVirtualComposite = self.composites.shouldBeDisassembled(datasetType)

        # Add Registry Dataset entry.  If not a virtual composite, add
        # and attach components at the same time.
        dataId = self.registry.expandDataId(dataId, graph=datasetType.dimensions, **kwds)
        ref = self.registry.addDataset(datasetType, dataId, run=self.run, producer=producer,
                                       recursive=not isVirtualComposite)

        # Check to see if this datasetType requires disassembly
        if isVirtualComposite:
            components = datasetType.storageClass.assembler().disassemble(obj)
            for component, info in components.items():
                compTypeName = datasetType.componentTypeName(component)
                compRef = self.put(info.component, compTypeName, dataId, producer)
                self.registry.attachComponent(component, ref, compRef)
        else:
            # This is an entity without a disassembler.
            self.datastore.put(obj, ref)

        return ref

    def getDirect(self, ref, parameters=None):
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

    def getDeferred(self, datasetRefOrType: typing.Union[DatasetRef, DatasetType, str],
                    dataId: typing.Optional[DataId] = None, parameters: typing.Union[dict, None] = None,
                    **kwds) -> dDH.DeferredDatasetHandle:
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
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Returns
        -------
        obj : `DeferredDatasetHandle`
            A handle which can be used to retrieve a dataset at a later time
        """
        return dDH.DeferredDatasetHandle(self, datasetRefOrType, dataId, parameters, kwds)

    def get(self, datasetRefOrType, dataId=None, parameters=None, **kwds):
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
        kwds
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Returns
        -------
        obj : `object`
            The dataset.
        """
        log.debug("Butler get: %s, dataId=%s, parameters=%s", datasetRefOrType, dataId, parameters)
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        if isinstance(datasetRefOrType, DatasetRef):
            idNumber = datasetRefOrType.id
        else:
            idNumber = None
        dataId = DataCoordinate.standardize(dataId, graph=datasetType.dimensions, **kwds)
        # Always lookup the DatasetRef, even if one is given, to ensure it is
        # present in the current collection.
        ref = self.registry.find(self.collection, datasetType, dataId, **kwds)
        if ref is None:
            raise LookupError("Dataset {} with data ID {} could not be found in {}".format(
                              datasetType.name, dataId, self.collection))
        if idNumber is not None and idNumber != ref.id:
            raise ValueError("DatasetRef.id does not match id in registry")
        return self.getDirect(ref, parameters=parameters)

    def getUri(self, datasetRefOrType, dataId=None, predict=False, **kwds):
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
        FileNotFoundError
            A URI has been requested for a dataset that does not exist and
            guessing is not allowed.
        """
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        dataId = self.registry.expandDataId(dataId, graph=datasetType.dimensions, **kwds)
        ref = self.registry.find(self.collection, datasetType, dataId)
        if ref is None:
            if predict:
                if self.run is None:
                    raise ValueError("Cannot predict location from read-only Butler.")
                ref = DatasetRef(datasetType, dataId, run=self.run)
            else:
                raise FileNotFoundError(f"Dataset {datasetType} {dataId} does not exist in Registry.")
        return self.datastore.getUri(ref, predict)

    def datasetExists(self, datasetRefOrType, dataId=None, **kwds):
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
        kwds
            Additional keyword arguments used to augment or construct a
            `DataCoordinate`.  See `DataCoordinate.standardize`
            parameters.

        Raises
        ------
        LookupError
            Raised if the Dataset is not even present in the Registry.
        """
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        dataId = self.registry.expandDataId(dataId, graph=datasetType.dimensions, **kwds)
        ref = self.registry.find(self.collection, datasetType, dataId)
        if ref is None:
            raise LookupError(
                "{} with {} not found in collection {}".format(datasetType, dataId, self.collection)
            )
        return self.datastore.exists(ref)

    def remove(self, datasetRefOrType, dataId=None, *, delete=True, remember=True, **kwds):
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
        kwds
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Raises
        ------
        ValueError
            Raised if ``delete`` and ``remember`` are both `False`; a dataset
            cannot remain in a `Datastore` if all of its `Registry` entries are
            removed.
        OrphanedRecordError
            Raised if ``remember`` is `False` but the dataset is still present
            in a `Datastore` not recognized by this `Butler` client.
        """
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        dataId = self.registry.expandDataId(dataId, graph=datasetType.dimensions, **kwds)
        ref = self.registry.find(self.collection, datasetType, dataId)
        if delete:
            for r in itertools.chain([ref], ref.components.values()):
                # If dataset is a composite, we don't know whether it's the
                # parent or the components that actually need to be removed,
                # so try them all and swallow errors.
                try:
                    self.datastore.remove(r)
                except FileNotFoundError:
                    pass
        elif not remember:
            raise ValueError("Cannot retain dataset in Datastore without keeping Registry dataset record.")
        if remember:
            self.registry.disassociate(self.collection, [ref])
        else:
            # This also implicitly disassociates.
            self.registry.removeDataset(ref)

    @transactional
    def ingest(self, *datasets: FileDataset, transfer: typing.Optional[str] = None):
        """Store and register one or more datasets that already exist on disk.

        Parameters
        ----------
        datasets : `FileDataset`
            Each positional argument is a struct containing information about
            a file to be ingested, including its path (either absolute or
            relative to the datastore root, if applicable), a `DatasetRef`,
            and optionally a formatter class or its fully-qualified string
            name.  If a formatter is not provided, the formatter that would be
            used for `put` is assumed.  On return, all `FileDataset.ref`
            attributes will have their `DatasetRef.id` attribute populated and
            all `FileDataset.formatter` attributes will be set to the formatter
            class used.  `FileDataset.path` attributes may be modified to put
            paths in whatever the datastore considers a standardized form.
        transfer : `str`, optional
            If not `None`, must be one of 'move', 'copy', 'hardlink', or
            'symlink', indicating how to transfer the file.

        Raises
        ------
        TypeError
            Raised if the butler was not constructed with a run, and is hence
            read-only.
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
        """
        if self.run is None:
            raise TypeError("Butler is read-only.")
        # TODO: once Registry has vectorized API for addDataset, use it here.
        for dataset in datasets:
            dataset.ref = self.registry.addDataset(dataset.ref.datasetType, dataset.ref.dataId,
                                                   run=self.run, recursive=True)
        self.datastore.ingest(*datasets, transfer=transfer)

    @contextlib.contextmanager
    def export(self, *, directory: typing.Optional[str] = None,
               filename: typing.Optional[str] = None,
               format: typing.Optional[str] = None,
               transfer: typing.Optional[str] = None) -> typing.ContextManager[RepoExport]:
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
        BackendClass = getClassOf(self.config["repo_transfer_formats"][format]["export"])
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

    def import_(self, *, directory: typing.Optional[str] = None,
                filename: typing.Optional[str] = None,
                format: typing.Optional[str] = None,
                transfer: typing.Optional[str] = None):
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
            Raised if the set of arguments passed is inconsistent.
        """
        if format is None:
            if filename is None:
                raise TypeError("At least one of 'filename' or 'format' must be provided.")
            else:
                _, format = os.path.splitext(filename)
        elif filename is None:
            filename = f"export.{format}"
        if directory is not None and not os.path.exists(filename):
            filename = os.path.join(directory, filename)
        BackendClass = getClassOf(self.config["repo_transfer_formats"][format]["import"])
        with open(filename, 'r') as stream:
            backend = BackendClass(stream)
            with self.transaction():
                backend.load(self.registry, self.datastore,
                             directory=directory, transfer=transfer)

    def validateConfiguration(self, logFailures=False, datasetTypeNames=None, ignore=None):
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
