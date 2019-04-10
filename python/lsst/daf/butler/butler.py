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
import tempfile
import io

NO_BOTO = False
try:
    import boto3
except ImportError:
    NO_BOTO = True

from lsst.utils import doImport
from .core.utils import transactional, parsePath2Uri
from .core.datasets import DatasetRef, DatasetType
from .core.datastore import Datastore
from .core.registry import Registry
from .core.run import Run
from .core.storageClass import StorageClassFactory
from .core.config import Config, ConfigSubset
from .core.butlerConfig import ButlerConfig
from .core.composites import CompositesMap
from .core.dimensions import DataId
from .core.exceptions import ValidationError
from .core.repoRelocation import BUTLER_ROOT_TAG
from .core.safeFileIo import safeMakeDir

log = logging.getLogger(__name__)


class ButlerValidationError(ValidationError):
    """There is a problem with the Butler configuration."""
    pass


# I didn't want to clog up the butler with more classmethods or a long makeRepo with ifs
# Realistically this seems to warrant a S3RDSButler that inherits from Butler and has a
# different makeRepo method, but I like having just one Butler. It could be re-assigned
# dynamically in __new__, or the methods can be just added as classmethods.
# Don't think that's really my call to make so there we go and here we are
def _makeLocalRepo(root, config=None, standalone=False, createRegistry=True):
    """Create an empty data repository by adding a butler.yaml config
    to a repository root directory on a local filesystem.
    """
    if isinstance(config, (ButlerConfig, ConfigSubset)):
        raise ValueError("makeRepo must be passed a regular Config without defaults applied.")

    root = os.path.abspath(root)
    if not os.path.isdir(root):
        safeMakeDir(root)

    config = Config(config)

    # If we are creating a new repo from scratch with relative roots,
    # do not propagate an explicit root from the config file
    if "root" in config:
        del config["root"]

    full = ButlerConfig(config)  # this applies defaults
    datastoreClass = doImport(full["datastore", "cls"])

    datastoreClass.setConfigRoot(BUTLER_ROOT_TAG, config, full)
    registryClass = doImport(full["registry", "cls"])
    registryClass.setConfigRoot(BUTLER_ROOT_TAG, config, full)
    if standalone:
        config.merge(full)

    config.dumpToFile(os.path.join(root, "butler.yaml"))

    # Create Registry and populate tables
    registryClass.fromConfig(config, create=createRegistry, butlerRoot=root)
    return config


def _makeBucketRepo(root, config=None, standalone=False, createRegistry=True):
    """Create an empty data repository by adding a butler.yaml config
    to a repository root directory in a S3 Bucket.
    """
    if NO_BOTO:
        raise ModuleNotFoundError("Could not find boto3. Are you sure it is installed?")

    if isinstance(config, (ButlerConfig, ConfigSubset)):
        raise ValueError("makeRepo must be passed a regular Config without defaults applied.")

    # Assumes bucket exists. Another level of checks is needed to verify, but the DataStore
    # is what should have the bucket creation code, so that's where it was placed (example only)
    scheme, rootpath, relpath = parsePath2Uri(root)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(rootpath)
    bucket.put_object(Bucket=rootpath, Key=(relpath))

    # this part is the same for both makerepo functions, except BUTLER_ROOT_TAG
    # I was not sure what the deal with butler root tag is so I didn't touch it. It appeared
    # after merging upstream and I didn't want to spend time figuring out paths again
    config = Config(config)
    # If we are creating a new repo from scratch with relative roots
    # do not propagate an explicit root from the config file
    #if "root" in config:
    #    del config["root"]
    full = ButlerConfig(config)  # this applies defaults

    datastoreClass = doImport(full["datastore", "cls"])
    datastoreClass.setConfigRoot(root, config, full)
    registryClass = doImport(full["registry", "cls"])
    # this if here is a monkeypatch for the bucket datastore, local repo tests
    # I don't really understand the BUTLER_ROOT_TAG thing
    if full['.registry.db'] != 'sqlite:///:memory:':
        registryClass.setConfigRoot(BUTLER_ROOT_TAG, config, full)
    if standalone:
        config.merge(full)

    # instead of a lot of temporary files, dump to stream, rewind and read
    stream = io.StringIO()
    config.dump(stream)
    stream.seek(0)

    s3 = boto3.client('s3')
    bucketpath = os.path.join(relpath, 'butler.yaml')
    s3.put_object(Bucket=rootpath, Key=bucketpath, Body=stream.read())

    # Create Registry and populate tables
    registryClass.fromConfig(config, create=createRegistry, butlerRoot=root)
    return config



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
    run : `str`, `Run`, optional
        Collection associated with the `Run` to use for outputs, overriding
        config["run"].  If a `Run` associated with the given Collection does
        not exist, it will be created.  If "collection" is None, this
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
        root = os.path.abspath(root)
        if not os.path.isdir(root):
            safeMakeDir(root)
        config = Config(config)

        scheme, rootpath, relpath = parsePath2Uri(root)
        if scheme == 'file://':
            root = os.path.abspath(root)
            if not os.path.isdir(root):
                os.makedirs(root)
        elif scheme == 's3://':
            s3 = boto3.resource('s3')
            # implies bucket exists, if not another level of checks
            bucket = s3.Bucket(rootpath)
            bucket.put_object(Bucket=rootpath, Key=(relpath))

        # If we are creating a new repo from scratch with relative roots,
        # do not propagate an explicit root from the config file
        if "root" in config:
            del config["root"]

        full = ButlerConfig(config)  # this applies defaults
        datastoreClass = doImport(full["datastore", "cls"])
        datastoreClass.setConfigRoot(BUTLER_ROOT_TAG, config, full, overwrite=forceConfigRoot)
        datastoreClass.setConfigRoot(root, config, full)
        registryClass = doImport(full["registry", "cls"])
        registryClass.setConfigRoot(BUTLER_ROOT_TAG, config, full, overwrite=forceConfigRoot)
        if standalone:
            config.merge(full)

        if scheme == 'file://':
            config.dumpToFile(os.path.join(root, "butler.yaml"))
        elif scheme == 's3://':
            config.dumpToS3(rootpath, os.path.join(relpath, 'butler.yaml'))

        # Create Registry and populate tables
        registryClass.fromConfig(config, create=createRegistry, butlerRoot=root)
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
            runCollection = self.config.get("run", None)
            self.run = None
        else:
            if isinstance(run, Run):
                self.run = run
                runCollection = self.run.collection
            else:
                runCollection = run
                self.run = None
            # if run *arg* is not None and collection arg is, use run for
            # collection.
            if collection is None:
                collection = runCollection
        del run  # it's a logic bug if we try to use this variable below
        if collection is None:  # didn't get a collection from collection or run *args*
            collection = self.config.get("collection", None)
            if collection is None:  # didn't get a collection from config["collection"]
                collection = runCollection    # get collection from run found in config
        if collection is None:
            raise ValueError("No run or collection provided.")
        if runCollection is not None and collection != runCollection:
            raise ValueError(
                "Run ({}) and collection ({}) are inconsistent.".format(runCollection, collection)
            )
        self.collection = collection
        if runCollection is not None and self.run is None:
            self.run = self.registry.getRun(collection=runCollection)
            if self.run is None:
                self.run = self.registry.makeRun(runCollection)

    def __del__(self):
        # Attempt to close any open resources when this object is
        # garbage collected. Python does not guarantee that this method
        # will be called, or that it will be called in the proper order
        # so if possible the user should explicitly call close. This
        # exists only to make an attempt to shut things down as a last
        # resort if at all possible.
        self.close()

    def close(self):
        """This method should be called to properly close any resources the
        butler may have open. The instance on which this method is closed
        should be considered unusable and no further methods should be called
        on it.
        """
        self.registry.close()

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
        dataId : `dict` or `DataId`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the second argument.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

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
        dataId : `dict` or `DataId`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the second argument.
        producer : `Quantum`, optional
            The producer.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Returns
        -------
        ref : `DatasetRef`
            A reference to the stored dataset, updated with the correct id if
            given.

        Raises
        ------
        TypeError
            Raised if the butler was not constructed with a Run, and is hence
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
        ref = self.registry.addDataset(datasetType, dataId, run=self.run, producer=producer,
                                       recursive=not isVirtualComposite, **kwds)

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
            raise FileNotFoundError("Unable to locate ref {} in datastore {}".format(ref.id,
                                                                                     self.datastore.name))

    def get(self, datasetRefOrType, dataId=None, parameters=None, **kwds):
        """Retrieve a stored dataset.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataId`
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
        obj : `object`
            The dataset.
        """
        log.debug("Butler get: %s, dataId=%s, parameters=%s", datasetRefOrType, dataId, parameters)
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        if isinstance(datasetRefOrType, DatasetRef):
            idNumber = datasetRefOrType.id
        else:
            idNumber = None
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
        dataId : `dict` or `DataId`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        predict : `bool`
            If `True`, allow URIs to be returned of datasets that have not
            been written.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

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
        dataId = DataId(dataId, dimensions=datasetType.dimensions, universe=self.registry.dimensions, **kwds)
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
        dataId : `dict` or `DataId`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the first argument.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Raises
        ------
        LookupError
            Raised if the Dataset is not even present in the Registry.
        """
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        ref = self.registry.find(self.collection, datasetType, dataId, **kwds)
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
        ref = self.registry.find(self.collection, datasetType, dataId, **kwds)
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
    def ingest(self, path, datasetRefOrType, dataId=None, *, formatter=None, transfer=None, **kwds):
        """Store and register a dataset that already exists on disk.

        Parameters
        ----------
        path : `str`
            Path to the file containing the dataset.
        datasetRefOrType : `DatasetRef`, `DatasetType`, or `str`
            When `DatasetRef` is provided, ``dataId`` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict` or `DataId`
            A `dict` of `Dimension` link name, value pairs that label the
            `DatasetRef` within a Collection. When `None`, a `DatasetRef`
            should be provided as the second argument.
        formatter : `Formatter` (optional)
            Formatter that should be used to retreive the Dataset.  If not
            provided, the formatter will be constructed according to
            Datastore configuration.
        transfer : str (optional)
            If not None, must be one of 'move', 'copy', 'hardlink', or
            'symlink' indicating how to transfer the file.
        kwds
            Additional keyword arguments used to augment or construct a
            `DataId`.  See `DataId` parameters.

        Returns
        -------
        ref : `DatasetRef`
            A reference to the stored dataset, updated with the correct id if
            given.

        Raises
        ------
        TypeError
            Raised if the butler was not constructed with a Run, and is hence
            read-only.
        NotImplementedError
            Raised if the `Datastore` does not support the given transfer mode.
        """
        if self.run is None:
            raise TypeError("Butler is read-only.")
        datasetType, dataId = self._standardizeArgs(datasetRefOrType, dataId, **kwds)
        ref = self.registry.addDataset(datasetType, dataId, run=self.run, recursive=True, **kwds)
        self.datastore.ingest(path, ref, transfer=transfer, formatter=formatter)
        return ref

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
        instruments = set()
        if not self.registry.limited:
            instrumentEntries = self.registry.findDimensionEntries("instrument")
            instruments = {e["instrument"] for e in instrumentEntries}

        # For each datasetType that has an instrument dimension, create
        # a DatasetRef for each defined instrument
        datasetRefs = []

        for datasetType in entities:
            if "instrument" in datasetType.dimensions:
                for instrument in instruments:
                    datasetRef = DatasetRef(datasetType, {"instrument": instrument})
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
