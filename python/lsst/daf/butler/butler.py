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

import os
import contextlib
import logging

from .core.utils import doImport, transactional
from .core.datasets import DatasetRef
from .core.datastore import Datastore
from .core.registry import Registry
from .core.run import Run
from .core.storageClass import StorageClassFactory
from .core.config import Config, ConfigSubset
from .core.butlerConfig import ButlerConfig
from .core.composites import CompositesMap


__all__ = ("Butler",)

log = logging.getLogger(__name__)


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
    config : `Config`
        Configuration.
    collection : `str`, optional
        Collection to use for all input lookups, overriding
        config["collection"] if provided.
    run : `str`, `Run`, optional
        Collection associated with the `Run` to use for outputs, overriding
        config["run"].  If a `Run` associated with the given Collection does
        not exist, it will be created.  If "collection" is None, this
        collection will be used for input lookups as well; if not, it must have
        the same value as "run".

    Raises
    ------
    ValueError
        Raised if neither "collection" nor "run" are provided by argument or
        config, or if both are provided and are inconsistent.
    """

    @staticmethod
    def makeRepo(root, config=None, standalone=False, createRegistry=True):
        """Create an empty data repository by adding a butler.yaml config
        to a repository root directory.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the new repository.  Will be created
            if it does not exist.
        config : `Config`, optional
            Configuration to write to the repository, after setting any
            root-dependent Registry or Datastore config options.  If `None`,
            default configuration will be used.
        standalone : `bool`
            If True, write all expanded defaults, not just customized or
            repository-specific settings.
            This (mostly) decouples the repository from the default
            configuration, insulating it from changes to the defaults (which
            may be good or bad, depending on the nature of the changes).
            Future *additions* to the defaults will still be picked up when
            initializing `Butlers` to repos created with ``standalone=True``.
        createRegistry : `bool`
            If `True` create a new Registry.

        Note that when ``standalone=False`` (the default), the configuration
        search path (see `ConfigSubset.defaultSearchPaths`) that was used to
        construct the repository should also be used to construct any Butlers
        to it to avoid configuration inconsistencies.

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
        """
        if isinstance(config, (ButlerConfig, ConfigSubset)):
            raise ValueError("makeRepo must be passed a regular Config without defaults applied.")
        root = os.path.abspath(root)
        if not os.path.isdir(root):
            os.makedirs(root)
        config = Config(config)
        full = ButlerConfig(config)  # this applies defaults
        datastoreClass = doImport(full["datastore.cls"])
        datastoreClass.setConfigRoot(root, config, full)
        registryClass = doImport(full["registry.cls"])
        registryClass.setConfigRoot(root, config, full)
        if standalone:
            config.merge(full)
        config.dumpToFile(os.path.join(root, "butler.yaml"))
        # Create Registry and populate tables
        registryClass.fromConfig(config, create=createRegistry)
        return config

    def __init__(self, config=None, collection=None, run=None):
        self.config = ButlerConfig(config)
        self.registry = Registry.fromConfig(self.config)
        self.datastore = Datastore.fromConfig(self.config, self.registry)
        self.storageClasses = StorageClassFactory()
        self.storageClasses.addFromConfig(self.config)
        self.composites = CompositesMap(self.config)
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
            # if run *arg* is not None and collection arg is, use run for collecion.
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

    def __reduce__(self):
        """Support pickling.
        """
        return (Butler, (self.config, ))

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

    @transactional
    def put(self, obj, datasetRefOrType, dataId=None, producer=None):
        """Store and register a dataset.

        Parameters
        ----------
        obj : `object`
            The dataset.
        datasetRefOrType : `DatasetRef`, `DatasetType` instance or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict`, optional
            An identifier with `DataUnit` names and values.
            When `None` a `DatasetRef` should be supplied as the second
            argument.
        producer : `Quantum`, optional
            The producer.

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
        if isinstance(datasetRefOrType, DatasetRef):
            if dataId is not None:
                raise ValueError("DatasetRef given, cannot use dataId as well")
            if datasetRefOrType.id is not None:
                raise ValueError("DatasetRef must not be in registry, must have None id")
            dataId = datasetRefOrType.dataId
            datasetType = datasetRefOrType.datasetType
        else:
            if dataId is None:
                raise ValueError("Must provide a dataId if first argument is not a DatasetRef")
            datasetType = self.registry.getDatasetType(datasetRefOrType)

        isVirtualComposite = self.composites.doDisassembly(datasetType)

        # Add Registry Dataset entry.  If not a virtual composite, add
        # and attach components at the same time.
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
            # Reconstruct the composite
            components = {}
            for compName, compRef in ref.components.items():
                if parameters is None:
                    compParams = None
                else:
                    # make a dictionary of parameters containing only the subset
                    # supported by the StorageClass of the components
                    compParams = {k: v for k, v in parameters.items()
                                  if k in compRef.datasetType.storageClass.components}
                components[compName] = self.datastore.get(compRef, parameters=compParams)

            # Assemble the components
            return ref.datasetType.storageClass.assembler().assemble(components)
        else:
            # single entity in datastore
            raise ValueError("Unable to locate ref {} in datastore {}".format(ref.id, self.datastore.name))

    def get(self, datasetRefOrType, dataId=None, parameters=None):
        """Retrieve a stored dataset.

        Parameters
        ----------
        datasetRefOrType : `DatasetRef`, `DatasetType` instance or `str`
            When `DatasetRef` the `dataId` should be `None`.
            Otherwise the `DatasetType` or name thereof.
        dataId : `dict`
            A `dict` of `DataUnit` link name, value pairs that label the `DatasetRef`
            within a Collection.
            When `None` a `DatasetRef` should be supplied as the second
            argument.
        parameters : `dict`
            Additional StorageClass-defined options to control reading,
            typically used to efficiently read only a subset of the dataset.

        Returns
        -------
        obj : `object`
            The dataset.
        """
        log.debug("Butler get: %s, dataId=%s, parameters=%s", datasetRefOrType, dataId, parameters)
        if isinstance(datasetRefOrType, DatasetRef):
            if dataId is not None:
                raise ValueError("DatasetRef given, cannot use dataId as well")
            datasetType = datasetRefOrType.datasetType
            dataId = datasetRefOrType.dataId
            idNumber = datasetRefOrType.id
        else:
            datasetType = self.registry.getDatasetType(datasetRefOrType)
            idNumber = None
        # Always lookup the DatasetRef, even if one is given, to ensure it is
        # present in the current collection.
        ref = self.registry.find(self.collection, datasetType, dataId)
        if ref is None:
            raise LookupError("Dataset {} with data ID {} could not be found in {}".format(
                              datasetType.name, dataId, self.collection))
        if idNumber is not None and idNumber != ref.id:
            raise ValueError("DatasetRef.id does not match id in registry")
        return self.getDirect(ref, parameters=parameters)

    def getUri(self, datasetType, dataId, predict=False):
        """Return the URI to the Dataset.

        Parameters
        ----------
        datasetType : `DatasetType` instance or `str`
            The `DatasetType`.
        dataId : `dict`
            A `dict` of `DataUnit` link name, value pairs that label the
            `DatasetRef` within a Collection.
        predict : `bool`
            If `True`, allow URIs to be returned of datasets that have not
            been written.

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
        datasetType = self.registry.getDatasetType(datasetType)
        ref = self.registry.find(self.collection, datasetType, dataId)
        return self.datastore.getUri(ref, predict)

    def datasetExists(self, datasetType, dataId):
        """Return True if the Dataset is actually present in the Datastore.

        Parameters
        ----------
        datasetType : `DatasetType` instance or `str`
            The `DatasetType`.
        dataId : `dict`
            A `dict` of `DataUnit` link name, value pairs that label the
            `DatasetRef` within a Collection.

        Raises
        ------
        LookupError
            Raised if the Dataset is not even present in the Registry.
        """
        datasetType = self.registry.getDatasetType(datasetType)
        ref = self.registry.find(self.collection, datasetType, dataId)
        if ref is None:
            raise LookupError(
                "{} with {} not found in collection {}".format(datasetType, dataId, self.collection)
            )
        return self.datastore.exists(ref)
