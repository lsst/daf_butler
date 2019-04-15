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

"""Chained datastore."""

__all__ = ("ChainedDatastore",)

import time
import logging
import os
import warnings

from lsst.utils import doImport
from lsst.daf.butler import Datastore, DatastoreConfig, StorageClassFactory, DatasetTypeNotSupportedError, \
    DatastoreValidationError, Constraints

log = logging.getLogger(__name__)


class ChainedDatastore(Datastore):
    """Chained Datastores to allow read and writes from multiple datastores.

    A ChainedDatastore is configured with multiple datastore configurations.
    A ``put()`` is always sent to each datastore. A ``get()``
    operation is sent to each datastore in turn and the first datastore
    to return a valid dataset is used.

    Attributes
    ----------
    config : `DatastoreConfig`
        Configuration used to create Datastore.
    storageClassFactory : `StorageClassFactory`
        Factory for creating storage class instances from name.
    name : `str`
        Label associated with this Datastore.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration.  This configuration must include a ``datastores`` field
        as a sequence of datastore configurations.  The order in this sequence
        indicates the order to use for read operations.
    """

    defaultConfigFile = "datastores/chainedDatastore.yaml"
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    containerKey = "datastores"
    """Key to specify where child datastores are configured."""

    @classmethod
    def setConfigRoot(cls, root, config, full):
        """Set any filesystem-dependent config options for child Datastores to
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
            converted to a `DatastoreConfig`. Read-only and will not be
            modified by this method.
            Repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from `full` to `Config`.
        """

        # Extract the part of the config we care about updating
        datastoreConfig = DatastoreConfig(config, mergeDefaults=False)

        # And the subset of the full config that we can use for reference.
        # Do not bother with defaults because we are told this already has
        # them.
        fullDatastoreConfig = DatastoreConfig(full, mergeDefaults=False)

        # Loop over each datastore config and pass the subsets to the
        # child datastores to process.

        containerKey = cls.containerKey
        for idx, (child, fullChild) in enumerate(zip(datastoreConfig[containerKey],
                                                     fullDatastoreConfig[containerKey])):
            childConfig = DatastoreConfig(child, mergeDefaults=False)
            fullChildConfig = DatastoreConfig(fullChild, mergeDefaults=False)
            datastoreClass = doImport(fullChildConfig["cls"])
            newroot = "{}/{}_{}".format(root, datastoreClass.__qualname__, idx)
            datastoreClass.setConfigRoot(newroot, childConfig, fullChildConfig)

            # Reattach to parent
            datastoreConfig[containerKey, idx] = childConfig

        # Reattach modified datastore config to parent
        # If this has a datastore key we attach there, otherwise we assume
        # this information goes at the top of the config hierarchy.
        if DatastoreConfig.component in config:
            config[DatastoreConfig.component] = datastoreConfig
        else:
            config.update(datastoreConfig)

        return

    def __init__(self, config, registry=None, butlerRoot=None):
        super().__init__(config, registry)

        self.storageClassFactory = StorageClassFactory()

        # Scan for child datastores and instantiate them with the same registry
        self.datastores = []
        for c in self.config["datastores"]:
            c = DatastoreConfig(c)
            datastoreType = doImport(c["cls"])
            datastore = datastoreType(c, registry, butlerRoot=butlerRoot)
            log.debug("Creating child datastore %s", datastore.name)
            self.datastores.append(datastore)

        # Name ourself based on our children
        if self.datastores:
            childNames = ",".join([d.name for d in self.datastores])
        else:
            childNames = "(empty@{})".format(time.time())
        self.name = "{}[{}]".format(type(self).__qualname__, childNames)

        # We declare we are ephemeral if all our child datastores declare
        # they are ephemeral
        isEphemeral = True
        for d in self.datastores:
            if not d.isEphemeral:
                isEphemeral = False
                break
        self.isEphemeral = isEphemeral

        # And read the constraints list
        constraintsConfig = self.config.get("constraints")
        self.constraints = Constraints(constraintsConfig, universe=self.registry.dimensions)

        # per-datastore override constraints
        if "datastore_constraints" in self.config:
            overrides = self.config["datastore_constraints"]

            if len(overrides) != len(self.datastores):
                raise DatastoreValidationError(f"Number of registered datastores ({len(self.datastores)})"
                                               " differs from number of constraints overrides"
                                               f" {len(overrides)}")

            self.datastoreConstraints = [Constraints(c.get("constraints"))
                                         for c in overrides]

        else:
            self.datastoreConstraints = (None,) * len(self.datastores)

        log.debug("Created %s (%s)", self.name, ("ephemeral" if self.isEphemeral else "permanent"))

    def __str__(self):
        chainName = ", ".join(str(ds) for ds in self.datastores)
        return chainName

    def exists(self, ref):
        """Check if the dataset exists in one of the datastores.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required dataset.

        Returns
        -------
        exists : `bool`
            `True` if the entity exists in one of the child datastores.
        """
        for datastore in self.datastores:
            if datastore.exists(ref):
                log.debug("Found %s in datastore %s", ref, datastore.name)
                return True
        return False

    def get(self, ref, parameters=None):
        """Load an InMemoryDataset from the store.

        The dataset is returned from the first datastore that has
        the dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
        parameters : `dict`
            `StorageClass`-specific parameters that specify, for example,
            a slice of the Dataset to be loaded.

        Returns
        -------
        inMemoryDataset : `object`
            Requested Dataset or slice thereof as an InMemoryDataset.

        Raises
        ------
        FileNotFoundError
            Requested dataset can not be retrieved.
        TypeError
            Return value from formatter has unexpected type.
        ValueError
            Formatter failed to process the dataset.
        """

        for datastore in self.datastores:
            try:
                inMemoryObject = datastore.get(ref, parameters)
                log.debug("Found Dataset %s in datastore %s", ref, datastore.name)
                return inMemoryObject
            except FileNotFoundError:
                pass

        raise FileNotFoundError("Dataset {} could not be found in any of the datastores".format(ref))

    def put(self, inMemoryDataset, ref):
        """Write a InMemoryDataset with a given `DatasetRef` to each
        datastore.

        The put() to child datastores can fail with
        `DatasetTypeNotSupportedError`.  The put() for this datastore will be
        deemed to have succeeded so long as at least one child datastore
        accepted the inMemoryDataset.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.
        ref : `DatasetRef`
            Reference to the associated Dataset.

        Raises
        ------
        TypeError
            Supplied object and storage class are inconsistent.
        DatasetTypeNotSupportedError
            All datastores reported `DatasetTypeNotSupportedError`.
        """
        log.debug("Put %s", ref)

        # Confirm that we can accept this dataset
        if not self.constraints.isAcceptable(ref):
            # Raise rather than use boolean return value.
            raise DatasetTypeNotSupportedError(f"Dataset {ref} has been rejected by this datastore via"
                                               " configuration.")

        isPermanent = False
        nsuccess = 0
        npermanent = 0
        nephemeral = 0
        for datastore, constraints in zip(self.datastores, self.datastoreConstraints):
            if constraints is not None and not constraints.isAcceptable(ref):
                log.debug("Datastore %s skipping put via configuration for ref %s",
                          datastore.name, ref)
                continue

            if datastore.isEphemeral:
                nephemeral += 1
            else:
                npermanent += 1
            try:
                datastore.put(inMemoryDataset, ref)
                nsuccess += 1
                if not datastore.isEphemeral:
                    isPermanent = True
            except DatasetTypeNotSupportedError:
                pass

        if nsuccess == 0:
            raise DatasetTypeNotSupportedError(f"None of the chained datastores supported ref {ref}")

        if not isPermanent and npermanent > 0:
            warnings.warn(f"Put of {ref} only succeeded in ephemeral databases", stacklevel=2)

        if self._transaction is not None:
            self._transaction.registerUndo('put', self.remove, ref)

    def ingest(self, path, ref, formatter=None, transfer=None):
        """Add an on-disk file with the given `DatasetRef` to the store,
        possibly transferring it.

        This method is forwarded to each of the chained datastores, trapping
        cases where a datastore has not implemented file ingest and ignoring
        them.

        Notes
        -----
        If an absolute path is given and "move" mode is specified, then
        we tell the child datastore to use "copy" mode and unlink it
        at the end.  If a relative path is given then it is assumed the file
        is already inside the child datastore.

        A transfer mode of None implies that the file is already within
        each of the (relevant) child datastores.

        Parameters
        ----------
        path : `str`
            File path.  Treated as relative to the repository root of each
            child datastore if not absolute.
        ref : `DatasetRef`
            Reference to the associated Dataset.
        formatter : `Formatter` (optional)
            Formatter that should be used to retreive the Dataset.  If not
            provided, the formatter will be constructed according to
            Datastore configuration.
        transfer : str (optional)
            If not None, must be one of 'move', 'copy', 'hardlink', or
            'symlink' indicating how to transfer the file.  The new
            filename and location will be determined via template substitution,
            as with ``put``.  If the file is outside the datastore root, it
            must be transferred somehow.

        Raises
        ------
        NotImplementedError
            If all chained datastores have no ingest implemented or if
            a transfer mode of `None` is specified.
        DatasetTypeNotSupportedError
            The associated `DatasetType` is not handled by this datastore.
        """
        log.debug("Ingesting %s (transfer=%s)", ref, transfer)

        # Confirm that we can accept this dataset
        if not self.constraints.isAcceptable(ref):
            # Raise rather than use boolean return value.
            raise DatasetTypeNotSupportedError(f"Dataset {ref} has been rejected by this datastore via"
                                               " configuration.")

        if transfer is None:
            raise NotImplementedError("ChainedDatastore does not support transfer=None")

        # A "move" is sometimes a "copy"
        moveIsCopy = False
        if transfer == "move" and os.path.isabs(path):
            moveIsCopy = True

        notImplementedCounter = 0
        notAcceptedCounter = 0
        for datastore, constraints in zip(self.datastores, self.datastoreConstraints):
            if constraints is not None and not constraints.isAcceptable(ref):
                log.debug("Datastore %s skipping ingest via configuration for ref %s",
                          datastore.name, ref)
                notAcceptedCounter += 1
                continue

            dstransfer = transfer
            # Each child datastore must copy the file for a move operation
            if moveIsCopy:
                dstransfer = "copy"
            try:
                datastore.ingest(path, ref, transfer=dstransfer, formatter=formatter)
            except NotImplementedError:
                notImplementedCounter += 1
            except DatasetTypeNotSupportedError:
                notAcceptedCounter += 1

        if (notAcceptedCounter + notImplementedCounter) == len(self.datastores):
            log.warning("Datastore %s: Not accepted counter: %d; Not implemented counter: %d for ref %s",
                        self.name, notAcceptedCounter, notImplementedCounter, ref)
            if notAcceptedCounter > 0:
                raise DatasetTypeNotSupportedError(f"Ingest of {ref} not supported by the chained datastores")
            else:
                raise NotImplementedError("Ingest not implemented by any of the chained datastores")

        # if the file was meant to be moved then we have to delete it
        if moveIsCopy:
            os.unlink(path)

    def getUri(self, ref, predict=False):
        """URI to the Dataset.

        The returned URI is from the first datastore in the list that has
        the dataset with preference given to the first dataset coming from
        a permanent datastore. If no datastores have the dataset and prediction
        is allowed, the predicted URI for the first datastore in the list will
        be returned.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
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

        Notes
        -----
        If the datastore does not have entities that relate well
        to the concept of a URI the returned URI string will be
        descriptive. The returned URI is not guaranteed to be obtainable.

        Raises
        ------
        FileNotFoundError
            A URI has been requested for a dataset that does not exist and
            guessing is not allowed.
        """
        log.debug("Requesting URI for %s", ref)
        predictedUri = None
        predictedEphemeralUri = None
        firstEphemeralUri = None
        for datastore in self.datastores:
            if datastore.exists(ref):
                if not datastore.isEphemeral:
                    uri = datastore.getUri(ref)
                    log.debug("Retrieved ephemeral URI: %s", uri)
                    return uri
                elif firstEphemeralUri is None:
                    firstEphemeralUri = datastore.getUri(ref)
            elif predict:
                if predictedUri is None and not datastore.isEphemeral:
                    predictedUri = datastore.getUri(ref, predict)
                elif predictedEphemeralUri is None and datastore.isEphemeral:
                    predictedEphemeralUri = datastore.getUri(ref, predict)

        if firstEphemeralUri is not None:
            log.debug("Retrieved ephemeral URI: %s", firstEphemeralUri)
            return firstEphemeralUri

        if predictedUri is not None:
            log.debug("Retrieved predicted URI: %s", predictedUri)
            return predictedUri

        if predictedEphemeralUri is not None:
            log.debug("Retrieved predicted ephemeral URI: %s", predictedEphemeralUri)
            return predictedEphemeralUri

        raise FileNotFoundError("Dataset {} not in any datastore".format(ref))

    def remove(self, ref):
        """Indicate to the Datastore that a Dataset can be removed.

        The dataset will be removed from each datastore.  The dataset is
        not required to exist in every child datastore.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.

        Raises
        ------
        FileNotFoundError
            Attempt to remove a dataset that does not exist.  Raised if none
            of the child datastores removed the dataset.
        """
        log.debug(f"Removing {ref}")

        counter = 0
        for datastore in self.datastores:
            try:
                datastore.remove(ref)
                counter += 1
            except FileNotFoundError:
                pass

        if counter == 0:
            raise FileNotFoundError(f"Could not remove from any child datastore: {ref}")

    def transfer(self, inputDatastore, ref):
        """Retrieve a Dataset from an input `Datastore`,
        and store the result in this `Datastore`.

        Parameters
        ----------
        inputDatastore : `Datastore`
            The external `Datastore` from which to retreive the Dataset.
        ref : `DatasetRef`
            Reference to the required Dataset in the input data store.

        Returns
        -------
        results : `list`
            List containing the return value from the ``put()`` to each
            child datastore.
        """
        assert inputDatastore is not self  # unless we want it for renames?
        inMemoryDataset = inputDatastore.get(ref)
        return [datastore.put(inMemoryDataset, ref) for datastore in self.datastores]

    def validateConfiguration(self, entities, logFailures=False):
        """Validate some of the configuration for this datastore.

        Parameters
        ----------
        entities : iterable of `DatasetRef`, `DatasetType`, or `StorageClass`
            Entities to test against this configuration.  Can be differing
            types.
        logFailures : `bool`, optional
            If `True`, output a log message for every validation error
            detected.

        Raises
        ------
        DatastoreValidationError
            Raised if there is a validation problem with a configuration.
            All the problems are reported in a single exception.

        Notes
        -----
        This method checks each datastore in turn.
        """

        # Need to catch each of the datastore outputs and ensure that
        # all are tested.
        failures = []
        for datastore in self.datastores:
            try:
                datastore.validateConfiguration(entities, logFailures=logFailures)
            except DatastoreValidationError as e:
                if logFailures:
                    log.fatal("Datastore %s failed validation", datastore.name)
                failures.append(f"Datastore {self.name}: {e}")

        if failures:
            msg = ";\n".join(failures)
            raise DatastoreValidationError(msg)

    def validateKey(self, lookupKey, entity):
        # Docstring is inherited from base class
        failures = []
        for datastore in self.datastores:
            try:
                datastore.validateKey(lookupKey, entity)
            except DatastoreValidationError as e:
                failures.append(f"Datastore {self.name}: {e}")

        if failures:
            msg = ";\n".join(failures)
            raise DatastoreValidationError(msg)

    def getLookupKeys(self):
        # Docstring is inherited from base class
        keys = set()
        for datastore in self.datastores:
            keys.update(datastore.getLookupKeys())

        keys.update(self.constraints.getLookupKeys())
        for p in self.datastoreConstraints:
            if p is not None:
                keys.update(p.getLookupKeys())

        return keys
