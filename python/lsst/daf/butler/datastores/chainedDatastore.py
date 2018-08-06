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

import time
import logging

from lsst.daf.butler.core.utils import doImport
from lsst.daf.butler.core.datastore import Datastore, DatastoreConfig
from lsst.daf.butler.core.storageClass import StorageClassFactory

log = logging.getLogger(__name__)


__all__ = ("ChainedDatastore", )


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
            datastoreConfig[f"{containerKey}.{idx}"] = childConfig

        # Reattach modified datastore config to parent
        # If this has a datastore key we attach there, otherwise we assume
        # this information goes at the top of the config hierarchy.
        if DatastoreConfig.component in config:
            config[DatastoreConfig.component] = datastoreConfig
        else:
            config.update(datastoreConfig)

        return

    def __init__(self, config, registry=None):
        super().__init__(config, registry)

        self.storageClassFactory = StorageClassFactory()

        # Scan for child datastores and instantiate them with the same registry
        self.datastores = []
        for c in self.config["datastores"]:
            c = DatastoreConfig(c)
            datastoreType = doImport(c["cls"])
            datastore = datastoreType(c, registry)
            log.debug("Creating child datastore %s", datastore.name)
            self.datastores.append(datastore)

        # Name ourself based on our children
        if self.datastores:
            childNames = ",".join([d.name for d in self.datastores])
        else:
            childNames = "(empty@{})".format(time.time())
        self.name = "{}[{}]".format(type(self).__qualname__, childNames)
        log.debug("Created %s", self.name)

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
        """

        for datastore in self.datastores:
            datastore.put(inMemoryDataset, ref)

        if self._transaction is not None:
            self._transaction.registerUndo('put', self.remove, ref)

    def ingest(self, *args, **kwargs):
        """Add an on-disk file with the given `DatasetRef` to the store,
        possibly transferring it.

        This method is forwarded to each of the chained datastores, trapping
        cases where a datastore has not implemented file ingest and ignoring
        them.

        A transfer mode of None is not supported since that requires the
        file to have been previously copied to each individual datastore.

        Raises
        ------
        NotImplementedError
            If all chained datastores have no ingest implemented or if
            a transfer mode of `None` is specified.
        """
        log.debug("Ingesting %s (transfer=%s)", args[1], kwargs["transfer"])

        if kwargs["transfer"] is None:
            raise NotImplementedError("ChainedDatastore does not support transfer=None")

        counter = 0
        for datastore in self.datastores:
            try:
                datastore.ingest(*args, **kwargs)
            except NotImplementedError:
                counter += 1
                pass
        if counter == len(self.datastores):
            raise NotImplementedError("Ingest not implemented by any of the chained datastores")

    def getUri(self, ref, predict=False):
        """URI to the Dataset.

        The returned URI is from the first datastore in the list that has
        the dataset. If no datastores have the dataset and prediction is
        allowed, the predicted URI for the first datastore in the list will
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
        predicted = None
        for datastore in self.datastores:
            if datastore.exists(ref):
                return datastore.getUri(ref)
            elif predicted is None and predict:
                predicted = datastore.getUri(ref, predict)

        if predicted is not None:
            return predicted

        raise FileNotFoundError("Dataset {} not in any datastore".format(ref))

    def remove(self, ref):
        """Indicate to the Datastore that a Dataset can be removed.

        The dataset will be removed from each datastore.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.

        Raises
        ------
        FileNotFoundError
            Attempt to remove a dataset that does not exist.
        """
        for datastore in self.datastores:
            datastore.remove(ref)

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
