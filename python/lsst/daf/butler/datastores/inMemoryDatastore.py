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

"""In-memory datastore."""

import time
from collections import namedtuple

from lsst.daf.butler.core.datastore import Datastore
from lsst.daf.butler.core.storageClass import StorageClassFactory
from lsst.daf.butler.core.utils import get_object_size
from lsst.daf.butler.core.storageInfo import StorageInfo


class StoredItemInfo:
    """Internal Metadata associated with a DatasetRef.

    Parameters
    ----------
    timestamp : `int`
        Unix timestamp indicating the time the dataset was stored.
    storageClass : `StorageClass`
        StorageClass associated with the dataset.
    """

    def __init__(self, timestamp, storageClass):
        self.timestamp = timestamp
        self.storageClass = storageClass

    def __str__(self):
        return "StoredItemInfo({}, {})".format(self.timestamp, self.storageClass.name)

    def __repr__(self):
        return "StoredItemInfo({!r}, {!r})".format(self.timestamp, self.storageClass)


class InMemoryDatastore(Datastore):
    """Basic Datastore for writing to an in memory cache.

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
        Configuration.
    """

    defaultConfigFile = "datastores/inMemoryDatastore.yaml"
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    RecordTuple = namedtuple("InMemoryDatastoreRecord",
                             ["timestamp", "storage_class"])

    def __init__(self, config, registry=None):
        super().__init__(config, registry)

        self.storageClassFactory = StorageClassFactory()

        # Name ourselves with the timestamp the datastore
        # was created.
        self.name = "InMemoryDatastore@{}".format(time.time())
        print("Creating with name: {}".format(self.name))

        # Storage of datasets, keyed by dataset_id
        self.datasets = {}
        self.records = {}

    @classmethod
    def setConfigRoot(cls, root, config, full):
        """Set any filesystem-dependent config options for this Datastore to
        be appropriate for a new empty repository with the given root.

        Does nothing in this implementation.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the data repository.
        config : `Config`
            A Butler-level config object to update (but not a
            `ButlerConfig`, to avoid included expanded defaults).
        full : `ButlerConfig`
            A complete Butler config with all defaults expanded;
            repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from `full` to `Config`.
        """
        return

    def addStoredItemInfo(self, ref, info):
        """Record internal storage information associated with this
        `DatasetRef`.

        Parameters
        ----------
        ref : `DatasetRef`
            The Dataset that has been stored.
        info : `StoredItemInfo`
            Metadata associated with the stored Dataset.

        Raises
        ------
        KeyError
            An entry with this DatasetRef already exists.
        """
        if ref.id in self.records:
            raise KeyError("Attempt to store item info with ID {}"
                           " when that ID exists as '{}'".format(ref.id, self.records[ref.id]))
        self.records[ref.id] = info

    def removeStoredItemInfo(self, ref):
        """Remove information about the object associated with this dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            The Dataset that has been removed.
        """
        del self.records[ref.id]

    def getStoredItemInfo(self, ref):
        """Retrieve information associated with object stored in this
        `Datastore`.

        Parameters
        ----------
        ref : `DatasetRef`
            The Dataset that is to be queried.

        Returns
        -------
        info : `StoredItemInfo`
            Stored information about the internal location of this file
            and its formatter.

        Raises
        ------
        KeyError
            Dataset with that id can not be found.
        """
        record = self.records.get(ref.id, None)
        if record is None:
            raise KeyError("Unable to retrieve information associated with Dataset {}".format(ref.id))
        return record

    def exists(self, ref):
        """Check if the dataset exists in the datastore.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required dataset.

        Returns
        -------
        exists : `bool`
            `True` if the entity exists in the `Datastore`.
        """
        print("Check existence datased {}".format(ref.id))
        print(self.datasets)
        print(self.records)
        return ref.id in self.datasets

    def get(self, ref, parameters=None):
        """Load an InMemoryDataset from the store.

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

        print("Attempting to retrieve dataset {}".format(ref))

        if not self.exists(ref):
            raise FileNotFoundError("Could not retrieve Dataset {}".format(ref))

        # We have a write storage class and a read storage class and they
        # can be different for concrete composites.
        readStorageClass = ref.datasetType.storageClass
        storedItemInfo = self.getStoredItemInfo(ref)
        writeStorageClass = storedItemInfo.storageClass

        inMemoryDataset = self.datasets[ref.id]

        # Different storage classes implies a component request
        if readStorageClass != writeStorageClass:

            component = ref.datasetType.component()

            if component is None:
                raise ValueError("Storage class inconsistency ({} vs {}) but no"
                                 " component requested".format(readStorageClass.name,
                                                               writeStorageClass.name))

            # Concrete composite written as a single object (we hope)
            inMemoryDataset = writeStorageClass.assembler().getComponent(inMemoryDataset, component)

        # Validate the returned data type matches the expected data type
        pytype = readStorageClass.pytype
        if pytype and not isinstance(inMemoryDataset, pytype):
            raise TypeError("Got Python type {} (datasetType '{}')"
                            " but expected {}".format(type(inMemoryDataset),
                                                      ref.datasetType.name, pytype))

        return inMemoryDataset

    def put(self, inMemoryDataset, ref):
        """Write a InMemoryDataset with a given `DatasetRef` to the store.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.
        ref : `DatasetRef`
            Reference to the associated Dataset.
        """

        datasetType = ref.datasetType
        storageClass = datasetType.storageClass

        # Sanity check
        if not isinstance(inMemoryDataset, storageClass.pytype):
            raise ValueError("Inconsistency between supplied object ({}) "
                             "and storage class type ({})".format(type(inMemoryDataset), storageClass.pytype))

        print("Store ID {}".format(ref.id))
        self.datasets[ref.id] = inMemoryDataset

        # We have to register this content with registry.
        # Currently this assumes we have a file so we need to use stub entries
        # TODO: Add to ephemeral part of registry
        checksum = str(id(inMemoryDataset))
        size = get_object_size(inMemoryDataset)
        info = StorageInfo(self.name, checksum, size)
        print("Store ID {}".format(ref.id))
        print("Registr: {}".format(self.registry))
        self.registry.addStorageInfo(ref, info)
        print("Store ID {}".format(ref.id))
        print("Put dataset: {} (Size: {})".format(ref.id, size))
        print(self.datasets)

        # Store time we received this content, to allow us to optionally
        # expire it.
        itemInfo = StoredItemInfo(time.time(), ref.datasetType.storageClass)
        self.addStoredItemInfo(ref, itemInfo)

        # Register all components with same information
        for compRef in ref.components.values():
            self.registry.addStorageInfo(compRef, info)
            self.addStoredItemInfo(compRef, itemInfo)

        if self._transaction is not None:
            self._transaction.registerUndo('put', self.remove, ref)

    def getUri(self, ref, predict=False):
        """URI to the Dataset.

        Always uses "mem://" URI prefix.

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
            `True`, the URI will be a prediction and will include a URI fragment
            "#predicted".
            If the datastore does not have entities that relate well
            to the concept of a URI the returned URI string will be
            descriptive. The returned URI is not guaranteed to be obtainable.

        Raises
        ------
        FileNotFoundError
            A URI has been requested for a dataset that does not exist and
            guessing is not allowed.

        """

        # if this has never been written then we have to guess
        if not self.exists(ref):
            if not predict:
                raise FileNotFoundError("Dataset {} not in this datastore".format(ref))
            name = "{}#predicted".format(ref.datasetType.name)
        else:
            name = "{}".format(id(self.datasets[ref.id]))

        return "mem://{}".format(name)

    def remove(self, ref):
        """Indicate to the Datastore that a Dataset can be removed.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.

        Raises
        ------
        FileNotFoundError
            Attempt to remove a dataset that does not exist.

        Notes
        -----
        Some Datastores may implement this method as a silent no-op to
        disable Dataset deletion through standard interfaces.
        """
        print("Removing dataset {}".format(ref.id))
        if ref.id not in self.datasets:
            raise FileNotFoundError("No such file dataset in memory: {}".format(ref))
        del self.datasets[ref.id]

        # Remove rows from registries
        self.removeStoredItemInfo(ref)
        self.registry.removeStorageInfo(self.name, ref)
        for compRef in ref.components.values():
            self.registry.removeStorageInfo(self.name, compRef)
            self.removeStoredItemInfo(compRef)

    def transfer(self, inputDatastore, ref):
        """Retrieve a Dataset from an input `Datastore`,
        and store the result in this `Datastore`.

        Parameters
        ----------
        inputDatastore : `Datastore`
            The external `Datastore` from which to retreive the Dataset.
        ref : `DatasetRef`
            Reference to the required Dataset in the input data store.

        """
        assert inputDatastore is not self  # unless we want it for renames?
        inMemoryDataset = inputDatastore.get(ref)
        return self.put(inMemoryDataset, ref)
