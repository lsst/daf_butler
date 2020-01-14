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

"""Generic datastore code useful for most datastores."""

__all__ = ("GenericBaseDatastore", )

import logging
from abc import abstractmethod

from lsst.daf.butler import Datastore, DatasetTypeNotSupportedError

log = logging.getLogger(__name__)


class GenericBaseDatastore(Datastore):
    """Methods useful for most implementations of a `Datastore`.

    Should always be sub-classed since key abstract methods are missing.
    """

    @abstractmethod
    def addStoredItemInfo(self, refs, infos):
        """Record internal storage information associated with one or more
        datasets.

        Parameters
        ----------
        refs : sequence of `DatasetRef`
            The datasets that have been stored.
        infos : sequence of `StoredDatastoreItemInfo`
            Metadata associated with the stored datasets.
        """
        raise NotImplementedError()

    @abstractmethod
    def getStoredItemInfo(self, ref):
        """Retrieve information associated with file stored in this
        `Datastore`.

        Parameters
        ----------
        ref : `DatasetRef`
            The Dataset that is to be queried.

        Returns
        -------
        info : `StoredFilenfo`
            Stored information about this file and its formatter.

        Raises
        ------
        KeyError
            Dataset with that id can not be found.
        """
        raise NotImplementedError()

    @abstractmethod
    def removeStoredItemInfo(self, ref):
        """Remove information about the file associated with this dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            The Dataset that has been removed.
        """
        raise NotImplementedError()

    def _register_datasets(self, refsAndInfos):
        """Update registry to indicate that one or more datasets have been
        stored.

        Parameters
        ----------
        refsAndInfos : sequence `tuple` [`DatasetRef`, `StoredDatasetItemInfo`]
            Datasets to register and the internal datastore metadata associated
            with them.
        """
        expandedRefs = []
        expandedItemInfos = []

        for ref, itemInfo in refsAndInfos:
            # Main dataset.
            expandedRefs.append(ref)
            expandedItemInfos.append(itemInfo)
            # Conmponents (using the same item info).
            expandedRefs.extend(ref.components.values())
            expandedItemInfos.extend([itemInfo] * len(ref.components))

        for ref in expandedRefs:
            # TODO: when a vectorized API for addDatasetLocation is available,
            # use it.
            self.registry.addDatasetLocation(ref, self.name)

        self.addStoredItemInfo(expandedRefs, expandedItemInfos)

    def _remove_from_registry(self, ref):
        """Remove rows from registry.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to remove from registry.
        """
        self.removeStoredItemInfo(ref)
        self.registry.removeDatasetLocation(self.name, ref)
        for compRef in ref.components.values():
            self.registry.removeDatasetLocation(self.name, compRef)
            self.removeStoredItemInfo(compRef)

    def _post_process_get(self, inMemoryDataset, readStorageClass, assemblerParams=None):
        """Given the Python object read from the datastore, manipulate
        it based on the supplied parameters and ensure the Python
        type is correct.

        Parameters
        ----------
        inMemoryDataset : `object`
            Dataset to check.
        readStorageClass: `StorageClass`
            The `StorageClass` used to obtain the assembler and to
            check the python type.
        assemblerParams : `dict`
            Parameters to pass to the assembler.  Can be `None`.
        """
        # Process any left over parameters
        if assemblerParams:
            inMemoryDataset = readStorageClass.assembler().handleParameters(inMemoryDataset, assemblerParams)

        # Validate the returned data type matches the expected data type
        pytype = readStorageClass.pytype
        if pytype and not isinstance(inMemoryDataset, pytype):
            raise TypeError("Got Python type {} from datastore but expected {}".format(type(inMemoryDataset),
                                                                                       pytype))

        return inMemoryDataset

    def _validate_put_parameters(self, inMemoryDataset, ref):
        """Validate the supplied arguments for put.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.
        ref : `DatasetRef`
            Reference to the associated Dataset.
        """
        storageClass = ref.datasetType.storageClass

        # Sanity check
        if not isinstance(inMemoryDataset, storageClass.pytype):
            raise TypeError("Inconsistency between supplied object ({}) "
                            "and storage class type ({})".format(type(inMemoryDataset),
                                                                 storageClass.pytype))

        # Confirm that we can accept this dataset
        if not self.constraints.isAcceptable(ref):
            # Raise rather than use boolean return value.
            raise DatasetTypeNotSupportedError(f"Dataset {ref} has been rejected by this datastore via"
                                               " configuration.")

        return

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
