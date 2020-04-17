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
            The dataset that is to be queried.

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
            The dataset that has been removed.
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
            # Need the main dataset and the components
            expandedRefs.extend(ref.flatten([ref]))

            # Need one for the main ref and then one for each component
            expandedItemInfos.extend([itemInfo] * (len(ref.components) + 1))

        self.registry.insertDatasetLocations(self.name, expandedRefs)
        self.addStoredItemInfo(expandedRefs, expandedItemInfos)

    def _move_to_trash_in_registry(self, ref):
        """Tell registry that this dataset and associated components
        are to be trashed.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to mark for removal from registry.

        Notes
        -----
        Dataset is not removed from internal stored item info table.
        """

        # Note that a ref can point to component dataset refs that
        # have been deleted already from registry but are still in
        # the python object. moveDatasetLocationToTrash will deal with that.
        self.registry.moveDatasetLocationToTrash(self.name, list(ref.flatten([ref])))

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
            The dataset to store.
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

    def remove(self, ref):
        """Indicate to the Datastore that a dataset can be removed.

        .. warning::

            This method deletes the artifact associated with this
            dataset and can not be reversed.

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
        This method is used for immediate removal of a dataset and is
        generally reserved for internal testing of datastore APIs.
        It is implemented by calling `trash()` and then immediately calling
        `emptyTrash()`.  This call is meant to be immediate so errors
        encountered during removal are not ignored.
        """
        self.trash(ref, ignore_errors=False)
        self.emptyTrash(ignore_errors=False)

    def transfer(self, inputDatastore, ref):
        """Retrieve a dataset from an input `Datastore`,
        and store the result in this `Datastore`.

        Parameters
        ----------
        inputDatastore : `Datastore`
            The external `Datastore` from which to retreive the Dataset.
        ref : `DatasetRef`
            Reference to the required dataset in the input data store.

        """
        assert inputDatastore is not self  # unless we want it for renames?
        inMemoryDataset = inputDatastore.get(ref)
        return self.put(inMemoryDataset, ref)
