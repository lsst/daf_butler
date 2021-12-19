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

from __future__ import annotations

"""Generic datastore code useful for most datastores."""

__all__ = ("GenericBaseDatastore",)

import logging
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Iterable, List, Mapping, Optional, Sequence, Tuple

from lsst.daf.butler import DatasetTypeNotSupportedError, Datastore
from lsst.daf.butler.registry.interfaces import DatastoreRegistryBridge

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef, StorageClass, StoredDatastoreItemInfo

log = logging.getLogger(__name__)


class GenericBaseDatastore(Datastore):
    """Methods useful for most implementations of a `Datastore`.

    Should always be sub-classed since key abstract methods are missing.
    """

    @property
    @abstractmethod
    def bridge(self) -> DatastoreRegistryBridge:
        """Object that manages the interface between this `Datastore` and the
        `Registry` (`DatastoreRegistryBridge`).
        """
        raise NotImplementedError()

    @abstractmethod
    def addStoredItemInfo(self, refs: Iterable[DatasetRef], infos: Iterable[Any]) -> None:
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
    def getStoredItemsInfo(self, ref: DatasetRef) -> Sequence[Any]:
        """Retrieve information associated with files stored in this
        `Datastore` associated with this dataset ref.

        Parameters
        ----------
        ref : `DatasetRef`
            The dataset that is to be queried.

        Returns
        -------
        items : `list` [`StoredDatastoreItemInfo`]
            Stored information about the files and associated formatters
            associated with this dataset. Only one file will be returned
            if the dataset has not been disassembled. Can return an empty
            list if no matching datasets can be found.
        """
        raise NotImplementedError()

    @abstractmethod
    def removeStoredItemInfo(self, ref: DatasetRef) -> None:
        """Remove information about the file associated with this dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            The dataset that has been removed.
        """
        raise NotImplementedError()

    def _register_datasets(self, refsAndInfos: Iterable[Tuple[DatasetRef, StoredDatastoreItemInfo]]) -> None:
        """Update registry to indicate that one or more datasets have been
        stored.

        Parameters
        ----------
        refsAndInfos : sequence `tuple` [`DatasetRef`,
                                         `StoredDatastoreItemInfo`]
            Datasets to register and the internal datastore metadata associated
            with them.
        """
        expandedRefs: List[DatasetRef] = []
        expandedItemInfos = []

        for ref, itemInfo in refsAndInfos:
            expandedRefs.append(ref)
            expandedItemInfos.append(itemInfo)

        # Dataset location only cares about registry ID so if we have
        # disassembled in datastore we have to deduplicate. Since they
        # will have different datasetTypes we can't use a set
        registryRefs = {r.id: r for r in expandedRefs}
        self.bridge.insert(registryRefs.values())
        self.addStoredItemInfo(expandedRefs, expandedItemInfos)

    def _post_process_get(
        self,
        inMemoryDataset: Any,
        readStorageClass: StorageClass,
        assemblerParams: Optional[Mapping[str, Any]] = None,
        isComponent: bool = False,
    ) -> Any:
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
        assemblerParams : `dict`, optional
            Parameters to pass to the assembler.  Can be `None`.
        isComponent : `bool`, optional
            If this is a component, allow the inMemoryDataset to be `None`.
        """
        # Process any left over parameters
        if assemblerParams:
            inMemoryDataset = readStorageClass.delegate().handleParameters(inMemoryDataset, assemblerParams)

        # Validate the returned data type matches the expected data type
        pytype = readStorageClass.pytype

        allowedTypes = []
        if pytype:
            allowedTypes.append(pytype)

        # Special case components to allow them to be None
        if isComponent:
            allowedTypes.append(type(None))

        if allowedTypes and not isinstance(inMemoryDataset, tuple(allowedTypes)):
            inMemoryDataset = readStorageClass.coerce_type(inMemoryDataset)

        return inMemoryDataset

    def _validate_put_parameters(self, inMemoryDataset: Any, ref: DatasetRef) -> None:
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
            raise TypeError(
                "Inconsistency between supplied object ({}) "
                "and storage class type ({})".format(type(inMemoryDataset), storageClass.pytype)
            )

        # Confirm that we can accept this dataset
        if not self.constraints.isAcceptable(ref):
            # Raise rather than use boolean return value.
            raise DatasetTypeNotSupportedError(
                f"Dataset {ref} has been rejected by this datastore via configuration."
            )

        return

    def remove(self, ref: DatasetRef) -> None:
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

    def transfer(self, inputDatastore: Datastore, ref: DatasetRef) -> None:
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
