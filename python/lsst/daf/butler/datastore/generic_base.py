# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from __future__ import annotations

__all__ = ("GenericBaseDatastore", "post_process_get")

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from ..datastore._datastore import Datastore
from .stored_file_info import StoredDatastoreItemInfo

if TYPE_CHECKING:
    from .._dataset_ref import DatasetRef
    from .._storage_class import StorageClass

log = logging.getLogger(__name__)

_InfoType = TypeVar("_InfoType", bound=StoredDatastoreItemInfo)


class GenericBaseDatastore(Datastore, Generic[_InfoType]):
    """Methods useful for most implementations of a `Datastore`.

    Should always be sub-classed since key abstract methods are missing.
    """

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


def post_process_get(
    inMemoryDataset: object,
    readStorageClass: StorageClass,
    assemblerParams: Mapping[str, Any] | None = None,
    isComponent: bool = False,
) -> object:
    """Given the Python object read from the datastore, manipulate
    it based on the supplied parameters and ensure the Python
    type is correct.

    Parameters
    ----------
    inMemoryDataset : `object`
        Dataset to check.
    readStorageClass : `StorageClass`
        The `StorageClass` used to obtain the assembler and to
        check the python type.
    assemblerParams : `dict`, optional
        Parameters to pass to the assembler.  Can be `None`.
    isComponent : `bool`, optional
        If this is a component, allow the inMemoryDataset to be `None`.

    Returns
    -------
    dataset : `object`
        In-memory dataset, potentially converted to expected type.
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
