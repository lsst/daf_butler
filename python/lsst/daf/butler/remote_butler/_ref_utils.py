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

from __future__ import annotations

__all__ = (
    "apply_storage_class_override",
    "normalize_dataset_type_name",
    "simplify_dataId",
)

from pydantic import TypeAdapter

from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType, get_dataset_type_name
from .._storage_class import StorageClass
from ..dimensions import DataCoordinate, DataId, DataIdValue, SerializedDataId
from .server_models import DatasetTypeName

_SERIALIZED_DATA_ID_TYPE_ADAPTER = TypeAdapter(SerializedDataId)


def apply_storage_class_override(
    ref: DatasetRef,
    original_dataset_ref_or_type: DatasetRef | DatasetType | str,
    explicit_storage_class: StorageClass | str | None,
) -> DatasetRef:
    """Return a DatasetRef with its storage class overridden to match the
    StorageClass supplied by the user as input to one of the search functions.

    Parameters
    ----------
    ref : `DatasetRef`
        The ref to which we will apply the StorageClass override.
    original_dataset_ref_or_type : `DatasetRef` | `DatasetType` | `str`
        The ref or type that was input to the search, which may contain a
        storage class override.
    explicit_storage_class : `StorageClass` | `str` | `None`
        A storage class that the user explicitly requested as an override.
    """
    if explicit_storage_class is not None:
        return ref.overrideStorageClass(explicit_storage_class)

    # If the caller provided a DatasetRef or DatasetType, they may have
    # overridden the storage class on it, and we need to propagate that to the
    # output.
    dataset_type = _extract_dataset_type(original_dataset_ref_or_type)
    if dataset_type is not None:
        return ref.overrideStorageClass(dataset_type.storageClass)

    return ref


def normalize_dataset_type_name(datasetTypeOrName: DatasetType | str) -> DatasetTypeName:
    """Convert DatasetType parameters in the format used by Butler methods
    to a standardized string name for the REST API.

    Parameters
    ----------
    datasetTypeOrName : `DatasetType` | `str`
        A DatasetType, or the name of a DatasetType. This union is a common
        parameter in many `Butler` methods.
    """
    return DatasetTypeName(get_dataset_type_name(datasetTypeOrName))


def simplify_dataId(dataId: DataId | None, kwargs: dict[str, DataIdValue]) -> SerializedDataId:
    """Take a generic Data ID and convert it to a serializable form.

    Parameters
    ----------
    dataId : `dict`, `None`, `DataCoordinate`
        The data ID to serialize.
    kwargs : `dict`
        Additional entries to augment or replace the values in ``dataId``.

    Returns
    -------
    data_id : `SerializedDataId`
        A serializable form.
    """
    if dataId is None:
        dataId = {}
    elif isinstance(dataId, DataCoordinate):
        dataId = dataId.to_simple(minimal=True).dataId
    else:
        dataId = dict(dataId)

    return _SERIALIZED_DATA_ID_TYPE_ADAPTER.validate_python(dataId | kwargs)


def _extract_dataset_type(datasetRefOrType: DatasetRef | DatasetType | str) -> DatasetType | None:
    """Return the DatasetType associated with the argument, or None if the
    argument is not an object that contains a DatasetType object.

    Parameters
    ----------
    datasetRefOrType : `DatasetRef` | `DatasetType` | `str`
        A DatasetRef, DatasetType, or name of a DatasetType.  This union is a
        common parameter in many `Butler` methods.
    """
    if isinstance(datasetRefOrType, DatasetType):
        return datasetRefOrType
    elif isinstance(datasetRefOrType, DatasetRef):
        return datasetRefOrType.datasetType
    else:
        return None
