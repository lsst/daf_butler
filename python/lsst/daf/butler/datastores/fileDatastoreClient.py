__all__ = ("get_dataset_as_python_object", "FileDatastoreGetPayload")

from typing import Any, Literal

import pydantic
from lsst.daf.butler import DatasetRef, DimensionUniverse, Location, SerializedDatasetRef, StorageClass
from lsst.daf.butler.datastore.cache_manager import DatastoreDisabledCacheManager
from lsst.daf.butler.datastore.stored_file_info import SerializedStoredFileInfo, StoredFileInfo
from lsst.daf.butler.datastores.file_datastore.get import (
    DatasetLocationInformation,
    Mapping,
    generate_datastore_get_information,
    get_dataset_as_python_object_from_get_info,
)
from pydantic import AnyHttpUrl


class FileDatastoreGetPayloadFileInfo(pydantic.BaseModel):
    """Information required to read a single file stored in `FileDatastore`."""

    # This is intentionally restricted to HTTP for security reasons.  Allowing
    # arbitrary URLs here would allow the server to trick the client into
    # fetching data from any file on its local filesystem or from remote
    # storage using credentials laying around in the environment.
    url: AnyHttpUrl
    """An HTTP URL that can be used to read the file."""

    datastoreRecords: SerializedStoredFileInfo
    """`FileDatastore` metadata records for this file."""


class FileDatastoreGetPayload(pydantic.BaseModel):
    """A serializable representation of the data needed for retrieving an
    artifact and converting it to a python object.
    """

    datastore_type: Literal["file"]

    file_info: list[FileDatastoreGetPayloadFileInfo]
    """List of retrieval information for each file associated with this
    artifact.
    """

    dataset_ref: SerializedDatasetRef
    """Registry information associated with this artifact."""


def get_dataset_as_python_object(
    payload: FileDatastoreGetPayload,
    *,
    universe: DimensionUniverse,
    parameters: Mapping[str, Any] | None,
    storageClass: StorageClass | str | None,
    component: str | None,
) -> Any:
    """Retrieve an artifact from storage and return it as a Python object.

    Parameters
    ----------
    payload : `FileDatastoreGetPayload`
        Pre-processed information about each file associated with this
        artifact.
    universe : `DimensionUniverse`
        The universe of dimensions associated with the `DatasetRef` contained
        in ``payload``.
    parameters : `Mapping`[`str`, `typing.Any`]
        `StorageClass` and `Formatter` parameters to be used when converting
        the artifact to a Python object.
    storageClass : `StorageClass` | `str` | `None`
        Overrides the `StorageClass` to be used when converting the artifact to
        a Python object.  If `None`, uses the `StorageClass` specified by
        ``payload``.
    component : `str` | `None`
        Selects which component of the artifact to retrieve.

    Returns
    -------
    python_object : `typing.Any`
        The retrieved artifact, converted to a Python object.
    """
    fileLocations: list[DatasetLocationInformation] = [
        (Location(None, str(file_info.url)), StoredFileInfo.from_simple(file_info.datastoreRecords))
        for file_info in payload.file_info
    ]

    ref = DatasetRef.from_simple(payload.dataset_ref, universe=universe)

    # If we have both a component override and a storage class override, the
    # component override has to be applied first.  DatasetRef cares because it
    # is checking compatibility of the storage class with its DatasetType.
    if component is not None:
        ref = ref.makeComponentRef(component)
    if storageClass is not None:
        ref = ref.overrideStorageClass(storageClass)

    datastore_file_info = generate_datastore_get_information(
        fileLocations,
        ref=ref,
        parameters=parameters,
    )
    return get_dataset_as_python_object_from_get_info(
        datastore_file_info, ref=ref, parameters=parameters, cache_manager=DatastoreDisabledCacheManager()
    )
