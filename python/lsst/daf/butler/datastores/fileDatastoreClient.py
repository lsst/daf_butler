__all__ = ("get_dataset_as_python_object", "FileDatastoreGetPayload")

from typing import Any, Literal

import pydantic
from lsst.daf.butler import DatasetRef, Location
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


def get_dataset_as_python_object(
    ref: DatasetRef,
    payload: FileDatastoreGetPayload,
    *,
    parameters: Mapping[str, Any] | None,
) -> Any:
    """Retrieve an artifact from storage and return it as a Python object.

    Parameters
    ----------
    ref : `DatasetRef`
        Metadata about this artifact.
    payload : `FileDatastoreGetPayload`
        Pre-processed information about each file associated with this
        artifact.
    parameters : `Mapping`[`str`, `typing.Any`]
        `StorageClass` and `Formatter` parameters to be used when converting
        the artifact to a Python object.

    Returns
    -------
    python_object : `typing.Any`
        The retrieved artifact, converted to a Python object.
    """
    fileLocations: list[DatasetLocationInformation] = [
        (Location(None, str(file_info.url)), StoredFileInfo.from_simple(file_info.datastoreRecords))
        for file_info in payload.file_info
    ]

    datastore_file_info = generate_datastore_get_information(
        fileLocations,
        ref=ref,
        parameters=parameters,
    )
    return get_dataset_as_python_object_from_get_info(
        datastore_file_info, ref=ref, parameters=parameters, cache_manager=DatastoreDisabledCacheManager()
    )
