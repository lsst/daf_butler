__all__ = ("FileDatastoreGetPayload", "get_dataset_as_python_object")

from typing import Any, Literal

import pydantic
from pydantic import AnyHttpUrl

from lsst.daf.butler import DatasetRef, Location
from lsst.daf.butler.datastore.cache_manager import (
    AbstractDatastoreCacheManager,
    DatastoreDisabledCacheManager,
)
from lsst.daf.butler.datastore.stored_file_info import SerializedStoredFileInfo, StoredFileInfo
from lsst.daf.butler.datastores.file_datastore.get import (
    DatasetLocationInformation,
    Mapping,
    generate_datastore_get_information,
    get_dataset_as_python_object_from_get_info,
)


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
    cache_manager: AbstractDatastoreCacheManager | None = None,
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
    cache_manager : `AbstractDatastoreCacheManager` or `None`, optional
        Cache manager to use. If `None` the cache is disabled.

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
    if cache_manager is None:
        cache_manager = DatastoreDisabledCacheManager()
    return get_dataset_as_python_object_from_get_info(
        datastore_file_info, ref=ref, parameters=parameters, cache_manager=cache_manager
    )
