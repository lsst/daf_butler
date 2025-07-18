from typing import Any

from .._dataset_ref import DatasetRef
from .._location import Location
from ..datastore.cache_manager import AbstractDatastoreCacheManager, DatastoreDisabledCacheManager
from ..datastore.stored_file_info import StoredFileInfo
from ..datastores.file_datastore.get import (
    DatasetLocationInformation,
    Mapping,
    generate_datastore_get_information,
    get_dataset_as_python_object_from_get_info,
)
from .server_models import FileInfoPayload


def get_dataset_as_python_object(
    ref: DatasetRef,
    payload: FileInfoPayload,
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
