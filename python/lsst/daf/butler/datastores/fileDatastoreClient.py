__all__ = ("get_dataset_as_python_object", "FileDatastoreGetPayload")

from typing import Any, Literal

from lsst.daf.butler import DatasetRef, DimensionUniverse, Location, SerializedDatasetRef, StorageClass
from lsst.daf.butler._compat import _BaseModelCompat
from lsst.daf.butler.datastore.cache_manager import DatastoreDisabledCacheManager
from lsst.daf.butler.datastore.stored_file_info import SerializedStoredFileInfo, StoredFileInfo
from lsst.daf.butler.datastores.file_datastore.get import (
    DatasetLocationInformation,
    Mapping,
    generate_datastore_get_information,
    get_dataset_as_python_object_from_get_info,
)


class FileDatastoreGetPayloadFileInfo(_BaseModelCompat):
    # TODO DM-41879: Allowing arbitrary URLs here is a severe security issue,
    # since it allows the server to trick the client into fetching data from
    # any file on its local filesystem or from remote storage using credentials
    # laying around in the environment.  This should be restricted to only
    # HTTP, but we don't yet have a means of mocking out HTTP gets in tests.
    url: str
    metadata: SerializedStoredFileInfo


class FileDatastoreGetPayload(_BaseModelCompat):
    datastore_type: Literal["file"]
    file_info: list[FileDatastoreGetPayloadFileInfo]
    dataset_ref: SerializedDatasetRef


def get_dataset_as_python_object(
    model: FileDatastoreGetPayload,
    *,
    universe: DimensionUniverse,
    parameters: Mapping[str, Any] | None,
    storageClass: StorageClass | str | None,
) -> Any:
    fileLocations: list[DatasetLocationInformation] = [
        (Location(None, file_info.url), StoredFileInfo.from_simple(file_info.metadata))
        for file_info in model.file_info
    ]

    ref = DatasetRef.from_simple(model.dataset_ref, universe=universe)
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
