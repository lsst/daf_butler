from typing import Any

from pydantic import AnyHttpUrl

from lsst.resources import ResourcePath
from lsst.resources.http import HttpResourcePath

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
from .authentication.interface import RemoteButlerAuthenticationProvider
from .server_models import FileAuthenticationMode, FileInfoPayload, FileInfoRecord


def get_dataset_as_python_object(
    registry_ref: DatasetRef,
    read_ref: DatasetRef,
    payload: FileInfoPayload,
    *,
    auth: RemoteButlerAuthenticationProvider,
    parameters: Mapping[str, Any] | None,
    cache_manager: AbstractDatastoreCacheManager | None = None,
) -> Any:
    """Retrieve an artifact from storage and return it as a Python object.

    Parameters
    ----------
    registry_ref : `DatasetRef`
        Metadata about this artifact using the dataset type definition from
        the repository, as returned by the server.  This is the ref given to
        the `Formatter`, so that it never sees a read-time override, matching
        the behavior of `DirectButler`.  It is never a component.
    read_ref : `DatasetRef`
        Metadata about this artifact using the `StorageClass` that the caller
        wants returned, and naming the component (if any) that they asked
        for.
    payload : `FileInfoPayload`
        Pre-processed information about each file associated with this
        artifact.
    auth : `RemoteButlerAuthenticationProvider`
        Provides authentication headers for HTTP service hosting the artifact
        files.
    parameters : `~collections.abc.Mapping` [`str`, `typing.Any`]
        `StorageClass` and `Formatter` parameters to be used when converting
        the artifact to a Python object.
    cache_manager : `AbstractDatastoreCacheManager` or `None`, optional
        Cache manager to use. If `None` the cache is disabled.

    Returns
    -------
    python_object : `typing.Any`
        The retrieved artifact, converted to a Python object.
    """
    fileLocations = [_to_dataset_location_information(file_info, auth) for file_info in payload.file_info]

    # The Formatter is constructed with the repository definition of the
    # dataset, while the component and read StorageClass requested by the
    # caller travel separately.  DirectButler does the same in
    # ``FileDatastore._prepare_for_direct_get``.
    datastore_file_info = generate_datastore_get_information(
        fileLocations,
        registry_ref=registry_ref,
        read_ref=read_ref,
        parameters=parameters,
    )
    if cache_manager is None:
        cache_manager = DatastoreDisabledCacheManager()
    return get_dataset_as_python_object_from_get_info(
        datastore_file_info, ref=read_ref, parameters=parameters, cache_manager=cache_manager
    )


def convert_http_url_to_resource_path(
    url: AnyHttpUrl, auth: RemoteButlerAuthenticationProvider, auth_mode: FileAuthenticationMode
) -> ResourcePath:
    """Convert an HTTP URL to a ResourcePath instance with authentication
    headers attached.

    Parameters
    ----------
    url : `AnyHttpUrl`
        URL to convert.
    auth : `RemoteButlerAuthenticationProvider`
        Provides authentication headers for the URL.
    auth_mode : `FileAuthenticationMode`
        Specifies which authentication headers to use.
    """
    if auth_mode == "none":
        headers = None
    elif auth_mode == "gafaelfawr":
        headers = auth.get_server_headers()
    elif auth_mode == "datastore":
        headers = auth.get_datastore_headers()
    else:
        raise ValueError(f"Unknown authentication type: '{auth_mode}'")

    return HttpResourcePath.create_http_resource_path(str(url), extra_headers=headers)


def _to_dataset_location_information(
    file_info: FileInfoRecord, auth: RemoteButlerAuthenticationProvider
) -> DatasetLocationInformation:
    path = convert_http_url_to_resource_path(file_info.url, auth, auth_mode=file_info.auth)
    return (Location(None, path), StoredFileInfo.from_simple(file_info.datastoreRecords))
