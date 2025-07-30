from ...._butler import Butler
from ...._dataset_ref import DatasetRef
from ....datastore.stored_file_info import make_datastore_path_relative
from ....datastores.file_datastore.get import DatasetLocationInformation
from ...server_models import FileInfoPayload, FileInfoRecord
from .._config import ButlerServerConfig


def get_file_info_payload(
    butler: Butler, ref: DatasetRef, config: ButlerServerConfig
) -> FileInfoPayload | None:
    """Return file information associated with ``ref``.

    Parameters
    ----------
    butler : `Butler`
        Butler used to look up file information.
    ref : `DatasetRef`
        Dataset for which we will look up file information.
    config : `ButlerServerConfig`
        Configuration for the Butler server.

    Returns
    -------
    info : `FileInfoPayload`
        The file information.
    """
    # 1 hour.  Chosen somewhat arbitrarily -- this is long enough that the
    # client should have time to download a large file with retries if
    # needed, but short enough that it will become obvious quickly that
    # these URLs expire.
    # From a strictly technical standpoint there is no reason this
    # shouldn't be a day or more, but there seems to be a political issue
    # where people think there is a risk of end users posting presigned
    # URLs for people without access rights to download.
    url_expiration_time_seconds = 1 * 60 * 60

    locations = butler._datastore.prepare_get_for_external_client(ref)
    if locations is None:
        return None

    return FileInfoPayload(
        datastore_type="file",
        file_info=[_to_file_info_payload(info, url_expiration_time_seconds, config) for info in locations],
    )


def _to_file_info_payload(
    info: DatasetLocationInformation, url_expiration_time_seconds: int, config: ButlerServerConfig
) -> FileInfoRecord:
    location, file_info = info

    datastoreRecords = file_info.to_simple()
    datastoreRecords.path = make_datastore_path_relative(datastoreRecords.path)

    if config.authentication == "rubin_science_platform":
        # At the RSP, we pre-sign download URLs so that the client does not
        # require authentication headers to access them.
        url = location.uri.generate_presigned_get_url(expiration_time_seconds=url_expiration_time_seconds)
        auth = "none"
    elif config.authentication == "cadc":
        # At the CADC, the HTTP service hosting the artifacts requires
        # authentication.  We return the unmodified URL and instruct the
        # client to provide authentication headers.
        url = str(location.uri)
        auth = "datastore"

    return FileInfoRecord(
        url=url,
        auth=auth,
        datastoreRecords=datastoreRecords,
    )
