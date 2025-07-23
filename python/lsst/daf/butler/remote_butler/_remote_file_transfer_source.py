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

from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from typing import Any, cast

from lsst.daf.butler._dataset_ref import DatasetId, DatasetRef
from lsst.resources import ResourcePath
from lsst.resources.http import HttpResourcePath
from lsst.utils.iteration import chunk_iterable

from .._location import Location
from ..datastore import FileTransferMap, FileTransferRecord, FileTransferSource
from ..datastore.stored_file_info import StoredFileInfo
from ._get import convert_http_url_to_resource_path
from ._http_connection import RemoteButlerHttpConnection, parse_model
from .server_models import (
    FileTransferRecordModel,
    GetFileTransferInfoRequestModel,
    GetFileTransferInfoResponseModel,
)


class RemoteFileTransferSource(FileTransferSource):
    """Implementation of `FileTransferSource` that retrieves information from
    Butler server.

    Parameters
    ----------
    connection : `RemoteButlerHttpConnection`
        HTTP connection used to access the Butler server.
    """

    def __init__(self, connection: RemoteButlerHttpConnection) -> None:
        self._connection = connection
        self.name = f"RemoteFileTransferSource{connection.server_url}"

    def get_file_info_for_transfer(self, dataset_ids: Iterable[DatasetId]) -> FileTransferMap:
        output: FileTransferMap = {}
        for chunk in chunk_iterable(dataset_ids, GetFileTransferInfoRequestModel.MAX_ITEMS_PER_REQUEST):
            request = GetFileTransferInfoRequestModel(dataset_ids=chunk)
            response = self._connection.post("file_transfer", request)
            model = parse_model(response, GetFileTransferInfoResponseModel)
            for id, records in model.files.items():
                output[id] = [self._deserialize_file_transfer_record(r) for r in records]

        return output

    def locate_missing_files_for_transfer(
        self, refs: Iterable[DatasetRef], artifact_existence: dict[ResourcePath, bool]
    ) -> FileTransferMap:
        # The server does not provide an alternate way to look up files that
        # could not be found using the file transfer endpoint.
        return {}

    def _deserialize_file_transfer_record(self, record: FileTransferRecordModel) -> FileTransferRecord:
        resource_path = convert_http_url_to_resource_path(record.url, self._connection.auth, record.auth)
        resource_path = _tweak_uri_for_unit_test(resource_path)

        return FileTransferRecord(
            location=Location(None, resource_path),
            file_info=StoredFileInfo.from_simple(record.file_info),
        )


def _tweak_uri_for_unit_test(path: ResourcePath) -> ResourcePath:
    # Provide a place for unit tests to hook in and modify URLs, since there is
    # no actual HTTP server reachable via a domain name during testing.
    return path


@contextmanager
def mock_file_transfer_uris_for_unit_test(
    callback: Callable[[HttpResourcePath], HttpResourcePath],
) -> Iterator[None]:
    """Hooks into the RemoteButler file transfer logic to modify URLs for unit
    testing.  The given callback will be used to transform file download URLs
    before attempting to access them.

    Parameters
    ----------
    callback : `~collections.abc.Callable`
        A function that takes an `HttpResourcePath` as its only parameter and
        returns a modified `HttpResourcePath`.
    """
    global _tweak_uri_for_unit_test
    orig = _tweak_uri_for_unit_test
    _tweak_uri_for_unit_test = cast(Any, callback)
    try:
        yield
    finally:
        _tweak_uri_for_unit_test = orig
