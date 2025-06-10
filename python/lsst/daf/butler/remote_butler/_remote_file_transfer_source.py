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

from collections.abc import Iterable

from lsst.daf.butler._dataset_ref import DatasetId, DatasetRef
from lsst.resources import ResourcePath
from lsst.utils.iteration import chunk_iterable

from .._location import Location
from ..datastore import FileTransferMap, FileTransferRecord, FileTransferSource
from ..datastore.stored_file_info import StoredFileInfo
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
                output[id] = [_deserialize_file_transfer_record(r) for r in records]

        return output

    def locate_missing_files_for_transfer(
        self, refs: Iterable[DatasetRef], artifact_existence: dict[ResourcePath, bool]
    ) -> FileTransferMap:
        # The server does not provide an alternate way to look up files that
        # could not be found using the file transfer endpoint.
        return {}


def _deserialize_file_transfer_record(record: FileTransferRecordModel) -> FileTransferRecord:
    return FileTransferRecord(
        location=Location(None, ResourcePath(str(record.url))),
        file_info=StoredFileInfo.from_simple(record.file_info),
    )
