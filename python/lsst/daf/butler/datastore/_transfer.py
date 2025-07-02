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

from collections.abc import Iterable
from typing import NamedTuple, Protocol, TypeAlias

from lsst.resources import ResourcePath

from .._dataset_ref import DatasetId, DatasetRef
from .stored_file_info import Location, StoredFileInfo

__all__ = ("FileTransferMap", "FileTransferRecord", "FileTransferSource")


class FileTransferSource(Protocol):
    """Protocol for an object that can return information about files that need
    to be transferred to copy datasets from one Butler repository to another.
    """

    name: str
    """A human-readable, descriptive name for this file transfer source."""

    def get_file_info_for_transfer(self, dataset_ids: Iterable[DatasetId]) -> FileTransferMap:
        """Given a list of dataset IDs, return all file information associated
        with the datasets that can be determined without searching the
        filesystem.

        Parameters
        ----------
        dataset_ids : `~collections.abc.Iterable` [ `DatasetId` ]
            List of dataset IDs for which we will retrieve file information.

        Returns
        -------
        transfer_map : `FileTransferMap`
            Dictionary from `DatasetId` to a list of files found for that
            dataset.  If information about any  given dataset IDs could not
            be found, the missing IDs are omitted from the dictionary.
        """

    def locate_missing_files_for_transfer(
        self, refs: Iterable[DatasetRef], artifact_existence: dict[ResourcePath, bool]
    ) -> FileTransferMap:
        """Given a list of `DatasetRef`, search the filesystem to locate
        artifacts associated with the dataset.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets to be checked.
        artifact_existence : `dict` [`lsst.resources.ResourcePath`, `bool`]
            Optional mapping of datastore artifact to existence. Updated by
            this method with details of all artifacts tested.

        Returns
        -------
        transfer_map : `FileTransferMap`
            Dictionary from `DatasetId` to a list of files found for that
            dataset.  If information about any  given dataset IDs could not
            be found, the missing IDs are omitted from the dictionary.
        """


class FileTransferRecord(NamedTuple):
    """Information needed to transfer a file from one Butler repository to
    another.
    """

    location: Location
    file_info: StoredFileInfo


FileTransferMap: TypeAlias = dict[DatasetId, list[FileTransferRecord]]
"""A dictionary from `DatasetId` to a list of `FileTransferRecord`, containing
the datastore information about a set of files to be transferred.
"""
