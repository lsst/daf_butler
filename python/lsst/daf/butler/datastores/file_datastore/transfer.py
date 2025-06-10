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

from lsst.resources import ResourcePath
from lsst.utils.logging import getLogger

from ..._dataset_ref import DatasetRef
from ...datastore import FileTransferMap, FileTransferSource

log = getLogger(__name__)


def retrieve_file_transfer_records(
    source_datastore: FileTransferSource,
    refs: Iterable[DatasetRef],
    artifact_existence: dict[ResourcePath, bool],
) -> FileTransferMap:
    """Look up the datastore records corresponding to the given datasets.

    Parameters
    ----------
    source_datastore : `FileTransferSource`
        Object used to look up records.
    refs : `~collections.abc.Iterable` [ `DatasetRef` ]
        List of datasets to retrieve records for.
    artifact_existence : `dict` [`lsst.resources.ResourcePath`, `bool`]
        Cache mapping datastore artifact to existence. Updated by
        this method with details of all artifacts tested.

    Return
    ------
    files : `FileTransferMap`
        A dictionary from `DatasetId` to a list of `FileTransferRecord`,
        containing information about the files that were found for these
        artifacts.  If files were not found for a given `DatasetRef`, there
        will be no entry for it in this dictionary.

    Notes
    -----
    This will first attempt to look up records using the database, and then
    fall back to searching the filesystem if the transfer source is configured
    to do so.
    """
    log.verbose("Looking up source datastore records in %s", source_datastore.name)
    refs_by_id = {ref.id: ref for ref in refs}
    source_records = source_datastore.get_file_info_for_transfer(refs_by_id.keys())

    log.debug("Number of datastore records found in source: %d", len(source_records))

    # If we couldn't find all of the datasets in the database, continue
    # searching.  Some datastores may have artifacts on disk that do not have
    # corresponding records in the database.
    missing_ids = refs_by_id.keys() - source_records.keys()
    if missing_ids:
        log.info(
            "Number of expected datasets missing from source datastore records: %d out of %d",
            len(missing_ids),
            len(refs_by_id),
        )
        missing_refs = {refs_by_id[id] for id in missing_ids}
        found_records = source_datastore.locate_missing_files_for_transfer(missing_refs, artifact_existence)
        source_records |= found_records

        still_missing = len(missing_refs) - len(found_records)
        if still_missing:
            for ref in missing_refs:
                if ref.id not in found_records:
                    log.warning("Asked to transfer dataset %s but no file artifacts exist for it.", ref)
            log.warning(
                "Encountered %d dataset%s where no file artifacts exist from the "
                "source datastore and will be skipped.",
                still_missing,
                "s" if still_missing != 1 else "",
            )

    return source_records
