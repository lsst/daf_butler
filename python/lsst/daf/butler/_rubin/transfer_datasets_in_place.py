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

__all__ = ("transfer_datasets_in_place",)

from collections.abc import Iterable
from logging import DEBUG

from .._butler import Butler
from .._dataset_ref import DatasetRef
from ..direct_butler import DirectButler
from ..progress import Progress


def transfer_datasets_in_place(
    source_butler: Butler, target_butler: Butler, refs: Iterable[DatasetRef]
) -> list[DatasetRef]:
    """Transfer registry and datastore records from one Butler repository to
    another, with the target datastore sharing artifact files at their original
    location in the source datastore.

    Parameters
    ----------
    source_butler
        DirectButler instance from which datasets will be copied.
    target_butler
        DirectButler instance to which datasets will be copied.
    refs
        List of datasets to be copied.

    Returns
    -------
    transferred_datasets
        Datasets which were actually transferred. (Excludes those that were
        already present in the target repository).

    Notes
    -----
    This assumes that ``target_butler`` has a datastore matching the name and
    type of each datastore configured in ``source_butler``.  ``target_butler``
    should only have read-only access to the datastore root shared with
    ``source_butler`` -- otherwise, it could accidentally delete files owned by
    ``source_butler``.

    Any run collections or dataset types not present in the target datastore
    will be copied from the source datastore.

    This operation is idempotent -- any of the given ``refs`` that already
    exist in the target repository will be skipped.
    """
    assert isinstance(source_butler, DirectButler)
    assert isinstance(target_butler, DirectButler)
    if not set(target_butler._datastore.names).issuperset(source_butler._datastore.names):
        # If this constraint was not satisfied, then
        # Datastore.import_records() would silently drop data for any missing
        # datastores in the target.
        raise AssertionError(
            "Datastore configuration differs between transfer_datasets_in_place() repositories."
            f" Source: {source_butler._datastore.names} Target: {target_butler._datastore.names}"
        )

    target_existence = target_butler._exists_many(refs, full_check=False)
    new_datasets = [ref for ref, exists in target_existence.items() if not exists]
    import_info = target_butler._prepare_for_import_refs(
        source_butler, new_datasets, register_dataset_types=True, transfer_dimensions=False
    )
    datastore_export = source_butler._datastore.export_records(new_datasets)
    with target_butler.transaction():
        progress = Progress(__name__, level=DEBUG)
        target_butler._import_grouped_refs(
            import_info.grouped_refs, source_butler, progress, expand_refs=False
        )
        target_butler._datastore.import_records(datastore_export)

    return new_datasets
