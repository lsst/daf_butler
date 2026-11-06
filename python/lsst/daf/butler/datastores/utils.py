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

__all__ = ["report_duplicate_ids"]

from collections.abc import Iterable

from lsst.daf.butler import DatasetId
from lsst.daf.butler.datastore import DuplicateIdError


def report_duplicate_ids(dataset_ids: Iterable[DatasetId]) -> None:
    """Raise exception if input list is non-empty.

    Parameters
    ----------
    dataset_ids : `~collections.abc.Iterable` [`DatasetId`]
        Dataset IDs.

    Raises
    ------
    DuplicateIdError
        Raised if input list is non empty.
    """
    known_ids = list(dataset_ids)
    if len(known_ids) == 1:
        raise DuplicateIdError(f"Attempt to override an existing dataset: {known_ids[0]}")
    elif len(known_ids) > 1:
        known_ids_str = " ".join(str(known_id) for known_id in known_ids[:10])
        if len(known_ids) <= 10:
            raise DuplicateIdError(f"Attempt to override existing datasets: {known_ids_str}")
        else:
            raise DuplicateIdError(
                f"Attempt to override existing datasets ({len(known_ids)} total): {known_ids_str} ..."
            )
