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

__all__ = ("DatasetAssociation",)

from dataclasses import dataclass
from typing import Any

from ._dataset_ref import DatasetRef
from ._timespan import Timespan


@dataclass(frozen=True, eq=True)
class DatasetAssociation:
    """Class representing the membership of a dataset in a single collection.

    One dataset is associated with one collection, possibly including
    a timespan.
    """

    __slots__ = ("ref", "collection", "timespan")

    ref: DatasetRef
    """Resolved reference to a dataset (`DatasetRef`).
    """

    collection: str
    """Name of a collection (`str`).
    """

    timespan: Timespan | None
    """Validity range of the dataset if this is a `~CollectionType.CALIBRATION`
    collection (`Timespan` or `None`).
    """

    def __lt__(self, other: Any) -> bool:
        # Allow sorting of associations
        if not isinstance(other, type(self)):
            return NotImplemented

        return (self.ref, self.collection, self.timespan) < (other.ref, other.collection, other.timespan)
