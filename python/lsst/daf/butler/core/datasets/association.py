# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

from dataclasses import dataclass
from typing import Optional

from .ref import DatasetRef
from ..timespan import Timespan


@dataclass
class DatasetAssociation:
    """A struct that represents the membership of a single dataset in a
    single collection.
    """

    __slots__ = ("ref", "collection", "timespan")

    ref: DatasetRef
    """Resolved reference to a dataset (`DatasetRef`).
    """

    collection: str
    """Name of a collection (`str`).
    """

    timespan: Optional[Timespan]
    """Validity range of the dataset if this is a `~CollectionType.CALIBRATION`
    collection (`Timespan` or `None`).
    """
