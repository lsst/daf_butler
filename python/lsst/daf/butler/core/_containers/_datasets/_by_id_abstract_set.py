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

__all__ = ("HomogeneousDatasetByIdAbstractSet",)

from typing import Any

from ...datasets import DatasetId, DatasetRef
from ._generic_sets import HomogeneousDatasetAbstractSet


class HomogeneousDatasetByIdAbstractSet(HomogeneousDatasetAbstractSet[DatasetId]):
    """Abstract base class for custom containers of `DatasetRef` that have
    a particular `DatasetType` and unique dataset IDs.

    Notes
    -----
    This class is only informally set-like; it does not inherit from or fully
    implement the `collections.abc.Set` interface.  See `DatasetAbstractSet`
    for details.

    There is no corresponding mutable set interface at this level because it
    wouldn't add anything; concrete mutable sets should just inherit from both
    `HomogeneousDatasetByIdAbstractSet` and
    ``HomogeneousDatasetMutableSet[DatasetId]``.
    """

    __slots__ = ()

    @property
    def all_resolved(self) -> bool:
        # Docstring inherited.
        return True

    @property
    def all_unresolved(self) -> bool:
        # Docstring inherited.
        return False

    def unique_by_id(self) -> HomogeneousDatasetByIdAbstractSet:
        # Docstring inherited.
        return self

    @classmethod
    def _key_type(cls) -> Any:
        # Docstring inherited.
        return DatasetId

    @classmethod
    def _get_key(cls, ref: DatasetRef) -> DatasetId:
        # Docstring inherited.
        return ref.getCheckedId()
