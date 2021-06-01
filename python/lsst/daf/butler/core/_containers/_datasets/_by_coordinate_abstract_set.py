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

__all__ = ("HomogeneousDatasetByCoordinateAbstractSet",)

from typing import Type

from ...datasets import DatasetRef
from ...dimensions import DataCoordinate
from .._data_coordinate import DataCoordinateAbstractSet
from ._generic_sets import HomogeneousDatasetAbstractSet


class HomogeneousDatasetByCoordinateAbstractSet(HomogeneousDatasetAbstractSet[DataCoordinate]):
    """Abstract base class for custom containers of `DatasetRef` that have
    a particular `DatasetType` and unique data IDs.

    Notes
    -----
    This class is only informally set-like; it does not inherit from or fully
    implement the `collections.abc.Set` interface.  See `DatasetAbstractSet`
    for details.

    There is no corresponding mutable set interface at this level because it
    wouldn't add anything; concrete mutable sets should just inherit from both
    `HomogeneousDatasetByCoordinateAbstractSet` and
    ``HomogeneousDatasetMutableSet[DataCoordinate]``.
    """

    __slots__ = ()

    def unique_by_coordinate(self) -> HomogeneousDatasetByCoordinateAbstractSet:
        # Docstring inherited.
        return self

    @property
    def coordinates(self) -> DataCoordinateAbstractSet:
        """The data IDs of the datasets (`DataCoordinateAbstractSet`).

        This is guaranteed to have the same iteration order and number of
        elements as ``self``.
        """
        raise NotImplementedError("TODO: need to track hasFull/hasRecords state.")

    @classmethod
    def _key_type(cls) -> Type[DataCoordinate]:
        # Docstring inherited.
        return DataCoordinate

    @classmethod
    def _get_key(cls, ref: DatasetRef) -> DataCoordinate:
        # Docstring inherited.
        return ref.dataId
