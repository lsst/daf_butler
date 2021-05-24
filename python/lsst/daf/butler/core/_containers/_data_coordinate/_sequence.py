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

__all__ = ("DataCoordinateSequence",)

from abc import abstractmethod
from typing import Any, Sequence, overload

from ...dimensions import DataCoordinate
from ._collection import DataCoordinateCollection


class DataCoordinateSequence(DataCoordinateCollection, Sequence[DataCoordinate]):
    """An abstract base class for homogeneous sequence-like containers of data
    IDs.
    """

    __slots__ = ()

    @abstractmethod
    def _unwrap(self) -> Sequence[DataCoordinate]:
        # Docstring inherited.
        raise NotImplementedError()

    def to_sequence(self) -> DataCoordinateSequence:
        # Docstring inherited from DataCoordinateIterable.
        return self

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateSequence):
            return self.graph == other.graph and self._unwrap() == other._unwrap()
        return False

    @overload
    def __getitem__(self, index: int) -> DataCoordinate:
        pass

    @overload
    def __getitem__(self, index: slice) -> DataCoordinateSequence:  # noqa: F811
        pass

    def __getitem__(self, index: Any) -> Any:  # noqa: F811
        r = self._unwrap()[index]
        if isinstance(index, slice):
            return self._wrap(r, self._common_state)
        return r
