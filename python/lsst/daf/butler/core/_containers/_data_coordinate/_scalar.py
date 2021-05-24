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

__all__ = ()

from typing import Any, Iterator

from ...dimensions import DataCoordinate, DimensionGraph
from ._iterable import DataCoordinateIterable


class _ScalarDataCoordinateIterable(DataCoordinateIterable):
    """An iterable for a single `DataCoordinate`.

    A `DataCoordinateIterable` implementation that adapts a single
    `DataCoordinate` instance.

    This class should only be used directly by other code in the module in
    which it is defined; all other code should interact with it only through
    the `DataCoordinateIterable` interface.

    Parameters
    ----------
    dataId : `DataCoordinate`
        The data ID to adapt.
    """

    def __init__(self, dataId: DataCoordinate):
        self._dataId = dataId

    __slots__ = ("_dataId",)

    def __iter__(self) -> Iterator[DataCoordinate]:
        yield self._dataId

    def __len__(self) -> int:
        return 1

    def __contains__(self, key: Any) -> bool:
        if isinstance(key, DataCoordinate):
            return key == self._dataId
        else:
            return False

    @property
    def graph(self) -> DimensionGraph:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.graph

    @property
    def has_full(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.has_full

    @property
    def has_records(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.has_records

    def subset(self, graph: DimensionGraph) -> _ScalarDataCoordinateIterable:
        # Docstring inherited from DataCoordinateIterable.
        return _ScalarDataCoordinateIterable(self._dataId.subset(graph))
