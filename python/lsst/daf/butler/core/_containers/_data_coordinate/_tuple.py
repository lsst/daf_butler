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

__all__ = ("DataCoordinateTuple",)

from typing import Iterable, Optional, Sequence

from ...dimensions import DataCoordinate, DimensionGraph
from ._sequence import DataCoordinateSequence
from ._iterable import DataCoordinateCommonState


class DataCoordinateTuple(DataCoordinateSequence):
    """A `DataCoordinateSequence` implementation backed by a tuple.

    Parameters
    ----------
    dataIds : `collections.abc.Iterable` [ `DataCoordinate` ]
        An iterable of `DataCoordinate` instances, with dimensions equal to
        ``graph``.
    graph : `DimensionGraph`
        Dimensions identified by all data IDs in the set.
    check: `bool`, optional
        If `True` (default) check that all data IDs are consistent with the
        given ``graph``.  If `False`, no checking will occur.
    hasFull : `bool`, optional
        If `True` or `False`, caller guarantees that this is the appropriate
        return value for the `hasFull` method.  If `None` (default), it is
        computed from the data IDs themselves on first use and then cached.
    hasRecords : `bool`, optional
        If `True` or `False`, caller guarantees that this is the appropriate
        return value for the `hasRecords` method.  If `None` (default), it is
        computed from the data IDs themselves on first use and then cached.
    """

    def __init__(
        self,
        dataIds: Iterable[DataCoordinate],
        graph: DimensionGraph,
        *,
        check: bool = True,
        hasFull: Optional[bool] = None,
        hasRecords: Optional[bool] = None,
    ):
        self._native = tuple(dataIds)
        self._common = DataCoordinateCommonState(graph, hasFull=hasFull, hasRecords=hasRecords)
        if check:
            self._common.check(self._native)

    __slots__ = ("_native", "_common")

    @property
    def _common_state(self) -> DataCoordinateCommonState:
        # Docstring inherited.
        return self._common

    @classmethod
    def _wrap(
        cls, native: Sequence[DataCoordinate], common: DataCoordinateCommonState
    ) -> DataCoordinateSequence:
        # Docstring inherited.
        return cls(native, check=False, **common.to_dict())

    def _unwrap(self) -> Sequence[DataCoordinate]:
        # Docstring inherited.
        return self._native

    def hasFull(self) -> bool:
        # Docstring inherited.
        return self._common_state.computeHasFull(self._unwrap(), cache=True)

    def hasRecords(self) -> bool:
        # Docstring inherited.
        return self._common_state.computeHasRecords(self._unwrap(), cache=True)

    def subset(self, graph: DimensionGraph) -> DataCoordinateSequence:
        # Docstring inherited
        if graph == self.graph:
            return self
        return super().subset(graph)

    def __str__(self) -> str:
        return str(self._native)

    def __repr__(self) -> str:
        return f"DataCoordinateTuple({self._native}, {self.graph!r})"
