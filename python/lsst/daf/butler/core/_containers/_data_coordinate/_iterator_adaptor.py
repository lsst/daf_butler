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

__all__ = ("DataCoordinateIteratorAdapter",)

from typing import Callable, Iterable, Iterator, Optional

from ...dimensions import DataCoordinate, DimensionGraph
from ._iterable import DataCoordinateIterable, DataCoordinateCommonState


class DataCoordinateIteratorAdapter(DataCoordinateIterable):
    """A concrete view iterable for homogeneous data IDs, backed by a function
    that returns an iterator.

    Parameters
    ----------
    iterator_callback : `Callable`
        Callable that takes no arguments and returns an iterator.  This must
        return a new iterator each time it is called, but the iterated elements
        may nevertheless change over the lifetime of the container (e.g. if it
        is a generated that filters some other container that can also chain).
    graph : `DimensionGraph`
        Dimensions identified by all data IDs in the iterable.  Caller
        guarantees that any data IDs added the iterable over the course of the
        view's lifetime have exactly these dimensions.
    hasFull : `bool`, optional
        If `True` or `False`, caller guarantees that this is the
        appropriate return value for the `hasFull` method *at all times*.
        If `None` (default), it will be computed from the data IDs themselves.
    hasRecords : `bool`, optional
        If `True` or `False`, caller guarantees that this is the
        appropriate return value for the `hasRecords` method *at
        all times*.  If `None` (default), it will be computed from the data
        IDs themselves.

    Notes
    -----
    While `DataCoordinateIteratorAdapter` never adds or removes data ID
    elements itself, it does not assume the ultimate backing for its callback
    remains constant throughout its lifetime.  This makes passing non-`None`
    values via ``hasFull`` and/or ``hasRecords`` a much stronger guarantee than
    the corresponding constructor arguments to e.g. `DataCoordinateFrozenSet`.
    """

    def __init__(
        self,
        iterator_callback: Callable[[], Iterator[DataCoordinate]],
        graph: DimensionGraph,
        *,
        hasFull: Optional[bool] = None,
        hasRecords: Optional[bool] = None,
    ):
        self._iterator_callback = iterator_callback
        self._common = DataCoordinateCommonState(graph, hasFull=hasFull, hasRecords=hasRecords)

    __slots__ = ("_iterator_callback", "_common")

    def __iter__(self) -> Iterator[DataCoordinate]:
        return self._iterator_callback()

    def _unwrap(self) -> Iterable[DataCoordinate]:
        # Docstring inherited.
        return self._iterator_callback()

    @property
    def _common_state(self) -> DataCoordinateCommonState:
        # Docstring inherited.
        return self._common

    def hasFull(self) -> bool:
        # Docstring inherited.
        return self._common.computeHasFull(self._unwrap())

    def hasRecords(self) -> bool:
        # Docstring inherited.
        return self._common.computeHasRecords(self._unwrap())

    def subset(self, graph: DimensionGraph) -> DataCoordinateIterable:
        # Docstring inherited.
        def gen() -> Iterator[DataCoordinate]:
            for data_id in self._iterator_callback():
                yield data_id.subset(graph)

        common = self._common.subset(graph)
        return DataCoordinateIteratorAdapter(gen, **common.to_dict())
