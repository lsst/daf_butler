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

from typing import (
    Any,
    Collection,
    Dict,
    Iterator,
    Optional,
    TYPE_CHECKING,
)

from ...dimensions import DataCoordinate, DimensionGraph
from ._iterable import DataCoordinateIterable

if TYPE_CHECKING:
    from ._set import DataCoordinateSet
    from ._sequence import DataCoordinateSequence


class _DataCoordinateCollectionBase(DataCoordinateIterable):
    """A partial iterable implementation backed by native Python collection.

    A partial `DataCoordinateIterable` implementation that is backed by a
    native Python collection.

    This class is intended only to be used as an intermediate base class for
    `DataCoordinateIterables` that assume a more specific type of collection
    and can hence make more informed choices for how to implement some methods.

    Parameters
    ----------
    dataIds : `collections.abc.Collection` [ `DataCoordinate` ]
         A collection of `DataCoordinate` instances, with dimensions equal to
        ``graph``.
    graph : `DimensionGraph`
        Dimensions identified by all data IDs in the set.
    hasFull : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.hasFull` returns
        `True` for all given data IDs.  If `False`, no such guarantee is made,
        and `hasFull` will always return `False`.  If `None` (default),
        `hasFull` will be computed from the given data IDs, immediately if
        ``check`` is `True`, or on first use if ``check`` is `False`.
    hasRecords : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.hasRecords`
        returns `True` for all given data IDs.  If `False`, no such guarantee
        is made and `hasRecords` will always return `False`.  If `None`
        (default), `hasRecords` will be computed from the given data IDs,
        immediately if ``check`` is `True`, or on first use if ``check`` is
        `False`.
    check: `bool`, optional
        If `True` (default) check that all data IDs are consistent with the
        given ``graph`` and state flags at construction.  If `False`, no
        checking will occur.
    """

    def __init__(
        self,
        dataIds: Collection[DataCoordinate],
        graph: DimensionGraph,
        *,
        hasFull: Optional[bool] = None,
        hasRecords: Optional[bool] = None,
        check: bool = True,
    ):
        self._dataIds = dataIds
        self._graph = graph
        if check:
            for dataId in self._dataIds:
                if hasFull and not dataId.hasFull():
                    raise ValueError(f"{dataId} is not complete, but is required to be.")
                if hasRecords and not dataId.hasRecords():
                    raise ValueError(f"{dataId} has no records, but is required to.")
                if dataId.graph != self._graph:
                    raise ValueError(f"Bad dimensions {dataId.graph}; expected {self._graph}.")
            if hasFull is None:
                hasFull = all(dataId.hasFull() for dataId in self._dataIds)
            if hasRecords is None:
                hasRecords = all(dataId.hasRecords() for dataId in self._dataIds)
        self._hasFull = hasFull
        self._hasRecords = hasRecords

    __slots__ = ("_graph", "_dataIds", "_hasFull", "_hasRecords")

    @property
    def graph(self) -> DimensionGraph:
        # Docstring inherited from DataCoordinateIterable.
        return self._graph

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        if self._hasFull is None:
            self._hasFull = all(dataId.hasFull() for dataId in self._dataIds)
        return self._hasFull

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        if self._hasRecords is None:
            self._hasRecords = all(dataId.hasRecords() for dataId in self._dataIds)
        return self._hasRecords

    def toSet(self) -> DataCoordinateSet:
        # Docstring inherited from DataCoordinateIterable.
        # Override base class to pass in attributes instead of results of
        # method calls for _hasFull and _hasRecords - those can be None,
        # and hence defer checking if that's what the user originally wanted.
        from ._set import DataCoordinateSet

        return DataCoordinateSet(
            frozenset(self._dataIds),
            graph=self._graph,
            hasFull=self._hasFull,
            hasRecords=self._hasRecords,
            check=False,
        )

    def toSequence(self) -> DataCoordinateSequence:
        # Docstring inherited from DataCoordinateIterable.
        # Override base class to pass in attributes instead of results of
        # method calls for _hasFull and _hasRecords - those can be None,
        # and hence defer checking if that's what the user originally wanted.
        from ._sequence import DataCoordinateSequence

        return DataCoordinateSequence(
            tuple(self._dataIds),
            graph=self._graph,
            hasFull=self._hasFull,
            hasRecords=self._hasRecords,
            check=False,
        )

    def __iter__(self) -> Iterator[DataCoordinate]:
        return iter(self._dataIds)

    def __len__(self) -> int:
        return len(self._dataIds)

    def __contains__(self, key: Any) -> bool:
        key = DataCoordinate.standardize(key, universe=self.universe)
        return key in self._dataIds

    def _subsetKwargs(self, graph: DimensionGraph) -> Dict[str, Any]:
        """Return constructor kwargs useful for subclasses implementing subset.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions passed to `subset`.

        Returns
        -------
        kwargs : `dict`
            A dict with `hasFull`, `hasRecords`, and `check` keys, associated
            with the appropriate values for a `subset` operation with the given
            dimensions.
        """
        hasFull: Optional[bool]
        if graph.dimensions <= self.graph.required:
            hasFull = True
        else:
            hasFull = self._hasFull
        return dict(hasFull=hasFull, hasRecords=self._hasRecords, check=False)
