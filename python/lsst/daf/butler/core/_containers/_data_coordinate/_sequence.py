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

from typing import (
    Any,
    Optional,
    overload,
    Sequence,
)

from ...dimensions import DataCoordinate, DimensionGraph
from ._collection import _DataCoordinateCollectionBase


class DataCoordinateSequence(_DataCoordinateCollectionBase, Sequence[DataCoordinate]):
    """Iterable supporting the full Sequence interface.

    A `DataCoordinateIterable` implementation that supports the full
    `collections.abc.Sequence` interface.

    Parameters
    ----------
    dataIds : `collections.abc.Sequence` [ `DataCoordinate` ]
        A sequence of `DataCoordinate` instances, with dimensions equal to
        ``graph``.
    graph : `DimensionGraph`
        Dimensions identified by all data IDs in the set.
    hasFull : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.hasFull` returns
        `True` for all given data IDs.  If `False`, no such guarantee is made,
        and `DataCoordinateSet.hasFull` will always return `False`.  If `None`
        (default), `DataCoordinateSet.hasFull` will be computed from the given
        data IDs, immediately if ``check`` is `True`, or on first use if
        ``check`` is `False`.
    hasRecords : `bool`, optional
        If `True`, the caller guarantees that `DataCoordinate.hasRecords`
        returns `True` for all given data IDs.  If `False`, no such guarantee
        is made and `DataCoordinateSet.hasRecords` will always return `False`.
        If `None` (default), `DataCoordinateSet.hasRecords` will be computed
        from the given data IDs, immediately if ``check`` is `True`, or on
        first use if ``check`` is `False`.
    check: `bool`, optional
        If `True` (default) check that all data IDs are consistent with the
        given ``graph`` and state flags at construction.  If `False`, no
        checking will occur.
    """

    def __init__(
        self,
        dataIds: Sequence[DataCoordinate],
        graph: DimensionGraph,
        *,
        hasFull: Optional[bool] = None,
        hasRecords: Optional[bool] = None,
        check: bool = True,
    ):
        super().__init__(tuple(dataIds), graph, hasFull=hasFull, hasRecords=hasRecords, check=check)

    _dataIds: Sequence[DataCoordinate]

    __slots__ = ()

    def __str__(self) -> str:
        return str(tuple(self._dataIds))

    def __repr__(self) -> str:
        return (
            f"DataCoordinateSequence({tuple(self._dataIds)}, {self._graph!r}, "
            f"hasFull={self._hasFull}, hasRecords={self._hasRecords})"
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateSequence):
            return self._graph == other._graph and self._dataIds == other._dataIds
        return False

    @overload
    def __getitem__(self, index: int) -> DataCoordinate:
        pass

    @overload  # noqa: F811 (FIXME: remove for py 3.8+)
    def __getitem__(self, index: slice) -> DataCoordinateSequence:  # noqa: F811
        pass

    def __getitem__(self, index: Any) -> Any:  # noqa: F811
        r = self._dataIds[index]
        if isinstance(index, slice):
            return DataCoordinateSequence(
                r, self._graph, hasFull=self._hasFull, hasRecords=self._hasRecords, check=False
            )
        return r

    def toSequence(self) -> DataCoordinateSequence:
        # Docstring inherited from DataCoordinateIterable.
        return self

    def subset(self, graph: DimensionGraph) -> DataCoordinateSequence:
        """Return a sequence whose data IDs identify a subset.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions to be identified by the data IDs in the returned
            iterable.  Must be a subset of ``self.graph``.

        Returns
        -------
        set : `DataCoordinateSequence`
            A `DataCoordinateSequence` with ``set.graph == graph``.
            Will be ``self`` if ``graph == self.graph``.  Elements are
            equivalent to those that would be created by calling
            `DataCoordinate.subset` on all elements in ``self``, in the same
            order and with no deduplication.
        """
        if graph == self.graph:
            return self
        return DataCoordinateSequence(
            tuple(dataId.subset(graph) for dataId in self._dataIds), graph, **self._subsetKwargs(graph)
        )
