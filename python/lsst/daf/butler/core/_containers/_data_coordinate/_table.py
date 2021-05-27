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

__all__ = ("DataCoordinateTable",)

from typing import Any, Iterable, Iterator, Optional, Sequence, overload

import astropy
import numpy as np

from ...dimensions import DataCoordinate, Dimension, DimensionGraph
from ._sequence import DataCoordinateSequence
from ._iterable import DataCoordinateCommonState, DataCoordinateIterable
from ._tuple import DataCoordinateTuple


def equal_range(array: np.ndarray, key: Any, window: Optional[slice] = None) -> Optional[slice]:
    if window is not None:
        array = array[window]
    begin = np.searchsorted(array, key, side="left")
    if begin == len(array) or array[begin] != key:
        return None
    length = np.searchsorted(array[begin:], key, side="right")
    if window is not None:
        begin += window.start
    return slice(begin, begin + length)


class DataCoordinateTable(DataCoordinateSequence):
    """A `DataCoordinateSequence` implementation backed by an astropy table.
    """

    def __init__(
        self,
        table: astropy.table.Table,
        graph: DimensionGraph,
    ):
        if table.columns.keys() == graph.dimensions.names:
            hasFull = True
        elif table.columns.keys() == graph.required.names:
            hasFull = False
        else:
            columns = [table[name] for name in graph.required.names]
            try:
                implied_columns = [table[name] for name in graph.implied.names]
                hasFull = True
                columns.extend(implied_columns)
            except KeyError:
                hasFull = False
            table = astropy.table.Table(columns)
        self._table = astropy.table.unique(table, keys=list(graph.required.names))
        for column in self._table:
            column.flags["WRITEABLE"] = False
        self._common = DataCoordinateCommonState(graph, hasFull=hasFull, hasRecords=False)

    __slots__ = ("_table", "_common")

    @staticmethod
    def make_dimension_column(dimension: Dimension, length: int) -> astropy.table.Column:
        if dimension.primaryKey.length is not None:
            dtype = np.dtype((dimension.primaryKey.getPythonType(), dimension.primaryKey.length))
        else:
            dtype = np.dtype(dimension.primaryKey.getPythonType())
        return astropy.table.Column(name=dimension.name, dtype=dtype, length=length)

    @classmethod
    def from_data_ids(
        cls,
        other: Iterable[DataCoordinate],
        *,
        graph: Optional[DimensionGraph] = None,
        length: Optional[int] = None,
        hasFull: Optional[bool] = None,
    ) -> DataCoordinateTable:
        if isinstance(other, DataCoordinateIterable):
            graph = other.graph
            hasFull = other.hasFull()
        else:
            if graph is None:
                raise TypeError("'graph' must be provided if iterable is not a DataCoordinateIterable.")
            if hasFull is None:
                other = DataCoordinateTuple(other, graph)
                hasFull = other.hasFull()
        if length is None:
            try:
                length = len(other)  # type: ignore
            except TypeError as err:
                raise TypeError("'length' must be provided if iterable is not sized.") from err
        columns = [
            cls.make_dimension_column(dimension, length=length)
            for dimension in graph.required
        ]
        if hasFull:
            columns.extend(
                cls.make_dimension_column(dimension, length=length)
                for dimension in graph.implied
            )
        table = astropy.table.Table(columns)
        for i, data_id in enumerate(other):
            for name, column in table.columns.items():
                column[i] = data_id[name]
        return cls(table, graph)

    def __iter__(self) -> Iterator[DataCoordinate]:
        ctor = DataCoordinate.fromFullValues if self.hasFull() else DataCoordinate.fromRequiredValues
        for row in self._table:
            yield ctor(self.graph, tuple(row))

    def __len__(self) -> int:
        return len(self._table)

    def __contains__(self, key: Any) -> bool:
        key = DataCoordinate.standardize(key, universe=self.universe)
        current, *rest = self.graph.required.names
        indices = equal_range(self._table[current], key[current])
        for current in rest:
            if indices is None:
                return False
            indices = equal_range(self._table[current], key[current], window=indices)
        return indices is not None

    @property
    def _common_state(self) -> DataCoordinateCommonState:
        # Docstring inherited.
        return self._common

    @classmethod
    def _wrap(
        cls, native: Sequence[DataCoordinate], common: DataCoordinateCommonState
    ) -> DataCoordinateSequence:
        # Docstring inherited.
        return cls.from_data_ids(native, graph=common.graph, hasFull=common.hasFull)

    def _unwrap(self) -> Sequence[DataCoordinate]:
        # Docstring inherited.
        return self

    def __str__(self) -> str:
        return str(self._table)

    def __repr__(self) -> str:
        return f"DataCoordinateTable({self._table!r}, {self.graph!r})"

    def subset(self, graph: DimensionGraph) -> DataCoordinateTable:
        """Return a subset table.

        This subset table contains data IDs that identify a subset of the
        dimensions that this one's do.

        Parameters
        ----------
        graph : `DimensionGraph`
            Dimensions to be identified by the data IDs in the returned
            sequence.  Must be a subset of ``self.graph``.

        Returns
        -------
        subset : `DataCoordinateTable`
            A `DataCoordinateTable` with ``subset.graph == graph``.  Will be
            ``self`` if ``graph == self.graph``.  Elements are equivalent to
            those that would be created by calling `DataCoordinate.subset` on
            all elements in ``self``, sorted and with deduplication.
        """
        if graph == self.graph:
            return self
        else:
            # Constructor automatically computes subset, because it drops
            # columns it doesn't need.
            return DataCoordinateTable(self._table, graph)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DataCoordinateTable):
            return self.graph == other.graph and (self._table == other._table).all()
        return False

    @overload
    def __getitem__(self, index: int) -> DataCoordinate:
        pass

    @overload
    def __getitem__(self, index: slice) -> DataCoordinateTable:  # noqa: F811
        pass

    def __getitem__(self, index: Any) -> Any:  # noqa: F811
        r = self._table[index]
        if isinstance(index, slice):
            return DataCoordinateTable(r, self.graph)
        ctor = DataCoordinate.fromFullValues if self.hasFull() else DataCoordinate.fromRequiredValues
        return ctor(self.graph, r)

    @property
    def table(self) -> astropy.table.Table:
        """A lightweight copy of the internal astropy table.

        This shares column values with the internal table (with the writeable
        flag set to `False`), but the table itself is new.
        """
        return astropy.table.Table(self._table)
