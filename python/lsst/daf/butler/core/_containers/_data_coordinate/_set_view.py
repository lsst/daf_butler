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

__all__ = ("DataCoordinateSetView",)

from typing import AbstractSet, Optional

from ...dimensions import DataCoordinate, DimensionGraph
from ._abstract_set import DataCoordinateAbstractSet
from ._iterable import DataCoordinateCommonState
from ._frozen_set import DataCoordinateFrozenSet


class DataCoordinateSetView(DataCoordinateAbstractSet):
    """A `DataCoordinateAbstractSet` implementation that wraps a native
    set-like object (often dictionary keys).

    Parameters
    ----------
    native : `collections.abc.Set` [ `DataCoordinate` ]
        A native set of `DataCoordinate` instances, with dimensions equal to
        ``graph``.
    graph : `DimensionGraph`
        Dimensions identified by all data IDs in the set.  Caller guarantees
        that any data IDs added to ``native`` over the course of the view's
        lifetime have exactly these dimensions.
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
    While `DataCoordinateSetView` never adds or removes data ID elements
    itself, it does not assume its internal set remains constant throughout its
    lifetime.  This makes passing non-`None` values via ``hasFull`` and/or
    ``hasRecords`` a much stronger guarantee than the corresponding constructor
    arguments to e.g. `DataCoordinateFrozenSet`.
    """

    def __init__(
        self,
        native: AbstractSet[DataCoordinate],
        graph: DimensionGraph,
        *,
        hasFull: Optional[bool] = None,
        hasRecords: Optional[bool] = None,
    ):
        self._native = native
        self._common = DataCoordinateCommonState(graph, hasFull=hasFull, hasRecords=hasRecords)

    __slots__ = ("_native", "_common")

    @classmethod
    def _wrap(
        cls,
        native: AbstractSet[DataCoordinate],
        common: DataCoordinateCommonState,
    ) -> DataCoordinateAbstractSet:
        # Docstring inherited.
        # Operations on views can't necessary return views; return a new
        # frozenset instead.
        return DataCoordinateFrozenSet(native, check=False, **common.to_dict())

    def _unwrap(self) -> AbstractSet[DataCoordinate]:
        # Docstring inherited.
        return self._native

    @property
    def _common_state(self) -> DataCoordinateCommonState:
        # Docstring inherited.
        return self._common

    def __str__(self) -> str:
        return str(self._native)

    def __repr__(self) -> str:
        return f"DataCoordinateSetView({self._native}, {self.graph!r})"
