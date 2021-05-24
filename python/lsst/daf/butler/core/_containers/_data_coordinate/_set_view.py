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

from typing import AbstractSet, Iterable

from ...dimensions import DataCoordinate, DimensionGraph, DataCoordinateCommonState
from ._abstract_set import DataCoordinateAbstractSet
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
        lifetime have exactly these dimensions and the given `has_full` /
        `has_records` state.
    has_full : `bool`
        `True` if all data IDs satisfy `DataCoordinate.has_full`; `False` if
        none do (mixed containers are not permitted).
    has_records : `bool`
        Like ``has_full``, but for `DataCoordinate.has_records`.
    """

    def __init__(
        self,
        native: AbstractSet[DataCoordinate],
        graph: DimensionGraph,
        *,
        has_full: bool,
        has_records: bool,
    ):
        self._native = native
        self._common = DataCoordinateCommonState(graph, has_full=has_full, has_records=has_records)

    __slots__ = ("_native", "_common")

    @classmethod
    def _wrap(  # type: ignore
        cls,
        iterable: Iterable[DataCoordinate],
        common: DataCoordinateCommonState,
    ) -> DataCoordinateFrozenSet:
        # Docstring inherited.
        # Operations on views can't generally return views; we return a new
        # frozenset instead.  MyPy doesn't like this, because the base class
        # uses generics to say "always returns the same type as `cls`".  The
        # _proper_ fix is to painstakingly redefine `_wrap` in each
        # `DataCoordinateIterable` subclass *except this one* to return that
        # subclass type.  But it's a lot easier to just tell mypy to ignore
        # this special case.
        return DataCoordinateFrozenSet._wrap(iterable, common)

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
        return (
            f"DataCoordinateSetView({self._native}, {self.graph!r}, "
            f"has_full={self.has_full}, has_records={self.has_records})"
        )
