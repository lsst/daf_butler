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

from typing import Iterable, Sequence

from ...dimensions import DataCoordinate, DimensionGraph, DataCoordinateCommonState
from ._sequence import DataCoordinateSequence


class DataCoordinateTuple(DataCoordinateSequence):
    """A `DataCoordinateSequence` implementation backed by a tuple.

    Parameters
    ----------
    dataIds : `collections.abc.Iterable` [ `DataCoordinate` ]
        An iterable of `DataCoordinate` instances, with dimensions equal to
        ``graph``.
    graph : `DimensionGraph`
        Dimensions identified by all data IDs in the set.
    has_full : `bool`
        `True` if all data IDs satisfy `DataCoordinate.has_full`; `False` if
        none do (mixed containers are not permitted).
    has_records : `bool`
        Like ``has_full``, but for `DataCoordinate.has_records`.
    conform: `bool`, optional
        If `True` (default) check that all data IDs are consistent with the
        given ``graph`` and state flags at construction, downgrading if
        possible and raising if necessary.  If `False`, the caller guarantees
        consistency.  See `DataCoordinate.subset` for possible exceptions.
        Note that conforming data IDs with more dimensions than ``graph`` can
        result in duplicates (if the data IDs only differed in dimensions not
        in ``graph``), which will not be removed.
    """

    def __init__(
        self,
        data_ids: Iterable[DataCoordinate],
        graph: DimensionGraph,
        *,
        has_full: bool,
        has_records: bool,
        conform: bool = True,
    ):
        self._common = DataCoordinateCommonState(graph, has_full=has_full, has_records=has_records)
        if conform:
            self._native = tuple(self._common.downgrade(data_id) for data_id in data_ids)
        else:
            self._native = tuple(data_ids)

    __slots__ = ("_native", "_common")

    @property
    def _common_state(self) -> DataCoordinateCommonState:
        # Docstring inherited.
        return self._common

    @classmethod
    def _wrap(
        cls,
        iterable: Iterable[DataCoordinate],
        common: DataCoordinateCommonState,
    ) -> DataCoordinateTuple:
        # Docstring inherited.
        self = super().__new__(cls)
        self._common = common
        self._native = tuple(iterable)
        return self

    def _unwrap(self) -> Sequence[DataCoordinate]:
        # Docstring inherited.
        return self._native

    def __str__(self) -> str:
        return str(self._native)

    def __repr__(self) -> str:
        return (
            f"DataCoordinateTuple({self._native}, {self.graph!r} "
            f"has_full={self.has_full}, has_records={self.has_records})"
        )
