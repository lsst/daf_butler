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

__all__ = ("DataCoordinateFrozenSet",)

from typing import FrozenSet, Iterable

from ...dimensions import DataCoordinate, DimensionGraph, DataCoordinateCommonState
from ._abstract_set import DataCoordinateAbstractSet


class DataCoordinateFrozenSet(DataCoordinateAbstractSet):
    """A `DataCoordinateAbstractSet` implementation backed by a `frozenset`.

    Parameters
    ----------
    data_ids : `Iterable` [ `DataCoordinate` ]
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
        in ``graph``), which will then be removed when added to the set.
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
            self._native = frozenset(self._common.downgrade(data_id) for data_id in data_ids)
        else:
            self._native = frozenset(data_ids)

    __slots__ = ("_native", "_common")

    @classmethod
    def from_scalar(cls, data_id: DataCoordinate) -> DataCoordinateFrozenSet:
        """Return a single-element frozen set that wraps a single data ID.

        Parameters
        ----------
        data_id : `DataCoordinate`
            Data ID to wrap.

        Returns
        -------
        scalar_set : `DataCoordinateFrozenSet`
            A new set containing just the given data ID.
        """
        return cls._wrap({data_id}, DataCoordinateCommonState.from_data_coordinate(data_id))

    @classmethod
    def _wrap(
        cls,
        iterable: Iterable[DataCoordinate],
        common: DataCoordinateCommonState,
    ) -> DataCoordinateFrozenSet:
        # Docstring inherited.
        self = super().__new__(cls)
        self._common = common
        self._native = frozenset(iterable)
        return self

    def _unwrap(self) -> FrozenSet[DataCoordinate]:
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
            f"DataCoordinateFrozenSet({self._native}, {self.graph!r}, "
            f"has_full={self.has_full}, has_records={self.has_records})"
        )
