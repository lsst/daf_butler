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

__all__ = ("ScalarDataCoordinateSet",)

from typing import AbstractSet, Any, Iterator

from ...dimensions import DataCoordinate, DimensionGraph
from ._abstract_set import DataCoordinateAbstractSet, DataCoordinateCommonState
from ._frozen_set import DataCoordinateFrozenSet


class ScalarDataCoordinateSet(DataCoordinateAbstractSet):
    """A `DataCoordinateAbstractSet` wrapper for a single `DataCoordinate`.

    Parameters
    ----------
    dataId : `DataCoordinate`
        The data ID to adapt.
    """

    def __init__(self, dataId: DataCoordinate):
        self._dataId = dataId

    __slots__ = ("_dataId",)

    @classmethod
    def _wrap(
        cls,
        native: AbstractSet[DataCoordinate],
        common: DataCoordinateCommonState,
    ) -> DataCoordinateAbstractSet:
        # Docstring inherited.
        # Operations on scalars can't necessary return scalars; return a new
        # frozenset instead.
        return DataCoordinateFrozenSet(native, check=False, **common.to_dict())

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

    def _unwrap(self) -> AbstractSet[DataCoordinate]:
        # Docstring inherited from DataCoordinateIterable.
        return {self._dataId}

    @property
    def _common_state(self) -> DataCoordinateCommonState:
        # Docstring inherited from DataCoordinateIterable.
        return DataCoordinateCommonState(
            graph=self.graph,
            hasFull=self._dataId.hasFull(),
            hasRecords=self._dataId.hasRecords(),
        )

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.hasFull()

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._dataId.hasRecords()

    def subset(self, graph: DimensionGraph) -> ScalarDataCoordinateSet:
        # Docstring inherited from DataCoordinateIterable.
        return ScalarDataCoordinateSet(self._dataId.subset(graph))
