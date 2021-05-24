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

__all__ = ("HeterogeneousDimensionRecordSet", "HomogeneousDimensionRecordSet")

from typing import Dict, Iterable, MutableMapping

from ...dimensions import DataCoordinate, DimensionElement, DimensionRecord, DimensionUniverse
from ...named import NamedKeyDict, NamedKeyMapping
from .._data_coordinate import DataCoordinateIterable
from ._mutable_set import (
    HeterogeneousDimensionRecordMutableSet,
    HomogeneousDimensionRecordMutableSet,
)


class HeterogeneousDimensionRecordSet(HeterogeneousDimensionRecordMutableSet):
    """A concrete mutable container for heterogeneous, unique dimension
    records.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Definitions for all dimensions.
    records : `Iterable` [ `DimensionRecord` ]
        Dimension records to include at construction.
    """

    def __init__(self, universe: DimensionUniverse, records: Iterable[DimensionRecord] = ()):
        self._by_definition = NamedKeyDict[DimensionElement, HomogeneousDimensionRecordSet]({
            element: HomogeneousDimensionRecordSet(element)
            for element in universe.getStaticElements()
        })
        self._universe = universe
        self.update(records)

    __slots__ = (
        "_universe",
        "_by_definition",
    )

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._universe

    @property
    def by_definition(
        self,
    ) -> NamedKeyMapping[DimensionElement, HomogeneousDimensionRecordSet]:
        # Docstring inherited.
        return self._by_definition


class HomogeneousDimensionRecordSet(HomogeneousDimensionRecordMutableSet):
    """A concrete mutable container for homogeneous, unique dimension
    records.

    Parameters
    ----------
    definition : `DimensionElement`
        Element associated with all records.
    records : `Iterable` [ `DimensionRecord` ]
        Dimension records to include at construction.

    Raises
    ------
    TypeError
        Raised if ``record.definition != definition`` for any given record.
    """

    def __init__(self, definition: DimensionElement, records: Iterable[DimensionRecord] = ()):
        self._definition = definition
        self._by_data_id: Dict[DataCoordinate, DimensionRecord] = {}
        self.update(records)

    __slots__ = ("_definition", "_by_data_id")

    @property
    def definition(self) -> DimensionElement:
        # Docstring inherited.
        return self._definition

    @property
    def by_data_id(self) -> MutableMapping[DataCoordinate, DimensionRecord]:
        # Docstring inherited.
        return self._by_data_id

    def _get_many(self, data_ids: DataCoordinateIterable) -> HomogeneousDimensionRecordSet:
        # Docstring inherited.
        if data_ids.graph != self.definition.graph:
            raise ValueError(
                f"Invalid dimensions for dimension record lookup; expected {self.definition.graph}, "
                f"got {data_ids.graph}."
            )
        result = HomogeneousDimensionRecordSet(self.definition)
        for data_id in data_ids:
            if (record := self._by_data_id.get(data_id)) is not None:
                result._by_data_id[data_id] = record
        return result
