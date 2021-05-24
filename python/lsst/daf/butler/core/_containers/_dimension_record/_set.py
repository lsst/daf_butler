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

from typing import Dict, Iterable, Mapping

from ...dimensions import DataCoordinate, DimensionElement, DimensionRecord, DimensionUniverse
from ...named import NamedKeyDict, NamedKeyMapping
from .._data_coordinate import DataCoordinateIterable
from ._abstract_set import (
    HeterogeneousDimensionRecordAbstractSet,
    HomogeneousDimensionRecordAbstractSet,
)


class HeterogeneousDimensionRecordSet(HeterogeneousDimensionRecordAbstractSet):
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
        return self._universe

    @classmethod
    def from_data_ids(
        cls, universe: DimensionUniverse, data_ids: Iterable[DataCoordinate]
    ) -> HeterogeneousDimensionRecordAbstractSet:
        """Construct by extracting records from data IDs.

        Parameters
        ----------
        universe : `DimensionUniverse`
            Definitions for all dimensions.
        data_ids : `Iterable` [ `DataCoordinate` ]
            Data IDs to extract records from.  Data IDs for which
            `~DataCoordinate.hasRecords` returns `False` are ignored, as are
            any `None` records.

        Returns
        -------
        records : `HeterogeneousDimensionRecordAbstractSet`
            Deduplicated records extracted from ``data_ids``.
        """
        result = cls(universe)
        for data_id in data_ids:
            if data_id.hasRecords():
                for record in data_id.records.values():
                    if record is not None:
                        result.add(record)
        return result

    @property
    def by_definition(
        self,
    ) -> NamedKeyMapping[DimensionElement, HomogeneousDimensionRecordSet]:
        # Docstring inherited.
        return self._by_definition

    def add(self, record: DimensionRecord) -> None:
        """Add a record to the container.

        Parameters
        ----------
        record : `DimensionRecord`
            Record to add.  If a record instance with the same definition and
            data ID already exists, the new one will be used.
        """
        self._by_definition[record.definition].add(record)

    def update(self, records: Iterable[DimensionRecord]) -> None:
        """Add multiple records to the container.

        Parameters
        ----------
        records : `Iterable` [ `DimensionRecord` ]
            Records to add.  If a record instance with the same definition and
            data ID already exists, the new one will be used.
        """
        if isinstance(records, HeterogeneousDimensionRecordAbstractSet):
            for element, records_for_element in records.by_definition.items():
                self._by_definition[element].update(records_for_element)
        else:
            for record in records:
                self.add(record)

    def clear(self) -> None:
        """Remove all records from the container."""
        for inner in self._by_definition.values():
            inner.clear()


class HomogeneousDimensionRecordSet(HomogeneousDimensionRecordAbstractSet):
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
    def by_data_id(self) -> Mapping[DataCoordinate, DimensionRecord]:
        # Docstring inherited.
        return self._by_data_id

    def extract(self, data_ids: DataCoordinateIterable) -> HomogeneousDimensionRecordSet:
        # Docstring inherited.
        if data_ids.graph != self.definition.graph:
            raise ValueError(
                f"Invalid dimensions for dimension record lookup; expected {self.definition.graph}, "
                f"got {data_ids.graph}."
            )
        result = HomogeneousDimensionRecordSet(self._definition)
        for data_id in data_ids:
            if (record := self._by_data_id.get(data_id)) is not None:
                result._by_data_id[data_id] = record
        return result

    def add(self, record: DimensionRecord) -> None:
        """Add a record to the container.

        Parameters
        ----------
        record : `DimensionRecord`
            Record to add.  If a record instance with the same definition and
            data ID already exists, the new one will be used.

        Raises
        ------
        TypeError
            Raised if ``record.definition != self.definition``.
        """
        if record.definition != self.definition:
            raise TypeError(
                f"Incorrect dimension element for this container; expected {self.definition.name!r}; "
                f"got {record.definition.name!r}."
            )
        self._by_data_id[record.dataId] = record

    def update(self, records: Iterable[DimensionRecord]) -> None:
        """Add multiple records to the container.

        Parameters
        ----------
        records : `Iterable` [ `DimensionRecord` ]
            Records to add.  If a record instance with the same definition and
            data ID already exists, the new one will be used.

        Raises
        ------
        TypeError
            Raised if ``record.definition != self.definition`` for any given
            record.
        """
        if isinstance(records, HomogeneousDimensionRecordAbstractSet):
            if records.definition != self.definition:
                raise TypeError(
                    f"Incorrect dimension element for this container; expected {self.definition.name!r}; "
                    f"got {records.definition.name!r}."
                )
            self._by_data_id.update(records.by_data_id)
        else:
            for record in records:
                self.add(record)

    def clear(self) -> None:
        """Remove all records from the container."""
        self._by_data_id.clear()
