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

__all__ = (
    "HeterogeneousDimensionRecordMutableSet",
    "HomogeneousDimensionRecordMutableSet",
)

from abc import abstractmethod
from typing import Iterable, MutableMapping

from ...dimensions import DataCoordinate, DimensionElement, DimensionRecord
from ...named import NamedKeyMapping
from ._abstract_set import HeterogeneousDimensionRecordAbstractSet, HomogeneousDimensionRecordAbstractSet


class HeterogeneousDimensionRecordMutableSet(HeterogeneousDimensionRecordAbstractSet):
    """An abstract base class for mutable heterogeneous containers of unique
    dimension records.

    Notes
    -----
    This interface is only informally set-like - it guarantees that there are
    no duplicate elements (as determined by `DimensionRecord.dataId` equality,
    since this should imply equality for other fields), and provides no
    ordering guarantees.  It intentionally does not provide much of the
    `collections.abc.MutableSet` interface, as this would add a lot of
    complexity without clear benefit (not just in implementation, but in
    behavior guarantees as well).
    """

    __slots__ = ()

    @property
    @abstractmethod
    def by_definition(
        self,
    ) -> NamedKeyMapping[DimensionElement, HomogeneousDimensionRecordMutableSet]:
        """A mapping view that groups records by `DimensionElement`
        definition.

        This container always has all elements in its universe, even when it
        has no records for an element.  Nested containers must be mutable, and
        mutating them must affect the parent container appropriately.
        """
        raise NotImplementedError()

    def add(self, record: DimensionRecord) -> None:
        """Add a record to the container.

        Parameters
        ----------
        record : `DimensionRecord`
            Record to add.  If a record instance with the same definition and
            data ID already exists, the new one will be used.
        """
        self.by_definition[record.definition].add(record)

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
                self.by_definition[element].update(records_for_element)
        else:
            for record in records:
                self.add(record)

    def clear(self) -> None:
        """Remove all records from the container."""
        for inner in self.by_definition.values():
            inner.clear()

    def update_from(self, data_id: DataCoordinate) -> None:
        """Add any records attached to the given data ID.

        Parameters
        ----------
        data_id : `DataCoordinate`
            Data ID to extract records from.
        """
        if data_id.hasRecords():
            for record in data_id.records.values():
                if record is not None:
                    self.add(record)


class HomogeneousDimensionRecordMutableSet(HomogeneousDimensionRecordAbstractSet):
    """An abstract base class for homogeneous containers of unique dimension
    records.

    Notes
    -----
    All elements of a `HomogeneousDimensionRecordMutableSet` correspond to the
    same `DimensionElement` (i.e. share the same value for their
    `~DimensionRecord.definition` attribute).

    This interface is only informally set-like - it guarantees that there are
    no duplicate elements (as determined by `DimensionRecord.dataId` equality,
    since this should imply equality for other fields), and provides no
    ordering guarantees.  It intentionally does not provide much of the
    `collections.abc.MutableSet` interface, as this would add a lot of
    complexity without clear benefit (not just in implementation, but in
    behavior guarantees as well).
    """

    __slots__ = ()

    @property
    @abstractmethod
    def by_data_id(self) -> MutableMapping[DataCoordinate, DimensionRecord]:
        """A mapping view keyed by data ID.

        This mapping must be mutable, and mutating it must affect the parent
        container.
        """
        raise NotImplementedError()

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
        self.by_data_id[record.dataId] = record

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
            self.by_data_id.update(records.by_data_id)
        else:
            for record in records:
                self.add(record)

    def clear(self) -> None:
        """Remove all records from the container."""
        self.by_data_id.clear()
