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
    "HeterogeneousDimensionRecordAbstractSet",
    "HomogeneousDimensionRecordAbstractSet",
)

from abc import abstractmethod
from typing import Any, Collection, Iterator, Mapping

from ...dimensions import DataCoordinate, DimensionElement, DimensionRecord
from ...named import NamedKeyMapping
from .._data_coordinate import (
    DataCoordinateAbstractSet,
    DataCoordinateIterable,
    DataCoordinateSetView,
)
from ._iterable import HeterogeneousDimensionRecordIterable, HomogeneousDimensionRecordIterable


class HeterogeneousDimensionRecordAbstractSet(HeterogeneousDimensionRecordIterable):
    """An abstract base class for heterogeneous containers of unique dimension
    records.

    Notes
    -----
    This interface is only informally set-like - it guarantees that there are
    no duplicate elements (as determined by `DimensionRecord.dataId` equality,
    since this should imply equality for other fields), and provides no
    ordering guarantees.  It intentionally does not provide much of the
    `collections.abc.Set` interface, as this would at a lot of complexity
    without clear benefit (not just in implementation, but in behavior
    guarantees as well).
    """

    __slots__ = ()

    def __iter__(self) -> Iterator[DimensionRecord]:
        for inner in self.by_definition.values():
            yield from inner

    def __contains__(self, key: Any) -> bool:
        try:
            return key.dataId in self.by_definition[key.definition]
        except (AttributeError, KeyError):
            return False

    def __len__(self) -> int:
        return sum(len(inner) for inner in self.by_definition.values())

    def __eq__(self, other: Any) -> bool:
        try:
            return self.by_definition == other.by_definition
        except AttributeError:
            return NotImplemented

    @property
    @abstractmethod
    def by_definition(
        self,
    ) -> NamedKeyMapping[DimensionElement, HomogeneousDimensionRecordAbstractSet]:
        """A mapping view that groups records by `DimensionElement`
        definition.

        This container always has all elements in its universe, even when it
        has no records for an element.
        """
        raise NotImplementedError()


class HomogeneousDimensionRecordAbstractSet(HomogeneousDimensionRecordIterable, Collection[DimensionRecord]):
    """An abstract base class for homogeneous containers of unique dimension
    records.

    Notes
    -----
    All elements of a `HomogeneousDimensionRecordAbstractSet` correspond to the
    same `DimensionElement` (i.e. share the same value for their
    `~DimensionRecord.definition` attribute).

    This interface is only informally set-like - it guarantees that there are
    no duplicate elements (as determined by `DimensionRecord.dataId` equality,
    since this should imply equality for other fields), and provides no
    ordering guarantees.  It intentionally does not provide much of the
    `collections.abc.Set` interface, as this would at a lot of complexity
    without clear benefit (not just in implementation, but in behavior
    guarantees as well).
    """

    __slots__ = ()

    def __iter__(self) -> Iterator[DimensionRecord]:
        return iter(self.by_data_id.values())

    def __contains__(self, key: Any) -> bool:
        try:
            return key.dataId in self.by_data_id
        except (AttributeError, KeyError):
            return False

    def __len__(self) -> int:
        return len(self.by_data_id)

    def __eq__(self, other: Any) -> bool:
        try:
            return self.by_data_id.keys() == other.by_data_id.keys()
        except AttributeError:
            return NotImplemented

    @property
    def data_ids(self) -> DataCoordinateAbstractSet:
        """A view of the data IDs these records correspond to
        (`DataCoordinateAbstractSet`).

        This is guaranteed to have the same elements and order as
        ``self.by_data_id.keys()`` (but with the extra functionality the
        specialized `DataCoordinateIterable` hierarchy provides).
        """
        return DataCoordinateSetView(
            self.by_data_id.keys(),
            graph=self.definition.graph,
            hasFull=(not self.definition.graph.implied),
            hasRecords=False,
        )

    @property
    @abstractmethod
    def by_data_id(self) -> Mapping[DataCoordinate, DimensionRecord]:
        """A mapping view keyed by data ID."""
        raise NotImplementedError()

    @abstractmethod
    def extract(self, data_ids: DataCoordinateIterable) -> HomogeneousDimensionRecordAbstractSet:
        """Return a new set with records for just the given data IDs.

        Parameters
        ----------
        data_ids : `DataCoordinateIterable`
            Data IDs to select.

        Returns
        -------
        extraction : `HomogeneousDimensionRecordAbstractSet`
            New set with just the given data IDs.  Data IDs whose records are
            not in ``self`` will be ignored, and the order of the given data
            IDs and returned records are not guaranteed to be consistent.

        Raises
        ------
        ValueError
            Raised if the given data IDs do not have the right dimensions
            (``self.definition.graph``).
        """
        raise NotImplementedError()
