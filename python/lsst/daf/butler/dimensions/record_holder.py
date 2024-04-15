# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

__all__ = ("DimensionRecordHolder",)

import dataclasses
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from ._coordinate import DataCoordinate, DataIdValue
from ._group import DimensionGroup
from ._record_set import DimensionRecordSet
from ._records import DimensionRecord

if TYPE_CHECKING:
    from ._elements import DimensionElement
    from .record_cache import DimensionRecordCache


@dataclasses.dataclass
class _DimensionRecordLookupSet:
    indexer: Sequence[int]
    records: DimensionRecordSet

    @classmethod
    def build(cls, dimensions: DimensionGroup, record_set: DimensionRecordSet) -> _DimensionRecordLookupSet:
        indexer = cls._build_indexer(dimensions, record_set.element)
        return cls(indexer, record_set)

    @classmethod
    def make_empty(
        cls, dimensions: DimensionGroup, element: DimensionElement | str
    ) -> _DimensionRecordLookupSet:
        record_set = DimensionRecordSet(element, universe=dimensions.universe)
        return cls.build(dimensions, record_set)

    @staticmethod
    def _build_indexer(dimensions: DimensionGroup, element: DimensionElement) -> Sequence[int]:
        return tuple([dimensions._data_coordinate_indices[d] for d in element.required.names])

    def __getitem__(self, data_id_values: tuple[DataIdValue, ...]) -> DimensionRecord:
        return self.records.find_with_required_values(tuple([data_id_values[n] for n in self.indexer]))

    def chain(self, other: _DimensionRecordLookupSet) -> _DimensionRecordLookupSet:
        return _DimensionRecordLookupSet(self.indexer, self.records.union(other.records, lazy=True))


class DimensionRecordHolder:
    """A container class for the dimension records of multiple elements."""

    def __init__(
        self,
        dimensions: DimensionGroup,
        record_sets: Iterable[DimensionRecordSet] = (),
        *,
        cache: DimensionRecordCache | None = None,
        _lookup_sets: dict[str, _DimensionRecordLookupSet] | None = None,
    ):
        self._dimensions = dimensions
        self._lookup_sets = {} if _lookup_sets is None else _lookup_sets
        if cache is not None:
            for element_name in cache.keys() & self._dimensions.elements:
                self._lookup_sets[element_name] = _DimensionRecordLookupSet.build(
                    dimensions, cache[element_name]
                )
        for record_set in record_sets:
            self._lookup_sets[record_set.element.name] = _DimensionRecordLookupSet.build(
                dimensions, record_set
            )
        assert self._lookup_sets.keys() == self._dimensions.elements

    @property
    def dimensions(self) -> DimensionGroup:
        return self._dimensions

    def __getitem__(self, element: str) -> DimensionRecordSet:
        return self._lookup_sets[element].records

    def __setitem__(self, element: str, records: DimensionRecordSet) -> None:
        self._lookup_sets[element].records = records

    def expand_data_id(self, data_id: DataCoordinate) -> DataCoordinate:
        if not data_id.hasFull():
            # This is not an intrinsic limitation, but it's a complicated
            # algorithm to write and we don't have a clear use case for
            # starting with required-only DataCoordinates and a
            # `DimensionRecordHolder` (we do have a use case for starting with
            # a dictionary data ID and querying for both full data coordinates
            # all dimension records, but that doesn't belong here).
            raise ValueError("Only data IDs with full values can be expanded without a query.")
        return data_id.expanded(self._lookup_from_values(data_id.full_values))

    def chain(self, other: DimensionRecordHolder) -> DimensionRecordHolder:
        return DimensionRecordHolder(
            self._dimensions,
            _lookup_sets={
                element_name: lookup_set.chain(other._lookup_sets[element_name])
                for element_name, lookup_set in self._lookup_sets.items()
            },
        )

    def _lookup_from_values(self, data_id_values: tuple[DataIdValue, ...]) -> dict[str, DimensionRecord]:
        return {
            element_name: self._lookup_sets[element_name][data_id_values]
            for element_name in self._dimensions.elements
        }
