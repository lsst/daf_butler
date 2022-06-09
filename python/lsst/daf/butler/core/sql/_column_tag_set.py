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

__all__ = ("ColumnTagSet",)

import dataclasses
from collections import defaultdict
from typing import AbstractSet, Any, Iterable, Iterator, Mapping

from ._column_tags import ColumnTag, DatasetColumnTag, DimensionKeyColumnTag, DimensionRecordColumnTag


@dataclasses.dataclass(frozen=True)
class ColumnTagSet(AbstractSet[ColumnTag]):
    dimensions: AbstractSet[str]
    dimension_records: Mapping[str, AbstractSet[str]]
    datasets: Mapping[str, AbstractSet[str]]

    @classmethod
    def _from_iterable(cls, iterable: Iterable[ColumnTag]) -> ColumnTagSet:
        dimensions: set[str] = set()
        dimension_records: defaultdict[str, set[str]] = defaultdict(set)
        datasets: defaultdict[str, set[str]] = defaultdict(set)
        for tag in iterable:
            match tag:
                case DimensionKeyColumnTag(dimension=dimension):
                    dimensions.add(dimension)
                case DimensionRecordColumnTag(element=element, column=column):
                    dimension_records[element].add(column)
                case DatasetColumnTag(dataset_type=dataset_type, column=column):
                    datasets[dataset_type].add(column)
                case _:
                    raise ValueError(f"Unexpected column tag: {tag}.")
        return ColumnTagSet(dimensions, dict(dimension_records), dict(datasets))

    def __iter__(self) -> Iterator[ColumnTag]:
        yield from DimensionKeyColumnTag.generate(self.dimensions)
        for element, columns in self.dimension_records.items():
            yield from DimensionRecordColumnTag.generate(element, columns)
        for dataset_type, columns in self.datasets.items():
            yield from DatasetColumnTag.generate(dataset_type, columns)

    def __len__(self) -> int:
        return len(self.dimensions) + len(self.dimension_records) + len(self.datasets)

    def __contains__(self, key: Any) -> bool:
        match key:
            case DimensionRecordColumnTag(element=element, column=column):
                return column in self.dimension_records.get(element, frozenset())
            case DatasetColumnTag(dataset_type=dataset_type, column=column):
                return column in self.datasets.get(dataset_type, frozenset())
            case DimensionKeyColumnTag(dimension=dimension):
                return dimension in self.dimensions
            case _:
                return False

    def union(self, *args: Iterable[ColumnTag]) -> ColumnTagSet:
        flat_set: set[ColumnTag] = set(self)
        flat_set.update(*args)
        return ColumnTagSet._from_iterable(flat_set)

    def intersection(self, *args: Iterable[ColumnTag]) -> ColumnTagSet:
        flat_set: set[ColumnTag] = set(self)
        flat_set.intersection_update(*args)
        return ColumnTagSet._from_iterable(flat_set)

    def get_timespans(self) -> AbstractSet[ColumnTag]:
        return {tag for tag in self if tag.is_timespan}

    def get_spatial_regions(self) -> AbstractSet[ColumnTag]:
        return {tag for tag in self if tag.is_spatial_region}
