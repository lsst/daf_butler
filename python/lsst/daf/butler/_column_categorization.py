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

__all__ = ("ColumnCategorization",)

import dataclasses
from collections import defaultdict
from collections.abc import Iterable, Iterator
from typing import Any

from ._column_tags import DatasetColumnTag, DimensionKeyColumnTag, DimensionRecordColumnTag
from .dimensions import DimensionUniverse, GovernorDimension, SkyPixDimension


@dataclasses.dataclass
class ColumnCategorization:
    """Split an iterable of ColumnTag objects by type."""

    dimension_keys: set[str] = dataclasses.field(default_factory=set)
    dimension_records: defaultdict[str, set[str]] = dataclasses.field(
        default_factory=lambda: defaultdict(set)
    )
    datasets: defaultdict[str, set[str]] = dataclasses.field(default_factory=lambda: defaultdict(set))

    @classmethod
    def from_iterable(cls, iterable: Iterable[Any]) -> ColumnCategorization:
        result = cls()
        for tag in iterable:
            match tag:
                case DimensionKeyColumnTag(dimension=dimension):
                    result.dimension_keys.add(dimension)
                case DimensionRecordColumnTag(element=element, column=column):
                    result.dimension_records[element].add(column)
                case DatasetColumnTag(dataset_type=dataset_type, column=column):
                    result.datasets[dataset_type].add(column)
        return result

    def filter_skypix(self, universe: DimensionUniverse) -> Iterator[SkyPixDimension]:
        return (
            dimension for name in self.dimension_keys if (dimension := universe.skypix_dimensions.get(name))
        )

    def filter_governors(self, universe: DimensionUniverse) -> Iterator[GovernorDimension]:
        return (
            dimension
            for name in self.dimension_keys
            if isinstance(dimension := universe[name], GovernorDimension)
        )

    def filter_timespan_dataset_types(self) -> Iterator[str]:
        return (dataset_type for dataset_type, columns in self.datasets.items() if "timespan" in columns)

    def filter_timespan_dimension_elements(self) -> Iterator[str]:
        return (element for element, columns in self.dimension_records.items() if "timespan" in columns)

    def filter_spatial_region_dimension_elements(self) -> Iterator[str]:
        return (element for element, columns in self.dimension_records.items() if "region" in columns)
