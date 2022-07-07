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
    "DataCoordinateReader",
    "DatasetRefReader",
)

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from typing import Any

from ...core import (
    ColumnTag,
    DataCoordinate,
    DatasetColumnTag,
    DatasetRef,
    DatasetType,
    DimensionGraph,
    DimensionKeyColumnTag,
)


class DataCoordinateReader(ABC):
    @staticmethod
    def make(dimensions: DimensionGraph, full: bool = True) -> DataCoordinateReader:
        if full:
            return _FullDataCoordinateReader(dimensions)
        else:
            return _BasicDataCoordinateReader(dimensions)

    __slots__ = ()

    @abstractmethod
    def read(self, row: Mapping[ColumnTag, Any]) -> DataCoordinate:
        raise NotImplementedError()


class _BasicDataCoordinateReader(DataCoordinateReader):
    def __init__(self, dimensions: DimensionGraph):
        self._dimensions = dimensions
        self._tags = tuple(DimensionKeyColumnTag(name) for name in self._dimensions.required.names)

    __slots__ = ("_dimensions", "_tags")

    def read(self, row: Mapping[ColumnTag, Any]) -> DataCoordinate:
        return DataCoordinate.fromRequiredValues(
            self._dimensions,
            tuple(row[tag] for tag in self._tags),
        )


class _FullDataCoordinateReader(DataCoordinateReader):
    def __init__(self, dimensions: DimensionGraph):
        self._dimensions = dimensions
        self._tags = tuple(DimensionKeyColumnTag(name) for name in self._dimensions._dataCoordinateIndices)

    __slots__ = ("_dimensions", "_tags")

    def read(self, row: Mapping[ColumnTag, Any]) -> DataCoordinate:
        return DataCoordinate.fromFullValues(
            self._dimensions,
            tuple(row[tag] for tag in self._tags),
        )


class DatasetRefReader:
    def __init__(
        self,
        dataset_type: DatasetType,
        *,
        full: bool = True,
        translate_collection: Callable[[Any], str] | None = None,
    ):
        self._data_coordinate_reader = DataCoordinateReader.make(dataset_type.dimensions, full=full)
        self._dataset_type = dataset_type
        self._translate_collection = translate_collection
        self._id_tag = DatasetColumnTag(dataset_type.name, "dataset_id")
        self._run_tag = DatasetColumnTag(dataset_type.name, "run")

    __slots__ = ("_data_coordinate_reader", "_dataset_type", "_translate_collection", "_id_tag", "_run_tag")

    def read(
        self,
        row: Mapping[ColumnTag, Any],
        *,
        run: str | None = None,
        data_id: DataCoordinate | None = None,
    ) -> DatasetRef:
        if data_id is None:
            data_id = self._data_coordinate_reader.read(row)
        if run is None:
            run_key = row[self._run_tag]
            if self._translate_collection is not None:
                run = self._translate_collection(run_key)
            else:
                run = run_key
        return DatasetRef(
            self._dataset_type,
            data_id,
            run=run,
            id=row[self._id_tag],
        )
