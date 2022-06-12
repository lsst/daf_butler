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
    "RowTransformer",
    "DataCoordinateReader",
    "DatasetRefReader",
    "DimensionRecordReader",
)

from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, Mapping, Optional

from .._topology import TopologicalExtentDatabaseRepresentation
from ..datasets import DatasetRef, DatasetType
from ..dimensions import DataCoordinate, DataIdValue, DimensionElement, DimensionGraph, DimensionRecord
from ._column_tags import (
    ColumnTag,
    DatasetColumnTag,
    DimensionKeyColumnTag,
    DimensionRecordColumnTag,
    ResultRow,
)
from ._column_type_info import ColumnTypeInfo


class RowTransformer:
    def __init__(self, columns: Iterable[ColumnTag], column_types: ColumnTypeInfo):
        self._scalar_columns = set(columns)
        self._special_columns: dict[ColumnTag, type[TopologicalExtentDatabaseRepresentation]] = {}
        self._special_columns.update({tag: column_types.timespan_cls for tag in columns if tag.is_timespan})
        self._special_columns.update(
            {tag: column_types.spatial_region_cls for tag in columns if tag.is_spatial_region}
        )
        self._scalar_columns.difference_update(self._special_columns.keys())

    __slots__ = ("_scalar_columns", "_special_columns")

    def raw_to_logical(self, raw_row: Mapping[str, Any]) -> ResultRow:
        logical_row: ResultRow = {tag: raw_row[str(tag)] for tag in self._scalar_columns}
        for tag, db_repr_cls in self._special_columns.items():
            logical_row[tag] = db_repr_cls.extract(raw_row, name=str(tag))
        return logical_row

    def logical_to_raw(self, logical_row: ResultRow) -> dict[str, Any]:
        raw_row = {str(tag): logical_row[tag] for tag in self._scalar_columns}
        for tag, db_repr_cls in self._special_columns.items():
            db_repr_cls.update(logical_row[tag], result=raw_row, name=str(tag))
        return raw_row


class DataCoordinateReader(ABC):
    @staticmethod
    def make(
        dimensions: DimensionGraph,
        full: bool = True,
        records: Optional[Mapping[str, Mapping[tuple, DimensionRecord]]] = None,
    ) -> DataCoordinateReader:
        if records:
            return _ExpandedDataCoordinateReader(dimensions, records)
        elif full:
            return _FullDataCoordinateReader(dimensions)
        else:
            return _BasicDataCoordinateReader(dimensions)

    __slots__ = ()

    @abstractmethod
    def read(self, row: ResultRow) -> DataCoordinate:
        raise NotImplementedError()


class _BasicDataCoordinateReader(DataCoordinateReader):
    def __init__(self, dimensions: DimensionGraph):
        self._dimensions = dimensions
        self._tags = tuple(DimensionKeyColumnTag(name) for name in self._dimensions.required.names)

    __slots__ = ("_dimensions", "_tags")

    def read(self, row: ResultRow) -> DataCoordinate:
        return DataCoordinate.fromRequiredValues(
            self._dimensions,
            tuple(row[tag] for tag in self._tags),
        )


class _FullDataCoordinateReader(DataCoordinateReader):
    def __init__(self, dimensions: DimensionGraph):
        self._dimensions = dimensions
        self._tags = tuple(DimensionKeyColumnTag(name) for name in self._dimensions._dataCoordinateIndices)

    __slots__ = ("_dimensions", "_tags")

    def read(self, row: ResultRow) -> DataCoordinate:
        return DataCoordinate.fromFullValues(
            self._dimensions,
            tuple(row[tag] for tag in self._tags),
        )


class _ExpandedDataCoordinateReader(DataCoordinateReader):
    def __init__(
        self,
        dimensions: DimensionGraph,
        records: Mapping[str, Mapping[tuple[DataIdValue, ...], DimensionRecord]],
    ):
        self._full_reader = _FullDataCoordinateReader(dimensions)
        self._records = records
        # Lists of indices into the data ID that extract the subset data IDs
        # for each element (easier to understand by looking at how it is used
        # in the read method).
        self._subset_indices = {
            element.name: [dimensions._dataCoordinateIndices[d] for d in element.required.names]
            for element in dimensions.elements
        }

    __slots__ = ("_full_reader", "_records", "subset_indices")

    def read(self, row: ResultRow) -> DataCoordinate:
        full = self._full_reader.read(row)
        values = tuple(full.values())
        records_for_row: dict[str, DimensionRecord] = {}
        for element_name, indices in self._subset_indices.items():
            subset_data_id_values = tuple(values[i] for i in indices)
            records_for_row[element_name] = self._records[element_name][subset_data_id_values]
        return full.expanded(records_for_row)


class DatasetRefReader:
    def __init__(
        self,
        dataset_type: DatasetType,
        *,
        full: bool = True,
        records: Optional[Mapping[str, Mapping[tuple, DimensionRecord]]] = None,
        translate_collection: Optional[Callable[[Any], str]] = None,
    ):
        self._data_coordinate_reader = DataCoordinateReader.make(
            dataset_type.dimensions, full=full, records=records
        )
        self._dataset_type = dataset_type
        self._translate_collection = translate_collection
        self._id_tag = DatasetColumnTag(dataset_type.name, "dataset_id")
        self._run_tag = DatasetColumnTag(dataset_type.name, "run")

    __slots__ = ("_data_coordinate_reader", "_dataset_type", "_translate_collection", "_id_tag", "_run_tag")

    def read(
        self,
        row: ResultRow,
        *,
        run: Optional[str] = None,
        data_id: Optional[DataCoordinate] = None,
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


class DimensionRecordReader:
    def __init__(self, element: DimensionElement):
        self._cls = element.RecordClass
        self._tags: dict[str, ColumnTag] = {}
        for dimension_name, column_name in zip(element.dimensions.names, self._cls.fields.dimensions.names):
            self._tags[column_name] = DimensionKeyColumnTag(dimension_name)
        for column_name in self._cls.fields.facts.names:
            self._tags[column_name] = DimensionRecordColumnTag(element.name, column_name)
        if element.spatial is not None:
            self._tags["region"] = DimensionRecordColumnTag(element.name, "region")
        if element.temporal is not None:
            self._tags["timespan"] = DimensionRecordColumnTag(element.name, "timespan")

    __slots__ = ("_cls", "_tags")

    def read(self, row: ResultRow) -> DimensionRecord:
        if (result := row.get(self._cls)) is not None:
            return result
        return self._cls(**{key: row[tag] for key, tag in self._tags.items()})
