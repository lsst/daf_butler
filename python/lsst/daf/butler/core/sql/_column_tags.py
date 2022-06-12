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
    "ColumnTag",
    "DimensionKeyColumnTag",
    "DimensionRecordColumnTag",
    "DatasetColumnTag",
    "LogicalColumn",
    "ResultRow",
    "ResultTag",
)

import dataclasses
import itertools
import re
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Dict, Iterable, Iterator, Mapping, Type, Union, cast, final

import sqlalchemy

from .._spatial_regions import SpatialRegionDatabaseRepresentation
from .._topology import TopologicalExtentDatabaseRepresentation
from ..ddl import FieldSpec, TableSpec
from ..dimensions import Dimension, DimensionRecord
from ..timespan import TimespanDatabaseRepresentation
from ._column_type_info import ColumnTypeInfo

LogicalColumn = Union[
    sqlalchemy.sql.ColumnElement,
    SpatialRegionDatabaseRepresentation,
    TimespanDatabaseRepresentation,
]


class ColumnTag(ABC):

    __slots__ = ()

    _QUALIFIED_COLUMN_NAME_REGEX: ClassVar[re.Pattern] = re.compile(
        r"((?P<prefix>[nt])\!(?P<relation>\w+):)?(?P<column>\w+)"
    )

    @staticmethod
    def parse(name: str) -> ColumnTag:
        if m := ColumnTag._QUALIFIED_COLUMN_NAME_REGEX.fullmatch(name):
            if (prefix := m.group("prefix")) is None:
                return DimensionKeyColumnTag(name)
            elif prefix == "n":
                return DimensionRecordColumnTag(m.group("relation"), m.group("column"))
            elif prefix == "t":
                return DatasetColumnTag(m.group("relation"), m.group("column"))
        raise ValueError(f"Could not parse {name!r} as a qualified column name.")

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError()

    @property
    def is_timespan(self) -> bool:
        return False

    @property
    def is_spatial_region(self) -> bool:
        return False

    @abstractmethod
    def extract_logical_column(
        self,
        sql_columns: sqlalchemy.sql.ColumnCollection,
        column_types: ColumnTypeInfo,
        name: str | None = None,
    ) -> LogicalColumn:
        raise NotImplementedError()

    @abstractmethod
    def flatten_logical_column(
        self, logical_column: LogicalColumn
    ) -> tuple[sqlalchemy.sql.ColumnElement, ...]:
        raise NotImplementedError()

    @abstractmethod
    def make_field_specs(self, column_types: ColumnTypeInfo) -> tuple[FieldSpec, ...]:
        raise NotImplementedError()

    def index_scalar(
        self, logical_columns: Mapping[ColumnTag, LogicalColumn]
    ) -> sqlalchemy.sql.ColumnElement:
        assert (
            not self.is_timespan and not self.is_spatial_region
        ), f"Logic error in use of index_scalar with tag {self}."
        return cast(sqlalchemy.sql.ColumnElement, logical_columns[self])

    def index_spatial_region(
        self, logical_columns: Mapping[ColumnTag, LogicalColumn]
    ) -> SpatialRegionDatabaseRepresentation:
        assert self.is_spatial_region, f"Logic error in use of index_spatial_region with tag {self}."
        return cast(SpatialRegionDatabaseRepresentation, logical_columns[self])

    def index_timespan(
        self, logical_columns: Mapping[ColumnTag, LogicalColumn]
    ) -> TimespanDatabaseRepresentation:
        assert self.is_timespan, f"Logic error in use of index_timespan with tag {self}."
        return cast(TimespanDatabaseRepresentation, logical_columns[self])

    @staticmethod
    def extract_logical_column_mapping(
        tags: Iterable[ColumnTag],
        sql_columns: sqlalchemy.sql.ColumnCollection,
        column_types: ColumnTypeInfo,
    ) -> dict[ColumnTag, LogicalColumn]:
        result: dict[ColumnTag, LogicalColumn] = {}
        for tag in tags:
            result[tag] = tag.extract_logical_column(sql_columns, column_types)
        return result

    @staticmethod
    def select_logical_column_items(
        items: Iterable[tuple[ColumnTag, LogicalColumn]],
        sql_from: sqlalchemy.sql.FromClause,
        *extra_columns: sqlalchemy.sql.ColumnElement,
    ) -> sqlalchemy.sql.Select:
        columns = list(
            itertools.chain.from_iterable(
                tag.flatten_logical_column(logical_column) for tag, logical_column in items
            )
        )
        columns.extend(extra_columns)
        if not columns:
            columns.append(sqlalchemy.sql.literal(True).label("ignored"))
        return sqlalchemy.sql.select(*columns).select_from(sql_from)

    @staticmethod
    def make_table_spec(tags: Iterable[ColumnTag], column_types: ColumnTypeInfo) -> TableSpec:
        return TableSpec(itertools.chain.from_iterable(tag.make_field_specs(column_types) for tag in tags))


@final
@dataclasses.dataclass(frozen=True)
class DimensionKeyColumnTag(ColumnTag):
    __slots__ = ("dimension",)
    dimension: str

    def __str__(self) -> str:
        return self.dimension

    def extract_logical_column(
        self,
        sql_columns: sqlalchemy.sql.ColumnCollection,
        column_types: ColumnTypeInfo,
        name: str | None = None,
    ) -> LogicalColumn:
        if name is None:
            name = str(self)
        return sql_columns[name]

    def flatten_logical_column(
        self, logical_column: LogicalColumn
    ) -> tuple[sqlalchemy.sql.ColumnElement, ...]:
        return (cast(sqlalchemy.sql.ColumnElement, logical_column).label(str(self)),)

    def make_field_specs(self, column_types: ColumnTypeInfo) -> tuple[FieldSpec, ...]:
        return (
            dataclasses.replace(
                cast(Dimension, column_types.universe[self.dimension]).primaryKey, name=str(self)
            ),
        )

    @classmethod
    def generate(cls, dimensions: Iterable[str]) -> Iterator[DimensionKeyColumnTag]:
        return (cls(d) for d in dimensions)


@final
@dataclasses.dataclass(frozen=True)
class DimensionRecordColumnTag(ColumnTag):
    __slots__ = ("element", "column")
    element: str
    column: str

    def __str__(self) -> str:
        return f"n!{self.element}:{self.column}"

    @property
    def is_timespan(self) -> bool:
        return self.column == "timespan"

    @property
    def is_spatial_region(self) -> bool:
        return self.column == "region"

    def extract_logical_column(
        self,
        sql_columns: sqlalchemy.sql.ColumnCollection,
        column_types: ColumnTypeInfo,
        name: str | None = None,
    ) -> LogicalColumn:
        if name is None:
            name = str(self)
        if self.is_spatial_region:
            return column_types.spatial_region_cls.from_columns(sql_columns, name=name)
        elif self.is_timespan:
            return column_types.timespan_cls.from_columns(sql_columns, name=name)
        else:
            return sql_columns[name]

    def flatten_logical_column(
        self, logical_column: LogicalColumn
    ) -> tuple[sqlalchemy.sql.ColumnElement, ...]:
        if self.is_spatial_region or self.is_timespan:
            return cast(TopologicalExtentDatabaseRepresentation, logical_column).flatten(name=str(self))
        else:
            return (cast(sqlalchemy.sql.ColumnElement, logical_column).label(str(self)),)

    def make_field_specs(self, column_types: ColumnTypeInfo) -> tuple[FieldSpec, ...]:
        if self.is_spatial_region:
            return column_types.spatial_region_cls.makeFieldSpecs(nullable=True, name=str(self))
        elif self.is_timespan:
            return column_types.timespan_cls.makeFieldSpecs(nullable=True, name=str(self))
        else:
            return (
                dataclasses.replace(
                    column_types.universe[self.element].RecordClass.fields.standard[self.column],
                    name=str(self),
                ),
            )

    @classmethod
    def generate(cls, element: str, columns: Iterable[str]) -> Iterator[DimensionRecordColumnTag]:
        return (cls(element, column) for column in columns)


@final
@dataclasses.dataclass(frozen=True)
class DatasetColumnTag(ColumnTag):
    __slots__ = ("dataset_type", "column")
    dataset_type: str
    column: str

    def __str__(self) -> str:
        return f"t!{self.dataset_type}:{self.column}"

    @property
    def is_timespan(self) -> bool:
        return self.column == "timespan"

    def extract_logical_column(
        self,
        sql_columns: sqlalchemy.sql.ColumnCollection,
        column_types: ColumnTypeInfo,
        name: str | None = None,
    ) -> LogicalColumn:
        if name is None:
            name = str(self)
        if self.is_timespan:
            return column_types.timespan_cls.from_columns(sql_columns, name=name)
        else:
            return sql_columns[name]

    def flatten_logical_column(
        self, logical_column: LogicalColumn
    ) -> tuple[sqlalchemy.sql.ColumnElement, ...]:
        if self.is_timespan:
            return cast(TimespanDatabaseRepresentation, logical_column).flatten(name=str(self))
        else:
            return (cast(sqlalchemy.sql.ColumnElement, logical_column).label(str(self)),)

    def make_field_specs(self, column_types: ColumnTypeInfo) -> tuple[FieldSpec, ...]:
        if self.is_timespan:
            return column_types.timespan_cls.makeFieldSpecs(nullable=True, name=str(self))
        elif self.column == "dataset_id":
            return (dataclasses.replace(column_types.dataset_id_spec, name=str(self)),)
        elif self.column == "run":
            return (dataclasses.replace(column_types.run_key_spec, name=str(self)),)
        elif self.column == "rank":
            return (FieldSpec(name=str(self), dtype=sqlalchemy.SmallInteger),)
        elif self.column == "ingest_date":
            return (FieldSpec(name=str(self), dtype=sqlalchemy.TIMESTAMP),)
        else:
            raise RuntimeError(f"Unexpected column {self.column!r} for dataset column tag {self}.")

    @classmethod
    def generate(cls, dataset_type: str, columns: Iterable[str]) -> Iterator[DatasetColumnTag]:
        return (cls(dataset_type, column) for column in columns)


ResultTag = Union[ColumnTag, Type[DimensionRecord]]

ResultRow = Dict[ResultTag, Any]
