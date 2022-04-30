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
    "ColumnKind",
    "ColumnTag",
    "ColumnTypeData",
    "DimensionKeyColumnTag",
    "DimensionRecordColumnTag",
    "DatasetColumnTag",
    "SqlLogicalColumn",
)

import dataclasses
import enum
import itertools
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar, Iterable, Iterator, Mapping, Optional, Union, final

import sqlalchemy

from ....core import (
    DataCoordinate,
    DatasetRef,
    DatasetType,
    DimensionGraph,
    SpatialRegionDatabaseRepresentation,
    TimespanDatabaseRepresentation,
)

if TYPE_CHECKING:
    from .._collections import CollectionManager
    from .._database import Database


SqlLogicalColumn = Union[
    sqlalchemy.sql.ColumnElement,
    SpatialRegionDatabaseRepresentation,
    TimespanDatabaseRepresentation,
]


@dataclasses.dataclass
class ColumnTypeData:
    spatial_region_cls: type[SpatialRegionDatabaseRepresentation]
    timespan_cls: type[TimespanDatabaseRepresentation]

    @classmethod
    def from_database(cls, db: Database) -> ColumnTypeData:
        return cls(
            spatial_region_cls=db.getSpatialRegionRepresentation(),
            timespan_cls=db.getTimespanRepresentation(),
        )


class ColumnTag(ABC):

    __slots__ = ()

    kind: ClassVar[ColumnKind]

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

    @abstractmethod
    def extract_logical_column(
        self, sql_columns: sqlalchemy.sql.ColumnCollection, column_types: ColumnTypeData
    ) -> SqlLogicalColumn:
        raise NotImplementedError()

    @abstractmethod
    def flatten_logical_column(
        self, logical_column: SqlLogicalColumn
    ) -> Iterator[sqlalchemy.sql.ColumnElement]:
        raise NotImplementedError()

    @staticmethod
    def extract_mapping(
        tags: Iterable[ColumnTag],
        sql_columns: sqlalchemy.sql.ColumnCollection,
        column_types: ColumnTypeData,
    ) -> dict[ColumnTag, SqlLogicalColumn]:
        result: dict[ColumnTag, SqlLogicalColumn] = {}
        for tag in tags:
            result[tag] = tag.extract_logical_column(sql_columns, column_types)
        return result

    @staticmethod
    def select(
        items: Iterable[tuple[ColumnTag, SqlLogicalColumn]],
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
    def join_on_condition(
        a: Mapping[ColumnTag, SqlLogicalColumn], b: Mapping[ColumnTag, SqlLogicalColumn]
    ) -> sqlalchemy.sql.ColumnElement:
        terms: list[sqlalchemy.sql.ColumnElement] = []
        for tag in a.keys() & b.keys():
            terms.append(a[tag] == b[tag])
        if not terms:
            return sqlalchemy.sql.literal(True)
        elif len(terms) == 1:
            return terms[0]
        else:
            return sqlalchemy.sql.and_(*terms)

    @staticmethod
    def read_data_coordinate(
        dimensions: DimensionGraph, row: sqlalchemy.engine.Row, *, full: bool = True
    ) -> DataCoordinate:
        required_values = tuple(row[name] for name in dimensions.required.names)
        if full:
            return DataCoordinate.fromFullValues(
                dimensions,
                required_values + tuple(row[name] for name in dimensions.implied.names),
            )
        else:
            return DataCoordinate.fromRequiredValues(dimensions, required_values)

    @staticmethod
    def read_dataset_ref(
        dataset_type: DatasetType,
        row: sqlalchemy.engine.Row,
        *,
        run: Optional[str] = None,
        data_id: Optional[DataCoordinate] = None,
        collections: Optional[CollectionManager] = None,
        full: bool = True,
    ) -> DatasetRef:
        if data_id is None:
            data_id = ColumnTag.read_data_coordinate(dataset_type.dimensions, row, full=full)
        if run is None:
            assert collections is not None, "Must pass one of collections or RUN."
            run_key = row[str(DatasetColumnTag(dataset_type.name, "run"))]
            run = collections[run_key].name
        return DatasetRef(
            dataset_type,
            data_id,
            run=run,
            id=row[str(DatasetColumnTag(dataset_type.name, "dataset_id"))],
        )


@final
@dataclasses.dataclass(frozen=True)
class DimensionKeyColumnTag(ColumnTag):
    __slots__ = ("dimension",)
    dimension: str

    def __str__(self) -> str:
        return self.dimension

    def extract_logical_column(
        self, sql_columns: sqlalchemy.sql.ColumnCollection, column_types: ColumnTypeData
    ) -> SqlLogicalColumn:
        return sql_columns[str(self)]

    def flatten_logical_column(
        self, logical_column: SqlLogicalColumn
    ) -> Iterator[sqlalchemy.sql.ColumnElement]:
        # We know dimension keys are never regions or timespans, but MyPy
        # doesn't.
        yield logical_column.label(str(self))  # type: ignore

    @classmethod
    def generate(cls, dimension_names: Iterable[str]) -> Iterator[DimensionKeyColumnTag]:
        return (cls(name) for name in dimension_names)


@final
@dataclasses.dataclass(frozen=True)
class DimensionRecordColumnTag(ColumnTag):
    __slots__ = ("element", "column")
    element: str
    column: str

    def __str__(self) -> str:
        return f"n!{self.element}:{self.column}"

    def extract_logical_column(
        self, sql_columns: sqlalchemy.sql.ColumnCollection, column_types: ColumnTypeData
    ) -> SqlLogicalColumn:
        if self.column == "region":
            return column_types.spatial_region_cls.from_columns(sql_columns, name=str(self))
        elif self.column == "timespan":
            return column_types.timespan_cls.from_columns(sql_columns, name=str(self))
        else:
            return sql_columns[str(self)]

    def flatten_logical_column(
        self, logical_column: SqlLogicalColumn
    ) -> Iterator[sqlalchemy.sql.ColumnElement]:
        if self.column == "region" or self.column == "timespan":
            yield from logical_column.flatten(name=str(self))
        else:
            # We know this logic branch is for regular columns, but MyPy
            # doesn't
            yield logical_column.label(str(self))  # type: ignore

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

    def extract_logical_column(
        self, sql_columns: sqlalchemy.sql.ColumnCollection, column_types: ColumnTypeData
    ) -> SqlLogicalColumn:
        if self.column == "timespan":
            return column_types.timespan_cls.from_columns(sql_columns, name=str(self))
        else:
            return sql_columns[str(self)]

    def flatten_logical_column(
        self, logical_column: SqlLogicalColumn
    ) -> Iterator[sqlalchemy.sql.ColumnElement]:
        if self.column == "timespan":
            yield from logical_column.flatten(name=str(self))
        else:
            # We know this logic branch is for regular columns, but MyPy
            # doesn't
            yield logical_column.label(str(self))  # type: ignore

    @classmethod
    def generate(
        cls, dataset_type: str, columns: Iterable[str], column_types: ColumnTypeData
    ) -> Iterator[DatasetColumnTag]:
        return (cls(dataset_type, column) for column in columns)


class ColumnKind(enum.Enum):
    DIMENSION_KEY = DimensionKeyColumnTag
    DIMENSION_RECORD = DimensionRecordColumnTag
    DATASET = DatasetColumnTag

    @property
    def tag_type(self) -> type[ColumnTag]:
        return self.value


DimensionKeyColumnTag.kind = ColumnKind.DIMENSION_KEY
DimensionRecordColumnTag.kind = ColumnKind.DIMENSION_RECORD
DatasetColumnTag.kind = ColumnKind.DATASET
