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

__all__ = ("ButlerSqlEngine",)

import dataclasses
from collections.abc import Iterable, Set
from typing import Any, cast

import astropy.time
import sqlalchemy
from lsst.daf.relation import ColumnTag, sql

from ...core import (
    ColumnTypeInfo,
    LogicalColumn,
    Timespan,
    TimespanDatabaseRepresentation,
    ddl,
    is_timespan_column,
)


@dataclasses.dataclass(repr=False, eq=False, kw_only=True)
class ButlerSqlEngine(sql.Engine[LogicalColumn]):
    column_types: ColumnTypeInfo

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"ButlerSqlEngine({self.name!r})@{id(self):0x}"

    def extract_mapping(
        self, tags: Iterable[ColumnTag], sql_columns: sqlalchemy.sql.ColumnCollection
    ) -> dict[ColumnTag, LogicalColumn]:
        # Docstring inherited.
        result: dict[ColumnTag, LogicalColumn] = {}
        for tag in tags:
            if is_timespan_column(tag):
                result[tag] = self.column_types.timespan_cls.from_columns(
                    sql_columns, name=tag.qualified_name
                )
            else:
                result[tag] = sql_columns[tag.qualified_name]
        return result

    def select_items(
        self,
        items: Iterable[tuple[ColumnTag, LogicalColumn]],
        sql_from: sqlalchemy.sql.FromClause,
        *extra: sqlalchemy.sql.ColumnElement,
    ) -> sqlalchemy.sql.Select:
        # Docstring inherited.
        select_columns: list[sqlalchemy.sql.ColumnElement] = []
        for tag, logical_column in items:
            if is_timespan_column(tag):
                select_columns.extend(
                    cast(TimespanDatabaseRepresentation, logical_column).flatten(name=tag.qualified_name)
                )
            else:
                select_columns.append(
                    cast(sqlalchemy.sql.ColumnElement, logical_column).label(tag.qualified_name)
                )
        select_columns.extend(extra)
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).select_from(sql_from)

    def make_zero_select(self, tags: Set[ColumnTag]) -> sqlalchemy.sql.Select:
        # Docstring inherited.
        select_columns: list[sqlalchemy.sql.ColumnElement] = []
        for tag in tags:
            if is_timespan_column(tag):
                select_columns.extend(
                    self.column_types.timespan_cls.fromLiteral(None).flatten(name=tag.qualified_name)
                )
            else:
                select_columns.append(sqlalchemy.sql.literal(None).label(tag.qualified_name))
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).where(sqlalchemy.sql.literal(False))

    def convert_column_literal(self, value: Any) -> LogicalColumn:
        # Docstring inherited.
        if isinstance(value, Timespan):
            return self.column_types.timespan_cls.fromLiteral(value)
        elif isinstance(value, astropy.time.Time):
            return sqlalchemy.sql.literal(value, type_=ddl.AstropyTimeNsecTai)
        else:
            return super().convert_column_literal(value)
