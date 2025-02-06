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

from ... import ddl

__all__ = ("ButlerSqlEngine",)

import dataclasses
from collections.abc import Iterable, Set
from typing import TYPE_CHECKING, Any, cast

import astropy.time
import sqlalchemy

from lsst.daf.relation import ColumnTag, Relation, Sort, UnaryOperation, UnaryOperationRelation, sql

from ..._column_tags import is_timespan_column
from ..._column_type_info import ColumnTypeInfo, LogicalColumn
from ..._timespan import Timespan
from ...timespan_database_representation import TimespanDatabaseRepresentation
from .find_first_dataset import FindFirstDataset

if TYPE_CHECKING:
    from ...name_shrinker import NameShrinker


@dataclasses.dataclass(repr=False, eq=False, kw_only=True)
class ButlerSqlEngine(sql.Engine[LogicalColumn]):
    """An extension of the `lsst.daf.relation.sql.Engine` class to add timespan
    and `FindFirstDataset` operation support.
    """

    column_types: ColumnTypeInfo
    """Struct containing information about column types that depend on registry
    configuration.
    """

    name_shrinker: NameShrinker
    """Object used to shrink the SQL identifiers that represent a ColumnTag to
    fit within the database's limit (`NameShrinker`).
    """

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"ButlerSqlEngine({self.name!r})@{id(self):0x}"

    def _append_unary_to_select(self, operation: UnaryOperation, target: sql.Select) -> sql.Select:
        # Docstring inherited.
        # This override exists to add support for the custom FindFirstDataset
        # operation.
        match operation:
            case FindFirstDataset():
                if target.has_sort and not target.has_slice:
                    # Existing target is sorted, but not sliced.  We want to
                    # move that sort outside (i.e. after) the FindFirstDataset,
                    # since otherwise the FindFirstDataset would put the Sort
                    # into a CTE where it will do nothing.
                    inner = target.reapply_skip(sort=Sort())
                    return sql.Select.apply_skip(operation._finish_apply(inner), sort=target.sort)
                else:
                    # Apply the FindFirstDataset directly to the existing
                    # target, which we've already asserted starts with a
                    # Select.  That existing Select will be used for the CTE
                    # that starts the FindFirstDataset implementation (see
                    # to_payload override).
                    return sql.Select.apply_skip(operation._finish_apply(target))
            case _:
                return super()._append_unary_to_select(operation, target)

    def get_identifier(self, tag: ColumnTag) -> str:
        # Docstring inherited.
        return self.name_shrinker.shrink(super().get_identifier(tag))

    def extract_mapping(
        self, tags: Iterable[ColumnTag], sql_columns: sqlalchemy.sql.ColumnCollection
    ) -> dict[ColumnTag, LogicalColumn]:
        # Docstring inherited.
        # This override exists to add support for Timespan columns.
        result: dict[ColumnTag, LogicalColumn] = {}
        for tag in tags:
            if is_timespan_column(tag):
                result[tag] = self.column_types.timespan_cls.from_columns(
                    sql_columns, name=self.get_identifier(tag)
                )
            else:
                result[tag] = sql_columns[self.get_identifier(tag)]
        return result

    def select_items(
        self,
        items: Iterable[tuple[ColumnTag, LogicalColumn]],
        sql_from: sqlalchemy.sql.FromClause,
        *extra: sqlalchemy.sql.ColumnElement,
    ) -> sqlalchemy.sql.Select:
        # Docstring inherited.
        # This override exists to add support for Timespan columns.
        select_columns: list[sqlalchemy.sql.ColumnElement] = []
        for tag, logical_column in items:
            if is_timespan_column(tag):
                select_columns.extend(
                    cast(TimespanDatabaseRepresentation, logical_column).flatten(
                        name=self.get_identifier(tag)
                    )
                )
            else:
                select_columns.append(
                    cast(sqlalchemy.sql.ColumnElement, logical_column).label(self.get_identifier(tag))
                )
        select_columns.extend(extra)
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).select_from(sql_from)

    def make_zero_select(self, tags: Set[ColumnTag]) -> sqlalchemy.sql.Select:
        # Docstring inherited.
        # This override exists to add support for Timespan columns.
        select_columns: list[sqlalchemy.sql.ColumnElement] = []
        for tag in tags:
            if is_timespan_column(tag):
                select_columns.extend(
                    self.column_types.timespan_cls.fromLiteral(None).flatten(name=self.get_identifier(tag))
                )
            else:
                select_columns.append(sqlalchemy.sql.literal(None).label(self.get_identifier(tag)))
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).where(sqlalchemy.sql.literal(False))

    def convert_column_literal(self, value: Any) -> LogicalColumn:
        # Docstring inherited.
        # This override exists to add support for Timespan columns.
        if isinstance(value, Timespan):
            return self.column_types.timespan_cls.fromLiteral(value)
        elif isinstance(value, astropy.time.Time):
            return sqlalchemy.sql.literal(value, type_=ddl.AstropyTimeNsecTai)
        else:
            return super().convert_column_literal(value)

    def to_payload(self, relation: Relation) -> sql.Payload[LogicalColumn]:
        # Docstring inherited.
        # This override exists to add support for the custom FindFirstDataset
        # operation.
        match relation:
            case UnaryOperationRelation(operation=FindFirstDataset() as operation, target=target):
                # We build a subquery of the form below to search the
                # collections in order.
                #
                # WITH {dst}_search AS (
                #     {target}
                #     ...
                # )
                # SELECT
                #     {dst}_window.*,
                # FROM (
                #     SELECT
                #         {dst}_search.*,
                #         ROW_NUMBER() OVER (
                #             PARTITION BY {dst_search}.{operation.dimensions}
                #             ORDER BY {operation.rank}
                #         ) AS rownum
                #     ) {dst}_window
                # WHERE
                #     {dst}_window.rownum = 1;
                #
                # We'll start with the Common Table Expression (CTE) at the
                # top, which we mostly get from the target relation.
                search = self.to_executable(target).cte(f"{operation.rank.dataset_type}_search")
                # Now we fill out the SELECT from the CTE, and the subquery it
                # contains (at the same time, since they have the same columns,
                # aside from the special 'rownum' window-function column).
                search_columns = self.extract_mapping(target.columns, search.columns)
                partition_by = [
                    _assert_column_is_directly_usable_by_sqlalchemy(search_columns[tag])
                    for tag in operation.dimensions
                ]
                row_number = sqlalchemy.sql.func.row_number()
                rank_column = _assert_column_is_directly_usable_by_sqlalchemy(search_columns[operation.rank])
                if partition_by:
                    rownum_column = row_number.over(partition_by=partition_by, order_by=rank_column)
                else:
                    rownum_column = row_number.over(order_by=rank_column)
                window = self.select_items(
                    search_columns.items(), search, rownum_column.label("rownum")
                ).subquery(f"{operation.rank.dataset_type}_window")
                return sql.Payload(
                    from_clause=window,
                    columns_available=self.extract_mapping(target.columns, window.columns),
                    where=[window.columns["rownum"] == 1],
                )
            case _:
                return super().to_payload(relation)


def _assert_column_is_directly_usable_by_sqlalchemy(column: LogicalColumn) -> sqlalchemy.sql.ColumnElement:
    """Narrow a `LogicalColumn` to a SqlAlchemy ColumnElement, to satisfy the
    typechecker in cases where no timespans are expected.
    """
    if isinstance(column, TimespanDatabaseRepresentation):
        raise TypeError("Timespans not expected here.")

    return column
