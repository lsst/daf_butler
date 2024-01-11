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

__all__ = ("SqlBuilder", "EmptySqlBuilder")

import dataclasses
import itertools
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, ClassVar

import sqlalchemy

from ..queries import relation_tree as rt
from ._postprocessing import Postprocessing

if TYPE_CHECKING:
    from ..timespan_database_representation import TimespanDatabaseRepresentation


@dataclasses.dataclass
class _BaseSqlBuilder:
    dimensions_provided: dict[str, list[sqlalchemy.ColumnElement]] = dataclasses.field(
        default_factory=dict, kw_only=True
    )

    fields_provided: dict[
        rt.DimensionFieldReference | rt.DatasetFieldReference, sqlalchemy.ColumnElement
    ] = dataclasses.field(default_factory=dict, kw_only=True)

    timespans_provided: dict[
        rt.DimensionFieldReference | rt.DatasetFieldReference, TimespanDatabaseRepresentation
    ] = dataclasses.field(default_factory=dict, kw_only=True)

    EMPTY_COLUMNS_NAME: ClassVar[str] = "IGNORED"
    """Name of the column added to a SQL ``SELECT`` query in order to represent
    relations that have no real columns.
    """

    EMPTY_COLUMNS_TYPE: ClassVar[type] = sqlalchemy.Boolean
    """Type of the column added to a SQL ``SELECT`` query in order to represent
    relations that have no real columns.
    """

    @classmethod
    def handle_empty_columns(
        cls, columns: list[sqlalchemy.sql.ColumnElement]
    ) -> list[sqlalchemy.ColumnElement]:
        """Handle the edge case where a SELECT statement has no columns, by
        adding a literal column that should be ignored.

        Parameters
        ----------
        columns : `list` [ `sqlalchemy.ColumnElement` ]
            List of SQLAlchemy column objects.  This may have no elements when
            this method is called, and will always have at least one element
            when it returns.

        Returns
        -------
        columns : `list` [ `sqlalchemy.ColumnElement` ]
            The same list that was passed in, after any modification.
        """
        if not columns:
            columns.append(sqlalchemy.sql.literal(True).label(cls.EMPTY_COLUMNS_NAME))
        return columns


@dataclasses.dataclass
class EmptySqlBuilder(_BaseSqlBuilder):
    def join(self, other: SqlBuilder) -> SqlBuilder:
        return other

    def sql_select(
        self,
        columns_to_select: Iterable[rt.ColumnReference],
        postprocessing: Postprocessing,
        *,
        sql_columns_to_select: Iterable[sqlalchemy.ColumnElement] = (),
        order_by: Iterable[sqlalchemy.ColumnElement[Any]] = (),
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[sqlalchemy.Select, Postprocessing]:
        assert not columns_to_select
        assert not postprocessing
        assert not order_by
        result = sqlalchemy.select(*self.handle_empty_columns([]), *sql_columns_to_select)
        if offset > 0 or limit == 0:
            result = result.where(sqlalchemy.literal(False))
        return result, postprocessing


@dataclasses.dataclass
class SqlBuilder(_BaseSqlBuilder):
    sql_from_clause: sqlalchemy.FromClause
    sql_where_terms: list[sqlalchemy.ColumnElement[bool]] = dataclasses.field(default_factory=list)

    def extract_keys(self, dimensions: Iterable[str], **kwargs: str) -> SqlBuilder:
        for dimension_name in dimensions:
            self.dimensions_provided[dimension_name] = [self.sql_from_clause.columns[dimension_name]]
        for k, v in kwargs.items():
            self.dimensions_provided[v] = [self.sql_from_clause.columns[k]]
        return self

    def extract_columns(
        self, fields: Iterable[rt.ColumnReference], timespan_db_repr: type[TimespanDatabaseRepresentation]
    ) -> SqlBuilder:
        for col_ref in fields:
            if col_ref.expression_type == "dimension_key":
                self.dimensions_provided[col_ref.dimension.name].append(
                    self.sql_from_clause.columns[col_ref.qualified_name]
                )
            elif col_ref.column_type == "timespan":
                self.timespans_provided[col_ref] = timespan_db_repr.from_columns(
                    self.sql_from_clause.columns, col_ref.qualified_name
                )
            else:
                self.fields_provided[col_ref] = self.sql_from_clause.columns[col_ref.qualified_name]
        return self

    def join(self, other: SqlBuilder) -> SqlBuilder:
        join_on: list[sqlalchemy.ColumnElement] = []
        for dimension_name in self.dimensions_provided.keys() & other.dimensions_provided.keys():
            for column1, column2 in itertools.product(
                self.dimensions_provided[dimension_name], other.dimensions_provided[dimension_name]
            ):
                join_on.append(column1 == column2)
            self.dimensions_provided[dimension_name].extend(other.dimensions_provided[dimension_name])
        self.sql_from_clause = self.sql_from_clause.join(
            other.sql_from_clause, onclause=sqlalchemy.and_(*join_on)
        )
        return self

    def sql_select(
        self,
        columns_to_select: Iterable[rt.ColumnReference],
        postprocessing: Postprocessing,
        *,
        sql_columns_to_select: Iterable[sqlalchemy.ColumnElement] = (),
        order_by: Iterable[sqlalchemy.ColumnElement[Any]] = (),
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[sqlalchemy.Select, Postprocessing]:
        # Build the list of columns for the SELECT clause itself.
        sql_columns: list[sqlalchemy.ColumnElement] = []
        # TODO: sort this list so nothing is dependent on set iteration order.
        columns_to_select = list(set(columns_to_select) | postprocessing.gather_columns_required())
        for col_ref in columns_to_select:
            if col_ref.expression_type == "dimension_key":
                sql_columns.append(
                    self.dimensions_provided[col_ref.dimension.name][0].label(col_ref.dimension.name)
                )
            elif col_ref.column_type == "timespan":
                sql_columns.extend(self.timespans_provided[col_ref].flatten())
            else:
                sql_columns.append(self.fields_provided[col_ref])
        sql_columns.extend(sql_columns_to_select)
        self.handle_empty_columns(sql_columns)
        result = sqlalchemy.select(*sql_columns).select_from(self.sql_from_clause)
        if self.sql_where_terms:
            result = result.where(*self.sql_where_terms)
        # Add ORDER BY, LIMIT, and OFFSET clauses as appropriate.
        if order_by:
            result = result.order_by(*order_by)
        if not postprocessing:
            if offset:
                result = result.offset(offset)
            if limit is not None:
                result = result.limit(limit)
        else:
            raise NotImplementedError("TODO")
        return result, postprocessing

    def where_sql(self, *arg: sqlalchemy.ColumnElement[bool]) -> SqlBuilder:
        self.sql_where_terms.extend(arg)
        return self
