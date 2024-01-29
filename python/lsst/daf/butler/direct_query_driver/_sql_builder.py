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

__all__ = ("SqlBuilder",)

import dataclasses
import itertools
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

import sqlalchemy

from .. import ddl
from .._utilities.nonempty_mapping import NonemptyMapping
from ..queries import tree as qt
from ._postprocessing import Postprocessing

if TYPE_CHECKING:
    from ..registry.interfaces import Database
    from ..timespan_database_representation import TimespanDatabaseRepresentation


@dataclasses.dataclass
class SqlBuilder:
    db: Database
    sql_from_clause: sqlalchemy.FromClause | None = None
    sql_where_terms: list[sqlalchemy.ColumnElement[bool]] = dataclasses.field(default_factory=list)
    needs_distinct: bool = False

    dimension_keys: NonemptyMapping[str, list[sqlalchemy.ColumnElement]] = dataclasses.field(
        default_factory=lambda: NonemptyMapping(list)
    )

    fields: NonemptyMapping[str, dict[str, sqlalchemy.ColumnElement[Any]]] = dataclasses.field(
        default_factory=lambda: NonemptyMapping(dict)
    )

    timespans: dict[str, TimespanDatabaseRepresentation] = dataclasses.field(default_factory=dict)

    special: dict[str, sqlalchemy.ColumnElement[Any]] = dataclasses.field(default_factory=dict)

    EMPTY_COLUMNS_NAME: ClassVar[str] = "IGNORED"
    """Name of the column added to a SQL ``SELECT`` query in order to represent
    relations that have no real columns.
    """

    EMPTY_COLUMNS_TYPE: ClassVar[type] = sqlalchemy.Boolean
    """Type of the column added to a SQL ``SELECT`` query in order to represent
    relations that have no real columns.
    """

    @property
    def sql_columns(self) -> sqlalchemy.ColumnCollection:
        assert self.sql_from_clause is not None
        return self.sql_from_clause.columns

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

    def select(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
        *,
        distinct: bool | Sequence[sqlalchemy.ColumnElement[Any]] = False,
        group_by: Sequence[sqlalchemy.ColumnElement] = (),
    ) -> sqlalchemy.Select:
        sql_columns: list[sqlalchemy.ColumnElement[Any]] = []
        for logical_table, field in columns:
            name = columns.get_qualified_name(logical_table, field)
            if field is None:
                sql_columns.append(self.dimension_keys[logical_table][0].label(name))
            elif columns.is_timespan(logical_table, field):
                sql_columns.extend(self.timespans[logical_table].flatten(name))
            else:
                sql_columns.append(self.fields[logical_table][field].label(name))
        if postprocessing is not None:
            for element in postprocessing.iter_missing(columns):
                assert (
                    element.name in columns.dimensions.elements
                ), "Region aggregates not handled by this method."
                sql_columns.append(
                    self.fields[element.name]["region"].label(
                        columns.get_qualified_name(element.name, "region")
                    )
                )
        for label, sql_column in self.special.items():
            sql_columns.append(sql_column.label(label))
        self.handle_empty_columns(sql_columns)
        result = sqlalchemy.select(*sql_columns)
        if self.sql_from_clause is not None:
            result = result.select_from(self.sql_from_clause)
        if self.needs_distinct or distinct:
            if distinct is True or distinct is False:
                result = result.distinct()
            else:
                result = result.distinct(*distinct)
        if group_by:
            result = result.group_by(*group_by)
        if self.sql_where_terms:
            result = result.where(*self.sql_where_terms)
        return result

    def make_table_spec(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
    ) -> ddl.TableSpec:
        assert not self.special, "special columns not supported in make_table_spec"
        results = ddl.TableSpec(
            [columns.get_column_spec(logical_table, field).to_sql_spec() for logical_table, field in columns]
        )
        if postprocessing:
            for element in postprocessing.iter_missing(columns):
                results.fields.add(
                    ddl.FieldSpec.for_region(columns.get_qualified_name(element.name, "region"))
                )
        return results

    def extract_dimensions(self, dimensions: Iterable[str], **kwargs: str) -> SqlBuilder:
        assert self.sql_from_clause is not None, "Cannot extract columns with no FROM clause."
        for dimension_name in dimensions:
            self.dimension_keys[dimension_name].append(self.sql_from_clause.columns[dimension_name])
        for k, v in kwargs.items():
            self.dimension_keys[v].append(self.sql_from_clause.columns[k])
        return self

    def extract_columns(
        self, columns: qt.ColumnSet, postprocessing: Postprocessing | None = None
    ) -> SqlBuilder:
        assert self.sql_from_clause is not None, "Cannot extract columns with no FROM clause."
        for logical_table, field in columns:
            name = columns.get_qualified_name(logical_table, field)
            if field is None:
                self.dimension_keys[logical_table].append(self.sql_from_clause.columns[name])
            elif columns.is_timespan(logical_table, field):
                self.timespans[logical_table] = self.db.getTimespanRepresentation().from_columns(
                    self.sql_from_clause.columns, name
                )
            else:
                self.fields[logical_table][field] = self.sql_from_clause.columns[name]
        if postprocessing is not None:
            for element in postprocessing.iter_missing(columns):
                self.fields[element.name]["region"] = self.sql_from_clause.columns[name]
            if postprocessing.check_validity_match_count:
                self.special[postprocessing.VALIDITY_MATCH_COUNT] = self.sql_from_clause.columns[
                    postprocessing.VALIDITY_MATCH_COUNT
                ]
        return self

    def join(self, other: SqlBuilder) -> SqlBuilder:
        join_on: list[sqlalchemy.ColumnElement] = []
        for dimension_name in self.dimension_keys.keys() & other.dimension_keys.keys():
            for column1, column2 in itertools.product(
                self.dimension_keys[dimension_name], other.dimension_keys[dimension_name]
            ):
                join_on.append(column1 == column2)
            self.dimension_keys[dimension_name].extend(other.dimension_keys[dimension_name])
        if self.sql_from_clause is None:
            self.sql_from_clause = other.sql_from_clause
        elif other.sql_from_clause is not None:
            self.sql_from_clause = self.sql_from_clause.join(
                other.sql_from_clause, onclause=sqlalchemy.and_(*join_on)
            )
        self.sql_where_terms += other.sql_where_terms
        self.needs_distinct = self.needs_distinct or other.needs_distinct
        self.special.update(other.special)
        return self

    def where_sql(self, *arg: sqlalchemy.ColumnElement[bool]) -> SqlBuilder:
        self.sql_where_terms.extend(arg)
        return self

    def cte(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
        *,
        distinct: bool | Sequence[sqlalchemy.ColumnElement[Any]] = False,
        group_by: Sequence[sqlalchemy.ColumnElement] = (),
    ) -> SqlBuilder:
        return SqlBuilder(
            self.db,
            self.select(columns, postprocessing, distinct=distinct, group_by=group_by).cte(),
        ).extract_columns(columns, postprocessing)

    def subquery(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
        *,
        distinct: bool | Sequence[sqlalchemy.ColumnElement[Any]] = False,
        group_by: Sequence[sqlalchemy.ColumnElement] = (),
    ) -> SqlBuilder:
        return SqlBuilder(
            self.db,
            self.select(columns, postprocessing, distinct=distinct, group_by=group_by).subquery(),
        ).extract_columns(columns, postprocessing)

    def union_subquery(
        self,
        others: Iterable[SqlBuilder],
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
    ) -> SqlBuilder:
        select0 = self.select(columns, postprocessing)
        other_selects = [other.select(columns, postprocessing) for other in others]
        return SqlBuilder(
            self.db,
            select0.union(*other_selects).subquery(),
        ).extract_columns(columns, postprocessing)
