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
from typing import TYPE_CHECKING, Any, ClassVar, Literal

import sqlalchemy

from .. import ddl
from .._utilities.nonempty_mapping import NonemptyMapping
from ..queries import tree as qt
from ..registry.nameShrinker import NameShrinker
from ._postprocessing import Postprocessing

if TYPE_CHECKING:
    from ..registry.interfaces import Database
    from ..timespan_database_representation import TimespanDatabaseRepresentation


@dataclasses.dataclass
class _BaseSqlBuilder:
    db: Database

    dimensions_provided: NonemptyMapping[str, list[sqlalchemy.ColumnElement]] = dataclasses.field(
        default_factory=lambda: NonemptyMapping(list), kw_only=True
    )

    fields_provided: NonemptyMapping[str, dict[str, sqlalchemy.ColumnElement[Any]]] = dataclasses.field(
        default_factory=lambda: NonemptyMapping(dict), kw_only=True
    )

    timespans_provided: dict[str, TimespanDatabaseRepresentation] = dataclasses.field(
        default_factory=dict, kw_only=True
    )

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

    def select(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
        *,
        sql_columns: Iterable[sqlalchemy.ColumnElement] = (),
        distinct: bool = False,
        required_dimensions_only: bool = False,
    ) -> sqlalchemy.Select:
        raise NotImplementedError()

    def make_table_spec(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
    ) -> ddl.TableSpec:
        # Note that we don't need to use a NameShrinker here because that will
        # happen inside the Database object when it's given a TableSpec.
        results = ddl.TableSpec(
            [columns.get_column_spec(logical_table, field).to_sql_spec() for logical_table, field in columns]
        )
        if postprocessing:
            for element in postprocessing.iter_missing(columns):
                results.fields.add(
                    ddl.FieldSpec.for_region(columns.get_qualified_name(element.name, "region"))
                )
        return results


@dataclasses.dataclass
class EmptySqlBuilder(_BaseSqlBuilder):
    def join(self, other: SqlBuilder) -> SqlBuilder:
        return other

    def select(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
        *,
        sql_columns: Iterable[sqlalchemy.ColumnElement] = (),
        distinct: bool = False,
        required_dimensions_only: bool = False,
    ) -> sqlalchemy.Select:
        assert not columns
        assert not postprocessing
        return sqlalchemy.select(*self.handle_empty_columns([]), *sql_columns)

    def where_sql(self, *args: sqlalchemy.ColumnElement[bool]) -> EmptySqlBuilder:
        assert not args, "Empty FROM clause implies empty WHERE clause."
        return self


@dataclasses.dataclass
class SqlBuilder(_BaseSqlBuilder):
    sql_from_clause: sqlalchemy.FromClause
    sql_where_terms: list[sqlalchemy.ColumnElement[bool]] = dataclasses.field(default_factory=list)

    def extract_dimensions(self, dimensions: Iterable[str], **kwargs: str) -> SqlBuilder:
        for dimension_name in dimensions:
            self.dimensions_provided[dimension_name].append(self.sql_from_clause.columns[dimension_name])
        for k, v in kwargs.items():
            self.dimensions_provided[v].append(self.sql_from_clause.columns[k])
        return self

    def extract_columns(self, columns: qt.ColumnSet, required_dimensions_only: bool = False) -> SqlBuilder:
        name_shrinker = NameShrinker(self.db.dialect.max_identifier_length)
        for logical_table, field in columns:
            name = name_shrinker.shrink(columns.get_qualified_name(logical_table, field))
            if field is None:
                if logical_table in columns.dimensions.required or not required_dimensions_only:
                    self.dimensions_provided[logical_table].append(self.sql_from_clause.columns[name])
            elif columns.is_timespan(logical_table, field):
                self.timespans_provided[logical_table] = self.db.getTimespanRepresentation().from_columns(
                    self.sql_from_clause.columns, name
                )
            else:
                self.fields_provided[logical_table][field] = self.sql_from_clause.columns[name]
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
        self.sql_where_terms += other.sql_where_terms
        return self

    def select(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
        *,
        sql_columns: Iterable[sqlalchemy.ColumnElement] = (),
        distinct: bool = False,
        required_dimensions_only: bool = False,
    ) -> sqlalchemy.Select:
        name_shrinker = NameShrinker(self.db.dialect.max_identifier_length)
        if distinct and columns:
            # Hard case: caller wants unique rows, and we have to tell the
            # database to do that because they aren't naturally unique.
            # That means delegating to Database.select_unique (since the
            # implementation is driver-dependent due differences in DISTINCT ON
            # and GROUP BY support).
            if sql_columns:
                # Just because it's a pain to implement and we don't have a
                # need for it right now.
                raise NotImplementedError("'distinct' and 'sql_columns' are mutually exclusive")
            column_triples: list[
                tuple[str, sqlalchemy.ColumnElement[Any], Literal["key", "natural", "aggregate"]]
            ] = []
            uniqueness_category: Literal["key", "natural", "aggregate"]
            for logical_table, field in columns:
                name = name_shrinker.shrink(columns.get_qualified_name(logical_table, field))
                uniqueness_category = columns.get_uniqueness_category(logical_table, field)
                if field is None:
                    if logical_table in columns.dimensions.required or not required_dimensions_only:
                        column_triples.append(
                            (name, self.dimensions_provided[logical_table][0], uniqueness_category)
                        )
                elif columns.is_timespan(logical_table, field):
                    column_triples.extend(
                        zip(
                            self.timespans_provided[logical_table].getFieldNames(name),
                            self.timespans_provided[logical_table].flatten(),
                            itertools.repeat(uniqueness_category),
                        )
                    )
                else:
                    column_triples.append(
                        (name, self.fields_provided[logical_table][field], uniqueness_category)
                    )
            if postprocessing is not None:
                for element in postprocessing.iter_missing(columns):
                    name = name_shrinker.shrink(columns.get_qualified_name(element.name, "region"))
                    sql_region_column = self.fields_provided[element.name]["region"]
                    if element.name not in columns.dimensions.elements:
                        sql_region_column = ddl.Base64Region.union_agg(sql_region_column)
                        uniqueness_category = "aggregate"
                    else:
                        uniqueness_category = "natural"
                    column_triples.append((name, sql_region_column, uniqueness_category))
            result = self.db.select_unique(self.sql_from_clause, column_triples)
        else:
            # Easy case with no DISTINCT [ON] or GROUP BY.
            sql_columns = list(sql_columns)
            for logical_table, field in columns:
                name = name_shrinker.shrink(columns.get_qualified_name(logical_table, field))
                if field is None:
                    sql_columns.append(self.dimensions_provided[logical_table][0].label(name))
                elif columns.is_timespan(logical_table, field):
                    sql_columns.extend(self.timespans_provided[logical_table].flatten(name))
                else:
                    sql_columns.append(self.fields_provided[logical_table][field].label(name))
            if postprocessing is not None:
                for element in postprocessing.iter_missing(columns):
                    sql_columns.append(
                        self.fields_provided[element.name]["region"].label(
                            name_shrinker.shrink(columns.get_qualified_name(element.name, "region"))
                        )
                    )
            self.handle_empty_columns(sql_columns)
            result = sqlalchemy.select(*sql_columns).select_from(self.sql_from_clause)
            if distinct:
                result = result.distinct()
        if self.sql_where_terms:
            result = result.where(*self.sql_where_terms)
        return result

    def where_sql(self, *arg: sqlalchemy.ColumnElement[bool]) -> SqlBuilder:
        self.sql_where_terms.extend(arg)
        return self

    def subquery(
        self,
        columns: qt.ColumnSet,
        distinct: bool = False,
        required_dimensions_only: bool = False,
        chain: Iterable[SqlBuilder] = (),
    ) -> SqlBuilder:
        other_selects = [
            other_sql_builder.select(
                columns, distinct=False, required_dimensions_only=required_dimensions_only
            )
            for other_sql_builder in chain
        ]
        select = self.select(
            columns,
            distinct=distinct and not other_selects,
            required_dimensions_only=required_dimensions_only,
        )
        if other_selects:
            if distinct:
                subquery = select.union(*other_selects).subquery()
            else:
                subquery = select.union_all(*other_selects).subquery()
        else:
            subquery = select.subquery()
        return SqlBuilder(self.db, subquery).extract_columns(columns)
