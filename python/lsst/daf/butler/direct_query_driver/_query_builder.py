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

__all__ = ("QueryJoiner", "QueryBuilder")

import dataclasses
import itertools
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

import sqlalchemy

from .. import ddl
from ..nonempty_mapping import NonemptyMapping
from ..queries import tree as qt
from ._postprocessing import Postprocessing

if TYPE_CHECKING:
    from ..registry.interfaces import Database
    from ..timespan_database_representation import TimespanDatabaseRepresentation


@dataclasses.dataclass
class QueryBuilder:
    """A struct used to represent an under-construction SQL SELECT query.

    This object's methods frequently "consume" ``self``, by either returning
    it after modification or returning related copy that may share state with
    the original.  Users should be careful never to use consumed instances, and
    are recommended to reuse the same variable name to make that hard to do
    accidentally.
    """

    joiner: QueryJoiner
    """Struct representing the SQL FROM and WHERE clauses, as well as the
    columns *available* to the query (but not necessarily in the SELECT
    clause).
    """

    columns: qt.ColumnSet
    """Columns to include the SELECT clause.

    This does not include columns required only by `postprocessing` and columns
    in `QueryJoiner.special`, which are also always included in the SELECT
    clause.
    """

    postprocessing: Postprocessing = dataclasses.field(default_factory=Postprocessing)
    """Postprocessing that will be needed in Python after the SQL query has
    been executed.
    """

    distinct: bool | Sequence[sqlalchemy.ColumnElement[Any]] = ()
    """A representation of a DISTINCT or DISTINCT ON clause.

    If `True`, this represents a SELECT DISTINCT.  If a non-empty sequence,
    this represents a SELECT DISTINCT ON.  If `False` or an empty sequence,
    there is no DISTINCT clause.
    """

    group_by: Sequence[sqlalchemy.ColumnElement[Any]] = ()
    """A representation of a GROUP BY clause.

    If not-empty, a GROUP BY clause with these columns is added.  This
    generally requires that every `sqlalchemy.ColumnElement` held in the nested
    `joiner` that is part of `columns` must either be part of `group_by` or
    hold an aggregate function.
    """

    EMPTY_COLUMNS_NAME: ClassVar[str] = "IGNORED"
    """Name of the column added to a SQL SELECT clause in order to construct
    queries that have no real columns.
    """

    EMPTY_COLUMNS_TYPE: ClassVar[type] = sqlalchemy.Boolean
    """Type of the column added to a SQL SELECT clause in order to construct
    queries that have no real columns.
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

    def select(self) -> sqlalchemy.Select:
        """Transform this builder into a SQLAlchemy representation of a SELECT
        query.

        Returns
        -------
        select : `sqlalchemy.Select`
            SQLAlchemy SELECT statement.
        """
        assert not (self.distinct and self.group_by), "At most one of distinct and group_by can be set."
        sql_columns: list[sqlalchemy.ColumnElement[Any]] = []
        for logical_table, field in self.columns:
            name = self.columns.get_qualified_name(logical_table, field)
            if field is None:
                sql_columns.append(self.joiner.dimension_keys[logical_table][0].label(name))
            else:
                name = self.joiner.db.name_shrinker.shrink(name)
                if self.columns.is_timespan(logical_table, field):
                    sql_columns.extend(self.joiner.timespans[logical_table].flatten(name))
                else:
                    sql_columns.append(self.joiner.fields[logical_table][field].label(name))
        if self.postprocessing is not None:
            for element in self.postprocessing.iter_missing(self.columns):
                sql_columns.append(
                    self.joiner.fields[element.name]["region"].label(
                        self.joiner.db.name_shrinker.shrink(
                            self.columns.get_qualified_name(element.name, "region")
                        )
                    )
                )
        for label, sql_column in self.joiner.special.items():
            sql_columns.append(sql_column.label(label))
        self.handle_empty_columns(sql_columns)
        result = sqlalchemy.select(*sql_columns)
        if self.joiner.from_clause is not None:
            result = result.select_from(self.joiner.from_clause)
        if self.distinct is True:
            result = result.distinct()
        elif self.distinct:
            result = result.distinct(*self.distinct)
        if self.group_by:
            result = result.group_by(*self.group_by)
        if self.joiner.where_terms:
            result = result.where(*self.joiner.where_terms)
        return result

    def join(self, other: QueryJoiner) -> QueryBuilder:
        """Join tables, subqueries, and WHERE clauses from another query into
        this one, in place.

        Parameters
        ----------
        other : `QueryJoiner`
            Object holding the FROM and WHERE clauses to add to this one.
            JOIN ON clauses are generated via the dimension keys in common.

        Returns
        -------
        self : `QueryBuilder`
            This `QueryBuilder` instance (never a copy); returned to enable
            method-chaining.
        """
        self.joiner.join(other)
        return self

    def to_joiner(self, cte: bool = False, force: bool = False) -> QueryJoiner:
        """Convert this builder into a `QueryJoiner`, nesting it in a subquery
        or common table expression only if needed to apply DISTINCT or GROUP BY
        clauses.

        This method consumes ``self``.

        Parameters
        ----------
        cte : `bool`, optional
            If `True`, nest via a common table expression instead of a
            subquery.
        force : `bool`, optional
            If `True`, nest via a subquery or common table expression even if
            there is no DISTINCT or GROUP BY.

        Returns
        -------
        joiner : `QueryJoiner`
            QueryJoiner` with at least all columns in `columns` available.
            This may or may not be the `joiner` attribute of this object.
        """
        if force or self.distinct or self.group_by:
            sql_from_clause = self.select().cte() if cte else self.select().subquery()
            return QueryJoiner(self.joiner.db, sql_from_clause).extract_columns(
                self.columns, self.postprocessing, special=self.joiner.special.keys()
            )
        return self.joiner

    def nested(self, cte: bool = False, force: bool = False) -> QueryBuilder:
        """Convert this builder into a `QueryBuiler` that is guaranteed to have
        no DISTINCT or GROUP BY, nesting it in a subquery or common table
        expression only if needed to apply any current DISTINCT or GROUP BY
        clauses.

        This method consumes ``self``.

        Parameters
        ----------
        cte : `bool`, optional
            If `True`, nest via a common table expression instead of a
            subquery.
        force : `bool`, optional
            If `True`, nest via a subquery or common table expression even if
            there is no DISTINCT or GROUP BY.

        Returns
        -------
        builder : `QueryBuilder`
            `QueryBuilder` with at least all columns in `columns` available.
            This may or may not be the `builder` attribute of this object.
        """
        return QueryBuilder(
            self.to_joiner(cte=cte, force=force), columns=self.columns, postprocessing=self.postprocessing
        )

    def union_subquery(
        self,
        others: Iterable[QueryBuilder],
    ) -> QueryJoiner:
        """Combine this builder with others to make a SELECT UNION subquery.

        Parameters
        ----------
        others : `~collections.abc.Iterable` [ `QueryBuilder` ]
            Other query builders to union with.  Their `columns` attributes
            must be the same as those of ``self``.

        Returns
        -------
        joiner : `QueryJoiner`
            `QueryJoiner` with at least all columns in `columns` available.
            This may or may not be the `joiner` attribute of this object.
        """
        select0 = self.select()
        other_selects = [other.select() for other in others]
        return QueryJoiner(
            self.joiner.db,
            from_clause=select0.union(*other_selects).subquery(),
        ).extract_columns(self.columns, self.postprocessing)

    def make_table_spec(self) -> ddl.TableSpec:
        """Make a specification that can be used to create a table to store
        this query's outputs.

        Returns
        -------
        spec : `.ddl.TableSpec`
            Table specification for this query's result columns (including
            those from `postprocessing` and `QueryJoiner.special`).
        """
        assert not self.joiner.special, "special columns not supported in make_table_spec"
        results = ddl.TableSpec(
            [
                self.columns.get_column_spec(logical_table, field).to_sql_spec(
                    name_shrinker=self.joiner.db.name_shrinker
                )
                for logical_table, field in self.columns
            ]
        )
        if self.postprocessing:
            for element in self.postprocessing.iter_missing(self.columns):
                results.fields.add(
                    ddl.FieldSpec.for_region(
                        self.joiner.db.name_shrinker.shrink(
                            self.columns.get_qualified_name(element.name, "region")
                        )
                    )
                )
        if not results.fields:
            results.fields.add(ddl.FieldSpec(name=self.EMPTY_COLUMNS_NAME, dtype=self.EMPTY_COLUMNS_TYPE))
        return results


@dataclasses.dataclass
class QueryJoiner:
    """A struct used to represent the FROM and WHERE clauses of an
    under-construction SQL SELECT query.

    This object's methods frequently "consume" ``self``, by either returning
    it after modification or returning related copy that may share state with
    the original.  Users should be careful never to use consumed instances, and
    are recommended to reuse the same variable name to make that hard to do
    accidentally.
    """

    db: Database
    """Object that abstracts over the database engine."""

    from_clause: sqlalchemy.FromClause | None = None
    """SQLAlchemy representation of the FROM clause.

    This is initialized to `None` but in almost all cases is immediately
    replaced.
    """

    where_terms: list[sqlalchemy.ColumnElement[bool]] = dataclasses.field(default_factory=list)
    """Sequence of WHERE clause terms to be combined with AND."""

    dimension_keys: NonemptyMapping[str, list[sqlalchemy.ColumnElement]] = dataclasses.field(
        default_factory=lambda: NonemptyMapping(list)
    )
    """Mapping of dimension keys included in the FROM clause.

    Nested lists correspond to different tables that have the same dimension
    key (which should all have equal values for all result rows).
    """

    fields: NonemptyMapping[str, dict[str, sqlalchemy.ColumnElement[Any]]] = dataclasses.field(
        default_factory=lambda: NonemptyMapping(dict)
    )
    """Mapping of columns that are neither dimension keys nor timespans.

    Inner and outer keys correspond to the "logical table" and "field" pairs
    that result from iterating over `~.queries.tree.ColumnSet`, with the former
    either a dimension element name or dataset type name.
    """

    timespans: dict[str, TimespanDatabaseRepresentation] = dataclasses.field(default_factory=dict)
    """Mapping of timespan columns.

    Keys are "logical tables" - dimension element names or dataset type names.
    """

    special: dict[str, sqlalchemy.ColumnElement[Any]] = dataclasses.field(default_factory=dict)
    """Special columns that are available from the FROM clause and
    automatically included in the SELECT clause when this joiner is nested
    within a `QueryBuilder`.

    These columns are not part of the dimension universe and are not associated
    with a dataset.  They are never returned to users, even if they may be
    included in raw SQL results.
    """

    def extract_dimensions(self, dimensions: Iterable[str], **kwargs: str) -> QueryJoiner:
        """Add dimension key columns from `from_clause` into `dimension_keys`.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` [ `str` ]
            Names of dimensions to include, assuming that their names in
            `sql_columns` are just the dimension names.
        **kwargs : `str`
            Additional dimensions to include, with the names in `sql_columns`
            as keys and the actual dimension names as values.

        Returns
        -------
        self : `QueryJoiner`
            This `QueryJoiner` instance (never a copy). Provided to enable
            method chaining.
        """
        assert self.from_clause is not None, "Cannot extract columns with no FROM clause."
        for dimension_name in dimensions:
            self.dimension_keys[dimension_name].append(self.from_clause.columns[dimension_name])
        for k, v in kwargs.items():
            self.dimension_keys[v].append(self.from_clause.columns[k])
        return self

    def extract_columns(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
        special: Iterable[str] = (),
    ) -> QueryJoiner:
        """Add columns from `from_clause` into `dimension_keys`.

        Parameters
        ----------
        columns : `.queries.tree.ColumnSet`
            Columns to include, assuming that
            `.queries.tree.ColumnSet.get_qualified_name` corresponds to the
            name used in `sql_columns` (after name shrinking).
        postprocessing : `Postprocessing`, optional
            Postprocessing object whose needed columns should also be included.
        special : `~collections.abc.Iterable` [ `str` ], optional
            Additional special columns to extract.

        Returns
        -------
        self : `QueryJoiner`
            This `QueryJoiner` instance (never a copy). Provided to enable
            method chaining.
        """
        assert self.from_clause is not None, "Cannot extract columns with no FROM clause."
        for logical_table, field in columns:
            name = columns.get_qualified_name(logical_table, field)
            if field is None:
                self.dimension_keys[logical_table].append(self.from_clause.columns[name])
            else:
                name = self.db.name_shrinker.shrink(name)
                if columns.is_timespan(logical_table, field):
                    self.timespans[logical_table] = self.db.getTimespanRepresentation().from_columns(
                        self.from_clause.columns, name
                    )
                else:
                    self.fields[logical_table][field] = self.from_clause.columns[name]
        if postprocessing is not None:
            for element in postprocessing.iter_missing(columns):
                self.fields[element.name]["region"] = self.from_clause.columns[
                    self.db.name_shrinker.shrink(columns.get_qualified_name(element.name, "region"))
                ]
            if postprocessing.check_validity_match_count:
                self.special[postprocessing.VALIDITY_MATCH_COUNT] = self.from_clause.columns[
                    postprocessing.VALIDITY_MATCH_COUNT
                ]
        for name in special:
            self.special[name] = self.from_clause.columns[name]
        return self

    def join(self, other: QueryJoiner) -> QueryJoiner:
        """Combine this `QueryJoiner` with another via an INNER JOIN on
        dimension keys.

        This method consumes ``self``.

        Parameters
        ----------
        other : `QueryJoiner`
            Other joiner to combine with this one.

        Returns
        -------
        joined : `QueryJoiner`
            A `QueryJoiner` with all columns present in either operand, with
            its `from_clause` representing a SQL INNER JOIN where the dimension
            key columns common to both operands are constrained to be equal.
            If either operand does not have `from_clause`, the other's is used.
            The `where_terms` of the two operands are concatenated,
            representing a logical AND (with no attempt at deduplication).
        """
        join_on: list[sqlalchemy.ColumnElement] = []
        for dimension_name in other.dimension_keys.keys():
            if dimension_name in self.dimension_keys:
                for column1, column2 in itertools.product(
                    self.dimension_keys[dimension_name], other.dimension_keys[dimension_name]
                ):
                    join_on.append(column1 == column2)
            self.dimension_keys[dimension_name].extend(other.dimension_keys[dimension_name])
        if self.from_clause is None:
            self.from_clause = other.from_clause
        elif other.from_clause is not None:
            join_on_sql: sqlalchemy.ColumnElement[bool]
            match len(join_on):
                case 0:
                    join_on_sql = sqlalchemy.true()
                case 1:
                    (join_on_sql,) = join_on
                case _:
                    join_on_sql = sqlalchemy.and_(*join_on)
            self.from_clause = self.from_clause.join(other.from_clause, onclause=join_on_sql)
        for logical_table, fields in other.fields.items():
            self.fields[logical_table].update(fields)
        self.timespans.update(other.timespans)
        self.special.update(other.special)
        self.where_terms += other.where_terms
        return self

    def where(self, *args: sqlalchemy.ColumnElement[bool]) -> QueryJoiner:
        """Add a WHERE clause term.

        Parameters
        ----------
        *args : `sqlalchemy.ColumnElement`
            SQL boolean column expressions to be combined with AND.

        Returns
        -------
        self : `QueryJoiner`
            This `QueryJoiner` instance (never a copy). Provided to enable
            method chaining.
        """
        self.where_terms.extend(args)
        return self

    def to_builder(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
        distinct: bool | Sequence[sqlalchemy.ColumnElement[Any]] = (),
        group_by: Sequence[sqlalchemy.ColumnElement[Any]] = (),
    ) -> QueryBuilder:
        """Convert this joiner into a `QueryBuilder` by providing SELECT clause
        columns and optional DISTINCT or GROUP BY clauses.

        This method consumes ``self``.

        Parameters
        ----------
        columns : `~.queries.tree.ColumnSet`
            Regular columns to include in the SELECT clause.
        postprocessing : `Postprocessing`, optional
            Addition processing to be performed on result rows after executing
            the SQL query.
        distinct : `bool` or `~collections.abc.Sequence` [ \
                `sqlalchemy.ColumnElement` ], optional
            Specification of the DISTINCT clause (see `QueryBuilder.distinct`).
        group_by : `~collections.abc.Sequence` [ \
                `sqlalchemy.ColumnElement` ], optional
            Specification of the GROUP BY clause (see `QueryBuilder.group_by`).

        Returns
        -------
        builder : `QueryBuilder`
            New query builder.
        """
        return QueryBuilder(
            self,
            columns,
            postprocessing=postprocessing if postprocessing is not None else Postprocessing(),
            distinct=distinct,
            group_by=group_by,
        )
