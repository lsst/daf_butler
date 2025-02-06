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

__all__ = ("SqlColumns", "SqlJoinsBuilder", "SqlSelectBuilder", "make_table_spec")

import dataclasses
import itertools
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Self

import sqlalchemy

from .. import ddl
from ..dimensions import DimensionGroup
from ..dimensions._group import SortedSequenceSet
from ..nonempty_mapping import NonemptyMapping
from ..queries import tree as qt
from ._postprocessing import Postprocessing

if TYPE_CHECKING:
    from ..registry.interfaces import Database
    from ..timespan_database_representation import TimespanDatabaseRepresentation


@dataclasses.dataclass
class SqlSelectBuilder:
    """A struct used to represent an under-construction SQL SELECT query.

    This object's methods frequently "consume" ``self``, by either returning
    it after modification or returning related copy that may share state with
    the original.  Users should be careful never to use consumed instances, and
    are recommended to reuse the same variable name to make that hard to do
    accidentally.
    """

    joins: SqlJoinsBuilder
    """Struct representing the SQL FROM and WHERE clauses, as well as the
    columns *available* to the query (but not necessarily in the SELECT
    clause).
    """

    columns: qt.ColumnSet
    """Columns to include the SELECT clause.

    This does not include columns required only by `Postprocessing` and columns
    in `SqlJoinsBuilder.special`, which are also always included in the SELECT
    clause.
    """

    distinct: bool | tuple[sqlalchemy.ColumnElement[Any], ...] = ()
    """A representation of a DISTINCT or DISTINCT ON clause.

    If `True`, this represents a SELECT DISTINCT.  If a non-empty sequence,
    this represents a SELECT DISTINCT ON.  If `False` or an empty sequence,
    there is no DISTINCT clause.
    """

    group_by: tuple[sqlalchemy.ColumnElement[Any], ...] = ()
    """A representation of a GROUP BY clause.

    If not-empty, a GROUP BY clause with these columns is added.  This
    generally requires that every `sqlalchemy.ColumnElement` held in the nested
    `joins` builder that is part of `columns` must either be part of `group_by`
    or hold an aggregate function.
    """

    EMPTY_COLUMNS_NAME: ClassVar[str] = "IGNORED"
    """Name of the column added to a SQL SELECT clause in order to construct
    queries that have no real columns.
    """

    EMPTY_COLUMNS_TYPE: ClassVar[type] = sqlalchemy.Boolean
    """Type of the column added to a SQL SELECT clause in order to construct
    queries that have no real columns.
    """

    def copy(self) -> SqlSelectBuilder:
        """Return a copy that can be safely mutated without affecting the
        original.
        """
        return dataclasses.replace(self, joins=self.joins.copy(), columns=self.columns.copy())

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

    def select(self, postprocessing: Postprocessing | None) -> sqlalchemy.Select:
        """Transform this builder into a SQLAlchemy representation of a SELECT
        query.

        Parameters
        ----------
        postprocessing : `Postprocessing`
            Struct representing post-query processing in Python, which may
            require additional columns in the query results.

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
                assert logical_table is not qt.ANY_DATASET
                sql_columns.append(self.joins.dimension_keys[logical_table][0].label(name))
            else:
                name = self.joins.db.name_shrinker.shrink(name)
                if self.columns.is_timespan(logical_table, field):
                    sql_columns.extend(self.joins.timespans[logical_table].flatten(name))
                else:
                    sql_columns.append(self.joins.fields[logical_table][field].label(name))
        if postprocessing is not None:
            for element in postprocessing.iter_missing(self.columns):
                sql_columns.append(
                    self.joins.fields[element.name]["region"].label(
                        self.joins.db.name_shrinker.shrink(
                            self.columns.get_qualified_name(element.name, "region")
                        )
                    )
                )
        for label, sql_column in self.joins.special.items():
            sql_columns.append(sql_column.label(label))
        self.handle_empty_columns(sql_columns)
        result = sqlalchemy.select(*sql_columns)
        if self.joins.from_clause is not None:
            result = result.select_from(self.joins.from_clause)
        if self.distinct is True:
            result = result.distinct()
        elif self.distinct:
            result = result.distinct(*self.distinct)
        if self.group_by:
            result = result.group_by(*self.group_by)
        if self.joins.where_terms:
            result = result.where(*self.joins.where_terms)
        return result

    def join(self, other: SqlJoinsBuilder) -> SqlSelectBuilder:
        """Join tables, subqueries, and WHERE clauses from another query into
        this one, in place.

        Parameters
        ----------
        other : `SqlJoinsBuilder`
            Object holding the FROM and WHERE clauses to add to this one.
            JOIN ON clauses are generated via the dimension keys in common.

        Returns
        -------
        self : `SqlSelectBuilder`
            This `SqlSelectBuilder` instance (never a copy); returned to enable
            method-chaining.
        """
        self.joins.join(other)
        return self

    def into_joins_builder(
        self, cte: bool = False, force: bool = False, *, postprocessing: Postprocessing | None
    ) -> SqlJoinsBuilder:
        """Convert this builder into a `SqlJoinsBuilder`, nesting it in a
        subquery or common table expression only if needed to apply DISTINCT or
        GROUP BY clauses.

        This method consumes ``self``.

        Parameters
        ----------
        cte : `bool`, optional
            If `True`, nest via a common table expression instead of a
            subquery.
        force : `bool`, optional
            If `True`, nest via a subquery or common table expression even if
            there is no DISTINCT or GROUP BY.
        postprocessing : `Postprocessing`
            Struct representing post-query processing in Python, which may
            require additional columns in the query results.

        Returns
        -------
        joins_builder : `SqlJoinsBuilder`
            SqlJoinsBuilder` with at least all columns in `columns` available.
            This may or may not be the `joins` attribute of this object.
        """
        if force or self.distinct or self.group_by:
            sql_from_clause = (
                self.select(postprocessing).cte() if cte else self.select(postprocessing).subquery()
            )
            return SqlJoinsBuilder(db=self.joins.db, from_clause=sql_from_clause).extract_columns(
                self.columns, postprocessing, special=self.joins.special.keys()
            )
        return self.joins

    def nested(
        self, cte: bool = False, force: bool = False, *, postprocessing: Postprocessing | None
    ) -> SqlSelectBuilder:
        """Convert this builder into a `SqlSelectBuilder` that is guaranteed to
        have no DISTINCT or GROUP BY, nesting it in a subquery or common table
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
        postprocessing : `Postprocessing`
            Struct representing post-query processing in Python, which may
            require additional columns in the query results.

        Returns
        -------
        builder : `SqlSelectBuilder`
            `SqlSelectBuilder` with at least all columns in `columns`
            available.  This may or may not be the `builder` attribute of this
            object.
        """
        return SqlSelectBuilder(
            self.into_joins_builder(cte=cte, force=force, postprocessing=postprocessing), columns=self.columns
        )

    def union_subquery(
        self, others: Iterable[SqlSelectBuilder], postprocessing: Postprocessing | None = None
    ) -> SqlJoinsBuilder:
        """Combine this builder with others to make a SELECT UNION subquery.

        Parameters
        ----------
        others : `~collections.abc.Iterable` [ `SqlSelectBuilder` ]
            Other query builders to union with.  Their `columns` attributes
            must be the same as those of ``self``.
        postprocessing : `Postprocessing`
            Struct representing post-query processing in Python, which may
            require additional columns in the query results.

        Returns
        -------
        joins_builder : `SqlJoinsBuilder`
            `SqlJoinsBuilder` with at least all columns in `columns` available.
            This may or may not be the `joins` attribute of this object.
        """
        select0 = self.select(postprocessing)
        other_selects = [other.select(postprocessing) for other in others]
        return SqlJoinsBuilder(
            db=self.joins.db,
            from_clause=select0.union(*other_selects).subquery(),
        ).extract_columns(self.columns, postprocessing)


@dataclasses.dataclass(kw_only=True)
class SqlColumns:
    """A struct that holds SQLAlchemy columns objects for a query, categorized
    by type.

    This class mostly serves as a base class for `SqlJoinsBuilder`, but unlike
    `SqlJoinsBuilder` it is capable of representing columns in a compound
    SELECT (i.e. UNION or UNION ALL) clause, not just a FROM clause.
    """

    db: Database
    """Object that abstracts over the database engine."""

    dimension_keys: NonemptyMapping[str, list[sqlalchemy.ColumnElement]] = dataclasses.field(
        default_factory=lambda: NonemptyMapping(list)
    )
    """Mapping of dimension keys included in the FROM clause.

    Nested lists correspond to different tables that have the same dimension
    key (which should all have equal values for all result rows).
    """

    fields: NonemptyMapping[str | qt.AnyDatasetType, dict[str, sqlalchemy.ColumnElement[Any]]] = (
        dataclasses.field(default_factory=lambda: NonemptyMapping(dict))
    )
    """Mapping of columns that are neither dimension keys nor timespans.

    Inner and outer keys correspond to the "logical table" and "field" pairs
    that result from iterating over `~.queries.tree.ColumnSet`, with the former
    either a dimension element name or dataset type name.
    """

    timespans: dict[str | qt.AnyDatasetType, TimespanDatabaseRepresentation] = dataclasses.field(
        default_factory=dict
    )
    """Mapping of timespan columns.

    Keys are "logical tables" - dimension element names or dataset type names.
    """

    special: dict[str, sqlalchemy.ColumnElement[Any]] = dataclasses.field(default_factory=dict)
    """Special columns that are available from the FROM clause and
    automatically included in the SELECT clause when this join builder is
    nested within a `SqlSelectBuilder`.

    These columns are not part of the dimension universe and are not associated
    with a dataset.  They are never returned to users, even if they may be
    included in raw SQL results.
    """

    def extract_dimensions(
        self, dimensions: Iterable[str], *, column_collection: sqlalchemy.ColumnCollection, **kwargs: str
    ) -> Self:
        """Add dimension key columns from `from_clause` into `dimension_keys`.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` [ `str` ]
            Names of dimensions to include, assuming that their names in
            `sql_columns` are just the dimension names.
        column_collection : `sqlalchemy.ColumnCollection`
            SQLAlchemy column collection to extract from.

        **kwargs : `str`
            Additional dimensions to include, with the names in `sql_columns`
            as keys and the actual dimension names as values.

        Returns
        -------
        self : `QueryColumns`
            This `QueryColumns` instance (never a copy). Provided to enable
            method chaining.
        """
        for dimension_name in dimensions:
            self.dimension_keys[dimension_name].append(column_collection[dimension_name])
        for k, v in kwargs.items():
            self.dimension_keys[v].append(column_collection[k])
        return self

    def extract_columns(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
        special: Iterable[str] = (),
        *,
        column_collection: sqlalchemy.ColumnCollection,
    ) -> Self:
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
        column_collection : `sqlalchemy.ColumnCollection`
            SQLAlchemy column collection to extract from.

        Returns
        -------
        self : `QueryColumns`
            This `QueryColumns` instance (never a copy). Provided to enable
            method chaining.
        """
        for logical_table, field in columns:
            name = columns.get_qualified_name(logical_table, field)
            if field is None:
                assert logical_table is not qt.ANY_DATASET
                self.dimension_keys[logical_table].append(column_collection[name])
            else:
                name = self.db.name_shrinker.shrink(name)
                if columns.is_timespan(logical_table, field):
                    self.timespans[logical_table] = self.db.getTimespanRepresentation().from_columns(
                        column_collection, name
                    )
                else:
                    self.fields[logical_table][field] = column_collection[name]
        if postprocessing is not None:
            for element in postprocessing.iter_missing(columns):
                self.fields[element.name]["region"] = column_collection[
                    self.db.name_shrinker.shrink(columns.get_qualified_name(element.name, "region"))
                ]
            for name in postprocessing.spatial_expression_filtering:
                self.special[name] = column_collection[name]
            if postprocessing.check_validity_match_count:
                self.special[postprocessing.VALIDITY_MATCH_COUNT] = column_collection[
                    postprocessing.VALIDITY_MATCH_COUNT
                ]
        for name in special:
            self.special[name] = column_collection[name]
        return self


@dataclasses.dataclass(kw_only=True)
class SqlJoinsBuilder(SqlColumns):
    """A struct used to represent the FROM and WHERE clauses of an
    under-construction SQL SELECT query.

    This object's methods frequently "consume" ``self``, by either returning
    it after modification or returning related copy that may share state with
    the original.  Users should be careful never to use consumed instances, and
    are recommended to reuse the same variable name to make that hard to do
    accidentally.
    """

    from_clause: sqlalchemy.FromClause | None = None
    """SQLAlchemy representation of the FROM clause.

    This is initialized to `None` but in almost all cases is immediately
    replaced.
    """

    where_terms: list[sqlalchemy.ColumnElement[bool]] = dataclasses.field(default_factory=list)
    """Sequence of WHERE clause terms to be combined with AND."""

    def copy(self) -> SqlJoinsBuilder:
        """Return a copy that can be safely mutated without affecting the
        original.
        """
        return dataclasses.replace(
            self,
            where_terms=self.where_terms.copy(),
            dimension_keys=self.dimension_keys.copy(),
            fields=self.fields.copy(),
            timespans=self.timespans.copy(),
            special=self.special.copy(),
        )

    def extract_dimensions(
        self,
        dimensions: Iterable[str],
        *,
        column_collection: sqlalchemy.ColumnCollection | None = None,
        **kwargs: str,
    ) -> Self:
        """Add dimension key columns from `from_clause` into `dimension_keys`.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` [ `str` ]
            Names of dimensions to include, assuming that their names in
            `sql_columns` are just the dimension names.
        column_collection : `sqlalchemy.ColumnCollection`, optional
            SQLAlchemy column collection to extract from.  Defaults to
            ``self.from_clause.columns``.
        **kwargs : `str`
            Additional dimensions to include, with the names in `sql_columns`
            as keys and the actual dimension names as values.

        Returns
        -------
        self : `SqlJoinsBuilder`
            This `SqlJoinsBuilder` instance (never a copy). Provided to enable
            method chaining.
        """
        if column_collection is None:
            assert self.from_clause is not None, "Cannot extract columns with no FROM clause."
            column_collection = self.from_clause.columns
        return super().extract_dimensions(dimensions, column_collection=column_collection, **kwargs)

    def extract_columns(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing | None = None,
        special: Iterable[str] = (),
        *,
        column_collection: sqlalchemy.ColumnCollection | None = None,
    ) -> Self:
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
        column_collection : `sqlalchemy.ColumnCollection`, optional
            SQLAlchemy column collection to extract from.  Defaults to
            ``self.from_clause.columns``.

        Returns
        -------
        self : `SqlJoinsBuilder`
            This `SqlJoinsBuilder` instance (never a copy). Provided to enable
            method chaining.
        """
        if column_collection is None:
            assert self.from_clause is not None, "Cannot extract columns with no FROM clause."
            column_collection = self.from_clause.columns
        return super().extract_columns(columns, postprocessing, special, column_collection=column_collection)

    def join(self, other: SqlJoinsBuilder) -> SqlJoinsBuilder:
        """Combine this `SqlJoinsBuilder` with another via an INNER JOIN on
        dimension keys.

        This method consumes ``self``.

        Parameters
        ----------
        other : `SqlJoinsBuilder`
            Other join builder to combine with this one.

        Returns
        -------
        joined : `SqlJoinsBuilder`
            A `SqlJoinsBuilder` with all columns present in either operand,
            with its `from_clause` representing a SQL INNER JOIN where the
            dimension key columns common to both operands are constrained to be
            equal.  If either operand does not have `from_clause`, the other's
            is used.  The `where_terms` of the two operands are concatenated,
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

    def where(self, *args: sqlalchemy.ColumnElement[bool]) -> SqlJoinsBuilder:
        """Add a WHERE clause term.

        Parameters
        ----------
        *args : `sqlalchemy.ColumnElement`
            SQL boolean column expressions to be combined with AND.

        Returns
        -------
        self : `SqlJoinsBuilder`
            This `SqlJoinsBuilder` instance (never a copy). Provided to enable
            method chaining.
        """
        self.where_terms.extend(args)
        return self

    def to_select_builder(
        self,
        columns: qt.ColumnSet,
        distinct: bool | Sequence[sqlalchemy.ColumnElement[Any]] = (),
        group_by: Sequence[sqlalchemy.ColumnElement[Any]] = (),
    ) -> SqlSelectBuilder:
        """Convert this join builder into a `SqlSelectBuilder` by providing
        SELECT clause columns and optional DISTINCT or GROUP BY clauses.

        This method consumes ``self``.

        Parameters
        ----------
        columns : `~.queries.tree.ColumnSet`
            Regular columns to include in the SELECT clause.
        distinct : `bool` or `~collections.abc.Sequence` [ \
                `sqlalchemy.ColumnElement` ], optional
            Specification of the DISTINCT clause (see
            `SqlSelectBuilder.distinct`).
        group_by : `~collections.abc.Sequence` [ \
                `sqlalchemy.ColumnElement` ], optional
            Specification of the GROUP BY clause (see
            `SqlSelectBuilder.group_by`).

        Returns
        -------
        builder : `SqlSelectBuilder`
            New query builder.
        """
        return SqlSelectBuilder(
            self,
            columns,
            distinct=distinct if type(distinct) is bool else tuple(distinct),
            group_by=tuple(group_by),
        )


def make_table_spec(
    columns: qt.ColumnSet, db: Database, postprocessing: Postprocessing | None, *, make_indices: bool = False
) -> ddl.TableSpec:
    """Make a specification that can be used to create a table to store
    this query's outputs.

    Parameters
    ----------
    columns : `lsst.daf.butler.queries.tree.ColumnSet`
        Columns to include in the table.
    db : `Database`
        Database engine and connection abstraction.
    postprocessing : `Postprocessing`
        Struct representing post-query processing in Python, which may
        require additional columns in the query results.
    make_indices : `bool`, optional
        If `True` add indices for groups of columns.

    Returns
    -------
    spec : `.ddl.TableSpec`
        Table specification for this query's result columns (including
        those from `postprocessing` and `SqlJoinsBuilder.special`).
    """
    indices = _make_table_indices(columns.dimensions) if make_indices else []
    results = ddl.TableSpec(
        [
            columns.get_column_spec(logical_table, field).to_sql_spec(name_shrinker=db.name_shrinker)
            for logical_table, field in columns
        ],
        indexes=indices,
    )
    if postprocessing:
        for element in postprocessing.iter_missing(columns):
            results.fields.add(
                ddl.FieldSpec.for_region(
                    db.name_shrinker.shrink(columns.get_qualified_name(element.name, "region"))
                )
            )
        for name in postprocessing.spatial_expression_filtering:
            results.fields.add(ddl.FieldSpec(name, dtype=sqlalchemy.types.LargeBinary, nullable=True))
    if not results.fields:
        results.fields.add(
            ddl.FieldSpec(name=SqlSelectBuilder.EMPTY_COLUMNS_NAME, dtype=SqlSelectBuilder.EMPTY_COLUMNS_TYPE)
        )
    return results


def _make_table_indices(dimensions: DimensionGroup) -> list[ddl.IndexSpec]:
    index_columns: list[SortedSequenceSet] = []
    for dimension in dimensions.required:
        minimal_group = dimensions.universe[dimension].minimal_group.required

        for idx in range(len(index_columns)):
            if index_columns[idx] <= minimal_group:
                index_columns[idx] = minimal_group
                break
        else:
            index_columns.append(minimal_group)

    return [ddl.IndexSpec(*columns) for columns in index_columns]
