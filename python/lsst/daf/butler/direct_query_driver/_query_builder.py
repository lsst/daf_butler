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

__all__ = (
    "QueryBuilder",
    "SingleSelectQueryBuilder",
    "UnionQueryBuilder",
    "UnionQueryBuilderTerm",
)

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Iterable, Set
from types import EllipsisType
from typing import TYPE_CHECKING, ClassVar, Literal, TypeAlias, TypeVar, overload

import sqlalchemy

from ..dimensions import DimensionGroup
from ..queries import tree as qt
from ..registry.interfaces import Database
from ._query_analysis import (
    QueryFindFirstAnalysis,
    QueryJoinsAnalysis,
    QueryTreeAnalysis,
    ResolvedDatasetSearch,
)
from ._sql_builders import SqlColumns, SqlJoinsBuilder, SqlSelectBuilder

if TYPE_CHECKING:
    from ._driver import DirectQueryDriver
    from ._postprocessing import Postprocessing

_T = TypeVar("_T")


class QueryBuilderBase(ABC):
    """A struct that aggregates information about a complete butler query.

    Notes
    -----
    Butler queries are transformed into a combination of SQL and Python-side
    postprocessing in three stages, with each corresponding to an attributes of
    this class and a method of `DirectQueryDriver`

    - In the `joins` stage (`~DirectQueryDriver.apply_query_joins`), we define
      the main SQL FROM and WHERE clauses, by joining all tables needed to
      bring in any columns, or constrain the keys of its rows.

    - In the `projection` stage (`~DirectQueryDriver.apply_query_projection`),
      we select only the columns needed for the query's result rows (including
      columns needed only by postprocessing and ORDER BY, as well those needed
      by the objects returned to users).  If the result rows are not naturally
      unique given what went into the query in the "joins" stage, the
      projection involves a SELECT DISTINCT [ON] or GROUP BY to make them
      unique, and in a few rare cases uses aggregate functions with GROUP BY.

    - In the `find_first` stage (`~DirectQueryDriver.apply_query_find_first`),
      we use a window function (PARTITION BY) subquery to find only the first
      dataset in the collection search path for each data ID.  This stage does
      nothing if there is no find-first dataset search, or if the search is
      trivial because there is only one collection.

    In `DirectQueryDriver.build_query`, a `QueryPlan` instance is constructed
    via `DirectQueryDriver.analyze_query`, which also returns an initial
    `SqlSelectBuilder`.  After this point the plans are considered frozen, and
    the nested plan attributes are then passed to each of the corresponding
    `DirectQueryDriver` methods along with the builder, which is mutated (and
    occasionally replaced) into the complete SQL/postprocessing form of the
    query.

    In queries for datasets of multiple types with the same dimensions (unioned
    together, not joined/intersected), a separate `QueryPlan` is generated for
    each distinct post-filtering collection search path (since the collection
    search path can affect the projection and find-first logic).  Each such
    plan can in turn yield mutiple SELECTs in the final UNION or UNION ALL.
    """

    def __init__(
        self,
        tree_analysis: QueryTreeAnalysis,
        *,
        projection_columns: qt.ColumnSet,
        final_columns: qt.ColumnSet,
    ):
        self.joins_analysis = tree_analysis.joins
        self.postprocessing = tree_analysis.postprocessing
        self.projection_columns = projection_columns
        self.final_columns = final_columns
        self.needs_dimension_distinct = False
        self.find_first_dataset = None

    joins_analysis: QueryJoinsAnalysis
    """Description of the "joins" stage of query construction."""

    projection_columns: qt.ColumnSet
    """The columns present in the query after the projection is applied.

    This is always a subset of `QueryJoinsPlan.columns`.
    """

    needs_dimension_distinct: bool = False
    """If `True`, the projection's dimensions do not include all dimensions in
    the "joins" stage, and hence a SELECT DISTINCT [ON] or GROUP BY must be
    used to make post-projection rows unique.
    """

    find_first_dataset: str | EllipsisType | None = None
    """If not `None`, this is a find-first query for this dataset.

    This is set even if the find-first search is trivial because there is only
    one resolved collection.
    """

    final_columns: qt.ColumnSet
    """The columns included in the SELECT clause of the complete SQL query
    that is actually executed.

    This is a subset of `QueryProjectionPlan.columns` that differs only in
    columns used by the `find_first` stage or an ORDER BY expression.

    Like all other `.queries.tree.ColumnSet` attributes, it does not include
    fields added directly to `SqlSelectBuilder.special`, which may also be
    added to the SELECT clause.
    """

    postprocessing: Postprocessing
    """Struct representing post-query processing in Python, which may require
    additional columns in the query results.
    """

    @abstractmethod
    def analyze_projection(self) -> None:
        # The projection gets interesting if it does not have all of the
        # dimension keys or dataset fields of the "joins" stage, because that
        # means it needs to do a GROUP BY or DISTINCT ON to get unique rows.
        # Subclass implementations handle the check for dataset fields.
        if self.projection_columns.dimensions != self.joins_analysis.columns.dimensions:
            assert self.projection_columns.dimensions.issubset(self.joins_analysis.columns.dimensions)
            # We're going from a larger set of dimensions to a smaller set;
            # that means we'll be doing a SELECT DISTINCT [ON] or GROUP BY.
            self.needs_dimension_distinct = True

    @abstractmethod
    def analyze_find_first(self, find_first_dataset: str | EllipsisType) -> None:
        raise NotImplementedError()

    @abstractmethod
    def apply_joins(self, driver: DirectQueryDriver) -> None:
        raise NotImplementedError()

    @overload
    def finish_select(
        self, return_columns: Literal[True] = True
    ) -> tuple[sqlalchemy.CompoundSelect | sqlalchemy.Select, SqlColumns]: ...

    @overload
    def finish_select(
        self, return_columns: Literal[False]
    ) -> tuple[sqlalchemy.CompoundSelect | sqlalchemy.Select, None]: ...

    @abstractmethod
    def finish_select(
        self, return_columns: bool = True
    ) -> tuple[sqlalchemy.CompoundSelect | sqlalchemy.Select, SqlColumns | None]:
        raise NotImplementedError()

    @abstractmethod
    def finish_nested(self, cte: bool = False) -> SqlSelectBuilder:
        raise NotImplementedError()


class SingleSelectQueryBuilder(QueryBuilderBase):

    def __init__(
        self,
        tree_analysis: QueryTreeAnalysis,
        *,
        projection_columns: qt.ColumnSet,
        final_columns: qt.ColumnSet,
    ) -> None:
        super().__init__(
            tree_analysis=tree_analysis,
            projection_columns=projection_columns,
            final_columns=final_columns,
        )
        assert not tree_analysis.union_datasets, "UnionQueryPlan should be used instead."
        self._select_builder = tree_analysis.initial_select_builder
        self.find_first = None
        self.needs_dataset_distinct = False

    needs_dataset_distinct: bool = False
    """If `True`, the projection columns do not include collection-specific
    dataset fields that were present in the "joins" stage, and hence a SELECT
    DISTINCT [ON] or GROUP BY must be added to make post-projection rows
    unique.
    """

    find_first: QueryFindFirstAnalysis[str] | None = None
    """Description of the "find_first" stage of query construction.

    This attribute is `None` if there is no find-first search at all, and
    `False` in boolean contexts if the search is trivial because there is only
    one collection after the collections have been resolved.
    """

    union_dataset_dimensions: ClassVar[None] = None

    def analyze_projection(self) -> None:
        super().analyze_projection()
        # See if we need to do a DISTINCT [ON] or GROUP BY to get unique rows
        # because we have rows for datasets in multiple collections with the
        # same data ID and dataset type.
        for dataset_type in self.joins_analysis.columns.dataset_fields:
            assert dataset_type is not ..., "Union dataset in non-dataset-union query."
            if not self.projection_columns.dataset_fields[dataset_type]:
                # The "joins"-stage query has one row for each collection for
                # each data ID, but the projection-stage query just wants
                # one row for each data ID.
                if len(self.joins_analysis.datasets[dataset_type].collection_records) > 1:
                    self.needs_dataset_distinct = True
                    break
        # If there are any dataset fields being propagated through the
        # projection and there is more than one collection, we need to include
        # the collection_key column so we can use that as one of the DISTINCT
        # or GROUP BY columns.
        for dataset_type, fields_for_dataset in self.projection_columns.dataset_fields.items():
            assert dataset_type is not ..., "Union dataset in non-dataset-union query."
            if len(self.joins_analysis.datasets[dataset_type].collection_records) > 1:
                fields_for_dataset.add("collection_key")

    def analyze_find_first(self, find_first_dataset: str | EllipsisType) -> None:
        assert find_first_dataset is not ..., "No dataset union in this query"
        self.find_first = QueryFindFirstAnalysis(self.joins_analysis.datasets[find_first_dataset])
        # If we're doing a find-first search and there's a calibration
        # collection in play, we need to make sure the rows coming out of
        # the base query have only one timespan for each data ID +
        # collection, and we can only do that with a GROUP BY and COUNT
        # that we inspect in postprocessing.
        if self.find_first.search.is_calibration_search:
            self.postprocessing.check_validity_match_count = True

    def apply_joins(self, driver: DirectQueryDriver) -> None:
        driver.apply_initial_query_joins(self._select_builder, self.joins_analysis)
        driver.apply_missing_dimension_joins(self._select_builder, self.joins_analysis)

    def apply_projection(self, driver: DirectQueryDriver, order_by: Iterable[qt.OrderExpression]) -> None:
        driver.apply_query_projection(
            self._select_builder,
            self.postprocessing,
            join_datasets=self.joins_analysis.datasets,
            union_datasets=None,
            projection_columns=self.projection_columns,
            needs_dimension_distinct=self.needs_dimension_distinct,
            needs_dataset_distinct=self.needs_dataset_distinct,
            needs_validity_match_count=self.postprocessing.check_validity_match_count,
            find_first_dataset=None if self.find_first is None else self.find_first.search.name,
            order_by=order_by,
        )

    def apply_find_first(self, driver: DirectQueryDriver) -> None:
        if not self.find_first:
            return
        self._select_builder = driver.apply_query_find_first(
            self._select_builder, self.postprocessing, self.find_first
        )

    # The overloads in the base class seem to keep MyPy from recognizing the
    # return type as covariant.
    def finish_select(  # type: ignore
        self,
        return_columns: bool = True,
    ) -> tuple[sqlalchemy.Select, SqlColumns]:
        self._select_builder.columns = self.final_columns
        return self._select_builder.select(self.postprocessing), self._select_builder.joins

    def finish_nested(self, cte: bool = False) -> SqlSelectBuilder:
        self._select_builder.columns = self.final_columns
        return self._select_builder.nested(cte=cte, postprocessing=self.postprocessing)


@dataclasses.dataclass
class UnionQueryBuilderTerm:

    select_builders: list[SqlSelectBuilder]
    """Under-construction SQL queries associated with this plan, to be unioned
    together when complete.
    """

    datasets: ResolvedDatasetSearch[list[str]]
    """Searches for datasets of different types to be joined into the rest of
    the query, with the results (after projection and find-first) unioned
    together.

    The dataset types in a single `QueryUnionTermPlan` have the exact same
    post-filtering collection search path, and hence the exact same query
    plan, aside from the dataset type used to generate their dataset subquery.
    Dataset types that have the same dimensions but do not have the same
    post-filtering collection search path go in different `QueryUnionTermPlan`
    instances, which still contribute to the same UNION [ALL] query.
    Dataset types with different dimensions cannot go in the same SQL query
    at all.
    """

    needs_dataset_distinct: bool = False
    """If `True`, the projection columns do not include collection-specific
    dataset fields that were present in the "joins" stage, and hence a SELECT
    DISTINCT [ON] or GROUP BY must be added to make post-projection rows
    unique.
    """

    needs_validity_match_count: bool = False
    """Whether this query needs a validity match column for postprocessing
    to check.

    This can be `False` even if `Postprocessing.check_validity_match_count` is
    `True`, indicating that some other term in the union needs the column and
    hence this term just needs a dummy column (with "1" as the value).
    """

    find_first: QueryFindFirstAnalysis[list[str]] | None = None
    """Description of the "find_first" stage of query construction.

    This attribute is `None` if there is no find-first search at all, and
    `False` in boolean contexts if the search is trivial because there is only
    one collection after the collections have been resolved.
    """


class UnionQueryBuilder(QueryBuilderBase):

    def __init__(
        self,
        tree_analysis: QueryTreeAnalysis,
        *,
        projection_columns: qt.ColumnSet,
        final_columns: qt.ColumnSet,
        union_dataset_dimensions: DimensionGroup,
    ):
        super().__init__(
            tree_analysis=tree_analysis,
            projection_columns=projection_columns,
            final_columns=final_columns,
        )
        self.initial_select_builder = tree_analysis.initial_select_builder
        self.union_dataset_dimensions = union_dataset_dimensions
        self.union_terms = [
            UnionQueryBuilderTerm(select_builders=[], datasets=datasets)
            for datasets in tree_analysis.union_datasets
        ]

    initial_select_builder: SqlSelectBuilder | None

    union_dataset_dimensions: DimensionGroup

    union_terms: list[UnionQueryBuilderTerm]

    @property
    def db(self) -> Database:
        if self.initial_select_builder is not None:
            return self.initial_select_builder.joins.db
        else:
            return self.union_terms[0].select_builders[0].joins.db

    @property
    def special(self) -> Set[str]:
        if self.initial_select_builder is not None:
            return self.initial_select_builder.joins.special.keys()
        else:
            return self.union_terms[0].select_builders[0].joins.special.keys()

    def analyze_projection(self) -> None:
        super().analyze_projection()
        # See if we need to do a DISTINCT [ON] or GROUP BY to get unique rows
        # because we have rows for datasets in multiple collections with the
        # same data ID and dataset type.
        for dataset_type in self.joins_analysis.columns.dataset_fields:
            if not self.projection_columns.dataset_fields[dataset_type]:
                if dataset_type is ...:
                    for union_term in self.union_terms:
                        if len(union_term.datasets.collection_records) > 1:
                            union_term.needs_dataset_distinct = True
                elif len(self.joins_analysis.datasets[dataset_type].collection_records) > 1:
                    # If a dataset being joined into all union terms has
                    # multiple collections, need_dataset_distinct is true
                    # for all union terms and we can exit the loop early.
                    for union_term in self.union_terms:
                        union_term.needs_dataset_distinct = True
                    break
        # If there are any dataset fields being propagated through the
        # projection and there is more than one collection, we need to include
        # the collection_key column so we can use that as one of the DISTINCT
        # or GROUP BY columns.
        for dataset_type, fields_for_dataset in self.projection_columns.dataset_fields.items():
            if dataset_type is ...:
                for union_term in self.union_terms:
                    # If there is more than one collection for one union term,
                    # we need to add collection_key to all of them to keep the
                    # SELECT columns uniform.
                    if len(union_term.datasets.collection_records) > 1:
                        fields_for_dataset.add("collection_key")
                        break
            elif len(self.joins_analysis.datasets[dataset_type].collection_records) > 1:
                fields_for_dataset.add("collection_key")

    def analyze_find_first(self, find_first_dataset: str | EllipsisType) -> None:
        if find_first_dataset is ...:
            for union_term in self.union_terms:
                union_term.find_first = QueryFindFirstAnalysis(union_term.datasets)
                # If we're doing a find-first search and there's a calibration
                # collection in play, we need to make sure the rows coming out
                # of the base query have only one timespan for each data ID +
                # collection, and we can only do that with a GROUP BY and COUNT
                # that we inspect in postprocessing.
                # Because the postprocessing is applied to the full query, all
                # union terms will need this column, even if only one populates
                # it with a nontrivial value.
                if union_term.find_first.search.is_calibration_search:
                    self.postprocessing.check_validity_match_count = True
                    union_term.needs_validity_match_count = True
        else:
            # The query system machinery should actually be able to handle this
            # case without too much difficulty (we just put the same
            # find_first plan in each union term), but the result doesn't seem
            # like it'd be useful, so it's better not to have to maintain that
            # logic branch.
            raise NotImplementedError(
                f"Additional dataset search {find_first_dataset!r} can only be joined into a "
                "union dataset query as a constraint in data IDs, not as a find-first result."
            )

    def apply_joins(self, driver: DirectQueryDriver) -> None:
        assert self.initial_select_builder is not None
        driver.apply_initial_query_joins(self.initial_select_builder, self.joins_analysis)
        # Join in the union datasets. This makes one copy of the initial
        # select builder for each dataset type, and hence from here on we have
        # to repeat whatever we do to all select builders.
        for union_term in self.union_terms:
            for dataset_type_name in union_term.datasets.name:
                select_builder = self.initial_select_builder.copy()
                driver.join_dataset_search(
                    select_builder.joins,
                    union_term.datasets,
                    self.joins_analysis.columns.dataset_fields[...],
                    union_dataset_type_name=dataset_type_name,
                )
                union_term.select_builders.append(select_builder)
        self.initial_select_builder = None
        for union_term in self.union_terms:
            for select_builder in union_term.select_builders:
                driver.apply_missing_dimension_joins(select_builder, self.joins_analysis)

    def apply_projection(self, driver: DirectQueryDriver, order_by: Iterable[qt.OrderExpression]) -> None:
        for union_term in self.union_terms:
            for builder in union_term.select_builders:
                driver.apply_query_projection(
                    builder,
                    self.postprocessing,
                    join_datasets=self.joins_analysis.datasets,
                    union_datasets=union_term.datasets,
                    projection_columns=self.projection_columns,
                    needs_dimension_distinct=self.needs_dimension_distinct,
                    needs_dataset_distinct=union_term.needs_dataset_distinct,
                    needs_validity_match_count=union_term.needs_validity_match_count,
                    find_first_dataset=None if union_term.find_first is None else ...,
                    order_by=order_by,
                )

    def apply_find_first(self, driver: DirectQueryDriver) -> None:
        for union_term in self.union_terms:
            if not union_term.find_first:
                continue
            union_term.select_builders = [
                driver.apply_query_find_first(builder, self.postprocessing, union_term.find_first)
                for builder in union_term.select_builders
            ]

    @overload
    def finish_select(
        self, return_columns: Literal[True] = True
    ) -> tuple[sqlalchemy.CompoundSelect | sqlalchemy.Select, SqlColumns]: ...

    @overload
    def finish_select(
        self, return_columns: Literal[False]
    ) -> tuple[sqlalchemy.CompoundSelect | sqlalchemy.Select, None]: ...

    def finish_select(
        self, return_columns: bool = True
    ) -> tuple[sqlalchemy.CompoundSelect | sqlalchemy.Select, SqlColumns | None]:
        terms: list[sqlalchemy.Select] = []
        for union_term in self.union_terms:
            for select_builder in union_term.select_builders:
                select_builder.columns = self.final_columns
                terms.append(select_builder.select(self.postprocessing))
        sql: sqlalchemy.Select | sqlalchemy.CompoundSelect = (
            sqlalchemy.union_all(*terms) if len(terms) > 1 else terms[0]
        )
        columns: SqlColumns | None = None
        if return_columns:
            columns = SqlColumns(
                db=self.db,
            )
            columns.extract_columns(
                self.final_columns,
                self.postprocessing,
                self.special,
                column_collection=sql.selected_columns,
            )
        return sql, columns

    def finish_nested(self, cte: bool = False) -> SqlSelectBuilder:
        sql_select, _ = self.finish_select(return_columns=False)
        from_clause = sql_select.cte() if cte else sql_select.subquery()
        joins_builder = SqlJoinsBuilder(
            db=self.db,
            from_clause=from_clause,
        ).extract_columns(self.final_columns, self.postprocessing)
        return SqlSelectBuilder(joins_builder, columns=self.final_columns)


QueryBuilder: TypeAlias = SingleSelectQueryBuilder | UnionQueryBuilder
