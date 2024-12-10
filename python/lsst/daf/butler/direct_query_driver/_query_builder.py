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
import itertools
from abc import ABC, abstractmethod
from collections.abc import Iterable, Set
from typing import TYPE_CHECKING, Literal, TypeVar, overload

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


class QueryBuilder(ABC):
    """An abstract base class for objects that transform query descriptions
    into SQL and `Postprocessing`.

    See `DirectQueryDriver.build_query` for an overview of query construction,
    including the role this class plays in it.

    Parameters
    ----------
    tree_analysis : `QueryTreeAnalysis`
        Result of initial analysis of the most of the query description.
        considered consumed because nested attributes will be referenced and
        may be modified in-place in the future.
    projection_columns : `.queries.tree.ColumnSet`
        Columns to include in the query's "projection" stage, where a GROUP BY
        or DISTINCT may be performed.
    final_columns : `.queries.tree.ColumnSet`
        Columns to include in the final query.
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

    This is always a subset of `QueryJoinsAnalysis.columns`.
    """

    needs_dimension_distinct: bool = False
    """If `True`, the projection's dimensions do not include all dimensions in
    the "joins" stage, and hence a SELECT DISTINCT [ON] or GROUP BY must be
    used to make post-projection rows unique.
    """

    find_first_dataset: str | qt.AnyDatasetType | None = None
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
        """Analyze the "projection" stage of query construction, in which the
        query may be nested in a GROUP BY or DISTINCT subquery in order to
        ensure rows do not have duplicates.

        This modifies the builder in place, and should be called immediately
        after construction.

        Notes
        -----
        Implementations should delegate to `super` to set
        `needs_dimension_distinct`, but generally need to provide additional
        logic to determine whether a GROUP BY or DISTINCT will be needed for
        other reasons (e.g. duplication due to dataset searches over multiple
        collections).
        """
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
    def analyze_find_first(self, find_first_dataset: str | qt.AnyDatasetType) -> None:
        """Analyze the "find first" stage of query construction, in  which a
        Common Table Expression with PARTITION ON may be used to find the first
        dataset for each data ID and dataset type in an ordered collection
        sequence.

        This modifies the builder in place, and should be called immediately
        after `analyze_projection`.

        Parameters
        ----------
        find_first_dataset : `str` or ``...``
            Name of the dataset type that needs a find-first search.  ``...``
            is used to indicate the dataset types in a union dataset query.
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_joins(self, driver: DirectQueryDriver) -> None:
        """Translate the "joins" stage of the query to SQL.

        This modifies the builder in place.  It is the first step in the
        "apply" phase, and should be called after `analyze_find_first` finishes
        the "analysis" phase (if more than analysis is needed).

        Parameters
        ----------
        driver : `DirectQueryDriver`
            Driver that invoked this builder and may be called back into for
            lower-level SQL generation operations.
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_projection(self, driver: DirectQueryDriver, order_by: Iterable[qt.OrderExpression]) -> None:
        """Translate the "projection" stage of the query to SQL.

        This modifies the builder in place.  It is the second step in the
        "apply" phase, after `apply_joins`.

        Parameters
        ----------
        driver : `DirectQueryDriver`
            Driver that invoked this builder and may be called back into for
            lower-level SQL generation operations.
        order_by : `~collections.abc.Iterable` [ \
                `.queries.tree.OrderExpression` ]
            Column expression used to order the query rows.
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_find_first(self, driver: DirectQueryDriver) -> None:
        """Transform the "find first" stage of the query to SQL.

        This modifies the builder in place.  It is the third and final step in
        the "apply" phase, after "apply_projection".

        Parameters
        ----------
        driver : `DirectQueryDriver`
            Driver that invoked this builder and may be called back into for
            lower-level SQL generation operations.
        """
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
        """Finish translating the query into executable SQL.

        Parameters
        ----------
        return_columns : `bool`
            If `True`, return a structure that organizes the SQLAlchemy
            column objects available to the query.

        Returns
        -------
        sql_select : `sqlalchemy.Select` or `sqlalchemy.CompoundSelect`.
            A SELECT [UNION ALL] SQL query.
        sql_columns : `SqlColumns` or `None`
            The columns available to the query (including any available to
            an ORDER BY clause, not just those in the SELECT clause, in
            contexts where those are not the same.  May be `None` (but is not
            guaranteed to be) if ``return_columns=False``.
        """
        raise NotImplementedError()

    @abstractmethod
    def finish_nested(self, cte: bool = False) -> SqlSelectBuilder:
        """Finish translating the query into SQL that can be used as a
        subquery.

        Parameters
        ----------
        cte : `bool`, optional
            If `True`, nest the query in a common table expression (i.e. SQL
            WITH statement) instead of a subquery.

        Returns
        -------
        select_builder : `SqlSelectBuilder`
            A builder object that maps to a single SELECT statement.  This may
            directly hold the original query with no subquery or CTE if that
            query was a single SELECT with no GROUP BY or DISTINCT; in either
            case it is guaranteed that modifying this builder's result columns
            and transforming it into a SELECT will not change the number of
            rows.
        """
        raise NotImplementedError()


class SingleSelectQueryBuilder(QueryBuilder):
    """An implementation of `QueryBuilder` for queries that are structured as
    a single SELECT (i.e. not a union).

    See `DirectQueryDriver.build_query` for an overview of query construction,
    including the role this class plays in it.  This builder is used for most
    butler queries, for which `.queries.tree.QueryTree.any_dataset` is `None`.

    Parameters
    ----------
    tree_analysis : `QueryTreeAnalysis`
        Result of initial analysis of the most of the query description.
        considered consumed because nested attributes will be referenced and
        may be modified in-place in the future.
    projection_columns : `.queries.tree.ColumnSet`
        Columns to include in the query's "projection" stage, where a GROUP BY
        or DISTINCT may be performed.
    final_columns : `.queries.tree.ColumnSet`
        Columns to include in the final query.
    """

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

    def analyze_projection(self) -> None:
        # Docstring inherited.
        super().analyze_projection()
        # See if we need to do a DISTINCT [ON] or GROUP BY to get unique rows
        # because we have rows for datasets in multiple collections with the
        # same data ID and dataset type.
        for dataset_type in self.joins_analysis.columns.dataset_fields:
            assert dataset_type is not qt.ANY_DATASET, "Union dataset in non-dataset-union query."
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
            assert dataset_type is not qt.ANY_DATASET, "Union dataset in non-dataset-union query."
            if len(self.joins_analysis.datasets[dataset_type].collection_records) > 1:
                fields_for_dataset.add("collection_key")

    def analyze_find_first(self, find_first_dataset: str | qt.AnyDatasetType) -> None:
        # Docstring inherited.
        assert find_first_dataset is not qt.ANY_DATASET, "No dataset union in this query"
        self.find_first = QueryFindFirstAnalysis(self.joins_analysis.datasets[find_first_dataset])
        # If we're doing a find-first search and there's a calibration
        # collection in play, we need to make sure the rows coming out of
        # the base query have only one timespan for each data ID +
        # collection, and we can only do that with a GROUP BY and COUNT
        # that we inspect in postprocessing.
        if self.find_first.search.is_calibration_search:
            self.postprocessing.check_validity_match_count = True

    def apply_joins(self, driver: DirectQueryDriver) -> None:
        # Docstring inherited.
        driver.apply_initial_query_joins(
            self._select_builder, self.joins_analysis, union_dataset_dimensions=None
        )
        driver.apply_missing_dimension_joins(self._select_builder, self.joins_analysis)

    def apply_projection(self, driver: DirectQueryDriver, order_by: Iterable[qt.OrderExpression]) -> None:
        # Docstring inherited.
        driver.project_spatial_join_filtering(
            self.projection_columns, self.postprocessing, [self._select_builder]
        )
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
        # Docstring inherited.
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
        # Docstring inherited.
        self._select_builder.columns = self.final_columns
        return self._select_builder.select(self.postprocessing), self._select_builder.joins

    def finish_nested(self, cte: bool = False) -> SqlSelectBuilder:
        # Docstring inherited.
        self._select_builder.columns = self.final_columns
        return self._select_builder.nested(cte=cte, postprocessing=self.postprocessing)


@dataclasses.dataclass
class UnionQueryBuilderTerm:
    """A helper struct that holds state for `UnionQueryBuilder` that
    corresponds to a set of dataset types with the same post-filtering
    collection sequence.
    """

    select_builders: list[SqlSelectBuilder]
    """Under-construction SQL queries associated with this plan, to be unioned
    together when complete.

    Each term corresponds to a different dataset type and a single SELECT; note
    that this means a `UnionQueryBuilderTerm` does not map 1-1 with a SELECT in
    the final UNION - it maps to a set of extremely similar SELECTs that differ
    only in the dataset type name injected into each SELECT at the end.
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


class UnionQueryBuilder(QueryBuilder):
    """An implementation of `QueryBuilder` for queries that are structured as
    a UNION ALL with one SELECT for each dataset type.

    See `DirectQueryDriver.build_query` for an overview of query construction,
    including the role this class plays in it.  This builder is used
    special butler queries where `.queries.tree.QueryTree.any_dataset` is not
    `None`.

    Parameters
    ----------
    tree_analysis : `QueryTreeAnalysis`
        Result of initial analysis of the most of the query description.
        considered consumed because nested attributes will be referenced and
        may be modified in-place in the future.
    projection_columns : `.queries.tree.ColumnSet`
        Columns to include in the query's "projection" stage, where a GROUP BY
        or DISTINCT may be performed.
    final_columns : `.queries.tree.ColumnSet`
        Columns to include in the final query.
    union_dataset_dimensions : `DimensionGroup`
        Dimensions of the dataset types that comprise the union.

    Notes
    -----
    `UnionQueryBuilder` can be in one of two states:

    - During the "analysis" phase and at the beginning of the "apply" phase,
      it has a single initial `SqlSelectBuilder`, because all union terms are
      identical at this stage.  The `UnionQueryTerm.builder` lists are empty.
    - Within `apply_joins`, this single `SqlSelectBuilder` is copied to
      populate the per-dataset type `SqlSelectBuilder` instances in the
      `UnionQueryTerm.builders` lists.
    """

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
        self._initial_select_builder: SqlSelectBuilder | None = tree_analysis.initial_select_builder
        self.union_dataset_dimensions = union_dataset_dimensions
        self.union_terms = [
            UnionQueryBuilderTerm(select_builders=[], datasets=datasets)
            for datasets in tree_analysis.union_datasets
        ]

    @property
    def db(self) -> Database:
        """The database object associated with the nested select builders."""
        if self._initial_select_builder is not None:
            return self._initial_select_builder.joins.db
        else:
            return self.union_terms[0].select_builders[0].joins.db

    @property
    def special(self) -> Set[str]:
        """The special columns associated with the nested select builders."""
        if self._initial_select_builder is not None:
            return self._initial_select_builder.joins.special.keys()
        else:
            return self.union_terms[0].select_builders[0].joins.special.keys()

    def analyze_projection(self) -> None:
        # Docstring inherited.
        super().analyze_projection()
        # See if we need to do a DISTINCT [ON] or GROUP BY to get unique rows
        # because we have rows for datasets in multiple collections with the
        # same data ID and dataset type.
        for dataset_type in self.joins_analysis.columns.dataset_fields:
            if not self.projection_columns.dataset_fields[dataset_type]:
                if dataset_type is qt.ANY_DATASET:
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
            if dataset_type is qt.ANY_DATASET:
                for union_term in self.union_terms:
                    # If there is more than one collection for one union term,
                    # we need to add collection_key to all of them to keep the
                    # SELECT columns uniform.
                    if len(union_term.datasets.collection_records) > 1:
                        fields_for_dataset.add("collection_key")
                        break
            elif len(self.joins_analysis.datasets[dataset_type].collection_records) > 1:
                fields_for_dataset.add("collection_key")

    def analyze_find_first(self, find_first_dataset: str | qt.AnyDatasetType) -> None:
        # Docstring inherited.
        if find_first_dataset is qt.ANY_DATASET:
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
        # Docstring inherited.
        assert self._initial_select_builder is not None
        driver.apply_initial_query_joins(
            self._initial_select_builder, self.joins_analysis, self.union_dataset_dimensions
        )
        # Join in the union datasets. This makes one copy of the initial
        # select builder for each dataset type, and hence from here on we have
        # to repeat whatever we do to all select builders.
        for union_term in self.union_terms:
            for dataset_type_name in union_term.datasets.name:
                select_builder = self._initial_select_builder.copy()
                driver.join_dataset_search(
                    select_builder.joins,
                    union_term.datasets,
                    self.joins_analysis.columns.dataset_fields[qt.ANY_DATASET],
                    union_dataset_type_name=dataset_type_name,
                )
                union_term.select_builders.append(select_builder)
        self._initial_select_builder = None
        for union_term in self.union_terms:
            for select_builder in union_term.select_builders:
                driver.apply_missing_dimension_joins(select_builder, self.joins_analysis)

    def apply_projection(self, driver: DirectQueryDriver, order_by: Iterable[qt.OrderExpression]) -> None:
        # Docstring inherited.
        driver.project_spatial_join_filtering(
            self.projection_columns,
            self.postprocessing,
            itertools.chain.from_iterable(union_term.select_builders for union_term in self.union_terms),
        )
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
                    find_first_dataset=None if union_term.find_first is None else qt.ANY_DATASET,
                    order_by=order_by,
                )

    def apply_find_first(self, driver: DirectQueryDriver) -> None:
        # Docstring inherited.
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
        # Docstring inherited.
        terms: list[sqlalchemy.Select] = []
        for union_term in self.union_terms:
            for dataset_type_name, select_builder in zip(
                union_term.datasets.name, union_term.select_builders
            ):
                select_builder.columns = self.final_columns
                select_builder.joins.special["_DATASET_TYPE_NAME"] = sqlalchemy.literal(dataset_type_name)
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
        # Docstring inherited.
        sql_select, _ = self.finish_select(return_columns=False)
        from_clause = sql_select.cte() if cte else sql_select.subquery()
        joins_builder = SqlJoinsBuilder(
            db=self.db,
            from_clause=from_clause,
        ).extract_columns(self.final_columns, self.postprocessing)
        return SqlSelectBuilder(joins_builder, columns=self.final_columns)
