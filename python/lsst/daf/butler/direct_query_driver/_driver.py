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

__all__ = ("DirectQueryDriver",)

import dataclasses
import itertools
import logging
import sys
import uuid
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Set
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

import sqlalchemy

from .. import ddl
from .._collection_type import CollectionType
from .._dataset_type import DatasetType
from .._exceptions import InvalidQueryError
from ..dimensions import DataCoordinate, DataIdValue, DimensionElement, DimensionGroup, DimensionUniverse
from ..dimensions.record_cache import DimensionRecordCache
from ..queries import tree as qt
from ..queries.driver import (
    DataCoordinateResultPage,
    DatasetRefResultPage,
    DimensionRecordResultPage,
    GeneralResultPage,
    QueryDriver,
    ResultPage,
)
from ..queries.result_specs import (
    DataCoordinateResultSpec,
    DatasetRefResultSpec,
    DimensionRecordResultSpec,
    GeneralResultSpec,
    ResultSpec,
)
from ..registry import CollectionSummary, NoDefaultCollectionError
from ..registry.interfaces import ChainedCollectionRecord, CollectionRecord
from ..registry.managers import RegistryManagerInstances
from ..registry.wildcards import CollectionWildcard
from ._postprocessing import Postprocessing
from ._predicate_constraints_summary import PredicateConstraintsSummary
from ._query_analysis import (
    QueryCollectionAnalysis,
    QueryFindFirstAnalysis,
    QueryJoinsAnalysis,
    QueryTreeAnalysis,
    ResolvedDatasetSearch,
)
from ._query_builder import QueryBuilder, SingleSelectQueryBuilder, UnionQueryBuilder
from ._result_page_converter import (
    DataCoordinateResultPageConverter,
    DatasetRefResultPageConverter,
    DimensionRecordResultPageConverter,
    GeneralResultPageConverter,
    ResultPageConverter,
    ResultPageConverterContext,
)
from ._sql_builders import SqlJoinsBuilder, SqlSelectBuilder, make_table_spec
from ._sql_column_visitor import SqlColumnVisitor

if TYPE_CHECKING:
    from ..registry.interfaces import Database


_LOG = logging.getLogger(__name__)

_T = TypeVar("_T", bound=str | qt.AnyDatasetType)


class DirectQueryDriver(QueryDriver):
    """The `QueryDriver` implementation for `DirectButler`.

    Parameters
    ----------
    db : `Database`
        Abstraction for the SQL database.
    universe : `DimensionUniverse`
        Definitions of all dimensions.
    managers : `RegistryManagerInstances`
        Struct of registry manager objects.
    dimension_record_cache : `DimensionRecordCache`
        Cache of dimension records for infrequently-changing, commonly-used
        dimensions.
    default_collections : `~collections.abc.Sequence` [ `str` ]
        Default collection search path.
    default_data_id : `DataCoordinate`
        Default governor dimension values.
    raw_page_size : `int`, optional
        Number of database rows to fetch for each result page.  The actual
        number of rows in a page may be smaller due to postprocessing.
    constant_rows_limit : `int`, optional
        Maximum number of uploaded rows to include in queries via
        `Database.constant_rows`; above this limit a temporary table is used
        instead.
    postprocessing_filter_factor : `int`, optional
        The number of database rows we expect to have to fetch to yield a
        single output row for queries that involve postprocessing.  This is
        purely a performance tuning parameter that attempts to balance between
        fetching too much and requiring multiple fetches; the true value is
        highly dependent on the actual query.
    """

    def __init__(
        self,
        db: Database,
        universe: DimensionUniverse,
        managers: RegistryManagerInstances,
        dimension_record_cache: DimensionRecordCache,
        default_collections: Iterable[str],
        default_data_id: DataCoordinate,
        # Increasing raw_page_size increases memory usage for queries on
        # Butler server, so if you increase this you may need to increase the
        # memory allocation for the server in Phalanx as well.
        raw_page_size: int = 2000,
        constant_rows_limit: int = 1000,
        postprocessing_filter_factor: int = 10,
    ):
        self.db = db
        self.managers = managers
        self._dimension_record_cache = dimension_record_cache
        self._universe = universe
        self._default_collections = tuple(default_collections)
        self._default_data_id = default_data_id
        self._materializations: dict[qt.MaterializationKey, _MaterializationState] = {}
        self._upload_tables: dict[qt.DataCoordinateUploadKey, sqlalchemy.FromClause] = {}
        self._exit_stack: ExitStack | None = None
        self._raw_page_size = raw_page_size
        self._postprocessing_filter_factor = postprocessing_filter_factor
        self._constant_rows_limit = min(constant_rows_limit, db.get_constant_rows_max())
        self._cursors: set[_Cursor] = set()

    def __enter__(self) -> None:
        self._exit_stack = ExitStack()
        # It might be nice to defer opening a transaction here until first use
        # to reduce the time spent in transactions.  But it's worth noting that
        # this is the default low-level behavior of the Python SQLite driver,
        # and it makes it incredibly prone to deadlocks.  We might be okay
        # here, because Query doesn't do true write operations - just temp
        # table writes - but I'm not confident that's enough to make delayed
        # transaction starts safe against deadlocks, and it'd be more
        # complicated to implement anyway.
        #
        # We start a transaction rather than just opening a connection to make
        # temp table and cursors work with pg_bouncer transaction affinity.
        self._exit_stack.enter_context(self.db.transaction(for_temp_tables=True))

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        assert self._exit_stack is not None
        self._materializations.clear()
        self._upload_tables.clear()
        # Transfer open cursors' close methods to exit stack, this will help
        # with the cleanup in case a cursor raises an exceptions on close.
        for cursor in self._cursors:
            self._exit_stack.push(cursor.close)
        self._exit_stack.__exit__(exc_type, exc_value, traceback)
        self._cursors = set()
        self._exit_stack = None

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    @overload
    def execute(
        self, result_spec: DataCoordinateResultSpec, tree: qt.QueryTree
    ) -> Iterator[DataCoordinateResultPage]: ...

    @overload
    def execute(
        self, result_spec: DimensionRecordResultSpec, tree: qt.QueryTree
    ) -> Iterator[DimensionRecordResultPage]: ...

    @overload
    def execute(
        self, result_spec: DatasetRefResultSpec, tree: qt.QueryTree
    ) -> Iterator[DatasetRefResultPage]: ...

    @overload
    def execute(self, result_spec: GeneralResultSpec, tree: qt.QueryTree) -> Iterator[GeneralResultPage]: ...

    def execute(self, result_spec: ResultSpec, tree: qt.QueryTree) -> Iterator[ResultPage]:
        # Docstring inherited.
        if self._exit_stack is None:
            raise RuntimeError("QueryDriver context must be entered before queries can be executed.")
        builder = self.build_query(
            tree,
            final_columns=result_spec.get_result_columns(),
            order_by=result_spec.order_by,
            find_first_dataset=result_spec.find_first_dataset,
            allow_duplicate_overlaps=result_spec.allow_duplicate_overlaps,
        )
        sql_select, sql_columns = builder.finish_select()
        if result_spec.order_by:
            visitor = SqlColumnVisitor(sql_columns, self)
            sql_select = sql_select.order_by(*[visitor.expect_scalar(term) for term in result_spec.order_by])
        if result_spec.limit is not None:
            if builder.postprocessing:
                builder.postprocessing.limit = result_spec.limit
            else:
                sql_select = sql_select.limit(result_spec.limit)
        if builder.postprocessing.limit is not None:
            # We might want to fetch many fewer rows than the default page
            # size if we have to implement limit in postprocessing.
            raw_page_size = min(
                self._postprocessing_filter_factor * builder.postprocessing.limit,
                self._raw_page_size,
            )
        else:
            raw_page_size = self._raw_page_size
        # Execute the query by initializing a _Cursor object that manages the
        # lifetime of the result.
        cursor = _Cursor(
            self.db,
            sql_select,
            postprocessing=builder.postprocessing,
            raw_page_size=raw_page_size,
            page_converter=self._create_result_page_converter(result_spec, builder.final_columns),
        )
        # Since this function isn't a context manager and the caller could stop
        # iterating before we retrieve all the results, we have to track open
        # cursors to ensure we can close them as part of higher-level cleanup.
        self._cursors.add(cursor)

        # Return the iterator as a separate function, so that all the code
        # above runs immediately instead of later when we first read from the
        # iterator.  This ensures that any exceptions that are triggered during
        # set-up for this query occur immediately.
        return self._read_results(cursor)

    def _read_results(self, cursor: _Cursor) -> Iterator[ResultPage]:
        """Read out all of the result pages from the database."""
        try:
            while (result_page := cursor.next()) is not None:
                yield result_page
        finally:
            self._cursors.discard(cursor)
            cursor.close()

    def _create_result_page_converter(self, spec: ResultSpec, columns: qt.ColumnSet) -> ResultPageConverter:
        context = ResultPageConverterContext(
            db=self.db,
            column_order=columns.get_column_order(),
            dimension_record_cache=self._dimension_record_cache,
        )
        match spec:
            case DimensionRecordResultSpec():
                return DimensionRecordResultPageConverter(spec, context)
            case DataCoordinateResultSpec():
                return DataCoordinateResultPageConverter(spec, context)
            case DatasetRefResultSpec():
                return DatasetRefResultPageConverter(
                    spec, self.get_dataset_type(spec.dataset_type_name), context
                )
            case GeneralResultSpec():
                return GeneralResultPageConverter(spec, context)
            case _:
                raise NotImplementedError(f"Result type '{spec.result_type}' not yet implemented")

    def materialize(
        self,
        tree: qt.QueryTree,
        dimensions: DimensionGroup,
        datasets: frozenset[str],
        allow_duplicate_overlaps: bool = False,
        key: qt.MaterializationKey | None = None,
    ) -> qt.MaterializationKey:
        # Docstring inherited.
        if self._exit_stack is None:
            raise RuntimeError("QueryDriver context must be entered before 'materialize' is called.")
        plan = self.build_query(
            tree, qt.ColumnSet(dimensions), allow_duplicate_overlaps=allow_duplicate_overlaps
        )
        # Current implementation ignores 'datasets' aside from remembering
        # them, because figuring out what to put in the temporary table for
        # them is tricky, especially if calibration collections are involved.
        # That's okay because:
        #
        # - the query whose results we materialize includes the dataset
        #   searches as constraints;
        #
        # - we still (in Query.materialize) join the dataset searches back in
        #   anyway, and given materialized data IDs the join to the dataset
        #   search is straightforward and definitely well-indexed, and not much
        #   (if at all) worse than joining back in on a materialized UUID.
        #
        sql_select, _ = plan.finish_select(return_columns=False)
        table = self._exit_stack.enter_context(
            self.db.temporary_table(
                make_table_spec(plan.final_columns, self.db, plan.postprocessing, make_indices=True)
            )
        )
        self.db.insert(table, select=sql_select)
        if key is None:
            key = uuid.uuid4()
        self._materializations[key] = _MaterializationState(table, datasets, plan.postprocessing)
        return key

    def upload_data_coordinates(
        self,
        dimensions: DimensionGroup,
        rows: Iterable[tuple[DataIdValue, ...]],
        key: qt.DataCoordinateUploadKey | None = None,
    ) -> qt.DataCoordinateUploadKey:
        # Docstring inherited.
        if self._exit_stack is None:
            raise RuntimeError(
                "QueryDriver context must be entered before 'upload_data_coordinates' is called."
            )
        columns = qt.ColumnSet(dimensions).drop_implied_dimension_keys()
        table_spec = ddl.TableSpec(
            [columns.get_column_spec(logical_table, field).to_sql_spec() for logical_table, field in columns]
        )
        dict_rows: list[dict[str, Any]]
        if not columns:
            table_spec.fields.add(
                ddl.FieldSpec(
                    SqlSelectBuilder.EMPTY_COLUMNS_NAME,
                    dtype=SqlSelectBuilder.EMPTY_COLUMNS_TYPE,
                    nullable=True,
                )
            )
            dict_rows = [{SqlSelectBuilder.EMPTY_COLUMNS_NAME: None}]
        else:
            dict_rows = [dict(zip(dimensions.required, values)) for values in rows]
        from_clause: sqlalchemy.FromClause
        if len(dict_rows) > self._constant_rows_limit:
            from_clause = self._exit_stack.enter_context(self.db.temporary_table(table_spec))
            self.db.insert(from_clause, *dict_rows)
        else:
            from_clause = self.db.constant_rows(table_spec.fields, *dict_rows)
        if key is None:
            key = uuid.uuid4()
        self._upload_tables[key] = from_clause
        return key

    def count(
        self,
        tree: qt.QueryTree,
        result_spec: ResultSpec,
        *,
        exact: bool,
        discard: bool,
    ) -> int:
        # Docstring inherited.
        columns = result_spec.get_result_columns()
        builder = self.build_query(tree, columns, find_first_dataset=result_spec.find_first_dataset)
        if not all(d.collection_records for d in builder.joins_analysis.datasets.values()):
            return 0
        # No need to do similar check on
        if not exact:
            builder.postprocessing = Postprocessing()
        if builder.postprocessing:
            if not discard:
                raise InvalidQueryError("Cannot count query rows exactly without discarding them.")
            sql_select, _ = builder.finish_select(return_columns=False)
            builder.postprocessing.limit = result_spec.limit
            n = 0
            with self.db.query(sql_select.execution_options(yield_per=self._raw_page_size)) as results:
                for _ in builder.postprocessing.apply(results):
                    n += 1
            return n
        # If the query has DISTINCT, GROUP BY, or UNION [ALL], nest it in a
        # subquery so we count deduplicated rows.
        select_builder = builder.finish_nested()
        # Replace the columns of the query with just COUNT(*).
        select_builder.columns = qt.ColumnSet(self._universe.empty)
        select_builder.joins.special.clear()
        count_func: sqlalchemy.ColumnElement[int] = sqlalchemy.func.count()
        select_builder.joins.special["_ROWCOUNT"] = count_func
        # Render and run the query.
        sql_select = select_builder.select(builder.postprocessing)
        with self.db.query(sql_select) as result:
            count = cast(int, result.scalar())
        if result_spec.limit is not None:
            count = min(count, result_spec.limit)
        return count

    def any(self, tree: qt.QueryTree, *, execute: bool, exact: bool) -> bool:
        # Docstring inherited.
        builder = self.build_query(tree, qt.ColumnSet(tree.dimensions), allow_duplicate_overlaps=True)
        if not all(d.collection_records for d in builder.joins_analysis.datasets.values()):
            return False
        if not execute:
            if exact:
                raise InvalidQueryError("Cannot obtain exact result for 'any' without executing.")
            return True
        if builder.postprocessing and exact:
            sql_select, _ = builder.finish_select(return_columns=False)
            with self.db.query(
                sql_select.execution_options(yield_per=self._postprocessing_filter_factor)
            ) as result:
                for _ in builder.postprocessing.apply(result):
                    return True
                return False
        sql_select, _ = builder.finish_select()
        with self.db.query(sql_select.limit(1)) as result:
            return result.first() is not None

    def explain_no_results(self, tree: qt.QueryTree, execute: bool) -> Iterable[str]:
        # Docstring inherited.
        plan = self.build_query(tree, qt.ColumnSet(tree.dimensions), analyze_only=True)
        if plan.joins_analysis.messages or not execute:
            return plan.joins_analysis.messages
        # TODO: guess at ways to split up query that might fail or succeed if
        # run separately, execute them with LIMIT 1 and report the results.
        return []

    def get_dataset_type(self, name: str) -> DatasetType:
        # Docstring inherited
        return self.managers.datasets.get_dataset_type(name)

    def get_default_collections(self) -> tuple[str, ...]:
        # Docstring inherited.
        if not self._default_collections:
            raise NoDefaultCollectionError("No collections provided and no default collections.")
        return self._default_collections

    def build_query(
        self,
        tree: qt.QueryTree,
        final_columns: qt.ColumnSet,
        *,
        order_by: Iterable[qt.OrderExpression] = (),
        find_first_dataset: str | qt.AnyDatasetType | None = None,
        analyze_only: bool = False,
        allow_duplicate_overlaps: bool = False,
    ) -> QueryBuilder:
        """Convert a query description into a nearly-complete builder object
        for the SQL version of that query.

        Parameters
        ----------
        tree : `.queries.tree.QueryTree`
            Description of the joins and row filters in the query.
        final_columns : `.queries.tree.ColumnSet`
            Final output columns that should be emitted by the SQL query.
        order_by : `~collections.abc.Iterable` [ \
                `.queries.tree.OrderExpression` ], optional
            Column expressions to sort by.
        find_first_dataset : `str`, ``ANY_DATASET``, or `None`, optional
            Name of a dataset type for which only one result row for each data
            ID should be returned, with the collections searched in order.
            ``ANY_DATASET`` is used to represent the search for all dataset
            types with a particular set of dimensions in ``tree.any_dataset``.
        analyze_only : `bool`, optional
            If `True`, perform the initial analysis needed to construct the
            builder, but do not call methods that build its SQL form.  This can
            be useful for obtaining diagnostic information about the query that
            would be generated.
        allow_duplicate_overlaps : `bool`, optional
            If set to `True` then query will be allowed to generate
            non-distinct rows for spatial overlaps.

        Returns
        -------
        builder : `QueryBuilder`
            An object that contains an analysis of the queries columns and
            general structure, SQLAlchemy representations of most of the
            constructed query, and a description of Python-side postprocessing
            to be performed after executing it.  Callers should generally only
            have to call `finish_select` or `finish_nested`; all ``apply``
            methods will have already been called.

        Notes
        -----
        The SQL queries produced by this driver are built in three steps:

        - First we "analyze" the entire query, fetching extra information as
          needed to identify the tables we'll join in, the columns we'll have
          at each level of what may be a nested SELECT, and the degree to which
          we'll have to use GROUP BY / DISTINCT or window functions to obtain
          just the rows we want.  This process initializes a `QueryBuilder`
          instance, but does not call any of its ``apply*`` methods to actually
          build the SQLAlchemy version of the query, and it is all that is done
          when ``analyze_only=True``.
        - In the next step we call `QueryBuilder` ``apply*`` methods to mutate
          and/or replace the `QueryBuilder` object's nested `SqlSelectBuilder`
          instances to reflect that analysis, building the SQL from the inside
          out (subqueries before their parents).  This is also done by the
          `build_query` method by default
        - The returned builder can be used by calling either `finish_select`
          (to get the full executable query) or `finish_nested` (to wrap
          that query as a subquery - if needed - in order to do related
          queries, like ``COUNT(*)`` or ``LIMIT 1`` checks).

        Within both the analysis and application steps, we further split the
        query *structure* and building process into three stages, mapping
        roughly to levels of (potential) subquery nesting:

        - In the "joins" stage, we join all the tables we need columns or
          constraints from and apply the predicate as a WHERE clause.  All
          non-calculated columns are included in the query at this stage.
        - In the optional "projection" stage, we apply a GROUP BY or DISTINCT
          to reduce the set of columns and/or rows, and in some cases add new
          calculated columns.
        - In the optional "find first" stage, we use a common table expression
          with PARTITION ON to search for datasets in a sequence of collections
          in the order those collections were provided.

        In addition, there are two different overall structures for butler
        queries, modeled as two different `QueryBuilder` subclasses.

        - `SingleSelectQueryBuilder` produces a single possibly-nested SELECT
          query structured directly according to the stages above.  This is
          used for almost all queries - specifically whenever
          `QueryTree.any_dataset` is `None`.
        - `UnionQueryBuilder` implements queries for datasets of multiple types
          with the same dimensions as a UNION ALL combination of SELECTs, in
          which each component SELECT has the joins/projection/find-first
          structure.  This cannot be implemented as a sequence of
          `SingleSelectQueryBuilder` objects, however, since the need for the
          terms to share a single `Postprocessing` object and column list means
          that they are not independent.

        The `build_query` method delegates to methods of the `QueryBuilder`
        object when "applying" to let them handle these differences.  These
        often call back to other methods on the driver (so code shared by both
        builders mostly lives in the driver class).
        """
        # Analyze the dimensions, dataset searches, and other join operands
        # that will go into the query.  This also initializes a
        # SqlSelectBuilder and Postprocessing with spatial/temporal constraints
        # potentially transformed by the dimensions manager (but none of the
        # rest of the analysis reflected in that SqlSelectBuilder).
        query_tree_analysis = self._analyze_query_tree(tree, allow_duplicate_overlaps)
        # The "projection" columns differ from the final columns by not
        # omitting any dimension keys (this keeps queries for different result
        # types more similar during construction), including any columns needed
        # only by order_by terms, and including the collection key if we need
        # it for GROUP BY or DISTINCT.
        projection_columns = final_columns.copy()
        projection_columns.restore_dimension_keys()
        for term in order_by:
            term.gather_required_columns(projection_columns)
        # There are two kinds of query builders: simple SELECTS and UNIONs
        # over dataset types.
        builder: QueryBuilder
        if tree.any_dataset is not None:
            builder = UnionQueryBuilder(
                query_tree_analysis,
                union_dataset_dimensions=tree.any_dataset.dimensions,
                projection_columns=projection_columns,
                final_columns=final_columns,
            )
        else:
            builder = SingleSelectQueryBuilder(
                tree_analysis=query_tree_analysis,
                projection_columns=projection_columns,
                final_columns=final_columns,
            )
        # Finish setting up the projection part of the builder.
        builder.analyze_projection()
        # The joins-stage query also needs to include all columns needed by the
        # downstream projection query.  Note that this:
        # - never adds new dimensions to the joins stage (since those are
        #   always a superset of the projection-stage dimensions);
        # - does not affect our previous determination of
        #   needs_dataset_distinct, because any dataset fields being added to
        #   the joins stage here are already in the projection.
        builder.joins_analysis.columns.update(builder.projection_columns)
        # Set up the find-first part of the builder.
        if find_first_dataset is not None:
            builder.analyze_find_first(find_first_dataset)
        # At this point, analysis is complete, and we can proceed to making
        # the select_builder(s) reflect that analysis.
        if not analyze_only:
            builder.apply_joins(self)
            builder.apply_projection(self, order_by)
            builder.apply_find_first(self)
        return builder

    def _analyze_query_tree(self, tree: qt.QueryTree, allow_duplicate_overlaps: bool) -> QueryTreeAnalysis:
        """Analyze a `.queries.tree.QueryTree` as the first step in building
        a SQL query.

        Parameters
        ----------
        tree : `.queries.tree.QueryTree`
            Description of the joins and row filters in the query.

        Returns
        -------
        tree_analysis : `QueryTreeAnalysis`
            Struct containing additional information need to build the joins
            stage of a query.
        allow_duplicate_overlaps : `bool`, optional
            If set to `True` then query will be allowed to generate
            non-distinct rows for spatial overlaps.

        Notes
        -----
        See `build_query` for the complete picture of how SQL queries are
        constructed.  This method is the very first step for all queries.

        The fact that this method returns both a QueryPlan and an initial
        SqlSelectBuilder (rather than just a QueryPlan) is a tradeoff that lets
        DimensionRecordStorageManager.process_query_overlaps (which is called
        by the `_analyze_query_tree` call below) pull out overlap expressions
        from the predicate at the same time it turns them into SQL table joins
        (in the builder).
        """
        # Fetch the records and summaries for any collections we might be
        # searching for datasets and organize them for the kind of lookups
        # we'll do later.
        collection_analysis = self._analyze_collections(tree)
        # Delegate to the dimensions manager to rewrite the predicate and start
        # a SqlSelectBuilder to cover any spatial overlap joins or constraints.
        # We'll return that SqlSelectBuilder (or copies of it) at the end.
        (
            predicate,
            select_builder,
            postprocessing,
        ) = self.managers.dimensions.process_query_overlaps(
            tree.dimensions,
            tree.predicate,
            tree.get_joined_dimension_groups(),
            collection_analysis.calibration_dataset_types,
            allow_duplicate_overlaps,
        )
        # Extract the data ID implied by the predicate; we can use the governor
        # dimensions in that to constrain the collections we search for
        # datasets later.
        predicate_constraints = PredicateConstraintsSummary(predicate)
        # Use the default data ID to apply additional constraints where needed.
        predicate_constraints.apply_default_data_id(self._default_data_id, tree.dimensions)
        predicate = predicate_constraints.predicate
        # Initialize the plan we're return at the end of the method.
        joins = QueryJoinsAnalysis(predicate=predicate, columns=select_builder.columns)
        joins.messages.extend(predicate_constraints.messages)
        # Add columns required by postprocessing.
        postprocessing.gather_columns_required(joins.columns)
        # Add materializations, which can also bring in more postprocessing.
        for m_key, m_dimensions in tree.materializations.items():
            m_state = self._materializations[m_key]
            joins.materializations[m_key] = m_dimensions
            # When a query is materialized, the new tree has an empty
            # (trivially true) predicate because the original was used to make
            # the materialized rows.  But the original postprocessing isn't
            # executed when the materialization happens, so we have to include
            # it here.
            postprocessing.spatial_join_filtering.extend(m_state.postprocessing.spatial_join_filtering)
            postprocessing.spatial_where_filtering.extend(m_state.postprocessing.spatial_where_filtering)
            postprocessing.spatial_expression_filtering.extend(
                m_state.postprocessing.spatial_expression_filtering
            )
        # Add data coordinate uploads.
        joins.data_coordinate_uploads.update(tree.data_coordinate_uploads)
        # Add dataset_searches and filter out collections that don't have the
        # right dataset type or governor dimensions.  We re-resolve dataset
        # searches now that we have a constraint data ID.
        for dataset_type_name, dataset_search in tree.datasets.items():
            resolved_dataset_search = self._resolve_dataset_search(
                dataset_type_name,
                dataset_search,
                predicate_constraints.constraint_data_id,
                collection_analysis.summaries_by_dataset_type[dataset_type_name],
            )
            if resolved_dataset_search.dimensions != self.get_dataset_type(dataset_type_name).dimensions:
                # This is really for server-side defensiveness; it's hard to
                # imagine the query getting different dimensions for a dataset
                # type in two calls to the same query driver.
                raise InvalidQueryError(
                    f"Incorrect dimensions {resolved_dataset_search.dimensions} for dataset "
                    f"{dataset_type_name!r} in query "
                    f"(vs. {self.get_dataset_type(dataset_type_name).dimensions})."
                )
            joins.datasets[dataset_type_name] = resolved_dataset_search
            if not resolved_dataset_search.collection_records:
                joins.messages.append(
                    f"Search for dataset type {resolved_dataset_search.name!r} in "
                    f"{list(dataset_search.collections)} is doomed to fail."
                )
                joins.messages.extend(resolved_dataset_search.messages)
        # Process the special any_dataset search, if there is one. This entails
        # making a modified copy of the plan for each distinct post-filtering
        # collection search path.
        if tree.any_dataset is None:
            return QueryTreeAnalysis(
                joins, union_datasets=[], initial_select_builder=select_builder, postprocessing=postprocessing
            )
        union_datasets = self._resolve_union_datasets(tree.any_dataset.dimensions, collection_analysis)
        return QueryTreeAnalysis(
            joins,
            union_datasets=union_datasets,
            initial_select_builder=select_builder,
            postprocessing=postprocessing,
        )

    def _resolve_union_datasets(
        self, dimensions: DimensionGroup, collection_analysis: QueryCollectionAnalysis
    ) -> list[ResolvedDatasetSearch[list[str]]]:
        """Resolve searches for union datasets.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions of the union dataset types in this query.
        collection_analysis : `CollectionAnalysis`
            Information about the collections appearing in this collection.

        Returns
        -------
        searches : `list` [ `ResolvedDatasetSearch` ]
            Resolved dataset searches for all union dataset types with these
            dimensions.  Each item in the list groups dataset types with the
            same collection search path.
        """
        # Gather the filtered collection search path for each union dataset
        # type.
        collections_by_dataset_type = defaultdict[str, list[str]](list)
        for collection_record, collection_summary in collection_analysis.summaries_by_dataset_type[
            qt.ANY_DATASET
        ]:
            for dataset_type in collection_summary.dataset_types:
                if dataset_type.dimensions == dimensions:
                    collections_by_dataset_type[dataset_type.name].append(collection_record.name)
        # Reverse the lookup order on the mapping we just made to group
        # dataset types by their collection search path.  Each such group
        # yields a term in a union query builder
        dataset_searches_by_collections: dict[tuple[str, ...], ResolvedDatasetSearch[list[str]]] = {}
        for dataset_type_name, collection_path in collections_by_dataset_type.items():
            key = tuple(collection_path)
            if (resolved_search := dataset_searches_by_collections.get(key)) is None:
                resolved_search = ResolvedDatasetSearch[list[str]](
                    [],
                    dimensions=dimensions,
                    collection_records=[
                        collection_analysis.collection_records[collection_name]
                        for collection_name in collection_path
                    ],
                    messages=[],
                )
                resolved_search.is_calibration_search = any(
                    r.type is CollectionType.CALIBRATION for r in resolved_search.collection_records
                )
                dataset_searches_by_collections[key] = resolved_search
            resolved_search.name.append(dataset_type_name)
        return list(dataset_searches_by_collections.values())

    def apply_initial_query_joins(
        self,
        select_builder: SqlSelectBuilder,
        joins_analysis: QueryJoinsAnalysis,
        union_dataset_dimensions: DimensionGroup | None,
    ) -> None:
        """Apply most of the "joins" stage of query construction to a single
        SELECT.

        This method is expected to be invoked by `QueryBuilder.apply_joins`
        implementations.  It handles all tables and subqueries in the FROM
        clause, except:

        - the `QueryTree.any_dataset` search (handled by `UnionQueryBuilder`
          directly);
        - joins of dimension tables needed only for their keys (handled by
          `apply_missing_dimension_joins`).

        Parameters
        ----------
        select_builder : `SqlSelectBuilder`
            Low-level SQL builder for a single SELECT term, modified in place.
        joins_analysis : `QueryJoinsAnalysis`
            Information about the joins stage of query construction.
        union_dataset_dimensions : `DimensionGroup` or `None`
            Dimensions of the union dataset types, or `None` if this is not
            a union dataset query.
        """
        # Process data coordinate upload joins.
        for upload_key, upload_dimensions in joins_analysis.data_coordinate_uploads.items():
            select_builder.joins.join(
                SqlJoinsBuilder(db=self.db, from_clause=self._upload_tables[upload_key]).extract_dimensions(
                    upload_dimensions.required
                )
            )
        # Process materialization joins. We maintain a set of dataset types
        # that were included in a materialization; searches for these datasets
        # can be dropped if they are only present to provide a constraint on
        # data IDs, since that's already embedded in a materialization.
        materialized_datasets: set[str] = set()
        for materialization_key, materialization_dimensions in joins_analysis.materializations.items():
            materialized_datasets.update(
                self._join_materialization(
                    select_builder.joins, materialization_key, materialization_dimensions
                )
            )
        # Process dataset joins (not including any union dataset). Datasets
        # searches included in materialization can be skipped unless we need
        # something from their tables.
        materialized_datasets = set()
        for m_state in self._materializations.values():
            materialized_datasets.update(m_state.datasets)
        for dataset_type_name, dataset_search in joins_analysis.datasets.items():
            if (
                dataset_type_name not in materialized_datasets
                or dataset_type_name in select_builder.columns.dataset_fields
            ):
                self.join_dataset_search(
                    select_builder.joins,
                    dataset_search,
                    joins_analysis.columns.dataset_fields[dataset_search.name],
                )
        # Join in dimension element tables that we know we need relationships
        # or columns from.
        for element in joins_analysis.iter_mandatory(union_dataset_dimensions):
            select_builder.joins.join(
                self.managers.dimensions.make_joins_builder(
                    element, joins_analysis.columns.dimension_fields[element.name]
                )
            )

    def apply_missing_dimension_joins(
        self, select_builder: SqlSelectBuilder, joins_analysis: QueryJoinsAnalysis
    ) -> None:
        """Apply dimension-table joins to a single SQL SELECT builder to ensure
        the full set of desired dimension keys is preset.

        This method is expected to be invoked by `QueryBuilder.apply_joins`
        implementations.

        Parameters
        ----------
        select_builder : `SqlSelectBuilder`
            Low-level SQL builder for a single SELECT term, modified in place.
        joins_analysis : `QueryJoinsAnalysis`
            Information about the joins stage of query construction.
        """
        # See if any dimension keys are still missing, and if so join in
        # their tables. Note that we know there are no fields needed from
        # these.
        while not (select_builder.joins.dimension_keys.keys() >= joins_analysis.columns.dimensions.names):
            # Look for opportunities to join in multiple dimensions via
            # single table, to reduce the total number of tables joined in.
            missing_dimension_names = (
                joins_analysis.columns.dimensions.names - select_builder.joins.dimension_keys.keys()
            )
            best = self._universe[
                max(
                    missing_dimension_names,
                    key=lambda name: len(self._universe[name].dimensions.names & missing_dimension_names),
                )
            ]
            to_join = self.managers.dimensions.make_joins_builder(best, frozenset())
            select_builder.joins.join(to_join)
        # Add the WHERE clause to the builder.
        select_builder.joins.where(
            joins_analysis.predicate.visit(SqlColumnVisitor(select_builder.joins, self))
        )

    def project_spatial_join_filtering(
        self,
        columns: qt.ColumnSet,
        postprocessing: Postprocessing,
        select_builders: Iterable[SqlSelectBuilder],
    ) -> None:
        """Transform spatial join postprocessing into expressions that can be
        OR'd together via an aggregate function in a GROUP BY.

        This only affects spatial join constraints involving region columns
        whose dimensions are being projected away.

        Parameters
        ----------
        columns : `.queries.tree.ColumnSet`
            Columns that will be included in the final query.
        postprocessing : `Postprocessing`
            Object that describes post-query processing; modified in place.
        select_builders : `~collections.abc.Iterable` [ `SqlSelectBuilder` ]
            SQL Builder objects to be modified in place.
        """
        kept: list[tuple[DimensionElement, DimensionElement]] = []
        for a, b in postprocessing.spatial_join_filtering:
            if a.name not in columns.dimensions.elements or b.name not in columns.dimensions.elements:
                expr_name = f"_{a}_OVERLAPS_{b}"
                postprocessing.spatial_expression_filtering.append(expr_name)
                for select_builder in select_builders:
                    expr = sqlalchemy.cast(
                        sqlalchemy.cast(
                            select_builder.joins.fields[a.name]["region"], type_=sqlalchemy.String
                        )
                        + sqlalchemy.literal("&", type_=sqlalchemy.String)
                        + sqlalchemy.cast(
                            select_builder.joins.fields[b.name]["region"], type_=sqlalchemy.String
                        ),
                        type_=sqlalchemy.LargeBinary,
                    )
                    select_builder.joins.special[expr_name] = expr
            else:
                kept.append((a, b))
        postprocessing.spatial_join_filtering = kept

    def apply_query_projection(
        self,
        select_builder: SqlSelectBuilder,
        postprocessing: Postprocessing,
        *,
        join_datasets: Mapping[str, ResolvedDatasetSearch[str]],
        union_datasets: ResolvedDatasetSearch[list[str]] | None,
        projection_columns: qt.ColumnSet,
        needs_dimension_distinct: bool,
        needs_dataset_distinct: bool,
        needs_validity_match_count: bool,
        find_first_dataset: str | qt.AnyDatasetType | None,
        order_by: Iterable[qt.OrderExpression],
    ) -> None:
        """Apply the "projection" stage of query construction to a single
        SQL SELECT builder.

        This method is expected to be invoked by
        `QueryBuilder.apply_projection` implementations.

        Parameters
        ----------
        select_builder : `SqlSelectBuilder`
            Low-level SQL builder for a single SELECT term, modified in place.
        postprocessing : `Postprocessing`
            Description of query processing that happens in Python after the
            query is executed by the database.
        join_datasets : `~collections.abc.Mapping` [ `str`, \
                `ResolvedDatasetSearch` [ `str` ] ]
            Information about regular (non-union) dataset searches joined into
            the query.
        union_datasets : `ResolvedDatasetSearch [ `list` [ `str` ] ] or `None`
            Information about a search for dataset of multiple types with the
            same dimensions and the same post-filtering collection search path.
        projection_columns : `.queries.tree.ColumnSet`
            Columns to include in this projection stage of the query.
        needs_dimension_distinct : `bool`
            Whether this query needs a GROUP BY or DISTINCT to filter out rows
            where the only differences are dimension values that are not being
            returned to the user.
        needs_dataset_distinct : `bool`
            Whether this query needs a GROUP BY or DISTINCT to filter out rows
            that correspond to entries for different collections.
        needs_validity_match_count : `bool`
            Whether this query needs a COUNT column to track the number of
            datasets for each data ID and dataset type.  If this is `False` but
            ``postprocessing.check_validity_match_count`` is `True`, a dummy
            count column that is just "1" should be added, because the check
            is needed only for some other SELECT term in a UNION ALL.
        find_first_dataset : `str` or ``ANY_DATASET`` or `None`
            Name of the dataset type that will need a find-first stage.
            ``ANY_DATASET`` is used when the union datasets need a find-first
            search, while `None` is used to represent both ``find_first=False``
            and the case when ``find_first=True`` but only only collection has
            survived filtering.
        order_by : `~collections.abc.Iterable` [ \
              `.queries.tree.OrderExpression` ]
            Order by clause associated with the query.
        """
        select_builder.columns = projection_columns
        if not needs_dimension_distinct and not needs_dataset_distinct and not needs_validity_match_count:
            if postprocessing.check_validity_match_count:
                select_builder.joins.special[postprocessing.VALIDITY_MATCH_COUNT] = sqlalchemy.literal(1)
            # Rows are already unique; nothing else to do in this method.
            return
        # This method generates  either a SELECT DISTINCT [ON] or a SELECT with
        # GROUP BY. We'll work out which as we go.
        have_aggregates: bool = False
        # Dimension key columns form at least most of our GROUP BY or DISTINCT
        # ON clause.
        unique_keys: list[sqlalchemy.ColumnElement[Any]] = [
            select_builder.joins.dimension_keys[k][0]
            for k in projection_columns.dimensions.data_coordinate_keys
        ]

        # Many of our fields derive their uniqueness from the unique_key
        # fields: if rows are unique over the 'unique_key' fields, then they're
        # automatically unique over these 'derived_fields'.  We just remember
        # these as pairs of (logical_table, field) for now.
        derived_fields: list[tuple[str | qt.AnyDatasetType, str]] = []

        # There are two reasons we might need an aggregate function:
        # - to make sure temporal constraints and joins have resulted in at
        #   most one validity range match for each data ID and collection,
        #   when we're doing a find-first query.
        # - to compute the unions of regions we need for postprocessing, when
        #   the data IDs for those regions are not wholly included in the
        #   results (i.e. we need to postprocess on
        #   visit_detector_region.region, but the output rows don't have
        #   detector, just visit - so we pack the overlap expression into a
        #   blob via an aggregate function and interpret it later).
        if postprocessing.check_validity_match_count:
            if needs_validity_match_count:
                select_builder.joins.special[postprocessing.VALIDITY_MATCH_COUNT] = (
                    sqlalchemy.func.count().label(postprocessing.VALIDITY_MATCH_COUNT)
                )
                have_aggregates = True
            else:
                select_builder.joins.special[postprocessing.VALIDITY_MATCH_COUNT] = sqlalchemy.literal(1)

        for element in postprocessing.iter_missing(projection_columns):
            if element.name in projection_columns.dimensions.elements:
                # The region associated with dimension keys returned by the
                # query are derived fields, since there is only one region
                # associated with each dimension key value.
                derived_fields.append((element.name, "region"))
            else:
                # If there's a projection and we're doing postprocessing, we
                # might be collapsing the dimensions of the postprocessing
                # regions.  When that happens, we want to apply an aggregate
                # function to them that computes the union of the regions that
                # are grouped together.  Note that this should only happen for
                # constraints that involve a "given", external-to-the-database
                # region (postprocessing.spatial_where_filtering); join
                # constraints that need aggregates should have already been
                # transformed in advance.
                select_builder.joins.fields[element.name]["region"] = ddl.Base64Region.union_aggregate(
                    select_builder.joins.fields[element.name]["region"]
                )
                have_aggregates = True
        # Postprocessing spatial join constraints where at least one region's
        # dimensions are being projected away will have already been turned
        # into the kind of expression that sphgeom.Region.decodeOverlapsBase64
        # processes.  We can just apply an aggregate function to these.  Note
        # that we don't do this to other constraints in order to minimize
        # duplicate fetches of the same region blob.
        for expr_name in postprocessing.spatial_expression_filtering:
            select_builder.joins.special[expr_name] = sqlalchemy.cast(
                sqlalchemy.func.aggregate_strings(select_builder.joins.special[expr_name], "|"),
                type_=sqlalchemy.LargeBinary,
            )
            have_aggregates = True

        # All dimension record fields are derived fields.
        for element_name, fields_for_element in projection_columns.dimension_fields.items():
            for element_field in fields_for_element:
                derived_fields.append((element_name, element_field))
        # Some dataset fields are derived fields and some are unique keys, and
        # it depends on the kinds of collection(s) we're searching and whether
        # it's a find-first query.
        for dataset_type, fields_for_dataset in projection_columns.dataset_fields.items():
            dataset_search: ResolvedDatasetSearch[Any]
            if dataset_type is qt.ANY_DATASET:
                assert union_datasets is not None
                dataset_search = union_datasets
            else:
                dataset_search = join_datasets[dataset_type]
            for dataset_field in fields_for_dataset:
                if dataset_field == "collection_key":
                    # If the collection_key field is present, it's needed for
                    # uniqueness if we're looking in more than one collection.
                    # If not, it's a derived field.
                    if len(dataset_search.collection_records) > 1:
                        unique_keys.append(select_builder.joins.fields[dataset_type]["collection_key"])
                    else:
                        derived_fields.append((dataset_type, "collection_key"))
                elif dataset_field == "timespan" and dataset_search.is_calibration_search:
                    # The timespan is also a unique key...
                    if dataset_type == find_first_dataset:
                        # ...unless we're doing a find-first search on this
                        # dataset, in which case we need to use ANY_VALUE on
                        # the timespan and check that _VALIDITY_MATCH_COUNT
                        # (added earlier) is one, indicating that there was
                        # indeed only one timespan for each data ID in each
                        # collection that survived the base query's WHERE
                        # clauses and JOINs.
                        if not self.db.has_any_aggregate:
                            raise NotImplementedError(
                                f"Cannot generate query that returns timespan for {dataset_type!r} after a "
                                "find-first search, because this database does not support the ANY_VALUE "
                                "aggregate function (or equivalent)."
                            )
                        select_builder.joins.timespans[dataset_type] = select_builder.joins.timespans[
                            dataset_type
                        ].apply_any_aggregate(self.db.apply_any_aggregate)
                    else:
                        unique_keys.extend(select_builder.joins.timespans[dataset_type].flatten())
                else:
                    # Other dataset fields derive their uniqueness from key
                    # fields.
                    derived_fields.append((dataset_type, dataset_field))
        if not have_aggregates and not derived_fields:
            # SELECT DISTINCT is sufficient.
            select_builder.distinct = True
        # With DISTINCT ON, Postgres requires that the leftmost parts of the
        # ORDER BY match the DISTINCT ON expressions.  It's somewhat tricky to
        # enforce that, so instead we just don't use DISTINCT ON if ORDER BY is
        # present. There may be an optimization opportunity by relaxing this
        # restriction.
        elif not have_aggregates and self.db.has_distinct_on and len(list(order_by)) == 0:
            # SELECT DISTINCT ON is sufficient and supported by this database.
            select_builder.distinct = tuple(unique_keys)
        else:
            # GROUP BY is the only option.
            if derived_fields:
                if self.db.has_any_aggregate:
                    for logical_table, field in derived_fields:
                        if field == "timespan":
                            select_builder.joins.timespans[logical_table] = select_builder.joins.timespans[
                                logical_table
                            ].apply_any_aggregate(self.db.apply_any_aggregate)
                        else:
                            select_builder.joins.fields[logical_table][field] = self.db.apply_any_aggregate(
                                select_builder.joins.fields[logical_table][field]
                            )
                else:
                    _LOG.debug(
                        "Adding %d fields to GROUP BY because this database backend does not support the "
                        "ANY_VALUE aggregate function (or equivalent).  This may result in a poor query "
                        "plan.  Materializing the query first sometimes avoids this problem.  This warning "
                        "can be ignored unless query performance is a problem.",
                        len(derived_fields),
                    )
                    for logical_table, field in derived_fields:
                        if field == "timespan":
                            unique_keys.extend(select_builder.joins.timespans[logical_table].flatten())
                        else:
                            unique_keys.append(select_builder.joins.fields[logical_table][field])
            select_builder.group_by = tuple(unique_keys)

    def apply_query_find_first(
        self,
        select_builder: SqlSelectBuilder,
        postprocessing: Postprocessing,
        find_first_analysis: QueryFindFirstAnalysis,
    ) -> SqlSelectBuilder:
        """Apply the "find first" stage of query construction to a single
        SQL SELECT builder.

        This method is expected to be invoked by
        `QueryBuilder.apply_find_first` implementations.

        Parameters
        ----------
        select_builder : `SqlSelectBuilder`
            Low-level SQL builder for a single SELECT term, consumed on return.
        postprocessing : `Postprocessing`
            Description of query processing that happens in Python after the
            query is executed by the database.
        find_first_analysis : `QueryFindFirstAnalysis`
            Information about the find-first stage gathered during the analysis
            phase of query construction.

        Returns
        -------
        select_builder : `SqlSelectBuilder`
            Low-level SQL builder that includes the find-first logic.
        """
        # The query we're building looks like this:
        #
        # WITH {dst}_base AS (
        #     {target}
        #     ...
        # )
        # SELECT
        #     {dst}_window.*,
        # FROM (
        #     SELECT
        #         {dst}_base.*,
        #         ROW_NUMBER() OVER (
        #             PARTITION BY {dst_base}.{dimensions}
        #             ORDER BY {rank}
        #         ) AS rownum
        #     ) {dst}_window
        # WHERE
        #     {dst}_window.rownum = 1;
        #
        # The outermost SELECT will be represented by the SqlSelectBuilder we
        # return. The SqlSelectBuilder we're given corresponds to the Common
        # Table Expression (CTE) at the top.
        #
        # For SQLite only, we could use a much simpler GROUP BY instead,
        # because it extends the standard to do exactly what we want when MIN
        # or MAX appears once and a column does not have an aggregate function
        # (https://www.sqlite.org/quirks.html).  But since that doesn't work
        # with PostgreSQL it doesn't help us.
        #
        select_builder = select_builder.nested(cte=True, force=True, postprocessing=postprocessing)
        # We start by filling out the "window" SELECT statement...
        partition_by = [
            select_builder.joins.dimension_keys[d][0] for d in select_builder.columns.dimensions.required
        ]
        rank_sql_column = sqlalchemy.case(
            {record.key: n for n, record in enumerate(find_first_analysis.search.collection_records)},
            value=select_builder.joins.fields[find_first_analysis.dataset_type]["collection_key"],
        )
        if partition_by:
            select_builder.joins.special["_ROWNUM"] = sqlalchemy.sql.func.row_number().over(
                partition_by=partition_by, order_by=rank_sql_column
            )
        else:
            select_builder.joins.special["_ROWNUM"] = sqlalchemy.sql.func.row_number().over(
                order_by=rank_sql_column
            )
        # ... and then turn that into a subquery with a constraint on rownum.
        select_builder = select_builder.nested(force=True, postprocessing=postprocessing)
        # We can now add the WHERE constraint on rownum into the outer query.
        select_builder.joins.where(select_builder.joins.special["_ROWNUM"] == 1)
        # Don't propagate _ROWNUM into downstream queries.
        del select_builder.joins.special["_ROWNUM"]
        return select_builder

    def _analyze_collections(self, tree: qt.QueryTree) -> QueryCollectionAnalysis:
        """Fetch and organize information about all collections appearing in a
        query.

        Parameters
        ----------
        tree : `.queries.tree.QueryTree`
            Description of the joins and row filters in the query.

        Returns
        -------
        collection_analysis : `QueryCollectionAnalysis`
            Struct containing collection records and summaries, organized
            for later access by dataset type.
        """
        # Retrieve collection information for all collections in a tree.
        collection_names = set(
            itertools.chain.from_iterable(
                dataset_search.collections for dataset_search in tree.datasets.values()
            )
        )
        if tree.any_dataset is not None:
            collection_names.update(tree.any_dataset.collections)
        collection_records = {
            record.name: record
            for record in self.managers.collections.resolve_wildcard(
                CollectionWildcard.from_names(collection_names), flatten_chains=True, include_chains=True
            )
        }
        non_chain_records = [
            record for record in collection_records.values() if record.type is not CollectionType.CHAINED
        ]
        # Fetch summaries for a subset of dataset types.
        if tree.any_dataset is not None:
            summaries = self.managers.datasets.fetch_summaries(non_chain_records, dataset_types=None)
        else:
            dataset_types = [self.get_dataset_type(dataset_type_name) for dataset_type_name in tree.datasets]
            summaries = self.managers.datasets.fetch_summaries(non_chain_records, dataset_types)
        result = QueryCollectionAnalysis(collection_records=collection_records)
        # Do a preliminary resolution for dataset searches to identify any
        # calibration lookups that might participate in temporal joins.
        for dataset_type_name, dataset_search in tree.iter_all_dataset_searches():
            collection_summaries = self._filter_collections(
                dataset_search.collections, collection_records, summaries
            )
            result.summaries_by_dataset_type[dataset_type_name] = collection_summaries
            resolved_dataset_search = self._resolve_dataset_search(
                dataset_type_name, dataset_search, {}, collection_summaries
            )
            if resolved_dataset_search.is_calibration_search:
                result.calibration_dataset_types.add(dataset_type_name)
        return result

    def _filter_collections(
        self,
        collection_names: Iterable[str],
        records: Mapping[str, CollectionRecord],
        summaries: Mapping[Any, CollectionSummary],
    ) -> list[tuple[CollectionRecord, CollectionSummary]]:
        """Return a subset of collection records and summaries ordered
        according to the input collection list.

        Parameters
        ----------
        collection_names : `~collections.abc.Iterable` [`str`]
            List of collection names.
        records : `~collections.abc.Mapping` [`str`, `CollectionRecord`]
            Mapping of collection names to collection records, must contain
            records for all collections in ``collection_names`` and all their
            children collections.
        summaries : `~collections.abc.Mapping` [`Any`, `CollectionSummary`]
            Mapping of collection IDs to collection summaries, must contain
            summaries for all non-chained collections in the collection tree.

        Returns
        -------
        result `list` [`tuple` [`CollectionRecord`, `CollectionSummary`]]
            Sequence of collection records and their corresponding summaries
            ordered according to the order of input collections and their
            child collections. Does not include chained collections.
        """
        done: set[str] = set()

        def recurse(names: Iterable[str]) -> Iterator[tuple[CollectionRecord, CollectionSummary]]:
            for name in names:
                if name not in done:
                    done.add(name)
                    record = records[name]
                    if record.type is CollectionType.CHAINED:
                        yield from recurse(cast(ChainedCollectionRecord, record).children)
                    else:
                        yield record, summaries[record.key]

        return list(recurse(collection_names))

    def _resolve_dataset_search(
        self,
        dataset_type_name: _T,
        dataset_search: qt.DatasetSearch,
        constraint_data_id: Mapping[str, DataIdValue],
        collections: list[tuple[CollectionRecord, CollectionSummary]],
    ) -> ResolvedDatasetSearch[_T]:
        """Resolve the collections that should actually be searched for
        datasets of a particular type.

        Parameters
        ----------
        dataset_type_name : `str` or ``ANY_DATASET``
            Name of the dataset being searched for.
        dataset_search : `.queries.tree.DatasetSearch`
            Struct holding the dimensions and original collection search path.
        constraint_data_id : `~collections.abc.Mapping`
            Data ID mapping derived from the query predicate that may be used
            to filter out some collections based on their governor dimensions.
        collections : `list` [ `tuple` [ \
                `.registry.interfaces.CollectionRecord`, \
                `.registry.CollectionSummary` ] ]
            Tuples of collection record and summary.

        Returns
        -------
        resolved : `ResolvedDatasetSearch`
            Struct that extends `dataset_search`` with the dataset type name
            and resolved collection records.
        """
        result = ResolvedDatasetSearch(dataset_type_name, dataset_search.dimensions)
        if not collections:
            result.messages.append("No datasets can be found because collection list is empty.")
        for collection_record, collection_summary in collections:
            rejected: bool = False
            if (
                result.name is not qt.ANY_DATASET
                and result.name not in collection_summary.dataset_types.names
            ):
                result.messages.append(
                    f"No datasets of type {result.name!r} in collection {collection_record.name!r}."
                )
                rejected = True
            for governor in (
                constraint_data_id.keys()
                & collection_summary.governors.keys()
                & dataset_search.dimensions.names
            ):
                if constraint_data_id[governor] not in collection_summary.governors[governor]:
                    result.messages.append(
                        f"No datasets with {governor}={constraint_data_id[governor]!r} "
                        f"in collection {collection_record.name!r}."
                    )
                    rejected = True
            if not rejected:
                if collection_record.type is CollectionType.CALIBRATION:
                    result.is_calibration_search = True
                result.collection_records.append(collection_record)
        return result

    def _join_materialization(
        self,
        joins_builder: SqlJoinsBuilder,
        key: qt.MaterializationKey,
        dimensions: DimensionGroup,
    ) -> frozenset[str]:
        """Join a materialization into an under-construction query.

        Parameters
        ----------
        joins_builder : `SqlJoinsBuilder`
            Component of a `SqlSelectBuilder` that holds the FROM and WHERE
            clauses.  This will be modified in-place on return.
        key : `.queries.tree.MaterializationKey`
            Unique identifier created for this materialization when it was
            created.
        dimensions : `DimensionGroup`
            Dimensions of the materialization.

        Returns
        -------
        datasets : `frozenset` [ `str` ]
            Dataset types that were included as constraints when this
            materialization was created.
        """
        columns = qt.ColumnSet(dimensions)
        m_state = self._materializations[key]
        joins_builder.join(
            SqlJoinsBuilder(db=self.db, from_clause=m_state.table).extract_columns(
                columns, m_state.postprocessing
            )
        )
        return m_state.datasets

    @overload
    def join_dataset_search(
        self,
        joins_builder: SqlJoinsBuilder,
        resolved_search: ResolvedDatasetSearch[list[str]],
        fields: Set[str],
        union_dataset_type_name: str,
    ) -> None: ...

    @overload
    def join_dataset_search(
        self,
        joins_builder: SqlJoinsBuilder,
        resolved_search: ResolvedDatasetSearch[str],
        fields: Set[str],
    ) -> None: ...

    def join_dataset_search(
        self,
        joins_builder: SqlJoinsBuilder,
        resolved_search: ResolvedDatasetSearch[Any],
        fields: Set[str],
        union_dataset_type_name: str | None = None,
    ) -> None:
        """Join a dataset search into an under-construction query.

        Parameters
        ----------
        joins_builder : `SqlJoinsBuilder`
            Component of a `SqlSelectBuilder` that holds the FROM and WHERE
            clauses.  This will be modified in-place on return.
        resolved_search : `ResolvedDatasetSearch`
            Struct that describes the dataset type and collections.
        fields : `~collections.abc.Set` [ `str` ]
            Dataset fields to include.
        union_dataset_type_name : `str`, optional
            Dataset type name to use when `resolved_search` represents multiple
            union datasets.
        """
        # The asserts below will need to be dropped (and the implications
        # dealt with instead) if materializations start having dataset fields.
        if union_dataset_type_name is None:
            dataset_type = self.get_dataset_type(cast(str, resolved_search.name))
            assert dataset_type.name not in joins_builder.fields, (
                "Dataset fields have unexpectedly already been joined in."
            )
            assert dataset_type.name not in joins_builder.timespans, (
                "Dataset timespan has unexpectedly already been joined in."
            )
        else:
            dataset_type = self.get_dataset_type(union_dataset_type_name)
            assert qt.ANY_DATASET not in joins_builder.fields, (
                "Union dataset fields have unexpectedly already been joined in."
            )
            assert qt.ANY_DATASET not in joins_builder.timespans, (
                "Union dataset timespan has unexpectedly already been joined in."
            )

        joins_builder.join(
            self.managers.datasets.make_joins_builder(
                dataset_type,
                resolved_search.collection_records,
                fields,
                is_union=(union_dataset_type_name is not None),
            )
        )


@dataclasses.dataclass
class _MaterializationState:
    table: sqlalchemy.Table
    datasets: frozenset[str]
    postprocessing: Postprocessing


class _Cursor:
    """A helper class for managing paged query results and cursor lifetimes.

    This class holds a context manager for the SQLAlchemy cursor object but is
    not itself a context manager.  It always cleans up (i.e. calls its `close`
    method) when it raises an exception or exhausts the cursor, but external
    code is responsible for calling `close` when the cursor is abandoned before
    it is exhausted, including when that happens due to an external exception.

    Parameters
    ----------
    db : `.registry.interface.Database`
        Database to run the query against.
    sql : `sqlalchemy.Executable`
        SQL query to execute.
    postprocessing : `Postprocessing`
        Post-query filtering and checks to perform.
    raw_page_size : `int`
        Maximum number of SQL result rows to return in each page, before
        postprocessing.
    page_converter : `ResultPageConverter`
        Object for converting raw SQL result rows into ResultPage instances.
    """

    def __init__(
        self,
        db: Database,
        sql: sqlalchemy.Executable,
        postprocessing: Postprocessing,
        raw_page_size: int,
        page_converter: ResultPageConverter,
    ):
        self._raw_page_size = raw_page_size
        self._postprocessing = postprocessing
        self._context = db.query(sql, execution_options=dict(yield_per=raw_page_size))
        self._page_converter = page_converter
        self._closed = False
        cursor = self._context.__enter__()
        try:
            self._iterator = cursor.partitions()
        except BaseException:
            self.close(*sys.exc_info())
            raise

    def close(self, exc_type: Any = None, exc_value: Any = None, traceback: Any = None) -> None:
        """Close this cursor.

        Parameters
        ----------
        exc_type : `type`
            Exception type as obtained from `sys.exc_info`, or `None` if there
            was no error.
        exc_value : `BaseException` or `None`
            Exception instance as obtained from `sys.exc_info`, or `None` if
            there was no error.
        traceback : `object`
            Traceback as obtained from `sys.exc_info`, or `None` if there was
            no error.
        """
        if not self._closed:
            self._context.__exit__(exc_type, exc_value, traceback)
            self._closed = True

    def next(self) -> ResultPage | None:
        """Return the next result page from this query.

        When there are no more results after this result page, the `next_page`
        attribute of the returned object is `None` and the cursor will be
        closed.  The cursor is also closed if this method raises an exception.
        """
        if self._closed:
            raise RuntimeError("Cannot continue query result iteration: cursor has been closed")
        try:
            raw_page = next(self._iterator, None)
            if raw_page is None:
                self.close()
                return None

            postprocessed_rows = self._postprocessing.apply(raw_page)
            return self._page_converter.convert(postprocessed_rows)
        except BaseException:
            self.close(*sys.exc_info())
            raise
