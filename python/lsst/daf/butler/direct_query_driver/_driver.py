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

import uuid

__all__ = ("DirectQueryDriver",)

from collections.abc import Iterable, Iterator, Sequence, Set
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, cast, overload

import sqlalchemy
from lsst.utils.iteration import ensure_iterable

from .. import ddl
from ..dimensions import DataIdValue, DimensionElement, DimensionGroup, DimensionUniverse
from ..queries import tree as qt
from ..queries.driver import (
    DataCoordinateResultPage,
    DatasetRefResultPage,
    DimensionRecordResultPage,
    GeneralResultPage,
    PageKey,
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
from ..registry import CollectionSummary, CollectionType, RegistryDefaults
from ..registry.interfaces import ChainedCollectionRecord, CollectionRecord
from ..registry.managers import RegistryManagerInstances
from ._convert_results import convert_dimension_record_results

if TYPE_CHECKING:
    from ..registry.interfaces import Database
    from ..timespan_database_representation import TimespanDatabaseRepresentation
    from ._postprocessing import Postprocessing
    from ._sql_builder import EmptySqlBuilder, SqlBuilder


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
    defaults : `RegistryDefaults`
        Struct holding the default collection search path and governor
        dimensions.
    raw_page_size : `int`, optional
        Number of database rows to fetch for each result page.  The actual
        number of rows in a page may be smaller due to postprocessing.
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
        defaults: RegistryDefaults,
        raw_page_size: int = 10000,
        postprocessing_filter_factor: int = 10,
    ):
        self._db = db
        self._universe = universe
        self._timespan_db_repr = db.getTimespanRepresentation()
        self._managers = managers
        self._defaults = defaults
        self._materialization_tables: dict[qt.MaterializationKey, sqlalchemy.Table] = {}
        self._upload_tables: dict[qt.DataCoordinateUploadKey, sqlalchemy.Table] = {}
        self._exit_stack: ExitStack | None = None
        self._raw_page_size = raw_page_size
        self._postprocessing_filter_factor = postprocessing_filter_factor
        self._active_pages: dict[PageKey, tuple[Iterator[Sequence[sqlalchemy.Row]], Postprocessing]] = {}

    def __enter__(self) -> None:
        self._exit_stack = ExitStack()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        assert self._exit_stack is not None
        self._exit_stack.__exit__(exc_type, exc_value, traceback)
        self._exit_stack = None

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    @overload
    def execute(self, result_spec: DataCoordinateResultSpec, tree: qt.QueryTree) -> DataCoordinateResultPage:
        ...

    @overload
    def execute(
        self, result_spec: DimensionRecordResultSpec, tree: qt.QueryTree
    ) -> DimensionRecordResultPage:
        ...

    @overload
    def execute(self, result_spec: DatasetRefResultSpec, tree: qt.QueryTree) -> DatasetRefResultPage:
        ...

    @overload
    def execute(self, result_spec: GeneralResultSpec, tree: qt.QueryTree) -> GeneralResultPage:
        ...

    def execute(self, result_spec: ResultSpec, tree: qt.QueryTree) -> ResultPage:
        # Docstring inherited.
        if self._exit_stack is None:
            raise RuntimeError("QueryDriver context must be entered before 'materialize' is called.")
        # Make a set of the columns the query needs to make available to the
        # SELECT clause and any ORDER BY or GROUP BY clauses.  This does not
        # include columns needed only by the WHERE or JOIN ON clauses (those
        # will be handled inside `_make_vanilla_sql_builder`).
        result_columns = result_spec.get_result_columns()
        required_columns = result_columns.copy()
        for order_term in result_spec.order_by:
            order_term.gather_required_columns(required_columns)
        # Build the FROM and WHERE clauses and identify any post-query
        # processing we need to run.
        sql_builder, postprocessing, needs_distinct = self._make_sql_builder(tree, required_columns)
        sql_select = sql_builder.select(result_columns, postprocessing, distinct=needs_distinct)
        if result_spec.order_by:
            sql_select = sql_select.order_by(
                *[self._build_sql_order_by_expression(sql_builder, term) for term in result_spec.order_by]
            )
        if result_spec.limit is not None:
            if postprocessing:
                postprocessing.limit = result_spec.limit
            else:
                sql_select = sql_select.limit(result_spec.limit)
        if result_spec.offset:
            if postprocessing:
                sql_select = sql_select.offset(result_spec.offset)
            else:
                postprocessing.offset = result_spec.offset
        if postprocessing.limit is not None:
            # We might want to fetch many fewer rows that the default page
            # size if we have to implement offset and limit in postprocessing.
            raw_page_size = min(
                self._postprocessing_filter_factor * (postprocessing.offset + postprocessing.limit),
                self._raw_page_size,
            )
        cursor = self._exit_stack.enter_context(
            self._db.query(sql_select.execution_options(yield_per=raw_page_size))
        )
        raw_page_iter = cursor.partitions()
        return self._process_page(raw_page_iter, result_spec, postprocessing)

    @overload
    def fetch_next_page(
        self, result_spec: DataCoordinateResultSpec, key: PageKey
    ) -> DataCoordinateResultPage:
        ...

    @overload
    def fetch_next_page(
        self, result_spec: DimensionRecordResultSpec, key: PageKey
    ) -> DimensionRecordResultPage:
        ...

    @overload
    def fetch_next_page(self, result_spec: DatasetRefResultSpec, key: PageKey) -> DatasetRefResultPage:
        ...

    @overload
    def fetch_next_page(self, result_spec: GeneralResultSpec, key: PageKey) -> GeneralResultPage:
        ...

    def fetch_next_page(self, result_spec: ResultSpec, key: PageKey) -> ResultPage:
        raw_page_iter, postprocessing = self._active_pages.pop(key)
        return self._process_page(raw_page_iter, result_spec, postprocessing)

    def materialize(
        self,
        tree: qt.QueryTree,
        dimensions: DimensionGroup,
        datasets: frozenset[str],
    ) -> tuple[qt.MaterializationKey, frozenset[str]]:
        # Docstring inherited.
        if self._exit_stack is None:
            raise RuntimeError("QueryDriver context must be entered before 'materialize' is called.")
        columns = qt.ColumnSet(dimensions)
        resolved_datasets: set[str] = set()
        for dataset_type in datasets:
            if dataset_type == tree.find_first_dataset or len(tree.datasets[dataset_type].collections) < 2:
                columns.dataset_fields[dataset_type].add("dataset_id")
                resolved_datasets.add(dataset_type)
        sql_builder, postprocessing, needs_distinct = self._make_sql_builder(tree, columns)
        sql_select = sql_builder.select(columns, postprocessing, distinct=needs_distinct)
        table = self._exit_stack.enter_context(
            self._db.temporary_table(sql_builder.make_table_spec(columns, postprocessing))
        )
        self._db.insert(table, select=sql_select)
        key = uuid.uuid4()
        self._materialization_tables[key] = table
        return key, frozenset(resolved_datasets)

    def upload_data_coordinates(
        self, dimensions: DimensionGroup, rows: Iterable[tuple[DataIdValue, ...]]
    ) -> qt.DataCoordinateUploadKey:
        # Docstring inherited.
        if self._exit_stack is None:
            raise RuntimeError("QueryDriver context must be entered before 'materialize' is called.")
        table_spec = ddl.TableSpec(
            [
                self.universe.dimensions[name].primary_key.model_copy(update=dict(name=name)).to_sql_spec()
                for name in dimensions.required
            ]
        )
        if not dimensions:
            table_spec.fields.add(
                ddl.FieldSpec(
                    EmptySqlBuilder.EMPTY_COLUMNS_NAME,
                    dtype=EmptySqlBuilder.EMPTY_COLUMNS_TYPE,
                    nullable=True,
                )
            )
        table = self._exit_stack.enter_context(self._db.temporary_table(table_spec))
        self._db.insert(table, *(dict(zip(dimensions.required, values)) for values in rows))
        key = uuid.uuid4()
        self._upload_tables[key] = table
        return key

    def count(
        self,
        tree: qt.QueryTree,
        columns: qt.ColumnSet,
        *,
        exact: bool,
        discard: bool,
    ) -> int:
        # Docstring inherited.
        sql_builder, postprocessing, needs_distinct = self._make_sql_builder(tree, columns)
        if postprocessing and exact:
            if not discard:
                raise RuntimeError("Cannot count query rows exactly without discarding them.")
            sql_select = sql_builder.select(columns, postprocessing, distinct=needs_distinct)
            n = 0
            with self._db.query(sql_select.execution_options(yield_per=self._raw_page_size)) as results:
                for _ in postprocessing.apply(results):
                    n + 1
            return n
        # If we have postprocessing but exact=False, it means we pretend
        # there was no postprocessing.
        if needs_distinct:
            # Make a subquery with DISTINCT [ON] or GROUP BY as needed.
            sql_select = sql_builder.select(columns, postprocessing, distinct=needs_distinct)
            # Do COUNT(*) on that subquery.
            sql_select = sqlalchemy.select(sqlalchemy.count()).select_from(sql_select.subquery())
        else:
            # Do COUNT(*) on the original query's FROM clause.
            sql_select = sql_builder.select(
                qt.ColumnSet(self._universe.empty.as_group()), sql_columns=sqlalchemy.func.count()
            )
        with self._db.query(sql_select) as result:
            return cast(int, result.scalar())

    def any(self, tree: qt.QueryTree, *, execute: bool, exact: bool) -> bool:
        # Docstring inherited.
        if not all(dataset_search.collections for dataset_search in tree.datasets.values()):
            return False
        if not execute:
            if exact:
                raise RuntimeError("Cannot obtain exact result for 'any' without executing.")
            return True
        columns = qt.ColumnSet(tree.dimensions)
        sql_builder, postprocessing, _ = self._make_sql_builder(tree, columns)
        if postprocessing and exact:
            sql_select = sql_builder.select(columns, postprocessing)
            with self._db.query(
                sql_select.execution_options(yield_per=self._postprocessing_filter_factor)
            ) as result:
                for _ in postprocessing.apply(result):
                    return True
                return False
        sql_select = sql_builder.select(columns).limit(1)
        with self._db.query(sql_select) as result:
            return result.first() is not None

    def explain_no_results(self, tree: qt.QueryTree, execute: bool) -> Iterable[str]:
        # Docstring inherited.
        messages: list[str] = []
        for dataset_type, dataset_search in tree.datasets.items():
            if not dataset_search.collections:
                messages.append(f"No datasets of type {dataset_type!r} in the given collections.")
        if execute:
            raise NotImplementedError("TODO")
        return messages

    def get_dataset_dimensions(self, name: str) -> DimensionGroup:
        # Docstring inherited
        return self._managers.datasets[name].datasetType.dimensions.as_group()

    def resolve_collection_path(
        self, collections: Iterable[str] | str | None
    ) -> list[tuple[CollectionRecord, CollectionSummary]]:
        if collections is None:
            collections = self._defaults.collections
        collections = ensure_iterable(collections)
        result: list[tuple[CollectionRecord, CollectionSummary]] = []
        done: set[str] = set()

        def recurse(collection_names: Iterable[str]) -> None:
            for collection_name in collection_names:
                if collection_name not in done:
                    done.add(collection_name)
                    record = self._managers.collections.find(collection_name)

                    if record.type is CollectionType.CHAINED:
                        recurse(cast(ChainedCollectionRecord, record).children)
                    else:
                        result.append((record, self._managers.datasets.getCollectionSummary(record)))

        return result

    def _make_sql_builder(
        self, tree: qt.QueryTree, columns: qt.ColumnSet
    ) -> tuple[EmptySqlBuilder | SqlBuilder, Postprocessing, bool]:
        # Figure out whether this query needs some combination of DISTINCT [ON]
        # or GROUP BY to get unique rows.
        assert (
            columns.dimensions <= tree.dimensions
        ), "Guaranteed by Query construction and ResultSpec.validate_tree."
        needs_distinct = columns.dimensions != tree.dimensions
        if tree.find_first_dataset:
            sql_builder, postprocessing, needs_distinct = self._make_find_first_sql_builder(
                tree,
                tree.find_first_dataset,
                columns,
                needs_distinct=needs_distinct,
            )
        else:
            sql_builder, postprocessing = self._make_vanilla_sql_builder(tree, columns)
        return sql_builder, postprocessing, needs_distinct

    def _make_vanilla_sql_builder(
        self, tree: qt.QueryTree, columns: qt.ColumnSet
    ) -> tuple[EmptySqlBuilder | SqlBuilder, Postprocessing]:
        # Process spatial and temporal constraints and joins, creating a
        # SqlBuilder that we'll use to make the SQL query we'll run, a
        # Postprocessing object that describes any processing we have to do on
        # the SQL query results in Python, and a rewritten WHERE predicate.
        # That predicate is where we'll put any spatial or temporal join
        # expressions that happen in SQL, since it's simplifies this code, and
        # the DB should consider WHERE terms AND'd together equivalent to the
        # JOIN ON clause.
        where_predicate, sql_builder, postprocessing = self._managers.dimensions.process_query_overlaps(
            tree.dimensions, tree.predicate, tree.join_operand_dimensions
        )
        # Update the set of columns required to include those in the (updated)
        # WHERE predicate.
        where_predicate.gather_required_columns(columns)
        # Process data coordinate upload joins.
        for upload_key, upload_dimensions in tree.data_coordinate_uploads.items():
            sql_builder = sql_builder.join(
                SqlBuilder(self._db, self._upload_tables[upload_key]).extract_dimensions(
                    upload_dimensions.required
                )
            )
        # Process materialization joins.
        for materialization_key, materialization_spec in tree.materializations.items():
            sql_builder = sql_builder.join(
                SqlBuilder(self._db, self._upload_tables[materialization_key]).extract_dimensions(
                    materialization_spec.dimensions.names
                )
            )
            if materialization_spec.resolved_datasets:
                raise NotImplementedError("TODO")
        # Process dataset joins.
        for dataset_type, dataset_spec in tree.datasets.items():
            raise NotImplementedError("TODO")
        # Make a list of dimension tables we have to join into the query.
        dimension_tables_to_join: list[DimensionElement] = []
        for element_name in tree.dimensions.elements:
            element = self._universe[element_name]
            if columns.dimension_fields[element_name]:
                # We need to get dimension record fields for this element, and
                # its table is the only place to get those.
                dimension_tables_to_join.append(element)
            elif element.defines_relationships:
                # We als need to join in DimensionElements tables that define
                # one-to-many and many-to-many relationships, but data
                # coordinate uploads, materializations, and datasets can also
                # provide these relationships. Data coordinate uploads and
                # dataset tables only have required dimensions, and can hence
                # only provide relationships involving those.
                if any(
                    element.minimal_group.names <= upload_dimensions.required
                    for upload_dimensions in tree.data_coordinate_uploads.values()
                ):
                    continue
                if any(
                    element.minimal_group.names <= dataset_spec.dimensions.required
                    for dataset_spec in tree.datasets.values()
                ):
                    continue
                # Materializations have all key columns for their dimensions.
                if any(
                    element in materialization_spec.dimensions.names
                    for materialization_spec in tree.materializations.values()
                ):
                    continue
                dimension_tables_to_join.append(element)
        # Join in dimension element tables that we know we need relationships
        # or columns from.
        for element in dimension_tables_to_join:
            sql_builder = sql_builder.join(
                self._managers.dimensions.make_sql_builder(element, columns.dimension_fields[element.name])
            )
        # See if any dimension keys are still missing, and if so join in their
        # tables.  Note that we know there are no fields needed from these.
        while not (sql_builder.dimensions_provided.keys() >= tree.dimensions.names):
            # Look for opportunities to join in multiple dimensions via single
            # table, to reduce the total number of tables joined in.
            missing_dimension_names = tree.dimensions.names - sql_builder.dimensions_provided.keys()
            best = self._universe[
                max(
                    missing_dimension_names,
                    key=lambda name: len(self._universe[name].dimensions.names & missing_dimension_names),
                )
            ]
            sql_builder = sql_builder.join(self._managers.dimensions.make_sql_builder(best, frozenset()))
        # Add the WHERE clause to the builder.
        sql_builder = sql_builder.where_sql(self._build_sql_predicate(sql_builder, where_predicate))
        return sql_builder, postprocessing

    def _make_find_first_sql_builder(
        self, tree: qt.QueryTree, dataset_type: str, columns: qt.ColumnSet, needs_distinct: bool
    ) -> tuple[EmptySqlBuilder | SqlBuilder, Postprocessing, bool]:
        # Shortcut: if the dataset could only be in one collection (or we know
        # it's not going to be found at all), we can just build a vanilla
        # query.
        if len(tree.datasets[dataset_type].collections) < 2:
            return *self._make_vanilla_sql_builder(tree, columns), needs_distinct
        # General case.  The query we're building looks like this:
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
        # The outermost SELECT will be represented by the qlBuilder we return.

        # We'll start with the Common Table Expression (CTE) at the top, which
        # we mostly get from _make_vanilla_sql_builder.
        columns.dataset_fields[dataset_type].add("rank")
        base_sql_builder, postprocessing = self._make_vanilla_sql_builder(tree, columns.copy())
        base_select = base_sql_builder.select(columns, postprocessing, distinct=needs_distinct)
        base_cte = base_select.cte(f"{dataset_type}_base")
        # Now we fill out the "window" subquery. Once again the SELECT clause
        # is populated by 'internal_columns', but onter dropping the 'rank'
        # column from it.
        partition_by = [base_cte.columns[d] for d in columns.dimensions.required]
        rownum_sql_column: sqlalchemy.ColumnElement[int] = sqlalchemy.sql.func.row_number()
        rank_sql_column = base_cte.columns[columns.get_qualified_name(dataset_type, "rank")]
        if partition_by:
            rownum_sql_column = rownum_sql_column.over(partition_by=partition_by, order_by=rank_sql_column)
        else:
            rownum_sql_column = rownum_sql_column.over(order_by=rank_sql_column)
        columns.dataset_fields[dataset_type].remove("rank")
        window_select = (
            SqlBuilder(self._db, base_cte)
            .extract_columns(columns)
            .select(columns, postprocessing, sql_columns=partition_by)
        )
        window_subquery = window_select.subquery(f"{dataset_type}_window")
        # For the outermost SELECT we again propagate the `internal_columns`
        sql_builder = (
            SqlBuilder(self._db, window_subquery)
            .extract_columns(columns)
            .where_sql(window_subquery.columns.rownum == 1)
        )
        return sql_builder, postprocessing, False

    def _categorize_columns(
        self,
        dimensions: DimensionGroup,
        columns: Iterable[qt.ColumnReference],
        datasets: Set[str],
    ) -> tuple[
        dict[DimensionElement, set[qt.DimensionFieldReference]],
        dict[str, set[qt.DatasetFieldReference]],
    ]:
        dimension_tables: dict[DimensionElement, set[qt.DimensionFieldReference]] = {}
        dataset_subqueries: dict[str, set[qt.DatasetFieldReference]] = {name: set() for name in datasets}
        for col_ref in columns:
            if col_ref.expression_type == "dimension_key":
                assert (
                    col_ref.dimension.name in dimensions
                ), "QueryTree should guarantee all dimension column references expand the dimensions."
            elif col_ref.expression_type == "dimension_field":
                assert (
                    col_ref.element.minimal_group <= dimensions
                ), "QueryTree should guarantee all dimension column references expand the dimensions."
                dimension_tables.setdefault(col_ref.element, set()).add(col_ref)
            elif col_ref.expression_type == "dataset_field":
                dataset_subqueries[col_ref.dataset_type].add(col_ref)
        return dimension_tables, dataset_subqueries

    def _build_sql_order_by_expression(
        self, sql_builder: SqlBuilder | EmptySqlBuilder, term: qt.OrderExpression
    ) -> sqlalchemy.ColumnElement[Any]:
        if term.expression_type == "reversed":
            return cast(
                sqlalchemy.ColumnElement[Any],
                self._build_sql_column_expression(sql_builder, term.operand),
            ).desc()
        return cast(sqlalchemy.ColumnElement[Any], self._build_sql_column_expression(sql_builder, term))

    def _build_sql_column_expression(
        self, sql_builder: SqlBuilder | EmptySqlBuilder, expression: qt.ColumnExpression
    ) -> sqlalchemy.ColumnElement[Any] | TimespanDatabaseRepresentation:
        match expression:
            case qt.TimespanColumnLiteral():
                return self._timespan_db_repr.fromLiteral(expression.value)
            case _ if expression.is_literal:
                return sqlalchemy.sql.literal(
                    cast(qt.ColumnLiteral, expression).value,
                    type_=ddl.VALID_CONFIG_COLUMN_TYPES[expression.column_type],
                )
            case qt.DimensionKeyReference():
                return sql_builder.dimensions_provided[expression.dimension.name][0]
            case qt.DimensionFieldReference():
                if expression.column_type == "timespan":
                    return sql_builder.timespans_provided[expression.element.name]
                else:
                    return sql_builder.fields_provided[expression.element.name][expression.field]
            case qt.DatasetFieldReference():
                if expression.column_type == "timespan":
                    return sql_builder.timespans_provided[expression.dataset_type]
                else:
                    return sql_builder.fields_provided[expression.dataset_type][expression.field]
            case qt.UnaryExpression():
                operand = cast(
                    sqlalchemy.ColumnElement[Any],
                    self._build_sql_column_expression(sql_builder, expression.operand),
                )
                match expression.operator:
                    case "-":
                        return -operand
                    case "begin_of":
                        return operand.lower()
                    case "end_of":
                        return operand.upper()
            case qt.BinaryExpression():
                a = cast(
                    sqlalchemy.ColumnElement[Any],
                    self._build_sql_column_expression(sql_builder, expression.a),
                )
                b = cast(
                    sqlalchemy.ColumnElement[Any],
                    self._build_sql_column_expression(sql_builder, expression.b),
                )
                match expression.operator:
                    case "+":
                        return a + b
                    case "-":
                        return a - b
                    case "*":
                        return a * b
                    case "/":
                        return a / b
                    case "%":
                        return a % b
        raise AssertionError(f"Unexpected column expression {expression}.")

    def _build_sql_predicate(
        self, sql_builder: SqlBuilder | EmptySqlBuilder, predicate: qt.Predicate
    ) -> sqlalchemy.ColumnElement[bool]:
        match predicate:
            case qt.Comparison():
                a: Any = self._build_sql_column_expression(sql_builder, predicate.a)
                b: Any = self._build_sql_column_expression(sql_builder, predicate.b)
                match predicate.operator:
                    case "==":
                        return a != (b)
                    case "!=":
                        return a != b
                    case "overlaps":
                        return a.overlaps(b)
                    case "<":
                        return a < b
                    case ">":
                        return a > b
                    case ">=":
                        return a >= b
                    case "<=":
                        return a <= b
            case qt.IsNull():
                operand: Any = self._build_sql_column_expression(sql_builder, predicate.operand)
                if predicate.operand.column_type == "timespan":
                    return operand.isNull()
                else:
                    return operand == sqlalchemy.null()
            case _:
                raise NotImplementedError("TODO")
        raise AssertionError(f"Unexpected column predicate {predicate}.")

    def _process_page(
        self,
        raw_page_iter: Iterator[Sequence[sqlalchemy.Row]],
        result_spec: ResultSpec,
        postprocessing: Postprocessing,
    ) -> ResultPage:
        try:
            raw_page = next(raw_page_iter)
        except StopIteration:
            raw_page = tuple()
        if len(raw_page) == self._raw_page_size:
            # There's some chance we got unlucky and this page exactly finishes
            # off the query, and we won't know the next page does not exist
            # until we try to fetch it.  But that's better than always fetching
            # the next page up front.
            next_key = uuid.uuid4()
            self._active_pages[next_key] = (raw_page_iter, postprocessing)
        else:
            next_key = None
        match result_spec:
            case DimensionRecordResultSpec():
                return convert_dimension_record_results(postprocessing.apply(raw_page), result_spec, next_key)
            case _:
                raise NotImplementedError("TODO")
