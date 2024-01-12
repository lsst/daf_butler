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

from collections.abc import Iterable, Set
from types import EllipsisType
from typing import TYPE_CHECKING, Any, cast, overload

import sqlalchemy

from .. import ddl
from .._dataset_type import DatasetType
from ..dimensions import DataIdValue, DimensionElement, DimensionGroup, DimensionUniverse
from ..queries import tree as qt
from ..queries.data_coordinate_results import DataCoordinateResultPage, DataCoordinateResultSpec
from ..queries.dataset_results import DatasetRefResultPage, DatasetRefResultSpec
from ..queries.dimension_record_results import DimensionRecordResultPage, DimensionRecordResultSpec
from ..queries.driver import PageKey, QueryDriver, ResultPage, ResultSpec
from ..queries.general_results import GeneralResultPage, GeneralResultSpec
from ..registry.managers import RegistryManagerInstances

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
    """

    def __init__(
        self,
        db: Database,
        universe: DimensionUniverse,
        managers: RegistryManagerInstances,
    ):
        self._db = db
        self._universe = universe
        self._timespan_db_repr = db.getTimespanRepresentation()
        self._managers = managers
        self._materialization_tables: dict[qt.MaterializationKey, sqlalchemy.Table] = {}
        self._upload_tables: dict[qt.DataCoordinateUploadKey, sqlalchemy.Table] = {}

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    @overload
    def execute(self, tree: qt.QueryTree, result_spec: DataCoordinateResultSpec) -> DataCoordinateResultPage:
        ...

    @overload
    def execute(
        self, tree: qt.QueryTree, result_spec: DimensionRecordResultSpec
    ) -> DimensionRecordResultPage:
        ...

    @overload
    def execute(self, tree: qt.QueryTree, result_spec: DatasetRefResultSpec) -> DatasetRefResultPage:
        ...

    @overload
    def execute(self, tree: qt.QueryTree, result_spec: GeneralResultSpec) -> GeneralResultPage:
        ...

    def execute(self, tree: qt.QueryTree, result_spec: ResultSpec) -> ResultPage:
        raise NotImplementedError("TODO")

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
        raise NotImplementedError("TODO")

    def materialize(
        self,
        tree: qt.QueryTree,
        dimensions: DimensionGroup,
        datasets: frozenset[str],
    ) -> tuple[qt.MaterializationKey, frozenset[str]]:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def upload_data_coordinates(
        self, dimensions: DimensionGroup, rows: Iterable[tuple[DataIdValue, ...]]
    ) -> qt.DataCoordinateUploadKey:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def count(
        self,
        tree: qt.QueryTree,
        *,
        dimensions: DimensionGroup,
        datasets: frozenset[str],
        exact: bool,
        discard: bool,
    ) -> int:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def any(self, tree: qt.QueryTree, *, execute: bool, exact: bool) -> bool:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def explain_no_results(self, tree: qt.QueryTree, execute: bool) -> Iterable[str]:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def resolve_collection_wildcard(
        self, collections: str | Iterable[str] | EllipsisType | None = None
    ) -> tuple[list[str], bool]:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def resolve_dataset_type_wildcard(
        self, dataset_type: str | DatasetType | Iterable[str] | Iterable[DatasetType] | EllipsisType
    ) -> dict[str, DatasetType]:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def get_dataset_dimensions(self, name: str) -> DimensionGroup:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def _build_sql_select(
        self,
        tree: qt.QueryTree,
        dimensions: DimensionGroup,
        columns_to_select: Set[qt.ColumnReference],
    ) -> tuple[sqlalchemy.Select, Postprocessing]:
        if tree.find_first_dataset:
            sql_builder, postprocessing = self._make_find_first_sql_builder(
                tree,
                tree.find_first_dataset,
                dimensions,
                columns_to_select,
            )
        else:
            sql_builder, postprocessing = self._make_vanilla_sql_builder(tree, columns_to_select)
        # TODO: make results unique over columns_to_select, while taking into
        # account postprocessing columns
        return sql_builder.sql_select(
            columns_to_select,
            postprocessing,
            order_by=[self._build_sql_order_by_expression(sql_builder, term) for term in tree.order_terms],
            limit=tree.limit,
            offset=tree.offset,
        )

    def _make_vanilla_sql_builder(
        self,
        tree: qt.QueryTree,
        columns_required: Set[qt.ColumnReference],
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
        columns_required = set(columns_required)
        columns_required.update(where_predicate.gather_required_columns())
        columns_required.update(postprocessing.gather_columns_required())
        # Now that we're done gathering columns_required, we categorize them
        # into mappings keyed by what kind of table they come from:
        full_dimensions, dimension_tables_to_join, datasets_subqueries_to_join = self._categorize_columns(
            tree.dimensions, columns_required, tree.datasets.keys()
        )
        # From here down, 'columns_required' is no long authoritative.
        del columns_required
        # Process data coordinate upload joins.
        for upload_key, upload_dimensions in tree.data_coordinate_uploads.items():
            sql_builder = sql_builder.join(
                SqlBuilder(self._upload_tables[upload_key]).extract_keys(upload_dimensions.required)
            )
        # Process materialization joins.
        for materialization_key, materialization_spec in tree.materializations.items():
            sql_builder = sql_builder.join(
                SqlBuilder(self._upload_tables[materialization_key]).extract_keys(
                    materialization_spec.dimensions.names
                )
            )
            if materialization_spec.resolved_datasets:
                raise NotImplementedError("TODO")
        # Process dataset joins.
        for dataset_type, dataset_spec in tree.datasets.items():
            raise NotImplementedError("TODO")
        # Record that we need to join in DimensionElements whose tables define
        # one-to-many and many-to-many relationships.  Data coordinate uploads,
        # materializations, and datasets can also provide these relationships.
        for element_name in full_dimensions.elements:
            if (element := self._universe[element_name]).defines_relationships:
                # Data coordinate uploads and dataset tables only have required
                # dimensions, and can hence only provide relationships
                # involving those.
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
                dimension_tables_to_join.setdefault(element, set())
        # Join in dimension element tables that we know we need relationships
        # or columns from.
        for element, fields_for_element in dimension_tables_to_join.items():
            sql_builder = sql_builder.join(
                self._managers.dimensions.make_sql_builder(element, fields_for_element)
            )
        # See if any dimension keys are still missing, and if so join in their
        # tables.  Note that we know there are no fields needed from these.
        while not (sql_builder.dimensions_provided.keys() >= full_dimensions.names):
            # Look for opportunities to join in multiple dimensions at once.
            missing_dimension_names = full_dimensions.names - sql_builder.dimensions_provided.keys()
            best = self._universe[
                max(
                    missing_dimension_names,
                    key=lambda name: len(self._universe[name].dimensions.names & missing_dimension_names),
                )
            ]
            if best.viewOf:
                best = self._universe[best.viewOf]
                full_dimensions = full_dimensions | best.minimal_group
            if not best.hasTable():
                raise NotImplementedError(f"No way to join missing dimension {best.name!r} into query.")
            sql_builder = sql_builder.join(self._managers.dimensions.make_sql_builder(best, frozenset()))
        raise NotImplementedError("TODO")

    def _make_find_first_sql_builder(
        self,
        tree: qt.QueryTree,
        dataset_type: str,
        dimensions: DimensionGroup,
        columns_to_select: Set[qt.ColumnReference],
    ) -> tuple[EmptySqlBuilder | SqlBuilder, Postprocessing]:
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
        #             ORDER BY {operation.rank}
        #         ) AS rownum
        #     ) {dst}_window
        # WHERE
        #     {dst}_window.rownum = 1;
        #

        # We'll start with the Common Table Expression (CTE) at the top, which
        # we mostly get from _build_sql_selet.  Note that we need to use
        # 'columns_required' to populate the SELECT clause list, because this
        # isn't the outermost query, and hence we need to propagate columns
        # we'll use but not return to the user through it.
        rank = qt.DatasetFieldReference.model_construct(dataset_type=dataset_type, field="rank")
        internal_columns = set(columns_to_select)
        internal_columns.add(rank)
        for term in tree.order_terms:
            internal_columns.update(term.gather_required_columns())
        base_sql_builder, postprocessing = self._make_vanilla_sql_builder(tree, internal_columns)
        base_select, postprocessing = base_sql_builder.sql_select(internal_columns, postprocessing)
        internal_columns.update(postprocessing.gather_columns_required())
        base_cte = base_select.cte(f"{dataset_type}_base")
        # Now we fill out the SELECT from the CTE, and the subquery it
        # contains (at the same time, since they have the same columns,
        # aside from the special 'rownum' window-function column). Once
        # again the SELECT clause is populated by 'internal_columns'.
        partition_by = [base_cte.columns[d] for d in dimensions.required]
        rownum_column: sqlalchemy.ColumnElement[int] = sqlalchemy.sql.func.row_number()
        if partition_by:
            rownum_column = rownum_column.over(
                partition_by=partition_by, order_by=base_cte.columns[rank.qualified_name]
            )
        else:
            rownum_column = rownum_column.over(order_by=base_cte.columns[rank.qualified_name])
        window_select, postprocessing = (
            SqlBuilder(base_cte)
            .extract_columns(internal_columns, self._timespan_db_repr)
            .sql_select(internal_columns, postprocessing, sql_columns_to_select=partition_by)
        )
        window_subquery = window_select.subquery(f"{dataset_type}_window")
        sql_builder = (
            SqlBuilder(window_subquery)
            .extract_columns(internal_columns, self._timespan_db_repr)
            .where_sql(window_subquery.columns.rownum == 1)
        )
        return sql_builder, postprocessing

    def _categorize_columns(
        self,
        dimensions: DimensionGroup,
        columns: Iterable[qt.ColumnReference],
        datasets: Set[str],
    ) -> tuple[
        DimensionGroup,
        dict[DimensionElement, set[qt.DimensionFieldReference]],
        dict[str, set[qt.DatasetFieldReference]],
    ]:
        dimension_names = set(dimensions.names)
        dimension_tables: dict[DimensionElement, set[qt.DimensionFieldReference]] = {}
        dataset_subqueries: dict[str, set[qt.DatasetFieldReference]] = {name: set() for name in datasets}
        for col_ref in columns:
            if col_ref.expression_type == "dimension_key":
                dimension_names.add(col_ref.dimension.name)
            elif col_ref.expression_type == "dimension_field":
                # The only field for SkyPix dimensions is their region, and we
                # can always compute those from the ID outside the query
                # system.
                if col_ref.element in dimensions.universe.skypix_dimensions:
                    dimension_names.add(col_ref.element.name)
                else:
                    dimension_tables.setdefault(col_ref.element, set()).add(col_ref)
            elif col_ref.expression_type == "dataset_field":
                dataset_subqueries[col_ref.dataset_type].add(col_ref)
        return self._universe.conform(dimension_names), dimension_tables, dataset_subqueries

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
            case qt.DimensionFieldReference() | qt.DatasetFieldReference():
                if expression.column_type == "timespan":
                    return sql_builder.timespans_provided[expression]
                else:
                    return sql_builder.fields_provided[expression]
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
