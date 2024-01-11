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

from collections.abc import Iterable, Mapping, Set
from types import EllipsisType
from typing import TYPE_CHECKING, Any, cast, overload

import sqlalchemy

from .. import ddl
from .._dataset_type import DatasetType
from ..dimensions import DataIdValue, DimensionElement, DimensionGroup, DimensionUniverse
from ..queries import relation_tree as rt
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
        self._materialization_tables: dict[rt.MaterializationKey, sqlalchemy.Table] = {}
        self._upload_tables: dict[rt.UploadKey, sqlalchemy.Table] = {}

    @property
    def universe(self) -> DimensionUniverse:
        return self._universe

    @overload
    def execute(
        self, tree: rt.RootRelation, result_spec: DataCoordinateResultSpec
    ) -> DataCoordinateResultPage:
        ...

    @overload
    def execute(
        self, tree: rt.RootRelation, result_spec: DimensionRecordResultSpec
    ) -> DimensionRecordResultPage:
        ...

    @overload
    def execute(self, tree: rt.RootRelation, result_spec: DatasetRefResultSpec) -> DatasetRefResultPage:
        ...

    @overload
    def execute(self, tree: rt.RootRelation, result_spec: GeneralResultSpec) -> GeneralResultPage:
        ...

    def execute(self, tree: rt.RootRelation, result_spec: ResultSpec) -> ResultPage:
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

    def materialize(self, tree: rt.RootRelation, dataset_types: frozenset[str]) -> rt.MaterializationKey:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def upload_data_coordinates(
        self, dimensions: DimensionGroup, rows: Iterable[tuple[DataIdValue, ...]]
    ) -> rt.UploadKey:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def count(self, tree: rt.RootRelation, *, exact: bool, discard: bool) -> int:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def any(self, tree: rt.RootRelation, *, execute: bool, exact: bool) -> bool:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def explain_no_results(self, tree: rt.RootRelation, execute: bool) -> Iterable[str]:
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
        tree: rt.RootRelation,
        columns_to_select: Set[rt.ColumnReference],
        *,
        columns_required: Set[rt.ColumnReference] = frozenset(),
        order_by: Iterable[rt.OrderExpression] = (),
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[sqlalchemy.Select, Postprocessing]:
        columns_required = set(columns_required)
        columns_required.update(columns_to_select)
        for term in order_by:
            columns_required.update(term.gather_required_columns())
        match tree:
            case rt.Select():
                sql_builder, postprocessing = self._process_select_tree(tree, columns_required)
                sql_select, postprocessing = sql_builder.sql_select(
                    columns_to_select,
                    postprocessing,
                    order_by=[self._build_sql_order_by_expression(sql_builder, term) for term in order_by],
                    limit=limit,
                    offset=offset,
                )
            case rt.OrderedSlice():
                assert (
                    not order_by and limit is None and not offset
                ), "order_by/limit/offset args are for recursion only"
                sql_select, postprocessing = self._build_sql_select(
                    tree.operand,
                    columns_to_select,
                    columns_required=columns_required,
                    order_by=tree.order_terms,
                    limit=tree.limit,
                    offset=tree.offset,
                )
            case rt.FindFirst():
                sql_builder, postprocessing = self._process_find_first_tree(tree, columns_required)
                sql_select, postprocessing = sql_builder.sql_select(
                    columns_to_select,
                    postprocessing,
                    order_by=[self._build_sql_order_by_expression(sql_builder, term) for term in order_by],
                    limit=limit,
                    offset=offset,
                )
            case _:
                raise AssertionError(f"Invalid root relation: {tree}.")
        return sql_select, postprocessing

    def _process_select_tree(
        self,
        tree: rt.Select,
        columns_required: Set[rt.ColumnReference],
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
            tree.dimensions, tree.where_predicate, [operand.dimensions for operand in tree.join_operands]
        )
        columns_required = set(columns_required)
        columns_required.update(where_predicate.gather_required_columns())
        columns_required.update(postprocessing.gather_columns_required())
        # Now that we're done gathering columns_required, we categorize them
        # into mappings keyed by what kind of table they come from:
        full_dimensions, dimension_tables_to_join, datasets_to_join = self._categorize_columns(
            tree.dimensions, columns_required, tree.available_dataset_types
        )
        # From here down, 'columns_required' is no long authoritative.
        del columns_required
        # Process explicit join operands.  This also returns a set of dimension
        # elements whose tables should be joined in, in order to enforce
        # one-to-many or many-to-many relationships that should be part of this
        # query's dimensions but were not provided by any join operand.
        sql_builder, relationship_elements = self._process_join_operands(
            sql_builder, tree.join_operands, full_dimensions, datasets_to_join
        )
        # Actually join in all of the dimension tables that provide either
        # fields or relationships.
        for element in relationship_elements:
            dimension_tables_to_join.setdefault(element, set())
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

    def _process_find_first_tree(
        self, tree: rt.FindFirst, columns_required: Set[rt.ColumnReference]
    ) -> tuple[EmptySqlBuilder | SqlBuilder, Postprocessing]:
        # The query we're building looks like this:
        #
        # WITH {dst}_search AS (
        #     {target}
        #     ...
        # )
        # SELECT
        #     {dst}_window.*,
        # FROM (
        #     SELECT
        #         {dst}_search.*,
        #         ROW_NUMBER() OVER (
        #             PARTITION BY {dst_search}.{operation.dimensions}
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
        rank_column = rt.DatasetFieldReference.model_construct(dataset_type=tree.dataset_type, field="rank")
        search_select, postprocessing = self._build_sql_select(tree.operand, columns_required | {rank_column})
        columns_required |= postprocessing.gather_columns_required()
        search_cte = search_select.cte(f"{tree.dataset_type}_search")
        # Now we fill out the SELECT from the CTE, and the subquery it
        # contains (at the same time, since they have the same columns,
        # aside from the special 'rownum' window-function column). Once
        # again the SELECT clause is populated by 'columns_required'.
        partition_by = [search_cte.columns[d] for d in tree.dimensions.required]
        rownum_column: sqlalchemy.ColumnElement[int] = sqlalchemy.sql.func.row_number()
        if partition_by:
            rownum_column = rownum_column.over(
                partition_by=partition_by, order_by=search_cte.columns[rank_column.qualified_name]
            )
        else:
            rownum_column = rownum_column.over(order_by=search_cte.columns[rank_column.qualified_name])
        window_select, postprocessing = (
            SqlBuilder(search_cte)
            .extract_keys(tree.dimensions.names)
            .extract_fields(columns_required, self._timespan_db_repr)
            .sql_select(
                columns_required,
                postprocessing,
                sql_columns_to_select=[rownum_column.label("rownum")],
            )
        )
        window_subquery = window_select.subquery(f"{tree.dataset_type}_window")
        return (
            SqlBuilder(window_subquery)
            .extract_keys(tree.dimensions.names)
            .extract_fields(columns_required, self._timespan_db_repr)
            .where_sql(window_subquery.c.rownum == 1)
        ), postprocessing

    def _categorize_columns(
        self,
        dimensions: DimensionGroup,
        columns: Iterable[rt.ColumnReference],
        available_dataset_types: frozenset[str],
    ) -> tuple[
        DimensionGroup,
        dict[DimensionElement, set[rt.DimensionFieldReference]],
        dict[str, set[rt.DatasetFieldReference]],
    ]:
        dimension_names = set(dimensions.names)
        dimension_tables: dict[DimensionElement, set[rt.DimensionFieldReference]] = {}
        datasets: dict[str, set[rt.DatasetFieldReference]] = {name: set() for name in available_dataset_types}
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
                datasets[col_ref.dataset_type].add(col_ref)
        return self._universe.conform(dimension_names), dimension_tables, datasets

    def _process_join_operands(
        self,
        sql_builder: SqlBuilder | EmptySqlBuilder,
        join_operands: Iterable[rt.JoinOperand],
        dimensions: DimensionGroup,
        datasets: Mapping[str, Set[rt.DatasetFieldReference]],
    ) -> tuple[SqlBuilder | EmptySqlBuilder, Set[DimensionElement]]:
        # Make a set of DimensionElements whose tables need to be joined in to
        # make sure the output rows reflect one-to-many and many-to-many
        # relationships.  We'll remove from this as we join in dataset
        # searches, data ID uploads, and materializations, because those can
        # also provide those relationships.
        relationship_elements: set[DimensionElement] = {
            element
            for name in dimensions.elements
            if (element := self._universe[name]).implied or element.alwaysJoin
        }
        for join_operand in join_operands:
            match join_operand:
                case rt.DatasetSearch():
                    # Drop relationship elements whose dimensions are a subset
                    # of the *required* dimensions of the dataset, since
                    # dataset tables only have columns for required dimensions.
                    relationship_elements = {
                        element
                        for element in relationship_elements
                        if not (element.minimal_group.names <= join_operand.dimensions.required)
                    }
                    raise NotImplementedError("TODO")
                case rt.DataCoordinateUpload():
                    sql_builder = sql_builder.join(
                        SqlBuilder(self._upload_tables[join_operand.key]).extract_keys(
                            join_operand.dimensions.required
                        )
                    )
                    # Drop relationship elements whose dimensions are a subset
                    # of the *required* dimensions of the upload, since uploads
                    # only have columns for required dimensions.
                    relationship_elements = {
                        element
                        for element in relationship_elements
                        if not (element.minimal_group.names <= join_operand.dimensions.required)
                    }
                case rt.Materialization():
                    if join_operand.dataset_types:
                        raise NotImplementedError("TODO")
                    sql_builder = sql_builder.join(
                        SqlBuilder(self._materialization_tables[join_operand.key]).extract_keys(
                            join_operand.dimensions.names
                        )
                    )
                    # Drop relationship elements whose dimensions are a subset
                    # dimensions of the materialization, since materializations
                    # have full dimension columns.
                    relationship_elements = {
                        element
                        for element in relationship_elements
                        if not (element.minimal_group <= join_operand.dimensions)
                    }
                case _:
                    raise AssertionError(f"Invalid join operand {join_operand}.")
        return sql_builder, relationship_elements

    def _build_sql_order_by_expression(
        self, sql_builder: SqlBuilder | EmptySqlBuilder, term: rt.OrderExpression
    ) -> sqlalchemy.ColumnElement[Any]:
        if term.expression_type == "reversed":
            return cast(
                sqlalchemy.ColumnElement[Any],
                self._build_sql_column_expression(sql_builder, term.operand),
            ).desc()
        return cast(sqlalchemy.ColumnElement[Any], self._build_sql_column_expression(sql_builder, term))

    def _build_sql_column_expression(
        self, sql_builder: SqlBuilder | EmptySqlBuilder, expression: rt.ColumnExpression
    ) -> sqlalchemy.ColumnElement[Any] | TimespanDatabaseRepresentation:
        match expression:
            case rt.TimespanColumnLiteral():
                return self._timespan_db_repr.fromLiteral(expression.value)
            case _ if expression.is_literal:
                return sqlalchemy.sql.literal(
                    cast(rt.ColumnLiteral, expression).value,
                    type_=ddl.VALID_CONFIG_COLUMN_TYPES[expression.column_type],
                )
            case rt.DimensionKeyReference():
                return sql_builder.dimensions_provided[expression.dimension.name][0]
            case rt.DimensionFieldReference() | rt.DatasetFieldReference():
                if expression.column_type == "timespan":
                    return sql_builder.timespans_provided[expression]
                else:
                    return sql_builder.fields_provided[expression]
            case rt.UnaryExpression():
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
            case rt.BinaryExpression():
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
        self, sql_builder: SqlBuilder | EmptySqlBuilder, predicate: rt.Predicate
    ) -> sqlalchemy.ColumnElement[bool]:
        match predicate:
            case rt.Comparison():
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
            case rt.IsNull():
                operand: Any = self._build_sql_column_expression(sql_builder, predicate.operand)
                if predicate.operand.column_type == "timespan":
                    return operand.isNull()
                else:
                    return operand == sqlalchemy.null()
            case _:
                raise NotImplementedError("TODO")
        raise AssertionError(f"Unexpected column predicate {predicate}.")
