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
from collections.abc import Iterable, Mapping, Set
from types import EllipsisType
from typing import TYPE_CHECKING, ClassVar, cast, overload

import sqlalchemy

from ._dataset_type import DatasetType
from .dimensions import DataIdValue, DimensionElement, DimensionGroup, DimensionUniverse, SkyPixDimension
from .queries import relation_tree as rt
from .queries.data_coordinate_results import DataCoordinateResultPage, DataCoordinateResultSpec
from .queries.dataset_results import DatasetRefResultPage, DatasetRefResultSpec
from .queries.dimension_record_results import DimensionRecordResultPage, DimensionRecordResultSpec
from .queries.driver import PageKey, QueryDriver, ResultPage, ResultSpec
from .queries.general_results import GeneralResultPage, GeneralResultSpec

if TYPE_CHECKING:
    from .registry.interfaces import Database
    from .timespan_database_representation import TimespanDatabaseRepresentation


class DirectQueryDriver(QueryDriver):
    """The `QueryDriver` implementation for `DirectButler`.

    Parameters
    ----------
    db : `Database`
        Abstraction for the SQL database.
    universe : `DimensionUniverse`
        Definitions of all dimensions.
    dimension_tables : `~collections.abc.Mapping` [ `str`, `sqlalchemy.Table` ]
        Tables that hold dimension records.
    dimension_skypix_overlap_tables : `~collections.abc.Mapping` [ `str`, \
            `sqlalchemy.Table` ]
        Many-to-many join tables that relate spatial dimensions to skypix
        systems.
    """

    def __init__(
        self,
        db: Database,
        universe: DimensionUniverse,
        dimension_tables: Mapping[str, sqlalchemy.Table],
        dimension_skypix_overlap_tables: Mapping[str, sqlalchemy.Table],
    ):
        self._db = db
        self._universe = universe
        self._timespan_db_repr = db.getTimespanRepresentation()
        self._dimension_tables = dimension_tables
        self._dimension_skypix_overlap_tables = dimension_skypix_overlap_tables
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
                return sql_builder.sql_select(
                    columns_to_select, postprocessing, order_by=order_by, limit=limit, offset=offset
                )
            case rt.OrderedSlice():
                assert (
                    not order_by and limit is None and not offset
                ), "order_by/limit/offset args are for recursion only"
                return self._build_sql_select(
                    tree.operand,
                    columns_to_select,
                    columns_required=columns_required,
                    order_by=tree.order_terms,
                    limit=tree.limit,
                    offset=tree.offset,
                )
            case rt.FindFirst():
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

                # We'll start with the Common Table Expression (CTE) at the
                # top, which we mostly get from recursing on the FindFirst
                # relation's operand.  Note that we need to use
                # 'columns_required' to populate the SELECT clause list,
                # because this isn't the outermost query.
                rank_column = rt.DatasetFieldReference.model_construct(
                    dataset_type=tree.dataset_type, field="rank"
                )
                search_select, postprocessing = self._build_sql_select(
                    tree.operand, columns_required | {rank_column}
                )
                columns_required.update(postprocessing.gather_columns_required())
                search_cte = search_select.cte(f"{tree.dataset_type}_search")
                # Now we fill out the SELECT from the CTE, and the subquery it
                # contains (at the same time, since they have the same columns,
                # aside from the special 'rownum' window-function column). Once
                # again the SELECT clause is populated by 'columns_required'.
                partition_by = [search_cte.columns[d] for d in tree.dimensions.required]
                rownum_column = sqlalchemy.sql.func.row_number()
                if partition_by:
                    rownum_column = rownum_column.over(
                        partition_by=partition_by, order_by=search_cte.columns[rank_column.qualified_name]
                    )
                else:
                    rownum_column = rownum_column.over(
                        order_by=search_cte.columns[rank_column.qualified_name]
                    )
                window_select, postprocessing = (
                    SqlBuilder(search_cte)
                    .with_data_coordinate_columns(tree.dimensions.names)
                    .with_qualified_name_fields(columns_required, self._timespan_db_repr)
                    .sql_select(
                        columns_required,
                        postprocessing,
                        sql_columns_to_select=[rownum_column.label("rownum")],
                    )
                )
                window_subquery = window_select.subquery(f"{tree.dataset_type}_window")
                # Finally we make the outermost select, which is where we put
                # the order_by, limit, and offset clauses, and use the actual
                # 'columns_to_select` list for the SELECT clause.
                full_select, postprocessing = (
                    SqlBuilder(window_subquery)
                    .with_data_coordinate_columns(tree.dimensions.names)
                    .with_qualified_name_fields(columns_required, self._timespan_db_repr)
                    .sql_select(
                        columns_to_select, postprocessing, order_by=order_by, limit=limit, offset=offset
                    )
                )
                return full_select.where(window_subquery.columns["rownum"] == 1), postprocessing
        raise AssertionError(f"Invalid root relation: {tree}.")

    def _process_select_tree(
        self, tree: rt.Select, columns_required: Set[rt.ColumnReference]
    ) -> tuple[EmptySqlBuilder | SqlBuilder, Postprocessing]:
        columns_required = set(columns_required)
        # We start by processing spatial joins by joining together overlap
        # tables and adding any postprocess filtering.  We do this first
        # because that postprocessing can require additional columns to be
        # included in the SQL query, and that can affect what else we join in.
        sql_builder, postprocessing = self._process_spatial_joins(tree.complete_spatial_joins())
        # Process temporal joins next, but only to put them in a bidirectional
        # map and add their timespans to the columns_required set.
        temporal_join_map: dict[str, list[rt.DimensionFieldReference | rt.DatasetFieldReference]] = {}
        for name_pair in tree.complete_temporal_joins():
            for i in (False, True):
                col_ref: rt.DimensionFieldReference | rt.DatasetFieldReference
                if element := self._universe.get(name_pair[i]):
                    col_ref = rt.DimensionFieldReference.model_construct(element=element, field="timespan")
                else:
                    col_ref = rt.DatasetFieldReference.model_construct(
                        dataset_type=name_pair[i], field="timespan"
                    )
                columns_required.add(col_ref)
                temporal_join_map.setdefault(name_pair[not i], []).append(col_ref)
        # Gather more required columns, then categorize them by where they need
        # to come from.
        columns_required.update(postprocessing.gather_columns_required())
        for predicate in tree.where_terms:
            columns_required.update(predicate.gather_required_columns())
        fields = CategorizedFields().categorize(
            columns_required, tree.dimensions, tree.available_dataset_types
        )
        del columns_required
        # Process explicit join terms.
        for join_operand in tree.join_operands:
            match join_operand:
                case rt.DatasetSearch():
                    raise NotImplementedError("TODO")
                case rt.DataCoordinateUpload():
                    sql_builder = sql_builder.join(
                        SqlBuilder(self._upload_tables[join_operand.key]).with_data_coordinate_columns(
                            join_operand.dimensions.required
                        )
                    )
                case rt.Materialization():
                    if join_operand.dataset_types:
                        raise NotImplementedError("TODO")
                    sql_builder = sql_builder.join(
                        SqlBuilder(
                            self._materialization_tables[join_operand.key]
                        ).with_data_coordinate_columns(join_operand.dimensions.names)
                    )
        for element, fields_for_element in fields.dimension_fields.items():
            assert fields_for_element, "element should be absent if it does not provide any required fields"
            element_sql_builder = SqlBuilder(
                self._dimension_tables[element.name]
            ).with_dimension_record_columns(element, fields_for_element, self._timespan_db_repr)
            sql_builder = sql_builder.join(
                element_sql_builder,
                sql_join_on=[
                    temporal_join_sql_column.overlaps(
                        element_sql_builder.timespans_provided[
                            rt.DimensionFieldReference.model_construct(element=element, field="timespan")
                        ]
                    )
                    for temporal_join_col_ref in temporal_join_map.get(element.name, [])
                    if (temporal_join_sql_column := sql_builder.timespans_provided.get(temporal_join_col_ref))
                    is not None
                ],
            )
        raise NotImplementedError("TODO")

    def _process_spatial_joins(
        self, spatial_joins: Set[rt.JoinTuple]
    ) -> tuple[EmptySqlBuilder | SqlBuilder, Postprocessing]:
        # Set up empty output objects to update to update or replace as we go.
        sql_builder: EmptySqlBuilder | SqlBuilder = EmptySqlBuilder()
        postprocessing = Postprocessing()
        for name_pair in spatial_joins:
            table_pair = (
                self._dimension_skypix_overlap_tables.get(name_pair[0]),
                self._dimension_skypix_overlap_tables.get(name_pair[1]),
            )
            sql_columns: list[sqlalchemy.ColumnElement] = []
            where_terms: list[sqlalchemy.ColumnElement] = []
            for name, table in zip(name_pair, table_pair):
                if table:
                    for dimension_name in self._universe[name].required.names:
                        sql_columns.append(table.columns[dimension_name].label(dimension_name))
                    where_terms.append(table.c.skypix_system == self._universe.commonSkyPix.system.name)
                    where_terms.append(table.c.skypix_level == self._universe.commonSkyPix.level)
                elif name != self._universe.commonSkyPix.name:
                    raise NotImplementedError(
                        f"Only {self._universe.commonSkyPix.name} and non-skypix dimensions "
                        "can participate in spatial joins."
                    )
            from_clause: sqlalchemy.FromClause
            if table_pair[0] and table_pair[1]:
                from_clause = table_pair[0].join(
                    table_pair[1], onclause=(table_pair[0].c.skypix_index == table_pair[1].c.skypix_index)
                )
                postprocessing.spatial_join_filtering.append(
                    (self._universe[name_pair[0]], self._universe[name_pair[1]])
                )
            elif table_pair[0]:
                from_clause = table_pair[0]
                sql_columns.append(table_pair[0].c.skypix_index.label(name_pair[1]))
            elif table_pair[1]:
                from_clause = table_pair[1]
                sql_columns.append(table_pair[1].c.skypix_index.label(name_pair[0]))
            else:
                raise AssertionError("Should be impossible due to join validation.")
            sql_builder = sql_builder.join(
                SqlBuilder(
                    sqlalchemy.select(*sql_columns)
                    .select_from(from_clause)
                    .where(*where_terms)
                    .subquery(f"{name_pair[0]}_{name_pair[1]}_overlap")
                )
            )
        return sql_builder, postprocessing


@dataclasses.dataclass
class CategorizedFields:
    dimension_fields: dict[DimensionElement, list[rt.DimensionFieldReference]] = dataclasses.field(
        default_factory=dict
    )
    dataset_fields: dict[str, list[rt.DatasetFieldReference]] = dataclasses.field(default_factory=dict)

    def categorize(
        self,
        columns_required: Iterable[rt.ColumnReference],
        dimensions: DimensionGroup,
        dataset_types: Set[str],
    ) -> CategorizedFields:
        only_dataset_type: str | None = None
        for col_ref in columns_required:
            if col_ref.expression_type == "dimension_key":
                assert col_ref.dimension.name in dimensions
            elif col_ref.expression_type == "dimension_field":
                self.dimension_fields.setdefault(col_ref.element, []).append(col_ref)
            elif col_ref.expression_type == "dataset_field":
                if col_ref.dataset_type is ...:
                    if only_dataset_type is None:
                        if len(dataset_types) > 1:
                            raise ValueError(
                                f"Reference to dataset field {col_ref} with no dataset type is " "ambiguous."
                            )
                        elif not dataset_types:
                            raise ValueError(f"No datasets in query for reference to ataset field {col_ref}.")
                        (only_dataset_type,) = dataset_types
                    col_ref = col_ref.model_copy(update={"dataset_type": only_dataset_type})
                self.dataset_fields.setdefault(cast(str, col_ref.dataset_type), []).append(col_ref)
        return self


@dataclasses.dataclass
class BaseSqlBuilder:
    dimensions_provided: dict[str, list[sqlalchemy.ColumnElement]] = dataclasses.field(
        default_factory=dict, kw_only=True
    )

    fields_provided: dict[
        rt.DimensionFieldReference | rt.DatasetFieldReference, sqlalchemy.ColumnElement
    ] = dataclasses.field(default_factory=dict, kw_only=True)

    timespans_provided: dict[
        rt.DimensionFieldReference | rt.DatasetFieldReference, TimespanDatabaseRepresentation
    ] = dataclasses.field(default_factory=dict, kw_only=True)


@dataclasses.dataclass
class EmptySqlBuilder(BaseSqlBuilder):
    EMPTY_COLUMNS_NAME: ClassVar[str] = "IGNORED"
    """Name of the column added to a SQL ``SELECT`` query in order to represent
    relations that have no real columns.
    """

    EMPTY_COLUMNS_TYPE: ClassVar[type] = sqlalchemy.Boolean
    """Type of the column added to a SQL ``SELECT`` query in order to represent
    relations that have no real columns.
    """

    def join(self, other: SqlBuilder, sql_join_on: Iterable[sqlalchemy.ColumnElement] = ()) -> SqlBuilder:
        return other

    def sql_select(
        self,
        columns_to_select: Iterable[rt.ColumnReference],
        postprocessing: Postprocessing,
        *,
        sql_columns_to_select: Iterable[sqlalchemy.ColumnElement] = (),
        order_by: Iterable[rt.OrderExpression] = (),
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[sqlalchemy.Select, Postprocessing]:
        assert not columns_to_select
        assert not postprocessing
        assert not order_by
        result = sqlalchemy.select(*self.handle_empty_columns([]), *sql_columns_to_select)
        if offset > 0 or limit == 0:
            result = result.where(sqlalchemy.literal(False))
        return result, postprocessing

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


@dataclasses.dataclass
class SqlBuilder(BaseSqlBuilder):
    sql_from_clause: sqlalchemy.FromClause

    def with_data_coordinate_columns(self, dimensions: Iterable[str]) -> SqlBuilder:
        for dimension_name in dimensions:
            self.dimensions_provided[dimension_name] = [self.sql_from_clause.columns[dimension_name]]
        return self

    def with_dimension_record_columns(
        self,
        element: DimensionElement,
        fields: Iterable[rt.DimensionFieldReference],
        timespan_db_repr: type[TimespanDatabaseRepresentation],
    ) -> SqlBuilder:
        for dimension_name, column_name in zip(element.required.names, element.schema.required.names):
            self.dimensions_provided[dimension_name] = [self.sql_from_clause.columns[column_name]]
        self.with_data_coordinate_columns(element.implied.names)
        for col_ref in fields:
            if col_ref.column_type == "timespan":
                self.timespans_provided[col_ref] = timespan_db_repr.from_columns(
                    self.sql_from_clause.columns, col_ref.field
                )
            else:
                self.fields_provided[col_ref] = self.sql_from_clause.columns[col_ref.field]
        return self

    def with_qualified_name_fields(
        self, fields: Iterable[rt.ColumnReference], timespan_db_repr: type[TimespanDatabaseRepresentation]
    ) -> SqlBuilder:
        for col_ref in fields:
            if col_ref.expression_type == "dimension_key":
                # Logic branch is for MyPy: rule out DimensionKeyReference.
                pass
            elif col_ref.column_type == "timespan":
                self.timespans_provided[col_ref] = timespan_db_repr.from_columns(
                    self.sql_from_clause.columns, col_ref.qualified_name
                )
            else:
                self.fields_provided[col_ref] = self.sql_from_clause.columns[col_ref.qualified_name]
        return self

    def join(self, other: SqlBuilder, sql_join_on: Iterable[sqlalchemy.ColumnElement] = ()) -> SqlBuilder:
        join_on: list[sqlalchemy.ColumnElement] = list(sql_join_on)
        for dimension_name in self.dimensions_provided.keys() & other.dimensions_provided.keys():
            for column1, column2 in itertools.product(
                self.dimensions_provided[dimension_name], other.dimensions_provided[dimension_name]
            ):
                join_on.append(column1 == column2)
            self.dimensions_provided[dimension_name].extend(other.dimensions_provided[dimension_name])
        self.sql_from_clause = self.sql_from_clause.join(
            other.sql_from_clause, onclause=sqlalchemy.and_(*join_on)
        )
        return self

    def sql_select(
        self,
        columns_to_select: Iterable[rt.ColumnReference],
        postprocessing: Postprocessing,
        *,
        sql_columns_to_select: Iterable[sqlalchemy.ColumnElement] = (),
        order_by: Iterable[rt.OrderExpression] = (),
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[sqlalchemy.Select, Postprocessing]:
        # Build the list of columns for the SELECT clause itself.
        sql_columns: list[sqlalchemy.ColumnElement] = []
        columns_to_select = list(set(columns_to_select) | postprocessing.gather_columns_required())
        for col_ref in columns_to_select:
            if col_ref.expression_type == "dimension_key":
                sql_columns.append(
                    self.dimensions_provided[col_ref.dimension.name][0].label(col_ref.dimension.name)
                )
            elif col_ref.column_type == "timespan":
                sql_columns.extend(self.timespans_provided[col_ref].flatten())
            else:
                sql_columns.append(self.fields_provided[col_ref])
        sql_columns.extend(sql_columns_to_select)
        EmptySqlBuilder.handle_empty_columns(sql_columns)
        result = sqlalchemy.select(*sql_columns).select_from(self.sql_from_clause)
        # Add ORDER BY, LIMIT, and OFFSET clauses as appropriate.
        if order_by:
            result = result.order_by(*[self.build_sql_order_by_expression(term) for term in order_by])
        if not postprocessing.can_remove_rows:
            if offset:
                result = result.offset(offset)
            if limit is not None:
                result = result.limit(limit)
        else:
            postprocessing.limit = limit
            postprocessing.offset = offset
        return result, postprocessing

    def build_sql_order_by_expression(self, term: rt.OrderExpression) -> sqlalchemy.ColumnElement:
        if term.expression_type == "reversed":
            return self.build_sql_column_expression(term.operand).desc()
        return self.build_sql_column_expression(term)

    def build_sql_column_expression(self, expression: rt.ColumnExpression) -> sqlalchemy.ColumnElement:
        raise NotImplementedError("TODO")

    def build_sql_predicate(self, predicate: rt.Predicate) -> sqlalchemy.ColumnElement:
        raise NotImplementedError("TODO")


@dataclasses.dataclass
class Postprocessing:
    skypix_regions_provided: set[SkyPixDimension] = dataclasses.field(default_factory=set)
    spatial_join_filtering: list[tuple[DimensionElement, DimensionElement]] = dataclasses.field(
        default_factory=list
    )
    limit: int | None = None
    offset: int = 0

    def gather_columns_required(self) -> Set[rt.ColumnReference]:
        result: set[rt.ColumnReference] = set()
        for element in itertools.chain.from_iterable(self.spatial_join_filtering):
            result.add(rt.DimensionFieldReference.model_construct(element=element, field="region"))
        for dimension in self.skypix_regions_provided:
            result.add(rt.DimensionKeyReference.model_construct(dimension=dimension))
        return result

    @property
    def can_remove_rows(self) -> bool:
        return bool(self.offset) or self.limit is not None

    def __bool__(self) -> bool:
        return bool(
            self.skypix_regions_provided
            or self.spatial_join_filtering
            or self.limit is not None
            or self.offset
        )
