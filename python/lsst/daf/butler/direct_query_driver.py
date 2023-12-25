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
from lsst.sphgeom import Region

from ._dataset_type import DatasetType
from .dimensions import DataIdValue, DimensionElement, DimensionGroup, DimensionUniverse
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
                sql_select, postprocessing = sql_builder.sql_select(
                    columns_to_select,
                    postprocessing,
                    order_by=order_by,
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
                    order_by=order_by,
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
        columns_required = set(columns_required)
        # A pair of empty-for-now objects that represent the query we're
        # building: the SQL query, and any post-SQL processing we'll do in
        # Python.
        sql_builder: SqlBuilder | EmptySqlBuilder = EmptySqlBuilder()
        postprocessing = Postprocessing()
        # Process temporal joins, but only to put them in a bidirectional
        # map for easier lookup later and mark their timespans as required
        # columns.
        temporal_join_map = self._make_temporal_join_map(tree)
        columns_required.update(itertools.chain.from_iterable(temporal_join_map.values()))
        # Process spatial constraints and joins first, since those define the
        # postprocessing we'll need to do we need to know about the columns
        # that will involve early.  This can also rewrite the WHERE clause
        # terms, by transforming "column overlaps literal region" comparisons
        # into a combination of "common skypix in (<ranges>)" and
        # postprocessing, and since there's no guarantee common skypix is in
        # tree.dimensions, it can update the dimensions of the query, too.
        full_dimensions, processed_where_terms = self._process_spatial(sql_builder, postprocessing, tree)
        columns_required.update(postprocessing.gather_columns_required())
        # Now that we're done gathering columns_required, we categorize them
        # into mappings keyed by what kind of table they come from:
        dimension_tables_to_join, datasets_to_join = self._categorize_columns(
            full_dimensions, columns_required, tree.available_dataset_types
        )
        # From here down, 'columns_required' is no long authoritative.
        del columns_required
        # Process explicit join operands.  This also returns a set of dimension
        # elements whose tables should be joined in in order to enforce
        # one-to-many or many-to-many relationships that should be part of this
        # query's dimensions but were not provided by any join operand.
        sql_builder, relationship_elements = self._process_join_operands(
            sql_builder, tree.join_operands, full_dimensions, temporal_join_map, datasets_to_join
        )
        # Actually join in all of the dimension tables that provide either
        # fields or relationships.
        for element in relationship_elements:
            dimension_tables_to_join.setdefault(element, set())
        for element, fields_for_element in dimension_tables_to_join.items():
            sql_builder = self._join_dimension_table(
                sql_builder, element, temporal_join_map, fields_for_element
            )
        # See if any dimension keys are still missing, and if so join in their
        # tables.  Note that we know there are no fields needed from these,
        # and no temporal joins in play.
        missing_dimension_names = full_dimensions.names - sql_builder.dimensions_provided.keys()
        while missing_dimension_names:
            # Look for opportunities to join in multiple dimensions at once.
            best = self._universe[
                max(
                    missing_dimension_names,
                    key=lambda name: len(self._universe[name].dimensions.names & missing_dimension_names),
                )
            ]
            if best.viewOf:
                best = self._universe[best.viewOf]
            elif not best.hasTable():
                raise NotImplementedError(f"No way to join missing dimension {best.name!r} into query.")
            sql_builder = self._join_dimension_table(sql_builder, best, {})
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

        # We'll start with the Common Table Expression (CTE) at the
        # top, which we mostly get from recursing on the FindFirst
        # relation's operand.  Note that we need to use
        # 'columns_required' to populate the SELECT clause list,
        # because this isn't the outermost query.
        rank_column = rt.DatasetFieldReference.model_construct(dataset_type=tree.dataset_type, field="rank")
        search_select, postprocessing = self._build_sql_select(tree.operand, columns_required | {rank_column})
        columns_required |= postprocessing.gather_columns_required()
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

    def _process_spatial(
        self,
        sql_builder: SqlBuilder | EmptySqlBuilder,
        postprocessing: Postprocessing,
        tree: rt.Select,
    ) -> tuple[DimensionGroup, list[rt.Predicate]]:
        processed_where_terms: list[rt.Predicate] = []
        dimensions = tree.dimensions
        for where_term in tree.where_terms:
            processed_where_terms.append(where_term)
            raise NotImplementedError(
                "TODO: extract overlaps and turn them into postprocessing + common skypix ID tests."
            )
        # Add automatic spatial joins to connect all spatial dimensions.
        spatial_joins = rt.joins.complete_joins(
            dimensions,
            [operand.dimensions.spatial for operand in tree.join_operands],
            tree.spatial_joins,
            "spatial",
        )
        # Categorize the joins into:
        # - joins that directly involve the "common" skypix dimension and
        #   a non-skypix dimension, so direct overlaps are in the database;
        common_skypix_joins: set[DimensionElement] = set()
        # - joins that don't involve any skypix dimension, but will have to use
        #   the common skypix as an intermediate and then use post-query
        #   spatial region filtering;
        postprocess_filter_joins: set[tuple[DimensionElement, DimensionElement]] = set()
        # - joins that involve a skypix dimension other than the common one;
        #   we don't support these, but we hope to in the future.
        for name_a, name_b in spatial_joins:
            if (
                self._universe.commonSkyPix.name == name_a
                and name_b not in self._universe.skypix_dimensions.names
            ):
                common_skypix_joins.add(self._universe[name_b])
            elif (
                self._universe.commonSkyPix.name == name_b
                and name_a not in self._universe.skypix_dimensions.names
            ):
                common_skypix_joins.add(self._universe[name_a])
            elif name_a in self._universe.skypix_dimensions.names:
                raise NotImplementedError(
                    f"Joins to skypix dimensions other than {self._universe.commonSkyPix.name} "
                    "are not yet supported."
                )
            elif name_b in self._universe.skypix_dimensions.names:
                raise NotImplementedError(
                    f"Joins to skypix dimensions other than {self._universe.commonSkyPix.name} "
                    "are not yet supported."
                )
            else:
                postprocess_filter_joins.add((self._universe[name_a], self._universe[name_b]))
        done: set[DimensionElement] = set()
        # Join in overlap tables for fully in-database joins to common skypix.
        for element in common_skypix_joins:
            sql_builder = self._join_skypix_overlap(sql_builder, element)
            done.add(element)
        # Join in overlap tables for in-database + postprocess-filtered joins
        # with mediated by common skypix.
        for element_a, element_b in postprocess_filter_joins:
            if element_a not in done:
                sql_builder = self._join_skypix_overlap(sql_builder, element_a)
                done.add(element_a)
            if element_b not in done:
                sql_builder = self._join_skypix_overlap(sql_builder, element_b)
                done.add(element_b)
            postprocessing.spatial_join_filtering.append((element_a, element_b))
        return dimensions, processed_where_terms

    def _make_temporal_join_map(
        self, tree: rt.Select
    ) -> Mapping[str, list[rt.DimensionFieldReference | rt.DatasetFieldReference]]:
        temporal_join_map: dict[str, list[rt.DimensionFieldReference | rt.DatasetFieldReference]] = {}
        for name_pair in rt.joins.complete_joins(
            tree.dimensions,
            [operand.dimensions.temporal for operand in tree.join_operands],
            tree.temporal_joins,
            "temporal",
        ):
            for i in (False, True):
                col_ref: rt.DimensionFieldReference | rt.DatasetFieldReference
                if element := self._universe.get(name_pair[i]):
                    col_ref = rt.DimensionFieldReference.model_construct(element=element, field="timespan")
                else:
                    col_ref = rt.DatasetFieldReference.model_construct(
                        dataset_type=name_pair[i], field="timespan"
                    )
                temporal_join_map.setdefault(name_pair[not i], []).append(col_ref)
        return temporal_join_map

    def _categorize_columns(
        self,
        dimensions: DimensionGroup,
        columns: Iterable[rt.ColumnReference],
        available_dataset_types: frozenset[str],
    ) -> tuple[
        dict[DimensionElement, set[rt.DimensionFieldReference]],
        dict[str, set[rt.DatasetFieldReference]],
    ]:
        only_dataset_type: str | None = None
        dimension_tables: dict[DimensionElement, set[rt.DimensionFieldReference]] = {}
        datasets: dict[str, set[rt.DatasetFieldReference]] = {name: set() for name in available_dataset_types}
        for col_ref in columns:
            if col_ref.expression_type == "dimension_key":
                assert col_ref.dimension.name in dimensions
            elif col_ref.expression_type == "dimension_field":
                # The only field for SkyPix dimensions is their region, and we
                # can always compute those from the ID outside the query
                # system.
                if col_ref.element in dimensions.universe.skypix_dimensions:
                    assert col_ref.element.name in dimensions
                else:
                    dimension_tables.setdefault(col_ref.element, set()).add(col_ref)
            elif col_ref.expression_type == "dataset_field":
                if col_ref.dataset_type is ...:
                    if only_dataset_type is None:
                        if len(datasets) > 1:
                            raise ValueError(
                                f"Reference to dataset field {col_ref} with no dataset type is " "ambiguous."
                            )
                        elif not datasets:
                            raise ValueError(f"No datasets in query for reference to ataset field {col_ref}.")
                        (only_dataset_type,) = datasets
                    col_ref = col_ref.model_copy(update={"dataset_type": only_dataset_type})
                datasets[cast(str, col_ref.dataset_type)].add(col_ref)
        return dimension_tables, datasets

    def _process_join_operands(
        self,
        sql_builder: SqlBuilder | EmptySqlBuilder,
        join_operands: Iterable[rt.JoinOperand],
        dimensions: DimensionGroup,
        temporal_join_map: Mapping[str, Iterable[rt.DimensionFieldReference | rt.DatasetFieldReference]],
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

    def _join_dimension_table(
        self,
        sql_builder: SqlBuilder | EmptySqlBuilder,
        element: DimensionElement,
        temporal_join_map: Mapping[str, Iterable[rt.DimensionFieldReference | rt.DatasetFieldReference]],
        fields: Set[rt.DimensionFieldReference] = frozenset(),
    ) -> SqlBuilder:
        table = self._dimension_tables[element.name]
        element_sql_builder = SqlBuilder(table)
        for dimension_name, column_name in zip(element.required.names, element.schema.required.names):
            element_sql_builder.dimensions_provided[dimension_name] = [table.columns[column_name]]
        element_sql_builder.extract_keys(element.implied.names)
        sql_join_on: list[sqlalchemy.ColumnElement[bool]] = []
        for col_ref in fields:
            if col_ref.column_type == "timespan":
                timespan = self._timespan_db_repr.from_columns(table.columns, col_ref.field)
                element_sql_builder.timespans_provided[col_ref] = timespan
                sql_join_on.extend(
                    [
                        temporal_join_sql_column.overlaps(timespan)
                        for temporal_join_col_ref in temporal_join_map.get(element.name, [])
                        if (
                            temporal_join_sql_column := sql_builder.timespans_provided.get(
                                temporal_join_col_ref
                            )
                        )
                    ]
                )
            else:
                element_sql_builder.fields_provided[col_ref] = table.columns[col_ref.field]
        return sql_builder.join(element_sql_builder, sql_join_on=sql_join_on)

    def _join_skypix_overlap(
        self, sql_builder: SqlBuilder | EmptySqlBuilder, element: DimensionElement
    ) -> SqlBuilder:
        table = self._dimension_skypix_overlap_tables[element.name]
        overlap_sql_builder = (
            SqlBuilder(table)
            .extract_keys(element.required.names)
            .where_sql(
                table.c.skypix_system == element.universe.commonSkyPix.system.name,
                table.c.skypix_level == element.universe.commonSkyPix.level,
            )
        )
        overlap_sql_builder.dimensions_provided[element.universe.commonSkyPix.name] = [
            table.c.skypix_index.label(element.universe.commonSkyPix.name)
        ]
        return sql_builder.join(overlap_sql_builder)


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


@dataclasses.dataclass
class EmptySqlBuilder(BaseSqlBuilder):
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


@dataclasses.dataclass
class SqlBuilder(BaseSqlBuilder):
    sql_from_clause: sqlalchemy.FromClause
    sql_where_terms: list[sqlalchemy.ColumnElement[bool]] = dataclasses.field(default_factory=list)

    def extract_keys(self, dimensions: Iterable[str]) -> SqlBuilder:
        for dimension_name in dimensions:
            self.dimensions_provided[dimension_name] = [self.sql_from_clause.columns[dimension_name]]
        return self

    def extract_fields(
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
        self.handle_empty_columns(sql_columns)
        result = sqlalchemy.select(*sql_columns).select_from(self.sql_from_clause)
        # Add ORDER BY, LIMIT, and OFFSET clauses as appropriate.
        if order_by:
            result = result.order_by(*[self.build_sql_order_by_expression(term) for term in order_by])
        if not postprocessing:
            if offset:
                result = result.offset(offset)
            if limit is not None:
                result = result.limit(limit)
        else:
            raise NotImplementedError("TODO")
        return result, postprocessing

    def where_sql(self, *arg: sqlalchemy.ColumnElement[bool]) -> SqlBuilder:
        self.sql_where_terms.extend(arg)
        return self

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
    spatial_join_filtering: list[tuple[DimensionElement, DimensionElement]] = dataclasses.field(
        default_factory=list
    )
    spatial_where_filtering: list[tuple[DimensionElement, Region]] = dataclasses.field(default_factory=list)

    def gather_columns_required(self) -> Set[rt.ColumnReference]:
        result: set[rt.ColumnReference] = set()
        for element in itertools.chain.from_iterable(self.spatial_join_filtering):
            result.add(rt.DimensionFieldReference.model_construct(element=element, field="region"))
        for element, _ in self.spatial_join_filtering:
            result.add(rt.DimensionFieldReference.model_construct(element=element, field="region"))
        return result

    def __bool__(self) -> bool:
        return bool(self.spatial_join_filtering) or bool(self.spatial_where_filtering)

    def extend(self, other: Postprocessing) -> None:
        self.spatial_join_filtering.extend(other.spatial_join_filtering)
        self.spatial_where_filtering.extend(other.spatial_where_filtering)
