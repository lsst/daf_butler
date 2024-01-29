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

import logging
from collections.abc import Iterable, Iterator, Sequence
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, cast, overload

import sqlalchemy

from .. import ddl
from ..dimensions import DataIdValue, DimensionGroup, DimensionUniverse
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
from ..registry import CollectionSummary, CollectionType, NoDefaultCollectionError, RegistryDefaults
from ..registry.interfaces import ChainedCollectionRecord, CollectionRecord
from ..registry.managers import RegistryManagerInstances
from ..registry.nameShrinker import NameShrinker
from ._analyzed_query import AnalyzedDatasetSearch, AnalyzedQuery, DataIdExtractionVisitor
from ._convert_results import convert_dimension_record_results
from ._sql_column_visitor import SqlColumnVisitor

if TYPE_CHECKING:
    from ..registry.interfaces import Database
    from ._postprocessing import Postprocessing
    from ._sql_builder import SqlBuilder


_LOG = logging.getLogger(__name__)


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
        self.db = db
        self.managers = managers
        self._universe = universe
        self._defaults = defaults
        self._materializations: dict[qt.MaterializationKey, tuple[sqlalchemy.Table, Postprocessing]] = {}
        self._upload_tables: dict[qt.DataCoordinateUploadKey, sqlalchemy.Table] = {}
        self._exit_stack: ExitStack | None = None
        self._raw_page_size = raw_page_size
        self._postprocessing_filter_factor = postprocessing_filter_factor
        self._active_pages: dict[PageKey, tuple[Iterator[Sequence[sqlalchemy.Row]], Postprocessing]] = {}
        self._name_shrinker = NameShrinker(self.db.dialect.max_identifier_length)

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

        # Build the FROM and WHERE clauses and identify any post-query
        # processing we need to run.
        query, sql_builder = self.analyze_query(
            tree,
            final_columns=result_spec.get_result_columns(),
            order_by=result_spec.order_by,
            find_first_dataset=result_spec.find_first_dataset,
        )
        sql_builder = self.build_query(query, sql_builder)
        sql_select = sql_builder.select(query.final_columns, query.postprocessing)
        if result_spec.order_by:
            visitor = SqlColumnVisitor(sql_builder, self)
            sql_select = sql_select.order_by(*[visitor.expect_scalar(term) for term in result_spec.order_by])
        if result_spec.limit is not None:
            if query.postprocessing:
                query.postprocessing.limit = result_spec.limit
            else:
                sql_select = sql_select.limit(result_spec.limit)
        if result_spec.offset:
            if query.postprocessing:
                sql_select = sql_select.offset(result_spec.offset)
            else:
                query.postprocessing.offset = result_spec.offset
        if query.postprocessing.limit is not None:
            # We might want to fetch many fewer rows that the default page
            # size if we have to implement offset and limit in postprocessing.
            raw_page_size = min(
                self._postprocessing_filter_factor
                * (query.postprocessing.offset + query.postprocessing.limit),
                self._raw_page_size,
            )
        cursor = self._exit_stack.enter_context(
            self.db.query(sql_select.execution_options(yield_per=raw_page_size))
        )
        raw_page_iter = cursor.partitions()
        return self._process_page(raw_page_iter, result_spec, query.postprocessing)

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
    ) -> qt.MaterializationKey:
        # Docstring inherited.
        if self._exit_stack is None:
            raise RuntimeError("QueryDriver context must be entered before 'materialize' is called.")
        query, sql_builder = self.analyze_query(tree, qt.ColumnSet(dimensions))
        # Current implementation ignores 'datasets' because figuring out what
        # to put in the temporary table for them is tricky, especially if
        # calibration collections are involved.
        sql_builder = self.build_query(query, sql_builder)
        sql_select = sql_builder.select(query.final_columns, query.postprocessing)
        table = self._exit_stack.enter_context(
            self.db.temporary_table(sql_builder.make_table_spec(query.final_columns, query.postprocessing))
        )
        self.db.insert(table, select=sql_select)
        key = uuid.uuid4()
        self._materializations[key] = (table, query.postprocessing)
        return key

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
                    SqlBuilder.EMPTY_COLUMNS_NAME, dtype=SqlBuilder.EMPTY_COLUMNS_TYPE, nullable=True
                )
            )
        table = self._exit_stack.enter_context(self.db.temporary_table(table_spec))
        self.db.insert(table, *(dict(zip(dimensions.required, values)) for values in rows))
        key = uuid.uuid4()
        self._upload_tables[key] = table
        return key

    def count(
        self,
        tree: qt.QueryTree,
        columns: qt.ColumnSet,
        find_first_dataset: str | None,
        *,
        exact: bool,
        discard: bool,
    ) -> int:
        # Docstring inherited.
        query, sql_builder = self.analyze_query(tree, columns, find_first_dataset=find_first_dataset)
        sql_builder = self.build_query(query, sql_builder)
        if query.postprocessing and exact:
            if not discard:
                raise RuntimeError("Cannot count query rows exactly without discarding them.")
            sql_select = sql_builder.select(columns, query.postprocessing)
            n = 0
            with self.db.query(sql_select.execution_options(yield_per=self._raw_page_size)) as results:
                for _ in query.postprocessing.apply(results):
                    n + 1
            return n
        # Do COUNT(*) on the original query's FROM clause.
        sql_builder.special["_ROWCOUNT"] = sqlalchemy.func.count()
        sql_select = sql_builder.select(qt.ColumnSet(self._universe.empty.as_group()))
        with self.db.query(sql_select) as result:
            return cast(int, result.scalar())

    def any(self, tree: qt.QueryTree, *, execute: bool, exact: bool) -> bool:
        # Docstring inherited.
        query, sql_builder = self.analyze_query(tree, qt.ColumnSet(tree.dimensions))
        if not all(d.collection_records for d in query.datasets.values()):
            return False
        if not execute:
            if exact:
                raise RuntimeError("Cannot obtain exact result for 'any' without executing.")
            return True
        sql_builder = self.build_query(query, sql_builder)
        if query.postprocessing and exact:
            sql_select = sql_builder.select(query.final_columns, query.postprocessing)
            with self.db.query(
                sql_select.execution_options(yield_per=self._postprocessing_filter_factor)
            ) as result:
                for _ in query.postprocessing.apply(result):
                    return True
                return False
        sql_select = sql_builder.select(query.final_columns).limit(1)
        with self.db.query(sql_select) as result:
            return result.first() is not None

    def explain_no_results(self, tree: qt.QueryTree, execute: bool) -> Iterable[str]:
        # Docstring inherited.
        query, _ = self.analyze_query(tree, qt.ColumnSet(tree.dimensions))
        if query.messages or not execute:
            return query.messages
        # TODO: guess at ways to split up query that might fail or succeed if
        # run separately, execute them with LIMIT 1 and report the results.
        return []

    def get_dataset_dimensions(self, name: str) -> DimensionGroup:
        # Docstring inherited
        return self.managers.datasets[name].datasetType.dimensions.as_group()

    def get_default_collections(self) -> tuple[str, ...]:
        # Docstring inherited.
        if not self._defaults.collections:
            raise NoDefaultCollectionError("No collections provided and no default collections.")
        return tuple(self._defaults.collections)

    def resolve_collection_path(
        self, collections: Iterable[str]
    ) -> list[tuple[CollectionRecord, CollectionSummary]]:
        result: list[tuple[CollectionRecord, CollectionSummary]] = []
        done: set[str] = set()

        def recurse(collection_names: Iterable[str]) -> None:
            for collection_name in collection_names:
                if collection_name not in done:
                    done.add(collection_name)
                    record = self.managers.collections.find(collection_name)

                    if record.type is CollectionType.CHAINED:
                        recurse(cast(ChainedCollectionRecord, record).children)
                    else:
                        result.append((record, self.managers.datasets.getCollectionSummary(record)))

        recurse(collections)

        return result

    def analyze_query(
        self,
        tree: qt.QueryTree,
        final_columns: qt.ColumnSet,
        order_by: Iterable[qt.OrderExpression] = (),
        find_first_dataset: str | None = None,
    ) -> tuple[AnalyzedQuery, SqlBuilder]:
        # Delegate to the dimensions manager to rewrite the predicate and
        # start a SqlBuilder and Postprocessing to cover any spatial overlap
        # joins or constraints.  We'll return that SqlBuilder at the end.
        (
            predicate,
            sql_builder,
            postprocessing,
        ) = self.managers.dimensions.process_query_overlaps(
            tree.dimensions,
            tree.predicate,
            tree.join_operand_dimensions,
        )
        # Initialize the AnalyzedQuery instance we'll update throughout this
        # method.
        query = AnalyzedQuery(
            predicate,
            postprocessing,
            base_columns=qt.ColumnSet(tree.dimensions),
            projection_columns=final_columns.copy(),
            final_columns=final_columns,
            find_first_dataset=find_first_dataset,
        )
        # The base query needs to join in all columns required by the
        # predicate.
        predicate.gather_required_columns(query.base_columns)
        # The "projection" query differs from the final query by not omitting
        # any dimension keys (since that makes it easier to reason about),
        # including any columns needed by order_by terms, and including
        # the dataset rank if there's a find-first search in play.
        query.projection_columns.restore_dimension_keys()
        for term in order_by:
            term.gather_required_columns(query.projection_columns)
        if query.find_first_dataset is not None:
            query.projection_columns.dataset_fields[query.find_first_dataset].add("collection_key")
        # The base query also needs to include all columns needed by the
        # downstream projection query.
        query.base_columns.update(query.projection_columns)
        # Extract the data ID implied by the predicate; we can use the governor
        # dimensions in that to constrain the collections we search for
        # datasets later.
        query.predicate.visit(DataIdExtractionVisitor(query.constraint_data_id, query.messages))
        # We also check that the predicate doesn't reference any dimensions
        # without constraining their governor dimensions, since that's a
        # particularly easy mistake to make and it's almost never intentional.
        # We also also the registry data ID values to provide governor values.
        where_columns = qt.ColumnSet(query.universe.empty.as_group())
        query.predicate.gather_required_columns(where_columns)
        for governor in where_columns.dimensions.governors:
            if governor not in query.constraint_data_id:
                if governor in self._defaults.dataId.dimensions:
                    query.constraint_data_id[governor] = self._defaults.dataId[governor]
                else:
                    raise qt.InvalidQueryTreeError(
                        f"Query 'where' expression references a dimension dependent on {governor} without "
                        "constraining it directly."
                    )
        # Add materializations, which can also bring in more postprocessing.
        for m_key, m_dimensions in tree.materializations.items():
            _, m_postprocessing = self._materializations[m_key]
            query.materializations[m_key] = m_dimensions
            # When a query is materialized, the new tree's has an empty
            # (trivially true) predicate, and the materialization prevents the
            # creation of automatic spatial joins that are already included in
            # the materialization, so we don't need to deduplicate these
            # filters.  It's possible for there to be duplicates, but only if
            # the user explicitly adds a redundant constraint, and we'll still
            # behave correctly (just less efficiently) if that happens.
            postprocessing.spatial_join_filtering.extend(m_postprocessing.spatial_join_filtering)
            postprocessing.spatial_where_filtering.extend(m_postprocessing.spatial_where_filtering)
        # Add data coordinate uploads.
        query.data_coordinate_uploads.update(tree.data_coordinate_uploads)
        # Add dataset_searches and filter out collections that don't have the
        # right dataset type or governor dimensions.
        name_shrinker = make_dataset_name_shrinker(self.db.dialect)
        for dataset_type_name, dataset_search in tree.datasets.items():
            dataset = AnalyzedDatasetSearch(
                dataset_type_name, name_shrinker.shrink(dataset_type_name), dataset_search.dimensions
            )
            for collection_record, collection_summary in self.resolve_collection_path(
                dataset_search.collections
            ):
                rejected: bool = False
                if dataset.name not in collection_summary.dataset_types.names:
                    dataset.messages.append(
                        f"No datasets of type {dataset.name!r} in collection {collection_record.name}."
                    )
                    rejected = True
                for governor in query.constraint_data_id.keys() & collection_summary.governors.keys():
                    if query.constraint_data_id[governor] not in collection_summary.governors[governor]:
                        dataset.messages.append(
                            f"No datasets with {governor}={query.constraint_data_id[governor]!r} "
                            f"in collection {collection_record.name}."
                        )
                        rejected = True
                if not rejected:
                    if collection_record.type is CollectionType.CALIBRATION:
                        dataset.is_calibration_search = True
                    dataset.collection_records.append(collection_record)
            if dataset.dimensions != self.get_dataset_type(dataset_type_name).dimensions.as_group():
                # This is really for server-side defensiveness; it's hard to
                # imagine the query getting different dimensions for a dataset
                # type in two calls to the same query driver.
                raise qt.InvalidQueryTreeError(
                    f"Incorrect dimensions {dataset.dimensions} for dataset {dataset_type_name} "
                    f"in query (vs. {self.get_dataset_type(dataset_type_name).dimensions.as_group()})."
                )
            query.datasets[dataset_type_name] = dataset
            if not dataset.collection_records:
                query.messages.append(f"Search for dataset type {dataset_type_name!r} is doomed to fail.")
                query.messages.extend(dataset.messages)
        # Set flags that indicate certain kinds of special processing the query
        # will need, mostly in the "projection" stage, where we might do a
        # GROUP BY or DISTINCT [ON].
        if query.find_first_dataset is not None:
            # If we're doing a find-first search and there's a calibration
            # collection in play, we need to make sure the rows coming out of
            # the base query have only one timespan for each data ID +
            # collection, and we can only do that with a GROUP BY and COUNT.
            query.postprocessing.check_validity_match_count = query.datasets[
                query.find_first_dataset
            ].is_calibration_search
            # We only actually need to include the find-first resolution query
            # logic if there's more than one collection.
            query.needs_find_first_resolution = (
                len(query.datasets[query.find_first_dataset].collection_records) > 1
            )
        if query.projection_columns.dimensions != query.base_columns.dimensions:
            # We're going from a larger set of dimensions to a smaller set,
            # that means we'll be doing a SELECT DISTINCT [ON] or GROUP BY.
            query.needs_dimension_distinct = True
            # If there are any dataset fields being propagated through that
            # projection and there is more than one collection, we need to
            # include the collection_key column so we can use that as one of
            # the DISTINCT ON or GROUP BY columns.
            for dataset_type, fields_for_dataset in query.projection_columns.dataset_fields.items():
                if len(query.datasets[dataset_type].collection_records) > 1:
                    fields_for_dataset.add("collection_key")
            # If there's a projection and we're doing postprocessing, we might
            # be collapsing the dimensions of the postprocessing regions.  When
            # that happens, we want to apply an aggregate function to them that
            # computes the union of the regions that are grouped together.
            for element in query.postprocessing.iter_missing(query.projection_columns):
                if element.name not in query.projection_columns.dimensions.elements:
                    query.projection_region_aggregates.append(element)
                    break
        return query, sql_builder

    def build_query(self, query: AnalyzedQuery, sql_builder: SqlBuilder) -> SqlBuilder:
        sql_builder = self._build_base_query(query, sql_builder)
        if query.needs_projection:
            sql_builder = self._project_query(query, sql_builder)
            if query.needs_find_first_resolution:
                sql_builder = self._apply_find_first(query, sql_builder)
        elif query.needs_find_first_resolution:
            sql_builder = self._apply_find_first(
                query, sql_builder.cte(query.projection_columns, query.postprocessing)
            )
        return sql_builder

    def _build_base_query(self, query: AnalyzedQuery, sql_builder: SqlBuilder) -> SqlBuilder:
        # Process data coordinate upload joins.
        for upload_key, upload_dimensions in query.data_coordinate_uploads.items():
            sql_builder = sql_builder.join(
                SqlBuilder(self.db, self._upload_tables[upload_key]).extract_dimensions(
                    upload_dimensions.required
                )
            )
        # Process materialization joins.
        for materialization_key, materialization_spec in query.materializations.items():
            sql_builder = self._join_materialization(sql_builder, materialization_key, materialization_spec)
        # Process dataset joins.
        for dataset_type, dataset_search in query.datasets.items():
            sql_builder = self._join_dataset_search(
                sql_builder,
                dataset_type,
                dataset_search,
                query.base_columns,
            )
        # Join in dimension element tables that we know we need relationships
        # or columns from.
        for element in query.iter_mandatory_base_elements():
            sql_builder = sql_builder.join(
                self.managers.dimensions.make_sql_builder(
                    element, query.base_columns.dimension_fields[element.name]
                )
            )
        # See if any dimension keys are still missing, and if so join in their
        # tables.  Note that we know there are no fields needed from these.
        while not (sql_builder.dimension_keys.keys() >= query.base_columns.dimensions.names):
            # Look for opportunities to join in multiple dimensions via single
            # table, to reduce the total number of tables joined in.
            missing_dimension_names = query.base_columns.dimensions.names - sql_builder.dimension_keys.keys()
            best = self._universe[
                max(
                    missing_dimension_names,
                    key=lambda name: len(self._universe[name].dimensions.names & missing_dimension_names),
                )
            ]
            sql_builder = sql_builder.join(self.managers.dimensions.make_sql_builder(best, frozenset()))
        # Add the WHERE clause to the builder.
        return sql_builder.where_sql(query.predicate.visit(SqlColumnVisitor(sql_builder, self)))

    def _project_query(self, query: AnalyzedQuery, sql_builder: SqlBuilder) -> SqlBuilder:
        assert query.needs_projection
        # This method generates a Common Table Expresssion (CTE) using either a
        # SELECT DISTINCT [ON] or a SELECT with GROUP BY.
        # We'll work out which as we go
        have_aggregates: bool = False
        # Dimension key columns form at least most of our GROUP BY or DISTINCT
        # ON clause; we'll work out which of those we'll use.
        unique_keys: list[sqlalchemy.ColumnElement[Any]] = [
            sql_builder.dimension_keys[k][0] for k in query.projection_columns.dimensions.data_coordinate_keys
        ]
        # There are two reasons we might need an aggregate function:
        # - to make sure temporal constraints and joins have resulted in at
        #   most one validity range match for each data ID and collection,
        #   when we're doing a find-first query.
        # - to compute the unions of regions we need for postprocessing, when
        #   the data IDs for those regions are not wholly included in the
        #   results (i.e. we need to postprocess on
        #   visit_detector_region.region, but the output rows don't have
        #   detector, just visit - so we compute the union of the
        #   visit_detector region over all matched detectors).
        if query.postprocessing.check_validity_match_count:
            sql_builder.special[query.postprocessing.VALIDITY_MATCH_COUNT] = sqlalchemy.func.count().label(
                query.postprocessing.VALIDITY_MATCH_COUNT
            )
            have_aggregates = True
        for element in query.projection_region_aggregates:
            sql_builder.fields[element.name]["region"] = ddl.Base64Region.union_agg(
                sql_builder.fields[element.name]["region"]
            )
            have_aggregates = True
        # Many of our fields derive their uniqueness from the unique_key
        # fields: if rows are uniqe over the 'unique_key' fields, then they're
        # automatically unique over these 'derived_fields'.  We just remember
        # these as pairs of (logical_table, field) for now.
        derived_fields: list[tuple[str, str]] = []
        # All dimension record fields are derived fields.
        for element_name, fields_for_element in query.projection_columns.dimension_fields.items():
            for element_field in fields_for_element:
                derived_fields.append((element_name, element_field))
        # Some dataset fields are derived fields and some are unique keys, and
        # it depends on the kinds of collection(s) we're searching and whether
        # it's a find-first query.
        for dataset_type, fields_for_dataset in query.projection_columns.dataset_fields.items():
            for dataset_field in fields_for_dataset:
                if dataset_field == "collection_key":
                    # If the collection_key field is present, it's needed for
                    # uniqueness if we're looking in more than one collection.
                    # If not, it's a derived field.
                    if len(query.datasets[dataset_type].collection_records) > 1:
                        unique_keys.append(sql_builder.fields[dataset_type]["collection_key"])
                    else:
                        derived_fields.append((dataset_type, "collection_key"))
                elif dataset_field == "timespan" and query.datasets[dataset_type].is_calibration_search:
                    # If we're doing a non-find-first query against a
                    # CALIBRATION collection, the timespan is also a unique
                    # key...
                    if dataset_type == query.find_first_dataset:
                        # ...unless we're doing a find-first search on this
                        # dataset, in which case we need to use ANY_VALUE on
                        # the timespan and check that _VALIDITY_MATCH_COUNT
                        # (added earlier) is one, indicating that there was
                        # indeed only one timespan for each data ID in each
                        # collection that survived the base query's WHERE
                        # clauses and JOINs.
                        if not self.db.has_any_aggregate:
                            raise NotImplementedError(
                                f"Cannot generate query that returns {dataset_type}.timespan after a "
                                "find-first search, because this a database does not support the ANY_VALUE "
                                "aggregate function (or equivalent)."
                            )
                        sql_builder.timespans[dataset_type] = sql_builder.timespans[
                            dataset_type
                        ].apply_any_aggregate(self.db.apply_any_aggregate)
                    else:
                        unique_keys.extend(sql_builder.timespans[dataset_type].flatten())
                else:
                    # Other dataset fields derive their uniqueness from key
                    # fields.
                    derived_fields.append((dataset_type, dataset_field))
        if not have_aggregates and not derived_fields:
            # SELECT DISTINCT is sufficient.
            return sql_builder.cte(query.projection_columns, query.postprocessing, distinct=True)
        elif not have_aggregates and self.db.has_distinct_on:
            # SELECT DISTINCT ON is sufficient and works.
            return sql_builder.cte(query.projection_columns, query.postprocessing, distinct=unique_keys)
        else:
            # GROUP BY is the only option.
            if derived_fields:
                if self.db.has_any_aggregate:
                    for logical_table, field in derived_fields:
                        if field == "timespan":
                            sql_builder.timespans[logical_table] = sql_builder.timespans[
                                logical_table
                            ].apply_any_aggregate(self.db.apply_any_aggregate)
                        else:
                            sql_builder.fields[logical_table][field] = self.db.apply_any_aggregate(
                                sql_builder.fields[logical_table][field]
                            )
                else:
                    _LOG.warning(
                        "Adding %d fields to GROUP BY because this database backend does not support the "
                        "ANY_VALUE aggregate function (or equivalent).  This may result in a poor query "
                        "plan.  Materializing the query first sometimes avoids this problem.",
                        len(derived_fields),
                    )
                    for logical_table, field in derived_fields:
                        if field == "timespan":
                            unique_keys.extend(sql_builder.timespans[logical_table].flatten())
                        else:
                            unique_keys.append(sql_builder.fields[logical_table][field])
            return sql_builder.cte(query.projection_columns, query.postprocessing, group_by=unique_keys)

    def _apply_find_first(self, query: AnalyzedQuery, sql_builder: SqlBuilder) -> SqlBuilder:
        assert query.needs_find_first_resolution
        assert query.find_first_dataset is not None
        assert sql_builder.sql_from_clause is not None
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
        # The outermost SELECT will be represented by the SqlBuilder we return.

        # The sql_builder we're given corresponds to the Common Table
        # Expression (CTE) at the top, and is guaranteed to have
        # ``query.projected_columns`` (+ postprocessing columns).
        # We start by filling out the "window" SELECT statement...
        partition_by = [sql_builder.dimension_keys[d][0] for d in query.base_columns.dimensions.required]
        rank_sql_column = sqlalchemy.case(
            {
                record.key: n
                for n, record in enumerate(query.datasets[query.find_first_dataset].collection_records)
            },
            value=sql_builder.fields[query.find_first_dataset]["collection_key"],
        )
        if partition_by:
            sql_builder.special["_ROWNUM"] = sqlalchemy.sql.func.row_number().over(
                partition_by=partition_by, order_by=rank_sql_column
            )
        else:
            sql_builder.special["_ROWNUM"] = sqlalchemy.sql.func.row_number().over(order_by=rank_sql_column)
        # ... and then turn that into a subquery with a constraint on rownum.
        sql_builder = sql_builder.subquery(query.base_columns, query.postprocessing)
        sql_builder = sql_builder.where_sql(sql_builder.special["_ROWNUM"] == 1)
        del sql_builder.special["_ROWNUM"]
        return sql_builder

    def _join_materialization(
        self,
        sql_builder: SqlBuilder,
        materialization_key: qt.MaterializationKey,
        dimensions: DimensionGroup,
    ) -> SqlBuilder:
        columns = qt.ColumnSet(dimensions)
        table, postprocessing = self._materializations[materialization_key]
        return sql_builder.join(SqlBuilder(self.db, table).extract_columns(columns, postprocessing))

    def _join_dataset_search(
        self,
        sql_builder: SqlBuilder,
        dataset_type: str,
        processed_dataset_search: AnalyzedDatasetSearch,
        columns: qt.ColumnSet,
    ) -> SqlBuilder:
        storage = self.managers.datasets[dataset_type]
        # The next two asserts will need to be dropped (and the implications
        # dealt with instead) if materializations start having dataset fields.
        assert (
            dataset_type not in sql_builder.fields
        ), "Dataset fields have unexpected already been joined in."
        assert (
            dataset_type not in sql_builder.timespans
        ), "Dataset timespan has unexpected already been joined in."
        return sql_builder.join(
            storage.make_sql_builder(
                processed_dataset_search.collection_records, columns.dataset_fields[dataset_type]
            )
        )

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
                return convert_dimension_record_results(
                    postprocessing.apply(raw_page),
                    result_spec,
                    next_key,
                    self._name_shrinker,
                )
            case _:
                raise NotImplementedError("TODO")


def make_dataset_name_shrinker(dialect: sqlalchemy.Dialect) -> NameShrinker:
    max_dataset_field_length = max(len(field) for field in qt.DATASET_FIELD_NAMES)
    return NameShrinker(dialect.max_identifier_length - max_dataset_field_length - 1, 6)
