# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

__all__ = ("SqlQueryBackend",)

from typing import TYPE_CHECKING, AbstractSet, Iterable, Mapping, Sequence

import sqlalchemy
from lsst.utils.classes import cached_getter

from ..core import DatasetType, DimensionGraph, DimensionUniverse, ddl, sql
from ..core.named import NamedValueAbstractSet, NamedValueSet
from ._collectionType import CollectionType
from ._query_backend import QueryBackend
from ._sql_query_context import SqlQueryContext
from .wildcards import CollectionSearch, CollectionWildcard

if TYPE_CHECKING:
    from .interfaces import CollectionRecord, Database, DimensionRecordStorage
    from .managers import RegistryManagerInstances


class SqlQueryBackend(QueryBackend[SqlQueryContext]):
    """An implementation of `QueryBackend` for `SqlRegistry`.

    Parameters
    ----------
    db : `Database`
        Object that abstracts the database engine.
    managers : `RegistryManagerInstances`
        Struct containing the manager objects that back a `SqlRegistry`.
    """

    def __init__(
        self,
        db: Database,
        managers: RegistryManagerInstances,
    ):
        self._db = db
        self._managers = managers

    @property
    def managers(self) -> RegistryManagerInstances:
        # Docstring inherited.
        return self._managers

    def make_context(self) -> SqlQueryContext:
        # Docstring inherited.
        return SqlQueryContext(self._db, self._managers.column_types)

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._managers.dimensions.universe

    @property
    def parent_dataset_types(self) -> NamedValueAbstractSet[DatasetType]:
        # Docstring inherited.
        return self._managers.datasets.parent_dataset_types

    @property
    def collection_records(self) -> NamedValueAbstractSet[CollectionRecord]:
        # Docstring inherited.
        return self._managers.collections.records

    @property  # type: ignore
    @cached_getter
    def unit_relation(self) -> sql.Relation:
        # Docstring inherited.
        return sql.Relation.make_unit(
            self._db.constant_rows(
                NamedValueSet({ddl.FieldSpec("ignored", dtype=sqlalchemy.Boolean)}), {"ignored": True}
            ),
            self._managers.column_types,
        )

    def make_doomed_relation(self, *messages: str, columns: AbstractSet[sql.ColumnTag]) -> sql.Relation:
        # Docstring inherited.
        spec = sql.ColumnTag.make_table_spec(columns, self._managers.column_types)
        row = {str(tag): None for tag in columns}
        return sql.Relation.make_doomed(
            *messages,
            constant_row=self._db.constant_rows(spec.fields, row),
            columns=sql.ColumnTagSet._from_iterable(columns),
            column_types=self._managers.column_types,
        )

    def resolve_dataset_collections(
        self,
        dataset_type: DatasetType,
        collections: CollectionSearch | CollectionWildcard,
        *,
        constraints: sql.LocalConstraints | None = None,
        rejections: list[str] | None = None,
        collection_types: AbstractSet[CollectionType] = CollectionType.all(),
        allow_calibration_collections: bool = False,
    ) -> list[CollectionRecord]:
        if constraints is None:
            constraints = sql.LocalConstraints.make_full()
        if collections == CollectionWildcard() and collections == CollectionType.all():
            collection_types = {CollectionType.RUN}
        explicit_collections = frozenset(collections.explicitNames())
        results: list[CollectionRecord] = []
        for record in collections.iter(self.collection_records, collectionTypes=collection_types):
            # Only include collections that (according to collection summaries)
            # might have datasets of this type and governor dimensions
            # consistent with the given constraint.
            collection_summary = self._managers.datasets.getCollectionSummary(record)
            if not collection_summary.is_compatible_with(
                dataset_type,
                constraints.dimensions,
                rejections=rejections,
                name=record.name,
            ):
                continue
            if record.type is CollectionType.CALIBRATION:
                # If collection name was provided explicitly then say sorry if
                # this is a kind of query we don't support yet; otherwise
                # collection is a part of chained one or regex match and we
                # skip it to not break queries of other included collections.
                if dataset_type.isCalibration():
                    if allow_calibration_collections:
                        results.append(record)
                    else:
                        if record.name in explicit_collections:
                            raise NotImplementedError(
                                f"Query for dataset type {dataset_type.name!r} in CALIBRATION-type "
                                f"collection {record.name!r} is not yet supported."
                            )
                        else:
                            if rejections is not None:
                                rejections.append(
                                    f"Not searching for dataset {dataset_type.name!r} in CALIBRATION "
                                    f"collection {record.name!r} because calibration queries aren't fully "
                                    "implemented; this is not an error only because the query structure "
                                    "implies that searching this collection may be incidental."
                                )
                            continue
                else:
                    # We can never find a non-calibration dataset in a
                    # CALIBRATION collection.
                    if rejections is not None:
                        rejections.append(
                            f"Not searching for non-calibration dataset {dataset_type.name!r} "
                            f"in CALIBRATION collection {record.name!r}."
                        )
                    continue
            else:
                results.append(record)
        if not results and rejections is not None and not rejections:
            rejections.append(f"No collections to search matching expression {collections!r}.")
        return results

    def make_dataset_query_relation(
        self,
        dataset_type: DatasetType,
        collections: Sequence[CollectionRecord],
        columns: AbstractSet[str],
        *,
        join_relation: sql.Relation | None = None,
        constraints: sql.LocalConstraints | None = None,
    ) -> sql.Relation:
        # Docstring inherited.
        assert len(collections) > 0, (
            "Caller is responsible for handling the case of all collections being rejected (we can't "
            "write a good error message without knowing why collections were rejected)."
        )
        dataset_storage = self._managers.datasets.find(dataset_type.name)
        if dataset_storage is None:
            # Unrecognized dataset type means no results.
            dataset_relation = self.make_doomed_dataset_relation(
                dataset_type,
                columns,
                messages=[
                    f"Dataset type {dataset_type.name!r} is not registered, "
                    "so no instances of it can exist in any collection."
                ],
            )
        else:
            dataset_relation = dataset_storage.get_relation(
                *collections, columns=columns, constraints=constraints
            )
        if join_relation is not None:
            return join_relation.join(dataset_relation)
        else:
            return dataset_relation

    def make_dataset_search_relation(
        self,
        dataset_type: DatasetType,
        collections: Sequence[CollectionRecord],
        columns: AbstractSet[str],
        *,
        join_relation: sql.Relation | None = None,
        constraints: sql.LocalConstraints | None = None,
    ) -> sql.Relation:
        # Docstring inherited.
        # Query-simplification shortcut: if there is only one collection, a
        # find-first search is just a regular result subquery.
        if len(collections) <= 1:
            return self.make_dataset_query_relation(
                dataset_type, collections, columns, join_relation=join_relation, constraints=constraints
            )
        # In the more general case, we build a subquery of the form below to
        # search the collections in order.
        #
        # WITH {dst}_search AS (
        #     SELECT {data-id-cols}, id, run_id, 1 AS rank
        #         FROM <collection1>
        #     UNION ALL
        #     SELECT {data-id-cols}, id, run_id, 2 AS rank
        #         FROM <collection2>
        #     UNION ALL
        #     ...
        # )
        # SELECT
        #     {dst}_window.{data-id-cols},
        #     {dst}_window.id,
        #     {dst}_window.run_id
        # FROM (
        #     SELECT
        #         {dst}_search.{data-id-cols},
        #         {dst}_search.id,
        #         {dst}_search.run_id,
        #         ROW_NUMBER() OVER (
        #             PARTITION BY {dst_search}.{data-id-cols}
        #             ORDER BY rank
        #         ) AS rownum
        #     ) {dst}_window
        # WHERE
        #     {dst}_window.rownum = 1;
        #
        # We'll start with the Common Table Expression (CTE) at the top.
        # We get that by delegating to join_dataset_query, which also takes
        # care of any relation or constraints the caller may have passed.
        search_relation = self.make_dataset_query_relation(
            dataset_type,
            collections,
            columns=columns | {"rank"},
            join_relation=join_relation,
            constraints=constraints,
        )
        if search_relation.doomed_by:
            return search_relation
        search_cte = search_relation.to_sql_executable().cte(f"{dataset_type.name}_search")
        # Create a columns object that holds the SQLAlchemy objects for the
        # columns that are SELECT'd in the CTE, and hence available downstream.
        search_columns = sql.ColumnTag.extract_logical_column_mapping(
            search_relation.columns,
            search_cte.columns,
            self._managers.column_types,
        )

        # Now we fill out the inner SELECT subquery from the CTE.  We replace
        # the rank column with the window-function 'rownum' calculated from it;
        # which is like the rank in that it orders datasets by the collection
        # in which they are found (separately for each data ID), but critically
        # it doesn't have gaps where the dataset wasn't found in one the input
        # collections, and hence ``rownum=1`` reliably picks out the find-first
        # result.  We use SQLAlchemy objects directly here instead of Relation,
        # because it involves some advanced SQL constructs we don't want
        # Relation to try to wrap.  We still use our columns classes to let
        # them encapsulate actual column names.
        rank_tag = sql.DatasetColumnTag(dataset_type.name, "rank")
        rownum_column = (
            sqlalchemy.sql.func.row_number()
            .over(
                partition_by=[
                    search_columns[sql.DimensionKeyColumnTag(dimension_name)]
                    for dimension_name in search_relation.columns.dimensions
                ],
                order_by=search_columns[rank_tag],
            )
            .label("rownum")
        )
        del search_columns[rank_tag]
        window_subquery = sql.ColumnTag.select_logical_column_items(
            search_columns.items(), search_cte, rownum_column
        ).alias(f"{dataset_type.name}_window")
        # Create a new columns object to hold the columns SELECT'd by the
        # subquery.  This does not include the calculated 'rownum' column,
        # which we'll handle separately; this works out well because we
        # only want it in the WHERE clause anyway.
        window_columns = sql.ColumnTag.extract_logical_column_mapping(
            search_columns.keys(), window_subquery.columns, self._managers.column_types
        )
        # We'll want to package up the full query as Relation instance, so we
        # build one from SQL parts instead of using SQLAlchemy to make a SELECT
        # directly.
        builder = sql.Relation.build(window_subquery, self._managers.column_types)
        builder.columns.update(window_columns)
        builder.where.append(window_subquery.columns["rownum"] == 1)
        return builder.finish()

    def make_dimension_relation(
        self,
        dimensions: DimensionGraph,
        sql_columns: Mapping[str, AbstractSet[str]],
        result_columns: Mapping[str, AbstractSet[str]],
        result_records: AbstractSet[str] = frozenset(),
        *,
        spatial_joins: Iterable[tuple[str, str]] = (),
        join_relation: sql.Relation | None = None,
        constraints: sql.LocalConstraints | None = None,
    ) -> sql.Relation:
        # Docstring inherited.

        # Start by gathering up all of the arguments to each call to
        # DimensionRecordStorage.join for each element in given dimensions.  We
        # won't actually call that method yet for any, because we'll have to
        # tweak some arguments later.  Tuples below are (storage, sql_columns,
        # result_columns).
        todo: dict[str, tuple[DimensionRecordStorage, set[str], set[str]]] = {}
        for element_name in dimensions.elements.names:
            storage = self._managers.dimensions[element_name]
            # Convert column arguments for this element to concrete sets (maybe
            # an empty one).
            element_sql_columns = set(sql_columns.get(element_name, frozenset()))
            element_result_columns = set(result_columns.get(element_name, frozenset()))
            # Remove result columns that have also been requested as sql
            # columns.
            element_result_columns.difference_update(element_sql_columns)
            # Remove columns that were already provided by the join relation.
            # We know other dimension relations we join here can't provide
            # these (an element's relation can only provide record columns for
            # that element), so we don't need to repeat this check as we join
            # more relations in, but we can't say the same for the relation we
            # were given, which could be e.g. a materialized subset of some of
            # the same relations we're considering joining in here.
            if element_sql_columns and join_relation is not None:
                element_sql_columns.difference_update(join_relation.columns.dimensions)
            if element_result_columns and join_relation is not None:
                element_result_columns.difference_update(join_relation.columns.dimensions)
                for postprocessor in join_relation.postprocessors:
                    for result_tag in postprocessor.columns_provided:
                        match result_tag:
                            case sql.DimensionRecordColumnTag(
                                element=tag_element, column=column_name
                            ) if tag_element == element_name:
                                element_result_columns.discard(column_name)
            # Finally we add this element and its updated column-request sets
            # to the 'todo' dict.
            todo[element_name] = (storage, element_sql_columns, element_result_columns)

        # Set up the relation variable we'll update as we join more in, as
        # well as the set of connections already present in the given relation.
        if join_relation is not None:
            relation = join_relation
            existing_connections = join_relation.connections
        else:
            relation = self.unit_relation
            existing_connections = frozenset()

        # Next we'll handle spatial joins, because those may require some
        # additional region result columns for postprocessors.
        postprocessors: list[sql.Postprocessor] = []
        for element1, element2 in spatial_joins:
            overlaps, needs_refinement = self._managers.dimensions.get_spatial_join_relation(
                element1, element2, constraints=constraints
            )
            if needs_refinement:
                postprocessors.append(
                    sql.Postprocessor.from_spatial_join(
                        sql.DimensionRecordColumnTag(element1, "region"),
                        sql.DimensionRecordColumnTag(element2, "region"),
                    )
                )
                _, _, element1_result_columns = todo[element1]
                element1_result_columns.add("region")
                _, _, element2_result_columns = todo[element2]
                element2_result_columns.add("region")
            relation = relation.join(overlaps)

        # Return to the dictionary of elements to process; we either join to
        # 'relation' now, move it to the new 'deferred' set below, or decide
        # that we don't necessarily need that element's relation at all.
        deferred: set[str] = set()
        for element_name, (storage, element_sql_columns, element_result_columns) in todo.items():
            if (
                # We may need to join this relation now, because we need
                # non-key columns or connections from it in sql.
                element_sql_columns
                or not (storage.join_connections <= existing_connections)
                # Or there's no downside to joining this relation now, because
                # we need result columns from it and it's going to provide them
                # as sql columns anyway.
                or (
                    not storage.join_results_postprocessed
                    and (element_result_columns or element_name in result_records)
                )
            ):
                relation = storage.join(
                    relation,
                    element_sql_columns,
                    constraints=constraints,
                    result_records=(element_name in result_records),
                    result_columns=element_result_columns,
                )
            elif element_result_columns or element_name in result_records:
                # We'll need to join this relation in at some point, but it may
                # better to do it later, in the sense that it may make the
                # sql query simpler by doing postprocessing instead.
                deferred.add(element_name)
            # If none of those conditions are satisfied, we don't necessarily
            # need to join this element in at all, so we just ignore it.  It
            # may be that we'll join it in later just to fill out the dimension
            # key columns.

        # At this point we've joined in all of the element relations that
        # definitely need to be included in the sql side, but we may not have
        # all of the dimension key columns in the query that we want.  To fill
        # out that set, we iterate over the given DimensionGraph's dimensions
        # (not all dimension elements) in reverse topological order.  That
        # order should reduce the total number of tables we bring in, since
        # each dimension will bring in keys for its required dependencies
        # before we get to those required dependencies.  This will probably
        # include some of the elements we deferred earlier.
        for dimension in self.universe.sorted(dimensions, reverse=True):
            if dimension.name not in relation.columns.dimensions:
                storage, _, element_result_columns = todo[dimension.name]
                relation = storage.join(
                    relation,
                    frozenset(),
                    constraints=constraints,
                    result_records=(element_name in result_records),
                    result_columns=element_result_columns,
                )
                deferred.discard(dimension.name)

        # At this point we should have keys for all dimensions in the query,
        # but there may still be some elements we deferred before that could
        # add postprocessors.
        for element_name in deferred:
            storage, _, element_result_columns = todo[element_name]
            relation = storage.join(
                relation,
                frozenset(),
                constraints=constraints,
                result_records=(element_name in result_records),
                result_columns=element_result_columns,
            )

        # Finally we can add the postprocessors from the spatial joins, since
        # we should have included (either in sql or via postprocessors) any
        # region columns they rely on.
        return relation.postprocessed(*postprocessors)
