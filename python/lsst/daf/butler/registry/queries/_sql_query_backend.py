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

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING

from lsst.daf.relation import Identity, Relation, sql, Zero, Predicate, iteration
from lsst.utils.sets.unboundable import UnboundableSet

from ...core import (
    ColumnTag,
    ColumnCategorization,
    DataIdValue,
    DimensionGraph,
    DatasetColumnTag,
    DimensionRecordColumnTag,
    DatasetType,
    DimensionKeyColumnTag,
    DimensionUniverse,
    LogicalColumn,
    SkyPixDimension,
)
from ...core.named import NamedValueAbstractSet
from .._collectionType import CollectionType
from ..wildcards import CollectionSearch, CollectionWildcard
from ._query_backend import QueryBackend
from ._sql_query_context import SqlQueryContext
from .find_first import FindFirst
from ._relation_helpers import SpatialJoinRefinement

if TYPE_CHECKING:
    from ..interfaces import CollectionRecord, Database
    from ..managers import RegistryManagerInstances


class SqlQueryBackend(QueryBackend):
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
        self._engine = sql.Engine("db")

    @property
    def managers(self) -> RegistryManagerInstances:
        # Docstring inherited.
        return self._managers

    def to_sql_select_parts(self, relation: Relation[ColumnTag]) -> sql.SelectParts[ColumnTag, LogicalColumn]:
        # Docstring inherited.
        return relation.visit(sql.ToSelectParts(self._managers.column_types))

    def context(self) -> SqlQueryContext:
        # Docstring inherited.
        return SqlQueryContext(self._db, self._managers.column_types, self._engine)

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

    def make_identity_relation(self) -> Relation[ColumnTag]:
        # Docstring inherited.
        return Identity(self._engine)

    def make_zero_relation(self, columns: Set[ColumnTag], doomed_by: Iterable[str]) -> Relation[ColumnTag]:
        # Docstring inherited.
        return Zero(self._engine, columns)  # TODO: doomed_by

    def resolve_dataset_collections(
        self,
        dataset_type: DatasetType,
        collections: CollectionSearch | CollectionWildcard,
        *,
        governors: Mapping[str, UnboundableSet[DataIdValue]] | None = None,
        rejections: list[str] | None = None,
        collection_types: Set[CollectionType] = CollectionType.all(),
        allow_calibration_collections: bool = False,
    ) -> list[CollectionRecord]:
        if governors is None:
            governors = {}
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
                governors,
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
        columns: Set[str],
        governors: Mapping[str, UnboundableSet[DataIdValue]] | None = None,
    ) -> Relation[ColumnTag]:
        # Docstring inherited.
        assert len(collections) > 0, (
            "Caller is responsible for handling the case of all collections being rejected (we can't "
            "write a good error message without knowing why collections were rejected)."
        )
        dataset_storage = self._managers.datasets.find(dataset_type.name)
        if dataset_storage is None:
            # Unrecognized dataset type means no results.
            return self.make_doomed_dataset_relation(
                dataset_type,
                columns,
                messages=[
                    f"Dataset type {dataset_type.name!r} is not registered, "
                    "so no instances of it can exist in any collection."
                ],
            )
        else:
            return dataset_storage.make_relation(*collections, columns=columns, engine=self._engine)

    def make_dataset_search_relation(
        self,
        dataset_type: DatasetType,
        collections: Sequence[CollectionRecord],
        columns: Set[str],
        *,
        join_to: Relation[ColumnTag] | None = None,
        governors: Mapping[str, UnboundableSet[DataIdValue]] | None = None,
    ) -> Relation[ColumnTag]:
        # Docstring inherited.
        base = self.make_dataset_query_relation(
            dataset_type,
            collections,
            columns | {"rank"},
            governors=governors,
        )
        if join_to is not None:
            base = base.join(join_to)
        # Query-simplification shortcut: if there is only one collection, a
        # find-first search is just a regular result subquery.  Same if there
        # are no collections.
        if len(collections) <= 1:
            return base
        return FindFirst(
            base,
            DatasetColumnTag(dataset_type.name, "rank"),
            DimensionKeyColumnTag.filter_from(base.columns),
        )

    def make_doomed_dataset_relation(
        self,
        dataset_type: DatasetType,
        columns: Set[str],
        messages: Iterable[str],
    ) -> Relation[ColumnTag]:
        # Docstring inherited.
        column_tags: set[ColumnTag] = set(
            DimensionKeyColumnTag.generate(dataset_type.dimensions.required.names)
        )
        column_tags.update(DatasetColumnTag.generate(dataset_type.name, columns))
        return Zero(self._engine, columns=column_tags)  # TODO: messages

    def make_dimension_relation(
        self,
        dimensions: DimensionGraph,
        columns: Mapping[str, Set[str]],
        *,
        initial_relation: Relation[ColumnTag] | None = None,
        initial_key_relationships: Set[frozenset[str]] = frozenset(),
        spatial_joins: Iterable[tuple[str, str]] = (),
        governors: Mapping[str, UnboundableSet[DataIdValue]] | None = None,
    ) -> Relation[ColumnTag]:
        # Docstring inherited.

        # Set up the relation variable we'll update as we join more in, as
        # well as the set of connections already present in the given relation.
        if initial_relation is not None:
            relation = initial_relation
        else:
            relation = self.make_identity_relation()

        # Make a mutable copy of the columns argument.  Note that the values of
        # this mapping are not copied, but because they're annotated as a
        # non-mutable type (collections.abc.Set`), mypy will make sure we don't
        # modify them in-place and hence surprise the caller.  Any
        # modifications will *replace* those value-sets instead.
        columns = defaultdict(set, columns)

        # Next we'll handle spatial joins, since those can require refinement
        # predicates that will need region columns to be included in the
        # relations we'll join.
        predicates: list[Predicate[ColumnTag]] = []
        for element1, element2 in spatial_joins:
            overlaps, needs_refinement = self._managers.dimensions.make_spatial_join_relation(
                element1, element2, engine=self._engine, governors=governors
            )
            if needs_refinement:
                predicates.append(
                    SpatialJoinRefinement(
                        DimensionRecordColumnTag(element1, "region"),
                        DimensionRecordColumnTag(element2, "region"),
                    )
                )
                columns[element1] |= {"region"}
                columns[element2] |= {"region"}
            relation = relation.join(overlaps)

        # All skypix columns need to come from either the initial_relation or
        # a spatial join, since we need all dimension key columns present in
        # the SQL engine and skypix relations (which just provide regions) are
        # added by postprocessing in the native iteration engine.
        for dimension in dimensions:
            if DimensionKeyColumnTag(dimension.name) not in relation.columns and isinstance(
                dimension, SkyPixDimension
            ):
                raise NotImplementedError(
                    f"Cannot construct query involving skypix dimension {dimension.name} unless "
                    "it is part of a dataset subquery, spatial join, or other initial relation."
                )

        # Categorize all of the initial columns to identify joins we don't need
        # to also include later.
        initial_columns = ColumnCategorization.from_iterable(relation.columns)

        # Iterate over all dimension elements whose relations definitely have
        # to be joined in.  The order doesn't matter as long as we can assume
        # the database query optimizer is going to try to reorder them anyway.
        for element in dimensions.elements:
            columns_still_needed = columns[element.name] - initial_columns.dimension_records[element.name]
            # Two separate conditions in play here:
            # - if we need a record column (not just key columns) from this
            #   element, we have to join in its relation;
            # - if the element establishes a relationship between key columns
            #   that wasn't already established by the initial relation, we
            #   always join that element's relation.  Any element with
            #   implied dependencies or the alwaysJoin flag establishes such a
            #   relationship.
            if columns_still_needed or (
                (element.alwaysJoin or element.implied)
                and not element.dimensions.names <= initial_key_relationships
            ):
                storage = self._managers.dimensions[element]
                element_relation = storage.make_relation(columns_still_needed, self._engine)
                if element_relation.engine is iteration.engine:
                    # Most of these element relations will be in the SQL
                    # engine, while at least those for skypix dimensions will
                    # be in the native iteration, representing relations whose
                    # rows are computed on-the-fly in Python, not stored in the
                    # SQL database.
                    #
                    # If we haven't seen any such relations so far, and the
                    # initial_relation didn't already include a transfer to the
                    # native iteration, add it now.
                    if relation.engine is not iteration.engine:
                        relation = relation.transfer(iteration.engine)
                # Relation.join is capable of inserting a join to a SQL
                # element_relation prior to the transfer from the SQL engine to
                # the native iteration engine if that transfer already exists
                # in LHS relation, as long as any operations between the LHS
                # root relation and that transfer commute with join.
                relation = relation.join(element_relation)

        # At this point we've joined in all of the element relations that
        # definitely need to be included, but we may not have all of the
        # dimension key columns in the query that we want.  To fill out that
        # set, we iterate over just the given DimensionGraph's dimensions (not
        # all dimension *elements*) in reverse topological order.  That order
        # should reduce the total number of tables we bring in, since each
        # dimension will bring in keys for its required dependencies before we
        # get to those required dependencies.
        for dimension in self.universe.sorted(dimensions, reverse=True):
            if DimensionKeyColumnTag(dimension.name) not in relation.columns:
                storage = self._managers.dimensions[dimension]
                dimension_relation = storage.make_relation(frozenset(), self._engine)
                relation = relation.join(dimension_relation)

        # Add the predicates we constructed earlier, with a transfer to native
        # iteration first if necessary.
        if predicates and relation.engine is not iteration.engine:
            relation = relation.transfer(iteration.engine)
        for predicate in predicates:
            relation = relation.selection(predicate)

        return relation
