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

__all__ = ("SqlQueryBackend",)

from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any, cast

from lsst.daf.relation import ColumnError, ColumnExpression, ColumnTag, Join, Predicate, Relation

from ..._column_categorization import ColumnCategorization
from ..._column_tags import DimensionKeyColumnTag, DimensionRecordColumnTag
from ..._dataset_type import DatasetType
from ...dimensions import DataCoordinate, DimensionGroup, DimensionRecord, DimensionUniverse
from .._collection_type import CollectionType
from .._exceptions import DataIdValueError
from ..interfaces import CollectionRecord, Database
from ._query_backend import QueryBackend
from ._sql_query_context import SqlQueryContext

if TYPE_CHECKING:
    from ..managers import RegistryManagerInstances


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
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._managers.dimensions.universe

    def context(self) -> SqlQueryContext:
        # Docstring inherited.
        return SqlQueryContext(self._db, self._managers.column_types)

    def get_collection_name(self, key: Any) -> str:
        return self._managers.collections[key].name

    def resolve_collection_wildcard(
        self,
        expression: Any,
        *,
        collection_types: Set[CollectionType] = CollectionType.all(),
        done: set[str] | None = None,
        flatten_chains: bool = True,
        include_chains: bool | None = None,
    ) -> list[CollectionRecord]:
        # Docstring inherited.
        return self._managers.collections.resolve_wildcard(
            expression,
            collection_types=collection_types,
            done=done,
            flatten_chains=flatten_chains,
            include_chains=include_chains,
        )

    def resolve_dataset_type_wildcard(
        self,
        expression: Any,
        components: bool | None = None,
        missing: list[str] | None = None,
        explicit_only: bool = False,
        components_deprecated: bool = True,
    ) -> dict[DatasetType, list[str | None]]:
        # Docstring inherited.
        return self._managers.datasets.resolve_wildcard(
            expression, components, missing, explicit_only, components_deprecated
        )

    def filter_dataset_collections(
        self,
        dataset_types: Iterable[DatasetType],
        collections: Sequence[CollectionRecord],
        *,
        governor_constraints: Mapping[str, Set[str]],
        rejections: list[str] | None = None,
    ) -> dict[DatasetType, list[CollectionRecord]]:
        # Docstring inherited.
        result: dict[DatasetType, list[CollectionRecord]] = {
            dataset_type: [] for dataset_type in dataset_types
        }
        summaries = self._managers.datasets.fetch_summaries(collections, result.keys())
        for dataset_type, filtered_collections in result.items():
            for collection_record in collections:
                if not dataset_type.isCalibration() and collection_record.type is CollectionType.CALIBRATION:
                    if rejections is not None:
                        rejections.append(
                            f"Not searching for non-calibration dataset of type {dataset_type.name!r} "
                            f"in CALIBRATION collection {collection_record.name!r}."
                        )
                else:
                    collection_summary = summaries[collection_record.key]
                    if collection_summary.is_compatible_with(
                        dataset_type,
                        governor_constraints,
                        rejections=rejections,
                        name=collection_record.name,
                    ):
                        filtered_collections.append(collection_record)
        return result

    def _make_dataset_query_relation_impl(
        self,
        dataset_type: DatasetType,
        collections: Sequence[CollectionRecord],
        columns: Set[str],
        context: SqlQueryContext,
    ) -> Relation:
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
                context=context,
            )
        else:
            return dataset_storage.make_relation(
                *collections,
                columns=columns,
                context=context,
            )

    def make_dimension_relation(
        self,
        dimensions: DimensionGroup,
        columns: Set[ColumnTag],
        context: SqlQueryContext,
        *,
        initial_relation: Relation | None = None,
        initial_join_max_columns: frozenset[ColumnTag] | None = None,
        initial_dimension_relationships: Set[frozenset[str]] | None = None,
        spatial_joins: Iterable[tuple[str, str]] = (),
        governor_constraints: Mapping[str, Set[str]],
    ) -> Relation:
        # Docstring inherited.

        default_join = Join(max_columns=initial_join_max_columns)

        # Set up the relation variable we'll update as we join more relations
        # in, and ensure it is in the SQL engine.
        relation = context.make_initial_relation(initial_relation)

        if initial_dimension_relationships is None:
            relationships = self.extract_dimension_relationships(relation)
        else:
            relationships = set(initial_dimension_relationships)

        # Make a mutable copy of the columns argument.
        columns_required = set(columns)

        # Sort spatial joins to put those involving the commonSkyPix dimension
        # first, since those join subqueries might get reused in implementing
        # other joins later.
        spatial_joins = list(spatial_joins)
        spatial_joins.sort(key=lambda j: self.universe.commonSkyPix.name not in j)

        # Next we'll handle spatial joins, since those can require refinement
        # predicates that will need region columns to be included in the
        # relations we'll join.
        predicate: Predicate = Predicate.literal(True)
        for element1, element2 in spatial_joins:
            (overlaps, needs_refinement) = self._managers.dimensions.make_spatial_join_relation(
                element1,
                element2,
                context=context,
                governor_constraints=governor_constraints,
                existing_relationships=relationships,
            )
            if needs_refinement:
                predicate = predicate.logical_and(
                    context.make_spatial_region_overlap_predicate(
                        ColumnExpression.reference(DimensionRecordColumnTag(element1, "region")),
                        ColumnExpression.reference(DimensionRecordColumnTag(element2, "region")),
                    )
                )
                columns_required.add(DimensionRecordColumnTag(element1, "region"))
                columns_required.add(DimensionRecordColumnTag(element2, "region"))
            relation = relation.join(overlaps)
            relationships.add(
                frozenset(self.universe[element1].dimensions.names | self.universe[element2].dimensions.names)
            )

        # All skypix columns need to come from either the initial_relation or a
        # spatial join, since we need all dimension key columns present in the
        # SQL engine and skypix regions are added by postprocessing in the
        # native iteration engine.
        for skypix_dimension_name in dimensions.skypix:
            if DimensionKeyColumnTag(skypix_dimension_name) not in relation.columns:
                raise NotImplementedError(
                    f"Cannot construct query involving skypix dimension {skypix_dimension_name} unless "
                    "it is part of a dataset subquery, spatial join, or other initial relation."
                )

        # Before joining in new tables to provide columns, attempt to restore
        # them from the given relation by weakening projections applied to it.
        relation, _ = context.restore_columns(relation, columns_required)

        # Categorize columns not yet included in the relation to associate them
        # with dimension elements and detect bad inputs.
        missing_columns = ColumnCategorization.from_iterable(columns_required - relation.columns)
        if not (missing_columns.dimension_keys <= dimensions.names):
            raise ColumnError(
                "Cannot add dimension key column(s) "
                f"{{{', '.join(name for name in missing_columns.dimension_keys)}}} "
                f"that were not included in the given dimensions {dimensions}."
            )
        if missing_columns.datasets:
            raise ColumnError(
                f"Unexpected dataset columns {missing_columns.datasets} in call to make_dimension_relation; "
                "use make_dataset_query_relation or make_dataset_search relation instead, or filter them "
                "out if they have already been added or will be added later."
            )
        for element_name in missing_columns.dimension_records:
            if element_name not in dimensions.elements.names:
                raise ColumnError(
                    f"Cannot join dimension element {element_name} whose dimensions are not a "
                    f"subset of {dimensions}."
                )

        # Iterate over all dimension elements whose relations definitely have
        # to be joined in.  The order doesn't matter as long as we can assume
        # the database query optimizer is going to try to reorder them anyway.
        for element_name in dimensions.elements:
            columns_still_needed = missing_columns.dimension_records[element_name]
            element = self.universe[element_name]
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
                and frozenset(element.dimensions.names) not in relationships
            ):
                storage = self._managers.dimensions[element_name]
                relation = storage.join(relation, default_join, context)
        # At this point we've joined in all of the element relations that
        # definitely need to be included, but we may not have all of the
        # dimension key columns in the query that we want.  To fill out that
        # set, we iterate over just the given DimensionGroup's dimensions (not
        # all dimension *elements*) in reverse topological order.  That order
        # should reduce the total number of tables we bring in, since each
        # dimension will bring in keys for its required dependencies before we
        # get to those required dependencies.
        for dimension_name in reversed(dimensions.names.as_tuple()):
            if DimensionKeyColumnTag(dimension_name) not in relation.columns:
                storage = self._managers.dimensions[dimension_name]
                relation = storage.join(relation, default_join, context)

        # Add the predicates we constructed earlier, with a transfer to native
        # iteration first if necessary.
        if not predicate.as_trivial():
            relation = relation.with_rows_satisfying(
                predicate, preferred_engine=context.iteration_engine, transfer=True
            )

        # Finally project the new relation down to just the columns in the
        # initial relation, the dimension key columns, and the new columns
        # requested.
        columns_kept = set(columns)
        if initial_relation is not None:
            columns_kept.update(initial_relation.columns)
        columns_kept.update(DimensionKeyColumnTag.generate(dimensions.names))
        relation = relation.with_only_columns(columns_kept, preferred_engine=context.preferred_engine)

        return relation

    def resolve_governor_constraints(
        self, dimensions: DimensionGroup, constraints: Mapping[str, Set[str]], context: SqlQueryContext
    ) -> Mapping[str, Set[str]]:
        # Docstring inherited.
        result: dict[str, Set[str]] = {}
        for dimension_name in dimensions.governors:
            storage = self._managers.dimensions[dimension_name]
            records = storage.get_record_cache(context)
            assert records is not None, "Governor dimensions are always cached."
            all_values = {cast(str, data_id[dimension_name]) for data_id in records}
            if (constraint_values := constraints.get(dimension_name)) is not None:
                if not (constraint_values <= all_values):
                    raise DataIdValueError(
                        f"Unknown values specified for governor dimension {dimension_name}: "
                        f"{constraint_values - all_values}."
                    )
                result[dimension_name] = constraint_values
            else:
                result[dimension_name] = all_values
        return result

    def get_dimension_record_cache(
        self,
        element_name: str,
        context: SqlQueryContext,
    ) -> Mapping[DataCoordinate, DimensionRecord] | None:
        return self._managers.dimensions[element_name].get_record_cache(context)
