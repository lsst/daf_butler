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

__all__ = (
    "DatasetLogicalTable",
    "DatasetLogicalTableFactory",
)

from typing import AbstractSet, Iterable, Optional

from lsst.utils.classes import cached_getter

from ...core import DatasetType, DimensionGraph, DimensionUniverse
from ...core.named import NamedKeyDict, NamedValueAbstractSet, NamedValueSet
from .._defaults import RegistryDefaults
from ..interfaces import CollectionRecord
from ..interfaces.queries import (
    ColumnTag,
    DimensionKeyColumnTag,
    LogicalTable,
    LogicalTableFactory,
    QueryConstructionDataRequest,
    QueryConstructionDataResult,
)
from ..summaries import CollectionSummary, GovernorDimensionRestriction
from ..wildcards import CollectionWildcard, DatasetTypeWildcard


class DatasetLogicalTableFactory(LogicalTableFactory):
    def __init__(
        self,
        dataset_types: DatasetTypeWildcard,
        collections: CollectionWildcard,
        *,
        components: Optional[bool] = None,
    ):
        self._dataset_types = dataset_types
        self._collections = collections
        self._components = components

    @property
    def data_requested(self) -> QueryConstructionDataRequest:
        return QueryConstructionDataRequest(
            self._dataset_types,
            self._collections,
        )

    def make_logical_table(
        self,
        data: QueryConstructionDataResult,
        columns_requested: AbstractSet[ColumnTag],
        *,
        defaults: RegistryDefaults,
        universe: DimensionUniverse,
    ) -> LogicalTable:
        missing: list[str] = []
        logical_tables: list[LogicalTable] = []
        for dataset_type in self._dataset_types.resolve_dataset_types(
            data.dataset_types, components=self._components, missing=missing
        ):
            collection_summaries = NamedKeyDict[CollectionRecord, CollectionSummary]()
            skipped_collections = NamedKeyDict[CollectionRecord, str]()

            for collection_record in self._collections.iter(collection_summaries.keys()):
                summary = collection_summaries[collection_record]
                if dataset_type.name in summary.datasetTypes.names:
                    collection_summaries[collection_record] = summary
                else:
                    skipped_collections[collection_record] = (
                        f"No datasets of type {dataset_type.name!r} in "
                        f"collection {collection_record.name!r}."
                    )
            logical_tables.append(
                DatasetLogicalTable(
                    dataset_type=dataset_type,
                    collection_summaries=collection_summaries,
                    skipped_collections=skipped_collections,
                    extra_columns=columns_requested,
                )
            )
        for dataset_type_name in missing:
            logical_tables.append(
                LogicalTable.without_rows(
                    f"Dataset type {dataset_type_name!r} is not registered, "
                    "so no datasets of this type can exist.",
                    universe=universe,
                )
            )
        if not logical_tables:
            return LogicalTable.without_rows(
                f"No dataset types match expression {self._dataset_types}.", universe=universe
            )
        return LogicalTable.join(*logical_tables, universe=universe)


class DatasetLogicalTable(LogicalTable):
    def __init__(
        self,
        *,
        dataset_type: DatasetType,
        collection_summaries: NamedKeyDict[CollectionRecord, CollectionSummary],
        skipped_collections: NamedKeyDict[CollectionRecord, str],
        extra_columns: Iterable[ColumnTag],
    ):
        self._dataset_type = dataset_type
        self._collection_summaries = collection_summaries
        self._skipped_collections = skipped_collections
        self._extra_columns = frozenset(extra_columns)

    @property
    def dimensions(self) -> DimensionGraph:
        return self._dataset_type.dimensions

    @property  # type: ignore
    @cached_getter
    def governor_restriction(self) -> GovernorDimensionRestriction:
        return GovernorDimensionRestriction.makeEmpty(self.dimensions.universe).union(
            *[s.dimensions for s in self._collection_summaries.values()]
        )

    @property  # type: ignore
    @cached_getter
    def dataset_types(self) -> NamedValueAbstractSet[DatasetType]:
        return NamedValueSet({self._dataset_type}).freeze()

    @property
    def is_doomed(self) -> bool:
        return not self._collection_summaries

    def diagnostics(self, verbose: bool = False) -> Iterable[str]:
        if verbose or not self._collection_summaries:
            return self._skipped_collections.values()
        else:
            return ()

    def restricted_to(
        self, dimensions: DimensionGraph, restriction: GovernorDimensionRestriction
    ) -> tuple[LogicalTable, bool]:
        new_skipped_collections = NamedKeyDict[CollectionRecord, str]()
        for collection_record, summary in self._collection_summaries.items():
            doomed_dimensions = summary.dimensions.intersection(restriction).dooms_on(dimensions)
            if doomed_dimensions:
                new_skipped_collections[collection_record] = (
                    f"No datasets of type {self._dataset_type.name!r} in "
                    f"collection {collection_record.name!r} are consistent "
                    f"with {doomed_dimensions} values from other query "
                    "constraints."
                )
        if not new_skipped_collections:
            return self, False
        collection_summaries = NamedKeyDict[CollectionRecord, CollectionSummary](
            {r: s for r, s in self._collection_summaries.items() if r not in new_skipped_collections}
        )
        new_skipped_collections.update(self._skipped_collections)
        return (
            DatasetLogicalTable(
                dataset_type=self._dataset_type,
                collection_summaries=collection_summaries,
                skipped_collections=new_skipped_collections,
                extra_columns=self._extra_columns,
            ),
            True,
        )

    @property  # type: ignore
    @cached_getter
    def columns_provided(self) -> AbstractSet[ColumnTag]:
        result: set[ColumnTag] = set()
        result.update(DimensionKeyColumnTag.generate(self._dataset_type.dimensions.required.names))
        result.update(self._extra_columns)
        return frozenset(result)
