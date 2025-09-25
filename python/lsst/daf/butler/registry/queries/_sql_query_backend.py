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

import warnings
from collections.abc import Iterable, Mapping, Sequence, Set
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, cast

from lsst.daf.relation import Relation
from lsst.utils.introspection import find_outside_stacklevel

from ..._collection_type import CollectionType
from ..._dataset_type import DatasetType
from ..._exceptions import DataIdValueError, MissingDatasetTypeError
from ...dimensions import DimensionGroup, DimensionRecordSet, DimensionUniverse
from ...dimensions.record_cache import DimensionRecordCache
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
    dimension_record_cache : `DimensionRecordCache`
        Cache of all records for dimension elements with
        `~DimensionElement.is_cached` `True`.
    """

    def __init__(
        self, db: Database, managers: RegistryManagerInstances, dimension_record_cache: DimensionRecordCache
    ):
        self._db = db
        self._managers = managers
        self._dimension_record_cache = dimension_record_cache

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._managers.dimensions.universe

    def caching_context(self) -> AbstractContextManager[None]:
        # Docstring inherited.
        return self._managers.caching_context_manager()

    def context(self) -> SqlQueryContext:
        # Docstring inherited.
        return SqlQueryContext(self._db, self._managers.column_types)

    def get_collection_name(self, key: Any) -> str:
        assert self._managers.caching_context.collection_records is not None, (
            "Collection-record caching should already been enabled any time this is called."
        )
        return self._managers.collections[key].name

    def resolve_collection_wildcard(
        self,
        expression: Any,
        *,
        collection_types: Set[CollectionType] = CollectionType.all(),
        flatten_chains: bool = True,
        include_chains: bool | None = None,
    ) -> list[CollectionRecord]:
        # Docstring inherited.
        return self._managers.collections.resolve_wildcard(
            expression,
            collection_types=collection_types,
            flatten_chains=flatten_chains,
            include_chains=include_chains,
        )

    def resolve_dataset_type_wildcard(
        self,
        expression: Any,
        missing: list[str] | None = None,
        explicit_only: bool = False,
    ) -> list[DatasetType]:
        # Docstring inherited.
        return self._managers.datasets.resolve_wildcard(
            expression,
            missing,
            explicit_only,
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
        try:
            return self._managers.datasets.make_relation(
                dataset_type,
                *collections,
                columns=columns,
                context=context,
            )
        except MissingDatasetTypeError:
            pass
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

    def resolve_governor_constraints(
        self, dimensions: DimensionGroup, constraints: Mapping[str, Set[str]]
    ) -> Mapping[str, Set[str]]:
        # Docstring inherited.
        result: dict[str, Set[str]] = {}
        for dimension_name in dimensions.governors:
            all_values = {
                cast(str, record.dataId[dimension_name])
                for record in self._dimension_record_cache[dimension_name]
            }
            if (constraint_values := constraints.get(dimension_name)) is not None:
                if not (constraint_values <= all_values):
                    warnings.warn(
                        "DataIdValueError will no longer be raised for invalid governor dimension values."
                        " Instead, an empty list will be returned.  Will be changed after v28.",
                        FutureWarning,
                        stacklevel=find_outside_stacklevel("lsst.daf.butler"),
                    )
                    raise DataIdValueError(
                        f"Unknown values specified for governor dimension {dimension_name}: "
                        f"{constraint_values - all_values}."
                    )
                result[dimension_name] = constraint_values
            else:
                result[dimension_name] = all_values
        return result

    def get_dimension_record_cache(self, element_name: str) -> DimensionRecordSet | None:
        return (
            self._dimension_record_cache[element_name]
            if element_name in self._dimension_record_cache
            else None
        )
