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

from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any

from lsst.daf.relation import Relation

from ...core import DatasetType, DimensionUniverse
from .._collectionType import CollectionType
from ._query_backend import QueryBackend
from ._sql_query_context import SqlQueryContext

if TYPE_CHECKING:
    from ..interfaces import CollectionRecord, Database
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
    def managers(self) -> RegistryManagerInstances:
        # Docstring inherited.
        return self._managers

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._managers.dimensions.universe

    def context(self) -> SqlQueryContext:
        # Docstring inherited.
        return SqlQueryContext(self._db, self._managers.column_types)

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
        for dataset_type, filtered_collections in result.items():
            for collection_record in collections:
                if not dataset_type.isCalibration() and collection_record.type is CollectionType.CALIBRATION:
                    if rejections is not None:
                        rejections.append(
                            f"Not searching for non-calibration dataset of type {dataset_type.name!r} "
                            f"in CALIBRATION collection {collection_record.name!r}."
                        )
                else:
                    collection_summary = self._managers.datasets.getCollectionSummary(collection_record)
                    if collection_summary.is_compatible_with(
                        dataset_type,
                        governor_constraints,
                        rejections=rejections,
                        name=collection_record.name,
                    ):
                        filtered_collections.append(collection_record)
        return result

    def make_dataset_query_relation(
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
