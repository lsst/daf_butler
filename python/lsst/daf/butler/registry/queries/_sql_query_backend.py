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
from typing import TYPE_CHECKING

import sqlalchemy
from lsst.daf.relation import Relation, sql
from lsst.utils.sets.unboundable import UnboundableSet

from ...core import (
    ColumnTag,
    DataIdValue,
    DatasetColumnTag,
    DatasetType,
    DimensionKeyColumnTag,
    DimensionUniverse,
)
from ...core.named import NamedValueAbstractSet
from .._collectionType import CollectionType
from ..wildcards import CollectionSearch, CollectionWildcard
from ._query_backend import QueryBackend
from ._sql_query_context import SqlQueryContext
from .find_first import FindFirst

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

    def to_sql_subquery(self, relation: Relation[ColumnTag]) -> sqlalchemy.sql.FromClause:
        # Docstring inherited.
        return self._engine.to_executable(relation, self._managers.column_types).subquery()

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
        ).assert_checked_and_simplified()

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
        return Relation.make_zero(self._engine, columns=column_tags, doomed_by=frozenset(messages))
