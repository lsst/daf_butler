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

__all__ = ("DirectButlerCollections",)

from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any

import sqlalchemy
from lsst.utils.iteration import ensure_iterable

from .._butler_collections import ButlerCollections, CollectionInfo
from .._collection_type import CollectionType
from ..registry._exceptions import OrphanedRecordError
from ..registry.interfaces import ChainedCollectionRecord
from ..registry.sql_registry import SqlRegistry
from ..registry.wildcards import CollectionWildcard

if TYPE_CHECKING:
    from .._dataset_type import DatasetType
    from ..registry._collection_summary import CollectionSummary


class DirectButlerCollections(ButlerCollections):
    """Implementation of ButlerCollections for DirectButler.

    Parameters
    ----------
    registry : `~lsst.daf.butler.registry.sql_registry.SqlRegistry`
        Registry object used to work with the collections database.
    """

    def __init__(self, registry: SqlRegistry):
        self._registry = registry

    @property
    def defaults(self) -> Sequence[str]:
        return self._registry.defaults.collections

    def extend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        return self._registry._managers.collections.extend_collection_chain(
            parent_collection_name, list(ensure_iterable(child_collection_names))
        )

    def prepend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        return self._registry._managers.collections.prepend_collection_chain(
            parent_collection_name, list(ensure_iterable(child_collection_names))
        )

    def redefine_chain(
        self, parent_collection_name: str, child_collection_names: str | Iterable[str]
    ) -> None:
        self._registry._managers.collections.update_chain(
            parent_collection_name, list(ensure_iterable(child_collection_names))
        )

    def remove_from_chain(
        self, parent_collection_name: str, child_collection_names: str | Iterable[str]
    ) -> None:
        return self._registry._managers.collections.remove_from_collection_chain(
            parent_collection_name, list(ensure_iterable(child_collection_names))
        )

    def query(
        self,
        expression: str | Iterable[str],
        collection_types: Set[CollectionType] | CollectionType | None = None,
        flatten_chains: bool = False,
        include_chains: bool | None = None,
    ) -> Sequence[str]:
        if collection_types is None:
            collection_types = CollectionType.all()
        # Do not use base implementation for now to avoid the additional
        # unused queries.
        return self._registry.queryCollections(
            expression,
            collectionTypes=collection_types,
            flattenChains=flatten_chains,
            includeChains=include_chains,
        )

    def query_info(
        self,
        expression: str | Iterable[str],
        collection_types: Set[CollectionType] | CollectionType | None = None,
        flatten_chains: bool = False,
        include_chains: bool | None = None,
        include_parents: bool = False,
        include_summary: bool = False,
        include_doc: bool = False,
        summary_datasets: Iterable[DatasetType] | Iterable[str] | None = None,
    ) -> Sequence[CollectionInfo]:
        info = []
        if collection_types is None:
            collection_types = CollectionType.all()
        elif isinstance(collection_types, CollectionType):
            collection_types = {collection_types}

        records = self._registry._managers.collections.resolve_wildcard(
            CollectionWildcard.from_expression(expression),
            collection_types=collection_types,
            flatten_chains=flatten_chains,
            include_chains=include_chains,
        )

        summaries: Mapping[Any, CollectionSummary] = {}
        if include_summary:
            summaries = self._registry._managers.datasets.fetch_summaries(records, summary_datasets)

        docs: Mapping[Any, str] = {}
        if include_doc:
            docs = self._registry._managers.collections.get_docs(record.key for record in records)

        for record in records:
            doc = docs.get(record.key, "")
            children: tuple[str, ...] = tuple()
            if record.type == CollectionType.CHAINED:
                assert isinstance(record, ChainedCollectionRecord)
                children = tuple(record.children)
            parents: frozenset[str] | None = None
            if include_parents:
                # TODO: This is non-vectorized, so expensive to do in a
                # loop.
                parents = frozenset(self._registry.getCollectionParentChains(record.name))
            dataset_types: Set[str] | None = None
            if summary := summaries.get(record.key):
                dataset_types = frozenset([dt.name for dt in summary.dataset_types])

            info.append(
                CollectionInfo(
                    name=record.name,
                    type=record.type,
                    doc=doc,
                    parents=parents,
                    children=children,
                    dataset_types=dataset_types,
                )
            )

        return info

    def get_info(
        self, name: str, include_parents: bool = False, include_summary: bool = False
    ) -> CollectionInfo:
        record = self._registry.get_collection_record(name)
        doc = self._registry.getCollectionDocumentation(name) or ""
        children: tuple[str, ...] = tuple()
        if record.type == CollectionType.CHAINED:
            assert isinstance(record, ChainedCollectionRecord)
            children = tuple(record.children)
        parents: set[str] | None = None
        if include_parents:
            parents = self._registry.getCollectionParentChains(name)
        dataset_types: Set[str] | None = None
        if include_summary:
            summary = self._registry.getCollectionSummary(name)
            dataset_types = frozenset([dt.name for dt in summary.dataset_types])

        return CollectionInfo(
            name=name,
            type=record.type,
            doc=doc,
            parents=parents,
            children=children,
            dataset_types=dataset_types,
        )

    def register(self, name: str, type: CollectionType = CollectionType.RUN, doc: str | None = None) -> bool:
        return self._registry.registerCollection(name, type, doc)

    def x_remove(self, name: str) -> None:
        try:
            self._registry.removeCollection(name)
        except sqlalchemy.exc.IntegrityError as e:
            raise OrphanedRecordError(f"Datasets in run {name} are still referenced elsewhere.") from e
