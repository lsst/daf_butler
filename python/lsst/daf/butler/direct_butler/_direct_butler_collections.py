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

from collections.abc import Iterable, Sequence, Set

import sqlalchemy
from lsst.utils.iteration import ensure_iterable

from .._butler_collections import ButlerCollections, CollectionInfo
from .._collection_type import CollectionType
from ..registry._exceptions import OrphanedRecordError
from ..registry.interfaces import ChainedCollectionRecord
from ..registry.sql_registry import SqlRegistry


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

    def x_query(
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

    def x_query_info(
        self,
        expression: str | Iterable[str],
        collection_types: Set[CollectionType] | CollectionType | None = None,
        flatten_chains: bool = False,
        include_chains: bool | None = None,
        include_parents: bool = False,
        include_summary: bool = False,
    ) -> Sequence[CollectionInfo]:
        info = []
        with self._registry.caching_context():
            if collection_types is None:
                collection_types = CollectionType.all()
            for name in self._registry.queryCollections(
                expression,
                collectionTypes=collection_types,
                flattenChains=flatten_chains,
                includeChains=include_chains,
            ):
                info.append(
                    self.get_info(name, include_parents=include_parents, include_summary=include_summary)
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
