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

__all__ = ("HybridButlerCollections",)

from collections.abc import Iterable, Sequence, Set
from typing import TYPE_CHECKING

from .._butler_collections import ButlerCollections, CollectionInfo
from .._collection_type import CollectionType

if TYPE_CHECKING:
    from .._dataset_type import DatasetType
    from .hybrid_butler import HybridButler


class HybridButlerCollections(ButlerCollections):
    """Implementation of ButlerCollections for HybridButler.

    Parameters
    ----------
    butler : `~lsst.daf.butler.tests.hybrid_butler.HybridButler`
        Hybrid butler to use.
    """

    def __init__(self, butler: HybridButler):
        self._hybrid = butler

    @property
    def defaults(self) -> Sequence[str]:
        return self._hybrid._remote_butler.collections.defaults

    def extend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        return self._hybrid._direct_butler.collections.extend_chain(
            parent_collection_name, child_collection_names
        )

    def prepend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        return self._hybrid._direct_butler.collections.prepend_chain(
            parent_collection_name, child_collection_names
        )

    def redefine_chain(
        self, parent_collection_name: str, child_collection_names: str | Iterable[str]
    ) -> None:
        self._hybrid._direct_butler.collections.redefine_chain(parent_collection_name, child_collection_names)

    def remove_from_chain(
        self, parent_collection_name: str, child_collection_names: str | Iterable[str]
    ) -> None:
        return self._hybrid._direct_butler.collections.remove_from_chain(
            parent_collection_name, child_collection_names
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
        return self._hybrid._remote_butler.collections.query_info(
            expression,
            collection_types=collection_types,
            flatten_chains=flatten_chains,
            include_chains=include_chains,
            include_parents=include_parents,
            include_summary=include_summary,
            include_doc=include_doc,
            summary_datasets=summary_datasets,
        )

    def get_info(
        self, name: str, include_parents: bool = False, include_summary: bool = False
    ) -> CollectionInfo:
        return self._hybrid._remote_butler.collections.get_info(
            name, include_parents=include_parents, include_summary=include_summary
        )

    def register(self, name: str, type: CollectionType = CollectionType.RUN, doc: str | None = None) -> bool:
        return self._hybrid._direct_butler.collections.register(name, type=type, doc=doc)

    def x_remove(self, name: str) -> None:
        self._hybrid._direct_butler.collections.x_remove(name)
