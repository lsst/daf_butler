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

__all__ = ("RemoteButlerCollections",)

from collections.abc import Iterable, Sequence, Set
from typing import TYPE_CHECKING

from lsst.utils.iteration import ensure_iterable

from .._butler_collections import ButlerCollections, CollectionInfo
from .._collection_type import CollectionType
from ._collection_args import convert_collection_arg_to_glob_string_list
from ._http_connection import RemoteButlerHttpConnection, parse_model
from .server_models import QueryCollectionInfoRequestModel, QueryCollectionInfoResponseModel

if TYPE_CHECKING:
    from .._dataset_type import DatasetType
    from ._registry import RemoteButlerRegistry


class RemoteButlerCollections(ButlerCollections):
    """Implementation of ButlerCollections for RemoteButler.

    Parameters
    ----------
    registry : `~lsst.daf.butler.registry.sql_registry.SqlRegistry`
        Registry object used to work with the collections database.
    connection : `RemoteButlerHttpConnection`
        HTTP connection to Butler server.
    """

    def __init__(self, registry: RemoteButlerRegistry, connection: RemoteButlerHttpConnection):
        self._registry = registry
        self._connection = connection

    @property
    def defaults(self) -> Sequence[str]:
        return self._registry.defaults.collections

    def extend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        raise NotImplementedError("Not yet available")

    def prepend_chain(self, parent_collection_name: str, child_collection_names: str | Iterable[str]) -> None:
        raise NotImplementedError("Not yet available")

    def redefine_chain(
        self, parent_collection_name: str, child_collection_names: str | Iterable[str]
    ) -> None:
        raise NotImplementedError("Not yet available")

    def remove_from_chain(
        self, parent_collection_name: str, child_collection_names: str | Iterable[str]
    ) -> None:
        raise NotImplementedError("Not yet available")

    def query_info(
        self,
        expression: str | Iterable[str],
        collection_types: Set[CollectionType] | CollectionType | None = None,
        flatten_chains: bool = False,
        include_chains: bool | None = None,
        include_parents: bool = False,
        include_summary: bool = False,
        include_doc: bool = False,
        summary_datasets: Iterable[DatasetType] | None = None,
    ) -> Sequence[CollectionInfo]:
        if collection_types is None:
            types = list(CollectionType.all())
        else:
            types = list(ensure_iterable(collection_types))

        if include_chains is None:
            include_chains = not flatten_chains

        request = QueryCollectionInfoRequestModel(
            expression=convert_collection_arg_to_glob_string_list(expression),
            collection_types=types,
            flatten_chains=flatten_chains,
            include_chains=include_chains,
            include_parents=include_parents,
            include_summary=include_summary,
        )
        response = self._connection.post("query_collection_info", request)
        model = parse_model(response, QueryCollectionInfoResponseModel)

        return model.collections

    def get_info(
        self, name: str, include_parents: bool = False, include_summary: bool = False
    ) -> CollectionInfo:
        info = self._registry._get_collection_info(name, include_doc=True, include_parents=include_parents)
        doc = info.doc or ""
        children = info.children or ()
        dataset_types: Set[str] | None = None
        if include_summary:
            summary = self._registry.getCollectionSummary(name)
            dataset_types = frozenset([dt.name for dt in summary.dataset_types])
        return CollectionInfo(
            name=name,
            type=info.type,
            doc=doc,
            parents=info.parents,
            children=children,
            dataset_types=dataset_types,
        )

    def register(self, name: str, type: CollectionType = CollectionType.RUN, doc: str | None = None) -> bool:
        raise NotImplementedError("Not yet available.")

    def x_remove(self, name: str) -> None:
        raise NotImplementedError("Not yet available.")
