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

from lsst.utils.iteration import ensure_iterable

from .._butler_collections import ButlerCollections, CollectionInfo
from .._collection_type import CollectionType
from .._dataset_type import DatasetType
from ..utils import has_globs
from ._collection_args import convert_collection_arg_to_glob_string_list
from ._defaults import DefaultsHolder
from ._http_connection import RemoteButlerHttpConnection, parse_model
from ._ref_utils import normalize_dataset_type_name
from .server_models import QueryCollectionInfoRequestModel, QueryCollectionInfoResponseModel


class RemoteButlerCollections(ButlerCollections):
    """Implementation of ButlerCollections for RemoteButler.

    Parameters
    ----------
    defaults : `DefaultsHolder`
        Registry object used to look up default collections.
    connection : `RemoteButlerHttpConnection`
        HTTP connection to Butler server.
    """

    def __init__(self, defaults: DefaultsHolder, connection: RemoteButlerHttpConnection):
        self._defaults = defaults
        self._connection = connection

    @property
    def defaults(self) -> Sequence[str]:
        return self._defaults.get().collections

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
        summary_datasets: Iterable[DatasetType] | Iterable[str] | None = None,
    ) -> Sequence[CollectionInfo]:
        if collection_types is None:
            types = list(CollectionType.all())
        else:
            types = list(ensure_iterable(collection_types))

        if include_chains is None:
            include_chains = not flatten_chains

        if summary_datasets is None:
            dataset_types = None
        else:
            dataset_types = [normalize_dataset_type_name(t) for t in summary_datasets]

        request = QueryCollectionInfoRequestModel(
            expression=convert_collection_arg_to_glob_string_list(expression),
            collection_types=types,
            flatten_chains=flatten_chains,
            include_chains=include_chains,
            include_parents=include_parents,
            include_summary=include_summary,
            include_doc=include_doc,
            summary_datasets=dataset_types,
        )
        response = self._connection.post("query_collection_info", request)
        model = parse_model(response, QueryCollectionInfoResponseModel)

        return model.collections

    def get_info(
        self, name: str, include_parents: bool = False, include_summary: bool = False
    ) -> CollectionInfo:
        if has_globs(name):
            raise ValueError("Search expressions are not allowed in 'name' parameter to get_info")
        results = self.query_info(
            name, include_parents=include_parents, include_summary=include_summary, include_doc=True
        )
        assert len(results) == 1, "Only one result should be returned for get_info."
        return results[0]

    def register(self, name: str, type: CollectionType = CollectionType.RUN, doc: str | None = None) -> bool:
        raise NotImplementedError("Not yet available.")

    def x_remove(self, name: str) -> None:
        raise NotImplementedError("Not yet available.")
