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

__all__ = ["DirectQuery"]

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from ._query import Query
from .direct_query_results import (
    DirectDataCoordinateQueryResults,
    DirectDatasetQueryResults,
    DirectDimensionRecordQueryResults,
    DirectSingleTypeDatasetQueryResults,
)
from .registry import queries as registry_queries
from .registry.sql_registry import SqlRegistry

if TYPE_CHECKING:
    from ._query_results import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults
    from .dimensions import DataId, DimensionGroup
    from .registry._registry import CollectionArgType


class DirectQuery(Query):
    """Implementation of `Query` interface used by `DirectButler`.

    Parameters
    ----------
    registry : `SqlRegistry`
        The object that manages dataset metadata and relationships.
    """

    _registry: SqlRegistry

    def __init__(self, registry: SqlRegistry):
        self._registry = registry

    def data_ids(
        self,
        dimensions: DimensionGroup | Iterable[str] | str,
        *,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> DataCoordinateQueryResults:
        # Docstring inherited.
        registry_query_result = self._registry.queryDataIds(
            dimensions,
            dataId=data_id,
            where=where,
            bind=bind,
            **kwargs,
        )
        return DirectDataCoordinateQueryResults(registry_query_result)

    def datasets(
        self,
        dataset_type: Any,
        collections: CollectionArgType | None = None,
        *,
        find_first: bool = True,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> DatasetQueryResults:
        # Docstring inherited.
        registry_query_result = self._registry.queryDatasets(
            dataset_type,
            collections=collections,
            dataId=data_id,
            where=where,
            findFirst=find_first,
            bind=bind,
            **kwargs,
        )
        if isinstance(registry_query_result, registry_queries.ParentDatasetQueryResults):
            return DirectSingleTypeDatasetQueryResults(registry_query_result)
        else:
            return DirectDatasetQueryResults(registry_query_result)

    def dimension_records(
        self,
        element: str,
        *,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> DimensionRecordQueryResults:
        # Docstring inherited.
        registry_query_result = self._registry.queryDimensionRecords(
            element,
            dataId=data_id,
            where=where,
            bind=bind,
            **kwargs,
        )
        return DirectDimensionRecordQueryResults(registry_query_result)
