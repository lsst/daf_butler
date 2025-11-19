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

__all__ = ("RegistryBase",)

from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from lsst.utils.iteration import ensure_iterable

from .._butler import Butler
from .._collection_type import CollectionType
from .._dataset_association import DatasetAssociation
from .._dataset_type import DatasetType
from ..dimensions import DataId, DimensionElement, DimensionGroup
from ..registry.wildcards import CollectionWildcard, DatasetTypeWildcard
from ._exceptions import ArgumentError, DatasetTypeExpressionError, NoDefaultCollectionError
from ._registry import CollectionArgType, Registry
from .queries import (
    ChainedDatasetQueryResults,
    DataCoordinateQueryResults,
    DatasetQueryResults,
    DimensionRecordQueryResults,
)
from .queries._query_common import CommonQueryArguments, resolve_collections
from .queries._query_data_coordinates import QueryDriverDataCoordinateQueryResults
from .queries._query_datasets import QueryDriverDatasetRefQueryResults
from .queries._query_dimension_records import QueryDriverDimensionRecordQueryResults


class RegistryBase(Registry):
    """Common implementation for `Registry` methods shared between
    DirectButler's RegistryShim and RemoteButlerRegistry.

    Parameters
    ----------
    butler : `Butler`
        Butler instance to which this registry delegates operations.
    """

    def __init__(self, butler: Butler) -> None:
        self._butler = butler

    def queryDatasets(
        self,
        datasetType: Any,
        *,
        collections: CollectionArgType | None = None,
        dimensions: Iterable[str] | None = None,
        dataId: DataId | None = None,
        where: str = "",
        findFirst: bool = False,
        components: bool = False,
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> DatasetQueryResults:
        doomed_by: list[str] = []
        dimension_group = self.dimensions.conform(dimensions) if dimensions is not None else None

        if collections is None and not self.defaults.collections:
            raise NoDefaultCollectionError("No collections provided, and no default collections set")
        if findFirst and collections is not None:
            wildcard = CollectionWildcard.from_expression(collections)
            if wildcard.patterns:
                raise TypeError(
                    "Collection search patterns not allowed in findFirst search, "
                    "because collections must be in a specific order."
                )

        args = self._convert_common_query_arguments(
            dataId=dataId,
            where=where,
            bind=bind,
            kwargs=kwargs,
            datasets=None,
            collections=collections,
            doomed_by=doomed_by,
            check=check,
        )

        if not args.collections:
            doomed_by.append("No datasets can be found because collection list is empty.")

        missing_dataset_types: list[str] = []
        dataset_types = list(self.queryDatasetTypes(datasetType, missing=missing_dataset_types))
        if missing_dataset_types:
            doomed_by.extend(f"Dataset type {name} is not registered." for name in missing_dataset_types)

        if len(dataset_types) == 0:
            doomed_by.extend(
                [
                    f"No registered dataset type matching {t!r} found, so no matching datasets can "
                    "exist in any collection."
                    for t in ensure_iterable(datasetType)
                ]
            )
            return ChainedDatasetQueryResults([], doomed_by=doomed_by)

        query_results = [
            QueryDriverDatasetRefQueryResults(
                self._butler,
                args,
                dataset_type=dt,
                find_first=findFirst,
                extra_dimensions=dimension_group,
                doomed_by=doomed_by,
                expanded=False,
            )
            for dt in dataset_types
        ]
        if len(query_results) == 1:
            return query_results[0]
        else:
            return ChainedDatasetQueryResults(query_results)

    def queryDataIds(
        self,
        dimensions: DimensionGroup | Iterable[str] | str,
        *,
        dataId: DataId | None = None,
        datasets: Any = None,
        collections: CollectionArgType | None = None,
        where: str = "",
        components: bool = False,
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> DataCoordinateQueryResults:
        if collections is not None and datasets is None:
            raise ArgumentError(f"Cannot pass 'collections' (='{collections}') without 'datasets'.")

        dimensions = self.dimensions.conform(dimensions)
        args = self._convert_common_query_arguments(
            dataId=dataId,
            where=where,
            bind=bind,
            kwargs=kwargs,
            datasets=datasets,
            collections=collections,
            check=check,
        )
        return QueryDriverDataCoordinateQueryResults(
            self._butler, dimensions=dimensions, expanded=False, args=args
        )

    def queryDimensionRecords(
        self,
        element: DimensionElement | str,
        *,
        dataId: DataId | None = None,
        datasets: Any = None,
        collections: CollectionArgType | None = None,
        where: str = "",
        components: bool = False,
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> DimensionRecordQueryResults:
        if not isinstance(element, DimensionElement):
            element = self.dimensions.elements[element]

        args = self._convert_common_query_arguments(
            dataId=dataId,
            where=where,
            bind=bind,
            kwargs=kwargs,
            datasets=datasets,
            collections=collections,
            check=check,
        )

        return QueryDriverDimensionRecordQueryResults(self._butler, element, args)

    def _convert_common_query_arguments(
        self,
        *,
        dataId: DataId | None = None,
        datasets: object | None = None,
        collections: CollectionArgType | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        kwargs: dict[str, int | str],
        doomed_by: list[str] | None = None,
        check: bool = True,
    ) -> CommonQueryArguments:
        dataset_types = self._resolve_dataset_types(datasets)
        if dataset_types and collections is None and not self.defaults.collections:
            raise NoDefaultCollectionError("'collections' must be provided if 'datasets' is provided")
        return CommonQueryArguments(
            dataId=dataId,
            where=where,
            bind=dict(bind) if bind else None,
            kwargs=dict(kwargs),
            dataset_types=dataset_types,
            collections=resolve_collections(self._butler, collections, doomed_by),
            check=check,
        )

    def queryDatasetAssociations(
        self,
        datasetType: str | DatasetType,
        collections: CollectionArgType | None = ...,
        *,
        collectionTypes: Iterable[CollectionType] = CollectionType.all(),
        flattenChains: bool = False,
    ) -> Iterator[DatasetAssociation]:
        if isinstance(datasetType, str):
            datasetType = self.getDatasetType(datasetType)
        with self._butler.query() as query:
            resolved_collections = self.queryCollections(
                collections,
                collectionTypes=collectionTypes,
                flattenChains=True,
            )
            # It's annoyingly difficult to just do the collection query once,
            # since query_info doesn't accept all the expression types that
            # queryCollections does.  But it's all cached anyway.
            collection_info = {
                info.name: info for info in self._butler.collections.query_info(resolved_collections)
            }
            query = query.join_dataset_search(datasetType, resolved_collections)
            result = query.general(
                datasetType.dimensions,
                dataset_fields={datasetType.name: {"dataset_id", "run", "collection", "timespan"}},
                find_first=False,
            )
            yield from DatasetAssociation.from_query_result(result, datasetType, collection_info)

    def _resolve_dataset_types(self, dataset_types: object | None) -> list[str]:
        if dataset_types is None:
            return []

        if dataset_types is ...:
            raise TypeError(
                "'...' not permitted for 'datasets'"
                " -- searching for all dataset types does not constrain the search."
            )

        wildcard = DatasetTypeWildcard.from_expression(dataset_types)
        if wildcard.patterns:
            raise DatasetTypeExpressionError(
                "Dataset type wildcard expressions are not supported in this context."
            )
        else:
            return list(wildcard.values.keys())
