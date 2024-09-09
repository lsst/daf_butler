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

import contextlib
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

from lsst.daf.butler import Butler
from lsst.utils.iteration import ensure_iterable

from .._collection_type import CollectionType
from .._dataset_association import DatasetAssociation
from .._dataset_ref import DatasetId, DatasetIdGenEnum, DatasetRef
from .._dataset_type import DatasetType
from .._storage_class import StorageClassFactory
from .._timespan import Timespan
from ..dimensions import (
    DataCoordinate,
    DataId,
    DimensionElement,
    DimensionGroup,
    DimensionRecord,
    DimensionUniverse,
)
from ..registry import (
    CollectionArgType,
    CollectionSummary,
    CollectionTypeError,
    DatasetTypeError,
    DatasetTypeExpressionError,
    NoDefaultCollectionError,
    Registry,
    RegistryDefaults,
)
from ..registry._exceptions import ArgumentError
from ..registry.queries import (
    ChainedDatasetQueryResults,
    DataCoordinateQueryResults,
    DatasetQueryResults,
    DimensionRecordQueryResults,
)
from ..registry.wildcards import CollectionWildcard, DatasetTypeWildcard
from ._collection_args import (
    convert_collection_arg_to_glob_string_list,
    convert_dataset_type_arg_to_glob_string_list,
)
from ._defaults import DefaultsHolder
from ._http_connection import RemoteButlerHttpConnection, parse_model
from .registry._query_common import CommonQueryArguments
from .registry._query_data_coordinates import QueryDriverDataCoordinateQueryResults
from .registry._query_datasets import QueryDriverDatasetRefQueryResults
from .registry._query_dimension_records import QueryDriverDimensionRecordQueryResults
from .server_models import (
    ExpandDataIdRequestModel,
    ExpandDataIdResponseModel,
    GetCollectionSummaryResponseModel,
    QueryDatasetTypesRequestModel,
    QueryDatasetTypesResponseModel,
)


class RemoteButlerRegistry(Registry):
    """Implementation of the `Registry` interface for `RemoteButler.

    Parameters
    ----------
    butler : `Butler`
        Butler instance to which this registry delegates operations.
    defaults : `DefaultHolder`
        Reference to object containing default collections and data ID.
    connection : `RemoteButlerHttpConnection`
        HTTP connection to Butler server for looking up data.
    """

    def __init__(self, butler: Butler, defaults: DefaultsHolder, connection: RemoteButlerHttpConnection):
        self._butler = butler
        self._connection = connection
        self._defaults = defaults

    def isWriteable(self) -> bool:
        return self._butler.isWriteable()

    @property
    def dimensions(self) -> DimensionUniverse:
        return self._butler.dimensions

    @property
    def defaults(self) -> RegistryDefaults:
        return self._defaults.get()

    @defaults.setter
    def defaults(self, value: RegistryDefaults) -> None:
        value.finish(self)
        self._defaults.set(value)

    def refresh(self) -> None:
        # In theory the server should manage all necessary invalidation of
        # state.
        pass

    def refresh_collection_summaries(self) -> None:
        # Docstring inherited from a base class.
        raise NotImplementedError()

    def caching_context(self) -> contextlib.AbstractContextManager[None]:
        raise NotImplementedError()

    @contextlib.contextmanager
    def transaction(self, *, savepoint: bool = False) -> Iterator[None]:
        # RemoteButler will never support transactions
        raise NotImplementedError()

    def registerCollection(
        self, name: str, type: CollectionType = CollectionType.TAGGED, doc: str | None = None
    ) -> bool:
        raise NotImplementedError()

    def getCollectionType(self, name: str) -> CollectionType:
        return self._butler.collections.get_info(name).type

    def registerRun(self, name: str, doc: str | None = None) -> bool:
        raise NotImplementedError()

    def removeCollection(self, name: str) -> None:
        raise NotImplementedError()

    def getCollectionChain(self, parent: str) -> Sequence[str]:
        info = self._butler.collections.get_info(parent)
        if info.type is not CollectionType.CHAINED:
            raise CollectionTypeError(f"Collection '{parent}' has type {info.type.name}, not CHAINED.")
        return info.children

    def setCollectionChain(self, parent: str, children: Any, *, flatten: bool = False) -> None:
        raise NotImplementedError()

    def getCollectionParentChains(self, collection: str) -> set[str]:
        info = self._butler.collections.get_info(collection, include_parents=True)
        assert info.parents is not None, "Requested list of parents from server, but it did not send them."
        return set(info.parents)

    def getCollectionDocumentation(self, collection: str) -> str | None:
        doc = self._butler.collections.get_info(collection).doc
        if not doc:
            return None
        return doc

    def setCollectionDocumentation(self, collection: str, doc: str | None) -> None:
        raise NotImplementedError()

    def getCollectionSummary(self, collection: str) -> CollectionSummary:
        response = self._connection.get("collection_summary", {"name": collection})
        parsed = parse_model(response, GetCollectionSummaryResponseModel)
        return CollectionSummary.from_simple(parsed.summary, self.dimensions)

    def registerDatasetType(self, datasetType: DatasetType) -> bool:
        raise NotImplementedError()

    def removeDatasetType(self, name: str | tuple[str, ...]) -> None:
        raise NotImplementedError()

    def getDatasetType(self, name: str) -> DatasetType:
        return self._butler.get_dataset_type(name)

    def supportsIdGenerationMode(self, mode: DatasetIdGenEnum) -> bool:
        raise NotImplementedError()

    def findDataset(
        self,
        datasetType: DatasetType | str,
        dataId: DataId | None = None,
        *,
        collections: CollectionArgType | None = None,
        timespan: Timespan | None = None,
        datastore_records: bool = False,
        **kwargs: Any,
    ) -> DatasetRef | None:
        # Components are supported by Butler.find_dataset, but are required to
        # raise an exception in registry.findDataset (see
        # RegistryTests.testComponentLookups).  Apparently the registry version
        # used to support components, but at some point a decision was made to
        # draw a hard architectural boundary between registry and datastore so
        # that registry is no longer allowed to know about components.
        if _is_component_dataset_type(datasetType):
            raise DatasetTypeError(
                "Component dataset types are not supported;"
                " use DatasetRef or DatasetType methods to obtain components from parents instead."
            )

        if collections is not None:
            collections = convert_collection_arg_to_glob_string_list(collections)

        return self._butler.find_dataset(
            datasetType,
            dataId,
            collections=collections,
            timespan=timespan,
            datastore_records=datastore_records,
            **kwargs,
        )

    def insertDatasets(
        self,
        datasetType: DatasetType | str,
        dataIds: Iterable[DataId],
        run: str | None = None,
        expand: bool = True,
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
    ) -> list[DatasetRef]:
        raise NotImplementedError()

    def _importDatasets(
        self,
        datasets: Iterable[DatasetRef],
        expand: bool = True,
    ) -> list[DatasetRef]:
        raise NotImplementedError()

    def getDataset(self, id: DatasetId) -> DatasetRef | None:
        return self._butler.get_dataset(id)

    def removeDatasets(self, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError()

    def associate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError()

    def disassociate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError()

    def certify(self, collection: str, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        raise NotImplementedError()

    def decertify(
        self,
        collection: str,
        datasetType: str | DatasetType,
        timespan: Timespan,
        *,
        dataIds: Iterable[DataId] | None = None,
    ) -> None:
        raise NotImplementedError()

    def getDatasetLocations(self, ref: DatasetRef) -> Iterable[str]:
        raise NotImplementedError()

    def expandDataId(
        self,
        dataId: DataId | None = None,
        *,
        dimensions: Iterable[str] | DimensionGroup | None = None,
        records: Mapping[str, DimensionRecord | None] | None = None,
        withDefaults: bool = True,
        **kwargs: Any,
    ) -> DataCoordinate:
        standardized = DataCoordinate.standardize(
            dataId,
            dimensions=dimensions,
            universe=self.dimensions,
            defaults=self.defaults.dataId if withDefaults else None,
            **kwargs,
        )
        if standardized.hasRecords():
            return standardized

        request = ExpandDataIdRequestModel(data_id=standardized.to_simple().dataId)
        response = self._connection.post("expand_data_id", request)
        model = parse_model(response, ExpandDataIdResponseModel)
        return DataCoordinate.from_simple(model.data_coordinate, self.dimensions)

    def insertDimensionData(
        self,
        element: DimensionElement | str,
        *data: Mapping[str, Any] | DimensionRecord,
        conform: bool = True,
        replace: bool = False,
        skip_existing: bool = False,
    ) -> None:
        raise NotImplementedError()

    def syncDimensionData(
        self,
        element: DimensionElement | str,
        row: Mapping[str, Any] | DimensionRecord,
        conform: bool = True,
        update: bool = False,
    ) -> bool | dict[str, Any]:
        raise NotImplementedError()

    def queryDatasetTypes(
        self,
        expression: Any = ...,
        *,
        components: bool = False,
        missing: list[str] | None = None,
    ) -> Iterable[DatasetType]:
        query = convert_dataset_type_arg_to_glob_string_list(expression)
        request = QueryDatasetTypesRequestModel(search=query.search)
        response = self._connection.post("query_dataset_types", request)
        model = parse_model(response, QueryDatasetTypesResponseModel)
        if missing is not None:
            missing.extend(model.missing)

        result = []
        for dt in model.dataset_types:
            if dt.name in query.explicit_dataset_types:
                # Users are permitted to pass in already-existing DatasetType
                # instances, and we are supposed to preserve their overridden
                # storage class etc.
                result.append(query.explicit_dataset_types[dt.name])
            else:
                result.append(DatasetType.from_simple(dt, self.dimensions))

        return result

    def queryCollections(
        self,
        expression: Any = ...,
        datasetType: DatasetType | None = None,
        collectionTypes: Iterable[CollectionType] | CollectionType = CollectionType.all(),
        flattenChains: bool = False,
        includeChains: bool | None = None,
    ) -> Sequence[str]:
        return self._butler.collections.query(
            expression,
            collection_types=set(ensure_iterable(collectionTypes)),
            flatten_chains=flattenChains,
            include_chains=includeChains,
        )

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
                self._butler.query,
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
            dataId=dataId, where=where, bind=bind, kwargs=kwargs, datasets=datasets, collections=collections
        )
        return QueryDriverDataCoordinateQueryResults(
            self._butler.query, dimensions=dimensions, expanded=False, args=args
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
            dataId=dataId, where=where, bind=bind, kwargs=kwargs, datasets=datasets, collections=collections
        )

        return QueryDriverDimensionRecordQueryResults(self._butler.query, element, args)

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
            collections=self._resolve_collections(collections, doomed_by),
        )

    def queryDatasetAssociations(
        self,
        datasetType: str | DatasetType,
        collections: CollectionArgType | None = ...,
        *,
        collectionTypes: Iterable[CollectionType] = CollectionType.all(),
        flattenChains: bool = False,
    ) -> Iterator[DatasetAssociation]:
        # queryCollections only accepts DatasetType.
        if isinstance(datasetType, str):
            datasetType = self.getDatasetType(datasetType)
        resolved_collections = self.queryCollections(
            collections, datasetType=datasetType, collectionTypes=collectionTypes, flattenChains=flattenChains
        )
        with self._butler.query() as query:
            query = query.join_dataset_search(datasetType, resolved_collections)
            result = query.general(
                datasetType.dimensions,
                dataset_fields={datasetType.name: {"dataset_id", "run", "collection", "timespan"}},
                find_first=False,
            )
            yield from DatasetAssociation.from_query_result(result, datasetType)

    @property
    def storageClasses(self) -> StorageClassFactory:
        return self._butler.storageClasses

    @storageClasses.setter
    def storageClasses(self, value: StorageClassFactory) -> None:
        raise NotImplementedError()

    def _resolve_collections(
        self, collections: CollectionArgType | None, doomed_by: list[str] | None = None
    ) -> list[str] | None:
        if collections is None:
            return list(self.defaults.collections)

        wildcard = CollectionWildcard.from_expression(collections)
        if wildcard.patterns:
            result = list(self.queryCollections(collections))
            if not result and doomed_by is not None:
                doomed_by.append(f"No collections found matching expression {wildcard}")
            return result
        else:
            return list(wildcard.strings)

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


def _is_component_dataset_type(dataset_type: DatasetType | str) -> bool:
    """Return true if the given dataset type refers to a component."""
    if isinstance(dataset_type, DatasetType):
        return dataset_type.component() is not None
    elif isinstance(dataset_type, str):
        parent, component = DatasetType.splitDatasetTypeName(dataset_type)
        return component is not None
    else:
        raise TypeError(f"Expected DatasetType or str, got {dataset_type!r}")
