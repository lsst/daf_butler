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

from lsst.utils.iteration import ensure_iterable

from .._dataset_association import DatasetAssociation
from .._dataset_ref import DatasetId, DatasetIdGenEnum, DatasetRef
from .._dataset_type import DatasetType
from .._named import NameLookupMapping
from .._storage_class import StorageClassFactory
from .._timespan import Timespan
from ..dimensions import (
    DataCoordinate,
    DataId,
    Dimension,
    DimensionElement,
    DimensionGraph,
    DimensionGroup,
    DimensionRecord,
    DimensionUniverse,
)
from ..registry import (
    CollectionArgType,
    CollectionSummary,
    CollectionType,
    CollectionTypeError,
    DatasetTypeError,
    DatasetTypeExpressionError,
    NoDefaultCollectionError,
    Registry,
    RegistryDefaults,
)
from ..registry.queries import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults
from ..registry.wildcards import CollectionWildcard, DatasetTypeWildcard
from ..remote_butler import RemoteButler
from ._collection_args import (
    convert_collection_arg_to_glob_string_list,
    convert_dataset_type_arg_to_glob_string_list,
)
from ._http_connection import RemoteButlerHttpConnection, parse_model
from .registry._query_dimension_records import (
    DimensionRecordsQueryArguments,
    QueryDriverDimensionRecordQueryResults,
)
from .server_models import (
    ExpandDataIdRequestModel,
    ExpandDataIdResponseModel,
    GetCollectionInfoResponseModel,
    GetCollectionSummaryResponseModel,
    QueryCollectionsRequestModel,
    QueryCollectionsResponseModel,
    QueryDatasetTypesRequestModel,
    QueryDatasetTypesResponseModel,
)


class RemoteButlerRegistry(Registry):
    """Implementation of the `Registry` interface for `RemoteButler.

    Parameters
    ----------
    butler : `RemoteButler`
        Butler instance to which this registry delegates operations.
    connection : `RemoteButlerHttpConnection`
        HTTP connection to Butler server for looking up data.
    """

    def __init__(self, butler: RemoteButler, connection: RemoteButlerHttpConnection):
        self._butler = butler
        self._connection = connection

    def isWriteable(self) -> bool:
        return self._butler.isWriteable()

    @property
    def dimensions(self) -> DimensionUniverse:
        return self._butler.dimensions

    @property
    def defaults(self) -> RegistryDefaults:
        return self._butler._registry_defaults

    @defaults.setter
    def defaults(self, value: RegistryDefaults) -> None:
        value.finish(self)
        self._butler._registry_defaults = value

    def refresh(self) -> None:
        # In theory the server should manage all necessary invalidation of
        # state.
        pass

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
        return self._get_collection_info(name).type

    def registerRun(self, name: str, doc: str | None = None) -> bool:
        raise NotImplementedError()

    def removeCollection(self, name: str) -> None:
        raise NotImplementedError()

    def getCollectionChain(self, parent: str) -> Sequence[str]:
        info = self._get_collection_info(parent)
        if info.type is not CollectionType.CHAINED:
            raise CollectionTypeError(f"Collection '{parent}' has type {info.type.name}, not CHAINED.")
        return info.children

    def setCollectionChain(self, parent: str, children: Any, *, flatten: bool = False) -> None:
        raise NotImplementedError()

    def getCollectionParentChains(self, collection: str) -> set[str]:
        info = self._get_collection_info(collection, include_parents=True)
        assert info.parents is not None, "Requested list of parents from server, but it did not send them."
        return info.parents

    def getCollectionDocumentation(self, collection: str) -> str | None:
        info = self._get_collection_info(collection, include_doc=True)
        return info.doc

    def setCollectionDocumentation(self, collection: str, doc: str | None) -> None:
        raise NotImplementedError()

    def _get_collection_info(
        self, collection_name: str, include_doc: bool = False, include_parents: bool = False
    ) -> GetCollectionInfoResponseModel:
        response = self._connection.get(
            "collection_info",
            {"name": collection_name, "include_doc": include_doc, "include_parents": include_parents},
        )
        return parse_model(response, GetCollectionInfoResponseModel)

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
        dimensions: Iterable[str] | DimensionGroup | DimensionGraph | None = None,
        graph: DimensionGraph | None = None,
        records: NameLookupMapping[DimensionElement, DimensionRecord | None] | None = None,
        withDefaults: bool = True,
        **kwargs: Any,
    ) -> DataCoordinate:
        standardized = DataCoordinate.standardize(
            dataId,
            graph=graph,
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
        if includeChains is None:
            includeChains = not flattenChains
        query = QueryCollectionsRequestModel(
            search=convert_collection_arg_to_glob_string_list(expression),
            collection_types=list(ensure_iterable(collectionTypes)),
            flatten_chains=flattenChains,
            include_chains=includeChains,
        )

        response = self._connection.post("query_collections", query)
        return parse_model(response, QueryCollectionsResponseModel).collections

    def queryDatasets(
        self,
        datasetType: Any,
        *,
        collections: CollectionArgType | None = None,
        dimensions: Iterable[Dimension | str] | None = None,
        dataId: DataId | None = None,
        where: str = "",
        findFirst: bool = False,
        components: bool = False,
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> DatasetQueryResults:
        raise NotImplementedError()

    def queryDataIds(
        self,
        # TODO: Drop `Dimension` objects on DM-41326.
        dimensions: DimensionGroup | Iterable[Dimension | str] | Dimension | str,
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
        raise NotImplementedError()

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

        dataset_types = self._resolve_dataset_types(datasets)
        if dataset_types and collections is None and not self.defaults.collections:
            raise NoDefaultCollectionError("'collections' must be provided if 'datasets' is provided")
        args = DimensionRecordsQueryArguments(
            element=element,
            dataId=dataId,
            where=where,
            bind=dict(bind) if bind else None,
            kwargs=dict(kwargs),
            dataset_types=dataset_types,
            collections=self._resolve_collections(collections),
        )

        return QueryDriverDimensionRecordQueryResults(self._butler._query, args)

    def queryDatasetAssociations(
        self,
        datasetType: str | DatasetType,
        collections: CollectionArgType | None = ...,
        *,
        collectionTypes: Iterable[CollectionType] = CollectionType.all(),
        flattenChains: bool = False,
    ) -> Iterator[DatasetAssociation]:
        raise NotImplementedError()

    @property
    def storageClasses(self) -> StorageClassFactory:
        return self._butler.storageClasses

    @storageClasses.setter
    def storageClasses(self, value: StorageClassFactory) -> None:
        raise NotImplementedError()

    def _resolve_collections(self, collections: CollectionArgType | None) -> list[str] | None:
        if collections is None:
            return list(self.defaults.collections)

        wildcard = CollectionWildcard.from_expression(collections)
        if wildcard.patterns:
            return list(self.queryCollections(collections))
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
