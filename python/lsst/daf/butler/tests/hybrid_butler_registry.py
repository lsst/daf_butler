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
from ..registry import CollectionArgType, CollectionSummary, CollectionType, Registry, RegistryDefaults
from ..registry.queries import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults
from ..registry.sql_registry import SqlRegistry


class HybridButlerRegistry(Registry):
    """A `Registry` that delegates methods to internal `RemoteButlerRegistry`
    and `SqlRegistry` instances.  Intended to allow testing of `RemoteButler`
    before its implementation is complete, by delegating unsupported methods to
    the direct `SqlRegistry`.

    Parameters
    ----------
    direct : `SqlRegistry`
        DirectButler SqlRegistry used to provide methods not yet implemented by
        RemoteButlerRegistry.

    remote : `Registry`
        The RemoteButler Registry implementation we are intending to test.
    """

    def __init__(self, direct: SqlRegistry, remote: Registry):
        self._direct = direct
        self._remote = remote

    def isWriteable(self) -> bool:
        return self._remote.isWriteable()

    @property
    def dimensions(self) -> DimensionUniverse:
        return self._remote.dimensions

    @property
    def defaults(self) -> RegistryDefaults:
        return self._remote.defaults

    @defaults.setter
    def defaults(self, value: RegistryDefaults) -> None:
        raise NotImplementedError()

    def refresh(self) -> None:
        self._direct.refresh()

    def caching_context(self) -> contextlib.AbstractContextManager[None]:
        return self._direct.caching_context()

    @contextlib.contextmanager
    def transaction(self, *, savepoint: bool = False) -> Iterator[None]:
        # RemoteButler doesn't support transactions, and if the direct registry
        # enters one its changes are invisible to the remote side.
        raise NotImplementedError()

    def registerCollection(
        self, name: str, type: CollectionType = CollectionType.TAGGED, doc: str | None = None
    ) -> bool:
        return self._direct.registerCollection(name, type, doc)

    def getCollectionType(self, name: str) -> CollectionType:
        return self._direct.getCollectionType(name)

    def registerRun(self, name: str, doc: str | None = None) -> bool:
        return self._direct.registerRun(name, doc)

    def removeCollection(self, name: str) -> None:
        return self._direct.removeCollection(name)

    def getCollectionChain(self, parent: str) -> Sequence[str]:
        return self._direct.getCollectionChain(parent)

    def setCollectionChain(self, parent: str, children: Any, *, flatten: bool = False) -> None:
        return self._direct.setCollectionChain(parent, children, flatten=flatten)

    def getCollectionParentChains(self, collection: str) -> set[str]:
        return self._direct.getCollectionParentChains(collection)

    def getCollectionDocumentation(self, collection: str) -> str | None:
        return self._direct.getCollectionDocumentation(collection)

    def setCollectionDocumentation(self, collection: str, doc: str | None) -> None:
        return self._direct.setCollectionDocumentation(collection, doc)

    def getCollectionSummary(self, collection: str) -> CollectionSummary:
        return self._direct.getCollectionSummary(collection)

    def registerDatasetType(self, datasetType: DatasetType) -> bool:
        return self._direct.registerDatasetType(datasetType)

    def removeDatasetType(self, name: str | tuple[str, ...]) -> None:
        return self._direct.removeDatasetType(name)

    def getDatasetType(self, name: str) -> DatasetType:
        return self._remote.getDatasetType(name)

    def supportsIdGenerationMode(self, mode: DatasetIdGenEnum) -> bool:
        return self._direct.supportsIdGenerationMode(mode)

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
        return self._remote.findDataset(
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
        return self._direct.insertDatasets(datasetType, dataIds, run, expand, idGenerationMode)

    def _importDatasets(
        self,
        datasets: Iterable[DatasetRef],
        expand: bool = True,
    ) -> list[DatasetRef]:
        return self._direct._importDatasets(datasets, expand)

    def getDataset(self, id: DatasetId) -> DatasetRef | None:
        return self._remote.getDataset(id)

    def removeDatasets(self, refs: Iterable[DatasetRef]) -> None:
        return self._direct.removeDatasets(refs)

    def associate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        return self._direct.associate(collection, refs)

    def disassociate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        return self._direct.disassociate(collection, refs)

    def certify(self, collection: str, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        return self._direct.certify(collection, refs, timespan)

    def decertify(
        self,
        collection: str,
        datasetType: str | DatasetType,
        timespan: Timespan,
        *,
        dataIds: Iterable[DataId] | None = None,
    ) -> None:
        return self._direct.decertify(collection, datasetType, timespan, dataIds=dataIds)

    def getDatasetLocations(self, ref: DatasetRef) -> Iterable[str]:
        return self._direct.getDatasetLocations(ref)

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
        return self._direct.expandDataId(
            dataId, dimensions=dimensions, graph=graph, records=records, withDefaults=withDefaults, **kwargs
        )

    def insertDimensionData(
        self,
        element: DimensionElement | str,
        *data: Mapping[str, Any] | DimensionRecord,
        conform: bool = True,
        replace: bool = False,
        skip_existing: bool = False,
    ) -> None:
        return self._direct.insertDimensionData(
            element, *data, conform=conform, replace=replace, skip_existing=skip_existing
        )

    def syncDimensionData(
        self,
        element: DimensionElement | str,
        row: Mapping[str, Any] | DimensionRecord,
        conform: bool = True,
        update: bool = False,
    ) -> bool | dict[str, Any]:
        return self._direct.syncDimensionData(element, row, conform, update)

    def queryDatasetTypes(
        self,
        expression: Any = ...,
        *,
        components: bool = False,
        missing: list[str] | None = None,
    ) -> Iterable[DatasetType]:
        return self._direct.queryDatasetTypes(expression, missing=missing)

    def queryCollections(
        self,
        expression: Any = ...,
        datasetType: DatasetType | None = None,
        collectionTypes: Iterable[CollectionType] | CollectionType = CollectionType.all(),
        flattenChains: bool = False,
        includeChains: bool | None = None,
    ) -> Sequence[str]:
        return self._direct.queryCollections(
            expression, datasetType, collectionTypes, flattenChains, includeChains
        )

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
        return self._direct.queryDatasets(
            datasetType,
            collections=collections,
            dimensions=dimensions,
            dataId=dataId,
            where=where,
            findFirst=findFirst,
            bind=bind,
            check=check,
            **kwargs,
        )

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
        return self._direct.queryDataIds(
            dimensions,
            dataId=dataId,
            datasets=datasets,
            collections=collections,
            where=where,
            bind=bind,
            check=check,
            **kwargs,
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
        return self._direct.queryDimensionRecords(
            element,
            dataId=dataId,
            datasets=datasets,
            collections=collections,
            where=where,
            bind=bind,
            check=check,
            **kwargs,
        )

    def queryDatasetAssociations(
        self,
        datasetType: str | DatasetType,
        collections: CollectionArgType | None = ...,
        *,
        collectionTypes: Iterable[CollectionType] = CollectionType.all(),
        flattenChains: bool = False,
    ) -> Iterator[DatasetAssociation]:
        return self._direct.queryDatasetAssociations(
            datasetType, collections, collectionTypes=collectionTypes, flattenChains=flattenChains
        )

    @property
    def storageClasses(self) -> StorageClassFactory:
        return self._remote.storageClasses

    @storageClasses.setter
    def storageClasses(self, value: StorageClassFactory) -> None:
        raise NotImplementedError()
