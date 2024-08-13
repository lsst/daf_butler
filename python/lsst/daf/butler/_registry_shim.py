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

__all__ = ("Registry",)

import contextlib
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from ._collection_type import CollectionType
from ._dataset_association import DatasetAssociation
from ._dataset_ref import DatasetId, DatasetIdGenEnum, DatasetRef
from ._dataset_type import DatasetType
from ._timespan import Timespan
from .dimensions import (
    DataCoordinate,
    DataId,
    DimensionElement,
    DimensionGroup,
    DimensionRecord,
    DimensionUniverse,
)
from .registry import Registry
from .registry._collection_summary import CollectionSummary
from .registry._defaults import RegistryDefaults
from .registry.queries import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults

if TYPE_CHECKING:
    from .direct_butler import DirectButler
    from .registry._registry import CollectionArgType
    from .registry.interfaces import ObsCoreTableManager


class RegistryShim(Registry):
    """Implementation of `Registry` interface exposed to clients by `Butler`.

    Parameters
    ----------
    butler : `DirectButler`
        Data butler instance.

    Notes
    -----
    This shim implementation of `Registry` forwards all methods to an actual
    Registry instance which is internal to Butler or to Butler methods. Its
    purpose is to provide a stable interface to many client-visible operations
    while we perform re-structuring of Registry and Butler implementations.
    """

    def __init__(self, butler: DirectButler):
        self._butler = butler
        self._registry = butler._registry

    def isWriteable(self) -> bool:
        # Docstring inherited from a base class.
        return self._registry.isWriteable()

    @property
    def dimensions(self) -> DimensionUniverse:
        # Docstring inherited from a base class.
        return self._registry.dimensions

    @property
    def defaults(self) -> RegistryDefaults:
        # Docstring inherited from a base class.
        return self._registry.defaults

    @defaults.setter
    def defaults(self, value: RegistryDefaults) -> None:
        # Docstring inherited from a base class.
        self._registry.defaults = value

    def refresh(self) -> None:
        # Docstring inherited from a base class.
        self._registry.refresh()

    def refresh_collection_summaries(self) -> None:
        # Docstring inherited from a base class.
        self._registry.refresh_collection_summaries()

    def caching_context(self) -> contextlib.AbstractContextManager[None]:
        # Docstring inherited from a base class.
        return self._butler._caching_context()

    @contextlib.contextmanager
    def transaction(self, *, savepoint: bool = False) -> Iterator[None]:
        # Docstring inherited from a base class.
        with self._registry.transaction(savepoint=savepoint):
            yield

    def resetConnectionPool(self) -> None:
        # Docstring inherited from a base class.
        self._registry.resetConnectionPool()

    def registerCollection(
        self, name: str, type: CollectionType = CollectionType.TAGGED, doc: str | None = None
    ) -> bool:
        # Docstring inherited from a base class.
        return self._registry.registerCollection(name, type, doc)

    def getCollectionType(self, name: str) -> CollectionType:
        # Docstring inherited from a base class.
        return self._registry.getCollectionType(name)

    def registerRun(self, name: str, doc: str | None = None) -> bool:
        # Docstring inherited from a base class.
        return self._registry.registerRun(name, doc)

    def removeCollection(self, name: str) -> None:
        # Docstring inherited from a base class.
        self._registry.removeCollection(name)

    def getCollectionChain(self, parent: str) -> Sequence[str]:
        # Docstring inherited from a base class.
        return self._registry.getCollectionChain(parent)

    def setCollectionChain(self, parent: str, children: Any, *, flatten: bool = False) -> None:
        # Docstring inherited from a base class.
        self._registry.setCollectionChain(parent, children, flatten=flatten)

    def getCollectionParentChains(self, collection: str) -> set[str]:
        # Docstring inherited from a base class.
        return self._registry.getCollectionParentChains(collection)

    def getCollectionDocumentation(self, collection: str) -> str | None:
        # Docstring inherited from a base class.
        return self._registry.getCollectionDocumentation(collection)

    def setCollectionDocumentation(self, collection: str, doc: str | None) -> None:
        # Docstring inherited from a base class.
        self._registry.setCollectionDocumentation(collection, doc)

    def getCollectionSummary(self, collection: str) -> CollectionSummary:
        # Docstring inherited from a base class.
        return self._registry.getCollectionSummary(collection)

    def registerDatasetType(self, datasetType: DatasetType) -> bool:
        # Docstring inherited from a base class.
        return self._registry.registerDatasetType(datasetType)

    def removeDatasetType(self, name: str | tuple[str, ...]) -> None:
        # Docstring inherited from a base class.
        self._registry.removeDatasetType(name)

    def getDatasetType(self, name: str) -> DatasetType:
        # Docstring inherited from a base class.
        return self._registry.getDatasetType(name)

    def supportsIdGenerationMode(self, mode: DatasetIdGenEnum) -> bool:
        # Docstring inherited from a base class.
        return self._registry.supportsIdGenerationMode(mode)

    def findDataset(
        self,
        datasetType: DatasetType | str,
        dataId: DataId | None = None,
        *,
        collections: CollectionArgType | None = None,
        timespan: Timespan | None = None,
        **kwargs: Any,
    ) -> DatasetRef | None:
        # Docstring inherited from a base class.
        return self._registry.findDataset(
            datasetType, dataId, collections=collections, timespan=timespan, **kwargs
        )

    def insertDatasets(
        self,
        datasetType: DatasetType | str,
        dataIds: Iterable[DataId],
        run: str | None = None,
        expand: bool = True,
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
    ) -> list[DatasetRef]:
        # Docstring inherited from a base class.
        return self._registry.insertDatasets(datasetType, dataIds, run, expand, idGenerationMode)

    def _importDatasets(self, datasets: Iterable[DatasetRef], expand: bool = True) -> list[DatasetRef]:
        # Docstring inherited from a base class.
        return self._registry._importDatasets(datasets, expand)

    def getDataset(self, id: DatasetId) -> DatasetRef | None:
        # Docstring inherited from a base class.
        return self._registry.getDataset(id)

    def removeDatasets(self, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited from a base class.
        self._registry.removeDatasets(refs)

    def associate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited from a base class.
        self._registry.associate(collection, refs)

    def disassociate(self, collection: str, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited from a base class.
        self._registry.disassociate(collection, refs)

    def certify(self, collection: str, refs: Iterable[DatasetRef], timespan: Timespan) -> None:
        # Docstring inherited from a base class.
        self._registry.certify(collection, refs, timespan)

    def decertify(
        self,
        collection: str,
        datasetType: str | DatasetType,
        timespan: Timespan,
        *,
        dataIds: Iterable[DataId] | None = None,
    ) -> None:
        # Docstring inherited from a base class.
        self._registry.decertify(collection, datasetType, timespan, dataIds=dataIds)

    def getDatasetLocations(self, ref: DatasetRef) -> Iterable[str]:
        # Docstring inherited from a base class.
        return self._registry.getDatasetLocations(ref)

    def expandDataId(
        self,
        dataId: DataId | None = None,
        *,
        dimensions: Iterable[str] | DimensionGroup | None = None,
        records: Mapping[str, DimensionRecord | None] | None = None,
        withDefaults: bool = True,
        **kwargs: Any,
    ) -> DataCoordinate:
        # Docstring inherited from a base class.
        return self._registry.expandDataId(
            dataId, dimensions=dimensions, records=records, withDefaults=withDefaults, **kwargs
        )

    def insertDimensionData(
        self,
        element: DimensionElement | str,
        *data: Mapping[str, Any] | DimensionRecord,
        conform: bool = True,
        replace: bool = False,
        skip_existing: bool = False,
    ) -> None:
        # Docstring inherited from a base class.
        self._registry.insertDimensionData(
            element, *data, conform=conform, replace=replace, skip_existing=skip_existing
        )

    def syncDimensionData(
        self,
        element: DimensionElement | str,
        row: Mapping[str, Any] | DimensionRecord,
        conform: bool = True,
        update: bool = False,
    ) -> bool | dict[str, Any]:
        # Docstring inherited from a base class.
        return self._registry.syncDimensionData(element, row, conform, update)

    def queryDatasetTypes(
        self,
        expression: Any = ...,
        *,
        missing: list[str] | None = None,
    ) -> Iterable[DatasetType]:
        # Docstring inherited from a base class.
        return self._registry.queryDatasetTypes(expression, missing=missing)

    def queryCollections(
        self,
        expression: Any = ...,
        datasetType: DatasetType | None = None,
        collectionTypes: Iterable[CollectionType] | CollectionType = CollectionType.all(),
        flattenChains: bool = False,
        includeChains: bool | None = None,
    ) -> Sequence[str]:
        # Docstring inherited from a base class.
        return self._registry.queryCollections(
            expression, datasetType, collectionTypes, flattenChains, includeChains
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
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> DatasetQueryResults:
        # Docstring inherited from a base class.
        return self._registry.queryDatasets(
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
        dimensions: DimensionGroup | Iterable[str] | str,
        *,
        dataId: DataId | None = None,
        datasets: Any = None,
        collections: CollectionArgType | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> DataCoordinateQueryResults:
        # Docstring inherited from a base class.
        return self._registry.queryDataIds(
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
        bind: Mapping[str, Any] | None = None,
        check: bool = True,
        **kwargs: Any,
    ) -> DimensionRecordQueryResults:
        # Docstring inherited from a base class.
        return self._registry.queryDimensionRecords(
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
        # Docstring inherited from a base class.
        return self._registry.queryDatasetAssociations(
            datasetType,
            collections,
            collectionTypes=collectionTypes,
            flattenChains=flattenChains,
        )

    @property
    def obsCoreTableManager(self) -> ObsCoreTableManager | None:
        # Docstring inherited from a base class.
        return self._registry.obsCoreTableManager
