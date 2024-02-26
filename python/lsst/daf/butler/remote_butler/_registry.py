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
from ..registry import (
    CollectionArgType,
    CollectionSummary,
    CollectionType,
    DatasetTypeError,
    Registry,
    RegistryDefaults,
)
from ..registry.queries import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults
from ..remote_butler import RemoteButler
from ._collection_args import convert_collection_arg_to_glob_string_list


class RemoteButlerRegistry(Registry):
    """Implementation of the `Registry` interface for `RemoteButler.

    Parameters
    ----------
    butler : `RemoteButler`
        Butler instance to which this registry delegates operations.
    """

    def __init__(self, butler: RemoteButler):
        self._butler = butler

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    def registerRun(self, name: str, doc: str | None = None) -> bool:
        raise NotImplementedError()

    def removeCollection(self, name: str) -> None:
        raise NotImplementedError()

    def getCollectionChain(self, parent: str) -> Sequence[str]:
        raise NotImplementedError()

    def setCollectionChain(self, parent: str, children: Any, *, flatten: bool = False) -> None:
        raise NotImplementedError()

    def getCollectionParentChains(self, collection: str) -> set[str]:
        raise NotImplementedError()

    def getCollectionDocumentation(self, collection: str) -> str | None:
        raise NotImplementedError()

    def setCollectionDocumentation(self, collection: str, doc: str | None) -> None:
        raise NotImplementedError()

    def getCollectionSummary(self, collection: str) -> CollectionSummary:
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    def queryCollections(
        self,
        expression: Any = ...,
        datasetType: DatasetType | None = None,
        collectionTypes: Iterable[CollectionType] | CollectionType = CollectionType.all(),
        flattenChains: bool = False,
        includeChains: bool | None = None,
    ) -> Sequence[str]:
        raise NotImplementedError()

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
        raise NotImplementedError()

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


def _is_component_dataset_type(dataset_type: DatasetType | str) -> bool:
    """Return true if the given dataset type refers to a component."""
    if isinstance(dataset_type, DatasetType):
        return dataset_type.component() is not None
    elif isinstance(dataset_type, str):
        parent, component = DatasetType.splitDatasetTypeName(dataset_type)
        return component is not None
    else:
        raise TypeError(f"Expected DatasetType or str, got {dataset_type!r}")
