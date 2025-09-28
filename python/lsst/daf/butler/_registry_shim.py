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

__all__ = ("RegistryShim",)

import contextlib
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from ._collection_type import CollectionType
from ._dataset_ref import DatasetId, DatasetIdGenEnum, DatasetRef
from ._dataset_type import DatasetType
from ._exceptions import CalibrationLookupError
from ._storage_class import StorageClassFactory
from ._timespan import Timespan
from .dimensions import (
    DataCoordinate,
    DataId,
    DimensionElement,
    DimensionGroup,
    DimensionRecord,
    DimensionUniverse,
)
from .registry._collection_summary import CollectionSummary
from .registry._defaults import RegistryDefaults
from .registry._exceptions import NoDefaultCollectionError
from .registry._registry_base import RegistryBase
from .registry.queries._query_common import resolve_collections

if TYPE_CHECKING:
    from .direct_butler import DirectButler
    from .registry._registry import CollectionArgType
    from .registry.interfaces import ObsCoreTableManager


class RegistryShim(RegistryBase):
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
        super().__init__(butler)
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
        datastore_records: bool = False,
        **kwargs: Any,
    ) -> DatasetRef | None:
        # Docstring inherited from a base class.
        if not isinstance(datasetType, DatasetType):
            datasetType = self.getDatasetType(datasetType)

        dataId = DataCoordinate.standardize(
            dataId,
            dimensions=datasetType.dimensions,
            universe=self.dimensions,
            defaults=self.defaults.dataId,
            **kwargs,
        )

        with self._butler.query() as query:
            resolved_collections = resolve_collections(self._butler, collections)
            if not resolved_collections:
                if collections is None:
                    raise NoDefaultCollectionError("No collections provided, and no default collections set")
                else:
                    return None

            if datasetType.isCalibration() and timespan is None:
                # Filter out calibration collections, because with no timespan
                # we have no way of selecting a dataset from them.
                collection_info = self._butler.collections.query_info(
                    resolved_collections, flatten_chains=True
                )
                resolved_collections = [
                    info.name for info in collection_info if info.type != CollectionType.CALIBRATION
                ]
                if not resolved_collections:
                    return None

            result = query.datasets(datasetType, resolved_collections, find_first=True).limit(2)
            dataset_type_name = result.dataset_type.name
            # Search only on the 'required' dimensions for the dataset type.
            # Any extra values provided by the user are ignored.
            minimal_data_id = DataCoordinate.standardize(
                dataId.subset(datasetType.dimensions.required).required, universe=self.dimensions
            )
            result = result.where(minimal_data_id)
            if (
                datasetType.isCalibration()
                and timespan is not None
                and (timespan.begin is not None or timespan.end is not None)
            ):
                timespan_column = query.expression_factory[dataset_type_name].timespan
                result = result.where(timespan_column.overlaps(timespan))

            datasets = list(result)
            if len(datasets) == 1:
                ref = datasets[0]
                if dataId.hasRecords():
                    ref = ref.expanded(dataId)
                # Propagate storage class from user-provided DatasetType, which
                # may not match the definition in the database.
                ref = ref.overrideStorageClass(datasetType.storageClass_name)
                if datastore_records:
                    ref = self._registry.get_datastore_records(ref)
                return ref
            elif len(datasets) == 0:
                return None
            else:
                raise CalibrationLookupError(
                    f"Ambiguous calibration lookup for {datasetType} with timespan {timespan}"
                    f" in collections {resolved_collections}."
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

    def _importDatasets(
        self, datasets: Iterable[DatasetRef], expand: bool = True, assume_new: bool = False
    ) -> list[DatasetRef]:
        # Docstring inherited from a base class.
        return self._registry._importDatasets(datasets, expand, assume_new)

    def getDataset(self, id: DatasetId) -> DatasetRef | None:
        # Docstring inherited from a base class.
        return self._registry.getDataset(id)

    def _fetch_run_dataset_ids(self, run: str) -> list[DatasetId]:
        # Docstring inherited.
        return self._registry._fetch_run_dataset_ids(run)

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

    @property
    def obsCoreTableManager(self) -> ObsCoreTableManager | None:
        # Docstring inherited from a base class.
        return self._registry.obsCoreTableManager

    @property
    def storageClasses(self) -> StorageClassFactory:
        return self._registry.storageClasses
