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

from collections.abc import Collection, Iterable, Sequence
from contextlib import AbstractContextManager
from types import EllipsisType
from typing import Any, TextIO, cast

from lsst.resources import ResourcePath, ResourcePathExpression

from .._butler import Butler
from .._butler_collections import ButlerCollections
from .._dataset_existence import DatasetExistence
from .._dataset_ref import DatasetId, DatasetRef
from .._dataset_type import DatasetType
from .._deferredDatasetHandle import DeferredDatasetHandle
from .._file_dataset import FileDataset
from .._limited_butler import LimitedButler
from .._storage_class import StorageClass
from .._timespan import Timespan
from ..datastore import DatasetRefURIs
from ..dimensions import DataCoordinate, DataId, DimensionElement, DimensionRecord, DimensionUniverse
from ..direct_butler import DirectButler
from ..queries import Query
from ..registry import CollectionArgType, Registry
from ..remote_butler import RemoteButler
from ..transfers import RepoExportContext
from .hybrid_butler_collections import HybridButlerCollections
from .hybrid_butler_registry import HybridButlerRegistry


class HybridButler(Butler):
    """A `Butler` that delegates methods to internal RemoteButler and
    DirectButler instances.  Intended to allow testing of RemoteButler before
    its implementation is complete, by delegating unsupported methods to
    DirectButler.
    """

    _remote_butler: RemoteButler
    _direct_butler: DirectButler
    _registry: Registry

    def __new__(cls, remote_butler: RemoteButler, direct_butler: DirectButler) -> HybridButler:
        self = cast(HybridButler, super().__new__(cls))
        self._remote_butler = remote_butler
        self._direct_butler = direct_butler
        self._datastore = direct_butler._datastore
        self._registry = HybridButlerRegistry(direct_butler._registry, remote_butler.registry)
        return self

    def isWriteable(self) -> bool:
        return self._remote_butler.isWriteable()

    def _caching_context(self) -> AbstractContextManager[None]:
        return self._direct_butler._caching_context()

    def transaction(self) -> AbstractContextManager[None]:
        return self._direct_butler.transaction()

    def put(
        self,
        obj: Any,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        run: str | None = None,
        **kwargs: Any,
    ) -> DatasetRef:
        return self._direct_butler.put(obj, datasetRefOrType, dataId, run=run, **kwargs)

    def getDeferred(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        parameters: dict | None = None,
        collections: Any = None,
        storageClass: str | StorageClass | None = None,
        **kwargs: Any,
    ) -> DeferredDatasetHandle:
        return self._remote_butler.getDeferred(
            datasetRefOrType,
            dataId,
            parameters=parameters,
            collections=collections,
            storageClass=storageClass,
            **kwargs,
        )

    def get(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        parameters: dict[str, Any] | None = None,
        collections: Any = None,
        storageClass: StorageClass | str | None = None,
        **kwargs: Any,
    ) -> Any:
        return self._remote_butler.get(
            datasetRefOrType,
            dataId,
            parameters=parameters,
            collections=collections,
            storageClass=storageClass,
            **kwargs,
        )

    def getURIs(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        predict: bool = False,
        collections: Any = None,
        run: str | None = None,
        **kwargs: Any,
    ) -> DatasetRefURIs:
        return self._remote_butler.getURIs(
            datasetRefOrType, dataId, predict=predict, collections=collections, run=run, **kwargs
        )

    def getURI(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        predict: bool = False,
        collections: Any = None,
        run: str | None = None,
        **kwargs: Any,
    ) -> ResourcePath:
        return self._remote_butler.getURI(
            datasetRefOrType, dataId, predict=predict, collections=collections, run=run, **kwargs
        )

    def get_dataset_type(self, name: str) -> DatasetType:
        return self._remote_butler.get_dataset_type(name)

    def get_dataset(
        self,
        id: DatasetId,
        *,
        storage_class: str | StorageClass | None = None,
        dimension_records: bool = False,
        datastore_records: bool = False,
    ) -> DatasetRef | None:
        return self._remote_butler.get_dataset(
            id,
            storage_class=storage_class,
            dimension_records=dimension_records,
            datastore_records=datastore_records,
        )

    def find_dataset(
        self,
        dataset_type: DatasetType | str,
        data_id: DataId | None = None,
        *,
        collections: str | Sequence[str] | None = None,
        timespan: Timespan | None = None,
        storage_class: str | StorageClass | None = None,
        dimension_records: bool = False,
        datastore_records: bool = False,
        **kwargs: Any,
    ) -> DatasetRef | None:
        return self._remote_butler.find_dataset(
            dataset_type,
            data_id,
            collections=collections,
            timespan=timespan,
            storage_class=storage_class,
            dimension_records=dimension_records,
            datastore_records=datastore_records,
            **kwargs,
        )

    def retrieveArtifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool = False,
    ) -> list[ResourcePath]:
        return self._remote_butler.retrieveArtifacts(refs, destination, transfer, preserve_path, overwrite)

    def exists(
        self,
        dataset_ref_or_type: DatasetRef | DatasetType | str,
        /,
        data_id: DataId | None = None,
        *,
        full_check: bool = True,
        collections: Any = None,
        **kwargs: Any,
    ) -> DatasetExistence:
        return self._remote_butler.exists(
            dataset_ref_or_type, data_id, full_check=full_check, collections=collections, **kwargs
        )

    def _exists_many(
        self,
        refs: Iterable[DatasetRef],
        /,
        *,
        full_check: bool = True,
    ) -> dict[DatasetRef, DatasetExistence]:
        return self._remote_butler._exists_many(refs, full_check=full_check)

    def removeRuns(self, names: Iterable[str], unstore: bool = True) -> None:
        return self._direct_butler.removeRuns(names, unstore)

    def ingest(
        self,
        *datasets: FileDataset,
        transfer: str | None = "auto",
        record_validation_info: bool = True,
    ) -> None:
        return self._direct_butler.ingest(
            *datasets, transfer=transfer, record_validation_info=record_validation_info
        )

    def export(
        self,
        *,
        directory: str | None = None,
        filename: str | None = None,
        format: str | None = None,
        transfer: str | None = None,
    ) -> AbstractContextManager[RepoExportContext]:
        return self._direct_butler.export(
            directory=directory, filename=filename, format=format, transfer=transfer
        )

    def import_(
        self,
        *,
        directory: ResourcePathExpression | None = None,
        filename: ResourcePathExpression | TextIO | None = None,
        format: str | None = None,
        transfer: str | None = None,
        skip_dimensions: set | None = None,
        record_validation_info: bool = True,
        without_datastore: bool = False,
    ) -> None:
        self._direct_butler.import_(
            directory=directory,
            filename=filename,
            format=format,
            transfer=transfer,
            skip_dimensions=skip_dimensions,
            record_validation_info=record_validation_info,
            without_datastore=without_datastore,
        )

    def transfer_dimension_records_from(
        self, source_butler: LimitedButler | Butler, source_refs: Iterable[DatasetRef]
    ) -> None:
        return self._direct_butler.transfer_dimension_records_from(source_butler, source_refs)

    def transfer_from(
        self,
        source_butler: LimitedButler,
        source_refs: Iterable[DatasetRef],
        transfer: str = "auto",
        skip_missing: bool = True,
        register_dataset_types: bool = False,
        transfer_dimensions: bool = False,
        dry_run: bool = False,
    ) -> Collection[DatasetRef]:
        return self._direct_butler.transfer_from(
            source_butler,
            source_refs,
            transfer,
            skip_missing,
            register_dataset_types,
            transfer_dimensions,
            dry_run,
        )

    def validateConfiguration(
        self,
        logFailures: bool = False,
        datasetTypeNames: Iterable[str] | None = None,
        ignore: Iterable[str] | None = None,
    ) -> None:
        return self._direct_butler.validateConfiguration(logFailures, datasetTypeNames, ignore)

    @property
    def run(self) -> str | None:
        return self._remote_butler.run

    @property
    def registry(self) -> Registry:
        return self._registry

    def query(self) -> AbstractContextManager[Query]:
        return self._remote_butler.query()

    def clone(
        self,
        *,
        collections: CollectionArgType | None | EllipsisType = ...,
        run: str | None | EllipsisType = ...,
        inferDefaults: bool | EllipsisType = ...,
        dataId: dict[str, str] | EllipsisType = ...,
    ) -> HybridButler:
        remote_butler = self._remote_butler.clone(
            collections=collections, run=run, inferDefaults=inferDefaults, dataId=dataId
        )
        direct_butler = self._direct_butler.clone(
            collections=collections, run=run, inferDefaults=inferDefaults, dataId=dataId
        )
        return HybridButler(remote_butler, direct_butler)

    def pruneDatasets(
        self,
        refs: Iterable[DatasetRef],
        *,
        disassociate: bool = True,
        unstore: bool = False,
        tags: Iterable[str] = (),
        purge: bool = False,
    ) -> None:
        return self._direct_butler.pruneDatasets(
            refs, disassociate=disassociate, unstore=unstore, tags=tags, purge=purge
        )

    @property
    def dimensions(self) -> DimensionUniverse:
        return self._remote_butler.dimensions

    def _extract_all_dimension_records_from_data_ids(
        self,
        source_butler: LimitedButler | Butler,
        data_ids: set[DataCoordinate],
        allowed_elements: frozenset[DimensionElement],
    ) -> dict[DimensionElement, dict[DataCoordinate, DimensionRecord]]:
        return self._direct_butler._extract_all_dimension_records_from_data_ids(
            source_butler, data_ids, allowed_elements
        )

    @property
    def collection_chains(self) -> ButlerCollections:
        return HybridButlerCollections(self)

    @property
    def collections(self) -> ButlerCollections:
        return HybridButlerCollections(self)
