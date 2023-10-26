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

__all__ = ("RemoteButler",)

from collections.abc import Collection, Iterable, Sequence
from contextlib import AbstractContextManager
from typing import Any, TextIO

import httpx
from lsst.daf.butler import __version__
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import get_full_type_name

from .._butler import Butler
from .._butler_config import ButlerConfig
from .._config import Config
from .._dataset_existence import DatasetExistence
from .._dataset_ref import DatasetIdGenEnum, DatasetRef
from .._dataset_type import DatasetType
from .._deferredDatasetHandle import DeferredDatasetHandle
from .._file_dataset import FileDataset
from .._limited_butler import LimitedButler
from .._storage_class import StorageClass
from ..datastore import DatasetRefURIs
from ..dimensions import DataId, DimensionConfig, DimensionUniverse
from ..registry import Registry, RegistryDefaults
from ..transfers import RepoExportContext
from ._config import RemoteButlerConfigModel


class RemoteButler(Butler):
    def __init__(
        self,
        # These parameters are inherited from the Butler() constructor
        config: Config | ResourcePathExpression | None = None,
        *,
        collections: Any = None,
        run: str | None = None,
        searchPaths: Sequence[ResourcePathExpression] | None = None,
        writeable: bool | None = None,
        inferDefaults: bool = True,
        # Parameters unique to RemoteButler
        http_client: httpx.Client | None = None,
        **kwargs: Any,
    ):
        butler_config = ButlerConfig(config, searchPaths, without_datastore=True)
        self._config = RemoteButlerConfigModel.model_validate(butler_config)
        self._dimensions: DimensionUniverse | None = None
        # TODO: RegistryDefaults should have finish() called on it, but this
        # requires getCollectionSummary() which is not yet implemented
        self._registry_defaults = RegistryDefaults(collections, run, inferDefaults, **kwargs)

        if http_client is not None:
            # We have injected a client explicitly in to the class.
            # This is generally done for testing.
            self._client = http_client
        else:
            headers = {"user-agent": f"{get_full_type_name(self)}/{__version__}"}
            self._client = httpx.Client(headers=headers, base_url=str(self._config.remote_butler.url))

    def isWriteable(self) -> bool:
        # Docstring inherited.
        return False

    @property
    def dimensions(self) -> DimensionUniverse:
        # Docstring inherited.
        if self._dimensions is not None:
            return self._dimensions

        response = self._client.get(self._get_url("universe"))
        response.raise_for_status()

        config = DimensionConfig.fromString(response.text, format="json")
        self._dimensions = DimensionUniverse(config)
        return self._dimensions

    def getDatasetType(self, name: str) -> DatasetType:
        # Docstring inherited.
        raise NotImplementedError()

    def transaction(self) -> AbstractContextManager[None]:
        """Will always raise NotImplementedError.
        Transactions are not supported by RemoteButler.
        """
        raise NotImplementedError()

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
        # Docstring inherited.
        raise NotImplementedError()

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
        # Docstring inherited.
        raise NotImplementedError()

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
        # Docstring inherited.
        raise NotImplementedError()

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
        # Docstring inherited.
        raise NotImplementedError()

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
        # Docstring inherited.
        raise NotImplementedError()

    def retrieveArtifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool = False,
    ) -> list[ResourcePath]:
        # Docstring inherited.
        raise NotImplementedError()

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
        # Docstring inherited.
        raise NotImplementedError()

    def _exists_many(
        self,
        refs: Iterable[DatasetRef],
        /,
        *,
        full_check: bool = True,
    ) -> dict[DatasetRef, DatasetExistence]:
        # Docstring inherited.
        raise NotImplementedError()

    def removeRuns(self, names: Iterable[str], unstore: bool = True) -> None:
        # Docstring inherited.
        raise NotImplementedError()

    def ingest(
        self,
        *datasets: FileDataset,
        transfer: str | None = "auto",
        run: str | None = None,
        idGenerationMode: DatasetIdGenEnum | None = None,
        record_validation_info: bool = True,
    ) -> None:
        # Docstring inherited.
        raise NotImplementedError()

    def export(
        self,
        *,
        directory: str | None = None,
        filename: str | None = None,
        format: str | None = None,
        transfer: str | None = None,
    ) -> AbstractContextManager[RepoExportContext]:
        # Docstring inherited.
        raise NotImplementedError()

    def import_(
        self,
        *,
        directory: ResourcePathExpression | None = None,
        filename: ResourcePathExpression | TextIO | None = None,
        format: str | None = None,
        transfer: str | None = None,
        skip_dimensions: set | None = None,
    ) -> None:
        # Docstring inherited.
        raise NotImplementedError()

    def transfer_from(
        self,
        source_butler: LimitedButler,
        source_refs: Iterable[DatasetRef],
        transfer: str = "auto",
        skip_missing: bool = True,
        register_dataset_types: bool = False,
        transfer_dimensions: bool = False,
    ) -> Collection[DatasetRef]:
        # Docstring inherited.
        raise NotImplementedError()

    def validateConfiguration(
        self,
        logFailures: bool = False,
        datasetTypeNames: Iterable[str] | None = None,
        ignore: Iterable[str] | None = None,
    ) -> None:
        # Docstring inherited.
        raise NotImplementedError()

    @property
    def collections(self) -> Sequence[str]:
        # Docstring inherited.
        return self._registry_defaults.collections

    @property
    def run(self) -> str | None:
        # Docstring inherited.
        return self._registry_defaults.run

    @property
    def registry(self) -> Registry:
        # Docstring inherited.
        raise NotImplementedError()

    def pruneDatasets(
        self,
        refs: Iterable[DatasetRef],
        *,
        disassociate: bool = True,
        unstore: bool = False,
        tags: Iterable[str] = (),
        purge: bool = False,
    ) -> None:
        # Docstring inherited.
        raise NotImplementedError()

    def _get_url(self, path: str, version: str = "v1") -> str:
        """Form the complete path to an endpoint on the server

        Parameters
        ----------
        path : `str`
            The relative path to the server endpoint. Should not include the
            "/butler" prefix.
        version : `str`, optional
            Version string to prepend to path. Defaults to "v1".

        Returns
        -------
        path : `str`
            The full path to the endpoint
        """
        prefix = "butler"
        return f"{prefix}/{version}/{path}"
