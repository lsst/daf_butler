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
from lsst.daf.butler.repo_relocation import replaceRoot
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import get_full_type_name

from .._butler import Butler
from .._butler_config import ButlerConfig
from .._config import Config
from .._dataset_existence import DatasetExistence
from .._dataset_ref import DatasetId, DatasetIdGenEnum, DatasetRef, SerializedDatasetRef
from .._dataset_type import DatasetType, SerializedDatasetType
from .._deferredDatasetHandle import DeferredDatasetHandle
from .._file_dataset import FileDataset
from .._limited_butler import LimitedButler
from .._storage_class import StorageClass
from .._timespan import Timespan
from ..datastore import DatasetRefURIs
from ..dimensions import DataCoordinate, DataId, DimensionConfig, DimensionUniverse, SerializedDataCoordinate
from ..registry import MissingDatasetTypeError, NoDefaultCollectionError, Registry, RegistryDefaults
from ..registry.wildcards import CollectionWildcard
from ..transfers import RepoExportContext
from ._authentication import get_authentication_headers, get_authentication_token_from_environment
from ._config import RemoteButlerConfigModel
from .server_models import FindDatasetModel


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
        access_token: str | None = None,
        **kwargs: Any,
    ):
        butler_config = ButlerConfig(config, searchPaths, without_datastore=True)
        # There is a convention in Butler config files where <butlerRoot> in a
        # configuration option refers to the directory containing the
        # configuration file. We allow this for the remote butler's URL so
        # that the server doesn't have to know which hostname it is being
        # accessed from.
        server_url_key = ("remote_butler", "url")
        if server_url_key in butler_config:
            butler_config[server_url_key] = replaceRoot(
                butler_config[server_url_key], butler_config.configDir
            )
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
            server_url = str(self._config.remote_butler.url)
            auth_headers = {}
            if access_token is None:
                access_token = get_authentication_token_from_environment(server_url)
            if access_token is not None:
                auth_headers = get_authentication_headers(access_token)

            headers = {"user-agent": f"{get_full_type_name(self)}/{__version__}"}
            headers.update(auth_headers)
            self._client = httpx.Client(headers=headers, base_url=server_url)

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

    def _simplify_dataId(
        self, dataId: DataId | None, **kwargs: dict[str, int | str]
    ) -> SerializedDataCoordinate | None:
        """Take a generic Data ID and convert it to a serializable form.

        Parameters
        ----------
        dataId : `dict`, `None`, `DataCoordinate`
            The data ID to serialize.
        **kwargs : `dict`
            Additional values that should be included if this is not
            a `DataCoordinate`.

        Returns
        -------
        data_id : `SerializedDataCoordinate` or `None`
            A serializable form.
        """
        if dataId is None and not kwargs:
            return None
        if isinstance(dataId, DataCoordinate):
            return dataId.to_simple()

        if dataId is None:
            data_id = kwargs
        elif kwargs:
            # Change variable because DataId is immutable and mypy complains.
            data_id = dict(dataId)
            data_id.update(kwargs)

        # Assume we can treat it as a dict.
        return SerializedDataCoordinate(dataId=data_id)

    def _caching_context(self) -> AbstractContextManager[None]:
        # Docstring inherited.
        # Not implemented for now, will have to think whether this needs to
        # do something on client side and/or remote side.
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

    def get_dataset_type(self, name: str) -> DatasetType:
        # In future implementation this should directly access the cache
        # and only go to the server if the dataset type is not known.
        path = f"dataset_type/{name}"
        response = self._client.get(self._get_url(path))
        if response.status_code != httpx.codes.OK:
            content = response.json()
            if content["exception"] == "MissingDatasetTypeError":
                raise MissingDatasetTypeError(content["detail"])
        response.raise_for_status()
        return DatasetType.from_simple(SerializedDatasetType(**response.json()), universe=self.dimensions)

    def get_dataset(
        self,
        id: DatasetId,
        storage_class: str | StorageClass | None = None,
        dimension_records: bool = False,
        datastore_records: bool = False,
    ) -> DatasetRef | None:
        path = f"dataset/{id}"
        if isinstance(storage_class, StorageClass):
            storage_class_name = storage_class.name
        elif storage_class:
            storage_class_name = storage_class
        params: dict[str, str | bool] = {
            "dimension_records": dimension_records,
            "datastore_records": datastore_records,
        }
        if datastore_records:
            raise ValueError("Datastore records can not yet be returned in client/server butler.")
        if storage_class:
            params["storage_class"] = storage_class_name
        response = self._client.get(self._get_url(path), params=params)
        response.raise_for_status()
        if response.json() is None:
            return None
        return DatasetRef.from_simple(SerializedDatasetRef(**response.json()), universe=self.dimensions)

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
        if collections is None:
            if not self.collections:
                raise NoDefaultCollectionError(
                    "No collections provided to find_dataset, and no defaults from butler construction."
                )
            collections = self.collections
        # Temporary hack. Assume strings for collections. In future
        # want to construct CollectionWildcard and filter it through collection
        # cache to generate list of collection names.
        wildcards = CollectionWildcard.from_expression(collections)

        if datastore_records:
            raise ValueError("Datastore records can not yet be returned in client/server butler.")
        if timespan:
            raise ValueError("Timespan can not yet be used in butler client/server.")

        if isinstance(dataset_type, DatasetType):
            dataset_type = dataset_type.name

        if isinstance(storage_class, StorageClass):
            storage_class = storage_class.name

        query = FindDatasetModel(
            data_id=self._simplify_dataId(data_id, **kwargs),
            collections=wildcards.strings,
            storage_class=storage_class,
            dimension_records=dimension_records,
            datastore_records=datastore_records,
        )

        path = f"find_dataset/{dataset_type}"
        response = self._client.post(
            self._get_url(path), json=query.model_dump(mode="json", exclude_unset=True, exclude_defaults=True)
        )
        response.raise_for_status()

        return DatasetRef.from_simple(SerializedDatasetRef(**response.json()), universe=self.dimensions)

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
        """Form the complete path to an endpoint on the server.

        Parameters
        ----------
        path : `str`
            The relative path to the server endpoint.
        version : `str`, optional
            Version string to prepend to path. Defaults to "v1".

        Returns
        -------
        path : `str`
            The full path to the endpoint.
        """
        return f"{version}/{path}"
