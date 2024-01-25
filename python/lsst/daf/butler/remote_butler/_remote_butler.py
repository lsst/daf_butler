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

__all__ = (
    "ButlerServerError",
    "RemoteButler",
)

from collections.abc import Collection, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TextIO, TypeVar, cast
from uuid import uuid4

import httpx
from lsst.daf.butler import __version__
from lsst.daf.butler.datastores.fileDatastoreClient import get_dataset_as_python_object
from lsst.resources import ResourcePath, ResourcePathExpression
from pydantic import BaseModel, TypeAdapter

from .._butler import Butler
from .._butler_instance_options import ButlerInstanceOptions
from .._dataset_ref import DatasetId, DatasetRef, SerializedDatasetRef
from .._dataset_type import DatasetType, SerializedDatasetType
from .._storage_class import StorageClass
from .._utilities.locked_object import LockedObject
from ..datastore import DatasetRefURIs
from ..dimensions import DataCoordinate, DataIdValue, DimensionConfig, DimensionUniverse, SerializedDataId
from ..registry import MissingDatasetTypeError, NoDefaultCollectionError, RegistryDefaults
from ..registry.wildcards import CollectionWildcard
from ._authentication import get_authentication_headers
from .server_models import (
    CLIENT_REQUEST_ID_HEADER_NAME,
    CollectionList,
    DatasetTypeName,
    FindDatasetModel,
    GetFileByDataIdRequestModel,
    GetFileResponseModel,
)

if TYPE_CHECKING:
    from .._dataset_existence import DatasetExistence
    from .._deferredDatasetHandle import DeferredDatasetHandle
    from .._file_dataset import FileDataset
    from .._limited_butler import LimitedButler
    from .._query import Query
    from .._timespan import Timespan
    from ..dimensions import DataId, DimensionGroup, DimensionRecord
    from ..registry import CollectionArgType, Registry
    from ..transfers import RepoExportContext


_AnyPydanticModel = TypeVar("_AnyPydanticModel", bound=BaseModel)
"""Generic type variable that accepts any Pydantic model class."""
_InputCollectionList = str | Sequence[str] | None
"""The possible types of the ``collections`` parameter of most Butler methods.
"""

_SERIALIZED_DATA_ID_TYPE_ADAPTER = TypeAdapter(SerializedDataId)


class RemoteButler(Butler):  # numpydoc ignore=PR02
    """A `Butler` that can be used to connect through a remote server.

    Parameters
    ----------
    server_url : `str`
        URL of the Butler server we will connect to.
    options : `ButlerInstanceOptions`
        Default values and other settings for the Butler instance.
    http_client : `httpx.Client`
        HTTP connection pool we will use to connect to the server.
    access_token : `str`
        Rubin Science Platform Gafaelfawr access token that will be used to
        authenticate with the server.
    cache : RemoteButlerCache
        Cache of data shared between multiple RemoteButler instances connected
        to the same server.

    Notes
    -----
    Instead of using this constructor, most users should use either
    `Butler.from_config` or `RemoteButlerFactory`.
    """

    _registry_defaults: RegistryDefaults
    _client: httpx.Client
    _server_url: str
    _headers: dict[str, str]
    _cache: RemoteButlerCache

    # This is __new__ instead of __init__ because we have to support
    # instantiation via the legacy constructor Butler.__new__(), which
    # reads the configuration and selects which subclass to instantiate.  The
    # interaction between __new__ and __init__ is kind of wacky in Python.  If
    # we were using __init__ here, __init__ would be called twice (once when
    # the RemoteButler instance is constructed inside Butler.from_config(), and
    # a second time with the original arguments to Butler() when the instance
    # is returned from Butler.__new__()
    def __new__(
        cls,
        *,
        server_url: str,
        options: ButlerInstanceOptions,
        http_client: httpx.Client,
        access_token: str,
        cache: RemoteButlerCache,
    ) -> RemoteButler:
        self = cast(RemoteButler, super().__new__(cls))

        self._client = http_client
        self._server_url = server_url
        self._cache = cache

        # TODO: RegistryDefaults should have finish() called on it, but this
        # requires getCollectionSummary() which is not yet implemented
        self._registry_defaults = RegistryDefaults(
            options.collections, options.run, options.inferDefaults, **options.kwargs
        )

        auth_headers = get_authentication_headers(access_token)
        headers = {"user-agent": f"RemoteButler/{__version__}"}

        self._headers = auth_headers | headers

        return self

    def isWriteable(self) -> bool:
        # Docstring inherited.
        return False

    @property
    def dimensions(self) -> DimensionUniverse:
        # Docstring inherited.
        with self._cache.access() as cache:
            if cache.dimensions is not None:
                return cache.dimensions

        response = self._get("universe")

        config = DimensionConfig.fromString(response.text, format="json")
        universe = DimensionUniverse(config)
        with self._cache.access() as cache:
            if cache.dimensions is None:
                cache.dimensions = universe
            return cache.dimensions

    def _simplify_dataId(self, dataId: DataId | None, kwargs: dict[str, DataIdValue]) -> SerializedDataId:
        """Take a generic Data ID and convert it to a serializable form.

        Parameters
        ----------
        dataId : `dict`, `None`, `DataCoordinate`
            The data ID to serialize.
        kwargs : `dict`
            Additional entries to augment or replace the values in ``dataId``.

        Returns
        -------
        data_id : `SerializedDataId`
            A serializable form.
        """
        if dataId is None:
            dataId = {}
        elif isinstance(dataId, DataCoordinate):
            dataId = dataId.to_simple(minimal=True).dataId
        else:
            dataId = dict(dataId)

        return _SERIALIZED_DATA_ID_TYPE_ADAPTER.validate_python(dataId | kwargs)

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
        model = self._get_file_info(datasetRefOrType, dataId, collections, kwargs)

        # If the caller provided a DatasetRef or DatasetType, they may have
        # overridden the storage class on it.  We need to respect this, if they
        # haven't asked to re-override it.
        explicitDatasetType = _extract_dataset_type(datasetRefOrType)
        if explicitDatasetType is not None:
            if storageClass is None:
                storageClass = explicitDatasetType.storageClass

        # If the caller provided a DatasetRef, they may have overridden the
        # component on it.  We need to explicitly handle this because we did
        # not send the DatasetType to the server in this case.
        componentOverride = None
        if isinstance(datasetRefOrType, DatasetRef):
            componentOverride = datasetRefOrType.datasetType.component()

        return get_dataset_as_python_object(
            model,
            parameters=parameters,
            storageClass=storageClass,
            universe=self.dimensions,
            component=componentOverride,
        )

    def _get_file_info(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        dataId: DataId | None,
        collections: _InputCollectionList,
        kwargs: dict[str, DataIdValue],
    ) -> GetFileResponseModel:
        """Send a request to the server for the file URLs and metadata
        associated with a dataset.
        """
        if isinstance(datasetRefOrType, DatasetRef):
            dataset_id = datasetRefOrType.id
            response = self._get(f"get_file/{dataset_id}", expected_errors=(404,))
            if response.status_code == 404:
                raise LookupError(f"Dataset not found: {datasetRefOrType}")
        else:
            request = GetFileByDataIdRequestModel(
                dataset_type_name=self._normalize_dataset_type_name(datasetRefOrType),
                collections=self._normalize_collections(collections),
                data_id=self._simplify_dataId(dataId, kwargs),
            )
            response = self._post("get_file_by_data_id", request, expected_errors=(404,))
            if response.status_code == 404:
                raise LookupError(
                    f"Dataset not found with DataId: {dataId} DatasetType: {datasetRefOrType}"
                    f" collections: {collections}"
                )

        return self._parse_model(response, GetFileResponseModel)

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
        if predict or run:
            raise NotImplementedError("Predict mode is not supported by RemoteButler")

        response = self._get_file_info(datasetRefOrType, dataId, collections, kwargs)
        file_info = response.file_info
        if len(file_info) == 1:
            return DatasetRefURIs(primaryURI=ResourcePath(str(file_info[0].url)))
        else:
            components = {}
            for f in file_info:
                component = f.datastoreRecords.component
                if component is None:
                    raise ValueError(
                        f"DatasetId {response.dataset_ref.id} has a component file"
                        " with no component name defined"
                    )
                components[component] = ResourcePath(str(f.url))
            return DatasetRefURIs(componentURIs=components)

    def get_dataset_type(self, name: str) -> DatasetType:
        # In future implementation this should directly access the cache
        # and only go to the server if the dataset type is not known.
        path = f"dataset_type/{name}"
        response = self._get(path, expected_errors=(404,))
        if response.status_code != httpx.codes.OK:
            content = response.json()
            if content["exception"] == "MissingDatasetTypeError":
                raise MissingDatasetTypeError(content["detail"])
        return DatasetType.from_simple(SerializedDatasetType(**response.json()), universe=self.dimensions)

    def get_dataset(
        self,
        id: DatasetId,
        *,
        storage_class: str | StorageClass | None = None,
        dimension_records: bool = False,
        datastore_records: bool = False,
    ) -> DatasetRef | None:
        path = f"dataset/{id}"
        params: dict[str, str | bool] = {
            "dimension_records": dimension_records,
            "datastore_records": datastore_records,
        }
        if datastore_records:
            raise ValueError("Datastore records can not yet be returned in client/server butler.")
        response = self._get(path, params=params)
        if response.json() is None:
            return None
        ref = DatasetRef.from_simple(
            self._parse_model(response, SerializedDatasetRef), universe=self.dimensions
        )
        if storage_class is not None:
            ref = ref.overrideStorageClass(storage_class)
        return ref

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
        if datastore_records:
            raise ValueError("Datastore records can not yet be returned in client/server butler.")
        if timespan:
            raise ValueError("Timespan can not yet be used in butler client/server.")

        dataset_type = self._normalize_dataset_type_name(dataset_type)

        query = FindDatasetModel(
            data_id=self._simplify_dataId(data_id, kwargs),
            collections=self._normalize_collections(collections),
            dimension_records=dimension_records,
            datastore_records=datastore_records,
        )

        path = f"find_dataset/{dataset_type}"
        response = self._post(path, query)

        ref = DatasetRef.from_simple(
            self._parse_model(response, SerializedDatasetRef), universe=self.dimensions
        )
        if storage_class is not None:
            ref = ref.overrideStorageClass(storage_class)
        return ref

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

    def transfer_dimension_records_from(
        self, source_butler: LimitedButler | Butler, source_refs: Iterable[DatasetRef]
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
        dry_run: bool = False,
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

    def _query(self) -> AbstractContextManager[Query]:
        # Docstring inherited.
        raise NotImplementedError()

    def _query_data_ids(
        self,
        dimensions: DimensionGroup | Iterable[str] | str,
        *,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        expanded: bool = False,
        order_by: Iterable[str] | str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        explain: bool = True,
        **kwargs: Any,
    ) -> list[DataCoordinate]:
        # Docstring inherited.
        raise NotImplementedError()

    def _query_datasets(
        self,
        dataset_type: Any,
        collections: CollectionArgType | None = None,
        *,
        find_first: bool = True,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        expanded: bool = False,
        explain: bool = True,
        **kwargs: Any,
    ) -> list[DatasetRef]:
        # Docstring inherited.
        raise NotImplementedError()

    def _query_dimension_records(
        self,
        element: str,
        *,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        order_by: Iterable[str] | str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        explain: bool = True,
        **kwargs: Any,
    ) -> list[DimensionRecord]:
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
        slash = "" if self._server_url.endswith("/") else "/"
        return f"{self._server_url}{slash}{version}/{path}"

    def _post(self, path: str, model: BaseModel, expected_errors: Iterable[int] = ()) -> httpx.Response:
        """Send a POST request to the Butler server."""
        json = model.model_dump_json(exclude_unset=True).encode("utf-8")
        return self._send_request(
            "POST",
            path,
            content=json,
            headers={"content-type": "application/json"},
            expected_errors=expected_errors,
        )

    def _get(
        self, path: str, params: Mapping[str, str | bool] | None = None, expected_errors: Iterable[int] = ()
    ) -> httpx.Response:
        """Send a GET request to the Butler server."""
        return self._send_request("GET", path, params=params, expected_errors=expected_errors)

    def _send_request(
        self,
        method: str,
        path: str,
        *,
        content: bytes | None = None,
        params: Mapping[str, str | bool] | None = None,
        headers: Mapping[str, str] | None = None,
        expected_errors: Iterable[int],
    ) -> httpx.Response:
        url = self._get_url(path)

        request_id = str(uuid4())
        request_headers = {CLIENT_REQUEST_ID_HEADER_NAME: request_id}
        request_headers.update(self._headers)
        if headers is not None:
            request_headers.update(headers)

        try:
            response = self._client.request(
                method, url, content=content, params=params, headers=request_headers
            )
            if response.status_code not in expected_errors:
                response.raise_for_status()
            return response
        except httpx.HTTPError as e:
            raise ButlerServerError(request_id) from e

    def _parse_model(self, response: httpx.Response, model: type[_AnyPydanticModel]) -> _AnyPydanticModel:
        """Deserialize a Pydantic model from the body of an HTTP response."""
        return model.model_validate_json(response.content)

    def _normalize_collections(self, collections: _InputCollectionList) -> CollectionList:
        """Convert the ``collections`` parameter in the format used by Butler
        methods to a standardized format for the REST API.
        """
        if collections is None:
            if not self.collections:
                raise NoDefaultCollectionError(
                    "No collections provided, and no defaults from butler construction."
                )
            collections = self.collections
        # Temporary hack. Assume strings for collections. In future
        # want to construct CollectionWildcard and filter it through collection
        # cache to generate list of collection names.
        wildcards = CollectionWildcard.from_expression(collections)
        return CollectionList(list(wildcards.strings))

    def _normalize_dataset_type_name(self, datasetTypeOrName: DatasetType | str) -> DatasetTypeName:
        """Convert DatasetType parameters in the format used by Butler methods
        to a standardized string name for the REST API.
        """
        if isinstance(datasetTypeOrName, DatasetType):
            return DatasetTypeName(datasetTypeOrName.name)
        else:
            return DatasetTypeName(datasetTypeOrName)

    def _clone(
        self,
        *,
        collections: Any = None,
        run: str | None = None,
        inferDefaults: bool = True,
        **kwargs: Any,
    ) -> RemoteButler:
        raise NotImplementedError()


def _extract_dataset_type(datasetRefOrType: DatasetRef | DatasetType | str) -> DatasetType | None:
    """Return the DatasetType associated with the argument, or None if the
    argument is not an object that contains a DatasetType object.
    """
    if isinstance(datasetRefOrType, DatasetType):
        return datasetRefOrType
    elif isinstance(datasetRefOrType, DatasetRef):
        return datasetRefOrType.datasetType
    else:
        return None


@dataclass
class _RemoteButlerCacheData:
    dimensions: DimensionUniverse | None = None


class RemoteButlerCache(LockedObject[_RemoteButlerCacheData]):
    def __init__(self) -> None:
        super().__init__(_RemoteButlerCacheData())


class ButlerServerError(RuntimeError):
    """Exception returned when there is an error communicating with the Butler
    server.

    Parameters
    ----------
    client_request_id : `str`
        Request ID to include in the exception message.
    """

    def __init__(self, client_request_id: str):
        super().__init__(f"Error while communicating with Butler server.  Request ID: {client_request_id}")
