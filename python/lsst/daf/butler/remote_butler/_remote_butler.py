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

__all__ = ("RemoteButler",)

from collections.abc import Collection, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, TextIO, Type, TypeVar

import httpx
from lsst.daf.butler import __version__
from lsst.daf.butler.datastores.fileDatastoreClient import get_dataset_as_python_object
from lsst.daf.butler.repo_relocation import replaceRoot
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import get_full_type_name
from pydantic import parse_obj_as

from .._butler import Butler
from .._butler_config import ButlerConfig
from .._compat import _BaseModelCompat
from .._config import Config
from .._dataset_ref import DatasetId, DatasetIdGenEnum, DatasetRef, SerializedDatasetRef
from .._dataset_type import DatasetType, SerializedDatasetType
from .._storage_class import StorageClass
from ..datastore import DatasetRefURIs
from ..dimensions import DataCoordinate, DataIdValue, DimensionConfig, DimensionUniverse, SerializedDataId
from ..registry import MissingDatasetTypeError, NoDefaultCollectionError, RegistryDefaults
from ..registry.wildcards import CollectionWildcard
from ._authentication import get_authentication_headers, get_authentication_token_from_environment
from ._config import RemoteButlerConfigModel
from .server_models import (
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


_AnyPydanticModel = TypeVar("_AnyPydanticModel", bound=_BaseModelCompat)
"""Generic type variable that accepts any Pydantic model class."""
_InputCollectionList = str | Sequence[str] | None
"""The possible types of the ``collections`` parameter of most Butler methods.
"""


class RemoteButler(Butler):
    """A `Butler` that can be used to connect through a remote server.

    Parameters
    ----------
    config : `ButlerConfig`, `Config` or `str`, optional
        Configuration. Anything acceptable to the `ButlerConfig` constructor.
        If a directory path is given the configuration will be read from a
        ``butler.yaml`` file in that location. If `None` is given default
        values will be used. If ``config`` contains "cls" key then its value is
        used as a name of butler class and it must be a sub-class of this
        class, otherwise `DirectButler` is instantiated.
    collections : `str` or `~collections.abc.Iterable` [ `str` ], optional
        An expression specifying the collections to be searched (in order) when
        reading datasets.
        This may be a `str` collection name or an iterable thereof.
        See :ref:`daf_butler_collection_expressions` for more information.
        These collections are not registered automatically and must be
        manually registered before they are used by any method, but they may be
        manually registered after the `Butler` is initialized.
    run : `str`, optional
        Name of the `~CollectionType.RUN` collection new datasets should be
        inserted into.  If ``collections`` is `None` and ``run`` is not `None`,
        ``collections`` will be set to ``[run]``.  If not `None`, this
        collection will automatically be registered.  If this is not set (and
        ``writeable`` is not set either), a read-only butler will be created.
    searchPaths : `list` of `str`, optional
        Directory paths to search when calculating the full Butler
        configuration.  Not used if the supplied config is already a
        `ButlerConfig`.
    writeable : `bool`, optional
        Explicitly sets whether the butler supports write operations.  If not
        provided, a read-write butler is created if any of ``run``, ``tags``,
        or ``chains`` is non-empty.
    inferDefaults : `bool`, optional
        If `True` (default) infer default data ID values from the values
        present in the datasets in ``collections``: if all collections have the
        same value (or no value) for a governor dimension, that value will be
        the default for that dimension.  Nonexistent collections are ignored.
        If a default value is provided explicitly for a governor dimension via
        ``**kwargs``, no default will be inferred for that dimension.
    http_client : `httpx.Client` or `None`, optional
        Client to use to connect to the server. This is generally only
        necessary for test code.
    access_token : `str` or `None`, optional
        Explicit access token to use when connecting to the server. If not
        given an attempt will be found to obtain one from the environment.
    **kwargs : `Any`
        Parameters that can be used to set defaults for governor dimensions.
    """

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

        return parse_obj_as(SerializedDataId, dataId | kwargs)

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
            response = self._get(f"get_file/{dataset_id}")
            if response.status_code == 404:
                raise LookupError(f"Dataset not found: {datasetRefOrType}")
        else:
            request = GetFileByDataIdRequestModel(
                dataset_type_name=self._normalize_dataset_type_name(datasetRefOrType),
                collections=self._normalize_collections(collections),
                data_id=self._simplify_dataId(dataId, kwargs),
            )
            response = self._post("get_file_by_data_id", request)
            if response.status_code == 404:
                raise LookupError(
                    f"Dataset not found with DataId: {dataId} DatasetType: {datasetRefOrType}"
                    f" collections: {collections}"
                )

        response.raise_for_status()
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
        response = self._client.get(self._get_url(path), params=params)
        response.raise_for_status()
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
        response.raise_for_status()

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
        return f"{version}/{path}"

    def _post(self, path: str, model: _BaseModelCompat) -> httpx.Response:
        """Send a POST request to the Butler server."""
        json = model.model_dump_json(exclude_unset=True).encode("utf-8")
        url = self._get_url(path)
        return self._client.post(url, content=json, headers={"content-type": "application/json"})

    def _get(self, path: str) -> httpx.Response:
        """Send a GET request to the Butler server."""
        url = self._get_url(path)
        return self._client.get(url)

    def _parse_model(self, response: httpx.Response, model: Type[_AnyPydanticModel]) -> _AnyPydanticModel:
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
