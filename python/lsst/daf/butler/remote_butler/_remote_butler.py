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

import uuid
from collections.abc import Collection, Iterable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from types import EllipsisType
from typing import TYPE_CHECKING, Any, TextIO, cast

from deprecated.sphinx import deprecated

from lsst.daf.butler.datastores.file_datastore.retrieve_artifacts import (
    ArtifactIndexInfo,
    ZipIndex,
    determine_destination_for_retrieved_artifact,
    retrieve_and_zip,
    unpack_zips,
)
from lsst.daf.butler.datastores.fileDatastoreClient import (
    FileDatastoreGetPayload,
    get_dataset_as_python_object,
)
from lsst.resources import ResourcePath, ResourcePathExpression

from .._butler import Butler
from .._butler_collections import ButlerCollections
from .._dataset_existence import DatasetExistence
from .._dataset_ref import DatasetId, DatasetRef
from .._dataset_type import DatasetType
from .._deferredDatasetHandle import DeferredDatasetHandle
from .._exceptions import DatasetNotFoundError
from .._query_all_datasets import QueryAllDatasetsParameters
from .._storage_class import StorageClass, StorageClassFactory
from .._utilities.locked_object import LockedObject
from ..datastore import DatasetRefURIs, DatastoreConfig
from ..datastore.cache_manager import AbstractDatastoreCacheManager, DatastoreCacheManager
from ..dimensions import DataIdValue, DimensionConfig, DimensionUniverse, SerializedDataId
from ..queries import Query
from ..queries.tree import make_column_literal
from ..registry import CollectionArgType, NoDefaultCollectionError, Registry, RegistryDefaults
from ._collection_args import convert_collection_arg_to_glob_string_list
from ._defaults import DefaultsHolder
from ._http_connection import RemoteButlerHttpConnection, parse_model, quote_path_variable
from ._query_driver import RemoteQueryDriver
from ._query_results import convert_dataset_ref_results, read_query_results
from ._ref_utils import apply_storage_class_override, normalize_dataset_type_name, simplify_dataId
from ._registry import RemoteButlerRegistry
from ._remote_butler_collections import RemoteButlerCollections
from .server_models import (
    CollectionList,
    FindDatasetRequestModel,
    FindDatasetResponseModel,
    GetDatasetTypeResponseModel,
    GetFileByDataIdRequestModel,
    GetFileResponseModel,
    GetUniverseResponseModel,
    QueryAllDatasetsRequestModel,
)

if TYPE_CHECKING:
    from .._dataset_provenance import DatasetProvenance
    from .._file_dataset import FileDataset
    from .._limited_butler import LimitedButler
    from .._timespan import Timespan
    from ..dimensions import DataId
    from ..transfers import RepoExportContext


class RemoteButler(Butler):  # numpydoc ignore=PR02
    """A `Butler` that can be used to connect through a remote server.

    Parameters
    ----------
    options : `ButlerInstanceOptions`
        Default values and other settings for the Butler instance.
    connection : `RemoteButlerHttpConnection`
        Connection to Butler server.
    cache : `RemoteButlerCache`
        Cache of data shared between multiple RemoteButler instances connected
        to the same server.
    use_disabled_datastore_cache : `bool`, optional
        If `True`, a datastore cache manager will be created with a default
        disabled state which can be enabled by the environment. If `False`
        a cache manager will be constructed from the default local
        configuration, likely caching by default but only specific storage
        classes.

    Notes
    -----
    Instead of using this constructor, most users should use either
    `Butler.from_config` or `RemoteButlerFactory`.
    """

    _registry_defaults: DefaultsHolder
    _connection: RemoteButlerHttpConnection
    _cache: RemoteButlerCache
    _registry: RemoteButlerRegistry
    _datastore_cache_manager: AbstractDatastoreCacheManager | None
    _use_disabled_datastore_cache: bool

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
        connection: RemoteButlerHttpConnection,
        defaults: RegistryDefaults,
        cache: RemoteButlerCache,
        use_disabled_datastore_cache: bool = True,
    ) -> RemoteButler:
        self = cast(RemoteButler, super().__new__(cls))
        self.storageClasses = StorageClassFactory()

        self._connection = connection
        self._cache = cache
        self._datastore_cache_manager = None
        self._use_disabled_datastore_cache = use_disabled_datastore_cache

        self._registry_defaults = DefaultsHolder(defaults)
        self._registry = RemoteButlerRegistry(self, self._registry_defaults, self._connection)
        defaults.finish(self._registry)

        return self

    def isWriteable(self) -> bool:
        # Docstring inherited.
        return False

    @property
    @deprecated(
        "Please use 'collections' instead. collection_chains will be removed after v28.",
        version="v28",
        category=FutureWarning,
    )
    def collection_chains(self) -> ButlerCollections:
        """Object with methods for modifying collection chains."""
        return self.collections

    @property
    def collections(self) -> ButlerCollections:
        """Object with methods for modifying and querying collections."""
        return RemoteButlerCollections(self._registry_defaults, self._connection)

    @property
    def dimensions(self) -> DimensionUniverse:
        # Docstring inherited.
        with self._cache.access() as cache:
            if cache.dimensions is not None:
                return cache.dimensions

        response = self._connection.get("universe")
        model = parse_model(response, GetUniverseResponseModel)

        config = DimensionConfig.from_simple(model.universe)
        universe = DimensionUniverse(
            config,
            # The process-global cache internal to DimensionUniverse can mask
            # problems in unit tests, since client and server live in the same
            # process for these tests.  We are doing our own caching, so we
            # don't benefit from it.  So just disable it.
            use_cache=False,
        )
        with self._cache.access() as cache:
            if cache.dimensions is None:
                cache.dimensions = universe
            return cache.dimensions

    @property
    def _cache_manager(self) -> AbstractDatastoreCacheManager:
        """Cache manager to use when reading files from the butler."""
        # RemoteButler does not get any cache configuration from the server.
        # Either create a disabled cache manager which can be enabled via the
        # environment, or create a cache manager from the default FileDatastore
        # config. This will not work properly if the defaults for
        # DatastoreConfig no longer include the cache.
        if self._datastore_cache_manager is None:
            datastore_config = DatastoreConfig()
            if not self._use_disabled_datastore_cache and "cached" in datastore_config:
                self._datastore_cache_manager = DatastoreCacheManager(
                    datastore_config["cached"], universe=self.dimensions
                )
            else:
                self._datastore_cache_manager = DatastoreCacheManager.create_disabled(
                    universe=self.dimensions
                )
        return self._datastore_cache_manager

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
        provenance: DatasetProvenance | None = None,
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
        timespan: Timespan | None = None,
        **kwargs: Any,
    ) -> DeferredDatasetHandle:
        response = self._get_file_info(datasetRefOrType, dataId, collections, timespan, kwargs)
        # Check that artifact information is available.
        _to_file_payload(response)
        ref = DatasetRef.from_simple(response.dataset_ref, universe=self.dimensions)
        return DeferredDatasetHandle(butler=self, ref=ref, parameters=parameters, storageClass=storageClass)

    def get(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        /,
        dataId: DataId | None = None,
        *,
        parameters: dict[str, Any] | None = None,
        collections: Any = None,
        storageClass: StorageClass | str | None = None,
        timespan: Timespan | None = None,
        **kwargs: Any,
    ) -> Any:
        # Docstring inherited.
        model = self._get_file_info(datasetRefOrType, dataId, collections, timespan, kwargs)

        ref = DatasetRef.from_simple(model.dataset_ref, universe=self.dimensions)
        # If the caller provided a DatasetRef, they may have overridden the
        # component on it.  We need to explicitly handle this because we did
        # not send the DatasetType to the server in this case.
        if isinstance(datasetRefOrType, DatasetRef):
            componentOverride = datasetRefOrType.datasetType.component()
            if componentOverride:
                ref = ref.makeComponentRef(componentOverride)
        ref = apply_storage_class_override(ref, datasetRefOrType, storageClass)

        return self._get_dataset_as_python_object(ref, model, parameters)

    def _get_dataset_as_python_object(
        self,
        ref: DatasetRef,
        model: GetFileResponseModel,
        parameters: dict[str, Any] | None,
    ) -> Any:
        # This thin wrapper method is here to provide a place to hook in a mock
        # mimicking DatastoreMock functionality for use in unit tests.
        return get_dataset_as_python_object(
            ref,
            _to_file_payload(model),
            parameters=parameters,
            cache_manager=self._cache_manager,
        )

    def _get_file_info(
        self,
        datasetRefOrType: DatasetRef | DatasetType | str,
        dataId: DataId | None,
        collections: CollectionArgType,
        timespan: Timespan | None,
        kwargs: dict[str, DataIdValue],
    ) -> GetFileResponseModel:
        """Send a request to the server for the file URLs and metadata
        associated with a dataset.
        """
        if isinstance(datasetRefOrType, DatasetRef):
            if dataId is not None:
                raise ValueError("DatasetRef given, cannot use dataId as well")
            return self._get_file_info_for_ref(datasetRefOrType)
        else:
            request = GetFileByDataIdRequestModel(
                dataset_type=normalize_dataset_type_name(datasetRefOrType),
                collections=self._normalize_collections(collections),
                data_id=simplify_dataId(dataId, kwargs),
                default_data_id=self._serialize_default_data_id(),
                timespan=timespan,
            )
            response = self._connection.post("get_file_by_data_id", request)
            return parse_model(response, GetFileResponseModel)

    def _get_file_info_for_ref(self, ref: DatasetRef) -> GetFileResponseModel:
        response = self._connection.get(f"get_file/{_to_uuid_string(ref.id)}")
        return parse_model(response, GetFileResponseModel)

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

        response = self._get_file_info(datasetRefOrType, dataId, collections, None, kwargs)
        file_info = _to_file_payload(response).file_info
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
        response = self._connection.get(f"dataset_type/{quote_path_variable(name)}")
        model = parse_model(response, GetDatasetTypeResponseModel)
        return DatasetType.from_simple(model.dataset_type, universe=self.dimensions)

    def get_dataset(
        self,
        id: DatasetId,
        *,
        storage_class: str | StorageClass | None = None,
        dimension_records: bool = False,
        datastore_records: bool = False,
    ) -> DatasetRef | None:
        # datastore_records is intentionally ignored.  It is an optimization
        # flag that only applies to DirectButler.
        path = f"dataset/{_to_uuid_string(id)}"
        response = self._connection.get(path, params={"dimension_records": bool(dimension_records)})
        model = parse_model(response, FindDatasetResponseModel)
        if model.dataset_ref is None:
            return None
        ref = DatasetRef.from_simple(model.dataset_ref, universe=self.dimensions)
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
        # datastore_records is intentionally ignored.  It is an optimization
        # flag that only applies to DirectButler.

        query = FindDatasetRequestModel(
            dataset_type=normalize_dataset_type_name(dataset_type),
            data_id=simplify_dataId(data_id, kwargs),
            default_data_id=self._serialize_default_data_id(),
            collections=self._normalize_collections(collections),
            timespan=timespan,
            dimension_records=dimension_records,
        )

        response = self._connection.post("find_dataset", query)

        model = parse_model(response, FindDatasetResponseModel)
        if model.dataset_ref is None:
            return None

        ref = DatasetRef.from_simple(model.dataset_ref, universe=self.dimensions)
        return apply_storage_class_override(ref, dataset_type, storage_class)

    def _retrieve_artifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool = False,
        write_index: bool = True,
        add_prefix: bool = False,
    ) -> dict[ResourcePath, ArtifactIndexInfo]:
        destination = ResourcePath(destination).abspath()
        if not destination.isdir():
            raise ValueError(f"Destination location must refer to a directory. Given {destination}.")

        if transfer not in ("auto", "copy"):
            raise ValueError("Only 'copy' and 'auto' transfer modes are supported.")

        requested_ids = {ref.id for ref in refs}
        have_copied: dict[ResourcePath, ResourcePath] = {}
        artifact_map: dict[ResourcePath, ArtifactIndexInfo] = {}
        # Sort to ensure that in many refs to one file situation the same
        # ref is used for any prefix that might be added.
        for ref in sorted(refs):
            prefix = str(ref.id)[:8] + "-" if add_prefix else ""
            file_info = _to_file_payload(self._get_file_info_for_ref(ref)).file_info
            for file in file_info:
                source_uri = ResourcePath(str(file.url))
                # For DECam/zip we only want to copy once.
                # For zip files we need to unpack so that they can be
                # zipped up again if needed.
                is_zip = source_uri.getExtension() == ".zip" and "zip-path" in source_uri.fragment
                cleaned_source_uri = source_uri.replace(fragment="", query="", params="")
                if is_zip:
                    if cleaned_source_uri not in have_copied:
                        zipped_artifacts = unpack_zips(
                            [cleaned_source_uri], requested_ids, destination, preserve_path, overwrite
                        )
                        artifact_map.update(zipped_artifacts)
                        have_copied[cleaned_source_uri] = cleaned_source_uri
                elif cleaned_source_uri not in have_copied:
                    relative_path = ResourcePath(file.datastoreRecords.path, forceAbsolute=False)
                    target_uri = determine_destination_for_retrieved_artifact(
                        destination, relative_path, preserve_path, prefix
                    )
                    # Because signed URLs expire, we want to do the transfer
                    # soon after retrieving the URL.
                    target_uri.transfer_from(source_uri, transfer="copy", overwrite=overwrite)
                    have_copied[cleaned_source_uri] = target_uri
                    artifact_map[target_uri] = ArtifactIndexInfo.from_single(file.datastoreRecords, ref.id)
                else:
                    target_uri = have_copied[cleaned_source_uri]
                    artifact_map[target_uri].append(ref.id)

        if write_index:
            index = ZipIndex.from_artifact_map(refs, artifact_map, destination)
            index.write_index(destination)

        return artifact_map

    def retrieve_artifacts_zip(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        overwrite: bool = True,
    ) -> ResourcePath:
        return retrieve_and_zip(refs, destination, self._retrieve_artifacts, overwrite)

    def retrieveArtifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool = False,
    ) -> list[ResourcePath]:
        artifact_map = self._retrieve_artifacts(
            refs,
            destination,
            transfer,
            preserve_path,
            overwrite,
        )
        return list(artifact_map)

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
        try:
            response = self._get_file_info(
                dataset_ref_or_type, dataId=data_id, collections=collections, timespan=None, kwargs=kwargs
            )
        except DatasetNotFoundError:
            return DatasetExistence.UNRECOGNIZED

        if response.artifact is None:
            if full_check:
                return DatasetExistence.RECORDED
            else:
                return DatasetExistence.RECORDED | DatasetExistence._ASSUMED

        if full_check:
            for file in response.artifact.file_info:
                if not ResourcePath(str(file.url)).exists():
                    return DatasetExistence.RECORDED | DatasetExistence.DATASTORE
            return DatasetExistence.VERIFIED
        else:
            return DatasetExistence.KNOWN

    def _exists_many(
        self,
        refs: Iterable[DatasetRef],
        /,
        *,
        full_check: bool = True,
    ) -> dict[DatasetRef, DatasetExistence]:
        return {ref: self.exists(ref, full_check=full_check) for ref in refs}

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

    def ingest_zip(self, zip_file: ResourcePathExpression, transfer: str = "auto") -> None:
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
        record_validation_info: bool = True,
        without_datastore: bool = False,
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
    def run(self) -> str | None:
        # Docstring inherited.
        return self._registry_defaults.get().run

    @property
    def registry(self) -> Registry:
        return self._registry

    @contextmanager
    def query(self) -> Iterator[Query]:
        driver = RemoteQueryDriver(self, self._connection)
        with driver:
            query = Query(driver)
            yield query

    @contextmanager
    def _query_all_datasets_by_page(
        self, args: QueryAllDatasetsParameters
    ) -> Iterator[Iterator[list[DatasetRef]]]:
        universe = self.dimensions

        request = QueryAllDatasetsRequestModel(
            collections=self._normalize_collections(args.collections),
            name=[normalize_dataset_type_name(name) for name in args.name],
            find_first=args.find_first,
            data_id=simplify_dataId(args.data_id, args.kwargs),
            default_data_id=self._serialize_default_data_id(),
            where=args.where,
            bind={k: make_column_literal(v) for k, v in args.bind.items()},
            limit=args.limit,
            with_dimension_records=args.with_dimension_records,
        )
        with self._connection.post_with_stream_response("query/all_datasets", request) as response:
            pages = read_query_results(response)
            yield (convert_dataset_ref_results(page, universe) for page in pages)

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

    def _normalize_collections(self, collections: CollectionArgType | None) -> CollectionList:
        """Convert the ``collections`` parameter in the format used by Butler
        methods to a standardized format for the REST API.
        """
        if collections is None:
            if not self.collections.defaults:
                raise NoDefaultCollectionError(
                    "No collections provided, and no defaults from butler construction."
                )
            collections = self.collections.defaults
        return convert_collection_arg_to_glob_string_list(collections)

    def clone(
        self,
        *,
        collections: CollectionArgType | None | EllipsisType = ...,
        run: str | None | EllipsisType = ...,
        inferDefaults: bool | EllipsisType = ...,
        dataId: dict[str, str] | EllipsisType = ...,
    ) -> RemoteButler:
        defaults = self._registry_defaults.get().clone(collections, run, inferDefaults, dataId)
        return RemoteButler(connection=self._connection, cache=self._cache, defaults=defaults)

    def __str__(self) -> str:
        return f"RemoteButler({self._connection.server_url})"

    def _serialize_default_data_id(self) -> SerializedDataId:
        """Convert the default data ID to a serializable format."""
        # In an ideal world, the default data ID would just get combined with
        # the rest of the data ID on the client side instead of being sent
        # separately to the server.  Unfortunately, that requires knowledge of
        # the DatasetType's dimensions which we don't always have available on
        # the client.  Data ID values can be specified indirectly by "implied"
        # dimensions, but knowing what things are implied depends on what the
        # required dimensions are.

        return self._registry_defaults.get().dataId.to_simple(minimal=True).dataId


def _to_file_payload(get_file_response: GetFileResponseModel) -> FileDatastoreGetPayload:
    if get_file_response.artifact is None:
        ref = get_file_response.dataset_ref
        raise DatasetNotFoundError(f"Dataset is known, but artifact is not available. (datasetId='{ref.id}')")

    return get_file_response.artifact


def _to_uuid_string(id: uuid.UUID | str) -> str:
    """Convert a UUID, or string parseable as a UUID, into a string formatted
    like '1481269e-4c8d-4696-bcca-d1b4c9005d06'
    """
    return str(uuid.UUID(str(id)))


@dataclass
class _RemoteButlerCacheData:
    dimensions: DimensionUniverse | None = None


class RemoteButlerCache(LockedObject[_RemoteButlerCacheData]):
    def __init__(self) -> None:
        super().__init__(_RemoteButlerCacheData())
