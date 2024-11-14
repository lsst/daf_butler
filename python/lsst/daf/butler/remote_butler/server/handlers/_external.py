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

__all__ = ()

import uuid
from typing import Any

from fastapi import APIRouter, Depends

from ...._butler import Butler
from ...._collection_type import CollectionType
from ...._dataset_ref import DatasetRef
from ...._exceptions import DatasetNotFoundError
from ....registry.interfaces import ChainedCollectionRecord
from ...server_models import (
    ExpandDataIdRequestModel,
    ExpandDataIdResponseModel,
    FindDatasetRequestModel,
    FindDatasetResponseModel,
    GetCollectionInfoResponseModel,
    GetCollectionSummaryResponseModel,
    GetDatasetTypeResponseModel,
    GetFileByDataIdRequestModel,
    GetFileResponseModel,
    GetUniverseResponseModel,
    QueryCollectionInfoRequestModel,
    QueryCollectionInfoResponseModel,
    QueryCollectionsRequestModel,
    QueryCollectionsResponseModel,
    QueryDatasetTypesRequestModel,
    QueryDatasetTypesResponseModel,
)
from .._dependencies import factory_dependency
from .._factory import Factory
from ._utils import set_default_data_id

external_router = APIRouter()


@external_router.get(
    "/butler.yaml",
    description=(
        "Returns a Butler YAML configuration file that can be used to instantiate a Butler client"
        " pointing at this server"
    ),
    summary="Client configuration file",
    response_model=dict[str, Any],
)
@external_router.get(
    "/butler.json",
    description=(
        "Returns a Butler JSON configuration file that can be used to instantiate a Butler client"
        " pointing at this server"
    ),
    summary="Client configuration file",
    response_model=dict[str, Any],
)
async def get_client_config() -> dict[str, Any]:
    # We can return JSON data for both the YAML and JSON case because all JSON
    # files are parseable as YAML.
    return {"cls": "lsst.daf.butler.remote_butler.RemoteButler", "remote_butler": {"url": "<butlerRoot>"}}


@external_router.get("/v1/universe")
def get_dimension_universe(
    factory: Factory = Depends(factory_dependency),
) -> GetUniverseResponseModel:
    # Allow remote client to get dimensions definition.
    butler = factory.create_butler()
    return GetUniverseResponseModel(universe=butler.dimensions.dimensionConfig.to_simple())


@external_router.get(
    "/v1/dataset_type/{name}",
    summary="Retrieve this dataset type definition.",
)
def get_dataset_type(
    name: str, factory: Factory = Depends(factory_dependency)
) -> GetDatasetTypeResponseModel:
    # Return the dataset type.
    butler = factory.create_butler()
    datasetType = butler.get_dataset_type(name)
    return GetDatasetTypeResponseModel(dataset_type=datasetType.to_simple())


@external_router.get(
    "/v1/dataset/{id}",
    summary="Retrieve this dataset definition.",
)
def get_dataset(
    id: uuid.UUID,
    dimension_records: bool = False,
    factory: Factory = Depends(factory_dependency),
) -> FindDatasetResponseModel:
    # Return a single dataset reference.
    butler = factory.create_butler()
    ref = butler.get_dataset(id, dimension_records=dimension_records)
    serialized_ref = ref.to_simple() if ref else None
    return FindDatasetResponseModel(dataset_ref=serialized_ref)


@external_router.post(
    "/v1/find_dataset",
    summary="Retrieve this dataset definition from collection, dataset type, and dataId",
)
def find_dataset(
    query: FindDatasetRequestModel,
    factory: Factory = Depends(factory_dependency),
) -> FindDatasetResponseModel:
    butler = factory.create_butler()
    set_default_data_id(butler, query.default_data_id)
    ref = butler.find_dataset(
        query.dataset_type,
        query.data_id,
        collections=query.collections,
        timespan=query.timespan,
        dimension_records=query.dimension_records,
    )
    serialized_ref = ref.to_simple() if ref else None
    return FindDatasetResponseModel(dataset_ref=serialized_ref)


@external_router.post("/v1/expand_data_id", summary="Return full dimension records for a given data ID")
def expand_data_id(
    request: ExpandDataIdRequestModel,
    factory: Factory = Depends(factory_dependency),
) -> ExpandDataIdResponseModel:
    butler = factory.create_butler()
    coordinate = butler.registry.expandDataId(request.data_id)
    return ExpandDataIdResponseModel(data_coordinate=coordinate.to_simple())


@external_router.get(
    "/v1/get_file/{dataset_id}",
    summary="Lookup via DatasetId (UUID) the information needed to download and use the files associated"
    " with a dataset.",
)
def get_file(
    dataset_id: uuid.UUID,
    factory: Factory = Depends(factory_dependency),
) -> GetFileResponseModel:
    butler = factory.create_butler()
    ref = butler.get_dataset(dataset_id, datastore_records=True)
    if ref is None:
        raise DatasetNotFoundError(f"Dataset ID {dataset_id} not found")
    return _get_file_by_ref(butler, ref)


@external_router.post(
    "/v1/get_file_by_data_id",
    summary="Lookup via DataId (metadata key/value pairs) the information needed to download"
    " and use the files associated with a dataset.",
)
def get_file_by_data_id(
    request: GetFileByDataIdRequestModel,
    factory: Factory = Depends(factory_dependency),
) -> GetFileResponseModel:
    butler = factory.create_butler()
    set_default_data_id(butler, request.default_data_id)
    ref = butler._findDatasetRef(
        datasetRefOrType=request.dataset_type,
        dataId=request.data_id,
        collections=request.collections,
        datastore_records=True,
        timespan=request.timespan,
    )
    return _get_file_by_ref(butler, ref)


def _get_file_by_ref(butler: Butler, ref: DatasetRef) -> GetFileResponseModel:
    """Return file information associated with ``ref``."""
    payload = butler._datastore.prepare_get_for_external_client(ref)
    return GetFileResponseModel(dataset_ref=ref.to_simple(), artifact=payload)


# TODO DM-46204: This can be removed once the RSP recommended image has been
# upgraded to a version that contains DM-46129.
@external_router.get(
    "/v1/collection_info", summary="Get information about a collection", response_model_exclude_unset=True
)
def get_collection_info(
    name: str,
    include_doc: bool = False,
    include_parents: bool = False,
    factory: Factory = Depends(factory_dependency),
) -> GetCollectionInfoResponseModel:
    butler = factory.create_butler()
    record = butler._registry.get_collection_record(name)
    if record.type == CollectionType.CHAINED:
        assert isinstance(record, ChainedCollectionRecord)
        children = record.children
    else:
        children = ()
    response = GetCollectionInfoResponseModel(name=record.name, type=record.type, children=children)
    if include_doc:
        response.doc = butler._registry.getCollectionDocumentation(name)
    if include_parents:
        response.parents = butler._registry.getCollectionParentChains(name)
    return response


@external_router.get(
    "/v1/collection_summary", summary="Get summary information about the datasets in a collection"
)
def get_collection_summary(
    name: str, factory: Factory = Depends(factory_dependency)
) -> GetCollectionSummaryResponseModel:
    butler = factory.create_butler()
    return GetCollectionSummaryResponseModel(summary=butler.registry.getCollectionSummary(name).to_simple())


# TODO DM-46204: This can be removed once the RSP recommended image has been
# upgraded to a version that contains DM-46129.
@external_router.post(
    "/v1/query_collections", summary="Search for collections with names that match an expression"
)
def query_collections(
    request: QueryCollectionsRequestModel, factory: Factory = Depends(factory_dependency)
) -> QueryCollectionsResponseModel:
    butler = factory.create_butler()
    collections = butler.registry.queryCollections(
        expression=request.search,
        collectionTypes=request.collection_types,
        flattenChains=request.flatten_chains,
        includeChains=request.include_chains,
    )
    return QueryCollectionsResponseModel(collections=collections)


@external_router.post(
    "/v1/query_collection_info", summary="Search for collections with names that match an expression"
)
def query_collection_info(
    request: QueryCollectionInfoRequestModel, factory: Factory = Depends(factory_dependency)
) -> QueryCollectionInfoResponseModel:
    butler = factory.create_butler()
    collections = butler.collections.query_info(
        expression=request.expression,
        collection_types=set(request.collection_types),
        flatten_chains=request.flatten_chains,
        include_chains=request.include_chains,
        include_parents=request.include_parents,
        include_summary=request.include_summary,
        include_doc=request.include_doc,
        summary_datasets=request.summary_datasets,
    )
    return QueryCollectionInfoResponseModel(collections=list(collections))


@external_router.post(
    "/v1/query_dataset_types", summary="Search for dataset types with names that match an expression"
)
def query_dataset_types(
    request: QueryDatasetTypesRequestModel, factory: Factory = Depends(factory_dependency)
) -> QueryDatasetTypesResponseModel:
    butler = factory.create_butler()
    missing: list[str] = []
    dataset_types = butler.registry.queryDatasetTypes(expression=request.search, missing=missing)
    return QueryDatasetTypesResponseModel(
        dataset_types=[dt.to_simple() for dt in dataset_types], missing=missing
    )
