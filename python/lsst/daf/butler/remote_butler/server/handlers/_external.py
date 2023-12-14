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

__all__ = ()

import uuid
from typing import Any

from fastapi import APIRouter, Depends
from lsst.daf.butler import Butler, DatasetRef, SerializedDatasetRef, SerializedDatasetType
from lsst.daf.butler.remote_butler.server_models import (
    FindDatasetModel,
    GetFileByDataIdRequestModel,
    GetFileResponseModel,
)

from .._dependencies import factory_dependency
from .._exceptions import NotFoundException
from .._factory import Factory

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


@external_router.get("/v1/universe", response_model=dict[str, Any])
def get_dimension_universe(factory: Factory = Depends(factory_dependency)) -> dict[str, Any]:
    # Allow remote client to get dimensions definition.
    butler = factory.create_butler()
    return butler.dimensions.dimensionConfig.toDict()


@external_router.get(
    "/v1/dataset_type/{dataset_type_name}",
    summary="Retrieve this dataset type definition.",
    response_model=SerializedDatasetType,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def get_dataset_type(
    dataset_type_name: str, factory: Factory = Depends(factory_dependency)
) -> SerializedDatasetType:
    # Return the dataset type.
    butler = factory.create_butler()
    datasetType = butler.get_dataset_type(dataset_type_name)
    return datasetType.to_simple()


@external_router.get(
    "/v1/dataset/{id}",
    summary="Retrieve this dataset definition.",
    response_model=SerializedDatasetRef | None,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def get_dataset(
    id: uuid.UUID,
    dimension_records: bool = False,
    datastore_records: bool = False,
    factory: Factory = Depends(factory_dependency),
) -> SerializedDatasetRef | None:
    # Return a single dataset reference.
    butler = factory.create_butler()
    ref = butler.get_dataset(
        id,
        dimension_records=dimension_records,
        datastore_records=datastore_records,
    )
    if ref is not None:
        return ref.to_simple()
    # This could raise a 404 since id is not found. The standard implementation
    # get_dataset method returns without error so follow that example here.
    return ref


# Not yet supported: TimeSpan is not yet a pydantic model.
# collections parameter assumes client-side has resolved regexes.
@external_router.post(
    "/v1/find_dataset/{dataset_type}",
    summary="Retrieve this dataset definition from collection, dataset type, and dataId",
    response_model=SerializedDatasetRef,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def find_dataset(
    dataset_type: str,
    query: FindDatasetModel,
    factory: Factory = Depends(factory_dependency),
) -> SerializedDatasetRef | None:
    collection_query = query.collections if query.collections else None

    butler = factory.create_butler()
    ref = butler.find_dataset(
        dataset_type,
        query.data_id,
        collections=collection_query,
        timespan=None,
        dimension_records=query.dimension_records,
        datastore_records=query.datastore_records,
    )
    return ref.to_simple() if ref else None


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
        raise NotFoundException(f"Dataset ID {dataset_id} not found")
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
    try:
        ref = butler._findDatasetRef(
            datasetRefOrType=request.dataset_type_name,
            dataId=request.data_id,
            collections=request.collections,
            datastore_records=True,
        )
        return _get_file_by_ref(butler, ref)
    except LookupError as e:
        raise NotFoundException() from e


def _get_file_by_ref(butler: Butler, ref: DatasetRef) -> GetFileResponseModel:
    """Return file information associated with ``ref``."""
    payload = butler._datastore.prepare_get_for_external_client(ref)
    return GetFileResponseModel.model_validate(payload)
