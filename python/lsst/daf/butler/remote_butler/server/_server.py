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

__all__ = ("app", "factory_dependency")

import logging
import uuid
from functools import cache
from typing import Any

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    MissingDatasetTypeError,
    SerializedDataCoordinate,
    SerializedDatasetRef,
    SerializedDatasetType,
)
from safir.metadata import Metadata, get_metadata

from ._config import get_config_from_env
from ._factory import Factory
from ._server_models import FindDatasetModel

log = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.exception_handler(MissingDatasetTypeError)
def missing_dataset_type_exception_handler(request: Request, exc: MissingDatasetTypeError) -> JSONResponse:
    # Remove the double quotes around the string form. These confuse
    # the JSON serialization when single quotes are in the message.
    message = str(exc).strip('"')
    return JSONResponse(
        status_code=404,
        content={"detail": message, "exception": "MissingDatasetTypeError"},
    )


@cache
def _make_global_butler() -> Butler:
    config = get_config_from_env()
    return Butler.from_config(config.config_uri)


def factory_dependency() -> Factory:
    return Factory(butler=_make_global_butler())


def unpack_dataId(butler: Butler, data_id: SerializedDataCoordinate | None) -> DataCoordinate | None:
    """Convert the serialized dataId back to full DataCoordinate.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler to use for registry and universe.
    data_id : `SerializedDataCoordinate` or `None`
        The serialized form.

    Returns
    -------
    dataId : `DataCoordinate` or `None`
        The DataId usable by registry.
    """
    if data_id is None:
        return None
    return DataCoordinate.from_simple(data_id, registry=butler.registry)


@app.get(
    "/",
    description=(
        "Return metadata about the running application. Can also be used as"
        " a health check. This route is not exposed outside the cluster and"
        " therefore cannot be used by external clients."
    ),
    include_in_schema=False,
    response_model=Metadata,
    response_model_exclude_none=True,
    summary="Application metadata",
)
async def get_index() -> Metadata:
    """GET ``/`` (the app's internal root).

    By convention, this endpoint returns only the application's metadata.
    """
    return get_metadata(package_name="lsst.daf.butler", application_name="butler")


@app.get(
    "/butler/butler.yaml",
    description=(
        "Returns a Butler YAML configuration file that can be used to instantiate a Butler client"
        " pointing at this server"
    ),
    summary="Client configuration file",
    response_model=dict[str, Any],
)
@app.get(
    "/butler/butler.json",
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


@app.get("/butler/v1/universe", response_model=dict[str, Any])
def get_dimension_universe(factory: Factory = Depends(factory_dependency)) -> dict[str, Any]:
    """Allow remote client to get dimensions definition."""
    butler = factory.create_butler()
    return butler.dimensions.dimensionConfig.toDict()


@app.get(
    "/butler/v1/dataset_type/{dataset_type_name}",
    summary="Retrieve this dataset type definition.",
    response_model=SerializedDatasetType,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def get_dataset_type(
    dataset_type_name: str, factory: Factory = Depends(factory_dependency)
) -> SerializedDatasetType:
    """Return the dataset type."""
    butler = factory.create_butler()
    datasetType = butler.get_dataset_type(dataset_type_name)
    return datasetType.to_simple()


@app.get(
    "/butler/v1/dataset/{id}",
    summary="Retrieve this dataset definition.",
    response_model=SerializedDatasetRef | None,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def get_dataset(
    id: uuid.UUID,
    storage_class: str | None = None,
    dimension_records: bool = False,
    datastore_records: bool = False,
    factory: Factory = Depends(factory_dependency),
) -> SerializedDatasetRef | None:
    """Return a single dataset reference."""
    butler = factory.create_butler()
    ref = butler.get_dataset(
        id,
        storage_class=storage_class,
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
@app.post(
    "/butler/v1/find_dataset/{dataset_type}",
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

    # Get the simple dict from the SerializedDataCoordinate. We do not know
    # if it is a well-defined DataCoordinate or needs some massaging first.
    # find_dataset will use dimension record queries if necessary.
    data_id = query.data_id.dataId

    butler = factory.create_butler()
    ref = butler.find_dataset(
        dataset_type,
        None,
        collections=collection_query,
        storage_class=query.storage_class,
        timespan=None,
        dimension_records=query.dimension_records,
        datastore_records=query.datastore_records,
        **data_id,
    )
    return ref.to_simple() if ref else None
