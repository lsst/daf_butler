# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

import logging
from collections.abc import Mapping
from enum import Enum, auto
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.gzip import GZipMiddleware
from lsst.daf.butler import (
    Butler,
    Config,
    DataCoordinate,
    DatasetId,
    DatasetRef,
    DimensionConfig,
    SerializedDataCoordinate,
    SerializedDatasetRef,
    SerializedDatasetType,
    SerializedDimensionRecord,
)
from lsst.daf.butler.core.serverModels import (
    ExpressionQueryParameter,
    QueryDataIdsModel,
    QueryDatasetsModel,
    QueryDimensionRecordsModel,
)
from lsst.daf.butler.registry import CollectionType

BUTLER_ROOT = "ci_hsc_gen3/DATA"

log = logging.getLogger("excalibur")


class CollectionTypeNames(str, Enum):
    """Collection type names supported by the interface."""

    def _generate_next_value_(name, start, count, last_values) -> str:  # type: ignore  # noqa: N805
        # Use the name directly as the value
        return name

    RUN = auto()
    CALIBRATION = auto()
    CHAINED = auto()
    TAGGED = auto()


app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)


GLOBAL_READWRITE_BUTLER: Butler | None = None
GLOBAL_READONLY_BUTLER: Butler | None = None


def _make_global_butler() -> None:
    global GLOBAL_READONLY_BUTLER, GLOBAL_READWRITE_BUTLER
    if GLOBAL_READONLY_BUTLER is None:
        GLOBAL_READONLY_BUTLER = Butler(BUTLER_ROOT, writeable=False)
    if GLOBAL_READWRITE_BUTLER is None:
        GLOBAL_READWRITE_BUTLER = Butler(BUTLER_ROOT, writeable=True)


def butler_readonly_dependency() -> Butler:
    _make_global_butler()
    return Butler(butler=GLOBAL_READONLY_BUTLER)


def butler_readwrite_dependency() -> Butler:
    _make_global_butler()
    return Butler(butler=GLOBAL_READWRITE_BUTLER)


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


@app.get("/butler/")
def read_root() -> str:
    return "Welcome to Excalibur... aka your Butler Server"


@app.get("/butler/butler.json", response_model=dict[str, Any])
def read_server_config() -> Mapping:
    """Return the butler configuration that the client should use."""
    config_str = f"""
datastore:
    root: {BUTLER_ROOT}
registry:
    cls: lsst.daf.butler.registries.remote.RemoteRegistry
    db: <butlerRoot>
"""
    config = Config.fromString(config_str, format="yaml")
    return config


@app.get("/butler/v1/universe", response_model=dict[str, Any])
def get_dimension_universe(butler: Butler = Depends(butler_readonly_dependency)) -> DimensionConfig:
    """Allow remote client to get dimensions definition."""
    return butler.registry.dimensions.dimensionConfig


@app.get("/butler/v1/uri/{id}", response_model=str)
def get_uri(id: DatasetId, butler: Butler = Depends(butler_readonly_dependency)) -> str:
    """Return a single URI of non-disassembled dataset."""
    ref = butler.registry.getDataset(id)
    if not ref:
        raise HTTPException(status_code=404, detail=f"Dataset with id {id} does not exist.")

    uri = butler.datastore.getURI(ref)

    # In reality would have to convert this to a signed URL
    return str(uri)


@app.put("/butler/v1/registry/refresh")
def refresh(butler: Butler = Depends(butler_readonly_dependency)) -> None:
    # Unclear whether this should exist. Which butler is really being
    # refreshed? How do we know the server we are refreshing is used later?
    # For testing at the moment it is important if a test adds a dataset type
    # directly in the server since the test client will not see it.
    butler.registry.refresh()


@app.get(
    "/butler/v1/registry/datasetType/{datasetTypeName}",
    summary="Retrieve this dataset type definition.",
    response_model=SerializedDatasetType,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def get_dataset_type(
    datasetTypeName: str, butler: Butler = Depends(butler_readonly_dependency)
) -> SerializedDatasetType:
    datasetType = butler.registry.getDatasetType(datasetTypeName)
    return datasetType.to_simple()


@app.get(
    "/butler/v1/registry/datasetTypes",
    summary="Retrieve all dataset type definitions.",
    response_model=list[SerializedDatasetType],
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def query_all_dataset_types(
    components: bool | None = Query(None), butler: Butler = Depends(butler_readonly_dependency)
) -> list[SerializedDatasetType]:
    datasetTypes = butler.registry.queryDatasetTypes(..., components=components)
    return [d.to_simple() for d in datasetTypes]


@app.get(
    "/butler/v1/registry/datasetTypes/re",
    summary="Retrieve dataset type definitions matching expressions",
    response_model=list[SerializedDatasetType],
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def query_dataset_types_re(
    regex: list[str] | None = Query(None),
    glob: list[str] | None = Query(None),
    components: bool | None = Query(None),
    butler: Butler = Depends(butler_readonly_dependency),
) -> list[SerializedDatasetType]:
    expression_params = ExpressionQueryParameter(regex=regex, glob=glob)

    datasetTypes = butler.registry.queryDatasetTypes(expression_params.expression(), components=components)
    return [d.to_simple() for d in datasetTypes]


@app.get("/butler/v1/registry/collection/chain/{parent:path}", response_model=list[str])
def get_collection_chain(parent: str, butler: Butler = Depends(butler_readonly_dependency)) -> list[str]:
    chain = butler.registry.getCollectionChain(parent)
    return list(chain)


@app.get("/butler/v1/registry/collections", response_model=list[str])
def query_collections(
    regex: list[str] | None = Query(None),
    glob: list[str] | None = Query(None),
    datasetType: str | None = Query(None),
    flattenChains: bool = Query(False),
    collectionType: list[CollectionTypeNames] | None = Query(None),
    includeChains: bool | None = Query(None),
    butler: Butler = Depends(butler_readonly_dependency),
) -> list[str]:
    expression_params = ExpressionQueryParameter(regex=regex, glob=glob)
    collectionTypes = CollectionType.from_names(collectionType)
    dataset_type = butler.registry.getDatasetType(datasetType) if datasetType else None

    collections = butler.registry.queryCollections(
        expression=expression_params.expression(),
        datasetType=dataset_type,
        collectionTypes=collectionTypes,
        flattenChains=flattenChains,
        includeChains=includeChains,
    )
    return list(collections)


@app.get("/butler/v1/registry/collection/type/{name:path}", response_model=str)
def get_collection_type(name: str, butler: Butler = Depends(butler_readonly_dependency)) -> str:
    collectionType = butler.registry.getCollectionType(name)
    return collectionType.name


@app.put("/butler/v1/registry/collection/{name:path}/{type_}", response_model=str)
def register_collection(
    name: str,
    collectionTypeName: CollectionTypeNames,
    doc: str | None = Query(None),
    butler: Butler = Depends(butler_readwrite_dependency),
) -> str:
    collectionType = CollectionType.from_name(collectionTypeName)
    butler.registry.registerCollection(name, collectionType, doc)

    # Need to refresh the global read only butler otherwise other clients
    # may not see this change.
    if GLOBAL_READONLY_BUTLER is not None:  # for mypy
        GLOBAL_READONLY_BUTLER.registry.refresh()

    return name


@app.get(
    "/butler/v1/registry/dataset/{id}",
    summary="Retrieve this dataset definition.",
    response_model=SerializedDatasetRef | None,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def get_dataset(
    id: DatasetId, butler: Butler = Depends(butler_readonly_dependency)
) -> SerializedDatasetRef | None:
    ref = butler.registry.getDataset(id)
    if ref is not None:
        return ref.to_simple()
    # This could raise a 404 since id is not found. The standard regsitry
    # getDataset method returns without error so follow that example here.
    return ref


@app.get("/butler/v1/registry/datasetLocations/{id}", response_model=list[str])
def get_dataset_locations(id: DatasetId, butler: Butler = Depends(butler_readonly_dependency)) -> list[str]:
    # Takes an ID so need to convert to a real DatasetRef
    fake_ref = SerializedDatasetRef(id=id)

    try:
        # Converting this to a real DatasetRef takes time and is not
        # needed internally since only the ID is used.
        ref = DatasetRef.from_simple(fake_ref, registry=butler.registry)
    except Exception:
        # SQL getDatasetLocations looks at ID in datastore and does not
        # check it is in registry. Follow that example and return without
        # error.
        return []

    return list(butler.registry.getDatasetLocations(ref))


# TimeSpan not yet a pydantic model
@app.post(
    "/butler/v1/registry/findDataset/{datasetType}",
    summary="Retrieve this dataset definition from collection, dataset type, and dataId",
    response_model=SerializedDatasetRef,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def find_dataset(
    datasetType: str,
    dataId: SerializedDataCoordinate | None = None,
    collections: list[str] | None = Query(None),
    butler: Butler = Depends(butler_readonly_dependency),
) -> SerializedDatasetRef | None:
    collection_query = collections if collections else None

    ref = butler.registry.findDataset(
        datasetType, dataId=unpack_dataId(butler, dataId), collections=collection_query
    )
    return ref.to_simple() if ref else None


# POST is used for the complex dict data structures
@app.post(
    "/butler/v1/registry/datasets",
    summary="Query all dataset holdings.",
    response_model=list[SerializedDatasetRef],
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def query_datasets(
    query: QueryDatasetsModel, butler: Butler = Depends(butler_readonly_dependency)
) -> list[SerializedDatasetRef]:
    # This method might return a lot of results

    if query.collections:
        collections = query.collections.expression()
    else:
        collections = None

    datasets = butler.registry.queryDatasets(
        query.datasetType.expression(),
        collections=collections,
        dimensions=query.dimensions,
        dataId=unpack_dataId(butler, query.dataId),
        where=query.where,
        findFirst=query.findFirst,
        components=query.components,
        bind=query.bind,
        check=query.check,
        **query.kwargs(),
    )
    return [ref.to_simple() for ref in datasets]


# POST is used for the complex dict data structures
@app.post(
    "/butler/v1/registry/dataIds",
    summary="Query all data IDs.",
    response_model=list[SerializedDataCoordinate],
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def query_data_ids(
    query: QueryDataIdsModel, butler: Butler = Depends(butler_readonly_dependency)
) -> list[SerializedDataCoordinate]:
    if query.datasets:
        datasets = query.datasets.expression()
    else:
        datasets = None
    if query.collections:
        collections = query.collections.expression()
    else:
        collections = None

    dataIds = butler.registry.queryDataIds(
        query.dimensions,
        collections=collections,
        datasets=datasets,
        dataId=unpack_dataId(butler, query.dataId),
        where=query.where,
        components=query.components,
        bind=query.bind,
        check=query.check,
        **query.kwargs(),
    )
    return [coord.to_simple() for coord in dataIds]


# Uses POST to handle the DataId
@app.post(
    "/butler/v1/registry/dimensionRecords/{element}",
    summary="Retrieve dimension records matching query",
    response_model=list[SerializedDimensionRecord],
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def query_dimension_records(
    element: str, query: QueryDimensionRecordsModel, butler: Butler = Depends(butler_readonly_dependency)
) -> list[SerializedDimensionRecord]:
    if query.datasets:
        datasets = query.datasets.expression()
    else:
        datasets = None
    if query.collections:
        collections = query.collections.expression()
    else:
        collections = None

    records = butler.registry.queryDimensionRecords(
        element,
        dataId=unpack_dataId(butler, query.dataId),
        collections=collections,
        where=query.where,
        datasets=datasets,
        components=query.components,
        bind=query.bind,
        check=query.check,
        **query.kwargs(),
    )
    return [r.to_simple() for r in records]
