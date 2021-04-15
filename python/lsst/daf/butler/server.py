from __future__ import annotations
from typing import Optional, List, Dict, Any, Union
import re
import logging
from uuid import UUID
from collections.abc import Mapping

from pydantic import BaseModel

from fastapi import FastAPI, Query
from fastapi.middleware.gzip import GZipMiddleware

from lsst.daf.butler import (
    Butler,
    Config,
    SerializedDatasetType,
    SerializedDatasetRef,
    DatasetRef,
    DimensionConfig,
)
from lsst.daf.butler.core.serverModels import (
    ExpressionQueryParameter,
    QueryDatasetsModel,
)
from lsst.daf.butler.core.utils import globToRegex
from lsst.daf.butler.registry import CollectionType

BUTLER_ROOT = "ci_hsc_gen3/DATA"

COLLECTION_TYPE_RE = "^(" + "|".join(t.name for t in CollectionType.all()) + ")$"

log = logging.getLogger("excalibur")


class MaximalDataId(BaseModel):
    """Something that looks like a DataId but isn't."""
    detector: Optional[int] = None
    instrument: Optional[str] = None
    physical_filter: Optional[str] = None
    exposure: Optional[int] = None


class BindParams(BaseModel):
    """Container for generic dict."""
    data: Dict[str, Any]


app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.get("/")
def read_root():
    return "Welcome to Excalibur... aka your Butler Server"


@app.get("/butler.json")
def read_server_config() -> Mapping:
    """Return the butler configuration that the client should use."""
    config_str = f"""
datastore:
    root: {BUTLER_ROOT}
registry:
    cls: lsst.daf.butler.registry.RemoteRegistry
    db: <butlerRoot>
"""
    config = Config.fromString(config_str, format="yaml")
    return config


@app.get("/universe")
def get_dimension_universe() -> DimensionConfig:
    """Allow remote client to get dimensions definition."""
    butler = Butler(BUTLER_ROOT)
    return butler.registry.dimensions.dimensionConfig


@app.get("/uri/{id}")
def get_uri(id: Union[int, UUID]) -> str:
    """Return a single URI of non-disassembled dataset."""
    butler = Butler(BUTLER_ROOT)
    ref = butler.registry.getDataset(id)
    uri = butler.datastore.getURI(ref)

    # In reality would have to convert this to a signed URL
    return str(uri)


@app.get(
    "/registry/datasetType/{datasetTypeName}",
    summary="Retrieve this dataset type definition.",
    response_model=SerializedDatasetType,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def get_dataset_type(datasetTypeName: str) -> SerializedDatasetType:
    butler = Butler(BUTLER_ROOT)
    datasetType = butler.registry.getDatasetType(datasetTypeName)
    return datasetType.to_simple()


@app.get(
    "/registry/datasetTypes",
    summary="Retrieve all dataset type definitions.",
    response_model=List[SerializedDatasetType],
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def query_all_dataset_types(components: Optional[bool] = None) -> List[SerializedDatasetType]:
    butler = Butler(BUTLER_ROOT)
    datasetTypes = butler.registry.queryDatasetTypes(..., components=components)
    return [d.to_simple() for d in datasetTypes]


@app.get(
    "/registry/datasetTypes/re",
    summary="Retrieve dataset type definitions matching expressions",
    response_model=List[SerializedDatasetType],
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def query_dataset_types_re(regex: Optional[List[str]] = Query(None),
                           glob: Optional[List[str]] = Query(None),
                           components: Optional[bool] = None) -> List[SerializedDatasetType]:
    butler = Butler(BUTLER_ROOT)
    expression_params = ExpressionQueryParameter(regex=regex, glob=glob)

    datasetTypes = butler.registry.queryDatasetTypes(expression_params.expression(),
                                                     components=components)
    return [d.to_simple() for d in datasetTypes]


@app.get("/registry/collections")
def query_collections(expression: Optional[str] = None,
                      datasetType: Optional[str] = None,
                      flattenChains: Optional[bool] = False,
                      collectionType: Optional[List[str]] = Query(None, regex=COLLECTION_TYPE_RE),
                      includeChains: Optional[bool] = None) -> List[str]:
    if expression is None:
        expression = ...
    else:
        expression = globToRegex([expression])
    if collectionType is None:
        collectionTypes = CollectionType.all()
    else:
        # Convert to real collection types
        collectionTypes = set()
        for item in collectionType:
            item = item.upper()
            try:
                collectionTypes.add(CollectionType.__members__[item])
            except KeyError:
                raise KeyError(f"Collection type of {item} not known to Butler.")

    butler = Butler(BUTLER_ROOT)
    collections = butler.registry.queryCollections(expression=expression,
                                                   datasetType=datasetType,
                                                   collectionTypes=collectionTypes,
                                                   flattenChains=flattenChains,
                                                   includeChains=includeChains)
    return list(collections)


@app.get("/registry/collection/type/{name:path}")
def get_collection_type(name: str):
    butler = Butler(BUTLER_ROOT)
    collectionType = butler.registry.getCollectionType(name)
    return collectionType.name


@app.put("/registry/collection/{name:path}/{type_}")
def register_collection(name: str, type_: str, doc: Optional[str] = None) -> str:
    type_ = type_.upper()
    try:
        collectionType = CollectionType.__members__[type_]
    except KeyError:
        raise KeyError(f"Collection type of {type_} not known to Butler.")

    butler = Butler(BUTLER_ROOT, writeable=True)
    butler.registry.registerCollection(name, collectionType, doc)

    return name


@app.get(
    "/registry/dataset/{id}",
    summary="Retrieve this dataset definition.",
    response_model=Optional[SerializedDatasetRef],
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def get_dataset(id: Union[int, UUID]) -> Optional[SerializedDatasetRef]:
    butler = Butler(BUTLER_ROOT)
    ref = butler.registry.getDataset(id)
    if ref is not None:
        return ref.to_simple()
    return ref


@app.get("/registry/datasetLocations/{id}")
def get_dataset_locations(id: Union[int, UUID]) -> List[str]:
    butler = Butler(BUTLER_ROOT)
    ref = SerializedDatasetRef(id=id)
    datastores = butler.registry.getDatasetLocations(DatasetRef.from_simple(ref,
                                                                            registry=butler.registry))
    return datastores


# TimeSpan not yet a pydantic model
@app.post(
    "/registry/findDataset/{datasetType}",
    summary="Retrieve this dataset definition from collection, dataset type, and dataId",
    response_model=SerializedDatasetRef,
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def find_dataset(datasetType: str,
                 dataId: Optional[MaximalDataId] = None,
                 collections: Optional[List[str]] = Query(None),
                 ) -> SerializedDatasetRef:
    butler = Butler(BUTLER_ROOT)
    if collections is None:
        collections = ...

    ref = butler.registry.findDataset(datasetType,
                                      dataId=dataId,
                                      collections=collections)
    return ref.to_simple()


# POST is used for the complex dict data structures
@app.post(
    "/registry/datasets",
    summary="Query all dataset holdings.",
    response_model=List[SerializedDatasetRef],
    response_model_exclude_unset=True,
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)
def query_datasets(query: QueryDatasetsModel) -> List[SerializedDatasetRef]:
    # This method might return a lot of results
    butler = Butler(BUTLER_ROOT)

    if query.collections:
        collections = query.collections.expression()
    if query.kwargs:
        kwargs = query.kwargs
    else:
        kwargs = {}

    datasets = butler.registry.queryDatasets(query.datasetType.expression(),
                                             collections=collections,
                                             dimensions=query.dimensions,
                                             dataId=query.dataId,
                                             where=query.where,
                                             findFirst=query.findFirst,
                                             components=query.components,
                                             bind=query.bind,
                                             check=query.check, **kwargs)
    return (ref.to_simple() for ref in datasets)


# POST is used for the complex dict data structures
@app.post(
    "/registry/dataIds",
    summary="Query all data IDs.",
)
def query_data_ids(dimensions: List[str],
                   collections: Optional[List[str]] = Query(None),
                   datasets: Optional[List[str]] = Query(None),
                   dataId: Optional[MaximalDataId] = None,
                   where: Optional[str] = None,
                   components: Optional[bool] = None,
                   bind: Optional[Dict[str, Any]] = None,
                   check: bool = True) -> List[Dict]:
    # Can use "*" dataset type to match everything.
    # This will take a long time.
    butler = Butler(BUTLER_ROOT)

    if collections is None:
        collections = ...

    filteredDataId = {k: v for k, v in dataId.dict().items() if v is not None}

    dataIds = butler.registry.queryDataIds(dimensions,
                                           collections=collections,
                                           datasets=datasets,
                                           dataId=filteredDataId,
                                           where=where,
                                           components=components,
                                           bind=bind,
                                           check=check)
    return (coord.to_simple() for coord in dataIds)


# Uses POST to handle the DataId
@app.post("/registry/dimensionRecords/{element}")
def query_dimension_records(element: str,
                            collections: Optional[List[str]] = Query(None),
                            datasets: Optional[List[str]] = Query(None),
                            where: Optional[str] = None,
                            components: Optional[bool] = None,
                            check: bool = True,
                            dataId: Optional[MaximalDataId] = None,
                            bind: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    butler = Butler(BUTLER_ROOT)

    if collections is None:
        collections = ...

    filteredDataId = {k: v for k, v in dataId.dict().items() if v is not None}

    records = butler.registry.queryDimensionRecords(element, dataId=filteredDataId,
                                                    collections=collections,
                                                    where=where,
                                                    datasets=datasets,
                                                    components=components,
                                                    bind=bind,
                                                    check=check)
    return [r.to_simple() for r in records]
