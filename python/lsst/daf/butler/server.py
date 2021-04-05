from __future__ import annotations
from typing import Optional, List, Dict, Any, Union
import logging
from uuid import UUID

from pydantic import BaseModel

from fastapi import FastAPI, Query
from fastapi.middleware.gzip import GZipMiddleware

from lsst.daf.butler import Butler, SerializedDatasetType, SerializedDatasetRef, DatasetRef
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
def read_server_config():
    """Return the butler configuration that the client should use."""
    # This is a pretend one for now
    return {"datastore": {"cls": "lsst.daf.butler.datastores.fileDatastore.FileDatastore"}}


@app.get("/universe.json")
def read_dimension_universe():
    """Allow remote client to get dimensions definition."""
    return {"dimension_universe": "goes here"}


@app.get("/uri/{id}")
def get_uri(id: Union[int, UUID]) -> str:
    """Return a single URI of non-disassembled dataset."""
    butler = Butler(BUTLER_ROOT)
    ref = butler.registry.getDataset(id)
    uri = butler.datastore.getURI(ref)

    # In reality would have to convert this to a signed URL
    return str(uri)


@app.get("/registry/datasetTypes")
def query_dataset_types(expression: Optional[str] = None,
                        components: Optional[bool] = None) -> List[SerializedDatasetType]:
    butler = Butler(BUTLER_ROOT)
    if expression is None:
        expression = ...
    else:
        expression = globToRegex([expression])
    datasetTypes = butler.registry.queryDatasetTypes(expression, components=components)
    datasetTypes = list(datasetTypes)
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


@app.get("/registry/dataset/{id}")
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
@app.post("/registry/findDataset/{datasetType}")
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
@app.post("/registry/datasets/{datasetType}")
def query_datasets(datasetType: str,
                   collections: Optional[List[str]] = Query(None),
                   dimensions: Optional[List[str]] = Query(None),
                   dataId: Optional[MaximalDataId] = None,
                   where: Optional[str] = None,
                   findFirst: bool = False,
                   components: Optional[bool] = None,
                   bind: Optional[Dict[str, Any]] = None,
                   check: bool = True) -> List[SerializedDatasetRef]:
    # Can use "*" dataset type to match everything.
    # This will take a long time.
    butler = Butler(BUTLER_ROOT)

    if collections is None:
        collections = ...

    filteredDataId = {k: v for k, v in dataId.dict().items() if v is not None}

    datasetType = globToRegex([datasetType])

    datasets = butler.registry.queryDatasets(datasetType,
                                             collections=collections,
                                             dimensions=dimensions,
                                             dataId=filteredDataId,
                                             where=where,
                                             findFirst=findFirst,
                                             components=components,
                                             bind=bind,
                                             check=check)
    return (ref.to_simple() for ref in datasets)


# POST is used for the complex dict data structures
@app.post("/registry/dataIds")
def query_data_ids(dimensions: List[str],
                   collections: Optional[List[str]] = Query(None),
                   datasets: Optional[List[str]] = Query(None),
                   dataId: Optional[MaximalDataId] = None,
                   where: Optional[str] = None,
                   components: Optional[bool] = None,
                   bind: Optional[Dict[str, Any]] = None,
                   check: bool = True) -> List[SerializedDatasetRef]:
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
