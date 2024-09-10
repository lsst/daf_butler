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

"""Models used for client/server communication."""

__all__ = [
    "CLIENT_REQUEST_ID_HEADER_NAME",
    "DatasetTypeName",
    "FindDatasetRequestModel",
    "FindDatasetResponseModel",
    "GetFileResponseModel",
    "GetCollectionInfoResponseModel",
    "GetCollectionSummaryResponseModel",
]

from typing import Annotated, Any, Literal, NewType, TypeAlias
from uuid import UUID

import pydantic
from lsst.daf.butler import (
    CollectionInfo,
    CollectionType,
    DataIdValue,
    SerializedDataCoordinate,
    SerializedDataId,
    SerializedDatasetRef,
    SerializedDatasetType,
    SerializedDimensionGroup,
    Timespan,
)
from lsst.daf.butler.datastores.fileDatastoreClient import FileDatastoreGetPayload
from lsst.daf.butler.registry import SerializedCollectionSummary

from ..dimensions import SerializedDimensionConfig, SerializedDimensionRecord
from ..queries.result_specs import SerializedResultSpec
from ..queries.tree import SerializedQueryTree

CLIENT_REQUEST_ID_HEADER_NAME = "X-Butler-Client-Request-Id"
ERROR_STATUS_CODE = 422

CollectionList = NewType("CollectionList", list[str])
"""A list of search patterns for collection names.  May use glob
syntax to specify wildcards."""
DatasetTypeName = NewType("DatasetTypeName", str)


class FindDatasetRequestModel(pydantic.BaseModel):
    """Request model for find_dataset."""

    dataset_type: DatasetTypeName
    data_id: SerializedDataId
    default_data_id: SerializedDataId = pydantic.Field(default_factory=dict)
    """Data ID values used as a fallback if required values are not specified
    in ``data_id``.
    """
    collections: CollectionList
    timespan: Timespan | None
    dimension_records: bool = False


class FindDatasetResponseModel(pydantic.BaseModel):
    """Response model for ``find_dataset`` and ``get_dataset``."""

    dataset_ref: SerializedDatasetRef | None


class GetDatasetTypeResponseModel(pydantic.BaseModel):
    """Response model for ``dataset_type``."""

    dataset_type: SerializedDatasetType


class GetUniverseResponseModel(pydantic.BaseModel):
    """Response model for ``universe``."""

    universe: SerializedDimensionConfig


class GetFileByDataIdRequestModel(pydantic.BaseModel):
    """Request model for ``get_file_by_data_id``."""

    dataset_type: DatasetTypeName
    data_id: SerializedDataId
    default_data_id: SerializedDataId = pydantic.Field(default_factory=dict)
    """Data ID values used as a fallback if required values are not specified
    in ``data_id``.
    """
    collections: CollectionList
    timespan: Timespan | None = None


class GetFileResponseModel(pydantic.BaseModel):
    """Response model for get_file and get_file_by_data_id."""

    dataset_ref: SerializedDatasetRef
    artifact: FileDatastoreGetPayload | None
    """The data needed to retrieve and use an artifact. If this is `None`, that
    means this dataset is known to the Butler but the associated files are no
    longer available ("known to registry but not known to datastore".)

    An example of a situation where this would be `None` is a per-visit image
    that is an intermediate file in the processing pipelines.  It is deleted to
    save space, but the fact that it was once available must be recorded for
    provenance tracking.
    """


class ErrorResponseModel(pydantic.BaseModel):
    """Error response sent with a 422 status code, to propagate server
    exceptions with user-facing error messages to the client.
    """

    error_type: str
    """The ``error_type`` string from one of the subclasses of
    `ButlerUserError`.
    """
    detail: str
    """Detailed explanation of the error that will be sent to the client."""


# TODO DM-46204: This can be removed once the RSP recommended image has been
# upgraded to a version that contains DM-46129.
class GetCollectionInfoResponseModel(pydantic.BaseModel):
    """Response model for get_collection_info."""

    name: str
    type: CollectionType
    children: list[str]
    doc: str | None = None
    """Will be `None` unless requested with ``include_doc=True`` query
    parameter."""
    parents: set[str] | None = None
    """Chained collections that directly contain this collection. Will be
    `None` unless requested with ``include_parents=True`` query parameter."""


class GetCollectionSummaryResponseModel(pydantic.BaseModel):
    """Response model for get_collection_summary."""

    summary: SerializedCollectionSummary


class ExpandDataIdRequestModel(pydantic.BaseModel):
    """Request model for expand_data_id."""

    data_id: SerializedDataId


class ExpandDataIdResponseModel(pydantic.BaseModel):
    """Response model for expand_data_id."""

    data_coordinate: SerializedDataCoordinate


# TODO DM-46204: This can be removed once the RSP recommended image has been
# upgraded to a version that contains DM-46129.
class QueryCollectionsRequestModel(pydantic.BaseModel):
    """Request model for query_collections."""

    search: CollectionList
    collection_types: list[CollectionType]
    flatten_chains: bool
    include_chains: bool


# TODO DM-46204: This can be removed once the RSP recommended image has been
# upgraded to a version that contains DM-46129.
class QueryCollectionsResponseModel(pydantic.BaseModel):
    """Response model for query_collections."""

    collections: list[str]
    """Collection names that match the search."""


class QueryCollectionInfoRequestModel(pydantic.BaseModel):
    """Request model for query_collection_info."""

    expression: CollectionList
    collection_types: list[CollectionType]
    flatten_chains: bool
    include_chains: bool
    include_parents: bool
    include_summary: bool
    include_doc: bool
    summary_datasets: list[DatasetTypeName] | None


class QueryCollectionInfoResponseModel(pydantic.BaseModel):
    """Response model for query_collection_info."""

    collections: list[CollectionInfo]


class QueryDatasetTypesRequestModel(pydantic.BaseModel):
    """Request model for queryDatasetTypes."""

    search: list[str]
    """List of glob patterns to match against the name of the dataset types."""


class QueryDatasetTypesResponseModel(pydantic.BaseModel):
    """Response model for query_collections."""

    dataset_types: list[SerializedDatasetType]
    """Dataset types that match the search."""
    missing: list[str]
    """Non-wildcard dataset type names included in the search that are not
    known to the server.
    """


class MaterializedQuery(pydantic.BaseModel):
    """Captures the parameters from a call to ``QueryDriver.materialize``."""

    type: Literal["materialized"] = "materialized"
    key: UUID
    tree: SerializedQueryTree
    dimensions: SerializedDimensionGroup
    datasets: list[str]


class DataCoordinateUpload(pydantic.BaseModel):
    """Captures the parameters from a call to
    ``QueryDriver.upload_data_coordinates``.
    """

    type: Literal["upload"] = "upload"
    key: UUID
    dimensions: SerializedDimensionGroup
    rows: list[list[DataIdValue]]


AdditionalQueryInput: TypeAlias = Annotated[
    MaterializedQuery | DataCoordinateUpload, pydantic.Discriminator("type")
]
"""Information about additional data tables that may be used by a query."""


class QueryInputs(pydantic.BaseModel):
    """Serialized Butler query with additional context needed to execute it."""

    tree: SerializedQueryTree
    default_data_id: SerializedDataCoordinate
    additional_query_inputs: list[AdditionalQueryInput]


class QueryExecuteRequestModel(pydantic.BaseModel):
    """Request model for /query/execute/."""

    query: QueryInputs
    result_spec: SerializedResultSpec


class DataCoordinateResultModel(pydantic.BaseModel):
    """Result model for /query/execute/ when user requested DataCoordinate
    results.
    """

    type: Literal["data_coordinate"] = "data_coordinate"
    rows: list[SerializedDataCoordinate]


class DimensionRecordsResultModel(pydantic.BaseModel):
    """Result model for /query/execute/ when user requested DimensionRecord
    results.
    """

    type: Literal["dimension_record"] = "dimension_record"
    rows: list[SerializedDimensionRecord]


class DatasetRefResultModel(pydantic.BaseModel):
    """Result model for /query/execute/ when user requested DatasetRef
    results.
    """

    type: Literal["dataset_ref"] = "dataset_ref"
    rows: list[SerializedDatasetRef]


class GeneralResultModel(pydantic.BaseModel):
    """Result model for /query/execute/ when user requested general results."""

    type: Literal["general"] = "general"
    rows: list[tuple[Any, ...]]


class QueryErrorResultModel(pydantic.BaseModel):
    """Result model for /query/execute when an error occurs part-way through
    returning rows.

    Because we are streaming results, the HTTP status code has already been
    sent before the error occurs.  So this provides a way to signal an error
    in-band with the results.
    """

    # (One example of this type of error is a CalibrationLookupError returned
    # by query row postprocessing.)

    type: Literal["error"] = "error"
    error: ErrorResponseModel


class QueryKeepAliveModel(pydantic.BaseModel):
    """Result model for /query/execute used to keep connection alive.

    Some queries require a significant start-up time before they can start
    returning results, or a long processing time for each chunk of rows.  This
    message signals that the server is still fetching the data.
    """

    type: Literal["keep-alive"] = "keep-alive"


QueryExecuteResultData: TypeAlias = Annotated[
    DataCoordinateResultModel
    | DimensionRecordsResultModel
    | DatasetRefResultModel
    | GeneralResultModel
    | QueryErrorResultModel
    | QueryKeepAliveModel,
    pydantic.Field(discriminator="type"),
]


class QueryCountRequestModel(pydantic.BaseModel):
    """Request model for /query/count/."""

    query: QueryInputs
    result_spec: SerializedResultSpec
    exact: bool
    discard: bool


class QueryCountResponseModel(pydantic.BaseModel):
    """Response model for /query/count/."""

    count: int


class QueryAnyRequestModel(pydantic.BaseModel):
    """Request model for /query/any/."""

    query: QueryInputs
    execute: bool
    exact: bool


class QueryAnyResponseModel(pydantic.BaseModel):
    """Response model for /query/any/."""

    found_rows: bool


class QueryExplainRequestModel(pydantic.BaseModel):
    """Request model for /query/explain/."""

    query: QueryInputs
    execute: bool


class QueryExplainResponseModel(pydantic.BaseModel):
    """Response model for /query/explain/."""

    messages: list[str]
