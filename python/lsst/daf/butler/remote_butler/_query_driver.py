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

from uuid import uuid4

__all__ = ("RemoteQueryDriver",)


from collections.abc import Iterable, Iterator
from contextlib import ExitStack
from typing import Any, Literal, overload

import httpx
from pydantic import TypeAdapter

from ...butler import Butler
from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType
from ..dimensions import DataCoordinate, DataIdValue, DimensionGroup, DimensionRecord, DimensionUniverse
from ..queries.driver import (
    DataCoordinateResultPage,
    DatasetRefResultPage,
    DimensionRecordResultPage,
    GeneralResultPage,
    QueryDriver,
    ResultPage,
)
from ..queries.result_specs import (
    DataCoordinateResultSpec,
    DatasetRefResultSpec,
    DimensionRecordResultSpec,
    GeneralResultSpec,
    ResultSpec,
    SerializedResultSpec,
)
from ..queries.tree import DataCoordinateUploadKey, MaterializationKey, QueryTree, SerializedQueryTree
from ..registry import NoDefaultCollectionError
from ._errors import deserialize_butler_user_error
from ._http_connection import RemoteButlerHttpConnection, parse_model
from .server_models import (
    AdditionalQueryInput,
    DataCoordinateUpload,
    GeneralResultModel,
    MaterializedQuery,
    QueryAnyRequestModel,
    QueryAnyResponseModel,
    QueryCountRequestModel,
    QueryCountResponseModel,
    QueryExecuteRequestModel,
    QueryExecuteResultData,
    QueryExplainRequestModel,
    QueryExplainResponseModel,
    QueryInputs,
)

_QueryResultTypeAdapter = TypeAdapter[QueryExecuteResultData](QueryExecuteResultData)


class RemoteQueryDriver(QueryDriver):
    """Implementation of QueryDriver for client/server Butler.

    Parameters
    ----------
    butler : `Butler`
        Butler instance that will use this QueryDriver.
    connection : `RemoteButlerHttpConnection`
        HTTP connection used to send queries to Butler server.
    """

    def __init__(self, butler: Butler, connection: RemoteButlerHttpConnection):
        self._butler = butler
        self._connection = connection
        self._stored_query_inputs: list[AdditionalQueryInput] = []
        self._pending_queries: set[httpx.Response] = set()
        self._closed = False

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> Literal[False]:
        self._closed = True
        # Clean up any queries that the user didn't finish iterating. The exit
        # stack helps handle any exceptions that may be thrown during cleanup.
        stack = ExitStack().__enter__()
        for pending in self._pending_queries:
            stack.callback(pending.close)
        self._pending_queries = set()
        stack.__exit__(exc_type, exc_value, traceback)
        return False

    @property
    def universe(self) -> DimensionUniverse:
        return self._butler.dimensions

    @overload
    def execute(
        self, result_spec: DataCoordinateResultSpec, tree: QueryTree
    ) -> Iterator[DataCoordinateResultPage]: ...

    @overload
    def execute(
        self, result_spec: DimensionRecordResultSpec, tree: QueryTree
    ) -> Iterator[DimensionRecordResultPage]: ...

    @overload
    def execute(
        self, result_spec: DatasetRefResultSpec, tree: QueryTree
    ) -> Iterator[DatasetRefResultPage]: ...

    @overload
    def execute(self, result_spec: GeneralResultSpec, tree: QueryTree) -> Iterator[GeneralResultPage]: ...

    def execute(self, result_spec: ResultSpec, tree: QueryTree) -> Iterator[ResultPage]:
        if self._closed:
            raise RuntimeError("Cannot execute query: query context has been closed")

        request = QueryExecuteRequestModel(
            query=self._create_query_input(tree), result_spec=SerializedResultSpec(result_spec)
        )
        universe = self.universe
        with self._connection.post_with_stream_response("query/execute", request) as response:
            self._pending_queries.add(response)
            try:
                # There is one result page JSON object per line of the
                # response.
                for line in response.iter_lines():
                    result_chunk: QueryExecuteResultData = _QueryResultTypeAdapter.validate_json(line)
                    if result_chunk.type == "keep-alive":
                        _received_keep_alive()
                    else:
                        yield _convert_query_result_page(result_spec, result_chunk, universe)
                    if self._closed:
                        raise RuntimeError(
                            "Cannot continue query result iteration: query context has been closed"
                        )
            finally:
                self._pending_queries.discard(response)

    def materialize(
        self,
        tree: QueryTree,
        dimensions: DimensionGroup,
        datasets: frozenset[str],
    ) -> MaterializationKey:
        key = uuid4()
        self._stored_query_inputs.append(
            MaterializedQuery(
                key=key,
                tree=SerializedQueryTree(tree.model_copy(deep=True)),
                dimensions=dimensions.to_simple(),
                datasets=datasets,
            ),
        )
        return key

    def upload_data_coordinates(
        self, dimensions: DimensionGroup, rows: Iterable[tuple[DataIdValue, ...]]
    ) -> DataCoordinateUploadKey:
        key = uuid4()
        self._stored_query_inputs.append(
            DataCoordinateUpload(key=key, dimensions=dimensions.to_simple(), rows=list(rows))
        )
        return key

    def count(
        self,
        tree: QueryTree,
        result_spec: ResultSpec,
        *,
        exact: bool,
        discard: bool,
    ) -> int:
        request = QueryCountRequestModel(
            query=self._create_query_input(tree),
            result_spec=SerializedResultSpec(result_spec),
            exact=exact,
            discard=discard,
        )
        response = self._connection.post("query/count", request)
        result = parse_model(response, QueryCountResponseModel)
        return result.count

    def any(self, tree: QueryTree, *, execute: bool, exact: bool) -> bool:
        request = QueryAnyRequestModel(
            query=self._create_query_input(tree),
            exact=exact,
            execute=execute,
        )
        response = self._connection.post("query/any", request)
        result = parse_model(response, QueryAnyResponseModel)
        return result.found_rows

    def explain_no_results(self, tree: QueryTree, execute: bool) -> Iterable[str]:
        request = QueryExplainRequestModel(
            query=self._create_query_input(tree),
            execute=execute,
        )
        response = self._connection.post("query/explain", request)
        result = parse_model(response, QueryExplainResponseModel)
        return result.messages

    def get_default_collections(self) -> tuple[str, ...]:
        collections = tuple(self._butler.collections.defaults)
        if not collections:
            raise NoDefaultCollectionError("No collections provided and no default collections.")
        return collections

    def get_dataset_type(self, name: str) -> DatasetType:
        return self._butler.get_dataset_type(name)

    def _create_query_input(self, tree: QueryTree) -> QueryInputs:
        return QueryInputs(
            tree=SerializedQueryTree(tree),
            default_data_id=self._butler.registry.defaults.dataId.to_simple(),
            additional_query_inputs=self._stored_query_inputs,
        )


def _convert_query_result_page(
    result_spec: ResultSpec, result: QueryExecuteResultData, universe: DimensionUniverse
) -> ResultPage:
    if result.type == "error":
        # A server-side exception occurred part-way through generating results.
        raise deserialize_butler_user_error(result.error)

    if result_spec.result_type == "dimension_record":
        assert result.type == "dimension_record"
        return DimensionRecordResultPage(
            spec=result_spec,
            rows=[DimensionRecord.from_simple(r, universe) for r in result.rows],
        )
    elif result_spec.result_type == "data_coordinate":
        assert result.type == "data_coordinate"
        return DataCoordinateResultPage(
            spec=result_spec,
            rows=[DataCoordinate.from_simple(r, universe) for r in result.rows],
        )
    elif result_spec.result_type == "dataset_ref":
        assert result.type == "dataset_ref"
        return DatasetRefResultPage(
            spec=result_spec,
            rows=[DatasetRef.from_simple(r, universe) for r in result.rows],
        )
    elif result_spec.result_type == "general":
        assert result.type == "general"
        return _convert_general_result(result_spec, result)
    else:
        raise NotImplementedError(f"Unhandled result type {result_spec.result_type}")


def _convert_general_result(spec: GeneralResultSpec, model: GeneralResultModel) -> GeneralResultPage:
    """Convert GeneralResultModel to a general result page."""
    columns = spec.get_result_columns()
    serializers = [
        columns.get_column_spec(column.logical_table, column.field).serializer() for column in columns
    ]
    rows = [
        tuple(serializer.deserialize(value) for value, serializer in zip(row, serializers))
        for row in model.rows
    ]
    return GeneralResultPage(spec=spec, rows=rows)


def _received_keep_alive() -> None:
    """Do nothing.  Gives a place for unit tests to hook in for testing
    keepalive behavior.
    """
    pass
