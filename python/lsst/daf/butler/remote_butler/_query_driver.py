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


from collections.abc import Iterable
from typing import Any, overload

from ...butler import Butler
from .._dataset_type import DatasetType
from ..dimensions import DataIdValue, DimensionGroup, DimensionRecord, DimensionUniverse
from ..queries.driver import (
    DataCoordinateResultPage,
    DatasetRefResultPage,
    DimensionRecordResultPage,
    GeneralResultPage,
    PageKey,
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
from ._http_connection import RemoteButlerHttpConnection, parse_model
from .server_models import (
    AdditionalQueryInput,
    DataCoordinateUpload,
    MaterializedQuery,
    QueryAnyRequestModel,
    QueryAnyResponseModel,
    QueryCountRequestModel,
    QueryCountResponseModel,
    QueryExecuteRequestModel,
    QueryExecuteResponseModel,
    QueryExplainRequestModel,
    QueryExplainResponseModel,
    QueryInputs,
)


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

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        pass

    @property
    def universe(self) -> DimensionUniverse:
        return self._butler.dimensions

    @overload
    def execute(self, result_spec: DataCoordinateResultSpec, tree: QueryTree) -> DataCoordinateResultPage: ...

    @overload
    def execute(
        self, result_spec: DimensionRecordResultSpec, tree: QueryTree
    ) -> DimensionRecordResultPage: ...

    @overload
    def execute(self, result_spec: DatasetRefResultSpec, tree: QueryTree) -> DatasetRefResultPage: ...

    @overload
    def execute(self, result_spec: GeneralResultSpec, tree: QueryTree) -> GeneralResultPage: ...

    def execute(self, result_spec: ResultSpec, tree: QueryTree) -> ResultPage:
        request = QueryExecuteRequestModel(
            query=self._create_query_input(tree), result_spec=SerializedResultSpec(result_spec)
        )
        response = self._connection.post("query/execute", request)
        result = parse_model(response, QueryExecuteResponseModel)
        if result_spec.result_type != "dimension_record":
            raise NotImplementedError()
        universe = self.universe
        return DimensionRecordResultPage(
            spec=result_spec,
            next_key=None,
            rows=[DimensionRecord.from_simple(r, universe=universe) for r in result.rows],
        )

    @overload
    def fetch_next_page(
        self, result_spec: DataCoordinateResultSpec, key: PageKey
    ) -> DataCoordinateResultPage: ...

    @overload
    def fetch_next_page(
        self, result_spec: DimensionRecordResultSpec, key: PageKey
    ) -> DimensionRecordResultPage: ...

    @overload
    def fetch_next_page(self, result_spec: DatasetRefResultSpec, key: PageKey) -> DatasetRefResultPage: ...

    @overload
    def fetch_next_page(self, result_spec: GeneralResultSpec, key: PageKey) -> GeneralResultPage: ...

    def fetch_next_page(self, result_spec: ResultSpec, key: PageKey) -> ResultPage:
        raise NotImplementedError()

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
        collections = tuple(self._butler.collections)
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
