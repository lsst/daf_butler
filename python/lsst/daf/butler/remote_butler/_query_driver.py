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

__all__ = ("RemoteQueryDriver",)


from collections.abc import Iterable
from typing import Any, overload

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
from ._http_connection import RemoteButlerHttpConnection, parse_model
from ._remote_butler import RemoteButler
from .server_models import (
    QueryCountRequestModel,
    QueryCountResponseModel,
    QueryExecuteRequestModel,
    QueryExecuteResponseModel,
)


class RemoteQueryDriver(QueryDriver):
    def __init__(self, butler: RemoteButler, connection: RemoteButlerHttpConnection):
        self._butler = butler
        self._connection = connection

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
            tree=SerializedQueryTree(tree), result_spec=SerializedResultSpec(result_spec)
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
        raise NotImplementedError()

    def upload_data_coordinates(
        self, dimensions: DimensionGroup, rows: Iterable[tuple[DataIdValue, ...]]
    ) -> DataCoordinateUploadKey:
        raise NotImplementedError()

    def count(
        self,
        tree: QueryTree,
        result_spec: ResultSpec,
        *,
        exact: bool,
        discard: bool,
    ) -> int:
        request = QueryCountRequestModel(
            tree=SerializedQueryTree(tree),
            result_spec=SerializedResultSpec(result_spec),
            exact=exact,
            discard=discard,
        )
        response = self._connection.post("query/count", request)
        result = parse_model(response, QueryCountResponseModel)
        return result.count

    def any(self, tree: QueryTree, *, execute: bool, exact: bool) -> bool:
        raise NotImplementedError()

    def explain_no_results(self, tree: QueryTree, execute: bool) -> Iterable[str]:
        raise NotImplementedError()

    def get_default_collections(self) -> tuple[str, ...]:
        return tuple(self._butler.collections)

    def get_dataset_type(self, name: str) -> DatasetType:
        return self._butler.get_dataset_type(name)
