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

__all__ = ("query_router",)

from collections.abc import Iterator
from contextlib import contextmanager
from typing import NamedTuple

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from lsst.daf.butler import Butler, DataCoordinate, DimensionGroup
from lsst.daf.butler.remote_butler.server_models import (
    DatasetRefResultModel,
    QueryAllDatasetsRequestModel,
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

from ...._query_all_datasets import QueryAllDatasetsParameters, query_all_datasets
from ....queries import Query
from ....queries.driver import QueryDriver, QueryTree
from .._dependencies import factory_dependency
from .._factory import Factory
from ._query_serialization import convert_query_page
from ._query_streaming import StreamingQuery, execute_streaming_query
from ._utils import set_default_data_id

query_router = APIRouter()


class _QueryContext(NamedTuple):
    driver: QueryDriver
    tree: QueryTree


class _StreamQueryDriverExecute(StreamingQuery[_QueryContext]):
    """Wrapper to call `QueryDriver.execute` from async stream handler."""

    def __init__(self, request: QueryExecuteRequestModel, factory: Factory) -> None:
        self._request = request
        self._factory = factory

    @contextmanager
    def setup(self) -> Iterator[_QueryContext]:
        with _get_query_context(self._factory, self._request.query) as context:
            yield context

    def execute(self, ctx: _QueryContext) -> Iterator[QueryExecuteResultData]:
        spec = self._request.result_spec.to_result_spec(ctx.driver.universe)
        pages = ctx.driver.execute(spec, ctx.tree)
        for page in pages:
            yield convert_query_page(spec, page)


@query_router.post("/v1/query/execute", summary="Query the Butler database and return full results")
async def query_execute(
    request: QueryExecuteRequestModel, factory: Factory = Depends(factory_dependency)
) -> StreamingResponse:
    query = _StreamQueryDriverExecute(request, factory)
    return await execute_streaming_query(query)


class _QueryAllDatasetsContext(NamedTuple):
    butler: Butler
    query: Query


class _StreamQueryAllDatasets(StreamingQuery[_QueryAllDatasetsContext]):
    def __init__(self, request: QueryAllDatasetsRequestModel, factory: Factory) -> None:
        self._request = request
        self._factory = factory

    @contextmanager
    def setup(self) -> Iterator[_QueryAllDatasetsContext]:
        butler = self._factory.create_butler()
        set_default_data_id(butler, self._request.default_data_id)
        with butler.query() as query:
            yield _QueryAllDatasetsContext(butler, query)

    def execute(self, ctx: _QueryAllDatasetsContext) -> Iterator[QueryExecuteResultData]:
        request = self._request
        bind = {k: v.get_literal_value() for k, v in request.bind.items()}
        args = QueryAllDatasetsParameters(
            collections=request.collections,
            name=request.name,
            find_first=request.find_first,
            data_id=request.data_id,
            where=request.where,
            bind=bind,
            limit=request.limit,
            with_dimension_records=request.with_dimension_records,
        )
        pages = query_all_datasets(ctx.butler, ctx.query, args)
        for page in pages:
            yield DatasetRefResultModel.from_refs(page.data)


@query_router.post(
    "/v1/query/all_datasets", summary="Query the Butler database across multiple dataset types."
)
async def query_all_datasets_execute(
    request: QueryAllDatasetsRequestModel, factory: Factory = Depends(factory_dependency)
) -> StreamingResponse:
    query = _StreamQueryAllDatasets(request, factory)
    return await execute_streaming_query(query)


@query_router.post(
    "/v1/query/count",
    summary="Query the Butler database and return a count of rows that would be returned.",
)
def query_count(
    request: QueryCountRequestModel, factory: Factory = Depends(factory_dependency)
) -> QueryCountResponseModel:
    with _get_query_context(factory, request.query) as ctx:
        spec = request.result_spec.to_result_spec(ctx.driver.universe)
        return QueryCountResponseModel(
            count=ctx.driver.count(ctx.tree, spec, exact=request.exact, discard=request.discard)
        )


@query_router.post(
    "/v1/query/any",
    summary="Determine whether any rows would be returned from a query of the Butler database.",
)
def query_any(
    request: QueryAnyRequestModel, factory: Factory = Depends(factory_dependency)
) -> QueryAnyResponseModel:
    with _get_query_context(factory, request.query) as ctx:
        return QueryAnyResponseModel(
            found_rows=ctx.driver.any(ctx.tree, execute=request.execute, exact=request.exact)
        )


@query_router.post(
    "/v1/query/explain",
    summary="Determine whether any rows would be returned from a query of the Butler database.",
)
def query_explain(
    request: QueryExplainRequestModel, factory: Factory = Depends(factory_dependency)
) -> QueryExplainResponseModel:
    with _get_query_context(factory, request.query) as ctx:
        return QueryExplainResponseModel(
            messages=ctx.driver.explain_no_results(ctx.tree, execute=request.execute)
        )


@contextmanager
def _get_query_context(factory: Factory, query: QueryInputs) -> Iterator[_QueryContext]:
    butler = factory.create_butler()
    tree = query.tree.to_query_tree(butler.dimensions)

    with butler._query_driver(
        default_collections=(),
        default_data_id=DataCoordinate.from_simple(query.default_data_id, universe=butler.dimensions),
    ) as driver:
        for input in query.additional_query_inputs:
            if input.type == "materialized":
                driver.materialize(
                    input.tree.to_query_tree(butler.dimensions),
                    DimensionGroup.from_simple(input.dimensions, butler.dimensions),
                    frozenset(input.datasets),
                    key=input.key,
                    allow_duplicate_overlaps=input.allow_duplicate_overlaps,
                )
            elif input.type == "upload":
                (
                    driver.upload_data_coordinates(
                        DimensionGroup.from_simple(input.dimensions, butler.dimensions),
                        [tuple(r) for r in input.rows],
                        key=input.key,
                    ),
                )

        yield _QueryContext(driver=driver, tree=tree)
