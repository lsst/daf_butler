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

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from typing import NamedTuple

from fastapi import APIRouter, Depends
from fastapi.concurrency import contextmanager_in_threadpool, iterate_in_threadpool
from fastapi.responses import StreamingResponse
from lsst.daf.butler import DataCoordinate, DimensionGroup
from lsst.daf.butler.remote_butler.server_models import (
    QueryAnyRequestModel,
    QueryAnyResponseModel,
    QueryCountRequestModel,
    QueryCountResponseModel,
    QueryErrorResultModel,
    QueryExecuteRequestModel,
    QueryExecuteResultData,
    QueryExplainRequestModel,
    QueryExplainResponseModel,
    QueryInputs,
    QueryKeepAliveModel,
)

from ...._exceptions import ButlerUserError
from ....queries.driver import QueryDriver, QueryTree, ResultSpec
from ..._errors import serialize_butler_user_error
from .._dependencies import factory_dependency
from .._factory import Factory
from ._query_serialization import convert_query_page

query_router = APIRouter()

# Alias this function so we can mock it during unit tests.
_timeout = asyncio.timeout


@query_router.post("/v1/query/execute", summary="Query the Butler database and return full results")
async def query_execute(
    request: QueryExecuteRequestModel, factory: Factory = Depends(factory_dependency)
) -> StreamingResponse:
    # We write the response incrementally, one page at a time, as
    # newline-separated chunks of JSON.  This allows clients to start
    # reading results earlier and prevents the server from exhausting
    # all its memory buffering rows from large queries.
    output_generator = _stream_query_pages(request, factory)
    return StreamingResponse(
        output_generator,
        media_type="application/jsonlines",
        headers={
            # Instruct the Kubernetes ingress to not buffer the response,
            # so that keep-alives reach the client promptly.
            "X-Accel-Buffering": "no"
        },
    )


async def _stream_query_pages(request: QueryExecuteRequestModel, factory: Factory) -> AsyncIterator[str]:
    """Stream the query output with one page object per line, as
    newline-delimited JSON records in the "JSON Lines" format
    (https://jsonlines.org/).

    When it takes longer than 15 seconds to get a response from the DB,
    sends a keep-alive message to prevent clients from timing out.
    """
    # `None` signals that there is no more data to send.
    queue = asyncio.Queue[QueryExecuteResultData | None](1)
    async with asyncio.TaskGroup() as tg:
        # Run a background task to read from the DB and insert the result pages
        # into a queue.
        tg.create_task(_enqueue_query_pages(queue, request, factory))
        # Read the result pages from the queue and send them to the client,
        # inserting a keep-alive message every 15 seconds if we are waiting a
        # long time for the database.
        async for message in _dequeue_query_pages_with_keepalive(queue):
            yield message.model_dump_json() + "\n"


async def _enqueue_query_pages(
    queue: asyncio.Queue[QueryExecuteResultData | None], request: QueryExecuteRequestModel, factory: Factory
) -> None:
    """Set up a QueryDriver to run the query, and copy the results into a
    queue.  Send `None` to the queue when there is no more data to read.
    """
    try:
        async with contextmanager_in_threadpool(_get_query_context(factory, request.query)) as ctx:
            spec = request.result_spec.to_result_spec(ctx.driver.universe)
            async for page in iterate_in_threadpool(_retrieve_query_pages(ctx, spec)):
                await queue.put(page)
    except ButlerUserError as e:
        # If a user-facing error occurs, serialize it and send it to the
        # client.
        await queue.put(QueryErrorResultModel(error=serialize_butler_user_error(e)))

    # Signal that there is no more data to read.
    await queue.put(None)


def _retrieve_query_pages(ctx: _QueryContext, spec: ResultSpec) -> Iterator[QueryExecuteResultData]:
    """Execute the database query and and return pages of results."""
    pages = ctx.driver.execute(spec, ctx.tree)
    for page in pages:
        yield convert_query_page(spec, page)


async def _dequeue_query_pages_with_keepalive(
    queue: asyncio.Queue[QueryExecuteResultData | None],
) -> AsyncIterator[QueryExecuteResultData]:
    """Read and return messages from the given queue until the end-of-stream
    message `None` is reached.  If the producer is taking a long time, returns
    a keep-alive message every 15 seconds while we are waiting.
    """
    while True:
        try:
            async with _timeout(15):
                message = await queue.get()
                if message is None:
                    return
                yield message
        except TimeoutError:
            yield QueryKeepAliveModel()


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
                )
            elif input.type == "upload":
                driver.upload_data_coordinates(
                    DimensionGroup.from_simple(input.dimensions, butler.dimensions),
                    [tuple(r) for r in input.rows],
                    key=input.key,
                ),

        yield _QueryContext(driver=driver, tree=tree)


class _QueryContext(NamedTuple):
    driver: QueryDriver
    tree: QueryTree
