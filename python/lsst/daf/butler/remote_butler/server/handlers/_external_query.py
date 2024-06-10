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

from collections.abc import AsyncIterator, Iterable, Iterator
from contextlib import ExitStack, contextmanager
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
    QueryExecuteRequestModel,
    QueryExplainRequestModel,
    QueryExplainResponseModel,
    QueryInputs,
)

from ....queries.driver import QueryDriver, QueryTree, ResultPage, ResultSpec
from .._dependencies import factory_dependency
from .._factory import Factory
from ._query_serialization import serialize_query_pages

query_router = APIRouter()


@query_router.post("/v1/query/execute", summary="Query the Butler database and return full results")
def query_execute(
    request: QueryExecuteRequestModel, factory: Factory = Depends(factory_dependency)
) -> StreamingResponse:
    # Managing the lifetime of the query context object is a little tricky.  We
    # need to enter the context here, so that we can immediately deal with any
    # exceptions raised by query set-up.  We eventually transfer control to an
    # iterator consumed by FastAPI's StreamingResponse handler, which will
    # start iterating after this function returns.  So we use this ExitStack
    # instance to hand over the context manager to the iterator.
    with ExitStack() as exit_stack:
        ctx = exit_stack.enter_context(_get_query_context(factory, request.query))
        spec = request.result_spec.to_result_spec(ctx.driver.universe)
        response_pages = ctx.driver.execute(spec, ctx.tree)

        # We write the response incrementally, one page at a time, as
        # newline-separated chunks of JSON.  This allows clients to start
        # reading results earlier and prevents the server from exhausting
        # all its memory buffering rows from large queries.
        output_generator = _stream_query_pages(
            # Transfer control of the context manager to
            # _stream_query_pages.
            exit_stack.pop_all(),
            spec,
            response_pages,
        )
        return StreamingResponse(output_generator, media_type="application/jsonlines")

    # Mypy thinks that ExitStack might swallow an exception.
    assert False, "This line is unreachable."


async def _stream_query_pages(
    exit_stack: ExitStack, spec: ResultSpec, pages: Iterable[ResultPage]
) -> AsyncIterator[str]:
    # Instead of declaring this as a sync generator with 'def', it's async to
    # give us more control over the lifetime of exit_stack.  StreamingResponse
    # ensures that this async generator is cancelled if the client
    # disconnects or another error occurs, ensuring that clean-up logic runs.
    #
    # If it was sync, it would get wrapped in an async function internal to
    # FastAPI that does not guarantee that the generator is fully iterated or
    # closed.
    # (There is an example in the FastAPI docs showing StreamingResponse with a
    # sync generator with a context manager, but after reading the FastAPI
    # source code I believe that for sync generators it will leak the context
    # manager if the client disconnects, and that it would be
    # difficult/impossible for them to fix this in the general case within
    # FastAPI.)
    async with contextmanager_in_threadpool(exit_stack):
        async for chunk in iterate_in_threadpool(serialize_query_pages(spec, pages)):
            yield chunk


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
