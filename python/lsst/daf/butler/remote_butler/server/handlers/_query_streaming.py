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

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import AbstractContextManager
from typing import Protocol, TypeVar

from fastapi import HTTPException
from fastapi.concurrency import contextmanager_in_threadpool, iterate_in_threadpool
from fastapi.responses import StreamingResponse

from lsst.daf.butler.remote_butler.server_models import (
    QueryErrorResultModel,
    QueryExecuteResultData,
    QueryKeepAliveModel,
)

from ...._exceptions import ButlerUserError
from ..._errors import serialize_butler_user_error

# Restrict the maximum number of streaming queries that can be running
# simultaneously, to prevent the database connection pool and the thread pool
# from being tied up indefinitely.  Beyond this number, the server will return
# an HTTP 503 Service Unavailable with a Retry-After header.  We are currently
# using the default FastAPI thread pool size of 40 (total) and have 40 maximum
# database connections (per Butler repository.)
_MAXIMUM_CONCURRENT_STREAMING_QUERIES = 25
# How long we ask callers to wait before trying their query again.
# The hope is that they will bounce to a less busy replica, so we don't want
# them to wait too long.
_QUERY_RETRY_SECONDS = 5

# Alias this function so we can mock it during unit tests.
_timeout = asyncio.timeout

_TContext = TypeVar("_TContext")

# Count of active streaming queries.
_current_streaming_queries = 0


class StreamingQuery(Protocol[_TContext]):
    """Interface for queries that can return streaming results."""

    def setup(self) -> AbstractContextManager[_TContext]:
        """Context manager that sets up any resources used to execute the
        query.
        """

    def execute(self, context: _TContext) -> Iterator[QueryExecuteResultData]:
        """Execute the database query and and return pages of results.

        Parameters
        ----------
        context : generic
            Value returned by the call to ``setup()``.
        """


async def execute_streaming_query(query: StreamingQuery) -> StreamingResponse:
    """Run a query, streaming the response incrementally, one page at a time,
    as newline-separated chunks of JSON.

    Parameters
    ----------
    query : ``StreamingQuery``
        Callers should define a class implementing the ``StreamingQuery``
        protocol to specify the inner logic that will be called during
        query execution.

    Returns
    -------
    response : `fastapi.StreamingResponse`
        FastAPI streaming response that can be returned by a route handler.

    Notes
    -----
    - Streaming the response allows clients to start reading results earlier
      and prevents the server from exhausting all its memory buffering rows
      from large queries.
    - If the query is taking a long time to execute, we insert a keepalive
      message in the JSON stream every 15 seconds.
    - If the caller closes the HTTP connection, async cancellation is
      triggered by FastAPI. The query will be cancelled after the next page is
      read -- ``StreamingQuery.execute()`` cannot be interrupted while it is
      in the middle of reading a page.
    """
    # Prevent an excessive number of streaming queries from jamming up the
    # thread pool and database connection pool.  We can't change the response
    # code after starting the StreamingResponse, so we enforce this here.
    #
    # This creates a small chance that more than the expected number of
    # streaming queries will be started, but there is no guarantee that the
    # StreamingResponse generator function will ever be called, so we can't
    # guarantee that we release the slot if we reserve one here.
    if _current_streaming_queries >= _MAXIMUM_CONCURRENT_STREAMING_QUERIES:
        await _block_retry_for_unit_test()
        raise HTTPException(
            status_code=503,  # service temporarily unavailable
            detail="The Butler Server is currently overloaded with requests.",
            headers={"retry-after": str(_QUERY_RETRY_SECONDS)},
        )

    output_generator = _stream_query_pages(query)
    return StreamingResponse(
        output_generator,
        media_type="application/jsonlines",
        headers={
            # Instruct the Kubernetes ingress to not buffer the response,
            # so that keep-alives reach the client promptly.
            "X-Accel-Buffering": "no"
        },
    )


async def _stream_query_pages(query: StreamingQuery) -> AsyncIterator[str]:
    """Stream the query output with one page object per line, as
    newline-delimited JSON records in the "JSON Lines" format
    (https://jsonlines.org/).

    When it takes longer than 15 seconds to get a response from the DB,
    sends a keep-alive message to prevent clients from timing out.
    """
    global _current_streaming_queries
    try:
        _current_streaming_queries += 1
        await _block_query_for_unit_test()

        # `None` signals that there is no more data to send.
        queue = asyncio.Queue[QueryExecuteResultData | None](1)
        async with asyncio.TaskGroup() as tg:
            # Run a background task to read from the DB and insert the result
            # pages into a queue.
            tg.create_task(_enqueue_query_pages(queue, query))
            # Read the result pages from the queue and send them to the client,
            # inserting a keep-alive message every 15 seconds if we are waiting
            # a long time for the database.
            async for message in _dequeue_query_pages_with_keepalive(queue):
                yield message.model_dump_json() + "\n"
    finally:
        _current_streaming_queries -= 1


async def _enqueue_query_pages(
    queue: asyncio.Queue[QueryExecuteResultData | None], query: StreamingQuery
) -> None:
    """Set up a QueryDriver to run the query, and copy the results into a
    queue.  Send `None` to the queue when there is no more data to read.
    """
    try:
        async with contextmanager_in_threadpool(query.setup()) as ctx:
            async for page in iterate_in_threadpool(query.execute(ctx)):
                await queue.put(page)
    except ButlerUserError as e:
        # If a user-facing error occurs, serialize it and send it to the
        # client.
        await queue.put(QueryErrorResultModel(error=serialize_butler_user_error(e)))

    # Signal that there is no more data to read.
    await queue.put(None)


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


async def _block_retry_for_unit_test() -> None:
    """Will be overridden during unit tests to block the server,
    in order to verify retry logic.
    """
    pass


async def _block_query_for_unit_test() -> None:
    """Will be overridden during unit tests to block the server,
    in order to verify maximum concurrency logic.
    """
    pass
