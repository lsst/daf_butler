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
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AbstractContextManager
from typing import Protocol, TypeVar

from fastapi.responses import StreamingResponse

from lsst.daf.butler.remote_butler.server_models import (
    QueryErrorResultModel,
    QueryExecuteResultData,
    QueryKeepAliveModel,
)

from ...._exceptions import ButlerUserError
from ..._errors import serialize_butler_user_error
from ._query_limits import QueryLimits

# Alias this function so we can mock it during unit tests.
_timeout = asyncio.timeout

_TContext = TypeVar("_TContext")

# Tracks active streaming queries.
_query_limits = QueryLimits()


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


async def execute_streaming_query(query: StreamingQuery, user: str | None) -> StreamingResponse:
    """Run a query, streaming the response incrementally, one page at a time,
    as newline-separated chunks of JSON.

    Parameters
    ----------
    query : ``StreamingQuery``
        Callers should define a class implementing the ``StreamingQuery``
        protocol to specify the inner logic that will be called during
        query execution.
    user : `str`, optional
        Name of user running the query -- used to enforce usage limits.

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
    await _query_limits.enforce_query_limits(user)

    output_generator = _stream_query_pages(query, user)
    return StreamingResponse(
        output_generator,
        media_type="application/jsonlines",
        headers={
            # Instruct the Kubernetes ingress to not buffer the response,
            # so that keep-alives reach the client promptly.
            "X-Accel-Buffering": "no"
        },
    )


async def _stream_query_pages(query: StreamingQuery, user: str | None) -> AsyncIterator[str]:
    """Stream the query output with one page object per line, as
    newline-delimited JSON records in the "JSON Lines" format
    (https://jsonlines.org/).

    When it takes longer than 15 seconds to get a response from the DB,
    sends a keep-alive message to prevent clients from timing out.
    """
    async with _query_limits.track_query(user):
        # `None` signals that there is no more data to send.
        queue = asyncio.Queue[QueryExecuteResultData | None](1)
        # Run a background task to read from the DB and insert the result
        # pages into a queue.
        task = asyncio.create_task(_enqueue_query_pages(queue, query))
        try:
            # Read the result pages from the queue and send them to the client,
            # inserting a keep-alive message every 15 seconds if we are waiting
            # a long time for the database.
            async for message in _dequeue_query_pages_with_keepalive(queue):
                yield message.model_dump_json() + "\n"
        finally:
            queue.shutdown()  # type: ignore[attr-defined]
            await task


async def _enqueue_query_pages(
    queue: asyncio.Queue[QueryExecuteResultData | None], query: StreamingQuery
) -> None:
    """Set up a QueryDriver to run the query, and copy the results into a
    queue.  Send `None` to the queue when there is no more data to read.
    """
    loop = asyncio.get_event_loop()

    def result_callback(page: QueryExecuteResultData) -> None:
        asyncio.run_coroutine_threadsafe(queue.put(page), loop).result()

    done_event = asyncio.Event()

    def done_callback() -> None:
        loop.call_soon_threadsafe(done_event.set)

    try:
        await asyncio.to_thread(_execute_query_sync, query, result_callback, done_callback)
        # Signal that there is no more data to read.
        await queue.put(None)
    finally:
        # Abort the database thread if cancellation or some other error occurs.
        queue.shutdown()  # type: ignore[attr-defined]
        # Wait for the thread to complete -- we don't want to release the
        # user's query limit until their query is actually done.
        # This is most relevant if they cancel a query that has a long startup
        # time (due to a bad ORDER BY clause or similar.)
        #
        # From the `await asyncio.to_thread` above, the thread is usually
        # finished before we get here -- but the `await` exits immediately
        # when cancellation occurs, without waiting for the thread.
        await done_event.wait()


def _execute_query_sync(
    query: StreamingQuery,
    result_callback: Callable[[QueryExecuteResultData], None],
    done_callback: Callable[[], None],
) -> None:
    # Postgres cursors are not thread safe.  Cursor setup/usage/teardown needs
    # to run in a single thread to ensure that cursors are always closed, and
    # that the closing doesn't happen until after we are done using the cursor.
    try:
        with query.setup() as ctx:
            for page in query.execute(ctx):
                result_callback(page)
    except ButlerUserError as e:
        # If a user-facing error occurs, serialize it and send it to the
        # client.
        result_callback(QueryErrorResultModel(error=serialize_butler_user_error(e)))
    except asyncio.QueueShutdown:  # type: ignore[attr-defined]
        pass
    finally:
        done_callback()


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
