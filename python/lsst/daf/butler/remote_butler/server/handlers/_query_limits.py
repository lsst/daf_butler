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

from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from logging import getLogger

from fastapi import HTTPException

_LOG = getLogger(__name__)

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


class QueryLimits:
    """Tracks number of concurrent running queries, and applies limits."""

    def __init__(self) -> None:
        self._active_queries = 0
        self._active_users: Counter[str] = Counter()

    async def enforce_query_limits(self, user: str | None) -> None:
        if self._active_queries >= _MAXIMUM_CONCURRENT_STREAMING_QUERIES:
            await _block_retry_for_unit_test()
            raise HTTPException(
                status_code=503,  # service temporarily unavailable
                detail="The Butler Server is currently overloaded with requests.",
                headers={"retry-after": str(_QUERY_RETRY_SECONDS)},
            )

        # This is kind of a bad hack, but is better than nothing for now.  The
        # effect of this will be to limit users to 2 queries per server
        # replica, which is a non-deterministic but small number.
        # There will be some backpressure from the 429 responses as they
        # attempt to bounce to other replicas on retry.
        if user is not None and self._active_users[user] >= 2:
            _LOG.warning(f"User '{user}' is running many queries simultaneously.")
            raise HTTPException(
                status_code=429,  # too many requests
                detail=f"User {user} has too many queries running already."
                " Wait for your existing queries to complete, then try again.",
                headers={"retry-after": str(_QUERY_RETRY_SECONDS)},
            )

    @asynccontextmanager
    async def track_query(self, user: str | None) -> AsyncIterator[None]:
        try:
            self._active_queries += 1
            if user is not None:
                self._active_users[user] += 1
            await _block_query_for_unit_test()
            yield
        finally:
            if user is not None:
                self._active_users[user] -= 1
            self._active_queries -= 1


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
