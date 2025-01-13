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

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Generic, TypeVar

__all__ = ["CachingContext"]

from ._collection_record_cache import CollectionRecordCache
from ._collection_summary_cache import CollectionSummaryCache


class CachingContext:
    """Collection of caches for various types of records retrieved from
    database.

    Notes
    -----
    Caching is usually disabled for most of the record types, but it can be
    explicitly and temporarily enabled in some context (e.g. quantum graph
    building) using Registry method. This class is a collection of cache
    instances which will be `None` when caching is disabled. Instance of this
    class is passed to the relevant managers that can use it to query or
    populate caches when caching is enabled.
    """

    def __init__(self) -> None:
        self._collection_records = _CacheToggle(CollectionRecordCache)
        self._collection_summaries = _CacheToggle(CollectionSummaryCache)

    def enable_collection_record_cache(self) -> AbstractContextManager[None]:
        """Enable the collection record cache.

        Notes
        -----
        When this cache is enabled, any changes made by other processes to
        collections in the database may not be visible.
        """
        return self._collection_records.enable()

    def enable_collection_summary_cache(self) -> AbstractContextManager[None]:
        """Enable the collection summary cache.

        Notes
        -----
        When this cache is enabled, changes made by other processes to
        collections in the database may not be visible.

        When the collection summary cache is enabled, the performance of
        database lookups for summaries changes.  Summaries will be aggressively
        fetched for all dataset types in the collections, which can cause
        significantly more rows to be returned than when the cache is disabled.
        This should only be enabled when you know that you will be doing many
        summary lookups for the same collections.
        """
        return self._collection_summaries.enable()

    @property
    def collection_records(self) -> CollectionRecordCache | None:
        """Cache for collection records (`CollectionRecordCache`)."""
        return self._collection_records.cache

    @property
    def collection_summaries(self) -> CollectionSummaryCache | None:
        """Cache for collection summary records (`CollectionSummaryCache`)."""
        return self._collection_summaries.cache


_T = TypeVar("_T")


class _CacheToggle(Generic[_T]):
    """Utility class to track nested enable/disable calls for a cache."""

    def __init__(self, enable_function: Callable[[], _T]):
        self.cache: _T | None = None
        self._enable_function = enable_function
        self._depth = 0

    @contextmanager
    def enable(self) -> Iterator[None]:
        """Context manager to enable the cache.  This context may be nested any
        number of times, and the cache will only be disabled once all callers
        have exited the context manager.
        """
        self._depth += 1
        try:
            if self._depth == 1:
                self.cache = self._enable_function()
            yield
        finally:
            self._depth -= 1
            if self._depth == 0:
                self.cache = None
