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

import dataclasses
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any, TypeAlias

from ...dimensions import DataId, DataIdValue, DimensionElement, DimensionRecord
from ...queries import DimensionRecordQueryResults, Query
from ...registry.queries import DimensionRecordQueryResults as LegacyDimensionRecordQueryResults

QueryFactory: TypeAlias = Callable[[], AbstractContextManager[Query]]
"""Function signature matching the interface of ``Butler._query``.  Returns a
context manager that can be used to obtain a `Query` instance.
"""


@dataclasses.dataclass(frozen=True)
class DimensionRecordsQueryArguments:
    """Simplified version of the function arguments passed to
    ``Registry.queryDimensionRecords``.
    """

    element: DimensionElement
    dataId: DataId | None
    dataset_types: list[str]
    collections: list[str] | None
    where: str
    bind: dict[str, Any] | None
    kwargs: dict[str, DataIdValue]


class QueryDriverDimensionRecordQueryResults(LegacyDimensionRecordQueryResults):
    """Implementation of the legacy ``DimensionRecordQueryResults`` interface
    using the new query system.

    Parameters
    ----------
    query_factory : `QueryFactory`
        Function that can be called to access the new query system.
    args : `DimensionRecordsQueryArguments`
        User-facing arguments forwarded from
        ``registry.queryDimensionRecords``.
    """

    def __init__(
        self,
        query_factory: QueryFactory,
        args: DimensionRecordsQueryArguments,
    ) -> None:
        self._query_factory = query_factory
        self._args = args
        self._limit: int | None = None
        self._order_by: list[str] = []

    @property
    def element(self) -> DimensionElement:
        return self._args.element

    def __iter__(self) -> Iterator[DimensionRecord]:
        with self._build_query() as result:
            # We have to eagerly fetch the results to prevent
            # leaking the resources associated with QueryDriver.
            records = list(result)
        return iter(records)

    def run(self) -> LegacyDimensionRecordQueryResults:
        return self

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        with self._build_query() as result:
            return result.count(exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        with self._build_query() as result:
            return result.any(execute=execute, exact=exact)

    def order_by(self, *args: str) -> LegacyDimensionRecordQueryResults:
        self._order_by.extend(args)
        return self

    def limit(self, limit: int, offset: int | None = 0) -> LegacyDimensionRecordQueryResults:
        if offset is not None and offset != 0:
            raise NotImplementedError("Offset is no longer supported.")

        self._limit = limit

        return self

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        with self._build_query() as result:
            return result.explain_no_results(execute=execute)

    @contextmanager
    def _build_query(self) -> Iterator[DimensionRecordQueryResults]:
        with self._query_factory() as query:
            a = self._args
            for dataset_type in a.dataset_types:
                query = query.join_dataset_search(dataset_type, a.collections)
            if a.where:
                query = query.where(a.where, bind=a.bind)
            if a.dataId or a.kwargs:
                id_list = [a.dataId] if a.dataId else []
                # dataId and kwargs have to be sent together as part of the
                # same call to where() so that the kwargs can override values
                # in the data ID.
                query = query.where(*id_list, **a.kwargs, bind=None)

            result = query.dimension_records(a.element.name).limit(self._limit)
            if self._order_by:
                result = result.order_by(*self._order_by)
            yield result
