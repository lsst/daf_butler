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
from abc import abstractmethod
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any, Generic, Self, TypeVar

from ..._butler import Butler
from ...dimensions import DataId
from ...queries import Query, QueryResultsBase
from .._registry import CollectionArgType
from ..wildcards import CollectionWildcard
from ._results import QueryResultsBase as LegacyQueryResultsBase


@dataclasses.dataclass(frozen=True)
class CommonQueryArguments:
    """Simplified version of the arguments passed to many ``Registry.query*``
    methods.
    """

    dataId: DataId | None
    dataset_types: list[str]
    collections: list[str] | None
    where: str
    bind: dict[str, Any] | None
    kwargs: dict[str, int | str]
    check: bool

    def replaceCollections(self, collections: list[str]) -> CommonQueryArguments:
        return dataclasses.replace(self, collections=collections)

    def replaceDatasetTypes(self, dataset_types: list[str]) -> CommonQueryArguments:
        return dataclasses.replace(self, dataset_types=dataset_types)


_T = TypeVar("_T", bound=QueryResultsBase)
_U = TypeVar("_U", bound=QueryResultsBase)


class LegacyQueryResultsMixin(Generic[_T], LegacyQueryResultsBase):
    """Implements common methods for the various ``QueryResults`` classes in
    the legacy query system by forwarding to the new query system.

    Parameters
    ----------
    butler : `Butler`
        Butler object used to execute queries.
    args : `CommonQueryArguments`
        User-facing arguments forwarded from the original ``registry.query*``
        method.
    """

    def __init__(
        self,
        butler: Butler,
        args: CommonQueryArguments,
    ) -> None:
        self._butler = butler
        self._args = args
        self._limit: int | None = None
        self._order_by: list[str] = []

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        with self._build_query() as result:
            return result.count(exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        with self._build_query() as result:
            return result.any(execute=execute, exact=exact)

    def order_by(self, *args: str) -> Self:
        self._order_by.extend(args)
        return self

    def limit(self, limit: int) -> Self:
        self._limit = limit
        return self

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        with self._build_query() as result:
            return result.explain_no_results(execute=execute)

    @contextmanager
    def _build_query(self) -> Iterator[_T]:
        with self._butler.query() as query:
            a = self._args
            for dataset_type in a.dataset_types:
                query = query.join_dataset_search(dataset_type, a.collections)
            if not a.check:
                query = query._skip_governor_validation()

            result = self._build_result(query)
            result = self._apply_result_modifiers(result)
            yield result

    def _apply_result_modifiers(self, result: _U) -> _U:
        a = self._args

        result = result.limit(self._limit)
        if self._order_by:
            result = result.order_by(*self._order_by)

        if a.where:
            result = result.where(a.where, bind=a.bind)
        if a.dataId or a.kwargs:
            id_list = [a.dataId] if a.dataId else []
            # dataId and kwargs have to be sent together as part of the
            # same call to where() so that the kwargs can override values
            # in the data ID.
            result = result.where(*id_list, **a.kwargs, bind=None)

        return result

    @abstractmethod
    def _build_result(self, query: Query) -> _T:
        raise NotImplementedError("Subclasses must implement _build_result")


def resolve_collections(
    butler: Butler, collections: CollectionArgType | None, doomed_by: list[str] | None = None
) -> list[str]:
    """Convert the collection argument used throughout the registry query
    methods to a concrete list of collections.

    Parameters
    ----------
    butler : `Butler`
        Butler object used to execute queries.
    collections : Any
        Any of the values that can be passed as the collections argument to
        Registry query methods.
    doomed_by : `list` [ `str` ]
        Diagnostic messages will be appended to this list.

    Returns
    -------
    collections : `list` [ `str` ]
        Concrete list of collection names to be used by the query.
    """
    if collections is None:
        return list(butler.collections.defaults)

    wildcard = CollectionWildcard.from_expression(collections)
    if wildcard.patterns:
        result = list(butler.registry.queryCollections(collections))
        if not result and doomed_by is not None:
            doomed_by.append(f"No collections found matching expression {wildcard}")
        return result
    else:
        return list(wildcard.strings)
