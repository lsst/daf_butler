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

__all__ = ("QueryBase", "HomogeneousQueryBase", "CountableQueryBase", "QueryResultsBase")

from abc import ABC, abstractmethod
from collections.abc import Iterable, Set
from typing import Any, Self

from ..dimensions import DimensionGroup
from .convert_args import convert_order_by_args
from .driver import QueryDriver
from .expression_factory import ExpressionProxy
from .tree import ColumnSet, OrderExpression, QueryTree


class QueryBase(ABC):
    @abstractmethod
    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        """Test whether the query would return any rows.

        Parameters
        ----------
        execute : `bool`, optional
            If `True`, execute at least a ``LIMIT 1`` query if it cannot be
            determined prior to execution that the query would return no rows.
        exact : `bool`, optional
            If `True`, run the full query and perform post-query filtering if
            needed, until at least one result row is found.  If `False`, the
            returned result does not account for post-query filtering, and
            hence may be `True` even when all result rows would be filtered
            out.

        Returns
        -------
        any : `bool`
            `True` if the query would (or might, depending on arguments) yield
            result rows.  `False` if it definitely would not.
        """
        raise NotImplementedError()

    @abstractmethod
    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        """Return human-readable messages that may help explain why the query
        yields no results.

        Parameters
        ----------
        execute : `bool`, optional
            If `True` (default) execute simplified versions (e.g. ``LIMIT 1``)
            of aspects of the tree to more precisely determine where rows were
            filtered out.

        Returns
        -------
        messages : `~collections.abc.Iterable` [ `str` ]
            String messages that describe reasons the query might not yield any
            results.
        """
        raise NotImplementedError()


class HomogeneousQueryBase(QueryBase):
    def __init__(self, driver: QueryDriver, tree: QueryTree):
        self._driver = driver
        self._tree = tree

    @property
    def dimensions(self) -> DimensionGroup:
        """All dimensions included in the query's columns."""
        return self._tree.dimensions

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        return self._driver.any(self._tree, execute=execute, exact=exact)

    def explain_no_results(self, execute: bool = True) -> Iterable[str]:
        # Docstring inherited.
        return self._driver.explain_no_results(self._tree, execute=execute)


class CountableQueryBase(QueryBase):
    @abstractmethod
    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        """Count the number of rows this query would return.

        Parameters
        ----------
        exact : `bool`, optional
            If `True`, run the full query and perform post-query filtering if
            needed to account for that filtering in the count.  If `False`, the
            result may be an upper bound.
        discard : `bool`, optional
            If `True`, compute the exact count even if it would require running
            the full query and then throwing away the result rows after
            counting them.  If `False`, this is an error, as the user would
            usually be better off executing the query first to fetch its rows
            into a new query (or passing ``exact=False``).  Ignored if
            ``exact=False``.

        Returns
        -------
        count : `int`
            The number of rows the query would return, or an upper bound if
            ``exact=False``.
        """
        raise NotImplementedError()


class QueryResultsBase(HomogeneousQueryBase, CountableQueryBase):
    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        return self._driver.count(
            self._tree,
            self._get_result_columns(),
            exact=exact,
            discard=discard,
        )

    def order_by(self, *args: str | OrderExpression | ExpressionProxy) -> Self:
        """Return a new query that yields ordered results.

        Parameters
        ----------
        *args : `str`
            Names of the columns/dimensions to use for ordering. Column name
            can be prefixed with minus (``-``) to use descending ordering.

        Returns
        -------
        result : `QueryResultsBase`
            An ordered version of this query results object.

        Notes
        -----
        If this method is called multiple times, the new sort terms replace
        the old ones.
        """
        return self._copy(
            self._tree, order_by=convert_order_by_args(self.dimensions, self._get_datasets(), *args)
        )

    def limit(self, limit: int | None = None, offset: int = 0) -> Self:
        """Return a new query that slices its result rows positionally.

        Parameters
        ----------
        limit : `int` or `None`, optional
            Upper limit on the number of returned records.
        offset : `int`, optional
            The number of records to skip before returning at most ``limit``
            records.

        Returns
        -------
        result : `QueryResultsBase`
            A sliced version of this query results object.

        Notes
        -----
        If this method is called multiple times, the new slice parameters
        replace the old ones.  Slicing always occurs after sorting, even if
        `limit` is called before `order_by`.
        """
        return self._copy(self._tree, limit=limit, offset=offset)

    @abstractmethod
    def _get_result_columns(self) -> ColumnSet:
        raise NotImplementedError()

    @abstractmethod
    def _get_datasets(self) -> Set[str]:
        """Return all dataset types included in the query's result rows."""
        raise NotImplementedError()

    @abstractmethod
    def _copy(self, tree: QueryTree, **kwargs: Any) -> Self:
        """Return a modified copy of ``self``."""
        raise NotImplementedError()
