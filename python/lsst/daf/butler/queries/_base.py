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

__all__ = ("QueryBase", "QueryResultsBase")

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Set
from typing import Any, Self

from ..dimensions import DataId, DimensionGroup
from .convert_args import convert_order_by_args, convert_where_args
from .driver import QueryDriver
from .expression_factory import ExpressionProxy
from .tree import OrderExpression, Predicate, QueryTree


class QueryBase(ABC):
    """Common base class for `~lsst.daf.butler.queries.Query` and all
    ``QueryResult`` objects.

    This class should rarely be referenced directly; it is public only because
    it provides public methods to its subclasses.

    Parameters
    ----------
    driver : `~lsst.daf.butler.queries.driver.QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `~lsst.daf.butler.queries.tree.QueryTree`
        Description of the query as a tree of joins and column expressions.
    """

    def __init__(self, driver: QueryDriver, tree: QueryTree):
        self._driver = driver
        self._tree = tree

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
        return self._driver.any(self._tree, execute=execute, exact=exact)

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
        return self._driver.explain_no_results(self._tree, execute=execute)

    @abstractmethod
    def where(
        self,
        *args: str | Predicate | DataId,
        bind: Mapping[str, Any] | None = None,
        **kwargs: int | str,
    ) -> Self:
        """Return a query with a boolean-expression filter on its rows.

        Parameters
        ----------
        *args
            Constraints to apply, combined with logical AND.  Arguments may be
            `str` expressions to parse,
            `~lsst.daf.butler.queries.tree.Predicate` objects (these are
            typically constructed via
            `Query.expression_factory <lsst.daf.butler.queries.Query.expression_factory>`)
            or data IDs.
        bind : `~collections.abc.Mapping`
            Mapping from string identifier appearing in a string expression to
            a literal value that should be substituted for it.  This is
            recommended instead of embedding literals directly into the
            expression, especially for strings, timespans, or other types where
            quoting or formatting is nontrivial.
        **kwargs
            Data ID key value pairs that extend and override any present in
            ``*args``.

        Returns
        -------
        query : `QueryBase`
            A new query object with the given row filters (as well as any
            already present in ``self``).  All row filters are combined with
            logical AND.

        Notes
        -----
        Expressions referring to dimensions or dimension elements are resolved
        automatically. References to dataset fields (see `expression_factory`
        for the distinction) may or may not be resolvable, depending on the
        implementation class.

        Data ID values are not checked for consistency; they are extracted from
        ``args`` and then ``kwargs`` and combined, with later values overriding
        earlier ones.
        """  # noqa: W505, long docstrings
        raise NotImplementedError()


class QueryResultsBase(QueryBase):
    """Common base class for query result objects with countable rows."""

    @property
    @abstractmethod
    def dimensions(self) -> DimensionGroup:
        """All dimensions included in the query's columns."""
        raise NotImplementedError()

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

    def limit(self, limit: int | None = None) -> Self:
        """Return a new query that slices its result rows positionally.

        Parameters
        ----------
        limit : `int` or `None`, optional
            Upper limit on the number of returned records.  `None` (default)
            means no limit.

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
        return self._copy(self._tree, limit=limit)

    def where(
        self,
        *args: str | Predicate | DataId,
        bind: Mapping[str, Any] | None = None,
        **kwargs: int | str,
    ) -> Self:
        # Docstring inherited.
        return self._copy(
            tree=self._tree.where(
                convert_where_args(self.dimensions, self._get_datasets(), *args, bind=bind, **kwargs)
            ),
            driver=self._driver,
        )

    @abstractmethod
    def _get_datasets(self) -> Set[str]:
        """Return all dataset types included in the query's result rows."""
        raise NotImplementedError()

    @abstractmethod
    def _copy(self, tree: QueryTree, **kwargs: Any) -> Self:
        """Return a modified copy of ``self``.

        Implementations should validate modifications, not assume they are
        correct.
        """
        raise NotImplementedError()
