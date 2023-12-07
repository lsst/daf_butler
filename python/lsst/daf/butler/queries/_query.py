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

__all__ = ("RelationQuery",)

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from .._query import Query
from ..dimensions import DataCoordinate, DataId, DimensionGroup
from .data_coordinate_results import DataCoordinateResultSpec, RelationDataCoordinateQueryResults
from .driver import QueryDriver
from .relation_tree import Relation

if TYPE_CHECKING:
    from .._query_results import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults
    from ..registry._registry import CollectionArgType


class RelationQuery(Query):
    """Implementation of the Query interface backed by a relation tree and a
    `QueryDriver`.

    Parameters
    ----------
    driver : `QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `Relation`
        Description of the query as a tree of relation operations.  The
        instance returned directly by the `Butler._query` entry point should
        be constructed via `make_unit_relation`.
    include_dimension_records : `bool`
        Whether query result objects created from this query should be expanded
        to include dimension records.

    Notes
    -----
    Ideally this will eventually just be "Query", because we won't need an ABC
    if this is the only implementation.
    """

    def __init__(self, driver: QueryDriver, tree: Relation, include_dimension_records: bool):
        self._driver = driver
        self._tree = tree
        self._include_dimension_records = include_dimension_records

    def data_ids(
        self,
        dimensions: DimensionGroup | Iterable[str] | str,
        *,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> DataCoordinateQueryResults:
        # Docstring inherited.
        dimensions = self._driver.universe.conform(dimensions)
        data_id = DataCoordinate.standardize(data_id, universe=self._driver.universe, **kwargs)
        tree = self._tree
        if not dimensions >= self._tree.dimensions:
            raise NotImplementedError("TODO: push a DimensionJoin onto tree.")
        if data_id or where:
            raise NotImplementedError("TODO: push a Selection onto tree.")
        result_spec = DataCoordinateResultSpec(
            dimensions=dimensions, include_dimension_records=self._include_dimension_records
        )
        return RelationDataCoordinateQueryResults(tree, self._driver, result_spec)

    def datasets(
        self,
        dataset_type: Any,
        collections: CollectionArgType | None = None,
        *,
        find_first: bool = True,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> DatasetQueryResults:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    def dimension_records(
        self,
        element: str,
        *,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> DimensionRecordQueryResults:
        # Docstring inherited.
        raise NotImplementedError("TODO")

    # TODO: methods below are not part of the base Query, but they have
    # counterparts on at least some QueryResults objects.  We need to think
    # about which should be duplicated in Query and QueryResults, and which
    # should not, and get naming consistent.

    def with_dimension_records(self) -> RelationQuery:
        """Return a new Query that will always include dimension records in
        any `DataCoordinate` or `DatasetRef` results.
        """
        return RelationQuery(self._driver, self._tree, include_dimension_records=True)

    def count(self, *, exact: bool = True, discard: bool = False) -> int:
        """Return the number of rows this query would return.

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
        return self._driver.count(self._tree, exact=exact, discard=discard)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        """Test whether the query would return any rows.

        Parameters
        ----------
        tree : `Relation`
            Description of the query as a tree of relation operations.
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
        tree : `Relation`
            Description of the query as a tree of relation operations.
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

    def order_by(self, *args: str) -> Query:
        """Sort any results returned by this query.

        Parameters
        ----------
        *args : `str`
            Names of the columns/dimensions to use for ordering. Column name
            can be prefixed with minus (``-``) to use descending ordering.

        Returns
        -------
        result : `Query`
            A new query object whose results will be sorted.
        """
        raise NotImplementedError("Copy and push an OrderedSlice onto the tree.")

    def limit(self, limit: int | None = None, offset: int = 0) -> Query:
        """Limit the results returned by this query via positional slicing.

        Parameters
        ----------
        limit : `int` or `None`, optional
            Upper limit on the number of returned records.
        offset : `int`, optional
            The number of records to skip before returning at most ``limit``
            records.

        Returns
        -------
        result : `Query`
            A new query object whose results will be sorted.
        """
        raise NotImplementedError("Copy and push an OrderedSlice onto the tree.")

    # TODO: Materialize should probably go here instead of
    # DataCoordinateQueryResults, but the signature should probably change,
    # too, and that requires more thought.

    # TODO: Add many new query advanced-API methods that just push new relation
    # operations onto the tree while returning a copy.
