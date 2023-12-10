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
from ..dimensions import DataCoordinate, DataId, DataIdValue, DimensionGroup
from .data_coordinate_results import DataCoordinateResultSpec, RelationDataCoordinateQueryResults
from .driver import QueryDriver
from .expression_factory import ExpressionFactory, ExpressionProxy
from .relation_tree import (
    DataCoordinateUpload,
    DatasetSearch,
    InvalidRelationError,
    Materialization,
    OrderExpression,
    Predicate,
    RootRelation,
    make_dimension_relation,
    make_unit_relation,
)
from .relation_tree.joins import JoinArg

if TYPE_CHECKING:
    from .._query_results import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults
    from ..registry import CollectionArgType


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

    def __init__(self, driver: QueryDriver, tree: RootRelation, include_dimension_records: bool):
        self._driver = driver
        self._tree = tree
        self._include_dimension_records = include_dimension_records

    @property
    def expression_factory(self) -> ExpressionFactory:
        """A factory for column expressions using overloaded operators.

        Notes
        -----
        Typically this attribute will be assigned to a single-character local
        variable, and then its (dynamic) attributes can be used to obtain
        references to columns that can be included in a query::

            with butler._query() as query:
                x = query.expression_factory
                query = query.where(
                    x.instrument == "LSSTCam",
                    x.visit.day_obs > 20240701,
                    x.any(x.band == 'u', x.band == 'y'),
                )

        As shown above, the returned object also has an `any` method to create
        combine expressions with logical OR (as well as `not_` and `all`,
        though the latter is rarely necessary since `where` already combines
        its arguments with AND).

        Proxies for fields associated with dataset types (``dataset_id``,
        ``ingest_date``, ``run``, ``collection``, as well as ``timespan`` for
        `~CollectionType.CALIBRATION` collection searches) can be obtained with
        dict-like access instead::

            with butler._query() as query:
                query = query.order_by(x["raw"].ingest_date)

        Expression proxy objects that correspond to scalar columns overload the
        standard comparison operators (``==``, ``!=``, ``<``, ``>``, ``<=``,
        ``>=``) and provide `~ScalarExpressionProxy.in_range`,
        `~ScalarExpressionProxy.in_iterable`, and
        `~ScalarExpressionProxy.in_query` methods for membership tests.  For
        `order_by` contexts, they also have a `~ScalarExpressionProxy.desc`
        property to indicate that the sort order for that expression should be
        reversed.

        Proxy objects for region and timespan fields have an `overlaps` method,
        and timespans also have `~TimespanProxy.begin` and `~TimespanProxy.end`
        properties to access scalar expression proxies for the bounds.

        All proxy objects also have a `~ExpressionProxy.is_null` property.

        Literal values can be created by calling `ExpressionFactory.literal`,
        but can almost always be created implicitly via overloaded operators
        instead.
        """
        return ExpressionFactory(self._driver.universe)

    def data_ids(
        self,
        dimensions: DimensionGroup | Iterable[str] | str,
        *,
        data_id: DataId | None = None,
        where: str | Predicate = "",
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> DataCoordinateQueryResults:
        # Docstring inherited.
        dimensions = self._driver.universe.conform(dimensions)
        data_id = DataCoordinate.standardize(data_id, universe=self._driver.universe, **kwargs)
        tree = self._tree
        if not dimensions >= self._tree.dimensions:
            tree = tree.join(make_dimension_relation(dimensions))
        if data_id or where:
            tree = tree.where(*self._convert_predicate_args(where, data_id, bind=bind, **kwargs))
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

    # TODO: add general, dict-row results method and QueryResults.

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

    def order_by(self, *args: str | OrderExpression | ExpressionProxy) -> RelationQuery:
        """Return a new query that sorts any results returned.

        Parameters
        ----------
        *args : `str` or `OrderExpression`
            Column names or expression objects to use for ordering. Names can
            be prefixed with minus (``-``) to use descending ordering.

        Returns
        -------
        query : `Query`
            A new query object whose results will be sorted.

        Notes
        -----
        Multiple `order_by` calls are combined; this::

            q.order_by(*a).order_by(*b)

        is equivalent to this::

            q.order_by(*(b + a))

        Note that this is consistent with sorting first by ``a`` and then by
        ``b``.

        If an expression references a dimension or dimension element that is
        not already present in the query, it will be joined in, but dataset
        searches must already be joined into a query in order to reference
        their fields in expressions.
        """
        return RelationQuery(
            tree=self._tree.order_by(*self._convert_order_by_args(*args)),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def limit(self, limit: int | None = None, offset: int = 0) -> RelationQuery:
        """Return a new query with results limited by positional slicing.

        Parameters
        ----------
        limit : `int` or `None`, optional
            Upper limit on the number of returned records.
        offset : `int`, optional
            The number of records to skip before returning at most ``limit``
            records.

        Returns
        -------
        query : `Query`
            A new query object whose results will be sliced.

        Notes
        -----
        Multiple `limit` calls are combined, with ``offset`` summed and the
        minimum ``limit``.
        """
        return RelationQuery(
            tree=self._tree.order_by(limit=limit, offset=offset),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def materialize(self, *, dataset_types: Iterable[str] | None = ()) -> RelationQuery:
        """Execute the query, save its results to a temporary location, and
        return a new query that represents fetching or joining against those
        saved results.

        Parameters
        ----------
        dataset_types : `~collections.abc.Iterable` [ `str` ], optional
            Names of dataset types whose ID fields (at least) should be
            included in the temporary results; default is to include all
            datasets types whose ID columns are currently available to the
            query.  Dataset searches over multiple collections are not
            resolved, but enough information is preserved to resolve them
            downstream of the materialization.

        Returns
        -------
        query : `Query`
            A new query object whose that represents the materialized rows.
        """
        if dataset_types is None:
            dataset_types = self._tree.available_dataset_types
        else:
            dataset_types = frozenset(dataset_types)
        key = self._driver.materialize(self._tree, dataset_types)
        return RelationQuery(
            self._driver,
            tree=make_unit_relation(self._driver.universe).join(
                Materialization.model_construct(key=key, operand=self._tree, dataset_types=dataset_types)
            ),
            include_dimension_records=self._include_dimension_records,
        )

    def join_dataset_search(
        self,
        dataset_type: str,
        collections: Iterable[str],
    ) -> RelationQuery:
        """Return a new query with a search for a dataset joined in.

        Parameters
        ----------
        dataset_type : `str`
            Name of the dataset type.  May not refer to a dataset component.
        collections : `~collections.abc.Iterable` [ `str` ]
            Iterable of collections to search.  Order is preserved, but will
            not matter if the dataset search is only used as a constraint on
            dimensions or if ``find_first=False`` when requesting results.

        Returns
        -------
        query : `Query`
            A new query object with dataset columns available and rows
            restricted to those consistent with the found data IDs.
        """
        return RelationQuery(
            tree=self._tree.join(
                DatasetSearch.model_construct(
                    dataset_type=dataset_type,
                    collections=tuple(collections),
                    dimensions=self._driver.get_dataset_dimensions(dataset_type),
                )
            ),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def join_data_coordinates(self, iterable: Iterable[DataCoordinate]) -> RelationQuery:
        """Return a new query that joins in an explicit table of data IDs.

        Parameters
        ----------
        iterable : `~collections.abc.Iterable` [ `DataCoordinate` ]
            Iterable of `DataCoordinate`.  All items must have the same
            dimensions.  Must have at least one item.

        Returns
        -------
        query : `Query`
            A new query object with the data IDs joined in.
        """
        rows: set[tuple[DataIdValue, ...]] = set()
        dimensions: DimensionGroup | None = None
        for data_coordinate in iterable:
            if dimensions is None:
                dimensions = data_coordinate.dimensions
            elif dimensions != data_coordinate.dimensions:
                raise RuntimeError(f"Inconsistent dimensions: {dimensions} != {data_coordinate.dimensions}.")
            rows.add(data_coordinate.required_values)
        if dimensions is None:
            raise RuntimeError("Cannot upload an empty data coordinate set.")
        key = self._driver.upload_data_coordinates(dimensions, rows)
        return RelationQuery(
            tree=self._tree.join(DataCoordinateUpload(dimensions=dimensions, key=key)),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def join_dimensions(self, dimensions: Iterable[str] | DimensionGroup) -> RelationQuery:
        """Return a new query that joins the logical tables for additional
        dimensions.

        Parameters
        ----------
        iterable : `~collections.abc.Iterable` [ `str` ] or `DimensionGroup`
            Names of dimensions to join in.

        Returns
        -------
        query : `Query`
            A new query object with the dimensions joined in.
        """
        dimensions = self._driver.universe.conform(dimensions)
        return RelationQuery(
            tree=self._tree.join(make_dimension_relation(dimensions)),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def joined_on(self, *, spatial: JoinArg = frozenset(), temporal: JoinArg = frozenset()) -> RelationQuery:
        """Return a new query with new spatial or temporal join constraints.

        Parameters
        ----------
        spatial : `tuple` [ `str`, `str` ] or `~collections.abc.Iterable` \
                [ `tuple [ `str`, `str` ], optional
            A pair or pairs of dimension element names whose regions must
            overlap.
        temporal : `tuple` [ `str`, `str` ] or `~collections.abc.Iterable` \
                [ `tuple [ `str`, `str` ], optional
            A pair or pairs of dimension element names and/or calibration
            dataset type names whose timespans must overlap.  Datasets in
            collections other than `~CollectionType.CALIBRATION` collections
            are associated with an unbounded timespan.

        Returns
        -------
        query : `Query`
            A new query object with the given join criteria as well as any
            join criteria already present in ``self``.

        Notes
        -----
        Implicit spatial and temporal joins are also added to queries (when
        they are actually executed) when an explicit join (of the sort added by
        this method) is absent between any pair of spatial/temporal "families"
        present in the query, and these implicit joins use the most
        fine-grained overlap possible.  For example, in a query with dimensions
        ``{instrument, visit, tract, patch}``, the implicit spatial join would
        be between ``visit`` and ``patch``, but an explicit join between
        ``visit`` and `tract`` would override it, because "tract" and "patch"
        are in the same family (as are ``visit`` and ``visit_detector_region``,
        but the latter is not used here because it is not present in the
        example query dimensions).  For "skypix" dimensions like ``healpixN``
        or `htmN`, all levels in the same system are in the same family.
        """
        return RelationQuery(
            tree=self._tree.joined_on(spatial=spatial, temporal=temporal),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def where(
        self,
        *args: str | Predicate | DataId,
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> RelationQuery:
        """Return a query with a boolean-expression filter on its rows.

        Parameters
        ----------
        *args
            Constraints to apply, combined with logical AND.  Arguments may be
            `str` expressions to parse, `Predicate` objects (these are
            typically constructed via `expression_factory`) or data IDs.
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
        query : `Query`
            A new query object with the given row filters as well as any
            already present in ``self`` (combined with logical AND).

        Notes
        -----
        If an expression references a dimension or dimension element that is
        not already present in the query, it will be joined in, but dataset
        searches must already be joined into a query in order to reference
        their fields in expressions.
        """
        return RelationQuery(
            tree=self._tree.where(*self._convert_predicate_args(*args, bind=bind, **kwargs)),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def find_first(
        self, dataset_type: str | None = None, dimensions: DimensionGroup | None = None
    ) -> RelationQuery:
        """Return a query that resolves the datasets by searching collections
        in order after grouping by data ID.

        Parameters
        ----------
        dataset_type : `str`, optional
            Dataset type name of the datasets to resolve.  May be omitted if
            the query has exactly one dataset type joined in.
        dimensions : `~collections.abc.Iterable` [ `str` ] or \
                `DimensionGroup`, optional
            Dimensions of the data IDs to group by.  If not provided, the
            dataset type's dimensions are used (not the query's dimensions).

        Returns
        -------
        query : `Query`
            A new query object that includes the dataset resolution operation.
            Dataset columns other than the target dataset type's are dropped,
            as are dimensions outside ``dimensions``.

        Notes
        -----
        This operation is typically applied automatically when obtain a results
        object from the query, but in rare cases it may need to be added
        directly by this method, such as prior to a `materialization` or when
        the query is being used as the container in a `relation_tree.InRange`
        expression.
        """
        if not self._tree.available_dataset_types:
            raise InvalidRelationError("Query does not have any dataset searches joined in.")
        if dataset_type is None:
            try:
                (dataset_type,) = self._tree.available_dataset_types
            except ValueError:
                raise InvalidRelationError(
                    "Find-first dataset type must be provided if the query has more than one."
                ) from None
        elif dataset_type not in self._tree.available_dataset_types:
            raise InvalidRelationError(
                f"Find-first dataset type {dataset_type} is not present in query."
            ) from None
        if dimensions is None:
            dimensions = self._driver.get_dataset_dimensions(dataset_type)
        return RelationQuery(
            tree=self._tree.find_first(dataset_type, dimensions),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def _convert_order_by_args(self, *args: str | OrderExpression | ExpressionProxy) -> list[OrderExpression]:
        """Convert ``order_by`` arguments to a list of column expressions."""
        raise NotImplementedError("TODO: Parse string expression.")

    def _convert_predicate_args(
        self, *args: str | Predicate | DataId, bind: Mapping[str, Any] | None = None
    ) -> list[Predicate]:
        """Convert ``where`` arguments to a list of column expressions."""
        raise NotImplementedError("TODO: Parse string expression.")
