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

__all__ = ("Query2",)

from collections.abc import Iterable, Mapping, Set
from typing import TYPE_CHECKING, Any

from .._query import Query
from ..dimensions import DataCoordinate, DataId, DataIdValue, DimensionGroup
from .convert_args import convert_where_args
from .data_coordinate_results import DataCoordinateQueryResults2, DataCoordinateResultSpec
from .dataset_results import ChainedDatasetQueryResults, DatasetRefResultSpec, SingleTypeDatasetQueryResults2
from .dimension_record_results import DimensionRecordQueryResults2, DimensionRecordResultSpec
from .driver import QueryDriver
from .expression_factory import ExpressionFactory
from .tree import (
    DatasetSpec,
    InvalidQueryTreeError,
    MaterializationSpec,
    Predicate,
    QueryTree,
    make_dimension_query_tree,
    make_unit_query_tree,
)

if TYPE_CHECKING:
    from .._query_results import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults
    from ..registry import CollectionArgType


class Query2(Query):
    """Implementation of the Query interface backed by a `QueryTree` and a
    `QueryDriver`.

    Parameters
    ----------
    driver : `QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `QueryTree`
        Description of the query as a tree of joins and column expressions.
        The instance returned directly by the `Butler._query` entry point
        should be constructed via `make_unit_query_tree`.
    include_dimension_records : `bool`
        Whether query result objects created from this query should be expanded
        to include dimension records.

    Notes
    -----
    Ideally this will eventually just be "Query", because we won't need an ABC
    if this is the only implementation.
    """

    def __init__(self, driver: QueryDriver, tree: QueryTree, include_dimension_records: bool):
        self._driver = driver
        self._tree = tree
        self._include_dimension_records = include_dimension_records

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions joined into the query."""
        return self._tree.dimensions

    @property
    def dataset_types(self) -> Set[str]:
        """The names of dataset types joined into the query."""
        return self._tree.datasets.keys()

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
        # TODO: Arguments below are redundant with chaining methods; which ones
        # are so convenient we have to keep them?
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
            tree = tree.join(make_dimension_query_tree(dimensions))
        if data_id or where:
            tree = tree.where(*convert_where_args(self._tree, where, data_id, bind=bind, **kwargs))
        result_spec = DataCoordinateResultSpec(
            dimensions=dimensions, include_dimension_records=self._include_dimension_records
        )
        return DataCoordinateQueryResults2(self._driver, tree, result_spec)

    # TODO add typing.overload variants for single-dataset-type and patterns.

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
        resolved_dataset_types = self._driver.resolve_dataset_type_wildcard(dataset_type)
        data_id = DataCoordinate.standardize(data_id, universe=self._driver.universe, **kwargs)
        where_terms = convert_where_args(self._tree, where, data_id, bind=bind, **kwargs)
        single_type_results: list[SingleTypeDatasetQueryResults2] = []
        for name, resolved_dataset_type in resolved_dataset_types.items():
            tree = self._tree
            if name not in tree.datasets:
                resolved_collections, collections_ordered = self._driver.resolve_collection_wildcard(
                    # TODO: drop regex support from base signature.
                    collections,  # type: ignore
                )
                if find_first and not collections_ordered:
                    raise InvalidQueryTreeError(
                        f"Unordered collections argument {collections} requires find_first=False."
                    )
                tree = tree.join_dataset(
                    name,
                    DatasetSpec.model_construct(
                        dimensions=resolved_dataset_type.dimensions.as_group(),
                        collections=tuple(resolved_collections),
                    ),
                )
            elif collections is not None:
                raise InvalidQueryTreeError(
                    f"Dataset type {name!r} was already joined into this query but new collections "
                    f"{collections!r} were still provided."
                )
            if where_terms:
                tree = tree.where(*where_terms)
            if find_first:
                tree = tree.find_first(name)
            spec = DatasetRefResultSpec.model_construct(
                dataset_type=resolved_dataset_type, include_dimension_records=self._include_dimension_records
            )
            single_type_results.append(SingleTypeDatasetQueryResults2(self._driver, tree=tree, spec=spec))
        if len(single_type_results) == 1:
            return single_type_results[0]
        else:
            return ChainedDatasetQueryResults(tuple(single_type_results))

    def dimension_records(
        self,
        element: str,
        *,
        # TODO: Arguments below are redundant with chaining methods; which ones
        # are so convenient we have to keep them?
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> DimensionRecordQueryResults:
        # Docstring inherited.
        data_id = DataCoordinate.standardize(data_id, universe=self._driver.universe, **kwargs)
        tree = self._tree
        if element not in tree.dimensions.elements:
            tree = tree.join(make_dimension_query_tree(self._driver.universe[element].minimal_group))
        if data_id or where:
            tree = tree.where(*convert_where_args(self._tree, where, data_id, bind=bind, **kwargs))
        result_spec = DimensionRecordResultSpec(element=self._driver.universe[element])
        return DimensionRecordQueryResults2(self._driver, tree, result_spec)

    # TODO: add general, dict-row results method and QueryResults.

    # TODO: methods below are not part of the base Query, but they have
    # counterparts on at least some QueryResults objects.  We need to think
    # about which should be duplicated in Query and QueryResults, and which
    # should not, and get naming consistent.

    def with_dimension_records(self) -> Query2:
        """Return a new Query that will always include dimension records in
        any `DataCoordinate` or `DatasetRef` results.
        """
        return Query2(self._driver, self._tree, include_dimension_records=True)

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

    def materialize(
        self,
        *,
        dimensions: Iterable[str] | DimensionGroup | None = None,
        datasets: Iterable[str] | None = None,
    ) -> Query2:
        """Execute the query, save its results to a temporary location, and
        return a new query that represents fetching or joining against those
        saved results.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` [ `str` ] or \
                `DimensionGroup`, optional
            Dimensions to include in the temporary results.  Default is to
            include all dimensions in the query.
        datasets : `~collections.abc.Iterable` [ `str` ], optional
            Names of dataset types that should be included in the new query;
            default is to include all datasets types currently available to the
            query.  Only resolved datasets UUIDs will actually be materialized;
            datasets whose UUIDs cannot be resolved be continue to be
            represented in the query via a join on their dimensions.

        Returns
        -------
        query : `Query`
            A new query object whose that represents the materialized rows.
        """
        if datasets is None:
            datasets = frozenset(self._tree.datasets)
        else:
            datasets = frozenset(datasets)
        if dimensions is None:
            dimensions = self._tree.dimensions
        else:
            dimensions = self._driver.universe.conform(dimensions)
        key, resolved_datasets = self._driver.materialize(self._tree, dimensions, datasets)
        tree = make_unit_query_tree(self._driver.universe).join_materialization(
            key,
            MaterializationSpec.model_construct(dimensions=dimensions, resolved_datasets=resolved_datasets),
        )
        for dataset_type_name in datasets:
            tree = tree.join_dataset(dataset_type_name, self._tree.datasets[dataset_type_name])
        return Query2(self._driver, tree=tree, include_dimension_records=self._include_dimension_records)

    def join_dataset_search(
        self,
        dataset_type: str,
        collections: Iterable[str],
    ) -> Query2:
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
        return Query2(
            tree=self._tree.join_dataset(
                dataset_type,
                DatasetSpec.model_construct(
                    collections=tuple(collections),
                    dimensions=self._driver.get_dataset_dimensions(dataset_type),
                ),
            ),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def join_data_coordinates(self, iterable: Iterable[DataCoordinate]) -> Query2:
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
        return Query2(
            tree=self._tree.join_data_coordinate_upload(dimensions=dimensions, key=key),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def join_dimensions(self, dimensions: Iterable[str] | DimensionGroup) -> Query2:
        """Return a new query that joins the logical tables for additional
        dimensions.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` [ `str` ] or `DimensionGroup`
            Names of dimensions to join in.

        Returns
        -------
        query : `Query`
            A new query object with the dimensions joined in.
        """
        dimensions = self._driver.universe.conform(dimensions)
        return Query2(
            tree=self._tree.join(make_dimension_query_tree(dimensions)),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def where(
        self,
        *args: str | Predicate | DataId,
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Query2:
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
        return Query2(
            tree=self._tree.where(*convert_where_args(self._tree, *args, bind=bind, **kwargs)),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )

    def find_first(self, dataset_type: str | None = None) -> Query2:
        """Return a query that resolves the datasets by searching collections
        in order after grouping by data ID.

        Parameters
        ----------
        dataset_type : `str`, optional
            Dataset type name of the datasets to resolve.  May be omitted if
            the query has exactly one dataset type joined in.

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
        the query is being used as the container in a `query_tree.InRange`
        expression.
        """
        if not self._tree.datasets:
            raise InvalidQueryTreeError("Query does not have any dataset searches joined in.")
        if dataset_type is None:
            try:
                (dataset_type,) = self._tree.datasets
            except ValueError:
                raise InvalidQueryTreeError(
                    "Find-first dataset type must be provided if the query has more than one."
                ) from None
        return Query2(
            tree=self._tree.find_first(dataset_type),
            driver=self._driver,
            include_dimension_records=self._include_dimension_records,
        )
