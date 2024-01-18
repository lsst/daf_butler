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

__all__ = ("Query",)

from collections.abc import Iterable, Mapping, Set
from types import EllipsisType
from typing import Any, overload

from .._dataset_type import DatasetType
from ..dimensions import DataCoordinate, DataId, DataIdValue, DimensionGroup
from ..registry import DatasetTypeError, MissingDatasetTypeError
from ._base import HomogeneousQueryBase
from ._data_coordinate_query_results import DataCoordinateQueryResults
from ._dataset_query_results import (
    ChainedDatasetQueryResults,
    DatasetQueryResults,
    SingleTypeDatasetQueryResults,
)
from ._dimension_record_query_results import DimensionRecordQueryResults
from .convert_args import convert_dataset_search_args, convert_where_args
from .driver import QueryDriver
from .expression_factory import ExpressionFactory
from .result_specs import DataCoordinateResultSpec, DatasetRefResultSpec, DimensionRecordResultSpec
from .tree import (
    DatasetSearch,
    InvalidQueryTreeError,
    MaterializationSpec,
    Predicate,
    QueryTree,
    make_dimension_query_tree,
    make_unit_query_tree,
)


class Query(HomogeneousQueryBase):
    """A method-chaining builder for butler queries.

    Parameters
    ----------
    driver : `QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `QueryTree`
        Description of the query as a tree of joins and column expressions.
        The instance returned directly by the `Butler._query` entry point
        should be constructed via `make_unit_query_tree`.

    Notes
    -----
    This largely mimics and considerably expands the `Query` ABC defined in
    `lsst.daf.butler._query`, but the intent is to replace that ABC with this
    concrete class, rather than inherit from it.
    """

    def __init__(self, driver: QueryDriver, tree: QueryTree):
        super().__init__(driver, tree)

    @property
    def all_dataset_types(self) -> Set[str]:
        """The names of all dataset types joined into the query.

        These dataset types are usable in 'where' expressions, but may or may
        not be available to result rows.
        """
        return self._tree.datasets.keys()

    @property
    def result_dataset_types(self) -> Set[str]:
        """The names of all dataset types available for result rows and
        order-by expressions.
        """
        return self._tree.available_result_datasets

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
    ) -> DataCoordinateQueryResults:
        """Query for data IDs matching user-provided criteria.

        Parameters
        ----------
        dimensions : `DimensionGroup`, `str`, or \
                `~collections.abc.Iterable` [`str`]
            The dimensions of the data IDs to yield, as either `DimensionGroup`
            instances or `str`.  Will be automatically expanded to a complete
            `DimensionGroup`.

        Returns
        -------
        dataIds : `DataCoordinateQueryResults`
            Data IDs matching the given query parameters.  These are guaranteed
            to identify all dimensions (`DataCoordinate.hasFull` returns
            `True`), but will not contain `DimensionRecord` objects
            (`DataCoordinate.hasRecords` returns `False`).  Call
            `~DataCoordinateQueryResults.with_dimension_records` on the
            returned object to fetch those.
        """
        dimensions = self._driver.universe.conform(dimensions)
        tree = self._tree
        if not dimensions >= self._tree.dimensions:
            tree = tree.join(make_dimension_query_tree(dimensions))
        result_spec = DataCoordinateResultSpec(dimensions=dimensions, include_dimension_records=False)
        return DataCoordinateQueryResults(self._driver, tree, result_spec)

    @overload
    def datasets(
        self,
        dataset_type: str | DatasetType,
        collections: str | Iterable[str] | None = None,
        *,
        find_first: bool = True,
    ) -> SingleTypeDatasetQueryResults:
        ...

    @overload
    def datasets(
        self,
        dataset_type: Iterable[str | DatasetType] | EllipsisType,
        collections: str | Iterable[str] | None = None,
        *,
        find_first: bool = True,
    ) -> DatasetQueryResults:
        ...

    def datasets(
        self,
        dataset_type: str | DatasetType | Iterable[str | DatasetType] | EllipsisType,
        collections: str | Iterable[str] | None = None,
        *,
        find_first: bool = True,
    ) -> DatasetQueryResults:
        """Query for and iterate over dataset references matching user-provided
        criteria.

        Parameters
        ----------
        dataset_type : `str`, `DatasetType`, \
                `~collections.abc.Iterable` [ `str` or `DatasetType` ], \
                or ``...``
            The dataset type or types to search for.  Passing ``...`` searches
            for all datasets in the given collections.
        collections : `str` or `~collections.abc.Iterable` [ `str` ], optional
            The collection or collections to search, in order.  If not provided
            or `None`, and the dataset has not already been joined into the
            query, the default collection search path for this butler is used.
        find_first : `bool`, optional
            If `True` (default), for each result data ID, only yield one
            `DatasetRef` of each `DatasetType`, from the first collection in
            which a dataset of that dataset type appears (according to the
            order of ``collections`` passed in).  If `True`, ``collections``
            must not contain regular expressions and may not be ``...``.

        Returns
        -------
        refs : `.queries.DatasetQueryResults`
            Dataset references matching the given query criteria.  Nested data
            IDs are guaranteed to include values for all implied dimensions
            (i.e. `DataCoordinate.hasFull` will return `True`), but will not
            include dimension records (`DataCoordinate.hasRecords` will be
            `False`) unless
            `~.queries.DatasetQueryResults.with_dimension_records` is
            called on the result object (which returns a new one).

        Raises
        ------
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``dataset_type`` expression is invalid.
        TypeError
            Raised when the arguments are incompatible, such as when a
            collection wildcard is passed when ``find_first`` is `True`, or
            when ``collections`` is `None` and default butler collections are
            not defined.

        Notes
        -----
        When multiple dataset types are queried in a single call, the
        results of this operation are equivalent to querying for each dataset
        type separately in turn, and no information about the relationships
        between datasets of different types is included.
        """
        resolved_dataset_searches = convert_dataset_search_args(
            dataset_type, self._driver.resolve_collection_path(collections)
        )
        single_type_results: list[SingleTypeDatasetQueryResults] = []
        for resolved_dataset_type, resolved_collections in resolved_dataset_searches:
            tree = self._tree
            if tree.find_first_dataset is not None and resolved_dataset_type.name != tree.find_first_dataset:
                raise InvalidQueryTreeError(
                    f"Query is already a find-first search for {tree.find_first_dataset!r}; cannot "
                    f"return results for {resolved_dataset_type.name!r} as well. "
                    "To avoid this error call Query.datasets before Query.find_first."
                )
            if resolved_dataset_type.name not in tree.datasets:
                tree = tree.join_dataset(
                    resolved_dataset_type.name,
                    DatasetSearch.model_construct(
                        dimensions=resolved_dataset_type.dimensions.as_group(),
                        collections=resolved_collections,
                    ),
                )
            elif collections is not None:
                raise InvalidQueryTreeError(
                    f"Dataset type {resolved_dataset_type.name!r} was already joined into this query "
                    f"but new collections {collections!r} were still provided."
                )
            if find_first:
                tree = tree.find_first(resolved_dataset_type.name)
            spec = DatasetRefResultSpec.model_construct(
                dataset_type_name=resolved_dataset_type.name,
                dimensions=resolved_dataset_type.dimensions.as_group(),
                storage_class_name=resolved_dataset_type.storageClass_name,
                include_dimension_records=False,
            )
            single_type_results.append(SingleTypeDatasetQueryResults(self._driver, tree=tree, spec=spec))
        if len(single_type_results) == 1:
            return single_type_results[0]
        else:
            return ChainedDatasetQueryResults(tuple(single_type_results))

    def dimension_records(self, element: str) -> DimensionRecordQueryResults:
        """Query for dimension information matching user-provided criteria.

        Parameters
        ----------
        element : `str`
            The name of a dimension element to obtain records for.

        Returns
        -------
        records : `.queries.DimensionRecordQueryResults`
            Data IDs matching the given query parameters.
        """
        tree = self._tree
        if element not in tree.dimensions.elements:
            tree = tree.join(make_dimension_query_tree(self._driver.universe[element].minimal_group))
        result_spec = DimensionRecordResultSpec(element=self._driver.universe[element])
        return DimensionRecordQueryResults(self._driver, tree, result_spec)

    # TODO: add general, dict-row results method and QueryResults.

    def materialize(
        self,
        *,
        dimensions: Iterable[str] | DimensionGroup | None = None,
        datasets: Iterable[str] | None = None,
    ) -> Query:
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
            default is to include `result_dataset_types`.  Only resolved
            dataset UUIDs will actually be materialized; datasets whose UUIDs
            cannot be resolved will continue to be represented in the query via
            a join on their dimensions.

        Returns
        -------
        query : `Query`
            A new query object whose that represents the materialized rows.
        """
        if datasets is None:
            datasets = frozenset(self.result_dataset_types)
        else:
            datasets = frozenset(datasets)
            if not (datasets <= self.result_dataset_types):
                raise InvalidQueryTreeError(
                    f"Dataset(s) {datasets - self.result_dataset_types} are not available as query results."
                )
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
        return Query(self._driver, tree)

    def join_dataset_search(
        self,
        dataset_type: str,
        collections: Iterable[str] | None = None,
        dimensions: DimensionGroup | None = None,
    ) -> Query:
        """Return a new query with a search for a dataset joined in.

        Parameters
        ----------
        dataset_type : `str`
            Name of the dataset type.  May not refer to a dataset component.
        collections : `~collections.abc.Iterable` [ `str` ], optional
            Iterable of collections to search.  Order is preserved, but will
            not matter if the dataset search is only used as a constraint on
            dimensions or if ``find_first=False`` when requesting results. If
            not present or `None`, the default collection search path will be
            used.
        dimensions : `DimensionGroup`, optional
            The dimensions to assume for the dataset type if it is not
            registered, or check if it is.  When the dataset is not registered
            and this is not provided, `MissingDatasetTypeError` is raised,
            since we cannot construct a query without knowing the dataset's
            dimensions; providing this argument causes the returned query to
            instead return no rows.

        Returns
        -------
        query : `Query`
            A new query object with dataset columns available and rows
            restricted to those consistent with the found data IDs.

        Raises
        ------
        DatasetTypeError
            Raised if the dimensions were provided but they do not match the
            registered dataset type.
        MissingDatasetTypeError
            Raised if the dimensions were not provided and the dataset type was
            not registered.
        """
        resolved_dataset_search = convert_dataset_search_args(
            dataset_type, self._driver.resolve_collection_path(collections)
        )
        if not resolved_dataset_search:
            resolved_collections: tuple[str, ...] = ()
            try:
                resolved_dimensions = self._driver.get_dataset_dimensions(dataset_type)
            except MissingDatasetTypeError:
                if dimensions is None:
                    raise
                else:
                    resolved_dimensions = dimensions
        elif len(resolved_dataset_search) > 1:
            raise TypeError(f"join_dataset_search expects a single dataset type, not {dataset_type!r}.")
        else:
            resolved_dataset_type, resolved_collections = resolved_dataset_search[0]
            resolved_dimensions = resolved_dataset_type.dimensions.as_group()
        if dimensions is not None and dimensions != resolved_dimensions:
            raise DatasetTypeError(
                f"Given dimensions {dimensions} for dataset type {dataset_type!r} do not match the "
                f"registered dimensions {resolved_dimensions}."
            )
        return Query(
            tree=self._tree.join_dataset(
                dataset_type,
                DatasetSearch.model_construct(
                    collections=resolved_collections, dimensions=resolved_dimensions
                ),
            ),
            driver=self._driver,
        )

    def join_data_coordinates(self, iterable: Iterable[DataCoordinate]) -> Query:
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
        return Query(
            tree=self._tree.join_data_coordinate_upload(dimensions=dimensions, key=key), driver=self._driver
        )

    def join_dimensions(self, dimensions: Iterable[str] | DimensionGroup) -> Query:
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
        return Query(
            tree=self._tree.join(make_dimension_query_tree(dimensions)),
            driver=self._driver,
        )

    def where(
        self,
        *args: str | Predicate | DataId,
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Query:
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
        return Query(
            tree=self._tree.where(
                *convert_where_args(self.dimensions, self.all_dataset_types, *args, bind=bind, **kwargs)
            ),
            driver=self._driver,
        )

    def find_first(self, dataset_type: str | None = None) -> Query:
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

        Calling this method on a query that is already a find-first search
        will replace the current find-first search with the new one.
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
        return Query(tree=self._tree.find_first(dataset_type), driver=self._driver)
