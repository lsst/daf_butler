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
from typing import Any, final

from lsst.utils.iteration import ensure_iterable

from .._dataset_type import DatasetType
from .._exceptions import DimensionNameError, InvalidQueryError
from .._storage_class import StorageClassFactory
from ..dimensions import DataCoordinate, DataId, DataIdValue, DimensionGroup
from ..registry import DatasetTypeError
from ._base import QueryBase
from ._data_coordinate_query_results import DataCoordinateQueryResults
from ._dataset_query_results import DatasetRefQueryResults
from ._dimension_record_query_results import DimensionRecordQueryResults
from ._general_query_results import GeneralQueryResults
from ._identifiers import IdentifierContext, interpret_identifier
from .convert_args import convert_where_args
from .driver import QueryDriver
from .expression_factory import ExpressionFactory
from .result_specs import (
    DataCoordinateResultSpec,
    DatasetRefResultSpec,
    DimensionRecordResultSpec,
    GeneralResultSpec,
)
from .tree import (
    ANY_DATASET,
    DatasetFieldName,
    DatasetFieldReference,
    DatasetSearch,
    DimensionFieldReference,
    DimensionKeyReference,
    Predicate,
    QueryTree,
    make_identity_query_tree,
)


@final
class Query(QueryBase):
    """A method-chaining builder for butler queries.

    Parameters
    ----------
    driver : `~lsst.daf.butler.queries.driver.QueryDriver`
        Implementation object that knows how to actually execute queries.
    tree : `~lsst.daf.butler.queries.tree.QueryTree`, optional
        Description of the query as a tree of joins and column expressions.
        Defaults to the result of a call to
        `~lsst.daf.butler.queries.tree.make_identity_query_tree`.

    Notes
    -----
    `Query` objects should never be constructed directly by users; use
    `Butler.query <lsst.daf.butler.Butler.query>` instead.

    A `Query` object represents the first stage of query construction, in which
    constraints and joins are defined (roughly corresponding to the WHERE and
    FROM clauses in SQL).  The various "results" objects represent the second
    (and final) stage, where the columns returned are specified and any sorting
    or integer slicing can be applied.  Result objects are obtained from the
    `data_ids`, `datasets`, and `dimension_records` methods.

    `Query` and query-result objects are always immutable (except for caching
    information fetched from the database or server), so modifier methods
    always return a new object without modifying the current one.
    """

    def __init__(self, driver: QueryDriver, tree: QueryTree | None = None):
        # __init__ defined here because there are multiple base classes and
        # not all define __init__ (and hence inherit object.__init__, which
        # just ignores its args).  Even if we just delegate to super(), it
        # seems less fragile to make it explicit here.
        if tree is None:
            tree = make_identity_query_tree(driver.universe)
        super().__init__(driver, tree)

        # If ``_allow_duplicate_overlaps`` is set to `True` then query will be
        # allowed to generate non-distinct rows for spatial overlaps. This is
        # not a part of public API for now, to be used by graph builder as
        # optimization.
        self._allow_duplicate_overlaps: bool = False

    @property
    def constraint_dataset_types(self) -> Set[str]:
        """The names of all dataset types joined into the query.

        The existence of datasets of these types constrains the data IDs of any
        type of result.  Fields for these dataset types are also usable in
        'where' expressions.
        """
        # Note that this includes only dataset type names, not `DatasetType`
        # instances; the `DatasetQueryResults` adapter returned by the
        # `datasets` method does include `DatasetType` instances, since it is
        # in a better position to track and respect any storage class override
        # specified.
        return self._tree.datasets.keys()

    @property
    def constraint_dimensions(self) -> DimensionGroup:
        """Dimensions currently present in the query, either directly or
        indirectly.

        This includes dimensions that are present in any joined subquery (such
        as a dataset search, materialization, or data ID upload) or `where`
        argument, as well as any required or implied dependency of those
        dimensions.
        """
        return self._tree.dimensions

    @property
    def expression_factory(self) -> ExpressionFactory:
        """A factory for column expressions using overloaded operators.
        (`~lsst.daf.butler.queries.expression_factory.ExpressionFactory`).

        Notes
        -----
        Typically this attribute will be assigned to a single-character local
        variable, and then its (dynamic) attributes can be used to obtain
        references to columns that can be included in a query::

            with butler.query() as query:
                x = query.expression_factory
                query = query.where(
                    x.instrument == "LSSTCam",
                    x.visit.day_obs > 20240701,
                    x.any(x.band == "u", x.band == "y"),
                )

        As shown above, the returned object also has an
        `~lsst.daf.butler.queries.expression_factory.ExpressionFactory.any`
        method to create combine expressions with logical OR (as well as
        `~lsst.daf.butler.queries.expression_factory.ExpressionFactory.not_`
        and
        `~lsst.daf.butler.queries.expression_factory.ExpressionFactory.all`,
        though the latter is rarely necessary since `where` already combines
        its arguments with AND).

        Proxies for fields associated with individual datasets but not
        dimension records (``dataset_id``, ``ingest_date``, ``run``,
        ``collection``, as well as ``timespan`` for
        `~lsst.daf.butler.CollectionType.CALIBRATION` collection searches) can
        be obtained with dict-like access instead::

            with butler.query() as query:
                query = query.order_by(x["raw"].ingest_date)

        Expression proxy objects that correspond to scalar columns overload the
        standard comparison operators (``==``, ``!=``, ``<``, ``>``, ``<=``,
        ``>=``) and provide
        `~lsst.daf.butler.queries.expression_factory.ScalarExpressionProxy.in_range`,
        `~lsst.daf.butler.queries.expression_factory.ScalarExpressionProxy.in_iterable`, and
        `~lsst.daf.butler.queries.expression_factory.ScalarExpressionProxy.in_query`
        methods for membership tests.  For ``order_by`` contexts, they also have a
        `~lsst.daf.butler.queries.expression_factory.ScalarExpressionProxy.desc`
        property to indicate that the sort order for that expression should be
        reversed.

        Proxy objects for
        `region <lsst.daf.butler.queries.expression_factory.RegionProxy>` and
        `timespan <lsst.daf.butler.queries.expression_factory.TimespanProxy>`
        fields have an ``overlaps`` method, and timespans also have
        `~lsst.daf.butler.queries.expression_factory.TimespanProxy.begin` and
        `~lsst.daf.butler.queries.expression_factory.TimespanProxy.end`
        properties to access scalar expression proxies for the bounds.

        All proxy objects also have an
        `~lsst.daf.butler.queries.expression_factory.ExpressionProxy.is_null`
        property.

        Literal values can be created by calling
        `ExpressionFactory.literal <lsst.daf.butler.queries.expression_factory.ExpressionFactory.literal>`,
        but can almost always be created implicitly via overloaded operators
        instead.
        """  # noqa: W505, long docstrings
        return ExpressionFactory(self._driver.universe)

    def data_ids(
        self, dimensions: DimensionGroup | Iterable[str] | str | None = None
    ) -> DataCoordinateQueryResults:
        """Return a result object that is a `~lsst.daf.butler.DataCoordinate`
        iterable.

        Parameters
        ----------
        dimensions : `~lsst.daf.butler.DimensionGroup`, `str`, or \
                `~collections.abc.Iterable` [`str`], optional
            The dimensions of the data IDs to yield, as either
            `~lsst.daf.butler.DimensionGroup` instances or `str` names.  Will
            be automatically expanded to a complete
            `~lsst.daf.butler.DimensionGroup`.  These dimensions do not need to
            match the query's current dimensions.  Default is
            `constraint_dimensions`.

        Returns
        -------
        data_ids : `~lsst.daf.butler.queries.DataCoordinateQueryResults`
            Data IDs matching the given query parameters.  These are guaranteed
            to identify all dimensions (``DataCoordinate.hasFull`` returns
            `True`), but will not contain `~lsst.daf.butler.DimensionRecord`
            objects (``DataCoordinate.hasRecords`` returns `False`).  Call
            `~DataCoordinateQueryResults.with_dimension_records` on the
            returned object to include dimension records as well.
        """
        tree = self._tree
        if dimensions is None:
            dimensions = self._tree.dimensions
        else:
            dimensions = self._driver.universe.conform(dimensions)
            if not dimensions <= self._tree.dimensions:
                tree = tree.join_dimensions(dimensions)
        result_spec = DataCoordinateResultSpec(
            dimensions=dimensions,
            include_dimension_records=False,
            allow_duplicate_overlaps=self._allow_duplicate_overlaps,
        )
        return DataCoordinateQueryResults(self._driver, tree, result_spec)

    def datasets(
        self,
        dataset_type: str | DatasetType,
        collections: str | Iterable[str] | None = None,
        *,
        find_first: bool = True,
    ) -> DatasetRefQueryResults:
        """Return a result object that is a `~lsst.daf.butler.DatasetRef`
        iterable.

        Parameters
        ----------
        dataset_type : `str` or `~lsst.daf.butler.DatasetType`
            The dataset type to search for.
        collections : `str` or `~collections.abc.Iterable` [ `str` ], optional
            The collection or collections to search, in order.  If not provided
            or `None`, and the dataset has not already been joined into the
            query, the default collection search path for this butler is used.
        find_first : `bool`, optional
            If `True` (default), for each result data ID, only yield one
            `~lsst.daf.butler.DatasetRef` of each
            `~lsst.daf.butler.DatasetType`, from the first collection in
            which a dataset of that dataset type appears (according to the
            order of ``collections`` passed in).  If `True`, ``collections``
            must not be ``...``.

        Returns
        -------
        refs : `lsst.daf.butler.queries.DatasetRefQueryResults`
            Dataset references matching the given query criteria.  Nested data
            IDs are guaranteed to include values for all implied dimensions
            (i.e. ``DataCoordinate.hasFull`` will return `True`), but will not
            include dimension records (``DataCoordinate.hasRecords`` will be
            `False`) unless
            `~.queries.DatasetRefQueryResults.with_dimension_records` is
            called on the result object (which returns a new one).

        Raises
        ------
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when the ``dataset_type`` expression is invalid.
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised when ``collections`` is `None` and default butler
            collections are not defined.
        TypeError
            Raised when the arguments are incompatible, such as when a
            collection wildcard is passed when ``find_first`` is `True`
        """
        dataset_type_name, storage_class_name, query = self._join_dataset_search_impl(
            dataset_type, collections
        )
        dataset_search = query._tree.datasets[dataset_type_name]
        spec = DatasetRefResultSpec.model_construct(
            dataset_type_name=dataset_type_name,
            dimensions=dataset_search.dimensions,
            storage_class_name=storage_class_name,
            include_dimension_records=False,
            find_first=find_first,
            allow_duplicate_overlaps=self._allow_duplicate_overlaps,
        )
        return DatasetRefQueryResults(self._driver, tree=query._tree, spec=spec)

    def dimension_records(self, element: str) -> DimensionRecordQueryResults:
        """Return a result object that is a `~lsst.daf.butler.DimensionRecord`
        iterable.

        Parameters
        ----------
        element : `str`
            The name of a dimension element to obtain records for.

        Returns
        -------
        records : `lsst.daf.butler.queries.DimensionRecordQueryResults`
            Data IDs matching the given query parameters.
        """
        if element not in self._driver.universe:
            # Prefer an explicit exception over a KeyError below.
            raise DimensionNameError(
                f"No such dimension '{element}', available dimensions: " + str(self._driver.universe.elements)
            )
        tree = self._tree
        if element not in tree.dimensions.elements:
            tree = tree.join_dimensions(self._driver.universe[element].minimal_group)
        result_spec = DimensionRecordResultSpec(
            element=self._driver.universe[element], allow_duplicate_overlaps=self._allow_duplicate_overlaps
        )
        return DimensionRecordQueryResults(self._driver, tree, result_spec)

    def general(
        self,
        dimensions: DimensionGroup | Iterable[str],
        *names: str,
        dimension_fields: Mapping[str, Set[str]] | None = None,
        dataset_fields: Mapping[str, Set[DatasetFieldName] | EllipsisType] | None = None,
        find_first: bool | None = None,
    ) -> GeneralQueryResults:
        """Execute query returning general result.

        **This is an experimental interface and may change at any time.**

        Parameters
        ----------
        dimensions : `~lsst.daf.butler.DimensionGroup` or \
                `~collections.abc.Iterable` [ `str` ]
            The dimensions that span all fields returned by this query.
        *names : `str`
            Names of dimensions fields (in  "dimension.field" format), dataset
            fields (in  "dataset_type.field" format) to include in this query.
        dimension_fields : `~collections.abc.Mapping` [`str`, \
                `~collections.abc.Set` [`str`]], optional
            Dimension record fields included in this query, keyed by dimension
            element name.
        dataset_fields : `~collections.abc.Mapping` [`str`, \
                `~collections.abc.Set` | ``...`` ], optional
            Dataset fields included in this query, the key in the mapping is
            dataset type name. Ellipsis (``...``) can be used for value
            to include all dataset fields needed to extract
            `~lsst.daf.butler.DatasetRef` instances later.
        find_first : `bool`, optional
            Whether this query requires find-first resolution for a dataset.
            This is ignored and can be omitted if the query has no dataset
            fields.  It must be explicitly set to `False` if there are multiple
            dataset types with fields, or if any dataset's ``collections``
            or ``timespan`` fields are included in the results.

        Returns
        -------
        result : `~lsst.daf.butler.queries.GeneralQueryResults`
            Query result that can be iterated over.

        Notes
        -----
        The dimensions of the returned query are automatically expanded to
        include those associated with all dimension and dataset fields; the
        ``dimensions`` argument is just the minimal dimensions to return.
        """
        if dimension_fields is None:
            dimension_fields = {}
        if dataset_fields is None:
            dataset_fields = {}
        # Processing fields from mapping args, processing the special `...`
        # wildcard and dropping keys with empty values.
        dataset_fields_dict: dict[str, set[DatasetFieldName]] = {}
        for dataset_type_name, fields_for_dataset_type in dataset_fields.items():
            if fields_for_dataset_type is ...:
                new_fields_for_dataset_type: set[DatasetFieldName] = {
                    "run",
                    "dataset_id",
                }  # all we need for DatasetRefs.
            else:
                new_fields_for_dataset_type = set(fields_for_dataset_type)
            if new_fields_for_dataset_type:
                dataset_fields_dict[dataset_type_name] = new_fields_for_dataset_type
        dimension_fields_dict = {
            element_name: new_fields_for_element
            for element_name, fields_for_element in dimension_fields.items()
            if (new_fields_for_element := set(fields_for_element))
        }
        # Parse all names passed as positional arguments, and start to
        # accumulate additional dimension names we'll need in the results.
        dimensions = self._driver.universe.conform(dimensions)
        context = IdentifierContext(dimensions, set(self._tree.datasets))
        extra_dimension_names: set[str] = set()
        for name in names:
            identifier = interpret_identifier(context, name)
            match identifier:
                case DimensionKeyReference(dimension=dimension):
                    # Could be because someone asked for the key field.
                    extra_dimension_names.add(dimension.name)
                case DimensionFieldReference(element=element, field=field):
                    dimension_fields_dict.setdefault(element.name, set()).add(field)
                case DatasetFieldReference(dataset_type=dataset_type, field=dataset_field):
                    if dataset_type is ANY_DATASET:
                        raise InvalidQueryError("Dataset wildcard fields are not supported by Query.general.")
                    dataset_fields_dict.setdefault(dataset_type, set()).add(dataset_field)
                case _:
                    raise TypeError(f"Unexpected type of identifier ({name}): {identifier}")
        # Add more dimension names from the field mappings (including those
        # we just populated from args).  Also check that the dataset fields
        # are consistent with find_first.
        for element_name, fields in dimension_fields_dict.items():
            extra_dimension_names.update(self._driver.universe[element_name].minimal_group.names)
        for dataset_type_name, fields_for_dataset_type in dataset_fields_dict.items():
            if "collections" in fields_for_dataset_type and find_first is not False:
                raise InvalidQueryError(
                    f"find_first=False must be passed explicitly if {dataset_type_name}.collections "
                    "is included in query results."
                )
            if "timespan" in fields_for_dataset_type and find_first is not False:
                raise InvalidQueryError(
                    f"find_first=False must be passed explicitly if {dataset_type_name}.timespan "
                    "is included in query results."
                )
            try:
                dataset_search = self._tree.datasets[dataset_type_name]
            except KeyError:
                raise InvalidQueryError(
                    f"A search for dataset type {dataset_type_name!r} must be explicitly joined into the "
                    "query before including its fields in query results."
                ) from None
            extra_dimension_names.update(dataset_search.dimensions.names)
        if find_first is None:
            if dataset_fields_dict:
                raise InvalidQueryError(
                    "find_first must be passed if dataset fields are included in query results."
                )
            else:
                find_first = False
        if find_first and len(dataset_fields_dict) != 1:
            raise InvalidQueryError(
                "find_first=True is not valid unless exactly one dataset type's fields are requested."
            )
        # Combine extra dimensions with the original ones.
        dimensions = self._driver.universe.conform(dimensions.names | extra_dimension_names)
        # Merge missing dimensions into the tree.
        tree = self._tree
        if not dimensions <= tree.dimensions:
            tree = tree.join_dimensions(dimensions)
        result_spec = GeneralResultSpec(
            dimensions=dimensions,
            dimension_fields=dimension_fields_dict,
            dataset_fields=dataset_fields_dict,
            find_first=find_first,
            allow_duplicate_overlaps=self._allow_duplicate_overlaps,
        )
        return GeneralQueryResults(self._driver, tree=tree, spec=result_spec)

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
                `~lsst.daf.butler.DimensionGroup`, optional
            Dimensions to include in the temporary results.  Default is to
            include all dimensions in the query.
        datasets : `~collections.abc.Iterable` [ `str` ], optional
            Names of dataset types that should be included in the new query;
            default is to include `constraint_dataset_types`.

        Returns
        -------
        query : `Query`
            A new query object whose that represents the materialized rows.

        Notes
        -----
        Only dimension key columns and (at the discretion of the
        implementation) certain dataset columns are actually materialized,
        since at this stage we do not know which dataset or dimension record
        fields are actually needed in result rows, and these can be joined back
        in on the materialized dimension keys.  But all constraints on those
        dimension keys (including dataset existence) are applied to the
        materialized rows.
        """
        if datasets is None:
            datasets = frozenset(self.constraint_dataset_types)
        else:
            datasets = frozenset(datasets)
            if not (datasets <= self.constraint_dataset_types):
                raise InvalidQueryError(
                    f"Dataset(s) {datasets - self.constraint_dataset_types} are present in the query."
                )
        if dimensions is None:
            dimensions = self._tree.dimensions
        else:
            dimensions = self._driver.universe.conform(dimensions)
        key = self._driver.materialize(
            self._tree, dimensions, datasets, allow_duplicate_overlaps=self._allow_duplicate_overlaps
        )
        tree = make_identity_query_tree(self._driver.universe).join_materialization(
            key, dimensions=dimensions
        )
        for dataset_type_name in datasets:
            dataset_search = self._tree.datasets[dataset_type_name]
            if not (dataset_search.dimensions <= tree.dimensions):
                raise InvalidQueryError(
                    f"Materialization-backed query has dimensions {tree.dimensions}, which do not "
                    f"cover the dimensions {dataset_search.dimensions} of dataset {dataset_type_name!r}. "
                    "Expand the dimensions or drop this dataset type in the arguments to materialize to "
                    "avoid this error."
                )
            tree = tree.join_dataset(dataset_type_name, dataset_search)
        return Query(self._driver, tree)

    def join_dataset_search(
        self,
        dataset_type: str | DatasetType,
        collections: Iterable[str] | None = None,
    ) -> Query:
        """Return a new query with a search for a dataset joined in.

        Parameters
        ----------
        dataset_type : `str` or `~lsst.daf.butler.DatasetType`
            Dataset type or name.  May not refer to a dataset component.
        collections : `~collections.abc.Iterable` [ `str` ], optional
            Iterable of collections to search.  Order is preserved, but will
            not matter if the dataset search is only used as a constraint on
            dimensions or if ``find_first=False`` when requesting results. If
            not present or `None`, the default collection search path will be
            used.

        Returns
        -------
        query : `Query`
            A new query object with dataset columns available and rows
            restricted to those consistent with the found data IDs.

        Raises
        ------
        DatasetTypeError
            Raised if given dataset type is inconsistent with the registered
            dataset type.
        MissingDatasetTypeError
            Raised if the dataset type has not been registered and only a
            `str` dataset type name was given.

        Notes
        -----
        This method may require communication with the server unless the
        dataset type and collections have already been referenced by the same
        query context.
        """
        _, _, query = self._join_dataset_search_impl(
            dataset_type, collections, allow_storage_class_overrides=False
        )
        return query

    def join_data_coordinates(self, iterable: Iterable[DataCoordinate]) -> Query:
        """Return a new query that joins in an explicit table of data IDs.

        Parameters
        ----------
        iterable : `~collections.abc.Iterable` \
                [`~lsst.daf.butler.DataCoordinate`]
            Iterable of `~lsst.daf.butler.DataCoordinate`.  All items must have
            the same dimensions.  Must have at least one item.

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
                raise InvalidQueryError(
                    f"Inconsistent dimensions: {dimensions} != {data_coordinate.dimensions}."
                )
            rows.add(data_coordinate.required_values)
        if dimensions is None:
            raise InvalidQueryError("Cannot upload an empty data coordinate set.")
        key = self._driver.upload_data_coordinates(dimensions, rows)
        return Query(
            tree=self._tree.join_data_coordinate_upload(dimensions=dimensions, key=key),
            driver=self._driver,
        )

    def join_dimensions(self, dimensions: Iterable[str] | DimensionGroup) -> Query:
        """Return a new query that joins the logical tables for additional
        dimensions.

        Parameters
        ----------
        dimensions : `~collections.abc.Iterable` [ `str` ] or \
                `~lsst.daf.butler.DimensionGroup`
            Names of dimensions to join in.

        Returns
        -------
        query : `Query`
            A new query object with the dimensions joined in.

        Notes
        -----
        Dimensions are automatically joined in whenever needed, so this method
        should rarely need to be called directly.
        """
        dimensions = self._driver.universe.conform(dimensions)
        return Query(tree=self._tree.join_dimensions(dimensions), driver=self._driver)

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
            `str` expressions to parse,
            `~lsst.daf.butler.queries.tree.Predicate` objects (these are
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
            A new query object with the given row filters (as well as any
            already present in ``self``).  All row filters are combined with
            logical AND.

        Notes
        -----
        Expressions referring to dimensions or dimension elements are resolved
        automatically. References to dataset fields (see `expression_factory`
        for the distinction) cannot be resolved by default; they must either be
        preceded by a call to `join_dataset_search` or must be passed to
        `DatasetRefQueryResults.where <lsst.daf.butler.queries.DatasetRefQueryResults.where>`
        instead.

        Data ID values are not checked for consistency; they are extracted from
        ``args`` and then ``kwargs`` and combined, with later values overriding
        earlier ones.
        """  # noqa: W505, long docstrings
        return Query(
            tree=self._tree.where(
                convert_where_args(
                    self.constraint_dimensions,
                    self.constraint_dataset_types,
                    *args,
                    bind=bind,
                    **kwargs,
                )
            ),
            driver=self._driver,
        )

    def _join_dataset_search_impl(
        self,
        dataset_type: str | DatasetType,
        collections: Iterable[str] | None = None,
        allow_storage_class_overrides: bool = True,
    ) -> tuple[str, str, Query]:
        """Implement `join_dataset_search`, and also return the dataset type
        name and storage class, in addition to the modified Query.
        """
        # In this method we need the dimensions of the dataset type, but we
        # might not need the storage class, since the dataset may only be used
        # as an existence constraint.  It depends on whether
        # `join_dataset_search` or `datasets` is calling this method.
        dimensions: DimensionGroup | None = None
        storage_class_name: str | None = None
        # Handle DatasetType vs. str arg.
        if isinstance(dataset_type, DatasetType):
            dataset_type_name = dataset_type.name
            dimensions = dataset_type.dimensions
            storage_class_name = dataset_type.storageClass_name
        elif isinstance(dataset_type, str):
            dataset_type_name = dataset_type
        else:
            raise TypeError(f"Invalid dataset type argument {dataset_type!r}.")
        # See if this dataset has already been joined into the query.
        if existing_search := self._tree.datasets.get(dataset_type_name):
            if collections is None:
                collections = existing_search.collections
            else:
                collections = tuple(ensure_iterable(collections))
                if collections != existing_search.collections:
                    raise InvalidQueryError(
                        f"Dataset type {dataset_type_name!r} was already joined into this "
                        "query with a different collection search path (previously "
                        f"[{', '.join(existing_search.collections)}], now [{', '.join(collections)}])."
                    )
            if dimensions is None:
                dimensions = existing_search.dimensions
        else:
            if collections is None:
                collections = self._driver.get_default_collections()
        collections = tuple(ensure_iterable(collections))
        # Look up the data repository definition of the dataset type to check
        # for consistency, or get dimensions and storage class if we don't have
        # them.
        resolved_dataset_type = self._driver.get_dataset_type(dataset_type_name)
        resolved_dimensions = resolved_dataset_type.dimensions
        if dimensions is not None and dimensions != resolved_dimensions:
            raise DatasetTypeError(
                f"Given dimensions {dimensions} for dataset type {dataset_type_name!r} do not match the "
                f"registered dimensions {resolved_dimensions}."
            )
        if storage_class_name is not None:
            if storage_class_name != resolved_dataset_type.storageClass_name:
                if not allow_storage_class_overrides:
                    raise InvalidQueryError(
                        f"Storage class {storage_class_name!r} for dataset type {dataset_type!r} differs "
                        f"from repository definition {resolved_dataset_type.storageClass_name!r}, but "
                        "join_dataset_search does not are about storage classes and cannot record this "
                        "override.  Pass the override to `Query.datasets` instead."
                    )
                if not (
                    StorageClassFactory()
                    .getStorageClass(storage_class_name)
                    .can_convert(resolved_dataset_type.storageClass)
                ):
                    raise DatasetTypeError(
                        f"Given storage class {storage_class_name!r} for {dataset_type_name!r} is not "
                        f"compatible with repository storage class {resolved_dataset_type.storageClass_name}."
                    )
        else:
            storage_class_name = resolved_dataset_type.storageClass_name
        dataset_search = DatasetSearch.model_construct(
            collections=collections,
            dimensions=resolved_dimensions,
        )
        return (
            dataset_type_name,
            storage_class_name,
            Query(self._driver, self._tree.join_dataset(dataset_type_name, dataset_search)),
        )
