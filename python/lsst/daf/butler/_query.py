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

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._query_results import DataCoordinateQueryResults, DatasetQueryResults, DimensionRecordQueryResults
    from .dimensions import DataId, DimensionGroup
    from .registry._registry import CollectionArgType


class Query(ABC):
    """Interface for construction and execution of complex queries."""

    @abstractmethod
    def data_ids(
        self,
        dimensions: DimensionGroup | Iterable[str] | str,
        *,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> DataCoordinateQueryResults:
        """Query for data IDs matching user-provided criteria.

        Parameters
        ----------
        dimensions : `DimensionGroup`, `str`, or \
                `~collections.abc.Iterable` [`str`]
            The dimensions of the data IDs to yield, as either `DimensionGroup`
            instances or `str`.  Will be automatically expanded to a complete
            `DimensionGroup`.
        data_id : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.  See
            :ref:`daf_butler_dimension_expressions` for more information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``data_id``
            argument (and may be used to provide a constraining data ID even
            when the ``data_id`` argument is `None`).

        Returns
        -------
        dataIds : `DataCoordinateQueryResults`
            Data IDs matching the given query parameters.  These are guaranteed
            to identify all dimensions (`DataCoordinate.hasFull` returns
            `True`), but will not contain `DimensionRecord` objects
            (`DataCoordinate.hasRecords` returns `False`).  Call
            `~DataCoordinateQueryResults.expanded` on the
            returned object to fetch those (and consider using
            `~DataCoordinateQueryResults.materialize` on the
            returned object first if the expected number of rows is very
            large). See documentation for those methods for additional
            information.

        Raises
        ------
        lsst.daf.butler.registry.DataIdError
            Raised when ``data_id`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.
        """
        raise NotImplementedError()

    @abstractmethod
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
        """Query for and iterate over dataset references matching user-provided
        criteria.

        Parameters
        ----------
        dataset_type : dataset type expression
            An expression that fully or partially identifies the dataset types
            to be queried.  Allowed types include `DatasetType`, `str`,
            `re.Pattern`, and iterables thereof.  The special value ``...`` can
            be used to query all dataset types.  See
            :ref:`daf_butler_dataset_type_expressions` for more information.
        collections : collection expression, optional
            An expression that identifies the collections to search, such as a
            `str` (for full matches or partial matches via globs), `re.Pattern`
            (for partial matches), or iterable thereof.  ``...`` can be used to
            search all collections (actually just all `~CollectionType.RUN`
            collections, because this will still find all datasets).
            If not provided, the default collections are used.  See
            :ref:`daf_butler_collection_expressions` for more information.
        find_first : `bool`, optional
            If `True` (default), for each result data ID, only yield one
            `DatasetRef` of each `DatasetType`, from the first collection in
            which a dataset of that dataset type appears (according to the
            order of ``collections`` passed in).  If `True`, ``collections``
            must not contain regular expressions and may not be ``...``.
        data_id : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  May involve
            any column of a dimension table or (as a shortcut for the primary
            key column of a dimension table) dimension name.  See
            :ref:`daf_butler_dimension_expressions` for more information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``data_id``
            argument (and may be used to provide a constraining data ID even
            when the ``data_id`` argument is `None`).

        Returns
        -------
        refs : `.queries.DatasetQueryResults`
            Dataset references matching the given query criteria.  Nested data
            IDs are guaranteed to include values for all implied dimensions
            (i.e. `DataCoordinate.hasFull` will return `True`), but will not
            include dimension records (`DataCoordinate.hasRecords` will be
            `False`) unless `~.queries.DatasetQueryResults.expanded` is
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
        lsst.daf.butler.registry.DataIdError
            Raised when ``data_id`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.

        Notes
        -----
        When multiple dataset types are queried in a single call, the
        results of this operation are equivalent to querying for each dataset
        type separately in turn, and no information about the relationships
        between datasets of different types is included.
        """
        raise NotImplementedError()

    @abstractmethod
    def dimension_records(
        self,
        element: str,
        *,
        data_id: DataId | None = None,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> DimensionRecordQueryResults:
        """Query for dimension information matching user-provided criteria.

        Parameters
        ----------
        element : `str`
            The name of a dimension element to obtain records for.
        data_id : `dict` or `DataCoordinate`, optional
            A data ID whose key-value pairs are used as equality constraints
            in the query.
        where : `str`, optional
            A string expression similar to a SQL WHERE clause.  See
            `queryDataIds` and :ref:`daf_butler_dimension_expressions` for more
            information.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
            Values of collection type can be expanded in some cases; see
            :ref:`daf_butler_dimension_expressions_identifiers` for more
            information.
        **kwargs
            Additional keyword arguments are forwarded to
            `DataCoordinate.standardize` when processing the ``data_id``
            argument (and may be used to provide a constraining data ID even
            when the ``data_id`` argument is `None`).

        Returns
        -------
        records : `.queries.DimensionRecordQueryResults`
            Data IDs matching the given query parameters.

        Raises
        ------
        lsst.daf.butler.registry.NoDefaultCollectionError
            Raised if ``collections`` is `None` and
            ``self.defaults.collections`` is `None`.
        lsst.daf.butler.registry.CollectionExpressionError
            Raised when ``collections`` expression is invalid.
        lsst.daf.butler.registry.DataIdError
            Raised when ``data_id`` or keyword arguments specify unknown
            dimensions or values, or when they contain inconsistent values.
        lsst.daf.butler.registry.DatasetTypeExpressionError
            Raised when ``datasetType`` expression is invalid.
        lsst.daf.butler.registry.UserExpressionError
            Raised when ``where`` expression is invalid.
        """
        raise NotImplementedError()
