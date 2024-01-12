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

__all__ = ("QueryDriver", "PageKey", "ResultSpec", "ResultPage")

import uuid
from abc import abstractmethod
from collections.abc import Iterable
from contextlib import AbstractContextManager
from types import EllipsisType
from typing import Annotated, TypeAlias, Union, overload

import pydantic

from .._dataset_type import DatasetType
from ..dimensions import DataIdValue, DimensionGroup, DimensionUniverse
from .data_coordinate_results import DataCoordinateResultPage, DataCoordinateResultSpec
from .dataset_results import DatasetRefResultPage, DatasetRefResultSpec
from .dimension_record_results import DimensionRecordResultPage, DimensionRecordResultSpec
from .general_results import GeneralResultPage, GeneralResultSpec
from .tree import DataCoordinateUploadKey, MaterializationKey, QueryTree

PageKey: TypeAlias = uuid.UUID


ResultSpec: TypeAlias = Annotated[
    Union[DataCoordinateResultSpec, DimensionRecordResultSpec, DatasetRefResultSpec, GeneralResultSpec],
    pydantic.Field(discriminator="result_type"),
]


ResultPage: TypeAlias = Annotated[
    Union[DataCoordinateResultPage, DimensionRecordResultPage, DatasetRefResultPage, GeneralResultPage],
    pydantic.Field(discriminator=lambda x: x.spec.result_type),
]


class QueryDriver(AbstractContextManager[None]):
    """Base class for the implementation object inside `Query2` objects
    that is specialized for DirectButler vs. RemoteButler.

    Notes
    -----
    Implementations should be context managers.  This allows them to manage the
    lifetime of server-side state, such as:

    - a SQL transaction, when necessary (DirectButler);
    - SQL cursors for queries that were not fully iterated over (DirectButler);
    - temporary database tables (DirectButler);
    - result-page Parquet files that were never fetched (RemoteButler);
    - uploaded Parquet files used to fill temporary database tables
      (RemoteButler);
    - cached content needed to construct SQL queries, like collection summaries
      (potentially all Butlers).

    When possible, these sorts of things should be cleaned up earlier when they
    are no longer needed, and the Butler server will still have to guard
    against the context manager's ``__exit__`` signal never reaching it, but a
    context manager will take care of these much more often than relying on
    garbage collection and ``__del__`` would.
    """

    @property
    @abstractmethod
    def universe(self) -> DimensionUniverse:
        """Object that defines all dimensions."""
        raise NotImplementedError()

    @overload
    def execute(self, tree: QueryTree, result_spec: DataCoordinateResultSpec) -> DataCoordinateResultPage:
        ...

    @overload
    def execute(self, tree: QueryTree, result_spec: DimensionRecordResultSpec) -> DimensionRecordResultPage:
        ...

    @overload
    def execute(self, tree: QueryTree, result_spec: DatasetRefResultSpec) -> DatasetRefResultPage:
        ...

    @overload
    def execute(self, tree: QueryTree, result_spec: GeneralResultSpec) -> GeneralResultPage:
        ...

    @abstractmethod
    def execute(self, tree: QueryTree, result_spec: ResultSpec) -> ResultPage:
        """Execute a query and return the first result page.

        Parameters
        ----------
        tree : `QueryTree`
            Query tree to evaluate.
        result_spec : `ResultSpec`
            The kind of results the user wants from the query.  This can affect
            the actual query (i.e. SQL and Python postprocessing) that is run,
            e.g. by changing what is in the SQL SELECT clause and even what
            tables are joined in, but it never changes the number or order of
            result rows.

        Returns
        -------
        first_page : `ResultPage`
            A page whose type corresponds to the type of ``result_spec``, with
            at least the initial rows from the query.  This should have an
            empty ``rows`` attribute if the query returned no results, and a
            ``next_key`` attribute that is not `None` if there were more
            results than could be returned in a single page.
        """
        raise NotImplementedError()

    @overload
    def fetch_next_page(
        self, result_spec: DataCoordinateResultSpec, key: PageKey
    ) -> DataCoordinateResultPage:
        ...

    @overload
    def fetch_next_page(
        self, result_spec: DimensionRecordResultSpec, key: PageKey
    ) -> DimensionRecordResultPage:
        ...

    @overload
    def fetch_next_page(self, result_spec: DatasetRefResultSpec, key: PageKey) -> DatasetRefResultPage:
        ...

    @overload
    def fetch_next_page(self, result_spec: GeneralResultSpec, key: PageKey) -> GeneralResultPage:
        ...

    @abstractmethod
    def fetch_next_page(self, result_spec: ResultSpec, key: PageKey) -> ResultPage:
        """Fetch the next page of results from an already-executed query.

        Parameters
        ----------
        result_spec : `ResultSpec`
            The kind of results the user wants from the query.  This must be
            identical to the ``result_spec`` passed to `execute`, but
            implementations are not *required* to check this.
        key : `PageKey`
            Key included in the previous page from this query.  This key may
            become unusable or even be reused after this call.

        Returns
        -------
        next_page : `ResultPage`
            The next page of query results.
        """
        # We can put off dealing with pagination initially by just making an
        # implementation of this method raise.
        #
        # In RemoteButler I expect this to work by having the call to execute
        # continue to write Parquet files (or whatever) to some location until
        # its cursor is exhausted, and then delete those files as they are
        # fetched (or, failing that, when receiving a signal from
        # ``__exit__``).
        #
        # In DirectButler I expect to have a dict[PageKey, Cursor], fetch a
        # blocks of rows from it, and just reuse the page key for the next page
        # until the cursor is exactly.
        raise NotImplementedError()

    @abstractmethod
    def materialize(
        self,
        tree: QueryTree,
        dimensions: DimensionGroup,
        datasets: frozenset[str],
    ) -> tuple[MaterializationKey, frozenset[str]]:
        """Execute a query tree, saving results to temporary storage for use
        in later queries.

        Parameters
        ----------
        tree : `QueryTree`
            Query tree to evaluate.
        dimensions : `DimensionGroup`
            Dimensions whose key columns should be preserved.
        datasets : `frozenset` [ `str` ]
            Names of dataset types whose ID columns should be materialized
            if they are fully resolved.

        Returns
        -------
        key
            Unique identifier for the result rows that allows them to be
            referenced in a `QueryTree`.
        resolved_datasets : `frozenset` [  `str` ]
            Names of dataset types that were resolved and had their UUIDs
            materialized.
        """
        raise NotImplementedError()

    @abstractmethod
    def upload_data_coordinates(
        self, dimensions: DimensionGroup, rows: Iterable[tuple[DataIdValue, ...]]
    ) -> DataCoordinateUploadKey:
        """Upload a table of data coordinates for use in later queries.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions of the data coordinates.
        rows : `Iterable` [ `tuple` ]
            Tuples of data coordinate values, covering just the "required"
            subset of ``dimensions``.

        Returns
        -------
        key
            Unique identifier for the upload that allows it to be referenced in
            a `QueryTree`.
        """
        raise NotImplementedError()

    @abstractmethod
    def count(
        self,
        tree: QueryTree,
        *,
        dimensions: DimensionGroup,
        datasets: frozenset[str],
        exact: bool,
        discard: bool,
    ) -> int:
        """Return the number of rows a query would return.

        Parameters
        ----------
        tree : `QueryTree`
            Query tree to evaluate.
        dimensions : `DimensionGroup`
            Dimension keys whose distinct rows should be counted.  Must be a
            subset of ``tree.dimensions``.
        datasets : `frozenset` [ `str` ]
            Datasets whose IDs might also count towards row distinctness, if
            they are unresolved.
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
        """
        raise NotImplementedError()

    @abstractmethod
    def any(self, tree: QueryTree, *, execute: bool, exact: bool) -> bool:
        """Test whether the query would return any rows.

        Parameters
        ----------
        tree : `QueryTree`
            Query tree to evaluate.
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
    def explain_no_results(self, tree: QueryTree, execute: bool) -> Iterable[str]:
        """Return human-readable messages that may help explain why the query
        yields no results.

        Parameters
        ----------
        tree : `QueryTree`
            Query tree to evaluate.
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

    @abstractmethod
    def resolve_collection_wildcard(
        self, collections: str | Iterable[str] | EllipsisType | None = None
    ) -> tuple[list[str], bool]:
        """Resolve a collection argument into a sequence of collection names.

        Parameters
        ----------
        collections : `str`, `~collections.abc.Iterable` [ `str` ], ``...``,
                or `None`, optional
            Collection search path argument.  If `None`, the default
            collections for the client should be used, if there are any.

        Returns
        -------
        matched : `list` [ `str` ]
            Matching collection names.  `~CollectionType.CHAINED` collections
            are included directly rather than flattened.
        ordered : `bool`
            If `True`, the expression specified an order that can be used in
            a find-first search.
        """
        raise NotImplementedError()

    @abstractmethod
    def resolve_dataset_type_wildcard(
        self, dataset_type: str | DatasetType | Iterable[str] | Iterable[DatasetType] | EllipsisType
    ) -> dict[str, DatasetType]:
        """Resolve a dataset type argument into a mapping of `DatasetType`
        objects.

        Parameters
        ----------
        dataset_type : `str`, `DatasetType`, `~collections.abc.Iterable` \
                [ `str` ], `~collections.abc.Iterable` [ `DatasetType` ], \
                or ``...``
            Dataset type name, object, or wildcard to resolve.

        Returns
        -------
        matched : `dict` [ `str`, `DatasetType` ]
            Mapping from dataset type name to dataset type.  Storage classes
            passed in should be preserved, but component dataset types should
            result in an exception.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_dataset_dimensions(self, name: str) -> DimensionGroup:
        """Return the dimensions for a dataset type.

        Parameters
        ----------
        name : `str`
            Name of the dataset type.

        Returns
        -------
        dimensions : `DimensionGroup`
            Dimensions of the dataset type.
        """
        raise NotImplementedError()
