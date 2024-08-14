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

__all__ = (
    "QueryDriver",
    "ResultPage",
    "DataCoordinateResultPage",
    "DimensionRecordResultPage",
    "DatasetRefResultPage",
    "GeneralResultPage",
)

import dataclasses
from abc import abstractmethod
from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager
from typing import Any, TypeAlias, Union, overload

from .._dataset_ref import DatasetRef
from .._dataset_type import DatasetType
from ..dimensions import (
    DataCoordinate,
    DataIdValue,
    DimensionGroup,
    DimensionRecord,
    DimensionRecordSet,
    DimensionRecordTable,
    DimensionUniverse,
)
from .result_specs import (
    DataCoordinateResultSpec,
    DatasetRefResultSpec,
    DimensionRecordResultSpec,
    GeneralResultSpec,
    ResultSpec,
)
from .tree import DataCoordinateUploadKey, MaterializationKey, QueryTree

# The Page types below could become Pydantic models instead of dataclasses if
# that makes them more directly usable by RemoteButler (at least once we have
# Pydantic-friendly containers for all of them).  We may want to add a
# discriminator annotation to the ResultPage union if we do that.


@dataclasses.dataclass
class DataCoordinateResultPage:
    """A single page of results from a data coordinate query."""

    spec: DataCoordinateResultSpec

    # TODO: On DM-41114 this will become a custom container that normalizes out
    # attached DimensionRecords and is Pydantic-friendly.
    rows: list[DataCoordinate]


@dataclasses.dataclass
class DimensionRecordResultPage:
    """A single page of results from a dimension record query."""

    spec: DimensionRecordResultSpec
    rows: Iterable[DimensionRecord]

    def as_table(self) -> DimensionRecordTable:
        if isinstance(self.rows, DimensionRecordTable):
            return self.rows
        else:
            return DimensionRecordTable(self.spec.element, self.rows)

    def as_set(self) -> DimensionRecordSet:
        if isinstance(self.rows, DimensionRecordSet):
            return self.rows
        else:
            return DimensionRecordSet(self.spec.element, self.rows)


@dataclasses.dataclass
class DatasetRefResultPage:
    """A single page of results from a dataset query."""

    spec: DatasetRefResultSpec

    # TODO: On DM-41115 this will become a custom container that normalizes out
    # attached DimensionRecords and is Pydantic-friendly.
    rows: list[DatasetRef]


@dataclasses.dataclass
class GeneralResultPage:
    """A single page of results from a general query."""

    spec: GeneralResultSpec

    # Raw tabular data, with columns in the same order as
    # spec.get_result_columns().
    rows: list[tuple[Any, ...]]


ResultPage: TypeAlias = Union[
    DataCoordinateResultPage, DimensionRecordResultPage, DatasetRefResultPage, GeneralResultPage
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
    - cached content needed to construct query trees, like collection summaries
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
    def execute(
        self, result_spec: DataCoordinateResultSpec, tree: QueryTree
    ) -> Iterator[DataCoordinateResultPage]: ...

    @overload
    def execute(
        self, result_spec: DimensionRecordResultSpec, tree: QueryTree
    ) -> Iterator[DimensionRecordResultPage]: ...

    @overload
    def execute(
        self, result_spec: DatasetRefResultSpec, tree: QueryTree
    ) -> Iterator[DatasetRefResultPage]: ...

    @overload
    def execute(self, result_spec: GeneralResultSpec, tree: QueryTree) -> Iterator[GeneralResultPage]: ...

    @abstractmethod
    def execute(self, result_spec: ResultSpec, tree: QueryTree) -> Iterator[ResultPage]:
        """Execute a query and return the first result page.

        Parameters
        ----------
        result_spec : `ResultSpec`
            The kind of results the user wants from the query.  This can affect
            the actual query (i.e. SQL and Python postprocessing) that is run,
            e.g. by changing what is in the SQL SELECT clause and even what
            tables are joined in, but it never changes the number or order of
            result rows.
        tree : `QueryTree`
            Query tree to evaluate.

        Yields
        ------
        page : `ResultPage`
            A page whose type corresponds to the type of ``result_spec``, with
            rows from the query.
        """
        raise NotImplementedError()

    @abstractmethod
    def materialize(
        self,
        tree: QueryTree,
        dimensions: DimensionGroup,
        datasets: frozenset[str],
    ) -> MaterializationKey:
        """Execute a query tree, saving results to temporary storage for use
        in later queries.

        Parameters
        ----------
        tree : `QueryTree`
            Query tree to evaluate.
        dimensions : `DimensionGroup`
            Dimensions whose key columns should be preserved.
        datasets : `frozenset` [ `str` ]
            Names of dataset types whose ID columns may be materialized.  It
            is implementation-defined whether they actually are.

        Returns
        -------
        key : `MaterializationKey`
            Unique identifier for the result rows that allows them to be
            referenced in a `QueryTree`.
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
        result_spec: ResultSpec,
        *,
        exact: bool,
        discard: bool,
    ) -> int:
        """Return the number of rows a query would return.

        Parameters
        ----------
        tree : `QueryTree`
            Query tree to evaluate.
        result_spec : `ResultSpec`
            The kind of results the user wants to count.
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
    def get_default_collections(self) -> tuple[str, ...]:
        """Return the default collection search path.

        Returns
        -------
        collections : `tuple` [ `str`, ... ]
            The default collection search path as a tuple of `str`.

        Raises
        ------
        NoDefaultCollectionError
            Raised if there are no default collections.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_dataset_type(self, name: str) -> DatasetType:
        """Return the dimensions for a dataset type.

        Parameters
        ----------
        name : `str`
            Name of the dataset type.

        Returns
        -------
        dataset_type : `DatasetType`
            Dimensions of the dataset type.

        Raises
        ------
        MissingDatasetTypeError
            Raised if the dataset type is not registered.
        """
        raise NotImplementedError()
