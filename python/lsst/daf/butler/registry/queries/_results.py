# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
    "ChainedDatasetQueryResults",
    "DatabaseDimensionRecordQueryResults",
    "DataCoordinateQueryResults",
    "DatasetQueryResults",
    "DimensionRecordQueryResults",
    "ParentDatasetQueryResults",
)

import itertools
import operator
from abc import abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, ExitStack, contextmanager
from typing import Any, Optional

import sqlalchemy

from ...core import (
    DataCoordinate,
    DataCoordinateIterable,
    DatasetRef,
    DatasetType,
    Dimension,
    DimensionGraph,
    DimensionRecord,
    SimpleQuery,
)
from ..interfaces import Database, DimensionRecordStorage
from ._query import Query
from ._structs import ElementOrderByClause, QuerySummary

QueryFactoryMethod = Callable[[Optional[Iterable[str]], Optional[tuple[int, Optional[int]]]], Query]
"""Type of a query factory method type used by DataCoordinateQueryResults.
"""


class DataCoordinateQueryResults(DataCoordinateIterable):
    """An enhanced implementation of `DataCoordinateIterable` that represents
    data IDs retrieved from a database query.

    Parameters
    ----------
    db : `Database`
        Database engine used to execute queries.
    query_factory : `QueryFactoryMethod`
        Method which creates an instance of `Query` class.
    graph : `DimensionGraph`
        Dimensions used by query.
    order_by : `Iterable` [ `str` ], optional
        Optional sequence of column names used for result ordering.
    limit : `Tuple` [ `int`, `int` ], optional
        Limit for the number of returned records and optional offset.
    records : `Mapping`, optional
        A nested mapping containing `DimensionRecord` objects for all
        dimensions and all data IDs this query will yield.  If `None`
        (default), `DataCoordinateIterable.hasRecords` will return `False`.
        The outer mapping has `str` keys (the names of dimension elements).
        The inner mapping has `tuple` keys representing data IDs (tuple
        conversions of `DataCoordinate.values()`) and `DimensionRecord` values.

    Notes
    -----
    Constructing an instance of this does nothing; the query is not executed
    until it is iterated over (or some other operation is performed that
    involves iteration).

    Instances should generally only be constructed by `Registry` methods or the
    methods of other query result objects.
    """

    def __init__(
        self,
        db: Database,
        query_factory: QueryFactoryMethod,
        graph: DimensionGraph,
        *,
        order_by: Iterable[str] | None = None,
        limit: tuple[int, int | None] | None = None,
        records: Mapping[str, Mapping[tuple, DimensionRecord]] | None = None,
    ):
        self._db = db
        self._query_factory = query_factory
        self._graph = graph
        self._order_by = order_by
        self._limit = limit
        self._records = records
        self._cached_query: Query | None = None

    __slots__ = ("_db", "_query_factory", "_graph", "_order_by", "_limit", "_records", "_cached_query")

    @classmethod
    def from_query(
        cls,
        db: Database,
        query: Query,
        graph: DimensionGraph,
        *,
        order_by: Iterable[str] | None = None,
        limit: tuple[int, int | None] | None = None,
        records: Mapping[str, Mapping[tuple, DimensionRecord]] | None = None,
    ) -> DataCoordinateQueryResults:
        """Make an instance from a pre-existing query instead of a factory.

        Parameters
        ----------
        db : `Database`
            Database engine used to execute queries.
        query : `Query`
            Low-level representation of the query that backs this result
            object.
        graph : `DimensionGraph`
            Dimensions used by query.
        order_by : `Iterable` [ `str` ], optional
            Optional sequence of column names used for result ordering.
        limit : `Tuple` [ `int`, `int` ], optional
            Limit for the number of returned records and optional offset.
        records : `Mapping`, optional
            A nested mapping containing `DimensionRecord` objects for all
            dimensions and all data IDs this query will yield.  If `None`
            (default), `DataCoordinateIterable.hasRecords` will return `False`.
            The outer mapping has `str` keys (the names of dimension elements).
            The inner mapping has `tuple` keys representing data IDs (tuple
            conversions of `DataCoordinate.values()`) and `DimensionRecord`
            values.
        """

        def factory(order_by: Iterable[str] | None, limit: tuple[int, int | None] | None) -> Query:
            return query

        return DataCoordinateQueryResults(db, factory, graph, order_by=order_by, limit=limit, records=records)

    def __iter__(self) -> Iterator[DataCoordinate]:
        return (self._query.extractDataId(row, records=self._records) for row in self._query.rows(self._db))

    def __repr__(self) -> str:
        return f"<DataCoordinate iterator with dimensions={self._graph}>"

    def _clone(
        self,
        *,
        query_factory: QueryFactoryMethod | None = None,
        query: Query | None = None,
        graph: DimensionGraph | None = None,
        order_by: Iterable[str] | None = None,
        limit: tuple[int, int | None] | None = None,
        records: Mapping[str, Mapping[tuple, DimensionRecord]] | None = None,
    ) -> DataCoordinateQueryResults:
        """Clone this instance potentially updating some attributes."""
        graph = graph if graph is not None else self._graph
        order_by = order_by if order_by is not None else self._order_by
        limit = limit if limit is not None else self._limit
        records = records if records is not None else self._records
        if query is None:
            query_factory = query_factory or self._query_factory
            return DataCoordinateQueryResults(
                self._db, query_factory, graph, order_by=order_by, limit=limit, records=records
            )
        else:
            return DataCoordinateQueryResults.from_query(
                self._db, query, graph, order_by=order_by, limit=limit, records=records
            )

    @property
    def _query(self) -> Query:
        """Query representation instance (`Query`)"""
        if self._cached_query is None:
            self._cached_query = self._query_factory(self._order_by, self._limit)
            assert (
                self._cached_query.datasetType is None
            ), "Query used to initialize data coordinate results should not have any datasets."
        return self._cached_query

    @property
    def graph(self) -> DimensionGraph:
        # Docstring inherited from DataCoordinateIterable.
        return self._graph

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return True

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._records is not None or not self._graph

    @contextmanager
    def materialize(self) -> Iterator[DataCoordinateQueryResults]:
        """Insert this query's results into a temporary table.

        Returns
        -------
        context : `typing.ContextManager` [ `DataCoordinateQueryResults` ]
            A context manager that ensures the temporary table is created and
            populated in ``__enter__`` (returning a results object backed by
            that table), and dropped in ``__exit__``.  If ``self`` is already
            materialized, the context manager may do nothing (reflecting the
            fact that an outer context manager should already take care of
            everything else).

        Notes
        -----
        When using a very large result set to perform multiple queries (e.g.
        multiple calls to `subset` with different arguments, or even a single
        call to `expanded`), it may be much more efficient to start by
        materializing the query and only then performing the follow up queries.
        It may also be less efficient, depending on how well database engine's
        query optimizer can simplify those particular follow-up queries and
        how efficiently it caches query results even when the are not
        explicitly inserted into a temporary table.  See `expanded` and
        `subset` for examples.
        """
        with self._query.materialize(self._db) as materialized:
            # Note that we depend on order_by columns to be passes from Query
            # to MaterializedQuery, so order_by and limit are not used.
            yield self._clone(query=materialized)

    def expanded(self) -> DataCoordinateQueryResults:
        """Return a results object for which `hasRecords` returns `True`.

        This method may involve actually executing database queries to fetch
        `DimensionRecord` objects.

        Returns
        -------
        results : `DataCoordinateQueryResults`
            A results object for which `hasRecords` returns `True`.  May be
            ``self`` if that is already the case.

        Notes
        -----
        For very result sets, it may be much more efficient to call
        `materialize` before calling `expanded`, to avoid performing the
        original query multiple times (as a subquery) in the follow-up queries
        that fetch dimension records.  For example::

            with registry.queryDataIds(...).materialize() as tempDataIds:
                dataIdsWithRecords = tempDataIds.expanded()
                for dataId in dataIdsWithRecords:
                    ...
        """
        if self._records is None:
            records = {}
            for element in self.graph.elements:
                subset = self.subset(graph=element.graph, unique=True)
                records[element.name] = {
                    tuple(record.dataId.values()): record
                    for record in self._query.backend.managers.dimensions[element].fetch(subset)
                }

            return self._clone(query=self._query, records=records)
        else:
            return self

    def subset(
        self, graph: DimensionGraph | None = None, *, unique: bool = False
    ) -> DataCoordinateQueryResults:
        """Return a results object containing a subset of the dimensions of
        this one, and/or a unique near-subset of its rows.

        This method may involve actually executing database queries to fetch
        `DimensionRecord` objects.

        Parameters
        ----------
        graph : `DimensionGraph`, optional
            Dimensions to include in the new results object.  If `None`,
            ``self.graph`` is used.
        unique : `bool`, optional
            If `True` (`False` is default), the query should only return unique
            data IDs.  This is implemented in the database; to obtain unique
            results via Python-side processing (which may be more efficient in
            some cases), use `toSet` to construct a `DataCoordinateSet` from
            this results object instead.

        Returns
        -------
        results : `DataCoordinateQueryResults`
            A results object corresponding to the given criteria.  May be
            ``self`` if it already qualifies.

        Raises
        ------
        ValueError
            Raised when ``graph`` is not a subset of the dimension graph in
            this result.

        Notes
        -----
        This method can only return a "near-subset" of the original result rows
        in general because of subtleties in how spatial overlaps are
        implemented; see `Query.subset` for more information.

        When calling `subset` multiple times on the same very large result set,
        it may be much more efficient to call `materialize` first.  For
        example::

            dimensions1 = DimensionGraph(...)
            dimensions2 = DimensionGraph(...)
            with registry.queryDataIds(...).materialize() as tempDataIds:
                for dataId1 in tempDataIds.subset(
                        graph=dimensions1,
                        unique=True):
                    ...
                for dataId2 in tempDataIds.subset(
                        graph=dimensions2,
                        unique=True):
                    ...
        """
        if graph is None:
            graph = self.graph
        if not graph.issubset(self.graph):
            raise ValueError(f"{graph} is not a subset of {self.graph}")
        if graph == self.graph and (not unique or self._query.isUnique()):
            return self
        records: Mapping[str, Mapping[tuple, DimensionRecord]] | None
        if self._records is not None:
            records = {element.name: self._records[element.name] for element in graph.elements}
        else:
            records = None
        query = self._query.subset(graph=graph, datasets=False, unique=unique)

        return self._clone(graph=graph, query=query, records=records)

    def constrain(self, query: SimpleQuery, columns: Callable[[str], sqlalchemy.sql.ColumnElement]) -> None:
        # Docstring inherited from DataCoordinateIterable.
        sql = self._query.sql
        if sql is not None:
            fromClause = sql.alias("c")
            query.join(
                fromClause,
                onclause=sqlalchemy.sql.and_(
                    *[
                        columns(dimension.name) == fromClause.columns[dimension.name]
                        for dimension in self.graph.required
                    ]
                ),
            )

    def findDatasets(
        self,
        datasetType: DatasetType | str,
        collections: Any,
        *,
        findFirst: bool = True,
        components: bool | None = None,
    ) -> ParentDatasetQueryResults:
        """Find datasets using the data IDs identified by this query.

        Parameters
        ----------
        datasetType : `DatasetType` or `str`
            Dataset type or the name of one to search for.  Must have
            dimensions that are a subset of ``self.graph``.
        collections : `Any`
            An expression that fully or partially identifies the collections
            to search for the dataset, such as a `str`, `re.Pattern`, or
            iterable  thereof.  ``...`` can be used to return all collections.
            See :ref:`daf_butler_collection_expressions` for more information.
        findFirst : `bool`, optional
            If `True` (default), for each result data ID, only yield one
            `DatasetRef`, from the first collection in which a dataset of that
            dataset type appears (according to the order of ``collections``
            passed in).  If `True`, ``collections`` must not contain regular
            expressions and may not be ``...``.
        components : `bool`, optional
            If `True`, apply all expression patterns to component dataset type
            names as well.  If `False`, never apply patterns to components.  If
            `None` (default), apply patterns to components only if their parent
            datasets were not matched by the expression.  Fully-specified
            component datasets (`str` or `DatasetType` instances) are always
            included.

            Values other than `False` are deprecated, and only `False` will be
            supported after v26.  After v27 this argument will be removed
            entirely.

        Returns
        -------
        datasets : `ParentDatasetQueryResults`
            A lazy-evaluation object representing dataset query results,
            iterable over `DatasetRef` objects.  If ``self.hasRecords()``, all
            nested data IDs in those dataset references will have records as
            well.

        Raises
        ------
        ValueError
            Raised if ``datasetType.dimensions.issubset(self.graph) is False``.
        MissingDatasetTypeError
            Raised if the given dataset type is not registered.
        """
        parent_dataset_type, components_found = self._query.backend.resolve_single_dataset_type_wildcard(
            datasetType, components=components, explicit_only=True
        )
        if not parent_dataset_type.dimensions.issubset(self.graph):
            raise ValueError(
                f"findDatasets requires that the dataset type have only dimensions in "
                f"the DataCoordinateQueryResult used as input to the search, but "
                f"{parent_dataset_type.name} has dimensions {parent_dataset_type.dimensions}, "
                f"while the input dimensions are {self.graph}."
            )
        summary = QuerySummary(
            self.graph, whereRegion=self._query.whereRegion, datasets=[parent_dataset_type]
        )
        builder = self._query.makeBuilder(summary)
        builder.joinDataset(parent_dataset_type, collections=collections, findFirst=findFirst)
        query = builder.finish(joinMissing=False)
        return ParentDatasetQueryResults(
            db=self._db,
            query=query,
            components=components_found,
            records=self._records,
            datasetType=parent_dataset_type,
        )

    def count(self, *, exact: bool = True) -> int:
        """Count the number of rows this query would return.

        Parameters
        ----------
        exact : `bool`, optional
            If `True`, run the full query and perform post-query filtering if
            needed to account for that filtering in the count.  If `False`, the
            result may be an upper bound.

        Returns
        -------
        count : `int`
            The number of rows the query would return, or an upper bound if
            ``exact=False``.

        Notes
        -----
        This counts the number of rows returned, not the number of unique rows
        returned, so even with ``exact=True`` it may provide only an upper
        bound on the number of *deduplicated* result rows.
        """
        return self._query.count(self._db, exact=exact)

    def any(
        self,
        *,
        execute: bool = True,
        exact: bool = True,
    ) -> bool:
        """Test whether this query returns any results.

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
        return self._query.any(self._db, execute=execute, exact=exact)

    def explain_no_results(self) -> Iterable[str]:
        """Return human-readable messages that may help explain why the query
        yields no results.

        Returns
        -------
        messages : `Iterable` [ `str` ]
            String messages that describe reasons the query might not yield any
            results.

        Notes
        -----
        Messages related to post-query filtering are only available if the
        iterator has been exhausted, or if `any` or `count` was already called
        (with ``exact=True`` for the latter two).

        This method first yields messages that are generated while the query is
        being built or filtered, but may then proceed to diagnostics generated
        by performing what should be inexpensive follow-up queries.  Callers
        can short-circuit this at any time by simplying not iterating further.
        """
        return self._query.explain_no_results(self._db)

    def order_by(self, *args: str) -> DataCoordinateQueryResults:
        """Make the iterator return ordered result.

        Parameters
        ----------
        *args : `str`
            Names of the columns/dimensions to use for ordering. Column name
            can be prefixed with minus (``-``) to use descending ordering.

        Returns
        -------
        result : `DataCoordinateQueryResults`
            Returns ``self`` instance which is updated to return ordered
            result.

        Notes
        -----
        This method modifies the iterator in place and returns the same
        instance to support method chaining.
        """
        return self._clone(order_by=args)

    def limit(self, limit: int, offset: int | None = None) -> DataCoordinateQueryResults:
        """Make the iterator return limited number of records.

        Parameters
        ----------
        limit : `int`
            Upper limit on the number of returned records.
        offset : `int` or `None`
            If not `None` then the number of records to skip before returning
            ``limit`` records.

        Returns
        -------
        result : `DataCoordinateQueryResults`
            Returns ``self`` instance which is updated to return limited set
            of records.

        Notes
        -----
        This method modifies the iterator in place and returns the same
        instance to support method chaining. Normally this method is used
        together with `order_by` method.
        """
        return self._clone(limit=(limit, offset))


class DatasetQueryResults(Iterable[DatasetRef]):
    """An interface for objects that represent the results of queries for
    datasets.
    """

    @abstractmethod
    def byParentDatasetType(self) -> Iterator[ParentDatasetQueryResults]:
        """Group results by parent dataset type.

        Returns
        -------
        iter : `Iterator` [ `ParentDatasetQueryResults` ]
            An iterator over `DatasetQueryResults` instances that are each
            responsible for a single parent dataset type (either just that
            dataset type, one or more of its component dataset types, or both).
        """
        raise NotImplementedError()

    @abstractmethod
    def materialize(self) -> AbstractContextManager[DatasetQueryResults]:
        """Insert this query's results into a temporary table.

        Returns
        -------
        context : `typing.ContextManager` [ `DatasetQueryResults` ]
            A context manager that ensures the temporary table is created and
            populated in ``__enter__`` (returning a results object backed by
            that table), and dropped in ``__exit__``.  If ``self`` is already
            materialized, the context manager may do nothing (reflecting the
            fact that an outer context manager should already take care of
            everything else).
        """
        raise NotImplementedError()

    @abstractmethod
    def expanded(self) -> DatasetQueryResults:
        """Return a `DatasetQueryResults` for which `DataCoordinate.hasRecords`
        returns `True` for all data IDs in returned `DatasetRef` objects.

        Returns
        -------
        expanded : `DatasetQueryResults`
            Either a new `DatasetQueryResults` instance or ``self``, if it is
            already expanded.

        Notes
        -----
        As with `DataCoordinateQueryResults.expanded`, it may be more efficient
        to call `materialize` before expanding data IDs for very large result
        sets.
        """
        raise NotImplementedError()

    @abstractmethod
    def count(self, *, exact: bool = True) -> int:
        """Count the number of rows this query would return.

        Parameters
        ----------
        exact : `bool`, optional
            If `True`, run the full query and perform post-query filtering if
            needed to account for that filtering in the count.  If `False`, the
            result may be an upper bound.

        Returns
        -------
        count : `int`
            The number of rows the query would return, or an upper bound if
            ``exact=False``.

        Notes
        -----
        This counts the number of rows returned, not the number of unique rows
        returned, so even with ``exact=True`` it may provide only an upper
        bound on the number of *deduplicated* result rows.
        """
        raise NotImplementedError()

    @abstractmethod
    def any(
        self,
        *,
        execute: bool = True,
        exact: bool = True,
    ) -> bool:
        """Test whether this query returns any results.

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
    def explain_no_results(self) -> Iterable[str]:
        """Return human-readable messages that may help explain why the query
        yields no results.

        Returns
        -------
        messages : `Iterable` [ `str` ]
            String messages that describe reasons the query might not yield any
            results.

        Notes
        -----
        Messages related to post-query filtering are only available if the
        iterator has been exhausted, or if `any` or `count` was already called
        (with ``exact=True`` for the latter two).

        This method first yields messages that are generated while the query is
        being built or filtered, but may then proceed to diagnostics generated
        by performing what should be inexpensive follow-up queries.  Callers
        can short-circuit this at any time by simplying not iterating further.
        """
        raise NotImplementedError()


class ParentDatasetQueryResults(DatasetQueryResults):
    """An object that represents results from a query for datasets with a
    single parent `DatasetType`.

    Parameters
    ----------
    db : `Database`
        Database engine to execute queries against.
    query : `Query`
        Low-level query object that backs these results.  ``query.datasetType``
        will be the parent dataset type for this object, and may not be `None`.
    components : `Sequence` [ `str` or `None` ]
        Names of components to include in iteration.  `None` may be included
        (at most once) to include the parent dataset type.
    records : `Mapping`, optional
        Mapping containing `DimensionRecord` objects for all dimensions and
        all data IDs this query will yield.  If `None` (default),
        `DataCoordinate.hasRecords` will return `False` for all nested data
        IDs.  This is a nested mapping with `str` names of dimension elements
        as outer keys, `DimensionRecord` instances as inner values, and
        ``tuple(record.dataId.values())`` for the inner keys / outer values
        (where ``record`` is the innermost `DimensionRecord` instance).
    datasetType : `DatasetType`, optional
        Parent dataset type for all datasets returned by this query.  If not
        provided, ``query.datasetType`` be used, and must not be `None` (as it
        is in the case where the query is known to yield no results prior to
        execution).
    """

    def __init__(
        self,
        db: Database,
        query: Query,
        *,
        components: Sequence[str | None],
        records: Mapping[str, Mapping[tuple, DimensionRecord]] | None = None,
        datasetType: DatasetType | None = None,
    ):
        self._db = db
        self._query = query
        self._components = components
        self._records = records
        if datasetType is None:
            datasetType = query.datasetType
        assert datasetType is not None, "Query used to initialize dataset results must have a dataset."
        assert datasetType.dimensions.issubset(
            query.graph
        ), f"Query dimensions {query.graph} do not match dataset type dimensions {datasetType.dimensions}."
        self._datasetType = datasetType

    __slots__ = ("_db", "_query", "_dimensions", "_components", "_records")

    def __iter__(self) -> Iterator[DatasetRef]:
        for row in self._query.rows(self._db):
            parentRef = self._query.extractDatasetRef(row, records=self._records)
            for component in self._components:
                if component is None:
                    yield parentRef
                else:
                    yield parentRef.makeComponentRef(component)

    def __repr__(self) -> str:
        return f"<DatasetRef iterator for [components of] {self._datasetType.name}>"

    def byParentDatasetType(self) -> Iterator[ParentDatasetQueryResults]:
        # Docstring inherited from DatasetQueryResults.
        yield self

    @contextmanager
    def materialize(self) -> Iterator[ParentDatasetQueryResults]:
        # Docstring inherited from DatasetQueryResults.
        with self._query.materialize(self._db) as materialized:
            yield ParentDatasetQueryResults(
                self._db, materialized, components=self._components, records=self._records
            )

    @property
    def parentDatasetType(self) -> DatasetType:
        """The parent dataset type for all datasets in this iterable
        (`DatasetType`).
        """
        return self._datasetType

    @property
    def dataIds(self) -> DataCoordinateQueryResults:
        """A lazy-evaluation object representing a query for just the data
        IDs of the datasets that would be returned by this query
        (`DataCoordinateQueryResults`).

        The returned object is not in general `zip`-iterable with ``self``;
        it may be in a different order or have (or not have) duplicates.
        """
        query = self._query.subset(graph=self.parentDatasetType.dimensions, datasets=False, unique=False)
        return DataCoordinateQueryResults.from_query(
            self._db,
            query,
            self.parentDatasetType.dimensions,
            records=self._records,
        )

    def withComponents(self, components: Sequence[str | None]) -> ParentDatasetQueryResults:
        """Return a new query results object for the same parent datasets but
        different components.

        components :  `Sequence` [ `str` or `None` ]
            Names of components to include in iteration.  `None` may be
            included (at most once) to include the parent dataset type.
        """
        return ParentDatasetQueryResults(
            self._db, self._query, records=self._records, components=components, datasetType=self._datasetType
        )

    def expanded(self) -> ParentDatasetQueryResults:
        # Docstring inherited from DatasetQueryResults.
        if self._records is None:
            records = self.dataIds.expanded()._records
            return ParentDatasetQueryResults(
                self._db,
                self._query,
                records=records,
                components=self._components,
                datasetType=self._datasetType,
            )
        else:
            return self

    def count(self, *, exact: bool = True) -> int:
        # Docstring inherited.
        return len(self._components) * self._query.count(self._db, exact=exact)

    def any(
        self,
        *,
        execute: bool = True,
        exact: bool = True,
    ) -> bool:
        # Docstring inherited.
        return self._query.any(self._db, execute=execute, exact=exact)

    def explain_no_results(self) -> Iterable[str]:
        # Docstring inherited.
        return self._query.explain_no_results(self._db)


class ChainedDatasetQueryResults(DatasetQueryResults):
    """A `DatasetQueryResults` implementation that simply chains together
    other results objects, each for a different parent dataset type.

    Parameters
    ----------
    chain : `Sequence` [ `ParentDatasetQueryResults` ]
        The underlying results objects this object will chain together.
    doomed_by : `Iterable` [ `str` ], optional
        A list of messages (appropriate for e.g. logging or exceptions) that
        explain why the query is known to return no results even before it is
        executed.  Queries with a non-empty list will never be executed.
        Child results objects may also have their own list.
    """

    def __init__(self, chain: Sequence[ParentDatasetQueryResults], doomed_by: Iterable[str] = ()):
        self._chain = chain
        self._doomed_by = tuple(doomed_by)

    __slots__ = ("_chain",)

    def __iter__(self) -> Iterator[DatasetRef]:
        return itertools.chain.from_iterable(self._chain)

    def __repr__(self) -> str:
        return "<DatasetRef iterator for multiple dataset types>"

    def byParentDatasetType(self) -> Iterator[ParentDatasetQueryResults]:
        # Docstring inherited from DatasetQueryResults.
        return iter(self._chain)

    @contextmanager
    def materialize(self) -> Iterator[ChainedDatasetQueryResults]:
        # Docstring inherited from DatasetQueryResults.
        with ExitStack() as stack:
            yield ChainedDatasetQueryResults([stack.enter_context(r.materialize()) for r in self._chain])

    def expanded(self) -> ChainedDatasetQueryResults:
        # Docstring inherited from DatasetQueryResults.
        return ChainedDatasetQueryResults([r.expanded() for r in self._chain])

    def count(self, *, exact: bool = True) -> int:
        # Docstring inherited.
        return sum(r.count(exact=exact) for r in self._chain)

    def any(
        self,
        *,
        execute: bool = True,
        exact: bool = True,
    ) -> bool:
        # Docstring inherited.
        return any(r.any(execute=execute, exact=exact) for r in self._chain)

    def explain_no_results(self) -> Iterable[str]:
        # Docstring inherited.
        for r in self._chain:
            yield from r.explain_no_results()
        yield from self._doomed_by


class DimensionRecordQueryResults(Iterable[DimensionRecord]):
    """An interface for objects that represent the results of queries for
    dimension records.
    """

    @abstractmethod
    def count(self, *, exact: bool = True) -> int:
        """Count the number of rows this query would return.

        Parameters
        ----------
        exact : `bool`, optional
            If `True`, run the full query and perform post-query filtering if
            needed to account for that filtering in the count.  If `False`, the
            result may be an upper bound.

        Returns
        -------
        count : `int`
            The number of rows the query would return, or an upper bound if
            ``exact=False``.

        Notes
        -----
        This counts the number of rows returned, not the number of unique rows
        returned, so even with ``exact=True`` it may provide only an upper
        bound on the number of *deduplicated* result rows.
        """
        raise NotImplementedError()

    @abstractmethod
    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        """Test whether this query returns any results.

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
    def order_by(self, *args: str) -> DimensionRecordQueryResults:
        """Make the iterator return ordered result.

        Parameters
        ----------
        *args : `str`
            Names of the columns/dimensions to use for ordering. Column name
            can be prefixed with minus (``-``) to use descending ordering.

        Returns
        -------
        result : `DimensionRecordQueryResults`
            Returns ``self`` instance which is updated to return ordered
            result.

        Notes
        -----
        This method can modify the iterator in place and return the same
        instance.
        """
        raise NotImplementedError()

    @abstractmethod
    def limit(self, limit: int, offset: int | None = None) -> DimensionRecordQueryResults:
        """Make the iterator return limited number of records.

        Parameters
        ----------
        limit : `int`
            Upper limit on the number of returned records.
        offset : `int` or `None`
            If not `None` then the number of records to skip before returning
            ``limit`` records.

        Returns
        -------
        result : `DimensionRecordQueryResults`
            Returns ``self`` instance which is updated to return limited set
            of records.

        Notes
        -----
        This method can modify the iterator in place and return the same
        instance. Normally this method is used together with `order_by`
        method.
        """
        raise NotImplementedError()

    @abstractmethod
    def explain_no_results(self) -> Iterable[str]:
        """Return human-readable messages that may help explain why the query
        yields no results.

        Returns
        -------
        messages : `Iterable` [ `str` ]
            String messages that describe reasons the query might not yield any
            results.

        Notes
        -----
        Messages related to post-query filtering are only available if the
        iterator has been exhausted, or if `any` or `count` was already called
        (with ``exact=True`` for the latter two).

        This method first yields messages that are generated while the query is
        being built or filtered, but may then proceed to diagnostics generated
        by performing what should be inexpensive follow-up queries.  Callers
        can short-circuit this at any time by simply not iterating further.
        """
        raise NotImplementedError()


class _DimensionRecordKey:
    """Class for objects used as keys in ordering `DimensionRecord` instances.

    Parameters
    ----------
    attributes : `Sequence` [ `str` ]
        Sequence of attribute names to use for comparison.
    ordering : `Sequence` [ `bool` ]
        Matching sequence of ordering flags, `False` for descending ordering,
        `True` for ascending ordering.
    record : `DimensionRecord`
        `DimensionRecord` to compare to other records.
    """

    def __init__(self, attributes: Sequence[str], ordering: Sequence[bool], record: DimensionRecord):
        self.attributes = attributes
        self.ordering = ordering
        self.rec = record

    def _cmp(self, other: _DimensionRecordKey) -> int:
        """Compare two records using provided comparison operator.

        Parameters
        ----------
        other : `_DimensionRecordKey`
            Key for other record.

        Returns
        -------
        result : `int`
            0 if keys are identical, negative if ``self`` is ordered before
            ``other``, positive otherwise.
        """
        for attribute, ordering in zip(self.attributes, self.ordering):
            # timespan.begin/end cannot use getattr
            attrgetter = operator.attrgetter(attribute)
            lhs = attrgetter(self.rec)
            rhs = attrgetter(other.rec)
            if not ordering:
                lhs, rhs = rhs, lhs
            if lhs != rhs:
                return 1 if lhs > rhs else -1
        return 0

    def __lt__(self, other: _DimensionRecordKey) -> bool:
        return self._cmp(other) < 0

    def __gt__(self, other: _DimensionRecordKey) -> bool:
        return self._cmp(other) > 0

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, _DimensionRecordKey):
            return NotImplemented
        return self._cmp(other) == 0

    def __le__(self, other: _DimensionRecordKey) -> bool:
        return self._cmp(other) <= 0

    def __ge__(self, other: _DimensionRecordKey) -> bool:
        return self._cmp(other) >= 0


class DatabaseDimensionRecordQueryResults(DimensionRecordQueryResults):
    """Implementation of DimensionRecordQueryResults using database query.

    Parameters
    ----------
    dataIds : `DataCoordinateQueryResults`
        Iterator for DataIds.
    recordStorage : `DimensionRecordStorage`
        Instance of storage class for dimension records.
    """

    def __init__(self, dataIds: DataCoordinateQueryResults, recordStorage: DimensionRecordStorage):
        self._dataIds = dataIds
        self._recordStorage = recordStorage
        self._order_by: Iterable[str] = ()

    def __iter__(self) -> Iterator[DimensionRecord]:
        # LIMIT is already applied at DataCoordinateQueryResults level
        # (assumption here is that if DataId exists then dimension record
        # exists too and their counts must be equal). fetch() does not
        # guarantee ordering, so we need to sort records in memory below.
        recordIter = self._recordStorage.fetch(self._dataIds)
        if not self._order_by:
            return iter(recordIter)

        # Parse list of column names and build a list of attribute name for
        # ordering. Note that here we only support ordering by direct
        # attributes of the element, and not other elements from the dimension
        # graph.
        orderBy = ElementOrderByClause(self._order_by, self._recordStorage.element)
        attributes: list[str] = []
        ordering: list[bool] = []
        for column in orderBy.order_by_columns:
            if column.column is None:
                assert isinstance(column.element, Dimension), "Element must be a Dimension"
                attributes.append(column.element.primaryKey.name)
            else:
                attributes.append(column.column)
            ordering.append(column.ordering)

        def _key(record: DimensionRecord) -> _DimensionRecordKey:
            return _DimensionRecordKey(attributes, ordering, record)

        records = sorted(recordIter, key=_key)
        return iter(records)

    def count(self, *, exact: bool = True) -> int:
        # Docstring inherited from base class.
        return self._dataIds.count(exact=exact)

    def any(self, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited from base class.
        return self._dataIds.any(execute=execute, exact=exact)

    def order_by(self, *args: str) -> DimensionRecordQueryResults:
        # Docstring inherited from base class.
        self._dataIds = self._dataIds.order_by(*args)
        self._order_by = args
        return self

    def limit(self, limit: int, offset: Optional[int] = None) -> DimensionRecordQueryResults:
        # Docstring inherited from base class.
        self._dataIds = self._dataIds.limit(limit, offset)
        return self

    def explain_no_results(self) -> Iterable[str]:
        # Docstring inherited.
        return self._dataIds.explain_no_results()
