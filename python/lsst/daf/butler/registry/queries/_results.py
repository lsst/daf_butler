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
    "DataCoordinateQueryResults",
)

from contextlib import contextmanager
from typing import (
    Callable,
    Iterator,
    Mapping,
    Optional,
)

import sqlalchemy

from ...core import (
    DataCoordinate,
    DataCoordinateIterable,
    DimensionGraph,
    DimensionRecord,
    SimpleQuery,
)
from ..interfaces import Database
from ._query import Query


class DataCoordinateQueryResults(DataCoordinateIterable):
    """An enhanced implementation of `DataCoordinateIterable` that represents
    data IDs retrieved from a database query.

    Parameters
    ----------
    db : `Database`
        Database engine used to execute queries.
    query : `Query`
        Low-level representation of the query that backs this result object.
    records : `Mapping`, optional
        Mapping containing `DimensionRecord` objects for all dimensions and
        all data IDs this query will yield.  If `None` (default),
        `DataCoordinateIterable.hasRecords` will return `False`.

    Notes
    -----
    Constructing an instance of this does nothing; the query is not executed
    until it is iterated over (or some other operation is performed that
    involves iteration).

    Instances should generally only be constructed by `Registry` methods or the
    methods of other query result objects.
    """
    def __init__(self, db: Database, query: Query, *,
                 records: Optional[Mapping[str, Mapping[tuple, DimensionRecord]]] = None):
        self._db = db
        self._query = query
        self._records = records
        assert query.datasetType is None, \
            "Query used to initialize data coordinate results should not have any datasets."

    __slots__ = ("_db", "_query", "_records")

    def __iter__(self) -> Iterator[DataCoordinate]:
        return (self._query.extractDataId(row, records=self._records) for row in self._query.rows(self._db))

    @property
    def graph(self) -> DimensionGraph:
        # Docstring inherited from DataCoordinateIterable.
        return self._query.graph

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return True

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._records is not None

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
            yield DataCoordinateQueryResults(self._db, materialized, records=self._records)

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
                    for record in self._query.managers.dimensions[element].fetch(subset)
                }
            return DataCoordinateQueryResults(self._db, self._query, records=records)
        else:
            return self

    def subset(self, graph: Optional[DimensionGraph] = None, *,
               unique: bool = False) -> DataCoordinateQueryResults:
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
        records: Optional[Mapping[str, Mapping[tuple, DimensionRecord]]]
        if self._records is not None:
            records = {element.name: self._records[element.name] for element in graph.elements}
        else:
            records = None
        return DataCoordinateQueryResults(
            self._db,
            self._query.subset(graph=graph, datasets=False, unique=unique),
            records=records,
        )

    def constrain(self, query: SimpleQuery, columns: Callable[[str], sqlalchemy.sql.ColumnElement]) -> None:
        # Docstring inherited from DataCoordinateIterable.
        fromClause = self._query.sql.alias("c")
        query.join(
            fromClause,
            onclause=sqlalchemy.sql.and_(*[
                columns(dimension.name) == fromClause.columns[dimension.name]
                for dimension in self.graph.required
            ])
        )
