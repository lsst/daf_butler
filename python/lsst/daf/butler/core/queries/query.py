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

__all__ = ("Query",)

import itertools
from typing import Optional, Dict, Any, Tuple, Callable

from sqlalchemy.sql import FromClause
from sqlalchemy.engine import RowProxy, ResultProxy, Connection

from lsst.sphgeom import Region

from ..datasets import DatasetType, DatasetRef
from ..dimensions import DimensionGraph, DataCoordinate, ExpandedDataCoordinate

from .structs import QuerySummary, QueryColumns, QueryParameters


class Query:
    """A wrapper for a SQLAlchemy query that knows how to re-bind parameters
    and transform result rows into data IDs and dataset references.

    A `Query` should almost always be constructed directly by a call to
    `QueryBuilder` finish; direct construction will make it difficult to be
    able to maintain invariants between arguments (see the documentation for
    `QueryColumns` and `QueryParameters` for more information).

    Parameters
    ----------
    connection: `sqlalchemy.engine.Connection`
        Connection used to execute the query.
    sql : `sqlalchemy.sql.FromClause`
        A complete SELECT query, including at least SELECT, FROM, and WHERE
        clauses.
    summary : `QuerySummary`
        Struct that organizes the dimensions involved in the query.
    columns : `QueryColumns`
        Columns that are referenced in the query in any clause.
    parameters : `QueryParameters`
        Bind parameters for the query.

    Notes
    -----
    SQLAlchemy is used in the public interface of `Query` rather than just its
    implementation simply because avoiding this would entail writing wrappers
    for the `sqlalchemy.engine.RowProxy` and `sqlalchemy.engine.ResultProxy`
    classes that are themselves generic wrappers for lower-level Python DBAPI
    classes.  Another layer would entail another set of computational
    overheads, but the only reason we would seriously consider not using
    SQLAlchemy here in the future would be to reduce computational overheads.
    """

    def __init__(self, *, connection: Connection, sql: FromClause,
                 summary: QuerySummary, columns: QueryColumns, parameters: QueryParameters):
        self.summary = summary
        self.sql = sql
        self._columns = columns
        self._parameters = parameters
        self._connection = connection

    def predicate(self, region: Optional[Region] = None) -> Callable[[RowProxy], bool]:
        """Return a callable that can perform extra Python-side filtering of
        query results.

        To get the expected results from a query, the returned predicate *must*
        be used to ignore rows for which it returns `False`; this permits the
        `QueryBuilder` implementation to move logic from the database to Python
        without changing the public interface.

        Parameters
        ----------
        region : `sphgeom.Region`, optional
            A region that any result-row regions must overlap in order for the
            predicate to return `True`.  If not provided, this will be the
            region in `QuerySummary.dataId`, if there is one.

        Returns
        -------
        func : `Callable`
            A callable that takes a single `sqlalchemy.engine.RowProxy`
            argmument and returns `bool`.
        """
        whereRegion = region if region is not None else self.summary.dataId.region

        def closure(row: RowProxy) -> bool:
            rowRegions = [row[column] for column in self._columns.regions.values()]
            if whereRegion and any(r.isdisjoint(whereRegion) for r in rowRegions):
                return False
            return not any(a.isdisjoint(b) for a, b in itertools.combinations(rowRegions, 2))

        return closure

    def bind(self, dataId: ExpandedDataCoordinate) -> Dict[str, Any]:
        """Return a dictionary that can be passed to a SQLAlchemy execute
        method to provide WHERE clause information at execution time rather
        than construction time.

        Most callers should call `Query.execute` directly instead; when called
        with a data ID, that calls `bind` internally.

        Parameters
        ----------
        dataId : `ExpandedDataCoordinate`
            Data ID to transform into bind parameters.  This must identify
            all dimensions in `QuerySummary.given`, and must have the same
            primary key values for all dimensions also identified by
            `QuerySummary.dataId`.

        Returns
        -------
        parameters : `dict`
            Dictionary that can be passed as the second argument (with
            ``self.sql`` this first argument) to SQLAlchemy execute methods.

        Notes
        -----
        Calling `bind` does not automatically update the callable returned by
        `predicate` with the given data ID's region (if it has one).  That
        must be done manually by passing the region when calling `predicate`.
        """
        assert dataId.graph == self.summary.given
        result = {}
        for dimension, parameter in self._parameters.keys.items():
            result[parameter] = dataId.full[dimension]
        if self._parameters.timespan:
            result[self._parameters.timespan.begin] = dataId.timespan.begin
            result[self._parameters.timespan.end] = dataId.timespan.end
        for dimension, parameter in self._parameters.skypix.items():
            result[parameter] = dimension.pixelization.envelope(dataId.region)
        return result

    def extractDataId(self, row: RowProxy, *, graph: Optional[DimensionGraph] = None) -> DataCoordinate:
        """Extract a data ID from a result row.

        Parameters
        ----------
        row : `sqlalchemy.engine.RowProxy`
            A result row from a SQLAlchemy SELECT query.
        graph : `DimensionGraph`, optional
            The dimensions the returned data ID should identify.  If not
            provided, this will be all dimensions in `QuerySummary.requested`.

        Returns
        -------
        dataId : `DataCoordinate`
            A minimal data ID that identifies the requested dimensions but
            includes no metadata or implied dimensions.
        """
        if graph is None:
            graph = self.summary.requested
        values = tuple(row[self._columns.getKeyColumn(dimension)] for dimension in graph.required)
        return DataCoordinate(graph, values)

    def extractDatasetRef(self, row: RowProxy, datasetType: DatasetType,
                          dataId: Optional[DataCoordinate] = None) -> Tuple[DatasetRef, Optional[int]]:
        """Extract a `DatasetRef` from a result row.

        Parameters
        ----------
        row : `sqlalchemy.engine.RowProxy`
            A result row from a SQLAlchemy SELECT query.
        datasetType : `DatasetType`
            Type of the dataset to extract.  Must have been included in the
            `Query` via a call to `QueryBuilder.joinDataset` with
            ``isResult=True``, or otherwise included in
            `QueryColumns.datasets`.
        dataId : `DataCoordinate`
            Data ID to attach to the `DatasetRef`.  A minimal (i.e. base class)
            `DataCoordinate` is constructed from ``row`` if `None`.

        Returns
        -------
        ref : `DatasetRef`
            Reference to the dataset; guaranteed to have `DatasetRef.id` not
            `None`.
        rank : `int` or `None`
            Integer index of the collection in which this dataset was found,
            within the sequence of collections passed when constructing the
            query.  `None` if `QueryBuilder.joinDataset` was called with
            ``addRank=False``.
        """
        if dataId is None:
            dataId = self.extractDataId(row, graph=datasetType.dimensions)
        datasetIdColumn, datasetRankColumn = self._columns.datasets[datasetType]
        return (DatasetRef(datasetType, dataId, id=row[datasetIdColumn]),
                row[datasetRankColumn] if datasetRankColumn is not None else None)

    def execute(self, dataId: Optional[ExpandedDataCoordinate] = None) -> ResultProxy:
        """Execute the query.

        This may be called multiple times with different arguments to apply
        different bind parameter values without repeating the work of
        constructing the query.

        Parameters
        ----------
        dataId : `ExpandedDataCoordinate`, optional
            Data ID to transform into bind parameters.  This must identify
            all dimensions in `QuerySummary.given`, and must have the same
            primary key values for all dimensions also identified by
            `QuerySummary.dataId`.  If not provided, `QuerySummary.dataId`
            must identify all dimensions in `QuerySummary.given`.

        Returns
        -------
        results : `sqlalchemy.engine.ResultProxy`
            Object representing the query results; see SQLAlchemy documentation
            for more information.
        """
        if dataId is not None:
            params = self.bind(dataId)
            return self._connection.execute(self.sql, params)
        else:
            return self._connection.execute(self.sql)
