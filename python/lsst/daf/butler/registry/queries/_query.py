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
from typing import Iterable, Optional, Callable

from sqlalchemy.sql import FromClause
from sqlalchemy.engine import RowProxy

from lsst.sphgeom import Region

from ...core import (
    DataCoordinate,
    DatasetRef,
    DatasetType,
    Dimension,
    DimensionGraph,
)
from ..interfaces import CollectionManager
from ._structs import QuerySummary, QueryColumns


class Query:
    """A wrapper for a SQLAlchemy query that knows how to transform result rows
    into data IDs and dataset references.

    A `Query` should almost always be constructed directly by a call to
    `QueryBuilder.finish`; direct construction will make it difficult to be
    able to maintain invariants between arguments (see the documentation for
    `QueryColumns` for more information).

    Parameters
    ----------
    sql : `sqlalchemy.sql.FromClause`
        A complete SELECT query, including at least SELECT, FROM, and WHERE
        clauses.
    summary : `QuerySummary`
        Struct that organizes the dimensions involved in the query.
    columns : `QueryColumns`
        Columns that are referenced in the query in any clause.
    collections : `CollectionsManager`,
        Manager object for collection tables.

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

    def __init__(self, *, sql: FromClause,
                 summary: QuerySummary,
                 columns: QueryColumns,
                 collections: CollectionManager):
        self.summary = summary
        self.sql = sql
        self._columns = columns
        self._collections = collections

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
        whereRegion = region if region is not None else self.summary.whereRegion

        def closure(row: RowProxy) -> bool:
            rowRegions = [row[column] for column in self._columns.regions.values()]
            if whereRegion and any(r.isDisjointFrom(whereRegion) for r in rowRegions):
                return False
            return not any(a.isDisjointFrom(b) for a, b in itertools.combinations(rowRegions, 2))

        return closure

    def extractDimensionsTuple(self, row: RowProxy, dimensions: Iterable[Dimension]) -> tuple:
        """Extract a tuple of data ID values from a result row.

        Parameters
        ----------
        row : `sqlalchemy.engine.RowProxy`
            A result row from a SQLAlchemy SELECT query.
        dimensions : `Iterable` [ `Dimension` ]
            The dimensions to include in the returned tuple, in order.

        Returns
        -------
        values : `tuple`
            A tuple of dimension primary key values.
        """
        return tuple(row[self._columns.getKeyColumn(dimension)] for dimension in dimensions)

    def extractDataId(self, row: RowProxy, *, graph: Optional[DimensionGraph] = None
                      ) -> DataCoordinate:
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
            A data ID that identifies all required and implied dimensions.
        """
        if graph is None:
            graph = self.summary.requested
        return DataCoordinate.fromFullValues(
            graph,
            self.extractDimensionsTuple(row, itertools.chain(graph.required, graph.implied))
        )

    def extractDatasetRef(self, row: RowProxy, datasetType: DatasetType,
                          dataId: Optional[DataCoordinate] = None) -> DatasetRef:
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
        """
        if dataId is None:
            dataId = self.extractDataId(row, graph=datasetType.dimensions)
        datasetColumns = self._columns.datasets[datasetType]
        runRecord = self._collections[row[datasetColumns.runKey]]
        return DatasetRef(datasetType, dataId, id=row[datasetColumns.id], run=runRecord.name)
