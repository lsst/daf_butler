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

__all__ = ("QueryBuilder",)

from typing import List, Iterable

from sqlalchemy.sql import ColumnElement, and_, literal, bindparam, select, FromClause
import sqlalchemy.sql
from sqlalchemy.engine import Connection

from ..datasets import DatasetType
from ..timespan import Timespan, TIMESPAN_FIELD_SPECS
from ..utils import NamedValueSet, NamedKeyDict
from ..dimensions import DimensionElement, SkyPixDimension, Dimension
from ..dimensions.schema import REGION_FIELD_SPEC
from ..dimensions.storage import DimensionRecordStorage

from .structs import QuerySummary, QueryColumns, QueryParameters, GivenTime
from .datasets import DatasetRegistryStorage, CollectionsExpression
from .expressions import ClauseVisitor
from .query import Query


class QueryBuilder:
    """A builder for potentially complex queries that join tables based
    on dimension relationships.

    Parameters
    ----------
    connection : `sqlalchemy.engine.Connection`
        SQLAlchemy connection object.  This is only used to pass through
        to the `Query` object returned by `finish`.
    summary : `QuerySummary`
        Struct organizing the dimensions involved in the query.
    dimensionStorage : `NamedKeyDict`
        Storage backend objects that abstract access to dimension tables,
        organized as a `NamedKeyDict` mapping `DimensionElement` to
        `DimensionRecordStorage`.
    datasetStorage : `DatasetRegistryStorage`
        Storage backend object that abstracts access to dataset tables.
    """

    def __init__(self, connection: Connection, summary: QuerySummary,
                 dimensionStorage: NamedKeyDict[DimensionElement, DimensionRecordStorage],
                 datasetStorage: DatasetRegistryStorage):
        self.summary = summary
        self._connection = connection
        self._dimensionStorage = dimensionStorage
        self._datasetStorage = datasetStorage
        self._sql = None
        self._elements: NamedKeyDict[DimensionElement, FromClause] = NamedKeyDict()
        self._columns = QueryColumns()

    def joinDimensionElement(self, element: DimensionElement):
        """Add the table for a `DimensionElement` to the query.

        This automatically joins the element table to all other tables in the
        query with which it is related, via both dimension keys and spatial
        and temporal relationships.

        External calls to this method should rarely be necessary; `finish` will
        automatically call it if the `DimensionElement` has been identified as
        one that must be included.

        Parameters
        ----------
        element : `DimensionElement`
            Element for which a table should be added.  The element must be
            associated with a database table (see `DimensionElement.hasTable`).
        """
        assert element not in self._elements, "Element already included in query."
        assert element.hasTable(), "Cannot join element with no table."
        table = self._dimensionStorage[element].getElementTable(self.summary.dataId)
        if element in self.summary.spatial:
            self.joinToCommonSkyPix(element)
            self._columns.regions[element] = table.columns[REGION_FIELD_SPEC.name]
        joinDimensions = list(element.graph.required)
        joinDimensions.extend(element.implied)
        joinOn = self._startJoin(table, joinDimensions, element.RecordClass.__slots__)
        if element in self.summary.temporal:
            intervalInTable = Timespan(
                begin=table.columns[TIMESPAN_FIELD_SPECS.begin.name],
                end=table.columns[TIMESPAN_FIELD_SPECS.end.name],
            )
            for intervalInQuery in self._columns.timespans.values():
                joinOn.append(intervalInQuery.overlaps(intervalInTable, ops=sqlalchemy.sql))
            self._columns.timespans[element] = intervalInTable
        self._finishJoin(table, joinOn)
        self._elements[element] = table

    def joinToCommonSkyPix(self, element: DimensionElement):
        """Add the table relating a spatial `DimensionElement` to the
        universe's `~DimensionUniverse.commonSkyPix` dimension to the query.

        External calls to this method should rarely be necessary; `finish` will
        automatically call it if the `DimensionElement` has been identified as
        one that must be included.

        Parameters
        ----------
        element : `DimensionElement`
            Element for which the relationship should be added.  The element
            must be associated with a database table (see
            `DimensionElement.hasTable`) and must have
            `Dimensionelement.spatial` `True`.
        """
        if element is self.summary.universe.commonSkyPix:
            return
        assert element in self.summary.spatial,\
            f"{element}'s spatial information is not relevant for this query."
        table = self._dimensionStorage[element].getCommonSkyPixOverlapTable(self.summary.dataId)
        assert table is not None, f"No relationship found from the common skypix to {element}."
        dimensions = NamedValueSet(element.graph.required)
        dimensions.add(self.summary.universe.commonSkyPix)
        self.joinTable(table, dimensions)

    def joinDataset(self, datasetType: DatasetType, collections: CollectionsExpression, *,
                    isResult: bool = True, addRank: bool = False):
        """Add a dataset search or constraint to the query.

        Unlike other `QueryBuilder` join methods, this *must* be called
        directly to search for datasets of a particular type or constrain the
        query results based on the exists of datasets.  However, all dimensions
        used to identify the dataset type must have already been included in
        `QuerySummary.requested` when initializing the `QueryBuilder`.

        Parameters
        ----------
        datasetType : `DatasetType`
            The type of datasets to search for.
        collections : sequence of `str` or `Like`, or ``...``
            An expression describing the collections in which to search for
            the datasets.  ``...`` indicates that all collections should be
            searched.
        isResult : `bool`, optional
            If `True` (default), include the ``dataset_id`` column in the
            result columns of the query, allowing complete `DatasetRef`
            instances to be produced from the query results for this dataset
            type.  If `False`, the existence of datasets of this type is used
            only to constrain the data IDs returned by the query.
        addRank : `bool`, optional
            If `True` (`False` is default), also include a calculated column
            that ranks the collection in which the dataset was found (lower
            is better).  Requires that all entries in ``collections`` be
            regular strings, so there is a clear search order.  Ignored if
            ``isResult`` is `False`.
        """
        assert datasetType.dimensions.issubset(self.summary.requested)
        table = self._datasetStorage.getDatasetSubquery(datasetType, collections=collections,
                                                        dataId=self.summary.dataId,
                                                        isResult=isResult, addRank=addRank)
        self.joinTable(table, datasetType.dimensions)
        if isResult:
            self._columns.datasets[datasetType] = (table.columns["dataset_id"],
                                                   table.columns["rank"] if addRank else None)

    def joinTable(self, table: FromClause, dimensions: Iterable[Dimension]):
        """Join an arbitrary table to the query via dimension relationships.

        External calls to this method should only be necessary for tables whose
        records represent neither dataset nor dimension elements (i.e.
        extensions to the standard `Registry` schema).

        Parameters
        ----------
        table : `sqlalchemy.sql.FromClause`
            SQLAlchemy object representing the logical table (which may be a
            join or subquery expression) to be joined.
        dimensions : iterable of `Dimension`
            The dimensions that relate this table to others that may be in the
            query.  The table must have columns with the names of the
            dimensions.
        """
        joinOn = self._startJoin(table, dimensions, dimensions.names)
        self._finishJoin(table, joinOn)

    def _startJoin(self, table: FromClause, dimensions: Iterable[Dimension], columnNames: Iterable[str]
                   ) -> List[ColumnElement]:
        """Begin a join on dimensions.

        This is intended for internal use by `QueryBuilder` only.  Must be
        followed by call to `_finishJoin`.

        Parameters
        ----------
        table : `sqlalchemy.sql.FromClause`
            SQLAlchemy object representing the logical table (which may be a
            join or subquery expression) to be joined.
        dimensions : iterable of `Dimension`
            The dimensions that relate this table to others that may be in the
            query.  The table must have columns with the names of the
            dimensions.
        columnNames : iterable of `str`
            Names of the columns that correspond to dimension key values; must
            be `zip` iterable with ``dimensions``.

        Returns
        -------
        joinOn : `list` of `sqlalchemy.sql.ColumnElement`
            Sequence of boolean expressions that should be combined with AND
            to form (part of) the ON expression for this JOIN.
        """
        joinOn = []
        for dimension, columnName in zip(dimensions, columnNames):
            columnInTable = table.columns[columnName]
            columnsInQuery = self._columns.keys.setdefault(dimension, [])
            for columnInQuery in columnsInQuery:
                joinOn.append(columnInQuery == columnInTable)
            columnsInQuery.append(columnInTable)
        return joinOn

    def _finishJoin(self, table, joinOn):
        """Complete a join on dimensions.

        This is intended for internal use by `QueryBuilder` only.  Must be
        preceded by call to `_startJoin`.

        Parameters
        ----------
        table : `sqlalchemy.sql.FromClause`
            SQLAlchemy object representing the logical table (which may be a
            join or subquery expression) to be joined.  Must be the same object
            passed to `_startJoin`.
        joinOn : `list` of `sqlalchemy.sql.ColumnElement`
            Sequence of boolean expressions that should be combined with AND
            to form (part of) the ON expression for this JOIN.  Should include
            at least the elements of the list returned by `_startJoin`.
        """
        if joinOn:
            self._sql = self._sql.join(table, and_(*joinOn))
        elif self._sql is None:
            self._sql = table
        else:
            # New table is completely unrelated to all already-included
            # tables.  We need a cross join here but SQLAlchemy does not
            # have a specific method for that. Using join() without
            # `onclause` will try to join on FK and will raise an exception
            # for unrelated tables, so we have to use `onclause` which is
            # always true.
            self._sql = self._sql.join(table, literal(True) == literal(True))

    def _joinMissingDimensionElements(self):
        """Join all dimension element tables that were identified as necessary
        by `QuerySummary` and have not yet been joined.

        For internal use by `QueryBuilder` only; will be called (and should
        only by called) by `finish`.
        """
        # Join all DimensionElement tables that we need for spatial/temporal
        # joins/filters or a nontrivial WHERE expression, skipping but
        # remembering any SkyPixDimensions for now.
        # We iterate over these in *reverse* topological order to minimize the
        # number of tables joined.  For example, the "visit" table provides
        # the primary key value for the "instrument" table it depends on, so we
        # don't need to join "instrument" as well unless we had a nontrivial
        # expression on it (and hence included it already above).
        skyPixDimensions = NamedValueSet()
        for element in self.summary.universe.sorted(self.summary.mustHaveTableJoined, reverse=True):
            if isinstance(element, SkyPixDimension):
                skyPixDimensions.add(element)
            else:
                self.joinDimensionElement(element)
        # Join in any requested Dimension tables that don't already have their
        # primary keys identified by the query.
        for dimension in self.summary.universe.sorted(self.summary.mustHaveKeysJoined, reverse=True):
            if dimension not in self._columns.keys:
                self.joinDimensionElement(dimension)
        # If the query involves the commonSkyPix dimension (usually an
        # indicator of a spatial join), we need to make sure any *other* SkyPix
        # dimensions are related to it.  We've already made sure above that any
        # other spatial dimension is related to it.
        if self.summary.universe.commonSkyPix in self.summary.spatial:
            for dimension in skyPixDimensions:
                self.joinToCommonSkyPix(dimension)

    def _addWhereClause(self):
        """Add a WHERE clause to the query under construction, connecting all
        joined dimensions to the expression and given dimensions from
        `QuerySummary`.

        For internal use by `QueryBuilder` only; will be called (and should
        only by called) by `finish`.
        """
        parameters = QueryParameters()
        whereTerms = []
        if self.summary.expression.tree is not None:
            visitor = ClauseVisitor(self.summary.universe, self._columns, self._elements)
            whereTerms.append(self.summary.expression.tree.visit(visitor))
        for dimension, columnsInQuery in self._columns.keys.items():
            if dimension in self.summary.given:
                if self.summary.whenIsDimensionGiven(dimension) == GivenTime.AT_EXECUTION:
                    givenKey = bindparam(f"_given_later_{dimension.name}")
                    parameters.keys[dimension] = givenKey
                else:
                    givenKey = self.summary.dataId[dimension]
                # Add a WHERE term for each column that corresponds to each
                # key.  This is redundant with the JOIN ON clauses that make
                # them equal to each other, but more constraints have a chance
                # of making things easier on the DB's query optimizer.
                for columnInQuery in columnsInQuery:
                    whereTerms.append(columnInQuery == givenKey)
            else:
                # Dimension is not fully identified, but it might be a skypix
                # dimension that's constrained by a given region.
                if self.summary.given.spatial and isinstance(dimension, SkyPixDimension):
                    if self.summary.whenIsRegionGiven() == GivenTime.AT_CONSTRUCTION:
                        # We know the region now.
                        givenSkyPixIds = []
                        for begin, end in dimension.pixelization.envelope(self.summary.dataId.region):
                            givenSkyPixIds.extend(range(begin, end))
                    else:
                        # We'll know the region later (there might be a region
                        # now, too, but we'll know a more precise one later,
                        # and hence we'll ignore the one we know now).
                        givenSkyPixIds = bindparam(f"_given_later_{dimension.name}")
                        parameters.skypix[dimension] = givenSkyPixIds
                    for columnInQuery in columnsInQuery:
                        whereTerms.append(columnInQuery.in_(givenSkyPixIds))
        # If we are [to be] given an dataId with a timespan, and there are
        # one or more timespans in the query that aren't given, add a WHERE
        # expression for each of them.
        if self.summary.given.temporal and self.summary.temporal:
            if self.summary.whenIsTimespanGiven() == GivenTime.AT_CONSTRUCTION:
                # Timespan is known now.
                givenInterval = self.summary.dataId.timespan
            else:
                # We'll know the timespan later (there might be a timespan now,
                # too, but we'll know a more precise one later, and hence we'll
                # ignore the one we know now).
                givenInterval = Timespan(
                    begin=bindparam(f"_given_later_timespan_begin"),
                    end=bindparam(f"_given_later_timespan_end"),
                )
            for element, intervalInQuery in self._columns.timespans.items():
                assert element not in self.summary.given.elements
                whereTerms.append(intervalInQuery.overlaps(givenInterval, ops=sqlalchemy.sql))
        # AND-together the full WHERE clause, and combine it with the FROM
        # clause.
        self._sql = self._sql.where(and_(*whereTerms))
        return parameters

    def _addSelectClause(self):
        """Add a SELECT clause to the query under construction containing all
        output columns identified by the `QuerySummary` and requested in calls
        to `joinDataset` with ``isResult=True``.

        For internal use by `QueryBuilder` only; will be called (and should
        only by called) by `finish`.
        """
        columns = []
        for dimension in self.summary.requested:
            columns.append(self._columns.getKeyColumn(dimension))
        for datasetType, columnPair in self._columns.datasets.items():
            columns.extend(columnPair)
        for element, column in self._columns.regions.items():
            columns.append(column)
        self._sql = select(columns).select_from(self._sql)

    def finish(self) -> Query:
        """Finish query constructing, returning a new `Query` instance.

        This automatically joins any missing dimension element tables
        (according to the categorization of the `QuerySummary` the builder was
        constructed with).

        This consumes the `QueryBuilder`; no other methods should be called
        after this one.

        Returns
        -------
        query : `Query`
            A `Query` object that can be executed (possibly multiple times
            with different bind parameter values) and used to interpret result
            rows.
        """
        self._joinMissingDimensionElements()
        self._addSelectClause()
        parameters = self._addWhereClause()
        return Query(summary=self.summary, connection=self._connection,
                     sql=self._sql, columns=self._columns, parameters=parameters)
