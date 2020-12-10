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

from typing import AbstractSet, Any, Iterable, List, Optional

import sqlalchemy.sql

from ...core import (
    DimensionElement,
    SkyPixDimension,
    Dimension,
    DatasetType,
    SimpleQuery,
)

from ...core.named import NamedKeyDict, NamedValueAbstractSet, NamedValueSet

from .._collectionType import CollectionType
from ._structs import QuerySummary, QueryColumns, DatasetQueryColumns, RegistryManagers
from .expressions import convertExpressionToSql
from ._query import DirectQuery, DirectQueryUniqueness, EmptyQuery, Query
from ..wildcards import CollectionSearch, CollectionQuery


class QueryBuilder:
    """A builder for potentially complex queries that join tables based
    on dimension relationships.

    Parameters
    ----------
    summary : `QuerySummary`
        Struct organizing the dimensions involved in the query.
    managers : `RegistryManagers`
        A struct containing the registry manager instances used by the query
        system.
    """
    def __init__(self, summary: QuerySummary, managers: RegistryManagers):
        self.summary = summary
        self._simpleQuery = SimpleQuery()
        self._elements: NamedKeyDict[DimensionElement, sqlalchemy.sql.FromClause] = NamedKeyDict()
        self._columns = QueryColumns()
        self._managers = managers

    def hasDimensionKey(self, dimension: Dimension) -> bool:
        """Return `True` if the given dimension's primary key column has
        been included in the query (possibly via a foreign key column on some
        other table).
        """
        return dimension in self._columns.keys

    def joinDimensionElement(self, element: DimensionElement) -> None:
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
        storage = self._managers.dimensions[element]
        fromClause = storage.join(
            self,
            regions=self._columns.regions if element in self.summary.spatial else None,
            timespans=self._columns.timespans if element in self.summary.temporal else None,
        )
        self._elements[element] = fromClause

    def joinDataset(self, datasetType: DatasetType, collections: Any, *,
                    isResult: bool = True, findFirst: bool = False) -> bool:
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
        collections : `Any`
            An expression that fully or partially identifies the collections
            to search for datasets, such as a `str`, `re.Pattern`, or iterable
            thereof.  `...` can be used to return all collections. See
            :ref:`daf_butler_collection_expressions` for more information.
        isResult : `bool`, optional
            If `True` (default), include the dataset ID column in the
            result columns of the query, allowing complete `DatasetRef`
            instances to be produced from the query results for this dataset
            type.  If `False`, the existence of datasets of this type is used
            only to constrain the data IDs returned by the query.
            `joinDataset` may be called with ``isResult=True`` at most one time
            on a particular `QueryBuilder` instance.
        findFirst : `bool`, optional
            If `True` (`False` is default), only include the first match for
            each data ID, searching the given collections in order.  Requires
            that all entries in ``collections`` be regular strings, so there is
            a clear search order.  Ignored if ``isResult`` is `False`.

        Returns
        -------
        anyRecords : `bool`
            If `True`, joining the dataset table was successful and the query
            should proceed.  If `False`, we were able to determine (from the
            combination of ``datasetType`` and ``collections``) that there
            would be no results joined in from this dataset, and hence (due to
            the inner join that would normally be present), the full query will
            return no results.
        """
        assert datasetType.dimensions.issubset(self.summary.requested)
        if isResult and findFirst:
            collections = CollectionSearch.fromExpression(collections)
        else:
            collections = CollectionQuery.fromExpression(collections)
        # If we are searching all collections with no constraints, loop over
        # RUN collections only, because that will include all datasets.
        collectionTypes: AbstractSet[CollectionType]
        if collections == CollectionQuery():
            collectionTypes = {CollectionType.RUN}
        else:
            collectionTypes = CollectionType.all()
        datasetRecordStorage = self._managers.datasets.find(datasetType.name)
        if datasetRecordStorage is None:
            # Unrecognized dataset type means no results.  It might be better
            # to raise here, but this is consistent with previous behavior,
            # which is expected by QuantumGraph generation code in pipe_base.
            return False
        subsubqueries = []
        runKeyName = self._managers.collections.getRunForeignKeyName()
        baseColumnNames = {"id", runKeyName, "ingest_date"} if isResult else set()
        baseColumnNames.update(datasetType.dimensions.required.names)
        for rank, collectionRecord in enumerate(collections.iter(self._managers.collections,
                                                                 collectionTypes=collectionTypes)):
            if collectionRecord.type is CollectionType.CALIBRATION:
                if datasetType.isCalibration():
                    raise NotImplementedError(
                        f"Query for dataset type '{datasetType.name}' in CALIBRATION-type collection "
                        f"'{collectionRecord.name}' is not yet supported."
                    )
                else:
                    # We can never find a non-calibration dataset in a
                    # CALIBRATION collection.
                    continue
            ssq = datasetRecordStorage.select(collection=collectionRecord,
                                              dataId=SimpleQuery.Select,
                                              id=SimpleQuery.Select if isResult else None,
                                              run=SimpleQuery.Select if isResult else None,
                                              ingestDate=SimpleQuery.Select if isResult else None)
            if ssq is None:
                continue
            assert {c.name for c in ssq.columns} == baseColumnNames
            if findFirst:
                ssq.columns.append(sqlalchemy.sql.literal(rank).label("rank"))
            subsubqueries.append(ssq.combine())
        if not subsubqueries:
            return False
        subquery = sqlalchemy.sql.union_all(*subsubqueries)
        columns: Optional[DatasetQueryColumns] = None
        if isResult:
            if findFirst:
                # Rewrite the subquery (currently a UNION ALL over
                # per-collection subsubqueries) to select the rows with the
                # lowest rank per data ID.  The block below will set subquery
                # to something like this:
                #
                # WITH {dst}_search AS (
                #     SELECT {data-id-cols}, id, run_id, 1 AS rank
                #         FROM <collection1>
                #     UNION ALL
                #     SELECT {data-id-cols}, id, run_id, 2 AS rank
                #         FROM <collection2>
                #     UNION ALL
                #     ...
                # )
                # SELECT
                #     {dst}_window.{data-id-cols},
                #     {dst}_window.id,
                #     {dst}_window.run_id
                # FROM (
                #     SELECT
                #         {dst}_search.{data-id-cols},
                #         {dst}_search.id,
                #         {dst}_search.run_id,
                #         ROW_NUMBER() OVER (
                #             PARTITION BY {dst_search}.{data-id-cols}
                #             ORDER BY rank
                #         ) AS rownum
                #     ) {dst}_window
                # WHERE
                #     {dst}_window.rownum = 1;
                #
                search = subquery.cte(f"{datasetType.name}_search")
                windowDataIdCols = [
                    search.columns[name].label(name) for name in datasetType.dimensions.required.names
                ]
                windowSelectCols = [
                    search.columns["id"].label("id"),
                    search.columns[runKeyName].label(runKeyName),
                    search.columns["ingest_date"].label("ingest_date"),
                ]
                windowSelectCols += windowDataIdCols
                assert {c.name for c in windowSelectCols} == baseColumnNames
                windowSelectCols.append(
                    sqlalchemy.sql.func.row_number().over(
                        partition_by=windowDataIdCols,
                        order_by=search.columns["rank"]
                    ).label("rownum")
                )
                window = sqlalchemy.sql.select(
                    windowSelectCols
                ).select_from(search).alias(
                    f"{datasetType.name}_window"
                )
                subquery = sqlalchemy.sql.select(
                    [window.columns[name].label(name) for name in baseColumnNames]
                ).select_from(
                    window
                ).where(
                    window.columns["rownum"] == 1
                ).alias(datasetType.name)
            else:
                subquery = subquery.alias(datasetType.name)
            columns = DatasetQueryColumns(
                datasetType=datasetType,
                id=subquery.columns["id"],
                runKey=subquery.columns[runKeyName],
                ingestDate=subquery.columns["ingest_date"],
            )
        else:
            subquery = subquery.alias(datasetType.name)
        self.joinTable(subquery, datasetType.dimensions.required, datasets=columns)
        return True

    def joinTable(self, table: sqlalchemy.sql.FromClause, dimensions: NamedValueAbstractSet[Dimension], *,
                  datasets: Optional[DatasetQueryColumns] = None) -> None:
        """Join an arbitrary table to the query via dimension relationships.

        External calls to this method should only be necessary for tables whose
        records represent neither datasets nor dimension elements.

        Parameters
        ----------
        table : `sqlalchemy.sql.FromClause`
            SQLAlchemy object representing the logical table (which may be a
            join or subquery expression) to be joined.
        dimensions : iterable of `Dimension`
            The dimensions that relate this table to others that may be in the
            query.  The table must have columns with the names of the
            dimensions.
        datasets : `DatasetQueryColumns`, optional
            Columns that identify a dataset that is part of the query results.
        """
        unexpectedDimensions = NamedValueSet(dimensions - self.summary.requested.dimensions)
        unexpectedDimensions.discard(self.summary.universe.commonSkyPix)
        if unexpectedDimensions:
            raise NotImplementedError(
                f"QueryBuilder does not yet support joining in dimensions {unexpectedDimensions} that "
                f"were not provided originally to the QuerySummary object passed at construction."
            )
        joinOn = self.startJoin(table, dimensions, dimensions.names)
        self.finishJoin(table, joinOn)
        if datasets is not None:
            assert self._columns.datasets is None, \
                "At most one result dataset type can be returned by a query."
            self._columns.datasets = datasets

    def startJoin(self, table: sqlalchemy.sql.FromClause, dimensions: Iterable[Dimension],
                  columnNames: Iterable[str]
                  ) -> List[sqlalchemy.sql.ColumnElement]:
        """Begin a join on dimensions.

        Must be followed by call to `finishJoin`.

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

    def finishJoin(self, table: sqlalchemy.sql.FromClause, joinOn: List[sqlalchemy.sql.ColumnElement]
                   ) -> None:
        """Complete a join on dimensions.

        Must be preceded by call to `startJoin`.

        Parameters
        ----------
        table : `sqlalchemy.sql.FromClause`
            SQLAlchemy object representing the logical table (which may be a
            join or subquery expression) to be joined.  Must be the same object
            passed to `startJoin`.
        joinOn : `list` of `sqlalchemy.sql.ColumnElement`
            Sequence of boolean expressions that should be combined with AND
            to form (part of) the ON expression for this JOIN.  Should include
            at least the elements of the list returned by `startJoin`.
        """
        onclause: Optional[sqlalchemy.sql.ColumnElement]
        if len(joinOn) == 0:
            onclause = None
        elif len(joinOn) == 1:
            onclause = joinOn[0]
        else:
            onclause = sqlalchemy.sql.and_(*joinOn)
        self._simpleQuery.join(table, onclause=onclause)

    def _joinMissingDimensionElements(self) -> None:
        """Join all dimension element tables that were identified as necessary
        by `QuerySummary` and have not yet been joined.

        For internal use by `QueryBuilder` only; will be called (and should
        only by called) by `finish`.
        """
        # Join all DimensionElement tables that we need for spatial/temporal
        # joins/filters or a nontrivial WHERE expression.
        # We iterate over these in *reverse* topological order to minimize the
        # number of tables joined.  For example, the "visit" table provides
        # the primary key value for the "instrument" table it depends on, so we
        # don't need to join "instrument" as well unless we had a nontrivial
        # expression on it (and hence included it already above).
        for element in self.summary.universe.sorted(self.summary.mustHaveTableJoined, reverse=True):
            self.joinDimensionElement(element)
        # Join in any requested Dimension tables that don't already have their
        # primary keys identified by the query.
        for dimension in self.summary.universe.sorted(self.summary.mustHaveKeysJoined, reverse=True):
            if dimension not in self._columns.keys:
                self.joinDimensionElement(dimension)

    def _addWhereClause(self) -> None:
        """Add a WHERE clause to the query under construction, connecting all
        joined dimensions to the expression and data ID dimensions from
        `QuerySummary`.

        For internal use by `QueryBuilder` only; will be called (and should
        only by called) by `finish`.
        """
        if self.summary.where.tree is not None:
            self._simpleQuery.where.append(
                convertExpressionToSql(
                    self.summary.where.tree,
                    self.summary.universe,
                    columns=self._columns,
                    elements=self._elements,
                    bind=self.summary.where.bind,
                    tsRepr=self._managers.tsRepr,
                )
            )
        for dimension, columnsInQuery in self._columns.keys.items():
            if dimension in self.summary.where.dataId.graph:
                givenKey = self.summary.where.dataId[dimension]
                # Add a WHERE term for each column that corresponds to each
                # key.  This is redundant with the JOIN ON clauses that make
                # them equal to each other, but more constraints have a chance
                # of making things easier on the DB's query optimizer.
                for columnInQuery in columnsInQuery:
                    self._simpleQuery.where.append(columnInQuery == givenKey)
            else:
                # Dimension is not fully identified, but it might be a skypix
                # dimension that's constrained by a given region.
                if self.summary.where.region is not None and isinstance(dimension, SkyPixDimension):
                    # We know the region now.
                    givenSkyPixIds: List[int] = []
                    for begin, end in dimension.pixelization.envelope(self.summary.where.region):
                        givenSkyPixIds.extend(range(begin, end))
                    for columnInQuery in columnsInQuery:
                        self._simpleQuery.where.append(columnInQuery.in_(givenSkyPixIds))
        # If we are given an dataId with a timespan, and there are one or more
        # timespans in the query that aren't given, add a WHERE expression for
        # each of them.
        if self.summary.where.dataId.graph.temporal and self.summary.temporal:
            # Timespan is known now.
            givenInterval = self.summary.where.dataId.timespan
            assert givenInterval is not None
            for element, intervalInQuery in self._columns.timespans.items():
                assert element not in self.summary.where.dataId.graph.elements
                self._simpleQuery.where.append(
                    intervalInQuery.overlaps(self._managers.tsRepr.fromLiteral(givenInterval))
                )

    def finish(self, joinMissing: bool = True) -> Query:
        """Finish query constructing, returning a new `Query` instance.

        Parameters
        ----------
        joinMissing : `bool`, optional
            If `True` (default), automatically join any missing dimension
            element tables (according to the categorization of the
            `QuerySummary` the builder was constructed with).  `False` should
            only be passed if the caller can independently guarantee that all
            dimension relationships are already captured in non-dimension
            tables that have been manually included in the query.

        Returns
        -------
        query : `Query`
            A `Query` object that can be executed and used to interpret result
            rows.
        """
        if joinMissing:
            self._joinMissingDimensionElements()
        self._addWhereClause()
        if self._columns.isEmpty():
            return EmptyQuery(self.summary.requested.universe, managers=self._managers)
        return DirectQuery(graph=self.summary.requested,
                           uniqueness=DirectQueryUniqueness.NOT_UNIQUE,
                           whereRegion=self.summary.where.dataId.region,
                           simpleQuery=self._simpleQuery,
                           columns=self._columns,
                           managers=self._managers)
