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

from collections.abc import Iterable, Set
from typing import Any

import sqlalchemy.sql

from ...core import DatasetType, Dimension, DimensionElement, SimpleQuery, SkyPixDimension
from ...core.named import NamedKeyDict, NamedValueAbstractSet, NamedValueSet
from .._collectionType import CollectionType
from .._exceptions import DataIdValueError
from ..interfaces import CollectionRecord, DatasetRecordStorage, GovernorDimensionRecordStorage
from ..wildcards import CollectionWildcard
from ._query import DirectQuery, DirectQueryUniqueness, EmptyQuery, OrderByColumn, Query
from ._query_backend import QueryBackend
from ._structs import DatasetQueryColumns, QueryColumns, QuerySummary
from .expressions import convertExpressionToSql


class QueryBuilder:
    """A builder for potentially complex queries that join tables based
    on dimension relationships.

    Parameters
    ----------
    summary : `QuerySummary`
        Struct organizing the dimensions involved in the query.
    backend : `QueryBackend`
        Backend object that represents the `Registry` implementation.
    doomed_by : `Iterable` [ `str` ], optional
        A list of messages (appropriate for e.g. logging or exceptions) that
        explain why the query is known to return no results even before it is
        executed.  Queries with a non-empty list will never be executed.
    """

    def __init__(
        self,
        summary: QuerySummary,
        backend: QueryBackend,
        doomed_by: Iterable[str] = (),
    ):
        self.summary = summary
        self._backend = backend
        self._simpleQuery = SimpleQuery()
        self._elements: NamedKeyDict[DimensionElement, sqlalchemy.sql.FromClause] = NamedKeyDict()
        self._columns = QueryColumns()
        self._doomed_by = list(doomed_by)

        self._validateGovernors()

    def _validateGovernors(self) -> None:
        """Check that governor dimensions specified by query actually exist.

        This helps to avoid mistakes in governor values. It also implements
        consistent failure behavior for cases when governor dimensions are
        specified in either DataId ow WHERE clause.

        Raises
        ------
        DataIdValueError
            Raised when governor dimension values are not found.
        """
        for dimension, bounds in self.summary.where.governor_constraints.items():
            storage = self._backend.managers.dimensions[self._backend.universe[dimension]]
            if isinstance(storage, GovernorDimensionRecordStorage):
                if not (storage.values >= bounds):
                    raise DataIdValueError(
                        f"Unknown values specified for governor dimension {dimension}: "
                        f"{set(bounds - storage.values)}."
                    )

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
        storage = self._backend.managers.dimensions[element]
        fromClause = storage.join(
            self,
            regions=self._columns.regions if element in self.summary.spatial else None,
            timespans=self._columns.timespans if element in self.summary.temporal else None,
        )
        self._elements[element] = fromClause

    def joinDataset(
        self, datasetType: DatasetType, collections: Any, *, isResult: bool = True, findFirst: bool = False
    ) -> bool:
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
        assert datasetType in self.summary.datasets
        collections = CollectionWildcard.from_expression(collections)
        if isResult and findFirst:
            collections.require_ordered()
        # If we are searching all collections with no constraints, loop over
        # RUN collections only, because that will include all datasets.
        collectionTypes: Set[CollectionType]
        if collections == CollectionWildcard():
            collectionTypes = {CollectionType.RUN}
        else:
            collectionTypes = CollectionType.all()
        datasetRecordStorage = self._backend.managers.datasets.find(datasetType.name)
        if datasetRecordStorage is None:
            # Unrecognized dataset type means no results.  It might be better
            # to raise here, but this is consistent with previous behavior,
            # which is expected by QuantumGraph generation code in pipe_base.
            self._doomed_by.append(
                f"Dataset type {datasetType.name!r} is not registered, so no instances of it can exist in "
                "any collection."
            )
            return False
        collectionRecords: list[CollectionRecord] = []
        rejections: list[str] = []
        for collectionRecord in self._backend.resolve_collection_wildcard(
            collections, collection_types=collectionTypes
        ):
            # Only include collections that (according to collection summaries)
            # might have datasets of this type and governor dimensions
            # consistent with the query's WHERE clause.
            collection_summary = self._backend.managers.datasets.getCollectionSummary(collectionRecord)
            if not collection_summary.is_compatible_with(
                datasetType,
                self.summary.where.governor_constraints,
                rejections=rejections,
                name=collectionRecord.name,
            ):
                continue
            if collectionRecord.type is CollectionType.CALIBRATION:
                # If collection name was provided explicitly then say sorry if
                # this is a kind of query we don't support yet; otherwise
                # collection is a part of chained one or regex match and we
                # skip it to not break queries of other included collections.
                if datasetType.isCalibration():
                    if self.summary.temporal or self.summary.mustHaveKeysJoined.temporal:
                        if collectionRecord.name in collections.strings:
                            raise NotImplementedError(
                                f"Temporal query for dataset type '{datasetType.name}' in CALIBRATION-type "
                                f"collection '{collectionRecord.name}' is not yet supported."
                            )
                        else:
                            rejections.append(
                                f"Not searching for dataset {datasetType.name!r} in CALIBRATION collection "
                                f"{collectionRecord.name!r} because temporal calibration queries aren't "
                                "implemented; this is not an error only because the query structure implies "
                                "that searching this collection may be incidental."
                            )
                            continue
                    elif findFirst:
                        if collectionRecord.name in collections.strings:
                            raise NotImplementedError(
                                f"Find-first query for dataset type '{datasetType.name}' in "
                                f"CALIBRATION-type collection '{collectionRecord.name}' is not yet "
                                "supported."
                            )
                        else:
                            rejections.append(
                                f"Not searching for dataset {datasetType.name!r} in CALIBRATION collection "
                                f"{collectionRecord.name!r} because find-first calibration queries aren't "
                                "implemented; this is not an error only because the query structure implies "
                                "that searching this collection may be incidental."
                            )
                            continue
                    else:
                        collectionRecords.append(collectionRecord)
                else:
                    # We can never find a non-calibration dataset in a
                    # CALIBRATION collection.
                    rejections.append(
                        f"Not searching for non-calibration dataset {datasetType.name!r} "
                        f"in CALIBRATION collection {collectionRecord.name!r}."
                    )
                    continue
            else:
                collectionRecords.append(collectionRecord)
        if isResult:
            if findFirst:
                subquery = self._build_dataset_search_subquery(
                    datasetRecordStorage,
                    collectionRecords,
                )
            else:
                subquery = self._build_dataset_query_subquery(
                    datasetRecordStorage,
                    collectionRecords,
                )
            columns = DatasetQueryColumns(
                datasetType=datasetType,
                id=subquery.columns["id"],
                runKey=subquery.columns[self._backend.managers.collections.getRunForeignKeyName()],
                ingestDate=subquery.columns["ingest_date"],
            )
        else:
            subquery = self._build_dataset_constraint_subquery(datasetRecordStorage, collectionRecords)
            columns = None
        self.joinTable(subquery, datasetType.dimensions.required, datasets=columns)
        if not collectionRecords:
            if rejections:
                self._doomed_by.extend(rejections)
            else:
                self._doomed_by.append(f"No collections to search matching expression {collections}.")
            return False
        return not self._doomed_by

    def _build_dataset_constraint_subquery(
        self, storage: DatasetRecordStorage, collections: list[CollectionRecord]
    ) -> sqlalchemy.sql.FromClause:
        """Internal helper method to build a dataset subquery for a parent
        query that does not return dataset results.

        Parameters
        ----------
        storage : `DatasetRecordStorage`
            Storage object for the dataset type the subquery is for.
        collections : `list` [ `CollectionRecord` ]
            Records for the collections to be searched.  Collections with no
            datasets of this type or with governor dimensions incompatible with
            the rest of the query should already have been filtered out.
            `~CollectionType.CALIBRATION` collections should also be filtered
            out if this is a temporal query.

        Returns
        -------
        sql : `sqlalchemy.sql.FromClause`
            A SQLAlchemy aliased subquery object.  Has columns for each
            dataset type dimension, or an unspecified column (just to prevent
            SQL syntax errors) where there is no data ID.
        """
        return storage.select(
            *collections,
            dataId=SimpleQuery.Select,
            # If this dataset type has no dimensions, we're in danger of
            # generating an invalid subquery that has no columns in the
            # SELECT clause.  An easy fix is to just select some arbitrary
            # column that goes unused, like the dataset ID.
            id=None if storage.datasetType.dimensions else SimpleQuery.Select,
            run=None,
            ingestDate=None,
            timespan=None,
        ).alias(storage.datasetType.name)

    def _build_dataset_query_subquery(
        self, storage: DatasetRecordStorage, collections: list[CollectionRecord]
    ) -> sqlalchemy.sql.FromClause:
        """Internal helper method to build a dataset subquery for a parent
        query that returns all matching dataset results.

        Parameters
        ----------
        storage : `DatasetRecordStorage`
            Storage object for the dataset type the subquery is for.
        collections : `list` [ `CollectionRecord` ]
            Records for the collections to be searched.  Collections with no
            datasets of this type or with governor dimensions incompatible with
            the rest of the query should already have been filtered out.
            `~CollectionType.CALIBRATION` collections should also be filtered
            out if this is a temporal query.

        Returns
        -------
        sql : `sqlalchemy.sql.FromClause`
            A SQLAlchemy aliased subquery object.  Has columns for each dataset
            type dimension, the dataset ID, the `~CollectionType.RUN`
            collection key, and the ingest date.
        """
        sql = storage.select(
            *collections,
            dataId=SimpleQuery.Select,
            id=SimpleQuery.Select,
            run=SimpleQuery.Select,
            ingestDate=SimpleQuery.Select,
            timespan=None,
        ).alias(storage.datasetType.name)
        return sql

    def _build_dataset_search_subquery(
        self, storage: DatasetRecordStorage, collections: list[CollectionRecord]
    ) -> sqlalchemy.sql.FromClause:
        """Internal helper method to build a dataset subquery for a parent
        query that returns the first matching dataset for each data ID and
        dataset type name from an ordered list of collections.

        Parameters
        ----------
        storage : `DatasetRecordStorage`
            Storage object for the dataset type the subquery is for.
        collections : `list` [ `CollectionRecord` ]
            Records for the collections to be searched.  Collections with no
            datasets of this type or with governor dimensions incompatible with
            the rest of the query should already have been filtered out.
            `~CollectionType.CALIBRATION` collections should be filtered out as
            well.

        Returns
        -------
        sql : `sqlalchemy.sql.FromClause`
            A SQLAlchemy aliased subquery object.  Has columns for each dataset
            type dimension, the dataset ID, the `~CollectionType.RUN`
            collection key, and the ingest date.
        """
        # Query-simplification shortcut: if there is only one collection, a
        # find-first search is just a regular result subquery.  Same is true
        # if this is a doomed query with no collections to search.
        if len(collections) <= 1:
            return self._build_dataset_query_subquery(storage, collections)
        # In the more general case, we build a subquery of the form below to
        # search the collections in order.
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
        # We'll start with the Common Table Expression (CTE) at the top.
        search = storage.select(
            *collections,
            dataId=SimpleQuery.Select,
            id=SimpleQuery.Select,
            run=SimpleQuery.Select,
            ingestDate=SimpleQuery.Select,
            timespan=None,
            rank=SimpleQuery.Select,
        ).cte(f"{storage.datasetType.name}_search")
        # Now we fill out the SELECT from the CTE, and the subquery it contains
        # (at the same time, since they have the same columns, aside from the
        # OVER clause).
        run_key_name = self._backend.managers.collections.getRunForeignKeyName()
        window_data_id_cols = [
            search.columns[name].label(name) for name in storage.datasetType.dimensions.required.names
        ]
        window_select_cols = [
            search.columns["id"].label("id"),
            search.columns[run_key_name].label(run_key_name),
            search.columns["ingest_date"].label("ingest_date"),
        ]
        window_select_cols += window_data_id_cols
        window_select_cols.append(
            sqlalchemy.sql.func.row_number()
            .over(partition_by=window_data_id_cols, order_by=search.columns["rank"])
            .label("rownum")
        )
        window = (
            sqlalchemy.sql.select(*window_select_cols)
            .select_from(search)
            .alias(f"{storage.datasetType.name}_window")
        )
        sql = (
            sqlalchemy.sql.select(*[window.columns[col.name].label(col.name) for col in window_select_cols])
            .select_from(window)
            .where(window.columns["rownum"] == 1)
            .alias(storage.datasetType.name)
        )
        return sql

    def joinTable(
        self,
        table: sqlalchemy.sql.FromClause,
        dimensions: NamedValueAbstractSet[Dimension],
        *,
        datasets: DatasetQueryColumns | None = None,
    ) -> None:
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
        unexpectedDimensions = NamedValueSet(dimensions - self.summary.mustHaveKeysJoined.dimensions)
        unexpectedDimensions.discard(self._backend.universe.commonSkyPix)
        if unexpectedDimensions:
            raise NotImplementedError(
                f"QueryBuilder does not yet support joining in dimensions {unexpectedDimensions} that "
                f"were not provided originally to the QuerySummary object passed at construction."
            )
        joinOn = self.startJoin(table, dimensions, dimensions.names)
        self.finishJoin(table, joinOn)
        if datasets is not None:
            assert (
                self._columns.datasets is None
            ), "At most one result dataset type can be returned by a query."
            self._columns.datasets = datasets

    def startJoin(
        self, table: sqlalchemy.sql.FromClause, dimensions: Iterable[Dimension], columnNames: Iterable[str]
    ) -> list[sqlalchemy.sql.ColumnElement]:
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

    def finishJoin(
        self, table: sqlalchemy.sql.FromClause, joinOn: list[sqlalchemy.sql.ColumnElement]
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
        onclause: sqlalchemy.sql.ColumnElement | None
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
        for element in self._backend.universe.sorted(self.summary.mustHaveTableJoined, reverse=True):
            self.joinDimensionElement(element)
        # Join in any requested Dimension tables that don't already have their
        # primary keys identified by the query.
        for dimension in self._backend.universe.sorted(self.summary.mustHaveKeysJoined, reverse=True):
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
                    self._backend.universe,
                    columns=self._columns,
                    elements=self._elements,
                    bind=self.summary.where.bind,
                    TimespanReprClass=self._backend.managers.column_types.timespan_cls,
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
                    givenSkyPixIds: list[int] = []
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
                    intervalInQuery.overlaps(
                        self._backend.managers.column_types.timespan_cls.fromLiteral(givenInterval)
                    )
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
            return EmptyQuery(
                self._backend.universe,
                backend=self._backend,
                doomed_by=self._doomed_by,
            )
        return DirectQuery(
            graph=self.summary.requested,
            uniqueness=DirectQueryUniqueness.NOT_UNIQUE,
            whereRegion=self.summary.where.region,
            simpleQuery=self._simpleQuery,
            columns=self._columns,
            order_by_columns=self._order_by_columns(),
            limit=self.summary.limit,
            backend=self._backend,
            doomed_by=self._doomed_by,
        )

    def _order_by_columns(self) -> Iterable[OrderByColumn]:
        """Generate columns to be used for ORDER BY clause.

        Returns
        -------
        order_by_columns : `Iterable` [ `ColumnIterable` ]
            Sequence of columns to appear in ORDER BY clause.
        """
        order_by_columns: list[OrderByColumn] = []
        if not self.summary.order_by:
            return order_by_columns

        for order_by_column in self.summary.order_by.order_by_columns:

            column: sqlalchemy.sql.ColumnElement
            if order_by_column.column is None:
                # dimension name, it has to be in SELECT list already, only
                # add it to ORDER BY
                assert isinstance(order_by_column.element, Dimension), "expecting full Dimension"
                column = self._columns.getKeyColumn(order_by_column.element)
            else:
                table = self._elements[order_by_column.element]

                if order_by_column.column in ("timespan.begin", "timespan.end"):
                    TimespanReprClass = self._backend.managers.column_types.timespan_cls
                    timespan_repr = TimespanReprClass.from_columns(table.columns)
                    if order_by_column.column == "timespan.begin":
                        column = timespan_repr.lower()
                        label = f"{order_by_column.element.name}_timespan_begin"
                    else:
                        column = timespan_repr.upper()
                        label = f"{order_by_column.element.name}_timespan_end"
                else:
                    column = table.columns[order_by_column.column]
                    # make a unique label for it
                    label = f"{order_by_column.element.name}_{order_by_column.column}"

                column = column.label(label)

            order_by_columns.append(OrderByColumn(column=column, ordering=order_by_column.ordering))

        return order_by_columns
