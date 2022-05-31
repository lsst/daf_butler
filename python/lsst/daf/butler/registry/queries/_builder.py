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

from typing import Any, Iterable, List, Optional

import sqlalchemy.sql

from ...core import DatasetType, Dimension, DimensionElement, SimpleQuery, SkyPixDimension, sql
from ...core.named import NamedKeyDict, NamedValueAbstractSet, NamedValueSet
from .._exceptions import DataIdValueError
from .._query_backend import QueryBackend
from ..interfaces import GovernorDimensionRecordStorage
from ..wildcards import CollectionSearch, CollectionWildcard
from ._query import DirectQuery, DirectQueryUniqueness, EmptyQuery, OrderByColumn, Query
from ._structs import DatasetQueryColumns, QueryColumns, QuerySummary


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
        for dimension, bounds in self.summary.where.constraints.dimensions.items():
            storage = self._backend.managers.dimensions[self.summary.requested.universe[dimension]]
            if isinstance(storage, GovernorDimensionRecordStorage):
                try:
                    bounds.with_concrete_bounds(storage.values)
                except LookupError as err:
                    raise DataIdValueError(
                        f"Unknown values specified for governor dimension {dimension}: {set(err.args[0])}."
                    ) from None

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
        if isResult and findFirst:
            collections = CollectionSearch.fromExpression(collections)
        else:
            collections = CollectionWildcard.fromExpression(collections)
        rejections: list[str] = []
        collection_records = self._backend.resolve_dataset_collections(
            datasetType,
            collections,
            constraints=self.summary.where.constraints,
            rejections=rejections,
            allow_calibration_collections=(
                not findFirst and not (self.summary.temporal or self.summary.mustHaveKeysJoined.temporal)
            ),
        )
        columns_requested = {"dataset_id", "run", "ingest_date"} if isResult else frozenset()
        if not collection_records:
            relation = self._backend.make_doomed_dataset_relation(datasetType, columns_requested, rejections)
        elif isResult and findFirst:
            relation = self._backend.make_dataset_search_relation(
                datasetType,
                collection_records,
                columns_requested,
                constraints=self.summary.where.constraints,
            )
        else:
            relation = self._backend.make_dataset_query_relation(
                datasetType,
                collection_records,
                columns_requested,
                constraints=self.summary.where.constraints,
            )
        subquery = relation.to_sql_from()
        if isResult:
            columns = DatasetQueryColumns(
                datasetType=datasetType,
                id=subquery.columns[str(sql.DatasetColumnTag(datasetType.name, "dataset_id"))],
                runKey=subquery.columns[str(sql.DatasetColumnTag(datasetType.name, "run"))],
                ingestDate=subquery.columns[str(sql.DatasetColumnTag(datasetType.name, "ingest_date"))],
            )
        else:
            columns = None
        self.joinTable(subquery, datasetType.dimensions.required, datasets=columns)
        self._doomed_by.extend(relation.doomed_by)
        return not self._doomed_by

    def joinTable(
        self,
        table: sqlalchemy.sql.FromClause,
        dimensions: NamedValueAbstractSet[Dimension],
        *,
        datasets: Optional[DatasetQueryColumns] = None,
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
        unexpectedDimensions.discard(self.summary.universe.commonSkyPix)
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

    def finishJoin(
        self, table: sqlalchemy.sql.FromClause, joinOn: List[sqlalchemy.sql.ColumnElement]
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
        # Make a new-style column mapping to feed to predicates.  After
        # Relation is fully integrated into query builder this will be created
        # automatically.
        logical_columns = self._columns.make_logical_column_mapping()
        if self.summary.where.expression_predicate is not None:
            # Most column types are tracked in self._columns, and handled above
            # this block; we only need to worry about scalar dimension record
            # fact columns referenced by the expression here.
            for tag in self.summary.where.expression_predicate.columns_required:
                match tag:
                    case sql.DimensionRecordColumnTag(element=element_name, column=column_name):
                        table = self._elements[element_name]
                        if not tag.is_timespan and not tag.is_spatial_region:
                            logical_columns[tag] = table.columns[column_name]
                    case _:
                        pass
        # Make ColumnTagSet to re-categorize those in the new style.  Again,
        # this will be automatic after more complete integration with Relation.
        column_tag_set = sql.ColumnTagSet._from_iterable(logical_columns)
        # Append WHERE clause terms from predicates.
        predicates: list[sql.Predicate] = []
        if self.summary.where.expression_predicate is not None:
            predicates.append(self.summary.where.expression_predicate)
        if self.summary.where.data_id:
            known_dimensions = self.summary.where.data_id.graph.intersection(self.summary.mustHaveKeysJoined)
            known_data_id = self.summary.where.data_id.subset(known_dimensions)
            predicates.append(sql.Predicate.from_data_coordinate(known_data_id))
        if self.summary.where.spatial_constraint is not None:
            for dimension_name in column_tag_set.dimensions:
                dimension = self._backend.managers.column_types.universe[dimension_name]
                if dimension_name not in self.summary.where.data_id.graph.names and isinstance(
                    dimension, SkyPixDimension
                ):
                    predicates.append(
                        sql.Predicate.from_spatial_constraint(
                            self.summary.where.spatial_constraint, dimension
                        )
                    )
        if self.summary.where.temporal_constraint is not None:
            for element_name, column_names in column_tag_set.dimension_records.items():
                if "timespan" in column_names and element_name not in self.summary.where.data_id.graph.names:
                    predicates.append(
                        sql.Predicate.from_temporal_constraint(
                            self.summary.where.temporal_constraint,
                            sql.DimensionRecordColumnTag(element_name, "timespan"),
                        )
                    )
        for predicate in predicates:
            self._simpleQuery.where.extend(
                predicate.to_sql_booleans(logical_columns, self._backend.managers.column_types)
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
                self.summary.requested.universe,
                backend=self._backend,
                doomed_by=self._doomed_by,
            )
        return DirectQuery(
            graph=self.summary.requested,
            uniqueness=DirectQueryUniqueness.NOT_UNIQUE,
            spatial_constraint=self.summary.where.spatial_constraint,
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
        order_by_columns: List[OrderByColumn] = []
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
