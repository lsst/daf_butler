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

import dataclasses
import enum
import itertools
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, ContextManager, Dict, Iterable, Iterator, Mapping, Optional, Tuple

import sqlalchemy
from lsst.sphgeom import Region

from ...core import (
    DataCoordinate,
    DatasetRef,
    DatasetType,
    Dimension,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
    DimensionUniverse,
    SimpleQuery,
    addDimensionForeignKey,
    ddl,
)
from ..interfaces import Database
from ._query_backend import QueryBackend
from ._structs import DatasetQueryColumns, QueryColumns, QuerySummary

if TYPE_CHECKING:
    from ._builder import QueryBuilder


@dataclasses.dataclass(frozen=True)
class OrderByColumn:
    """Information about single column in ORDER BY clause."""

    column: sqlalchemy.sql.ColumnElement
    """Name of the column or `None` for primary key (`str` or `None`)"""

    ordering: bool
    """True for ascending order, False for descending (`bool`)."""

    @property
    def column_order(self) -> sqlalchemy.sql.ColumnElement:
        """Column element for use in ORDER BY clause
        (`sqlalchemy.sql.ColumnElement`)
        """
        return self.column.asc() if self.ordering else self.column.desc()


class Query(ABC):
    """An abstract base class for queries that return some combination of
    `DatasetRef` and `DataCoordinate` objects.

    Parameters
    ----------
    graph : `DimensionGraph`
        Object describing the dimensions included in the query.
    whereRegion : `lsst.sphgeom.Region`, optional
        Region that all region columns in all returned rows must overlap.
    backend : `QueryBackend`
        Backend object that represents the `Registry` implementation.
    doomed_by : `Iterable` [ `str` ], optional
        A list of messages (appropriate for e.g. logging or exceptions) that
        explain why the query is known to return no results even before it is
        executed.  Queries with a non-empty list will never be executed.

    Notes
    -----
    The `Query` hierarchy abstracts over the database/SQL representation of a
    particular set of data IDs or datasets.  It is expected to be used as a
    backend for other objects that provide more natural interfaces for one or
    both of these, not as part of a public interface to query results.
    """

    def __init__(
        self,
        *,
        graph: DimensionGraph,
        whereRegion: Optional[Region],
        backend: QueryBackend,
        doomed_by: Iterable[str] = (),
    ):
        self.graph = graph
        self.whereRegion = whereRegion
        self.backend = backend
        self._doomed_by = tuple(doomed_by)
        self._filtered_by_join: Optional[int] = None
        self._filtered_by_where: Optional[int] = None

    @abstractmethod
    def isUnique(self) -> bool:
        """Return `True` if this query's rows are guaranteed to be unique, and
        `False` otherwise.

        If this query has dataset results (`datasetType` is not `None`),
        uniqueness applies to the `DatasetRef` instances returned by
        `extractDatasetRef` from the result of `rows`.  If it does not have
        dataset results, uniqueness applies to the `DataCoordinate` instances
        returned by `extractDataId`.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDimensionColumn(self, name: str) -> sqlalchemy.sql.ColumnElement:
        """Return the query column that contains the primary key value for
        the dimension with the given name.

        Parameters
        ----------
        name : `str`
            Name of the dimension.

        Returns
        -------
        column : `sqlalchemy.sql.ColumnElement`.
            SQLAlchemy object representing a column in the query.

        Notes
        -----
        This method is intended primarily as a hook for subclasses to implement
        and the ABC to call in order to provide higher-level functionality;
        code that uses `Query` objects (but does not implement one) should
        usually not have to call this method.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def spatial(self) -> Iterator[DimensionElement]:
        """An iterator over the dimension element columns used in post-query
        filtering of spatial overlaps (`Iterator` [ `DimensionElement` ]).

        Notes
        -----
        This property is intended primarily as a hook for subclasses to
        implement and the ABC to call in order to provide higher-level
        functionality; code that uses `Query` objects (but does not implement
        one) should usually not have to access this property.
        """
        raise NotImplementedError()

    @abstractmethod
    def getRegionColumn(self, name: str) -> sqlalchemy.sql.ColumnElement:
        """Return a region column for one of the dimension elements iterated
        over by `spatial`.

        Parameters
        ----------
        name : `str`
            Name of the element.

        Returns
        -------
        column : `sqlalchemy.sql.ColumnElement`
            SQLAlchemy representing a result column in the query.

        Notes
        -----
        This method is intended primarily as a hook for subclasses to implement
        and the ABC to call in order to provide higher-level functionality;
        code that uses `Query` objects (but does not implement one) should
        usually not have to call this method.
        """
        raise NotImplementedError()

    @property
    def datasetType(self) -> Optional[DatasetType]:
        """The `DatasetType` of datasets returned by this query, or `None`
        if there are no dataset results (`DatasetType` or `None`).
        """
        cols = self.getDatasetColumns()
        if cols is None:
            return None
        return cols.datasetType

    def count(self, db: Database, *, exact: bool = True) -> int:
        """Count the number of rows this query would return.

        Parameters
        ----------
        db : `Database`
            Object managing the database connection.
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
        if self._doomed_by:
            return 0
        sql = self.sql
        if sql is None:
            return 1
        if exact and self.spatial:
            filtered_count = 0
            for _ in self.rows(db):
                filtered_count += 1
            return filtered_count
        else:
            with db.query(sql.with_only_columns([sqlalchemy.sql.func.count()]).order_by(None)) as sql_result:
                return sql_result.scalar()

    def any(
        self,
        db: Database,
        *,
        execute: bool = True,
        exact: bool = True,
    ) -> bool:
        """Test whether this query returns any results.

        Parameters
        ----------
        db : `Database`
            Object managing the database connection.
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
        if self._doomed_by:
            return False
        sql = self.sql
        if sql is None:
            return True
        if exact and not execute:
            raise TypeError("Cannot obtain exact results without executing the query.")
        if exact and self.spatial:
            for _ in self.rows(db):
                return True
            return False
        elif execute:
            with db.query(sql.limit(1)) as sql_result:
                return sql_result.one_or_none() is not None
        else:
            return True

    def explain_no_results(
        self,
        db: Database,
        *,
        followup: bool = True,
    ) -> Iterator[str]:
        """Return human-readable messages that may help explain why the query
        yields no results.

        Parameters
        ----------
        db : `Database`
            Object managing the database connection.
        followup : `bool`, optional
            If `True` (default) perform inexpensive follow-up queries if no
            diagnostics are available from query generation alone.

        Returns
        -------
        messages : `Iterator` [ `str` ]
            String messages that describe reasons the query might not yield any
            results.

        Notes
        -----
        Messages related to post-query filtering are only available if `rows`,
        `any`, or `count` was already called with the same region (with
        ``exact=True`` for the latter two).
        """
        from ._builder import QueryBuilder

        if self._doomed_by:
            yield from self._doomed_by
            return
        if self._filtered_by_where:
            yield (
                f"{self._filtered_by_where} result rows were filtered out because "
                "one or more region did not overlap the WHERE-clause region."
            )
        if self._filtered_by_join:
            yield (
                f"{self._filtered_by_join} result rows were filtered out because "
                "one or more regions did not overlap."
            )
        if (not followup) or self._filtered_by_join or self._filtered_by_where:
            return
        # Query didn't return results even before client-side filtering, and
        # caller says we can do follow-up queries to determine why.
        # Start by seeing if there are _any_ dimension records for each element
        # involved.
        for element in self.graph.elements:
            summary = QuerySummary(element.graph)
            builder = QueryBuilder(summary, self.backend)
            followup_query = builder.finish()
            if not followup_query.any(db, exact=False):
                yield f"No dimension records for element '{element.name}' found."
                yield from followup_query.explain_no_results(db, followup=False)
                return

    @abstractmethod
    def getDatasetColumns(self) -> Optional[DatasetQueryColumns]:
        """Return the columns for the datasets returned by this query.

        Returns
        -------
        columns : `DatasetQueryColumns` or `None`
            Struct containing SQLAlchemy representations of the result columns
            for a dataset.

        Notes
        -----
        This method is intended primarily as a hook for subclasses to implement
        and the ABC to call in order to provide higher-level functionality;
        code that uses `Query` objects (but does not implement one) should
        usually not have to call this method.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def sql(self) -> Optional[sqlalchemy.sql.FromClause]:
        """A SQLAlchemy object representing the full query
        (`sqlalchemy.sql.FromClause` or `None`).

        This is `None` in the special case where the query has no columns, and
        only one logical row.
        """
        raise NotImplementedError()

    def rows(self, db: Database) -> Iterator[Optional[sqlalchemy.engine.Row]]:
        """Execute the query and yield result rows, applying `predicate`.

        Parameters
        ----------
        db : `Database`
            Object managing the database connection.

        Yields
        ------
        row : `sqlalchemy.engine.RowProxy` or `None`
            Result row from the query.  `None` may yielded exactly once instead
            of any real rows to indicate an empty query (see `EmptyQuery`).
        """
        if self._doomed_by:
            return
        self._filtered_by_where = 0
        self._filtered_by_join = 0
        with db.query(self.sql) as sql_result:
            sql_rows = sql_result.fetchall()
        for row in sql_rows:
            rowRegions = [row._mapping[self.getRegionColumn(element.name)] for element in self.spatial]
            if self.whereRegion and any(r.isDisjointFrom(self.whereRegion) for r in rowRegions):
                self._filtered_by_where += 1
                continue
            if not not any(a.isDisjointFrom(b) for a, b in itertools.combinations(rowRegions, 2)):
                self._filtered_by_join += 1
                continue
            yield row

    def extractDimensionsTuple(
        self, row: Optional[sqlalchemy.engine.RowProxy], dimensions: Iterable[Dimension]
    ) -> tuple:
        """Extract a tuple of data ID values from a result row.

        Parameters
        ----------
        row : `sqlalchemy.engine.RowProxy` or `None`
            A result row from a SQLAlchemy SELECT query, or `None` to indicate
            the row from an `EmptyQuery`.
        dimensions : `Iterable` [ `Dimension` ]
            The dimensions to include in the returned tuple, in order.

        Returns
        -------
        values : `tuple`
            A tuple of dimension primary key values.
        """
        if row is None:
            assert not tuple(dimensions), "Can only utilize empty query row when there are no dimensions."
            return ()
        return tuple(row._mapping[self.getDimensionColumn(dimension.name)] for dimension in dimensions)

    def extractDataId(
        self,
        row: Optional[sqlalchemy.engine.RowProxy],
        *,
        graph: Optional[DimensionGraph] = None,
        records: Optional[Mapping[str, Mapping[tuple, DimensionRecord]]] = None,
    ) -> DataCoordinate:
        """Extract a data ID from a result row.

        Parameters
        ----------
        row : `sqlalchemy.engine.RowProxy` or `None`
            A result row from a SQLAlchemy SELECT query, or `None` to indicate
            the row from an `EmptyQuery`.
        graph : `DimensionGraph`, optional
            The dimensions the returned data ID should identify.  If not
            provided, this will be all dimensions in `QuerySummary.requested`.
        records : `Mapping` [ `str`, `Mapping` [ `tuple`, `DimensionRecord` ] ]
            Nested mapping containing records to attach to the returned
            `DataCoordinate`, for which `~DataCoordinate.hasRecords` will
            return `True`.  If provided, outer keys must include all dimension
            element names in ``graph``, and inner keys should be tuples of
            dimension primary key values in the same order as
            ``element.graph.required``.  If not provided,
            `DataCoordinate.hasRecords` will return `False` on the returned
            object.

        Returns
        -------
        dataId : `DataCoordinate`
            A data ID that identifies all required and implied dimensions.  If
            ``records is not None``, this is have
            `~DataCoordinate.hasRecords()` return `True`.
        """
        if graph is None:
            graph = self.graph
        if not graph:
            return DataCoordinate.makeEmpty(self.graph.universe)
        dataId = DataCoordinate.fromFullValues(
            graph, self.extractDimensionsTuple(row, itertools.chain(graph.required, graph.implied))
        )
        if records is not None:
            recordsForRow = {}
            for element in graph.elements:
                key = tuple(dataId.subset(element.graph).values())
                recordsForRow[element.name] = records[element.name].get(key)
            return dataId.expanded(recordsForRow)
        else:
            return dataId

    def extractDatasetRef(
        self,
        row: sqlalchemy.engine.RowProxy,
        dataId: Optional[DataCoordinate] = None,
        records: Optional[Mapping[str, Mapping[tuple, DimensionRecord]]] = None,
    ) -> DatasetRef:
        """Extract a `DatasetRef` from a result row.

        Parameters
        ----------
        row : `sqlalchemy.engine.RowProxy`
            A result row from a SQLAlchemy SELECT query.
        dataId : `DataCoordinate`
            Data ID to attach to the `DatasetRef`.  A minimal (i.e. base class)
            `DataCoordinate` is constructed from ``row`` if `None`.
        records : `Mapping` [ `str`, `Mapping` [ `tuple`, `DimensionRecord` ] ]
            Records to use to return an `ExpandedDataCoordinate`.  If provided,
            outer keys must include all dimension element names in ``graph``,
            and inner keys should be tuples of dimension primary key values
            in the same order as ``element.graph.required``.

        Returns
        -------
        ref : `DatasetRef`
            Reference to the dataset; guaranteed to have `DatasetRef.id` not
            `None`.
        """
        datasetColumns = self.getDatasetColumns()
        assert datasetColumns is not None
        if dataId is None:
            dataId = self.extractDataId(row, graph=datasetColumns.datasetType.dimensions, records=records)
        runRecord = self.backend.managers.collections[row._mapping[datasetColumns.runKey]]
        return DatasetRef(
            datasetColumns.datasetType, dataId, id=row._mapping[datasetColumns.id], run=runRecord.name
        )

    def _makeSubsetQueryColumns(
        self, *, graph: Optional[DimensionGraph] = None, datasets: bool = True, unique: bool = False
    ) -> Tuple[DimensionGraph, Optional[QueryColumns]]:
        """Helper method for subclass implementations of `subset`.

        Parameters
        ----------
        graph : `DimensionGraph`, optional
            Dimensions to include in the new `Query` being constructed.
            ``subset`` implementations should generally just forward their
            own ``graph`` argument here.
        datasets : `bool`, optional
            Whether the new `Query` should include dataset results.  Defaults
            to `True`, but is ignored if ``self`` does not include dataset
            results.
        unique : `bool`, optional
            Whether the new `Query` should guarantee unique results (this may
            come with a performance penalty).

        Returns
        -------
        graph : `DimensionGraph`
            The dimensions of the new `Query`.  This is exactly the same as
            the argument of the same name, with ``self.graph`` used if that
            argument is `None`.
        columns : `QueryColumns` or `None`
            A struct containing the SQLAlchemy column objects to use in the
            new query, constructed by delegating to other (mostly abstract)
            methods on ``self``.  If `None`, `subset` may return ``self``.
        """
        if graph is None:
            graph = self.graph
        if (
            graph == self.graph
            and (self.getDatasetColumns() is None or datasets)
            and (self.isUnique() or not unique)
        ):
            return graph, None
        columns = QueryColumns()
        for dimension in graph.dimensions:
            col = self.getDimensionColumn(dimension.name)
            columns.keys[dimension] = [col]
        if not unique:
            for element in self.spatial:
                col = self.getRegionColumn(element.name)
                columns.regions[element] = col
        if datasets and self.getDatasetColumns() is not None:
            columns.datasets = self.getDatasetColumns()
        return graph, columns

    @abstractmethod
    def materialize(self, db: Database) -> ContextManager[Query]:
        """Execute this query and insert its results into a temporary table.

        Parameters
        ----------
        db : `Database`
            Database engine to execute the query against.

        Returns
        -------
        context : `typing.ContextManager` [ `MaterializedQuery` ]
            A context manager that ensures the temporary table is created and
            populated in ``__enter__`` (returning a `MaterializedQuery` object
            backed by that table), and dropped in ``__exit__``.  If ``self``
            is already a `MaterializedQuery`, ``__enter__`` may just return
            ``self`` and ``__exit__`` may do nothing (reflecting the fact that
            an outer context manager should already take care of everything
            else).
        """
        raise NotImplementedError()

    @abstractmethod
    def subset(
        self, *, graph: Optional[DimensionGraph] = None, datasets: bool = True, unique: bool = False
    ) -> Query:
        """Return a new `Query` whose columns and/or rows are (mostly) subset
        of this one's.

        Parameters
        ----------
        graph : `DimensionGraph`, optional
            Dimensions to include in the new `Query` being constructed.
            If `None` (default), ``self.graph`` is used.
        datasets : `bool`, optional
            Whether the new `Query` should include dataset results.  Defaults
            to `True`, but is ignored if ``self`` does not include dataset
            results.
        unique : `bool`, optional
            Whether the new `Query` should guarantee unique results (this may
            come with a performance penalty).

        Returns
        -------
        query : `Query`
            A query object corresponding to the given inputs.  May be ``self``
            if no changes were requested.

        Notes
        -----
        The way spatial overlaps are handled at present makes it impossible to
        fully guarantee in general that the new query's rows are a subset of
        this one's while also returning unique rows.  That's because the
        database is only capable of performing approximate, conservative
        overlaps via the common skypix system; we defer actual region overlap
        operations to per-result-row Python logic.  But including the region
        columns necessary to do that postprocessing in the query makes it
        impossible to do a SELECT DISTINCT on the user-visible dimensions of
        the query.  For example, consider starting with a query with dimensions
        (instrument, skymap, visit, tract).  That involves a spatial join
        between visit and tract, and we include the region columns from both
        tables in the results in order to only actually yield result rows
        (see `predicate` and `rows`) where the regions in those two columns
        overlap.  If the user then wants to subset to just (skymap, tract) with
        unique results, we have two unpalatable options:

         - we can do a SELECT DISTINCT with just the skymap and tract columns
           in the SELECT clause, dropping all detailed overlap information and
           including some tracts that did not actually overlap any of the
           visits in the original query (but were regarded as _possibly_
           overlapping via the coarser, common-skypix relationships);

         - we can include the tract and visit region columns in the query, and
           continue to filter out the non-overlapping pairs, but completely
           disregard the user's request for unique tracts.

        This interface specifies that implementations must do the former, as
        that's what makes things efficient in our most important use case
        (``QuantumGraph`` generation in ``pipe_base``).  We may be able to
        improve this situation in the future by putting exact overlap
        information in the database, either by using built-in (but
        engine-specific) spatial database functionality or (more likely)
        switching to a scheme in which pairwise dimension spatial relationships
        are explicitly precomputed (for e.g. combinations of instruments and
        skymaps).
        """
        raise NotImplementedError()

    @abstractmethod
    def makeBuilder(self, summary: Optional[QuerySummary] = None) -> QueryBuilder:
        """Return a `QueryBuilder` that can be used to construct a new `Query`
        that is joined to (and hence constrained by) this one.

        Parameters
        ----------
        summary : `QuerySummary`, optional
            A `QuerySummary` instance that specifies the dimensions and any
            additional constraints to include in the new query being
            constructed, or `None` to use the dimensions of ``self`` with no
            additional constraints.
        """
        raise NotImplementedError()

    graph: DimensionGraph
    """The dimensions identified by this query and included in any data IDs
    created from its result rows (`DimensionGraph`).
    """

    whereRegion: Optional[Region]
    """A spatial region that all regions in all rows returned by this query
    must overlap (`lsst.sphgeom.Region` or `None`).
    """

    backend: QueryBackend
    """Backend object that represents the `Registry` implementation.
    """


class DirectQueryUniqueness(enum.Enum):
    """An enum representing the ways in which a query can have unique rows (or
    not).
    """

    NOT_UNIQUE = enum.auto()
    """The query is not expected to have unique rows.
    """

    NATURALLY_UNIQUE = enum.auto()
    """The construction of the query guarantees that it will have unique
    result rows, even without SELECT DISTINCT or a GROUP BY clause.
    """

    NEEDS_DISTINCT = enum.auto()
    """The query is expected to yield unique result rows, and needs to use
    SELECT DISTINCT or an equivalent GROUP BY clause to achieve this.
    """


class DirectQuery(Query):
    """A `Query` implementation that represents a direct SELECT query that
    usually joins many tables.

    `DirectQuery` objects should generally only be constructed by
    `QueryBuilder` or the methods of other `Query` objects.

    Parameters
    ----------
    simpleQuery : `SimpleQuery`
        Struct representing the actual SELECT, FROM, and WHERE clauses.
    columns : `QueryColumns`
        Columns that are referenced in the query in any clause.
    uniqueness : `DirectQueryUniqueness`
        Enum value indicating whether the query should yield unique result
        rows, and if so whether that needs to be explicitly requested of the
        database.
    graph : `DimensionGraph`
        Object describing the dimensions included in the query.
    whereRegion : `lsst.sphgeom.Region`, optional
        Region that all region columns in all returned rows must overlap.
    backend : `QueryBackend`
        Backend object that represents the `Registry` implementation.
    doomed_by : `Iterable` [ `str` ], optional
        A list of messages (appropriate for e.g. logging or exceptions) that
        explain why the query is known to return no results even before it is
        executed.  Queries with a non-empty list will never be executed.
    """

    def __init__(
        self,
        *,
        simpleQuery: SimpleQuery,
        columns: QueryColumns,
        uniqueness: DirectQueryUniqueness,
        graph: DimensionGraph,
        whereRegion: Optional[Region],
        backend: QueryBackend,
        order_by_columns: Iterable[OrderByColumn] = (),
        limit: Optional[Tuple[int, Optional[int]]] = None,
        doomed_by: Iterable[str] = (),
    ):
        super().__init__(graph=graph, whereRegion=whereRegion, backend=backend, doomed_by=doomed_by)
        assert not simpleQuery.columns, "Columns should always be set on a copy in .sql"
        assert not columns.isEmpty(), "EmptyQuery must be used when a query would have no columns."
        self._simpleQuery = simpleQuery
        self._columns = columns
        self._uniqueness = uniqueness
        self._order_by_columns = order_by_columns
        self._limit = limit
        self._datasetQueryColumns: Optional[DatasetQueryColumns] = None
        self._dimensionColumns: Dict[str, sqlalchemy.sql.ColumnElement] = {}
        self._regionColumns: Dict[str, sqlalchemy.sql.ColumnElement] = {}

    def isUnique(self) -> bool:
        # Docstring inherited from Query.
        return self._uniqueness is not DirectQueryUniqueness.NOT_UNIQUE

    def getDimensionColumn(self, name: str) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from Query.
        column = self._dimensionColumns.get(name)
        if column is None:
            column = self._columns.getKeyColumn(name).label(name)
            self._dimensionColumns[name] = column
        return column

    @property
    def spatial(self) -> Iterator[DimensionElement]:
        # Docstring inherited from Query.
        return iter(self._columns.regions)

    def getRegionColumn(self, name: str) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from Query.
        column = self._regionColumns.get(name)
        if column is None:
            column = self._columns.regions[name].label(f"{name}_region")
            self._regionColumns[name] = column
        return column

    def getDatasetColumns(self) -> Optional[DatasetQueryColumns]:
        # Docstring inherited from Query.
        if self._datasetQueryColumns is None:
            base = self._columns.datasets
            if base is None:
                return None
            ingestDate = base.ingestDate
            if ingestDate is not None:
                ingestDate = ingestDate.label("ingest_date")
            self._datasetQueryColumns = DatasetQueryColumns(
                datasetType=base.datasetType,
                id=base.id.label("dataset_id"),
                runKey=base.runKey.label(self.backend.managers.collections.getRunForeignKeyName()),
                ingestDate=ingestDate,
            )
        return self._datasetQueryColumns

    @property
    def sql(self) -> sqlalchemy.sql.FromClause:
        # Docstring inherited from Query.
        simpleQuery = self._simpleQuery.copy()
        for dimension in self.graph:
            simpleQuery.columns.append(self.getDimensionColumn(dimension.name))
        for element in self.spatial:
            simpleQuery.columns.append(self.getRegionColumn(element.name))
        datasetColumns = self.getDatasetColumns()
        if datasetColumns is not None:
            simpleQuery.columns.extend(datasetColumns)

        assert not simpleQuery.order_by, "Input query cannot have ORDER BY"
        if self._order_by_columns:
            # add ORDER BY column
            order_by_columns = [column.column_order for column in self._order_by_columns]
            order_by_column = sqlalchemy.func.row_number().over(order_by=order_by_columns).label("_orderby")
            simpleQuery.columns.append(order_by_column)
            simpleQuery.order_by = [order_by_column]

        assert simpleQuery.limit is None, "Input query cannot have LIMIT"
        simpleQuery.limit = self._limit

        sql = simpleQuery.combine()

        if self._uniqueness is DirectQueryUniqueness.NEEDS_DISTINCT:
            return sql.distinct()
        else:
            return sql

    def _makeTableSpec(self, constraints: bool = False) -> ddl.TableSpec:
        """Helper method for subclass implementations of `materialize`.

        Parameters
        ----------
        constraints : `bool`, optional
            If `True` (`False` is default), define a specification that
            includes actual foreign key constraints for logical foreign keys.
            Some database engines do not permit temporary tables to reference
            normal tables, so this should be `False` when generating a spec
            for a temporary table unless the database engine is known to
            support them.

        Returns
        -------
        spec : `ddl.TableSpec`
            Specification for a table that could hold this query's result rows.
        """
        unique = self.isUnique()
        spec = ddl.TableSpec(fields=())
        for dimension in self.graph:
            addDimensionForeignKey(spec, dimension, primaryKey=unique, constraint=constraints)
        for element in self.spatial:
            spec.fields.add(ddl.FieldSpec.for_region(f"{element.name}_region"))
        datasetColumns = self.getDatasetColumns()
        if datasetColumns is not None:
            self.backend.managers.datasets.addDatasetForeignKey(
                spec, primaryKey=unique, constraint=constraints
            )
            self.backend.managers.collections.addRunForeignKey(spec, nullable=False, constraint=constraints)

        # Need a column for ORDER BY if ordering is requested
        if self._order_by_columns:
            spec.fields.add(
                ddl.FieldSpec(
                    name="_orderby",
                    dtype=sqlalchemy.BigInteger,
                    nullable=False,
                    doc="Column to use with ORDER BY",
                )
            )

        return spec

    @contextmanager
    def materialize(self, db: Database) -> Iterator[Query]:
        # Docstring inherited from Query.
        spec = self._makeTableSpec()
        with db.temporary_table(spec) as table:
            if not self._doomed_by:
                db.insert(table, select=self.sql, names=spec.fields.names)
            yield MaterializedQuery(
                table=table,
                spatial=self.spatial,
                datasetType=self.datasetType,
                isUnique=self.isUnique(),
                graph=self.graph,
                whereRegion=self.whereRegion,
                backend=self.backend,
                doomed_by=self._doomed_by,
            )

    def subset(
        self, *, graph: Optional[DimensionGraph] = None, datasets: bool = True, unique: bool = False
    ) -> Query:
        # Docstring inherited from Query.
        graph, columns = self._makeSubsetQueryColumns(graph=graph, datasets=datasets, unique=unique)
        if columns is None:
            return self
        if columns.isEmpty():
            return EmptyQuery(self.graph.universe, self.backend)
        return DirectQuery(
            simpleQuery=self._simpleQuery.copy(),
            columns=columns,
            uniqueness=DirectQueryUniqueness.NEEDS_DISTINCT if unique else DirectQueryUniqueness.NOT_UNIQUE,
            graph=graph,
            whereRegion=self.whereRegion if not unique else None,
            backend=self.backend,
            doomed_by=self._doomed_by,
        )

    def makeBuilder(self, summary: Optional[QuerySummary] = None) -> QueryBuilder:
        # Docstring inherited from Query.
        from ._builder import QueryBuilder

        if summary is None:
            summary = QuerySummary(self.graph, whereRegion=self.whereRegion)
        if not summary.requested.issubset(self.graph):
            raise NotImplementedError(
                f"Query.makeBuilder does not yet support augmenting dimensions "
                f"({summary.requested.dimensions}) beyond those originally included in the query "
                f"({self.graph.dimensions})."
            )
        builder = QueryBuilder(summary, backend=self.backend, doomed_by=self._doomed_by)
        builder.joinTable(
            self.sql.alias(), dimensions=self.graph.dimensions, datasets=self.getDatasetColumns()
        )
        return builder


class MaterializedQuery(Query):
    """A `Query` implementation that represents query results saved in a
    temporary table.

    `MaterializedQuery` instances should not be constructed directly; use
    `Query.materialize()` instead.

    Parameters
    ----------
    table : `sqlalchemy.schema.Table`
        SQLAlchemy object representing the temporary table.
    spatial : `Iterable` [ `DimensionElement` ]
        Spatial dimension elements whose regions must overlap for each valid
        result row (which may reject some rows that are in the table).
    datasetType : `DatasetType`
        The `DatasetType` of datasets returned by this query, or `None`
        if there are no dataset results
    isUnique : `bool`
        If `True`, the table's rows are unique, and there is no need to
        add ``SELECT DISTINCT`` to guarantee this in results.
    graph : `DimensionGraph`
        Dimensions included in the columns of this table.
    whereRegion : `Region` or `None`
        A spatial region all result-row regions must overlap to be valid (which
        may reject some rows that are in the table).
    backend : `QueryBackend`
        Backend object that represents the `Registry` implementation.
    doomed_by : `Iterable` [ `str` ], optional
        A list of messages (appropriate for e.g. logging or exceptions) that
        explain why the query is known to return no results even before it is
        executed.  Queries with a non-empty list will never be executed.
    """

    def __init__(
        self,
        *,
        table: sqlalchemy.schema.Table,
        spatial: Iterable[DimensionElement],
        datasetType: Optional[DatasetType],
        isUnique: bool,
        graph: DimensionGraph,
        whereRegion: Optional[Region],
        backend: QueryBackend,
        doomed_by: Iterable[str] = (),
    ):
        super().__init__(graph=graph, whereRegion=whereRegion, backend=backend, doomed_by=doomed_by)
        self._table = table
        self._spatial = tuple(spatial)
        self._datasetType = datasetType
        self._isUnique = isUnique

    def isUnique(self) -> bool:
        # Docstring inherited from Query.
        return self._isUnique

    def getDimensionColumn(self, name: str) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from Query.
        return self._table.columns[name]

    @property
    def spatial(self) -> Iterator[DimensionElement]:
        # Docstring inherited from Query.
        return iter(self._spatial)

    def getRegionColumn(self, name: str) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from Query.
        return self._table.columns[f"{name}_region"]

    def getDatasetColumns(self) -> Optional[DatasetQueryColumns]:
        # Docstring inherited from Query.
        if self._datasetType is not None:
            return DatasetQueryColumns(
                datasetType=self._datasetType,
                id=self._table.columns["dataset_id"],
                runKey=self._table.columns[self.backend.managers.collections.getRunForeignKeyName()],
                ingestDate=None,
            )
        else:
            return None

    @property
    def sql(self) -> sqlalchemy.sql.FromClause:
        # Docstring inherited from Query.
        select = self._table.select()
        if "_orderby" in self._table.columns:
            select = select.order_by(self._table.columns["_orderby"])
        return select

    @contextmanager
    def materialize(self, db: Database) -> Iterator[Query]:
        # Docstring inherited from Query.
        yield self

    def subset(
        self, *, graph: Optional[DimensionGraph] = None, datasets: bool = True, unique: bool = False
    ) -> Query:
        # Docstring inherited from Query.
        graph, columns = self._makeSubsetQueryColumns(graph=graph, datasets=datasets, unique=unique)
        if columns is None:
            return self
        if columns.isEmpty():
            return EmptyQuery(self.graph.universe, self.backend)
        simpleQuery = SimpleQuery()
        simpleQuery.join(self._table)
        return DirectQuery(
            simpleQuery=simpleQuery,
            columns=columns,
            uniqueness=DirectQueryUniqueness.NEEDS_DISTINCT if unique else DirectQueryUniqueness.NOT_UNIQUE,
            graph=graph,
            whereRegion=self.whereRegion if not unique else None,
            backend=self.backend,
            doomed_by=self._doomed_by,
        )

    def makeBuilder(self, summary: Optional[QuerySummary] = None) -> QueryBuilder:
        # Docstring inherited from Query.
        from ._builder import QueryBuilder

        if summary is None:
            summary = QuerySummary(self.graph, whereRegion=self.whereRegion)
        if not summary.requested.issubset(self.graph):
            raise NotImplementedError(
                f"Query.makeBuilder does not yet support augmenting dimensions "
                f"({summary.requested.dimensions}) beyond those originally included in the query "
                f"({self.graph.dimensions})."
            )
        builder = QueryBuilder(summary, backend=self.backend, doomed_by=self._doomed_by)
        builder.joinTable(self._table, dimensions=self.graph.dimensions, datasets=self.getDatasetColumns())
        return builder


class EmptyQuery(Query):
    """A `Query` implementation that handes the special case where the query
    would have no columns.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Set of all dimensions from which the null set is extracted.
    backend : `QueryBackend`
        Backend object that represents the `Registry` implementation.
    doomed_by : `Iterable` [ `str` ], optional
        A list of messages (appropriate for e.g. logging or exceptions) that
        explain why the query is known to return no results even before it is
        executed.  Queries with a non-empty list will never be executed.
    """

    def __init__(
        self,
        universe: DimensionUniverse,
        backend: QueryBackend,
        doomed_by: Iterable[str] = (),
    ):
        super().__init__(graph=universe.empty, whereRegion=None, backend=backend, doomed_by=doomed_by)

    def isUnique(self) -> bool:
        # Docstring inherited from Query.
        return True

    def getDimensionColumn(self, name: str) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from Query.
        raise KeyError(f"No dimension {name} in query (no dimensions at all, actually).")

    @property
    def spatial(self) -> Iterator[DimensionElement]:
        # Docstring inherited from Query.
        return iter(())

    def getRegionColumn(self, name: str) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from Query.
        raise KeyError(f"No region for {name} in query (no regions at all, actually).")

    def getDatasetColumns(self) -> Optional[DatasetQueryColumns]:
        # Docstring inherited from Query.
        return None

    def rows(self, db: Database) -> Iterator[Optional[sqlalchemy.engine.RowProxy]]:
        if not self._doomed_by:
            yield None

    @property
    def sql(self) -> Optional[sqlalchemy.sql.FromClause]:
        # Docstring inherited from Query.
        return None

    @contextmanager
    def materialize(self, db: Database) -> Iterator[Query]:
        # Docstring inherited from Query.
        yield self

    def subset(
        self, *, graph: Optional[DimensionGraph] = None, datasets: bool = True, unique: bool = False
    ) -> Query:
        # Docstring inherited from Query.
        assert graph is None or graph.issubset(self.graph)
        return self

    def makeBuilder(self, summary: Optional[QuerySummary] = None) -> QueryBuilder:
        # Docstring inherited from Query.
        from ._builder import QueryBuilder

        if summary is None:
            summary = QuerySummary(self.graph)
        if not summary.requested.issubset(self.graph):
            raise NotImplementedError(
                f"Query.makeBuilder does not yet support augmenting dimensions "
                f"({summary.requested.dimensions}) beyond those originally included in the query "
                f"({self.graph.dimensions})."
            )
        return QueryBuilder(summary, backend=self.backend, doomed_by=self._doomed_by)
