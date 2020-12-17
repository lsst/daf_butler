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

from abc import ABC, abstractmethod
from contextlib import contextmanager
import enum
import itertools
from typing import (
    Callable,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Tuple,
    TYPE_CHECKING,
)

import sqlalchemy

from lsst.sphgeom import Region

from ...core import (
    addDimensionForeignKey,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    ddl,
    Dimension,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
    DimensionUniverse,
    SpatialRegionDatabaseRepresentation,
    SimpleQuery,
)
from ..interfaces import Database
from ._structs import DatasetQueryColumns, QueryColumns, QuerySummary, RegistryManagers

if TYPE_CHECKING:
    from ._builder import QueryBuilder


class Query(ABC):
    """An abstract base class for queries that return some combination of
    `DatasetRef` and `DataCoordinate` objects.

    Parameters
    ----------
    graph : `DimensionGraph`
        Object describing the dimensions included in the query.
    whereRegion : `lsst.sphgeom.Region`, optional
        Region that all region columns in all returned rows must overlap.
    managers : `RegistryManagers`
        A struct containing the registry manager instances used by the query
        system.

    Notes
    -----
    The `Query` hierarchy abstracts over the database/SQL representation of a
    particular set of data IDs or datasets.  It is expected to be used as a
    backend for other objects that provide more natural interfaces for one or
    both of these, not as part of a public interface to query results.
    """
    def __init__(self, *,
                 graph: DimensionGraph,
                 whereRegion: Optional[Region],
                 managers: RegistryManagers,
                 ):
        self.graph = graph
        self.whereRegion = whereRegion
        self.managers = managers

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

    def predicate(self, region: Optional[Region] = None) -> Callable[[sqlalchemy.engine.RowProxy], bool]:
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
            predicate to return `True`.  If not provided, this will be
            ``self.whereRegion``, if that exists.

        Returns
        -------
        func : `Callable`
            A callable that takes a single `sqlalchemy.engine.RowProxy`
            argmument and returns `bool`.
        """
        whereRegion = region if region is not None else self.whereRegion

        def closure(row: sqlalchemy.engine.RowProxy) -> bool:
            rowRegions = [row[self.getRegionColumn(element.name)] for element in self.spatial]
            if whereRegion and any(r.isDisjointFrom(whereRegion) for r in rowRegions):
                return False
            return not any(a.isDisjointFrom(b) for a, b in itertools.combinations(rowRegions, 2))

        return closure

    def rows(self, db: Database, *, region: Optional[Region] = None
             ) -> Iterator[Optional[sqlalchemy.engine.RowProxy]]:
        """Execute the query and yield result rows, applying `predicate`.

        Parameters
        ----------
        region : `sphgeom.Region`, optional
            A region that any result-row regions must overlap in order to be
            yielded.  If not provided, this will be ``self.whereRegion``, if
            that exists.

        Yields
        ------
        row : `sqlalchemy.engine.RowProxy` or `None`
            Result row from the query.  `None` may yielded exactly once instead
            of any real rows to indicate an empty query (see `EmptyQuery`).
        """
        predicate = self.predicate(region)
        for row in db.query(self.sql):
            if predicate(row):
                yield row

    def extractDimensionsTuple(self, row: Optional[sqlalchemy.engine.RowProxy],
                               dimensions: Iterable[Dimension]) -> tuple:
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
        return tuple(row[self.getDimensionColumn(dimension.name)] for dimension in dimensions)

    def extractDataId(self, row: Optional[sqlalchemy.engine.RowProxy], *,
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
            graph,
            self.extractDimensionsTuple(row, itertools.chain(graph.required, graph.implied))
        )
        if records is not None:
            recordsForRow = {}
            for element in graph.elements:
                key = tuple(dataId.subset(element.graph).values())
                recordsForRow[element.name] = records[element.name].get(key)
            return dataId.expanded(recordsForRow)
        else:
            return dataId

    def extractDatasetRef(self, row: sqlalchemy.engine.RowProxy,
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
        runRecord = self.managers.collections[row[datasetColumns.runKey]]
        return DatasetRef(datasetColumns.datasetType, dataId, id=row[datasetColumns.id], run=runRecord.name)

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
            spec.fields.update(
                SpatialRegionDatabaseRepresentation.makeFieldSpecs(
                    nullable=True,
                    name=f"{element.name}_region",
                )
            )
        datasetColumns = self.getDatasetColumns()
        if datasetColumns is not None:
            self.managers.datasets.addDatasetForeignKey(spec, primaryKey=unique, constraint=constraints)
            self.managers.collections.addRunForeignKey(spec, nullable=False, constraint=constraints)
        return spec

    def _makeSubsetQueryColumns(self, *, graph: Optional[DimensionGraph] = None,
                                datasets: bool = True,
                                unique: bool = False) -> Tuple[DimensionGraph, Optional[QueryColumns]]:
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
            new query, contructed by delegating to other (mostly abstract)
            methods on ``self``.  If `None`, `subset` may return ``self``.
        """
        if graph is None:
            graph = self.graph
        if (graph == self.graph and (self.getDatasetColumns() is None or datasets)
                and (self.isUnique() or not unique)):
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

    @contextmanager
    def materialize(self, db: Database) -> Iterator[Query]:
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
        spec = self._makeTableSpec()
        table = db.makeTemporaryTable(spec)
        db.insert(table, select=self.sql, names=spec.fields.names)
        yield MaterializedQuery(table=table,
                                spatial=self.spatial,
                                datasetType=self.datasetType,
                                isUnique=self.isUnique(),
                                graph=self.graph,
                                whereRegion=self.whereRegion,
                                managers=self.managers)
        db.dropTemporaryTable(table)

    @abstractmethod
    def subset(self, *, graph: Optional[DimensionGraph] = None,
               datasets: bool = True,
               unique: bool = False) -> Query:
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

    managers: RegistryManagers
    """A struct containing `Registry` helper object (`RegistryManagers`).
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
    managers : `RegistryManagers`
        Struct containing the `Registry` manager helper objects, to be
        forwarded to the `Query` constructor.
    """
    def __init__(self, *,
                 simpleQuery: SimpleQuery,
                 columns: QueryColumns,
                 uniqueness: DirectQueryUniqueness,
                 graph: DimensionGraph,
                 whereRegion: Optional[Region],
                 managers: RegistryManagers):
        super().__init__(graph=graph, whereRegion=whereRegion, managers=managers)
        assert not simpleQuery.columns, "Columns should always be set on a copy in .sql"
        assert not columns.isEmpty(), "EmptyQuery must be used when a query would have no columns."
        self._simpleQuery = simpleQuery
        self._columns = columns
        self._uniqueness = uniqueness

    def isUnique(self) -> bool:
        # Docstring inherited from Query.
        return self._uniqueness is not DirectQueryUniqueness.NOT_UNIQUE

    def getDimensionColumn(self, name: str) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from Query.
        return self._columns.getKeyColumn(name).label(name)

    @property
    def spatial(self) -> Iterator[DimensionElement]:
        # Docstring inherited from Query.
        return iter(self._columns.regions)

    def getRegionColumn(self, name: str) -> sqlalchemy.sql.ColumnElement:
        # Docstring inherited from Query.
        return self._columns.regions[name].column.label(f"{name}_region")

    def getDatasetColumns(self) -> Optional[DatasetQueryColumns]:
        # Docstring inherited from Query.
        base = self._columns.datasets
        if base is None:
            return None
        ingestDate = base.ingestDate
        if ingestDate is not None:
            ingestDate = ingestDate.label("ingest_date")
        return DatasetQueryColumns(
            datasetType=base.datasetType,
            id=base.id.label("dataset_id"),
            runKey=base.runKey.label(self.managers.collections.getRunForeignKeyName()),
            ingestDate=ingestDate,
        )

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
        sql = simpleQuery.combine()
        if self._uniqueness is DirectQueryUniqueness.NEEDS_DISTINCT:
            return sql.distinct()
        else:
            return sql

    def subset(self, *, graph: Optional[DimensionGraph] = None,
               datasets: bool = True,
               unique: bool = False) -> Query:
        # Docstring inherited from Query.
        graph, columns = self._makeSubsetQueryColumns(graph=graph, datasets=datasets, unique=unique)
        if columns is None:
            return self
        if columns.isEmpty():
            return EmptyQuery(self.graph.universe, self.managers)
        return DirectQuery(
            simpleQuery=self._simpleQuery.copy(),
            columns=columns,
            uniqueness=DirectQueryUniqueness.NEEDS_DISTINCT if unique else DirectQueryUniqueness.NOT_UNIQUE,
            graph=graph,
            whereRegion=self.whereRegion if not unique else None,
            managers=self.managers,
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
        builder = QueryBuilder(summary, managers=self.managers)
        builder.joinTable(self.sql.alias(), dimensions=self.graph.dimensions,
                          datasets=self.getDatasetColumns())
        return builder


class MaterializedQuery(Query):
    """A `Query` implementation that represents query results saved in a
    temporary table.

    `MaterializedQuery` instances should not be constructed directly; use
    `Query.materialize()` instead.

    Parameters
    ----------
    table : `sqlalchemy.schema.Table`
        SQLAlchemy object represnting the temporary table.
    spatial : `Iterable` [ `DimensionElement` ]
        Spatial dimension elements whose regions must overlap for each valid
        result row (which may reject some rows that are in the table).
    datasetType : `DatasetType`
        The `DatasetType` of datasets returned by this query, or `None`
        if there are no dataset results
    isUnique : `bool`
        If `True`, the table's rows are unique, and there is no need to
        add ``SELECT DISTINCT`` to gaurantee this in results.
    graph : `DimensionGraph`
        Dimensions included in the columns of this table.
    whereRegion : `Region` or `None`
        A spatial region all result-row regions must overlap to be valid (which
        may reject some rows that are in the table).
    managers : `RegistryManagers`
        A struct containing `Registry` manager helper objects, forwarded to
        the `Query` constructor.
    """
    def __init__(self, *,
                 table: sqlalchemy.schema.Table,
                 spatial: Iterable[DimensionElement],
                 datasetType: Optional[DatasetType],
                 isUnique: bool,
                 graph: DimensionGraph,
                 whereRegion: Optional[Region],
                 managers: RegistryManagers):
        super().__init__(graph=graph, whereRegion=whereRegion, managers=managers)
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
                runKey=self._table.columns[self.managers.collections.getRunForeignKeyName()],
                ingestDate=None,
            )
        else:
            return None

    @property
    def sql(self) -> sqlalchemy.sql.FromClause:
        # Docstring inherited from Query.
        return self._table.select()

    @contextmanager
    def materialize(self, db: Database) -> Iterator[Query]:
        # Docstring inherited from Query.
        yield self

    def subset(self, *, graph: Optional[DimensionGraph] = None,
               datasets: bool = True,
               unique: bool = False) -> Query:
        # Docstring inherited from Query.
        graph, columns = self._makeSubsetQueryColumns(graph=graph, datasets=datasets, unique=unique)
        if columns is None:
            return self
        if columns.isEmpty():
            return EmptyQuery(self.graph.universe, managers=self.managers)
        simpleQuery = SimpleQuery()
        simpleQuery.join(self._table)
        return DirectQuery(
            simpleQuery=simpleQuery,
            columns=columns,
            uniqueness=DirectQueryUniqueness.NEEDS_DISTINCT if unique else DirectQueryUniqueness.NOT_UNIQUE,
            graph=graph,
            whereRegion=self.whereRegion if not unique else None,
            managers=self.managers,
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
        builder = QueryBuilder(summary, managers=self.managers)
        builder.joinTable(self._table, dimensions=self.graph.dimensions, datasets=self.getDatasetColumns())
        return builder


class EmptyQuery(Query):
    """A `Query` implementation that handes the special case where the query
    would have no columns.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Set of all dimensions from which the null set is extracted.
    managers : `RegistryManagers`
        A struct containing the registry manager instances used by the query
        system.
    """
    def __init__(self, universe: DimensionUniverse, managers: RegistryManagers):
        super().__init__(graph=universe.empty, whereRegion=None, managers=managers)

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

    def rows(self, db: Database, *, region: Optional[Region] = None
             ) -> Iterator[Optional[sqlalchemy.engine.RowProxy]]:
        yield None

    @property
    def sql(self) -> Optional[sqlalchemy.sql.FromClause]:
        # Docstring inherited from Query.
        return None

    @contextmanager
    def materialize(self, db: Database) -> Iterator[Query]:
        # Docstring inherited from Query.
        yield self

    def subset(self, *, graph: Optional[DimensionGraph] = None,
               datasets: bool = True,
               unique: bool = False) -> Query:
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
        return QueryBuilder(summary, managers=self.managers)
