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
    "DataCoordinateQueryResults",
    "DatasetQueryResults",
    "DimensionRecordQueryResults",
    "ParentDatasetQueryResults",
)

from abc import abstractmethod
from contextlib import contextmanager, ExitStack
import itertools
from typing import (
    Any,
    Callable,
    ContextManager,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    Union,
)

import sqlalchemy

from ...core import (
    DataCoordinate,
    DataCoordinateCommonState,
    DataCoordinateIterable,
    DataCoordinateIteratorAdapter,
    DatasetRef,
    DatasetType,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
    HomogeneousDimensionRecordIterable,
    SimpleQuery,
    TimespanDatabaseRepresentation,
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

    @property
    def _common_state(self) -> DataCoordinateCommonState:
        # Docstring inherited.
        return DataCoordinateCommonState(self.graph, hasFull=self.hasFull(), hasRecords=self.hasRecords())

    def hasFull(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return True

    def hasRecords(self) -> bool:
        # Docstring inherited from DataCoordinateIterable.
        return self._records is not None or not self._query.graph

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
            some cases), use `toSet` to construct a `DataCoordinateAbstractSet`
            from this results object instead.

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
        sql = self._query.sql
        if sql is not None:
            fromClause = sql.alias("c")
            query.join(
                fromClause,
                onclause=sqlalchemy.sql.and_(*[
                    columns(dimension.name) == fromClause.columns[dimension.name]
                    for dimension in self.graph.required
                ])
            )

    def findDatasets(self, datasetType: Union[DatasetType, str], collections: Any, *,
                     findFirst: bool = True) -> ParentDatasetQueryResults:
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
        """
        if not isinstance(datasetType, DatasetType):
            datasetType = self._query.managers.datasets[datasetType].datasetType
        # moving component handling down into managers.
        if not datasetType.dimensions.issubset(self.graph):
            raise ValueError(f"findDatasets requires that the dataset type have the same dimensions as "
                             f"the DataCoordinateQueryResult used as input to the search, but "
                             f"{datasetType.name} has dimensions {datasetType.dimensions}, while the input "
                             f"dimensions are {self.graph}.")
        builder = self._query.makeBuilder()
        if datasetType.isComponent():
            # We were given a true DatasetType instance, but it's a component.
            parentName, componentName = datasetType.nameAndComponent()
            storage = self._query.managers.datasets[parentName]
            datasetType = storage.datasetType
            components = [componentName]
        else:
            components = [None]
        if not builder.joinDataset(datasetType, collections=collections, findFirst=findFirst):
            raise RuntimeError(
                f"Error finding datasets of type {datasetType.name} in collections {collections}; "
                "it is impossible for any such datasets to be found in any of those collections, "
                "most likely because the dataset type is not registered.  "
                "This error may become a successful query that returns no results in the future, "
                "because queries with no results are not usually considered an error."
            )
        query = builder.finish(joinMissing=False)
        return ParentDatasetQueryResults(db=self._db, query=query, components=components,
                                         records=self._records)


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
    def materialize(self) -> ContextManager[DatasetQueryResults]:
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
    """
    def __init__(self, db: Database, query: Query, *,
                 components: Sequence[Optional[str]],
                 records: Optional[Mapping[str, Mapping[tuple, DimensionRecord]]] = None):
        self._db = db
        self._query = query
        self._components = components
        self._records = records
        assert query.datasetType is not None, \
            "Query used to initialize dataset results must have a dataset."
        assert query.datasetType.dimensions == query.graph

    __slots__ = ("_db", "_query", "_dimensions", "_components", "_records")

    def __iter__(self) -> Iterator[DatasetRef]:
        for row in self._query.rows(self._db):
            parentRef = self._query.extractDatasetRef(row, records=self._records)
            for component in self._components:
                if component is None:
                    yield parentRef
                else:
                    yield parentRef.makeComponentRef(component)

    def byParentDatasetType(self) -> Iterator[ParentDatasetQueryResults]:
        # Docstring inherited from DatasetQueryResults.
        yield self

    @contextmanager
    def materialize(self) -> Iterator[ParentDatasetQueryResults]:
        # Docstring inherited from DatasetQueryResults.
        with self._query.materialize(self._db) as materialized:
            yield ParentDatasetQueryResults(self._db, materialized,
                                            components=self._components,
                                            records=self._records)

    @property
    def parentDatasetType(self) -> DatasetType:
        """The parent dataset type for all datasets in this iterable
        (`DatasetType`).
        """
        assert self._query.datasetType is not None
        return self._query.datasetType

    @property
    def dataIds(self) -> DataCoordinateQueryResults:
        """A lazy-evaluation object representing a query for just the data
        IDs of the datasets that would be returned by this query
        (`DataCoordinateQueryResults`).

        The returned object is not in general `zip`-iterable with ``self``;
        it may be in a different order or have (or not have) duplicates.
        """
        return DataCoordinateQueryResults(
            self._db,
            self._query.subset(graph=self.parentDatasetType.dimensions, datasets=False, unique=False),
            records=self._records,
        )

    def withComponents(self, components: Sequence[Optional[str]]) -> ParentDatasetQueryResults:
        """Return a new query results object for the same parent datasets but
        different components.

        components :  `Sequence` [ `str` or `None` ]
            Names of components to include in iteration.  `None` may be
            included (at most once) to include the parent dataset type.
        """
        return ParentDatasetQueryResults(self._db, self._query, records=self._records,
                                         components=components)

    def expanded(self) -> ParentDatasetQueryResults:
        # Docstring inherited from DatasetQueryResults.
        if self._records is None:
            records = self.dataIds.expanded()._records
            return ParentDatasetQueryResults(self._db, self._query, records=records,
                                             components=self._components)
        else:
            return self


class ChainedDatasetQueryResults(DatasetQueryResults):
    """A `DatasetQueryResults` implementation that simply chains together
    other results objects, each for a different parent dataset type.

    Parameters
    ----------
    chain : `Sequence` [ `ParentDatasetQueryResults` ]
        The underlying results objects this object will chain together.
    """

    def __init__(self, chain: Sequence[ParentDatasetQueryResults]):
        self._chain = chain

    __slots__ = ("_chain",)

    def __iter__(self) -> Iterator[DatasetRef]:
        return itertools.chain.from_iterable(self._chain)

    def byParentDatasetType(self) -> Iterator[ParentDatasetQueryResults]:
        # Docstring inherited from DatasetQueryResults.
        return iter(self._chain)

    @contextmanager
    def materialize(self) -> Iterator[ChainedDatasetQueryResults]:
        # Docstring inherited from DatasetQueryResults.
        with ExitStack() as stack:
            yield ChainedDatasetQueryResults(
                [stack.enter_context(r.materialize()) for r in self._chain]
            )

    def expanded(self) -> ChainedDatasetQueryResults:
        # Docstring inherited from DatasetQueryResults.
        return ChainedDatasetQueryResults([r.expanded() for r in self._chain])


class DimensionRecordQueryResults(HomogeneousDimensionRecordIterable):
    """A `HomogeneousDimensionRecordIterable` that queries the database each
    time it is iterated over.

    Parameters
    ----------
    db : `Database`
        Interface to the database engine and namespace to query.
    sql : `sqlalchemy.sql.Select`
        Query to execute.
    definition : `DimensionElement`
        The element whose records will be queried.
    data_ids : `DataCoordinateIterable`, optional
        Data IDs that constrain the query.
    """

    def __init__(
        self,
        db: Database,
        sql: sqlalchemy.sql.Select,
        definition: DimensionElement,
        data_ids: Optional[DataCoordinateIterable],
    ):
        self._db = db
        self._sql = sql
        self._definition = definition
        self._data_ids = data_ids

    __slots__ = ("_db", "_sql", "_definition", "_data_ids")

    def __iter__(self) -> Iterator[DimensionRecord]:
        RecordClass = self._definition.RecordClass
        TimespanReprClass = self._db.getTimespanRepresentation()
        for row in self._db.query(self._sql):
            values = dict(row)
            if self._definition.temporal is not None:
                values[TimespanDatabaseRepresentation.NAME] = TimespanReprClass.extract(values)
            yield RecordClass(**values)

    @property
    def definition(self) -> DimensionElement:
        # Docstring inherited.
        return self._definition

    @property
    def data_ids(self) -> DataCoordinateIterable:
        # Docstring inherited.
        if self._data_ids is None:

            def gen() -> Iterator[DataCoordinate]:
                for record in self:
                    yield record.dataId

            return DataCoordinateIteratorAdapter(gen, self._definition.graph, hasFull=False,
                                                 hasRecords=False)
        else:
            return self._data_ids
