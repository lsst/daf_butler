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
    "setupDimensionStorage",
    "DimensionRecordStorage",
    "DatabaseDimensionRecordStorage",
    "CachingDimensionRecordStorage",
    "SkyPixDimensionRecordStorage",
    "ChainedDimensionRecordStorage",
)

from abc import ABC, abstractmethod
from typing import Optional, Dict, Iterable

from sqlalchemy.sql import FromClause, select, and_, union_all
from sqlalchemy.engine import Connection

from ..utils import NamedKeyDict
from .schema import OVERLAP_TABLE_NAME_PATTERN
from .elements import DimensionElement, SkyPixDimension
from .universe import DimensionUniverse
from .coordinate import DataCoordinate, DataId
from .records import DimensionRecord


class DimensionRecordStorage(ABC):
    """An abstract base class that represents a way of storing the records
    associated with a single `DimensionElement`.

    Concrete `DimensionRecordStorage` instances should generally be constructed
    via a call to `setupDimensionStorage`, which selects the appropriate
    subclass for each element according to its configuration.

    All `DimensionRecordStorage` methods are pure abstract, even though in some
    cases a reasonable default implementation might be possible, in order to
    better guarantee all methods are correctly overridden.  All of these
    potentially-defaultable implementations are extremely trivial, so asking
    subclasses to provide them is not a significant burden.
    """

    @property
    @abstractmethod
    def element(self) -> DimensionElement:
        """The element whose records this instance holds (`DimensionElement`).
        """
        raise NotImplementedError()

    @abstractmethod
    def clearCaches(self):
        """Clear any in-memory caches held by the storage instance.

        This is called by `Registry` when transactions are rolled back, to
        avoid in-memory caches from ever containing records that are not
        present in persistent storage.
        """
        raise NotImplementedError()

    @abstractmethod
    def matches(self, dataId: Optional[DataId] = None) -> bool:
        """Test whether this storage could hold any records consistent with the
        given data ID.

        Parameters
        ----------
        dataId : `DataId`, optional
            The data ID to test.  May be an informal data ID dictionary or
            a validated `DataCoordinate`.

        Returns
        -------
        matches : `bool`
            `True` if this storage might hold a record whose data ID matches
            the given on; this is not a guarantee that any such record exists.
            `False` only if a matching record definitely does not exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def getElementTable(self, dataId: Optional[DataId] = None) -> FromClause:
        """Return the logical table for the element as a SQLAlchemy object.

        The returned object may be a select statement or view instead of a
        true table.

        The caller is responsible for checking that the element actually has
        a table (via `DimensionElement.hasTable`).  The exception raised when
        this is not true is unspecified.

        Parameters
        ----------
        dataId : `DataId`, optional
            A data ID that restricts any query that includes the returned
            logical table, which may be used by implementations to return
            a simpler object in some contexts (for example, if the records
            are split across multiple tables that in general must be combined
            via a UNION query).  Implementations are *not* required to
            apply a filter based on this ID, and should only do so when it
            allows them to simplify what is returned.

        Returns
        -------
        table : `sqlalchemy.sql.FromClause`
            A table or table-like SQLAlchemy expression object that can be
            included in a select query.
        """
        raise NotImplementedError()

    @abstractmethod
    def getCommonSkyPixOverlapTable(self, dataId: Optional[DataId] = None) -> FromClause:
        """Return the logical table that relates the given element to the
        common skypix dimension.

        The returned object may be a select statement or view instead of a
        true table.

        The caller is responsible for checking that the element actually has
        a skypix overlap table, which is the case when
        `DimensionElement.hasTable` and `DimensionElement.spatial` are both
        `True`.

        Parameters
        ----------
        dataId : `DataId`, optional
            A data ID that restricts any query that includes the returned
            logical table, which may be used by implementations to return
            a simpler object in some contexts (for example, if the records
            are split across multiple tables that in general must be combined
            via a UNION query).  Implementations are *not* required to
            apply a filter based on this ID, and should only do so when it
            allows them to simplify what is returned.

        Returns
        -------
        table : `sqlalchemy.sql.FromClause`
            A table or table-like SQLAlchemy expression object that can be
            included in a select query.
        """
        raise NotImplementedError()

    @abstractmethod
    def insert(self, *records: DimensionRecord):
        """Insert one or more records into storage.

        Parameters
        ----------
        records
            One or more instances of the `DimensionRecord` subclass for the
            element this storage is associated with.

        Raises
        ------
        TypeError
            Raised if the element does not support record insertion.
        sqlalchemy.exc.IntegrityError
            Raised if one or more records violate database integrity
            constraints.

        Notes
        -----
        As `insert` is expected to be called only by a `Registry`, we rely
        on `Registry` to provide transactionality, both by using a SQLALchemy
        connection shared with the `Registry` and by relying on it to call
        `clearCaches` when rolling back transactions.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch(self, dataId: DataId) -> Optional[DimensionRecord]:
        """Retrieve a record from storage.

        Parameters
        ----------
        dataId : `DataId`
            A data ID that identifies the record to be retrieved.  This may
            be an informal data ID dict or a validated `DataCoordinate`.

        Returns
        -------
        record : `DimensionRecord` or `None`
            A record retrieved from storage, or `None` if there is no such
            record.
        """
        raise NotImplementedError()


class DatabaseDimensionRecordStorage(DimensionRecordStorage):
    """A record storage implementation that uses a SQL database by sharing
    a SQLAlchemy connection with a `Registry`.

    Parameters
    ----------
    connection : `sqlalchemy.engine.Connection`
        The SQLAlchemy connection to use for inserts and fetches.
    element : `DimensionElement`
        The element whose records this storage will manage.
    elementTable : `sqlalchemy.sql.FromClause`
        The logical table for the element.  May be a select query instead
        of a true table.
    commonSkyPixOvelapTable : `sqlalchemy.sql.FromClause`, optional
        The logical table for the overlap table with the dimension universe's
        common skypix dimension.
    """

    def __init__(self, connection: Connection, element: DimensionElement, *,
                 elementTable: FromClause,
                 commonSkyPixOverlapTable: Optional[FromClause] = None):
        self._connection = connection
        self._element = element
        self._elementTable = elementTable
        self._commonSkyPixOverlapTable = commonSkyPixOverlapTable
        if element.hasTable() and element.spatial and commonSkyPixOverlapTable is None:
            raise TypeError(f"No common skypix table provided for element {element.name}.")

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._element

    def clearCaches(self):
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def matches(self, dataId: Optional[DataId] = None) -> bool:
        # Docstring inherited from DimensionRecordStorage.matches.
        return True

    def getElementTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from DimensionRecordStorage.getElementTable.
        return self._elementTable

    def getCommonSkyPixOverlapTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from
        # DimensionRecordStorage.getCommonSkyPixOverlapTable.
        return self._commonSkyPixOverlapTable

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        if self.element.viewOf is not None:
            raise TypeError(f"Cannot insert {self.element.name} records because its "
                            f"logical table is a view into {self.element.viewOf}.")
        # Build lists of dicts to insert first, before any database operations,
        # to minimize the time spent in the transaction.
        elementRows = []
        if self.element.spatial:
            commonSkyPixRows = []
            commonSkyPix = self.element.universe.commonSkyPix
        for record in records:
            elementRows.append(record.toDict())
            if self.element.spatial:
                if record.region is None:
                    # TODO: should we warn about this case?
                    continue
                base = record.dataId.byName()
                for begin, end in commonSkyPix.pixelization.envelope(record.region):
                    for skypix in range(begin, end):
                        row = base.copy()
                        row[commonSkyPix.name] = skypix
                        commonSkyPixRows.append(row)
        # TODO: wrap the operations below in a transaction.
        self._connection.execute(self._elementTable.insert(), *elementRows)
        if self.element.spatial and commonSkyPixRows:
            self._connection.execute(self._commonSkyPixOverlapTable.insert(), *commonSkyPixRows)

    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        RecordClass = self.element.RecordClass
        # I don't know how expensive it is to construct the query below, and
        # hence how much gain there might be to caching it, so I'm going to
        # wait for it to appear as a hotspot in a profile before trying that.
        nRequired = len(self.element.graph.required)
        if self.element.viewOf is not None:
            whereColumns = [self._elementTable.columns[dimension.name]
                            for dimension in self.element.graph.required]
        else:
            whereColumns = [self._elementTable.columns[fieldName]
                            for fieldName in RecordClass.__slots__[:nRequired]]
        selectColumns = whereColumns + [self._elementTable.columns[name]
                                        for name in RecordClass.__slots__[nRequired:]]
        query = select(
            selectColumns
        ).select_from(self._elementTable).where(
            and_(*[column == dataId[dimension.name]
                   for column, dimension in zip(whereColumns, self.element.graph.required)])
        )
        row = self._connection.execute(query).fetchone()
        if row is None:
            return None
        return RecordClass(*row)


class CachingDimensionRecordStorage(DimensionRecordStorage):
    """A record storage implementation that adds caching to some other nested
    storage implementation.

    Parameters
    ----------
    nested : `DimensionRecordStorage`
        The other storage to cache fetches from and to delegate all other
        operations to.
    """

    def __init__(self, nested: DimensionRecordStorage):
        self._nested = nested
        self._cache = {}

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._nested.element

    def clearCaches(self):
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        self._cache.clear()
        self._nested.clearCaches()

    def matches(self, dataId: Optional[DataId]) -> bool:
        # Docstring inherited from DimensionRecordStorage.matches.
        return self._nested.matches(dataId)

    def getElementTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from DimensionRecordStorage.getElementTable.
        return self._nested.getElementTable(dataId)

    def getCommonSkyPixOverlapTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from
        # DimensionRecordStorage.getCommonSkyPixOverlapTable.
        return self._nested.getCommonSkyPixOverlapTable(dataId)

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        self._nested.insert(*records)
        for record in records:
            self._cache[record.dataId] = record

    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        dataId = DataCoordinate.standardize(dataId, graph=self.element.graph)
        record = self._cache.get(dataId)
        if record is None:
            record = self._nested.fetch(dataId)
            self._cache[dataId] = record
        return record


class SkyPixDimensionRecordStorage(DimensionRecordStorage):
    """A storage implementation specialized for `SkyPixDimension` records.

    `SkyPixDimension` records are never stored in a database, but are instead
    generated-on-the-fly from a `sphgeom.Pixelization` instance.

    Parameters
    ----------
    dimension : `SkyPixDimension`
        The dimension for which this instance will simulate storage.
    """

    def __init__(self, dimension: SkyPixDimension):
        self._dimension = dimension

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._dimension

    def clearCaches(self):
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def matches(self, dataId: Optional[DataId] = None) -> bool:
        # Docstring inherited from DimensionRecordStorage.matches.
        return True

    def getElementTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from DimensionRecordStorage.getElementTable.
        raise TypeError(f"SkyPix dimension {self._dimension.name} has no database representation.")

    def getCommonSkyPixOverlapTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from
        # DimensionRecordStorage.getCommonSkyPixOverlapTable.
        raise TypeError(f"SkyPix dimension {self._dimension.name} has no database representation.")

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        raise TypeError(f"Cannot insert into SkyPix dimension {self._dimension.name}.")

    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        return self._dimension.RecordClass(dataId[self._dimension.name],
                                           self._dimension.pixelization.pixel(dataId[self._dimension.name]))


def setupDimensionStorage(connection: Connection,
                          universe: DimensionUniverse,
                          tables: Dict[str, FromClause]
                          ) -> NamedKeyDict[DimensionElement, DimensionRecordStorage]:
    """Construct a suite of `DimensionRecordStorage` instances for all elements
    in a `DimensionUniverse`.

    Parameters
    ----------
    connection : `sqlalchemy.engine.Connection`
        A SQLAlchemy connection object, typically shared with the `Registry`
        that will own the storage instances.
    universe : `DimensionUniverse`
        The set of all dimensions for which storage instances should be
        constructed.
    tables : `dict`
        A dictionary whose keys are a superset of the keys of the dictionary
        returned by `DimensionUniverse.makeSchemaSpec`, and whose values are
        SQLAlchemy objects that represent tables or select queries.

    Returns
    -------
    storages : `NamedKeyDict`
        A dictionary mapping `DimensionElement` instances to the storage
        instances that manage their records.
    """
    result = NamedKeyDict()
    for element in universe.elements:
        if element.hasTable():
            if element.viewOf is not None:
                elementTable = tables[element.viewOf]
            else:
                elementTable = tables[element.name]
            if element.spatial:
                commonSkyPixOverlapTable = \
                    tables[OVERLAP_TABLE_NAME_PATTERN.format(element.name, universe.commonSkyPix.name)]
            else:
                commonSkyPixOverlapTable = None
            storage = DatabaseDimensionRecordStorage(connection, element, elementTable=elementTable,
                                                     commonSkyPixOverlapTable=commonSkyPixOverlapTable)
        elif isinstance(element, SkyPixDimension):
            storage = SkyPixDimensionRecordStorage(element)
        else:
            storage = None
        if element.cached:
            if storage is None:
                raise RuntimeError(f"Element {element.name} is marked as cached but has no table.")
            storage = CachingDimensionRecordStorage(storage)
        result[element] = storage
    return result


class ChainedDimensionRecordStorage(DimensionRecordStorage):
    """A storage implementation that delegates to multiple other nested storage
    implementations.

    `ChainedDimensionRecordStorage` is targeted at a future `Registry`
    implementation that combines multiple database schemas (in the namespace
    sense) with possibly different access restrictions and sharing.  It is not
    currently used by existing `Registry` implementations, but has been
    prototyped early to ensure the `DimensionRecordStorage` interface is
    sufficient for that role (including nesting within such a chain).
    """

    def __init__(self, chain: Iterable[DimensionRecordStorage]):
        self._chain = list(chain)

    @property
    def element(self) -> DimensionElement:
        # Docstring inherited from DimensionRecordStorage.element.
        return self._chain[0].element

    def clearCaches(self):
        # Docstring inherited from DimensionRecordStorage.clearCaches.
        pass

    def matches(self, dataId: Optional[DataId]) -> bool:
        # Docstring inherited from DimensionRecordStorage.matches.
        return any(link.matches(dataId) for link in self._chain)

    def getElementTable(self, *, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from DimensionRecordStorage.getElementTable.
        tables = [link.getElementTable(dataId) for link in self._chain if link.matches(dataId)]
        if len(tables) == 0:
            raise RuntimeError(f"No matching table for {self.element.name}, {dataId}.")
        elif len(tables) == 1:
            return tables[0]
        else:
            return union_all(*tables)

    def getCommonSkyPixOverlapTable(self, dataId: Optional[DataId] = None) -> FromClause:
        # Docstring inherited from
        # DimensionRecordStorage.getCommonSkyPixOverlapTable.
        tables = [link.getCommonSkyPixOverlapTable(dataId) for link in self._chain if link.matches(dataId)]
        if len(tables) == 0:
            raise RuntimeError(f"No matching skypix overlap table for {self.element.name}, {dataId}.")
        elif len(tables) == 1:
            return tables[0]
        else:
            return union_all(*tables)

    def insert(self, *records: DimensionRecord):
        # Docstring inherited from DimensionRecordStorage.insert.
        # Right now the only use case for ChainedDimensionDatabase involves
        # read-write per-user schemas sitting on top of read-only shared
        # schemas, and in that case there's no point to try to write to
        # anything but the first link in the chain (the per-user schema).
        return self._chain[0].insert(*records)

    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        # Docstring inherited from DimensionRecordStorage.fetch.
        for link in self._chain:
            if link.matches(dataId):
                result = link.fetch(dataId)
                if result is not None:
                    return result
        return None
