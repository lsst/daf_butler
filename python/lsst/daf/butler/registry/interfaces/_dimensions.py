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

__all__ = ["DimensionRecordStorage", "DimensionRecordStorageManager"]

from abc import ABC, abstractmethod
from typing import Iterable, Optional, Type, TYPE_CHECKING

import sqlalchemy

from ...core import SkyPixDimension

if TYPE_CHECKING:
    from ...core import (
        DataCoordinateIterable,
        DimensionElement,
        DimensionRecord,
        DimensionUniverse,
        NamedKeyDict,
        Timespan,
    )
    from ..queries import QueryBuilder
    from ._database import Database, StaticTablesContext


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

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, element: DimensionElement, *,
                   context: Optional[StaticTablesContext] = None) -> DimensionRecordStorage:
        """Construct an instance of this class using a standardized interface.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        element : `DimensionElement`
            Dimension element the new instance will manage records for.
        context : `StaticTablesContext`, optional
            If provided, an object to use to create any new tables.  If not
            provided, ``db.ensureTableExists`` should be used instead.

        Returns
        -------
        storage : `DimensionRecordStorage`
            A new `DimensionRecordStorage` subclass instance.
        """
        raise NotImplementedError()

    @staticmethod
    def getDefaultImplementation(element: DimensionElement, ignoreCached: bool = False
                                 ) -> Type[DimensionRecordStorage]:
        """Return the default `DimensionRecordStorage` implementation for the
        given `DimensionElement`.

        Parameters
        ----------
        element : `DimensionElement`
            The element whose properties should be examined to determine the
            appropriate default implementation class.
        ignoreCached : `bool`, optional
            If `True`, ignore `DimensionElement.cached` and always return the
            storage implementation that would be used without caching.

        Returns
        -------
        cls : `type`
            A concrete subclass of `DimensionRecordStorage`.

        Notes
        -----
        At present, these defaults are always used, but we may add support for
        explicitly setting the class to use in configuration in the future.
        """
        if not ignoreCached and element.cached:
            from ..dimensions.caching import CachingDimensionRecordStorage
            return CachingDimensionRecordStorage
        elif element.hasTable():
            if element.viewOf is not None:
                if element.spatial is not None:
                    raise NotImplementedError("Spatial view dimension storage is not supported.")
                from ..dimensions.query import QueryDimensionRecordStorage
                return QueryDimensionRecordStorage
            elif element.spatial is not None:
                from ..dimensions.spatial import SpatialDimensionRecordStorage
                return SpatialDimensionRecordStorage
            else:
                from ..dimensions.table import TableDimensionRecordStorage
                return TableDimensionRecordStorage
        elif isinstance(element, SkyPixDimension):
            from ..dimensions.skypix import SkyPixDimensionRecordStorage
            return SkyPixDimensionRecordStorage
        raise NotImplementedError(f"No default DimensionRecordStorage class for {element}.")

    @property
    @abstractmethod
    def element(self) -> DimensionElement:
        """The element whose records this instance holds (`DimensionElement`).
        """
        raise NotImplementedError()

    @abstractmethod
    def clearCaches(self) -> None:
        """Clear any in-memory caches held by the storage instance.

        This is called by `Registry` when transactions are rolled back, to
        avoid in-memory caches from ever containing records that are not
        present in persistent storage.
        """
        raise NotImplementedError()

    @abstractmethod
    def join(
        self,
        builder: QueryBuilder, *,
        regions: Optional[NamedKeyDict[DimensionElement, sqlalchemy.sql.ColumnElement]] = None,
        timespans: Optional[NamedKeyDict[DimensionElement, Timespan[sqlalchemy.sql.ColumnElement]]] = None,
    ) -> sqlalchemy.sql.FromClause:
        """Add the dimension element's logical table to a query under
        construction.

        This is a visitor pattern interface that is expected to be called only
        by `QueryBuilder.joinDimensionElement`.

        Parameters
        ----------
        builder : `QueryBuilder`
            Builder for the query that should contain this element.
        regions : `NamedKeyDict`, optional
            A mapping from `DimensionElement` to a SQLAlchemy column containing
            the region for that element, which should be updated to include a
            region column for this element if one exists.  If `None`,
            ``self.element`` is not being included in the query via a spatial
            join.
        timespan : `NamedKeyDict`, optional
            A mapping from `DimensionElement` to a `Timespan` of SQLALchemy
            columns containing the timespan for that element, which should be
            updated to include timespan columns for this element if they exist.
            If `None`, ``self.element`` is not being included in the query via
            a temporal join.

        Returns
        -------
        fromClause : `sqlalchemy.sql.FromClause`
            Table or clause for the element which is joined.

        Notes
        -----
        Elements are only included in queries via spatial and/or temporal joins
        when necessary to connect them to other elements in the query, so
        ``regions`` and ``timespans`` cannot be assumed to be not `None` just
        because an element has a region or timespan.
        """
        raise NotImplementedError()

    @abstractmethod
    def insert(self, *records: DimensionRecord) -> None:
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
    def sync(self, record: DimensionRecord) -> bool:
        """Synchronize a record with the database, inserting it only if it does
        not exist and comparing values if it does.

        Parameters
        ----------
        record : `DimensionRecord`.
            An instance of the `DimensionRecord` subclass for the
            element this storage is associated with.

        Returns
        -------
        inserted : `bool`
            `True` if a new row was inserted, `False` otherwise.

        Raises
        ------
        DatabaseConflictError
            Raised if the record exists in the database (according to primary
            key lookup) but is inconsistent with the given one.
        TypeError
            Raised if the element does not support record synchronization.
        sqlalchemy.exc.IntegrityError
            Raised if one or more records violate database integrity
            constraints.

        Notes
        -----
        This method cannot be called within transactions, as it needs to be
        able to perform its own transaction to be concurrent.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch(self, dataIds: DataCoordinateIterable) -> Iterable[DimensionRecord]:
        """Retrieve records from storage.

        Parameters
        ----------
        dataIds : `DataCoordinateIterable`
            Data IDs that identify the records to be retrieved.

        Returns
        -------
        records : `Iterable` [ `DimensionRecord` ]
            Record retrieved from storage.  Not all data IDs may have
            corresponding records (if there are no records that match a data
            ID), and even if they are, the order of inputs is not preserved.
        """
        raise NotImplementedError()


class DimensionRecordStorageManager(ABC):
    """An interface for managing the dimension records in a `Registry`.

    `DimensionRecordStorageManager` primarily serves as a container and factory
    for `DimensionRecordStorage` instances, which each provide access to the
    records for a different `DimensionElement`.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Universe of all dimensions and dimension elements known to the
        `Registry`.

    Notes
    -----
    In a multi-layer `Registry`, many dimension elements will only have
    records in one layer (often the base layer).  The union of the records
    across all layers forms the logical table for the full `Registry`.
    """
    def __init__(self, *, universe: DimensionUniverse):
        self.universe = universe

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *,
                   universe: DimensionUniverse) -> DimensionRecordStorageManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.
        universe : `DimensionUniverse`
            Universe graph containing dimensions known to this `Registry`.

        Returns
        -------
        manager : `DimensionRecordStorageManager`
            An instance of a concrete `DimensionRecordStorageManager` subclass.
        """
        raise NotImplementedError()

    @abstractmethod
    def refresh(self) -> None:
        """Ensure all other operations on this manager are aware of any
        dataset types that may have been registered by other clients since
        it was initialized or last refreshed.
        """
        raise NotImplementedError()

    def __getitem__(self, element: DimensionElement) -> DimensionRecordStorage:
        """Interface to `get` that raises `LookupError` instead of returning
        `None` on failure.
        """
        r = self.get(element)
        if r is None:
            raise LookupError(f"No dimension element '{element.name}' found in this registry layer.")
        return r

    @abstractmethod
    def get(self, element: DimensionElement) -> Optional[DimensionRecordStorage]:
        """Return an object that provides access to the records associated with
        the given element, if one exists in this layer.

        Parameters
        ----------
        element : `DimensionElement`
            Element for which records should be returned.

        Returns
        -------
        records : `DimensionRecordStorage` or `None`
            The object representing the records for the given element in this
            layer, or `None` if there are no records for that element in this
            layer.

        Notes
        -----
        Dimension elements registered by another client of the same layer since
        the last call to `initialize` or `refresh` may not be found.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(self, element: DimensionElement) -> DimensionRecordStorage:
        """Ensure that this layer can hold records for the given element,
        creating new tables as necessary.

        Parameters
        ----------
        element : `DimensionElement`
            Element for which a table should created (as necessary) and
            an associated `DimensionRecordStorage` returned.

        Returns
        -------
        records : `DimensionRecordStorage`
            The object representing the records for the given element in this
            layer.

        Raises
        ------
        TransactionInterruption
            Raised if this operation is invoked within a `Database.transaction`
            context.
        """
        raise NotImplementedError()

    @abstractmethod
    def clearCaches(self) -> None:
        """Clear any in-memory caches held by nested `DimensionRecordStorage`
        instances.

        This is called by `Registry` when transactions are rolled back, to
        avoid in-memory caches from ever containing records that are not
        present in persistent storage.
        """
        raise NotImplementedError()

    universe: DimensionUniverse
    """Universe of all dimensions and dimension elements known to the
    `Registry` (`DimensionUniverse`).
    """
