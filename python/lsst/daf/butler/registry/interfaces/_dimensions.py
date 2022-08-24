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
    "DatabaseDimensionOverlapStorage",
    "DatabaseDimensionRecordStorage",
    "DimensionRecordStorage",
    "DimensionRecordStorageManager",
    "GovernorDimensionRecordStorage",
    "SkyPixDimensionRecordStorage",
)

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Set
from typing import TYPE_CHECKING, Any, Tuple, Union

import sqlalchemy

from ...core import DatabaseDimensionElement, DimensionGraph, GovernorDimension, SkyPixDimension
from ._versioning import VersionedExtension

if TYPE_CHECKING:
    from ...core import (
        DataCoordinateIterable,
        DimensionElement,
        DimensionRecord,
        DimensionUniverse,
        NamedKeyDict,
        NamedKeyMapping,
        TimespanDatabaseRepresentation,
    )
    from ..queries import QueryBuilder
    from ._database import Database, StaticTablesContext


OverlapSide = Union[SkyPixDimension, Tuple[DatabaseDimensionElement, str]]


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
        """The element whose records this instance managers
        (`DimensionElement`).
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
        builder: QueryBuilder,
        *,
        regions: NamedKeyDict[DimensionElement, sqlalchemy.sql.ColumnElement] | None = None,
        timespans: NamedKeyDict[DimensionElement, TimespanDatabaseRepresentation] | None = None,
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
    def insert(self, *records: DimensionRecord, replace: bool = False, skip_existing: bool = False) -> None:
        """Insert one or more records into storage.

        Parameters
        ----------
        records
            One or more instances of the `DimensionRecord` subclass for the
            element this storage is associated with.
        replace: `bool`, optional
            If `True` (`False` is default), replace existing records in the
            database if there is a conflict.
        skip_existing : `bool`, optional
            If `True` (`False` is default), skip insertion if a record with
            the same primary key values already exists.

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
    def sync(self, record: DimensionRecord, update: bool = False) -> bool | dict[str, Any]:
        """Synchronize a record with the database, inserting it only if it does
        not exist and comparing values if it does.

        Parameters
        ----------
        record : `DimensionRecord`.
            An instance of the `DimensionRecord` subclass for the
            element this storage is associated with.
        update: `bool`, optional
            If `True` (`False` is default), update the existing record in the
            database if there is a conflict.

        Returns
        -------
        inserted_or_updated : `bool` or `dict`
            `True` if a new row was inserted, `False` if no changes were
            needed, or a `dict` mapping updated column names to their old
            values if an update was performed (only possible if
            ``update=True``).

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

    @abstractmethod
    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        """Return tables used for schema digest.

        Returns
        -------
        tables : `Iterable` [ `sqlalchemy.schema.Table` ]
            Possibly empty set of tables for schema digest calculations.
        """
        raise NotImplementedError()


class GovernorDimensionRecordStorage(DimensionRecordStorage):
    """Intermediate interface for `DimensionRecordStorage` objects that provide
    storage for `GovernorDimension` instances.
    """

    @classmethod
    @abstractmethod
    def initialize(
        cls,
        db: Database,
        dimension: GovernorDimension,
        *,
        context: StaticTablesContext | None = None,
        config: Mapping[str, Any],
    ) -> GovernorDimensionRecordStorage:
        """Construct an instance of this class using a standardized interface.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        dimension : `GovernorDimension`
            Dimension the new instance will manage records for.
        context : `StaticTablesContext`, optional
            If provided, an object to use to create any new tables.  If not
            provided, ``db.ensureTableExists`` should be used instead.
        config : `Mapping`
            Extra configuration options specific to the implementation.

        Returns
        -------
        storage : `GovernorDimensionRecordStorage`
            A new `GovernorDimensionRecordStorage` subclass instance.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def element(self) -> GovernorDimension:
        # Docstring inherited from DimensionRecordStorage.
        raise NotImplementedError()

    @abstractmethod
    def refresh(self) -> None:
        """Ensure all other operations on this manager are aware of any
        changes made by other clients since it was initialized or last
        refreshed.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def values(self) -> Set[str]:
        """All primary key values for this dimension (`set` [ `str` ]).

        This may rely on an in-memory cache and hence not reflect changes to
        the set of values made by other `Butler` / `Registry` clients.  Call
        `refresh` to ensure up-to-date results.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def table(self) -> sqlalchemy.schema.Table:
        """The SQLAlchemy table that backs this dimension
        (`sqlalchemy.schema.Table`).
        """
        raise NotImplementedError

    @abstractmethod
    def registerInsertionListener(self, callback: Callable[[DimensionRecord], None]) -> None:
        """Add a function or method to be called after new records for this
        dimension are inserted by `insert` or `sync`.

        Parameters
        ----------
        callback
            Callable that takes a single `DimensionRecord` argument.  This will
            be called immediately after any successful insertion, in the same
            transaction.
        """
        raise NotImplementedError()


class SkyPixDimensionRecordStorage(DimensionRecordStorage):
    """Intermediate interface for `DimensionRecordStorage` objects that provide
    storage for `SkyPixDimension` instances.
    """

    @property
    @abstractmethod
    def element(self) -> SkyPixDimension:
        # Docstring inherited from DimensionRecordStorage.
        raise NotImplementedError()


class DatabaseDimensionRecordStorage(DimensionRecordStorage):
    """Intermediate interface for `DimensionRecordStorage` objects that provide
    storage for `DatabaseDimensionElement` instances.
    """

    @classmethod
    @abstractmethod
    def initialize(
        cls,
        db: Database,
        element: DatabaseDimensionElement,
        *,
        context: StaticTablesContext | None = None,
        config: Mapping[str, Any],
        governors: NamedKeyMapping[GovernorDimension, GovernorDimensionRecordStorage],
    ) -> DatabaseDimensionRecordStorage:
        """Construct an instance of this class using a standardized interface.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        element : `DatabaseDimensionElement`
            Dimension element the new instance will manage records for.
        context : `StaticTablesContext`, optional
            If provided, an object to use to create any new tables.  If not
            provided, ``db.ensureTableExists`` should be used instead.
        config : `Mapping`
            Extra configuration options specific to the implementation.
        governors : `NamedKeyMapping`
            Mapping containing all governor dimension storage implementations.

        Returns
        -------
        storage : `DatabaseDimensionRecordStorage`
            A new `DatabaseDimensionRecordStorage` subclass instance.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def element(self) -> DatabaseDimensionElement:
        # Docstring inherited from DimensionRecordStorage.
        raise NotImplementedError()

    def connect(self, overlaps: DatabaseDimensionOverlapStorage) -> None:
        """Inform this record storage object of the object that will manage
        the overlaps between this element and another element.

        This will only be called if ``self.element.spatial is not None``,
        and will be called immediately after construction (before any other
        methods).  In the future, implementations will be required to call a
        method on any connected overlap storage objects any time new records
        for the element are inserted.

        Parameters
        ----------
        overlaps : `DatabaseDimensionRecordStorage`
            Object managing overlaps between this element and another
            database-backed element.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support spatial elements.")


class DatabaseDimensionOverlapStorage(ABC):
    """A base class for objects that manage overlaps between a pair of
    database-backed dimensions.
    """

    @classmethod
    @abstractmethod
    def initialize(
        cls,
        db: Database,
        elementStorage: tuple[DatabaseDimensionRecordStorage, DatabaseDimensionRecordStorage],
        governorStorage: tuple[GovernorDimensionRecordStorage, GovernorDimensionRecordStorage],
        context: StaticTablesContext | None = None,
    ) -> DatabaseDimensionOverlapStorage:
        """Construct an instance of this class using a standardized interface.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        elementStorage : `tuple` [ `DatabaseDimensionRecordStorage` ]
            Storage objects for the elements this object will related.
        governorStorage : `tuple` [ `GovernorDimensionRecordStorage` ]
            Storage objects for the governor dimensions of the elements this
            object will related.
        context : `StaticTablesContext`, optional
            If provided, an object to use to create any new tables.  If not
            provided, ``db.ensureTableExists`` should be used instead.

        Returns
        -------
        storage : `DatabaseDimensionOverlapStorage`
            A new `DatabaseDimensionOverlapStorage` subclass instance.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def elements(self) -> tuple[DatabaseDimensionElement, DatabaseDimensionElement]:
        """The pair of elements whose overlaps this object manages.

        The order of elements is the same as their ordering within the
        `DimensionUniverse`.
        """
        raise NotImplementedError()

    @abstractmethod
    def digestTables(self) -> Iterable[sqlalchemy.schema.Table]:
        """Return tables used for schema digest.

        Returns
        -------
        tables : `Iterable` [ `sqlalchemy.schema.Table` ]
            Possibly empty set of tables for schema digest calculations.
        """
        raise NotImplementedError()


class DimensionRecordStorageManager(VersionedExtension):
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
    def initialize(
        cls, db: Database, context: StaticTablesContext, *, universe: DimensionUniverse
    ) -> DimensionRecordStorageManager:
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
        changes made by other clients since it was initialized or last
        refreshed.
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
    def get(self, element: DimensionElement) -> DimensionRecordStorage | None:
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
    def saveDimensionGraph(self, graph: DimensionGraph) -> int:
        """Save a `DimensionGraph` definition to the database, allowing it to
        be retrieved later via the returned key.

        Parameters
        ----------
        graph : `DimensionGraph`
            Set of dimensions to save.

        Returns
        -------
        key : `int`
            Integer used as the unique key for this `DimensionGraph` in the
            database.

        Raises
        ------
        TransactionInterruption
            Raised if this operation is invoked within a `Database.transaction`
            context.
        """
        raise NotImplementedError()

    @abstractmethod
    def loadDimensionGraph(self, key: int) -> DimensionGraph:
        """Retrieve a `DimensionGraph` that was previously saved in the
        database.

        Parameters
        ----------
        key : `int`
            Integer used as the unique key for this `DimensionGraph` in the
            database.

        Returns
        -------
        graph : `DimensionGraph`
            Retrieved graph.

        Raises
        ------
        KeyError
            Raised if the given key cannot be found in the database.
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
