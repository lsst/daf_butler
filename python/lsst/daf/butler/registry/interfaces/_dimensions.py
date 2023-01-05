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
from lsst.daf.relation import Join, Relation, sql

from ...core import (
    ColumnTypeInfo,
    DatabaseDimensionElement,
    DataCoordinate,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
    DimensionUniverse,
    GovernorDimension,
    LogicalColumn,
    SkyPixDimension,
)
from ...core.named import NamedKeyMapping
from ._versioning import VersionedExtension

if TYPE_CHECKING:
    from .. import queries
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
        target: Relation,
        join: Join,
        context: queries.SqlQueryContext,
    ) -> Relation:
        """Join this dimension element's records to a relation.

        Parameters
        ----------
        target : `~lsst.daf.relation.Relation`
            Existing relation to join to.  Implementations may require that
            this relation already include dimension key columns for this
            dimension element and assume that dataset or spatial join relations
            that might provide these will be included in the relation tree
            first.
        join : `~lsst.daf.relation.Join`
            Join operation to use when the implementation is an actual join.
            When a true join is being simulated by other relation operations,
            this objects `~lsst.daf.relation.Join.min_columns` and
            `~lsst.daf.relation.Join.max_columns` should still be respected.
        context : `.queries.SqlQueryContext`
            Object that manages relation engines and database-side state (e.g.
            temporary tables) for the query.

        Returns
        -------
        joined : `~lsst.daf.relation.Relation`
            New relation that includes this relation's dimension key and record
            columns, as well as all columns in ``target``,  with rows
            constrained to those for which this element's dimension key values
            exist in the registry and rows already exist in ``target``.
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
    def fetch_one(self, data_id: DataCoordinate, context: queries.SqlQueryContext) -> DimensionRecord | None:
        """Retrieve a single record from storage.

        Parameters
        ----------
        data_id : `DataCoordinate`
            Data ID of the record to fetch.  Implied dimensions do not need to
            be present.
        context : `.queries.SqlQueryContext`
            Context to be used to execute queries when no cached result is
            available.

        Returns
        -------
        record : `DimensionRecord` or `None`
            Fetched record, or *possibly* `None` if there was no match for the
            given data ID.
        """
        raise NotImplementedError()

    def get_record_cache(
        self, context: queries.SqlQueryContext
    ) -> Mapping[DataCoordinate, DimensionRecord] | None:
        """Return a local cache of all `DimensionRecord` objects for this
        element, fetching it if necessary.

        Implementations that never cache records should return `None`.

        Parameters
        ----------
        context : `.queries.SqlQueryContext`
            Context to be used to execute queries when no cached result is
            available.

        Returns
        -------
        cache : `Mapping` [ `DataCoordinate`, `DimensionRecord` ] or `None`
            Mapping from data ID to dimension record, or `None`.
        """
        return None

    @abstractmethod
    def digestTables(self) -> list[sqlalchemy.schema.Table]:
        """Return tables used for schema digest.

        Returns
        -------
        tables : `list` [ `sqlalchemy.schema.Table` ]
            Possibly empty list of tables for schema digest calculations.
        """
        raise NotImplementedError()

    def _build_sql_payload(
        self,
        from_clause: sqlalchemy.sql.FromClause,
        column_types: ColumnTypeInfo,
    ) -> sql.Payload[LogicalColumn]:
        """Construct a `lsst.daf.relation.sql.Payload` for a dimension table.

        This is a conceptually "protected" helper method for use by subclass
        `make_relation` implementations.

        Parameters
        ----------
        from_clause : `sqlalchemy.sql.FromClause`
            SQLAlchemy table or subquery to select from.
        column_types : `ColumnTypeInfo`
            Struct with information about column types that depend on registry
            configuration.

        Returns
        -------
        payload : `lsst.daf.relation.sql.Payload`
            Relation SQL "payload" struct, containing a SQL FROM clause,
            columns, and optional WHERE clause.
        """
        payload = sql.Payload[LogicalColumn](from_clause)
        for tag, field_name in self.element.RecordClass.fields.columns.items():
            if field_name == "timespan":
                payload.columns_available[tag] = column_types.timespan_cls.from_columns(
                    from_clause.columns, name=field_name
                )
            else:
                payload.columns_available[tag] = from_clause.columns[field_name]
        return payload


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

    @property
    @abstractmethod
    def table(self) -> sqlalchemy.schema.Table:
        """The SQLAlchemy table that backs this dimension
        (`sqlalchemy.schema.Table`).
        """
        raise NotImplementedError()

    def join(
        self,
        target: Relation,
        join: Join,
        context: queries.SqlQueryContext,
    ) -> Relation:
        # Docstring inherited.
        # We use Join.partial(...).apply(...) instead of Join.apply(..., ...)
        # for the "backtracking" insertion capabilities of the former; more
        # specifically, if `target` is a tree that starts with SQL relations
        # and ends with iteration-engine operations (e.g. region-overlap
        # postprocessing), this will try to perform the join upstream in the
        # SQL engine before the transfer to iteration.
        return join.partial(self.make_relation(context)).apply(target)

    @abstractmethod
    def make_relation(self, context: queries.SqlQueryContext) -> Relation:
        """Return a relation that represents this dimension element's table.

        This is used to provide an implementation for
        `DimensionRecordStorage.join`, and is also callable in its own right.

        Parameters
        ----------
        context : `.queries.SqlQueryContext`
            Object that manages relation engines and database-side state
            (e.g. temporary tables) for the query.

        Returns
        -------
        relation : `~lsst.daf.relation.Relation`
            New relation that includes this relation's dimension key and
            record columns, with rows constrained to those for which the
            dimension key values exist in the registry.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_record_cache(self, context: queries.SqlQueryContext) -> Mapping[DataCoordinate, DimensionRecord]:
        raise NotImplementedError()

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
        view_target: DatabaseDimensionRecordStorage | None = None,
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
        view_target : `DatabaseDimensionRecordStorage`, optional
            Storage object for the element this target's storage is a view of
            (i.e. when `viewOf` is not `None`).

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

    def join(
        self,
        target: Relation,
        join: Join,
        context: queries.SqlQueryContext,
    ) -> Relation:
        # Docstring inherited.
        # See comment on similar code in GovernorDimensionRecordStorage.join
        # for why we use `Join.partial` here.
        return join.partial(self.make_relation(context)).apply(target)

    @abstractmethod
    def make_relation(self, context: queries.SqlQueryContext) -> Relation:
        """Return a relation that represents this dimension element's table.

        This is used to provide an implementation for
        `DimensionRecordStorage.join`, and is also callable in its own right.

        Parameters
        ----------
        context : `.queries.SqlQueryContext`
            Object that manages relation engines and database-side state
            (e.g. temporary tables) for the query.

        Returns
        -------
        relation : `~lsst.daf.relation.Relation`
            New relation that includes this relation's dimension key and
            record columns, with rows constrained to those for which the
            dimension key values exist in the registry.
        """
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

    def make_spatial_join_relation(
        self,
        other: DimensionElement,
        context: queries.SqlQueryContext,
        governor_constraints: Mapping[str, Set[str]],
    ) -> Relation | None:
        """Return a `lsst.daf.relation.Relation` that represents the spatial
        overlap join between two dimension elements.

        High-level code should generally call
        `DimensionRecordStorageManager.make_spatial_join_relation` (which
        delegates to this) instead of calling this method directly.

        Parameters
        ----------
        other : `DimensionElement`
            Element to compute overlaps with.  Guaranteed by caller to be
            spatial (as is ``self``), with a different topological family.  May
            be a `DatabaseDimensionElement` or a `SkyPixDimension`.
        context : `.queries.SqlQueryContext`
            Object that manages relation engines and database-side state
            (e.g. temporary tables) for the query.
        governor_constraints : `Mapping` [ `str`, `~collections.abc.Set` ], \
                optional
            Constraints imposed by other aspects of the query on governor
            dimensions.

        Returns
        -------
        relation : `lsst.daf.relation.Relation` or `None`
            Join relation.  Should be `None` when no direct overlaps for this
            combination are stored; higher-level code is responsible for
            working out alternative approaches involving multiple joins.
        """
        return None


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
    def clearCaches(self) -> None:
        """Clear any cached state about which overlaps have been
        materialized."""
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

    @abstractmethod
    def make_relation(
        self,
        context: queries.SqlQueryContext,
        governor_constraints: Mapping[str, Set[str]],
    ) -> Relation | None:
        """Return a `lsst.daf.relation.Relation` that represents the join
        table.

        High-level code should generally call
        `DimensionRecordStorageManager.make_spatial_join_relation` (which
        delegates to this) instead of calling this method directly.

        Parameters
        ----------
        context : `.queries.SqlQueryContext`
            Object that manages relation engines and database-side state
            (e.g. temporary tables) for the query.
        governor_constraints : `Mapping` [ `str`, `~collections.abc.Set` ], \
                optional
            Constraints imposed by other aspects of the query on governor
            dimensions; collections inconsistent with these constraints will be
            skipped.

        Returns
        -------
        relation : `lsst.daf.relation.Relation` or `None`
            Join relation.  Should be `None` when no direct overlaps for this
            combination are stored; higher-level code is responsible for
            working out alternative approaches involving multiple joins.
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

    def __getitem__(self, element: DimensionElement | str) -> DimensionRecordStorage:
        """Interface to `get` that raises `LookupError` instead of returning
        `None` on failure.
        """
        r = self.get(element)
        if r is None:
            raise LookupError(f"No dimension element '{element}' found in this registry layer.")
        return r

    @abstractmethod
    def get(self, element: DimensionElement | str) -> DimensionRecordStorage | None:
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

    @abstractmethod
    def make_spatial_join_relation(
        self,
        element1: str,
        element2: str,
        context: queries.SqlQueryContext,
        governor_constraints: Mapping[str, Set[str]],
    ) -> tuple[Relation, bool]:
        """Create a relation that represents the spatial join between two
        dimension elements.

        Parameters
        ----------
        element1 : `str`
            Name of one of the elements participating in the join.
        element2 : `str`
            Name of the other element participating in the join.
        context : `.queries.SqlQueryContext`
            Object that manages relation engines and database-side state
            (e.g. temporary tables) for the query.
        governor_constraints : `Mapping` [ `str`, `collections.abc.Set` ], \
                optional
            Constraints imposed by other aspects of the query on governor
            dimensions.

        Returns
        -------
        relation : ``lsst.daf.relation.Relation`
            New relation that represents a spatial join between the two given
            elements.  Guaranteed to have key columns for all required
            dimensions of both elements.
        needs_refinement : `bool`
            Whether the returned relation represents a conservative join that
            needs refinement via native-iteration predicate.
        """
        raise NotImplementedError()

    universe: DimensionUniverse
    """Universe of all dimensions and dimension elements known to the
    `Registry` (`DimensionUniverse`).
    """
