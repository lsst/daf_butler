from __future__ import annotations

__all__ = ["DimensionTableRecords", "DimensionTableManager"]

from abc import ABC, abstractmethod
from typing import (
    Optional,
)

import sqlalchemy

from ...core.dimensions import (
    DataCoordinate,
    DimensionElement,
    DimensionRecord,
    DimensionUniverse,
)

from .database import Database, StaticTablesContext


class DimensionTableRecords(ABC):
    """An interface that manages the records associated with a particular
    `DimensionElement` in a `RegistryLayer`.

    Parameters
    ----------
    element : `DimensionElement`
        Element whose records this object manages.

    Notes
    -----
    Some `DimensionTableRecords` subclasses may not actually interact with
    a database (especially in cases where record values can be computed
    on-the-fly, e.g. `SkyPixDimension`), or may map to a view or query
    rather than a true table.  In these cases, `insert` need not be
    implemented.
    """
    def __init__(self, element: DimensionElement):
        self.element = element

    @abstractmethod
    def insert(self, *records: DimensionRecord):
        """Insert the given records into the database.

        Parameters
        ----------
        records
            Positional arguments are records to be inserted.  All records must
            satisfy ``record.definition == self.element``.

        Raises
        ------
        TypeError
            Raised if the element does not support record insertion.
        sqlalchemy.exc.IntegrityError
            Raised if one or more records violate database integrity
            constraints.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        """Retrieve a record..

        Parameters
        ----------
        dataId : `DataCoordinate`
            A data ID that identifies the record to be retrieved.  Must
            satisfy ``dataId.graph == self.element.graph``.

        Returns
        -------
        record : `DimensionRecord` or `None`
            The retrieved record, or `None` if there is no such record.
        """
        raise NotImplementedError()

    @abstractmethod
    def select(self) -> Optional[sqlalchemy.sql.FromClause]:
        """Return a SQLAlchemy object that represents the logical table for
        this element in this layer.

        Returns
        -------
        selectable : `sqlalchemy.sql.FromClause` or `None`
            SQLAlchemy object that can be used in a ``SELECT`` query, or `None`
            if there is no database representation for this element in this
            layer.

        Notes
        -----
        The returned object is guaranteed to have the columns listed in
        ``self.element.makeTableSpec()`` or (equivalently)
        ``self.element.RecordClass.__slots__``.
        """
        raise NotImplementedError()

    @abstractmethod
    def selectCommonSkyPixOverlap(self) -> Optional[sqlalchemy.sql.FromClause]:
        """Return a SQLAlchemy object that represents the logical table that
        relates this element to `DimensionUniverse.commonSkyPix`.

        Returns
        -------
        selectable : `sqlalchemy.sql.FromClause` or `None`
            SQLAlchemy object that can be used in a ``SELECT`` query, or `None`
            if ``self.element.spatial`` is `False` or there is no database
            representation for this element in this layer.

        Notes
        -----
        The returned object is guaranteed to have columns whose names are the
        combination of ``self.element.graph.required.names`` and
        `DimensionUniverse.commonSkyPix.name`.
        """
        raise NotImplementedError()

    element: DimensionElement
    """Element whose records this object manages (`DimensionElement`).
    """


class DimensionTableManager(ABC):
    """An interface for managing the dimension records in a `RegistryLayer`.

    `DimensionTableManager` primarily serves as a container and factory for
    `DimensionTableRecords` instances, which each provide access to the records
    for a different `DimensionElement`.

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
                   universe: DimensionUniverse) -> DimensionTableManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        schema : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.
        universe : `DimensionUniverse`
            Universe graph containing dimensions known to this `Registry`.

        Returns
        -------
        manager : `DimensionTableManager`
            An instance of a concrete `DimensionTableManager` subclass.
        """
        raise NotImplementedError()

    @abstractmethod
    def refresh(self):
        """Ensure all other operations on this manager are aware of any
        dataset types that may have been registered by other clients since
        it was initialized or last refreshed.
        """
        raise NotImplementedError()

    @abstractmethod
    def get(self, element: DimensionElement) -> Optional[DimensionTableRecords]:
        """Return an object that provides access to the records associated with
        the given element, if one exists in this layer.

        Parameters
        ----------
        element : `DimensionElement`
            Element for which records should be returned.

        Returns
        -------
        records : `DimensionTableRecords` or `None`
            The object representing the records for the given element in this
            layer, or `None` if there are no records for that element in this
            layer.

        Note
        ----
        Dimension elements registered by another client of the same layer since
        the last call to `initialize` or `refresh` may not be found.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(self, element: DimensionElement) -> DimensionTableRecords:
        """Ensure that this layer can hold records for the given element,
        creating new tables as necessary.

        Parameters
        ----------
        element : `DimensionElement`
            Element for which a table should created (as necessary) and
            an associated `DimensionTableRecords` returned.

        Returns
        -------
        records : `DimensionTableRecords`
            The object representing the records for the given element in this
            layer.

        Raises
        ------
        TransactionInterruption
            Raised if this operation is invoked within a `Database.transaction`
            context.
        """
        raise NotImplementedError()

    universe: DimensionUniverse
    """Universe of all dimensions and dimension elements known to the
    `Registry` (`DimensionUniverse`).
    """
