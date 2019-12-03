from __future__ import annotations

__all__ = ["CollectionManager", "CollectionType"]

from abc import ABC, abstractmethod
from datetime import datetime
import enum
from typing import (
    Any,
    Optional,
    TYPE_CHECKING,
)

from ...core.schema import TableSpec, FieldSpec
from ...core.timespan import Timespan

if TYPE_CHECKING:
    from .database import Database, StaticTablesContext


class CollectionType(enum.IntEnum):
    """Enumeration used to label different types of collections.
    """

    RUN = 1
    """A ``RUN`` collection (also just called a 'run') is the initial
    collection a dataset is inserted into and the only one it can never be
    removed from.

    Within a particular run, there may only be one dataset with a particular
    dataset type and data ID, regardless of the `DatasetUniqueness` for that
    dataset type.
    """

    TAGGED = 2
    """Datasets can be associated with and removed from ``TAGGED`` collections
    arbitrarily.

    For `DatasetUniqueness.STANDARD` dataset types, there may be at most one
    dataset with a particular type and data ID in a ``TAGGED`` collection.
    There is no constraint on the number of `DatasetUniqueness.NONSINGULAR`
    datasets.
    """

    CALIBRATION = 3
    """Each dataset in a ``CALIBRATION`` collection is associated with a
    validity range (which may be different for the same dataset in different
    collections).

    There is no constraint on the number of datasets of any type or data ID
    in ``CALIBRATION`` collections, regardless of their `DatasetUniqueness`
    value.

    There is no database-level constraint on non-overlapping validity ranges,
    but some operations nevertheless assume that higher-level code that
    populates them ensures that there is at most one dataset with a particular
    dataset type and data ID that is valid at any particular time.
    """


class CollectionRecord(ABC):
    """A struct used to represent a collection in internal `Registry` APIs.

    User-facing code should always just use a `str` to represent collections.

    Parameters
    ----------
    name : `str`
        Name of the collection.
    id : `int`
        Unique ID used for primary and foreign key fields that refer to
        collections.
    type : `CollectionType`
        Enumeration value describing the type of the collection.

    Notes
    -----
    `CollectionRecord` inherits from `abc.ABC` because its `RunRecord` subclass
    is abstract, though `CollectionRecord` itself is not.
    """

    def __init__(self, name: str, id: int, type: CollectionType):
        self.name = name
        self.id = id
        self.type = type

    name: str
    """Name of the collection (`str`).
    """

    type: CollectionType
    """Enumeration value describing the type of the collection
    (`CollectionType`).
    """

    id: int
    """Unique ID used for primary and foreign key fields that refer to
    collections (`int`).

    Code should generally avoid assuming this is an integer value; we could
    imagine a schema that just uses names directly as the collection primary
    key.  While we could try to hide that choice behind a layer of abstraction,
    at present that looks like it'd be more work than just changing it
    everywhere it's referenced later, especially if downstream code just treats
    this as opaque.
    """


class RunRecord(CollectionRecord):
    """A subclass of `CollectionRecord` that adds execution information and
    an interface for updating it.
    """

    @abstractmethod
    def update(self, host: Optional[str] = None, timespan: Timespan[Optional[datetime]] = None):
        """Update the database record for this run with new execution
        information.

        Values not provided will set to ``NULL`` in the database, not ignored.

        Parameters
        ----------
        host : `str`, optional
            Name of the host or system on which this run was produced.
            Detailed form to be set by higher-level convention; from the
            `Registry` perspective, this is an entirely opaque value.
        timespan : `Timespan`, optional
            Begin and end timestamps for the period over which the run was
            produced.  `None`/``NULL`` values are interpreted as infinite
            bounds.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def host(self) -> Optional[str]:
        """Return the name of the host or system on which this run was
        produced (`str` or `None`).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def timespan(self) -> Timespan[Optional[datetime]]:
        """Begin and end timestamps for the period over which the run was
        produced.  `None`/``NULL`` values are interpreted as infinite
        bounds.
        """
        raise NotImplementedError()


class CollectionManager(ABC):
    """An interface for managing the collections (including runs) in a
    `RegistryLayer`.

    Notes
    -----
    Each layer in a multi-layer `Registry` has its own record for any
    collection for which it has datasets (or quanta).  Different layers may
    use different IDs for the same collection, so any usage of the IDs
    obtained through the `CollectionManager` APIs are strictly for internal
    (to `Registry`) use.
    """

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, schema: StaticTablesContext) -> CollectionManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        schema : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.

        Returns
        -------
        manager : `CollectionManager`
            An instance of a concrete `CollectionManager` subclass.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def addCollectionForeignKey(cls, tableSpec: TableSpec, *, name: str = "collection",
                                onDelete: Optional[str] = None, **kwds: Any) -> FieldSpec:
        """Add a foreign key (field and constraint) referencing the collection
        table.

        Parameters
        ----------
        tableSpec : `TableSpec`
            Specification for the table that should reference the collection
            table.  Will be modified in place.
        name: `str`, optional
            A name to use for the prefix of the new field; the full name is
            ``{name}_id``.
        onDelete: `str`, optional
            One of "CASCADE" or "SET NULL", indicating what should happen to
            the referencing row if the collection row is deleted.  `None`
            indicates that this should be an integrity error.
        kwds
            Additional keyword arguments are forwarded to the `FieldSpec`
            constructor (only the ``name`` and ``dtype`` arguments are
            otherwise provided).

        Returns
        -------
        fieldSpec : `FieldSpec`
            Specification for the field being added.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def addRunForeignKey(cls, tableSpec: TableSpec, *, name: str = "run",
                         onDelete: Optional[str] = None, **kwds: Any) -> FieldSpec:
        """Add a foreign key (field and constraint) referencing the run
        table.

        Parameters
        ----------
        tableSpec : `TableSpec`
            Specification for the table that should reference the run table.
            Will be modified in place.
        name: `str`, optional
            A name to use for the prefix of the new field; the full name is
            ``{name}_id``.
        onDelete: `str`, optional
            One of "CASCADE" or "SET NULL", indicating what should happen to
            the referencing row if the collection row is deleted.  `None`
            indicates that this should be an integrity error.
        kwds
            Additional keyword arguments are forwarded to the `FieldSpec`
            constructor (only the ``name`` and ``dtype`` arguments are
            otherwise provided).

        Returns
        -------
        fieldSpec : `FieldSpec`
            Specification for the field being added.
        """
        raise NotImplementedError()

    @abstractmethod
    def refresh(self):
        """Ensure all other operations on this manager are aware of any
        collections that may have been registered by other clients since it
        was initialized or last refreshed.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(self, name: str, type: CollectionType) -> CollectionRecord:
        """Ensure that a collection of the given name and type are present
        in the layer this manager is associated with.

        Parameters
        ----------
        name : `str`
            Name of the collection.
        type : `CollectionType`
            Enumeration value indicating the type of collection.

        Returns
        -------
        record : `CollectionRecord`
            Object representing the collection, including its type and ID.
            If ``type == CollectionType.RUN``, this will be a `RunRecord`
            instance.

        Raises
        ------
        TransactionInterruption
            Raised if this operation is invoked within a `Database.transaction`
            context.

        Notes
        -----
        Concurrent registrations of the same collection should be safe; nothing
        should happen if the types are consistent, and integrity errors due to
        inconsistent types should happen before any database changes are made
        """
        raise NotImplementedError()

    @abstractmethod
    def find(self, name: str) -> Optional[CollectionRecord]:
        """Return the collection record associated with the given name.

        Parameters
        ----------
        name : `str`
            Name of the collection.

        Returns
        -------
        record : `CollectionRecord` or `None`
            Object representing the collection, including its type and ID.
            If ``record/type == CollectionType.RUN``, this will be a
            `RunRecord` instance.  `None` if no collection with the given
            name exists in this layer.

        Note
        ----
        Collections registered by another client of the same layer since the
        last call to `initialize` or `refresh` may not be found.
        """
        raise NotImplementedError()

    @abstractmethod
    def get(self, id: int) -> Optional[CollectionRecord]:
        """Return the collection record associated with the given ID.

        Parameters
        ----------
        id : `int`
            Internal primary key value for the collection.

        Returns
        -------
        record : `CollectionRecord` or `None`
            Object representing the collection, including its type and name.
            If ``record/type == CollectionType.RUN``, this will be a
            `RunRecord` instance.  `None` if no collection with the given
            ID exists in this layer.

        Note
        ----
        Collections registered by another client of the same layer since the
        last call to `initialize` or `refresh` may not be found.
        """
        raise NotImplementedError()
