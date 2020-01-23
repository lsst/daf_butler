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

__all__ = [
    "CollectionManager",
    "CollectionRecord",
    "MissingCollectionError",
    "RunRecord",
]

from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Any,
    Iterator,
    Optional,
    TYPE_CHECKING,
)

from ...core import ddl
from ...core.timespan import Timespan
from .._collectionType import CollectionType

if TYPE_CHECKING:
    from ..wildcards import CategorizedWildcard
    from .database import Database, StaticTablesContext


class MissingCollectionError(Exception):
    """Exception raised when an operation attempts to use a collection that
    does not exist.
    """


class CollectionRecord(ABC):
    """A struct used to represent a collection in internal `Registry` APIs.

    User-facing code should always just use a `str` to represent collections.

    Parameters
    ----------
    name : `str`
        Name of the collection.
    type : `CollectionType`
        Enumeration value describing the type of the collection.
    """
    def __init__(self, name: str, type: CollectionType):
        self.name = name
        self.type = type

    @property
    @abstractmethod
    def key(self) -> Any:
        """The primary/foreign key value for this collection.
        """
        raise NotImplementedError()

    name: str
    """Name of the collection (`str`).
    """

    type: CollectionType
    """Enumeration value describing the type of the collection
    (`CollectionType`).
    """


class RunRecord(CollectionRecord):
    """A subclass of `CollectionRecord` that adds execution information and
    an interface for updating it.
    """

    @abstractmethod
    def update(self, host: Optional[str] = None, timespan: Optional[Timespan[Optional[datetime]]] = None):
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
    `Registry`.

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
    def initialize(cls, db: Database, context: StaticTablesContext) -> CollectionManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
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
    def addCollectionForeignKey(cls, tableSpec: ddl.TableSpec, *, prefix: str = "collection",
                                onDelete: Optional[str] = None, **kwds: Any) -> ddl.FieldSpec:
        """Add a foreign key (field and constraint) referencing the collection
        table.

        Parameters
        ----------
        tableSpec : `ddl.TableSpec`
            Specification for the table that should reference the collection
            table.  Will be modified in place.
        prefix: `str`, optional
            A name to use for the prefix of the new field; the full name may
            have a suffix (and is given in the returned `ddl.FieldSpec`).
        onDelete: `str`, optional
            One of "CASCADE" or "SET NULL", indicating what should happen to
            the referencing row if the collection row is deleted.  `None`
            indicates that this should be an integrity error.
        **kwds
            Additional keyword arguments are forwarded to the `ddl.FieldSpec`
            constructor (only the ``name`` and ``dtype`` arguments are
            otherwise provided).

        Returns
        -------
        fieldSpec : `ddl.FieldSpec`
            Specification for the field being added.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def addRunForeignKey(cls, tableSpec: ddl.TableSpec, *, prefix: str = "run",
                         onDelete: Optional[str] = None, **kwds: Any) -> ddl.FieldSpec:
        """Add a foreign key (field and constraint) referencing the run
        table.

        Parameters
        ----------
        tableSpec : `ddl.TableSpec`
            Specification for the table that should reference the run table.
            Will be modified in place.
        prefix: `str`, optional
            A name to use for the prefix of the new field; the full name may
            have a suffix (and is given in the returned `ddl.FieldSpec`).
        onDelete: `str`, optional
            One of "CASCADE" or "SET NULL", indicating what should happen to
            the referencing row if the collection row is deleted.  `None`
            indicates that this should be an integrity error.
        **kwds
            Additional keyword arguments are forwarded to the `ddl.FieldSpec`
            constructor (only the ``name`` and ``dtype`` arguments are
            otherwise provided).

        Returns
        -------
        fieldSpec : `ddl.FieldSpec`
            Specification for the field being added.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def getCollectionForeignKeyName(cls, prefix: str = "collection") -> str:
        """Return the name of the field added by `addCollectionForeignKey`
        if called with the same prefix.

        Parameters
        ----------
        prefix : `str`
            A name to use for the prefix of the new field; the full name may
            have a suffix.

        Returns
        -------
        name : `str`
            The field name.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def getRunForeignKeyName(cls, prefix: str = "run") -> str:
        """Return the name of the field added by `addRunForeignKey`
        if called with the same prefix.

        Parameters
        ----------
        prefix : `str`
            A name to use for the prefix of the new field; the full name may
            have a suffix.

        Returns
        -------
        name : `str`
            The field name.
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
            If ``type is CollectionType.RUN``, this will be a `RunRecord`
            instance.

        Raises
        ------
        TransactionInterruption
            Raised if this operation is invoked within a `Database.transaction`
            context.
        DatabaseConflictError
            Raised if a collection with this name but a different type already
            exists.

        Notes
        -----
        Concurrent registrations of the same collection should be safe; nothing
        should happen if the types are consistent, and integrity errors due to
        inconsistent types should happen before any database changes are made.
        """
        raise NotImplementedError()

    @abstractmethod
    def find(self, name: str) -> CollectionRecord:
        """Return the collection record associated with the given name.

        Parameters
        ----------
        name : `str`
            Name of the collection.

        Returns
        -------
        record : `CollectionRecord`
            Object representing the collection, including its type and ID.
            If ``record.type is CollectionType.RUN``, this will be a
            `RunRecord` instance.

        Raises
        ------
        MissingCollectionError
            Raised if the given collection does not exist.

        Notes
        -----
        Collections registered by another client of the same layer since the
        last call to `initialize` or `refresh` may not be found.
        """
        raise NotImplementedError()

    @abstractmethod
    def __getitem__(self, key: Any) -> CollectionRecord:
        """Return the collection record associated with the given
        primary/foreign key value.

        Parameters
        ----------
        key
            Internal primary key value for the collection.

        Returns
        -------
        record : `CollectionRecord`
            Object representing the collection, including its type and name.
            If ``record.type is CollectionType.RUN``, this will be a
            `RunRecord` instance.

        Raises
        ------
        MissingCollectionError
            Raised if no collection with this key exists.

        Notes
        -----
        Collections registered by another client of the same layer since the
        last call to `initialize` or `refresh` may not be found.
        """
        raise NotImplementedError()

    @abstractmethod
    def query(self, wildcard: Optional[CategorizedWildcard] = None) -> Iterator[CollectionRecord]:
        """Iterate over the collections whose names match an expression.

        Parameters
        ----------
        wildcard : `CategorizedWildcard`, optional
            Preprocessed version of a wildcard expression.  If `None`, all
            collections are returned.

        Yields
        ------
        record : `CollectionRecord`
            A collection matching the given expression.
        """
        raise NotImplementedError()
