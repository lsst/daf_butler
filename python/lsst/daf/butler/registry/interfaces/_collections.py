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
    "ChainedCollectionRecord",
    "CollectionManager",
    "CollectionRecord",
    "MissingCollectionError",
    "RunRecord",
]

from abc import ABC, abstractmethod
from typing import (
    Any,
    Iterator,
    Optional,
    TYPE_CHECKING,
)

import astropy.time

from ...core import ddl, Timespan
from ..wildcards import CollectionSearch
from .._collectionType import CollectionType

if TYPE_CHECKING:
    from ._database import Database, StaticTablesContext


class MissingCollectionError(Exception):
    """Exception raised when an operation attempts to use a collection that
    does not exist.
    """


class CollectionRecord:
    """A struct used to represent a collection in internal `Registry` APIs.

    User-facing code should always just use a `str` to represent collections.

    Parameters
    ----------
    key
        Unique collection ID, can be the same as ``name`` if ``name`` is used
        for identification. Usually this is an integer or string, but can be
        other database-specific type.
    name : `str`
        Name of the collection.
    type : `CollectionType`
        Enumeration value describing the type of the collection.
    """
    def __init__(self, key: Any, name: str, type: CollectionType):
        self.key = key
        self.name = name
        self.type = type
        assert isinstance(self.type, CollectionType)

    name: str
    """Name of the collection (`str`).
    """

    key: Any
    """The primary/foreign key value for this collection.
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
    def update(self, host: Optional[str] = None,
               timespan: Optional[Timespan[astropy.time.Time]] = None) -> None:
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
    def timespan(self) -> Timespan[astropy.time.Time]:
        """Begin and end timestamps for the period over which the run was
        produced.  `None`/``NULL`` values are interpreted as infinite
        bounds.
        """
        raise NotImplementedError()


class ChainedCollectionRecord(CollectionRecord):
    """A subclass of `CollectionRecord` that adds the list of child collections
    in a ``CHAINED`` collection.

    Parameters
    ----------
    key
        Unique collection ID, can be the same as ``name`` if ``name`` is used
        for identification. Usually this is an integer or string, but can be
        other database-specific type.
    name : `str`
        Name of the collection.
    """

    def __init__(self, key: Any, name: str):
        super().__init__(key=key, name=name, type=CollectionType.CHAINED)
        self._children = CollectionSearch.fromExpression([])

    @property
    def children(self) -> CollectionSearch:
        """The ordered search path of child collections that define this chain
        (`CollectionSearch`).
        """
        return self._children

    def update(self, manager: CollectionManager, children: CollectionSearch) -> None:
        """Redefine this chain to search the given child collections.

        This method should be used by all external code to set children.  It
        delegates to `_update`, which is what should be overridden by
        subclasses.

        Parameters
        ----------
        manager : `CollectionManager`
            The object that manages this records instance and all records
            instances that may appear as its children.
        children : `CollectionSearch`
            A collection search path that should be resolved to set the child
            collections of this chain.

        Raises
        ------
        ValueError
            Raised when the child collections contain a cycle.
        """
        for record in children.iter(manager, flattenChains=True, includeChains=True,
                                    collectionType=CollectionType.CHAINED):
            if record == self:
                raise ValueError(f"Cycle in collection chaining when defining '{self.name}'.")
        self._update(manager, children)
        self._children = children

    def refresh(self, manager: CollectionManager) -> None:
        """Load children from the database, using the given manager to resolve
        collection primary key values into records.

        This method exists to ensure that all collections that may appear in a
        chain are known to the manager before any particular chain tries to
        retrieve their records from it.  `ChainedCollectionRecord` subclasses
        can rely on it being called sometime after their own ``__init__`` to
        finish construction.

        Parameters
        ----------
        manager : `CollectionManager`
            The object that manages this records instance and all records
            instances that may appear as its children.
        """
        self._children = self._load(manager)

    @abstractmethod
    def _update(self, manager: CollectionManager, children: CollectionSearch) -> None:
        """Protected implementation hook for setting the `children` property.

        This method should be implemented by subclasses to update the database
        to reflect the children given.  It should never be called by anything
        other than the `children` setter, which should be used by all external
        code.

        Parameters
        ----------
        manager : `CollectionManager`
            The object that manages this records instance and all records
            instances that may appear as its children.
        children : `CollectionSearch`
            A collection search path that should be resolved to set the child
            collections of this chain.  Guaranteed not to contain cycles.
        """
        raise NotImplementedError()

    @abstractmethod
    def _load(self, manager: CollectionManager) -> CollectionSearch:
        """Protected implementation hook for `refresh`.

        This method should be implemented by subclasses to retrieve the chain's
        child collections from the database and return them.  It should never
        be called by anything other than `refresh`, which should be used by all
        external code.

        Parameters
        ----------
        manager : `CollectionManager`
            The object that manages this records instance and all records
            instances that may appear as its children.
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
                                onDelete: Optional[str] = None,
                                constraint: bool = True,
                                **kwargs: Any) -> ddl.FieldSpec:
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
        constraint: `bool`, optional
            If `False` (`True` is default), add a field that can be joined to
            the collection primary key, but do not add a foreign key
            constraint.
        **kwargs
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
                         onDelete: Optional[str] = None,
                         constraint: bool = True,
                         **kwargs: Any) -> ddl.FieldSpec:
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
        constraint: `bool`, optional
            If `False` (`True` is default), add a field that can be joined to
            the run primary key, but do not add a foreign key constraint.
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
    def refresh(self) -> None:
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
            instance.  If ``type is CollectionType.CHAIN``, this will be a
            `ChainedCollectionRecord` instance.

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
    def remove(self, name: str) -> None:
        """Completely remove a collection.

        Any existing `CollectionRecord` objects that correspond to the removed
        collection are considered invalidated.

        Parameters
        ----------
        name : `str`
            Name of the collection to remove.

        Notes
        -----
        If this collection is referenced by foreign keys in tables managed by
        other objects, the ON DELETE clauses of those tables will be invoked.
        That will frequently delete many dependent rows automatically (via
        "CASCADE", but it may also cause this operation to fail (with rollback)
        unless dependent rows that do not have an ON DELETE clause are removed
        first.
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
            `RunRecord` instance.  If ``record.type is CollectionType.CHAIN``,
            this will be a `ChainedCollectionRecord` instance.

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
            `RunRecord` instance.  If ``record.type is CollectionType.CHAIN``,
            this will be a `ChainedCollectionRecord` instance.

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
    def __iter__(self) -> Iterator[CollectionRecord]:
        """Iterate over all collections.

        Yields
        ------
        record : `CollectionRecord`
            The record for a managed collection.
        """
        raise NotImplementedError()
