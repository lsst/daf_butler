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
    "RunRecord",
]

from abc import abstractmethod
from collections import defaultdict
from collections.abc import Iterator, Set
from typing import TYPE_CHECKING, Any

from ...core import DimensionUniverse, Timespan, ddl
from .._collectionType import CollectionType
from ..wildcards import CollectionWildcard
from ._versioning import VersionedExtension

if TYPE_CHECKING:
    from ._database import Database, StaticTablesContext
    from ._dimensions import DimensionRecordStorageManager


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

    Notes
    -----
    The `name`, `key`, and `type` attributes set by the base class should be
    considered immutable by all users and derived classes (as these are used
    in the definition of equality and this is a hashable type).  Other
    attributes defined by subclasses may be mutable, as long as they do not
    participate in some subclass equality definition.
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

    def __eq__(self, other: Any) -> bool:
        try:
            return self.name == other.name and self.type == other.type and self.key == other.key
        except AttributeError:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self.name)

    def __repr__(self) -> str:
        return f"CollectionRecord(key={self.key!r}, name={self.name!r}, type={self.type!r})"

    def __str__(self) -> str:
        return self.name


class RunRecord(CollectionRecord):
    """A subclass of `CollectionRecord` that adds execution information and
    an interface for updating it.
    """

    @abstractmethod
    def update(self, host: str | None = None, timespan: Timespan | None = None) -> None:
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
    def host(self) -> str | None:
        """Return the name of the host or system on which this run was
        produced (`str` or `None`).
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def timespan(self) -> Timespan:
        """Begin and end timestamps for the period over which the run was
        produced.  `None`/``NULL`` values are interpreted as infinite
        bounds.
        """
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f"RunRecord(key={self.key!r}, name={self.name!r})"


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

    def __init__(self, key: Any, name: str, universe: DimensionUniverse):
        super().__init__(key=key, name=name, type=CollectionType.CHAINED)
        self._children: tuple[str, ...] = ()

    @property
    def children(self) -> tuple[str, ...]:
        """The ordered search path of child collections that define this chain
        (`tuple` [ `str` ]).
        """
        return self._children

    def update(self, manager: CollectionManager, children: tuple[str, ...], flatten: bool) -> None:
        """Redefine this chain to search the given child collections.

        This method should be used by all external code to set children.  It
        delegates to `_update`, which is what should be overridden by
        subclasses.

        Parameters
        ----------
        manager : `CollectionManager`
            The object that manages this records instance and all records
            instances that may appear as its children.
        children : `tuple` [ `str` ]
            A collection search path that should be resolved to set the child
            collections of this chain.
        flatten : `bool`
            If `True`, recursively flatten out any nested
            `~CollectionType.CHAINED` collections in ``children`` first.

        Raises
        ------
        ValueError
            Raised when the child collections contain a cycle.
        """
        children_as_wildcard = CollectionWildcard.from_names(children)
        for record in manager.resolve_wildcard(
            children_as_wildcard,
            flatten_chains=True,
            include_chains=True,
            collection_types={CollectionType.CHAINED},
        ):
            if record == self:
                raise ValueError(f"Cycle in collection chaining when defining '{self.name}'.")
        if flatten:
            children = tuple(
                record.name for record in manager.resolve_wildcard(children_as_wildcard, flatten_chains=True)
            )
        # Delegate to derived classes to do the database updates.
        self._update(manager, children)
        # Update the reverse mapping (from child to parents) in the manager,
        # by removing the old relationships and adding back in the new ones.
        for old_child in self._children:
            manager._parents_by_child[manager.find(old_child).key].discard(self.key)
        for new_child in children:
            manager._parents_by_child[manager.find(new_child).key].add(self.key)
        # Actually set this instances sequence of children.
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
        # Clear out the old reverse mapping (from child to parents).
        for child in self._children:
            manager._parents_by_child[manager.find(child).key].discard(self.key)
        self._children = self._load(manager)
        # Update the reverse mapping (from child to parents) in the manager.
        for child in self._children:
            manager._parents_by_child[manager.find(child).key].add(self.key)

    @abstractmethod
    def _update(self, manager: CollectionManager, children: tuple[str, ...]) -> None:
        """Protected implementation hook for `update`.

        This method should be implemented by subclasses to update the database
        to reflect the children given.  It should never be called by anything
        other than `update`, which should be used by all external code.

        Parameters
        ----------
        manager : `CollectionManager`
            The object that manages this records instance and all records
            instances that may appear as its children.
        children : `tuple` [ `str` ]
            A collection search path that should be resolved to set the child
            collections of this chain.  Guaranteed not to contain cycles.
        """
        raise NotImplementedError()

    @abstractmethod
    def _load(self, manager: CollectionManager) -> tuple[str, ...]:
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

        Returns
        -------
        children : `tuple` [ `str` ]
            The ordered sequence of collection names that defines the chained
            collection.  Guaranteed not to contain cycles.
        """
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f"ChainedCollectionRecord(key={self.key!r}, name={self.name!r}, children={self.children!r})"


class CollectionManager(VersionedExtension):
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

    def __init__(self) -> None:
        self._parents_by_child: defaultdict[Any, set[Any]] = defaultdict(set)

    @classmethod
    @abstractmethod
    def initialize(
        cls, db: Database, context: StaticTablesContext, *, dimensions: DimensionRecordStorageManager
    ) -> CollectionManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.
        dimensions : `DimensionRecordStorageManager`
            Manager object for the dimensions in this `Registry`.

        Returns
        -------
        manager : `CollectionManager`
            An instance of a concrete `CollectionManager` subclass.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def addCollectionForeignKey(
        cls,
        tableSpec: ddl.TableSpec,
        *,
        prefix: str = "collection",
        onDelete: str | None = None,
        constraint: bool = True,
        **kwargs: Any,
    ) -> ddl.FieldSpec:
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
    def addRunForeignKey(
        cls,
        tableSpec: ddl.TableSpec,
        *,
        prefix: str = "run",
        onDelete: str | None = None,
        constraint: bool = True,
        **kwargs: Any,
    ) -> ddl.FieldSpec:
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
    def register(
        self, name: str, type: CollectionType, doc: str | None = None
    ) -> tuple[CollectionRecord, bool]:
        """Ensure that a collection of the given name and type are present
        in the layer this manager is associated with.

        Parameters
        ----------
        name : `str`
            Name of the collection.
        type : `CollectionType`
            Enumeration value indicating the type of collection.
        doc : `str`, optional
            Documentation string for the collection.  Ignored if the collection
            already exists.

        Returns
        -------
        record : `CollectionRecord`
            Object representing the collection, including its type and ID.
            If ``type is CollectionType.RUN``, this will be a `RunRecord`
            instance.  If ``type is CollectionType.CHAIN``, this will be a
            `ChainedCollectionRecord` instance.
        registered : `bool`
            True if the collection was registered, `False` if it already
            existed.

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
    def resolve_wildcard(
        self,
        wildcard: CollectionWildcard,
        *,
        collection_types: Set[CollectionType] = CollectionType.all(),
        done: set[str] | None = None,
        flatten_chains: bool = True,
        include_chains: bool | None = None,
    ) -> list[CollectionRecord]:
        """Iterate over collection records that match a wildcard.

        Parameters
        ----------
        wildcard : `CollectionWildcard`
            Names and/or patterns for collections.
        collection_types : `collections.abc.Set` [ `CollectionType` ], optional
            If provided, only yield collections of these types.
        done : `set` [ `str` ], optional
            A `set` of collection names that will not be returned (presumably
            because they have already been returned in some higher-level logic)
            that will also be updated with the names of the collections
            returned.
        flatten_chains : `bool`, optional
            If `True` (default) recursively yield the child collections of
            `~CollectionType.CHAINED` collections.
        include_chains : `bool`, optional
            If `False`, return records for `~CollectionType.CHAINED`
            collections themselves.  The default is the opposite of
            ``flattenChains``: either return records for CHAINED collections or
            their children, but not both.

        Returns
        -------
        records : `list` [ `CollectionRecord` ]
            Matching collection records.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDocumentation(self, key: Any) -> str | None:
        """Retrieve the documentation string for a collection.

        Parameters
        ----------
        key
            Internal primary key value for the collection.

        Returns
        -------
        docs : `str` or `None`
            Docstring for the collection with the given key.
        """
        raise NotImplementedError()

    @abstractmethod
    def setDocumentation(self, key: Any, doc: str | None) -> None:
        """Set the documentation string for a collection.

        Parameters
        ----------
        key
            Internal primary key value for the collection.
        docs : `str`, optional
            Docstring for the collection with the given key.
        """
        raise NotImplementedError()

    def getParentChains(self, key: Any) -> Iterator[ChainedCollectionRecord]:
        """Find all CHAINED collections that directly contain the given
        collection.

        Parameters
        ----------
        key
            Internal primary key value for the collection.
        """
        for parent_key in self._parents_by_child[key]:
            result = self[parent_key]
            assert isinstance(result, ChainedCollectionRecord)
            yield result
