# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from ... import ddl

__all__ = ()

from abc import abstractmethod
from collections.abc import Callable, Iterable, Iterator, Set
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generic, Literal, NamedTuple, TypeVar, cast

import sqlalchemy

from ..._exceptions import CollectionCycleError, CollectionTypeError, MissingCollectionError
from ...timespan_database_representation import TimespanDatabaseRepresentation
from .._collection_type import CollectionType
from ..interfaces import ChainedCollectionRecord, CollectionManager, CollectionRecord, RunRecord, VersionTuple
from ..wildcards import CollectionWildcard

if TYPE_CHECKING:
    from .._caching_context import CachingContext
    from ..interfaces import Database


def _makeCollectionForeignKey(
    sourceColumnName: str, collectionIdName: str, **kwargs: Any
) -> ddl.ForeignKeySpec:
    """Define foreign key specification that refers to collections table.

    Parameters
    ----------
    sourceColumnName : `str`
        Name of the column in the referring table.
    collectionIdName : `str`
        Name of the column in collections table that identifies it (PK).
    **kwargs
        Additional keyword arguments passed directly to `ddl.ForeignKeySpec`.

    Returns
    -------
    spec : `ddl.ForeignKeySpec`
        Foreign key specification.

    Notes
    -----
    This method assumes fixed name ("collection") of a collections table.
    There is also a general assumption that collection primary key consists
    of a single column.
    """
    return ddl.ForeignKeySpec("collection", source=(sourceColumnName,), target=(collectionIdName,), **kwargs)


_T = TypeVar("_T")


class CollectionTablesTuple(NamedTuple, Generic[_T]):
    collection: _T
    run: _T
    collection_chain: _T


def makeRunTableSpec(
    collectionIdName: str, collectionIdType: type, TimespanReprClass: type[TimespanDatabaseRepresentation]
) -> ddl.TableSpec:
    """Define specification for "run" table.

    Parameters
    ----------
    collectionIdName : `str`
        Name of the column in collections table that identifies it (PK).
    collectionIdType : `type`
        Type of the PK column in the collections table, one of the
        `sqlalchemy` types.
    TimespanReprClass : `type` [ `TimespanDatabaseRepresentation` ]
        Subclass of `TimespanDatabaseRepresentation` that encapsulates how
        timespans are stored in this database.

    Returns
    -------
    spec : `ddl.TableSpec`
        Specification for run table.

    Notes
    -----
    Assumption here and in the code below is that the name of the identifying
    column is the same in both collections and run tables. The names of
    non-identifying columns containing run metadata are fixed.
    """
    result = ddl.TableSpec(
        fields=[
            ddl.FieldSpec(collectionIdName, dtype=collectionIdType, primaryKey=True),
            ddl.FieldSpec("host", dtype=sqlalchemy.String, length=128),
        ],
        foreignKeys=[
            _makeCollectionForeignKey(collectionIdName, collectionIdName, onDelete="CASCADE"),
        ],
    )
    for fieldSpec in TimespanReprClass.makeFieldSpecs(nullable=True):
        result.fields.add(fieldSpec)
    return result


def makeCollectionChainTableSpec(collectionIdName: str, collectionIdType: type) -> ddl.TableSpec:
    """Define specification for "collection_chain" table.

    Parameters
    ----------
    collectionIdName : `str`
        Name of the column in collections table that identifies it (PK).
    collectionIdType : `type`
        Type of the PK column in the collections table, one of the
        `sqlalchemy` types.

    Returns
    -------
    spec : `ddl.TableSpec`
        Specification for collection chain table.

    Notes
    -----
    Collection chain is simply an ordered one-to-many relation between
    collections. The names of the columns in the table are fixed and
    also hardcoded in the code below.
    """
    return ddl.TableSpec(
        fields=[
            ddl.FieldSpec("parent", dtype=collectionIdType, primaryKey=True),
            ddl.FieldSpec("position", dtype=sqlalchemy.SmallInteger, primaryKey=True),
            ddl.FieldSpec("child", dtype=collectionIdType, nullable=False),
        ],
        foreignKeys=[
            _makeCollectionForeignKey("parent", collectionIdName, onDelete="CASCADE"),
            _makeCollectionForeignKey("child", collectionIdName),
        ],
    )


K = TypeVar("K")


class DefaultCollectionManager(CollectionManager[K]):
    """Default `CollectionManager` implementation.

    This implementation uses record classes defined in this module and is
    based on the same assumptions about schema outlined in the record classes.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    tables : `CollectionTablesTuple`
        Named tuple of SQLAlchemy table objects.
    collectionIdName : `str`
        Name of the column in collections table that identifies it (PK).
    caching_context : `CachingContext`
        Caching context to use.
    registry_schema_version : `VersionTuple` or `None`, optional
        The version of the registry schema.

    Notes
    -----
    Implementation uses "aggressive" pre-fetching and caching of the records
    in memory. Memory cache is synchronized from database when `refresh`
    method is called.
    """

    def __init__(
        self,
        db: Database,
        tables: CollectionTablesTuple[sqlalchemy.Table],
        collectionIdName: str,
        *,
        caching_context: CachingContext,
        registry_schema_version: VersionTuple | None = None,
    ):
        super().__init__(registry_schema_version=registry_schema_version)
        self._db = db
        self._tables = tables
        self._collectionIdName = collectionIdName
        self._caching_context = caching_context

    def refresh(self) -> None:
        # Docstring inherited from CollectionManager.
        if self._caching_context.collection_records is not None:
            self._caching_context.collection_records.clear()

    def _fetch_all(self) -> list[CollectionRecord[K]]:
        """Retrieve all records into cache if not done so yet."""
        if self._caching_context.collection_records is not None:
            if self._caching_context.collection_records.full:
                return list(self._caching_context.collection_records.records())
        records = self._fetch_by_key(None)
        if self._caching_context.collection_records is not None:
            self._caching_context.collection_records.set(records, full=True)
        return records

    def register(
        self, name: str, type: CollectionType, doc: str | None = None
    ) -> tuple[CollectionRecord[K], bool]:
        # Docstring inherited from CollectionManager.
        registered = False
        record = self._getByName(name)
        if record is None:
            row, inserted_or_updated = self._db.sync(
                self._tables.collection,
                keys={"name": name},
                compared={"type": int(type)},
                extra={"doc": doc},
                returning=[self._collectionIdName],
            )
            assert isinstance(inserted_or_updated, bool)
            registered = inserted_or_updated
            assert row is not None
            collection_id = cast(K, row[self._collectionIdName])
            if type is CollectionType.RUN:
                TimespanReprClass = self._db.getTimespanRepresentation()
                row, _ = self._db.sync(
                    self._tables.run,
                    keys={self._collectionIdName: collection_id},
                    returning=("host",) + TimespanReprClass.getFieldNames(),
                )
                assert row is not None
                record = RunRecord[K](
                    key=collection_id,
                    name=name,
                    host=row["host"],
                    timespan=TimespanReprClass.extract(row),
                )
            elif type is CollectionType.CHAINED:
                record = ChainedCollectionRecord[K](
                    key=collection_id,
                    name=name,
                    children=[],
                )
            else:
                record = CollectionRecord[K](key=collection_id, name=name, type=type)
            self._addCachedRecord(record)
        return record, registered

    def remove(self, name: str) -> None:
        # Docstring inherited from CollectionManager.
        record = self._getByName(name)
        if record is None:
            raise MissingCollectionError(f"No collection with name '{name}' found.")
        # This may raise
        self._db.delete(
            self._tables.collection, [self._collectionIdName], {self._collectionIdName: record.key}
        )
        self._removeCachedRecord(record)

    def find(self, name: str) -> CollectionRecord[K]:
        # Docstring inherited from CollectionManager.
        result = self._getByName(name)
        if result is None:
            raise MissingCollectionError(f"No collection with name '{name}' found.")
        return result

    def _find_many(self, names: Iterable[str]) -> list[CollectionRecord[K]]:
        """Return multiple records given their names."""
        names = list(names)
        # To protect against potential races in cache updates.
        records: dict[str, CollectionRecord | None] = {}
        if self._caching_context.collection_records is not None:
            for name in names:
                records[name] = self._caching_context.collection_records.get_by_name(name)
            fetch_names = [name for name, record in records.items() if record is None]
        else:
            fetch_names = list(names)
            records = {name: None for name in fetch_names}
        if fetch_names:
            for record in self._fetch_by_name(fetch_names):
                records[record.name] = record
                self._addCachedRecord(record)
        missing_names = [name for name, record in records.items() if record is None]
        if len(missing_names) == 1:
            raise MissingCollectionError(f"No collection with name '{missing_names[0]}' found.")
        elif len(missing_names) > 1:
            raise MissingCollectionError(f"No collections with names '{' '.join(missing_names)}' found.")
        return [cast(CollectionRecord[K], records[name]) for name in names]

    def __getitem__(self, key: Any) -> CollectionRecord[K]:
        # Docstring inherited from CollectionManager.
        if self._caching_context.collection_records is not None:
            if (record := self._caching_context.collection_records.get_by_key(key)) is not None:
                return record
        if records := self._fetch_by_key([key]):
            record = records[0]
            if self._caching_context.collection_records is not None:
                self._caching_context.collection_records.add(record)
            return record
        else:
            raise MissingCollectionError(f"Collection with key '{key}' not found.")

    def resolve_wildcard(
        self,
        wildcard: CollectionWildcard,
        *,
        collection_types: Set[CollectionType] = CollectionType.all(),
        done: set[str] | None = None,
        flatten_chains: bool = True,
        include_chains: bool | None = None,
    ) -> list[CollectionRecord[K]]:
        # Docstring inherited
        if done is None:
            done = set()
        include_chains = include_chains if include_chains is not None else not flatten_chains

        def resolve_nested(record: CollectionRecord, done: set[str]) -> Iterator[CollectionRecord[K]]:
            if record.name in done:
                return
            if record.type in collection_types:
                done.add(record.name)
                if record.type is not CollectionType.CHAINED or include_chains:
                    yield record
            if flatten_chains and record.type is CollectionType.CHAINED:
                done.add(record.name)
                for child in self._find_many(cast(ChainedCollectionRecord[K], record).children):
                    # flake8 can't tell that we only delete this closure when
                    # we're totally done with it.
                    yield from resolve_nested(child, done)  # noqa: F821

        result: list[CollectionRecord[K]] = []

        if wildcard.patterns is ...:
            for record in self._fetch_all():
                result.extend(resolve_nested(record, done))
            del resolve_nested
            return result
        if wildcard.strings:
            for record in self._find_many(wildcard.strings):
                result.extend(resolve_nested(record, done))
        if wildcard.patterns:
            for record in self._fetch_all():
                if any(p.fullmatch(record.name) for p in wildcard.patterns):
                    result.extend(resolve_nested(record, done))
        del resolve_nested
        return result

    def getDocumentation(self, key: K) -> str | None:
        # Docstring inherited from CollectionManager.
        sql = (
            sqlalchemy.sql.select(self._tables.collection.columns.doc)
            .select_from(self._tables.collection)
            .where(self._tables.collection.columns[self._collectionIdName] == key)
        )
        with self._db.query(sql) as sql_result:
            return sql_result.scalar()

    def setDocumentation(self, key: K, doc: str | None) -> None:
        # Docstring inherited from CollectionManager.
        self._db.update(self._tables.collection, {self._collectionIdName: "key"}, {"key": key, "doc": doc})

    def _addCachedRecord(self, record: CollectionRecord[K]) -> None:
        """Add single record to cache."""
        if self._caching_context.collection_records is not None:
            self._caching_context.collection_records.add(record)

    def _removeCachedRecord(self, record: CollectionRecord[K]) -> None:
        """Remove single record from cache."""
        if self._caching_context.collection_records is not None:
            self._caching_context.collection_records.discard(record)

    def _getByName(self, name: str) -> CollectionRecord[K] | None:
        """Find collection record given collection name."""
        if self._caching_context.collection_records is not None:
            if (record := self._caching_context.collection_records.get_by_name(name)) is not None:
                return record
        records = self._fetch_by_name([name])
        for record in records:
            self._addCachedRecord(record)
        return records[0] if records else None

    @abstractmethod
    def _fetch_by_name(self, names: Iterable[str]) -> list[CollectionRecord[K]]:
        """Fetch collection record from database given its name."""
        raise NotImplementedError()

    @abstractmethod
    def _fetch_by_key(self, collection_ids: Iterable[K] | None) -> list[CollectionRecord[K]]:
        """Fetch collection record from database given its key, or fetch all
        collctions if argument is None.
        """
        raise NotImplementedError()

    def update_chain(
        self,
        parent_collection_name: str,
        child_collection_names: list[str],
        allow_use_in_caching_context: bool = False,
    ) -> None:
        with self._modify_collection_chain(
            parent_collection_name,
            child_collection_names,
            # update_chain is currently used in setCollectionChain, which is
            # called within caching contexts.  (At least in Butler.import_ and
            # possibly other places.)  So, unlike the other collection chain
            # modification methods, it has to update the collection cache.
            skip_caching_check=allow_use_in_caching_context,
        ) as c:
            self._db.delete(self._tables.collection_chain, ["parent"], {"parent": c.parent_key})
            self._block_for_concurrency_test()
            self._insert_collection_chain_rows(c.parent_key, 0, c.child_keys)

        names = [child.name for child in c.child_records]
        record = ChainedCollectionRecord[K](c.parent_key, parent_collection_name, children=tuple(names))
        self._addCachedRecord(record)

    def prepend_collection_chain(
        self, parent_collection_name: str, child_collection_names: list[str]
    ) -> None:
        self._add_to_collection_chain(
            parent_collection_name, child_collection_names, self._find_prepend_position
        )

    def extend_collection_chain(self, parent_collection_name: str, child_collection_names: list[str]) -> None:
        self._add_to_collection_chain(
            parent_collection_name, child_collection_names, self._find_extend_position
        )

    def _add_to_collection_chain(
        self,
        parent_collection_name: str,
        child_collection_names: list[str],
        position_func: Callable[[_CollectionChainModificationContext], int],
    ) -> None:
        with self._modify_collection_chain(parent_collection_name, child_collection_names) as c:
            # Remove any of the new children that are already in the
            # collection, so they move to a new position instead of being
            # duplicated.
            self._remove_collection_chain_rows(c.parent_key, c.child_keys)
            # Figure out where to insert the new children.
            starting_position = position_func(c)
            self._block_for_concurrency_test()
            self._insert_collection_chain_rows(c.parent_key, starting_position, c.child_keys)

    def remove_from_collection_chain(
        self, parent_collection_name: str, child_collection_names: list[str]
    ) -> None:
        with self._modify_collection_chain(
            parent_collection_name,
            child_collection_names,
            # Removing members from a chain can't create collection cycles
            skip_cycle_check=True,
        ) as c:
            self._remove_collection_chain_rows(c.parent_key, c.child_keys)

    @contextmanager
    def _modify_collection_chain(
        self,
        parent_collection_name: str,
        child_collection_names: list[str],
        *,
        skip_caching_check: bool = False,
        skip_cycle_check: bool = False,
    ) -> Iterator[_CollectionChainModificationContext[K]]:
        if (not skip_caching_check) and self._caching_context.is_enabled:
            # Avoid having cache-maintenance code around that is unlikely to
            # ever be used.
            raise RuntimeError("Chained collection modification not permitted with active caching context.")

        if not skip_cycle_check:
            self._sanity_check_collection_cycles(parent_collection_name, child_collection_names)

        # Look up the collection primary keys corresponding to the
        # user-provided list of child collection names.  Because there is no
        # locking for the child collections, it's possible for a concurrent
        # deletion of one of the children to cause a foreign key constraint
        # violation when we attempt to insert them in the collection chain
        # table later.
        child_records = self.resolve_wildcard(
            CollectionWildcard.from_names(child_collection_names), flatten_chains=False
        )
        child_keys = [child.key for child in child_records]

        with self._db.transaction():
            # Lock the parent collection to prevent concurrent updates to the
            # same collection chain.
            parent_key = self._find_and_lock_collection_chain(parent_collection_name)
            yield _CollectionChainModificationContext[K](
                parent_key=parent_key, child_keys=child_keys, child_records=child_records
            )

    def _sanity_check_collection_cycles(
        self, parent_collection_name: str, child_collection_names: list[str]
    ) -> None:
        """Raise an exception if any of the collections in the ``child_names``
        list have ``parent_name`` as a child, creating a collection cycle.

        This is only a sanity check, and does not guarantee that no collection
        cycles are possible.  Concurrent updates might allow collection cycles
        to be inserted.
        """
        for record in self.resolve_wildcard(
            CollectionWildcard.from_names(child_collection_names),
            flatten_chains=True,
            include_chains=True,
            collection_types={CollectionType.CHAINED},
        ):
            if record.name == parent_collection_name:
                raise CollectionCycleError(
                    f"Cycle in collection chaining when defining '{parent_collection_name}'."
                )

    def _insert_collection_chain_rows(
        self,
        parent_key: K,
        starting_position: int,
        child_keys: list[K],
    ) -> None:
        rows = [
            {
                "parent": parent_key,
                "child": child,
                "position": position,
            }
            for position, child in enumerate(child_keys, starting_position)
        ]

        # It's possible for the DB to raise an exception for the integers being
        # out of range here.  The position column is only a 16-bit number.
        # Even if there aren't an unreasonably large number of children in the
        # collection, a series of many deletes and insertions could cause the
        # space to become fragmented.
        #
        # If this ever actually happens, we should consider doing a migration
        # to increase the position column to a 32-bit number.
        # To fix it in the short term, you can re-write the collection chain to
        # defragment it by doing something like:
        # registry.setCollectionChain(
        #     parent,
        #     registry.getCollectionChain(parent)
        # )
        self._db.insert(self._tables.collection_chain, *rows)

    def _remove_collection_chain_rows(
        self,
        parent_key: K,
        child_keys: list[K],
    ) -> None:
        table = self._tables.collection_chain
        where = sqlalchemy.and_(table.c.parent == parent_key, table.c.child.in_(child_keys))
        self._db.deleteWhere(table, where)

    def _find_prepend_position(self, c: _CollectionChainModificationContext) -> int:
        """Return the position where children can be inserted to
        prepend them to a collection chain.
        """
        return self._find_position_in_collection_chain(c.parent_key, "begin") - len(c.child_keys)

    def _find_extend_position(self, c: _CollectionChainModificationContext) -> int:
        """Return the position where children can be inserted to append them to
        a collection chain.
        """
        return self._find_position_in_collection_chain(c.parent_key, "end") + 1

    def _find_position_in_collection_chain(self, chain_key: K, begin_or_end: Literal["begin", "end"]) -> int:
        """Return the lowest or highest numbered position in a collection
        chain, or 0 if the chain is empty.
        """
        table = self._tables.collection_chain

        func: sqlalchemy.Function
        match (begin_or_end):
            case "begin":
                func = sqlalchemy.func.min(table.c.position)
            case "end":
                func = sqlalchemy.func.max(table.c.position)

        query = sqlalchemy.select(func).where(table.c.parent == chain_key)
        with self._db.query(query) as cursor:
            position = cursor.scalar()

        if position is None:
            return 0

        return position

    def _find_and_lock_collection_chain(self, collection_name: str) -> K:
        """
        Take a row lock on the specified collection's row in the collections
        table, and return the collection's primary key.

        This lock is used to synchronize updates to collection chains.

        The locking strategy requires cooperation from everything modifying the
        collection chain table -- all operations that modify collection chains
        must obtain this lock first.  The database will NOT automatically
        prevent modification of tables based on this lock.  The only guarantee
        is that only one caller will be allowed to hold this lock for a given
        collection at a time.  Concurrent calls will block until the caller
        holding the lock has completed its transaction.

        Parameters
        ----------
        collection_name : `str`
            Name of the collection whose chain is being modified.

        Returns
        -------
        id : ``K``
            The primary key for the given collection.

        Raises
        ------
        MissingCollectionError
            If the specified collection is not in the database table.
        CollectionTypeError
            If the specified collection is not a chained collection.
        """
        assert self._db.isInTransaction(), (
            "Row locks are only held until the end of the current transaction,"
            " so it makes no sense to take a lock outside a transaction."
        )
        assert self._db.isWriteable(), "Collection row locks are only useful for write operations."

        query = self._select_pkey_by_name(collection_name).with_for_update()
        with self._db.query(query) as cursor:
            rows = cursor.all()

        if len(rows) == 0:
            raise MissingCollectionError(
                f"Parent collection {collection_name} not found when updating collection chain."
            )
        assert len(rows) == 1, "There should only be one entry for each collection in collection table."
        r = rows[0]._mapping
        if r["type"] != CollectionType.CHAINED:
            raise CollectionTypeError(f"Parent collection {collection_name} is not a chained collection.")
        return r["key"]

    @abstractmethod
    def _select_pkey_by_name(self, collection_name: str) -> sqlalchemy.Select:
        """Return a SQLAlchemy select statement that will return columns from
        the one row in the ``collection` table matching the given name.  The
        select statement includes two columns:

        - ``key`` : the primary key for the collection
        - ``type`` : the collection type
        """
        raise NotImplementedError()


class _CollectionChainModificationContext(NamedTuple, Generic[K]):
    parent_key: K
    child_keys: list[K]
    child_records: list[CollectionRecord[K]]
