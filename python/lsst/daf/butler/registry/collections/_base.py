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

import itertools
from abc import abstractmethod
from collections import namedtuple
from collections.abc import Iterable, Iterator, Set
from typing import TYPE_CHECKING, Any, TypeVar, cast

import sqlalchemy

from ..._timespan import TimespanDatabaseRepresentation
from .._collection_type import CollectionType
from .._exceptions import MissingCollectionError
from ..interfaces import ChainedCollectionRecord, CollectionManager, CollectionRecord, RunRecord, VersionTuple
from ..wildcards import CollectionWildcard

if TYPE_CHECKING:
    from .._caching_context import CachingContext
    from ..interfaces import Database, DimensionRecordStorageManager


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


CollectionTablesTuple = namedtuple("CollectionTablesTuple", ["collection", "run", "collection_chain"])


def makeRunTableSpec(
    collectionIdName: str, collectionIdType: type, TimespanReprClass: type[TimespanDatabaseRepresentation]
) -> ddl.TableSpec:
    """Define specification for "run" table.

    Parameters
    ----------
    collectionIdName : `str`
        Name of the column in collections table that identifies it (PK).
    collectionIdType
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
    collectionIdType
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
    dimensions : `DimensionRecordStorageManager`
        Manager object for the dimensions in this `Registry`.

    Notes
    -----
    Implementation uses "aggressive" pre-fetching and caching of the records
    in memory. Memory cache is synchronized from database when `refresh`
    method is called.
    """

    def __init__(
        self,
        db: Database,
        tables: CollectionTablesTuple,
        collectionIdName: str,
        *,
        dimensions: DimensionRecordStorageManager,
        caching_context: CachingContext,
        registry_schema_version: VersionTuple | None = None,
    ):
        super().__init__(registry_schema_version=registry_schema_version)
        self._db = db
        self._tables = tables
        self._collectionIdName = collectionIdName
        self._dimensions = dimensions
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
        self, chain: ChainedCollectionRecord[K], children: Iterable[str], flatten: bool = False
    ) -> ChainedCollectionRecord[K]:
        # Docstring inherited from CollectionManager.
        children_as_wildcard = CollectionWildcard.from_names(children)
        for record in self.resolve_wildcard(
            children_as_wildcard,
            flatten_chains=True,
            include_chains=True,
            collection_types={CollectionType.CHAINED},
        ):
            if record == chain:
                raise ValueError(f"Cycle in collection chaining when defining '{chain.name}'.")
        if flatten:
            children = tuple(
                record.name for record in self.resolve_wildcard(children_as_wildcard, flatten_chains=True)
            )

        rows = []
        position = itertools.count()
        names = []
        for child in self.resolve_wildcard(CollectionWildcard.from_names(children), flatten_chains=False):
            rows.append(
                {
                    "parent": chain.key,
                    "child": child.key,
                    "position": next(position),
                }
            )
            names.append(child.name)
        with self._db.transaction():
            self._db.delete(self._tables.collection_chain, ["parent"], {"parent": chain.key})
            self._db.insert(self._tables.collection_chain, *rows)

        record = ChainedCollectionRecord[K](chain.key, chain.name, children=tuple(names))
        self._addCachedRecord(record)
        return record
