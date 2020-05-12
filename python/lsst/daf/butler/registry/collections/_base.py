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

__all__ = []

from abc import abstractmethod
import astropy.time
import itertools
from typing import (
    Any,
    Iterator,
    NamedTuple,
    Optional,
    TYPE_CHECKING,
)

import sqlalchemy

from ...core import ddl
from ...core.timespan import Timespan, TIMESPAN_FIELD_SPECS
from .._collectionType import CollectionType
from ..interfaces import (
    ChainedCollectionRecord,
    CollectionManager,
    CollectionRecord,
    MissingCollectionError,
    RunRecord,
)
from ..wildcards import CollectionSearch

if TYPE_CHECKING:
    from .database import Database


def _makeCollectionForeignKey(sourceColumnName: str, collectionIdName: str, **kwargs) -> ddl.ForeignKeySpec:
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
    return ddl.ForeignKeySpec("collection", source=(sourceColumnName,), target=(collectionIdName,),
                              **kwargs)


def makeRunTableSpec(collectionIdName: str, collectionIdType: type):
    """Define specification for "run" table.

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
        Specification for run table.

    Notes
    -----
    Assumption here and in the code below is that the name of the identifying
    column is the same in both collections and run tables. The names of
    non-identifying columns containing run metadata are fixed.
    """
    return ddl.TableSpec(
        fields=[
            ddl.FieldSpec(collectionIdName, dtype=collectionIdType, primaryKey=True),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            ddl.FieldSpec("host", dtype=sqlalchemy.String, length=128),
        ],
        foreignKeys=[
            _makeCollectionForeignKey(collectionIdName, collectionIdName, onDelete="CASCADE"),
        ],
    )


def makeCollectionChainTableSpec(collectionIdName: str, collectionIdType: type):
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
            ddl.FieldSpec("dataset_type_name", dtype=sqlalchemy.String, length=128, nullable=True),
        ],
        foreignKeys=[
            _makeCollectionForeignKey("parent", collectionIdName, onDelete="CASCADE"),
            _makeCollectionForeignKey("child", collectionIdName),
        ],
    )


class DefaultRunRecord(RunRecord):
    """Default `RunRecord` implementation.

    This method assumes the same run table definition as produced by
    `makeRunTableSpec` method. The only non-fixed name in the schema
    is the PK column name, this needs to be passed in a constructor.

    Parameters
    ----------
    db : `Database`
        Registry database.
    key
        Unique collection ID, can be the same as ``name`` if ``name`` is used
        for identification. Usually this is an integer or string, but can be
        other database-specific type.
    name : `str`
        Run collection name.
    table : `sqlalchemy.schema.Table`
        Table for run records.
    idColumnName : `str`
        Name of the identifying column in run table.
    host : `str`, optional
        Name of the host where run was produced.
    timespan : `Timespan`, optional
        Timespan for this run.
    """
    def __init__(self, db: Database, key: Any, name: str, *, table: sqlalchemy.schema.Table,
                 idColumnName: str, host: Optional[str] = None,
                 timespan: Optional[Timespan[astropy.time.Time]] = None):
        super().__init__(key=key, name=name, type=CollectionType.RUN)
        self._db = db
        self._table = table
        self._host = host
        if timespan is None:
            timespan = Timespan(begin=None, end=None)
        self._timespan = timespan
        self._idName = idColumnName

    def update(self, host: Optional[str] = None, timespan: Optional[Timespan[astropy.time.Time]] = None):
        # Docstring inherited from RunRecord.
        if timespan is None:
            timespan = Timespan(begin=None, end=None)
        row = {
            self._idName: self.key,
            TIMESPAN_FIELD_SPECS.begin.name: timespan.begin,
            TIMESPAN_FIELD_SPECS.end.name: timespan.end,
            "host": host
        }
        count = self._db.update(self._table, {self._idName: self.key}, row)
        if count != 1:
            raise RuntimeError(f"Run update affected {count} records; expected exactly one.")
        self._host = host
        self._timespan = timespan

    @property
    def host(self) -> Optional[str]:
        # Docstring inherited from RunRecord.
        return self._host

    @property
    def timespan(self) -> Timespan[astropy.time.Time]:
        # Docstring inherited from RunRecord.
        return self._timespan


class DefaultChainedCollectionRecord(ChainedCollectionRecord):
    """Default `ChainedCollectionRecord` implementation.

    This method assumes the same chain table definition as produced by
    `makeCollectionChainTableSpec` method. All column names in the table are
    fixed and hard-coded in the methods.

    Parameters
    ----------
    db : `Database`
        Registry database.
    key
        Unique collection ID, can be the same as ``name`` if ``name`` is used
        for identification. Usually this is an integer or string, but can be
        other database-specific type.
    name : `str`
        Collection name.
    table : `sqlalchemy.schema.Table`
        Table for chain relationship records.
    """
    def __init__(self, db: Database, key: Any, name: str, *, table: sqlalchemy.schema.Table):
        super().__init__(key=key, name=name)
        self._db = db
        self._table = table

    def _update(self, manager: CollectionManager, children: CollectionSearch):
        # Docstring inherited from ChainedCollectionRecord.
        rows = []
        position = itertools.count()
        for child, restriction in children.iter(manager, withRestrictions=True, flattenChains=False):
            if restriction.names is ...:
                rows.append({"parent": self.key, "child": child.key,
                             "position": next(position), "dataset_type_name": None})
            else:
                for name in restriction.names:
                    rows.append({"parent": self.key, "child": child.key,
                                 "position": next(position), "dataset_type_name": name})
        with self._db.transaction():
            self._db.delete(self._table, ["parent"], {"parent": self.key})
            self._db.insert(self._table, *rows)

    def _load(self, manager: CollectionManager) -> CollectionSearch:
        # Docstring inherited from ChainedCollectionRecord.
        sql = sqlalchemy.sql.select(
            [self._table.columns.child, self._table.columns.dataset_type_name]
        ).select_from(
            self._table
        ).where(
            self._table.columns.parent == self.key
        ).order_by(
            self._table.columns.position
        )
        # It's fine to have consecutive rows with the same collection name
        # and different dataset type names - CollectionSearch will group those
        # up for us.
        children = []
        for row in self._db.query(sql):
            key = row[self._table.columns.child]
            restriction = row[self._table.columns.dataset_type_name]
            if not restriction:
                restriction = ...  # we store ... as "" in the database
            record = manager[key]
            children.append((record.name, restriction))
        return CollectionSearch.fromExpression(children)


class DefaultCollectionManager(CollectionManager):
    """Default `CollectionManager` implementation.

    This implementation uses record classes defined in this module and is
    based on the same assumptions about schema outlined in the record classes.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    tables : `NamedTuple`
        Named tuple of SQLAlchemy table objects.
    collectionIdName : `str`
        Name of the column in collections table that identifies it (PK).

    Notes
    -----
    Implementation uses "aggressive" pre-fetching and caching of the records
    in memory. Memory cache is synchronized from database when `refresh`
    method is called.
    """
    def __init__(self, db: Database, tables: NamedTuple[sqlalchemy.schema.Table, ...],
                 collectionIdName: str):
        self._db = db
        self._tables = tables
        self._collectionIdName = collectionIdName
        self._records = {}  # indexed by record ID

    def refresh(self):
        # Docstring inherited from CollectionManager.
        sql = sqlalchemy.sql.select(
            self._tables.collection.columns + self._tables.run.columns
        ).select_from(
            self._tables.collection.join(self._tables.run, isouter=True)
        )
        # Put found records into a temporary instead of updating self._records
        # in place, for exception safety.
        records = []
        chains = []
        for row in self._db.query(sql).fetchall():
            collection_id = row[self._tables.collection.columns[self._collectionIdName]]
            name = row[self._tables.collection.columns.name]
            type = CollectionType(row["type"])
            if type is CollectionType.RUN:
                record = DefaultRunRecord(
                    key=collection_id,
                    name=name,
                    db=self._db,
                    table=self._tables.run,
                    idColumnName=self._collectionIdName,
                    host=row[self._tables.run.columns.host],
                    timespan=Timespan(
                        begin=row[self._tables.run.columns[TIMESPAN_FIELD_SPECS.begin.name]],
                        end=row[self._tables.run.columns[TIMESPAN_FIELD_SPECS.end.name]],
                    )
                )
            elif type is CollectionType.CHAINED:
                record = DefaultChainedCollectionRecord(db=self._db,
                                                        key=collection_id,
                                                        table=self._tables.collection_chain,
                                                        name=name)
                chains.append(record)
            else:
                record = CollectionRecord(key=collection_id, name=name, type=type)
            records.append(record)
        self._setRecordCache(records)
        for chain in chains:
            chain.refresh(self)

    def register(self, name: str, type: CollectionType) -> CollectionRecord:
        # Docstring inherited from CollectionManager.
        record = self._getByName(name)
        if record is None:
            row, _ = self._db.sync(
                self._tables.collection,
                keys={"name": name},
                compared={"type": int(type)},
                returning=[self._collectionIdName],
            )
            collection_id = row[self._collectionIdName]
            if type is CollectionType.RUN:
                row, _ = self._db.sync(
                    self._tables.run,
                    keys={self._collectionIdName: collection_id},
                    returning={"host", TIMESPAN_FIELD_SPECS.begin.name, TIMESPAN_FIELD_SPECS.end.name},
                )
                record = DefaultRunRecord(
                    db=self._db,
                    key=collection_id,
                    name=name,
                    table=self._tables.run,
                    idColumnName=self._collectionIdName,
                    host=row["host"],
                    timespan=Timespan(
                        row[TIMESPAN_FIELD_SPECS.begin.name],
                        row[TIMESPAN_FIELD_SPECS.end.name]
                    ),
                )
            elif type is CollectionType.CHAINED:
                record = DefaultChainedCollectionRecord(db=self._db, key=collection_id, name=name,
                                                        table=self._tables.collection_chain)
            else:
                record = CollectionRecord(key=collection_id, name=name, type=type)
            self._addCachedRecord(record)
        return record

    def remove(self, name: str):
        # Docstring inherited from CollectionManager.
        record = self._getByName(name)
        if record is None:
            raise MissingCollectionError(f"No collection with name '{name}' found.")
        # This may raise
        self._db.delete(self._tables.collection, [self._collectionIdName],
                        {self._collectionIdName: record.key})
        self._removeCachedRecord(record)

    def find(self, name: str) -> CollectionRecord:
        # Docstring inherited from CollectionManager.
        result = self._getByName(name)
        if result is None:
            raise MissingCollectionError(f"No collection with name '{name}' found.")
        return result

    def __getitem__(self, key: Any) -> Optional[CollectionRecord]:
        # Docstring inherited from CollectionManager.
        try:
            return self._records[key]
        except KeyError as err:
            raise MissingCollectionError(f"Collection with key '{key}' not found.") from err

    def __iter__(self) -> Iterator[CollectionRecord]:
        yield from self._records.values()

    def _setRecordCache(self, records: Iterator[CollectionRecord]):
        """Set internal record cache to contain given records,
        old cached records will be removed.
        """
        self._records = {}
        for record in records:
            self._records[record.key] = record

    def _addCachedRecord(self, record: CollectionRecord):
        """Add single record to cache.
        """
        self._records[record.key] = record

    def _removeCachedRecord(self, record: CollectionRecord):
        """Remove single record from cache.
        """
        del self._records[record.key]

    @abstractmethod
    def _getByName(self, name: str):
        """Find collection record given collection name.
        """
        raise NotImplementedError()
