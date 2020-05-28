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

__all__ = ()

from abc import abstractmethod
import astropy.time
from collections import namedtuple
import itertools
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TYPE_CHECKING,
    Union,
)

import sqlalchemy

from ...core import ddl
from ...core.timespan import Timespan, TIMESPAN_FIELD_SPECS
from .._collectionType import CollectionType
from ..interfaces import (
    ChainedCollectionRecord,
    CollectionKey,
    CollectionManager,
    CollectionRecord,
    MissingCollectionError,
    RunRecord,
)
from ..wildcards import CollectionSearch, Ellipsis

if TYPE_CHECKING:
    from ..interfaces import Database


CollectionTablesTuple = namedtuple("CollectionTablesTuple", ["collection", "run", "collection_chain"])


def makeRunTableSpec(manager: Union[CollectionManager, Type[CollectionManager]]) -> ddl.TableSpec:
    """Define specification for "run" table.

    Parameters
    ----------
    manager : `CollectionManager` instance or subclass
        Manager object that can be used to add a foreign key to the collections
        table.

    Returns
    -------
    spec : `ddl.TableSpec`
        Specification for run table.

    Notes
    -----
    Assumption here and in the code below is that the names of the identifying
    columns are the same in both the collection and run tables. The names of
    non-identifying columns containing run metadata are fixed.
    """
    spec = ddl.TableSpec(
        fields=[
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            ddl.FieldSpec("host", dtype=sqlalchemy.String, length=128),
        ],
    )
    manager.addCollectionForeignKeys(spec, onDelete="CASCADE", primaryKey=True)
    return spec


def makeCollectionChainTableSpec(manager: Union[CollectionManager, Type[CollectionManager]]) -> ddl.TableSpec:
    """Define specification for "collection_chain" table.

    Parameters
    ----------
    manager : `CollectionManager` instance or subclass
        Manager object that can be used to add a foreign key to the collections
        table.

    Returns
    -------
    spec : `ddl.TableSpec`
        Specification for collection chain table.

    Notes
    -----
    Collection chain is simply an ordered one-to-many relation between
    collections.  Foreign keys to the collection table are prefixed with
    "parent_" and "suffix_" (with suffixes set by ``manager``).
    """
    spec = ddl.TableSpec(
        fields=[
            ddl.FieldSpec("position", dtype=sqlalchemy.SmallInteger, primaryKey=True),
            ddl.FieldSpec("dataset_type_name", dtype=sqlalchemy.String, length=128, nullable=True),
        ],
    )
    manager.addCollectionForeignKeys(spec, prefix="parent_", onDelete="CASCADE", primaryKey=True)
    manager.addCollectionForeignKeys(spec, prefix="child_")
    return spec


class DefaultRunRecord(RunRecord):
    """Default `RunRecord` implementation.

    This method assumes the same run table definition as produced by
    `makeRunTableSpec` method. The only non-fixed name in the schema
    is the PK column name, this needs to be passed in a constructor.

    Parameters
    ----------
    db : `Database`
        Registry database.
    key : `tuple`
        Unique collection identifier; the Python equivalent of a database
        primary key value.  Must be a tuple, even if the key is not compound
        in the database.
    name : `str`
        Run collection name.
    table : `sqlalchemy.schema.Table`
        Table for run records.
    keyColumnNames : `tuple` [ `str` ]
        Name of the identifying columns in the run table.
    host : `str`, optional
        Name of the host where run was produced.
    timespan : `Timespan`, optional
        Timespan for this run.
    """
    def __init__(self, db: Database, key: CollectionKey, name: str, *, table: sqlalchemy.schema.Table,
                 keyColumnNames: Tuple[str, ...], host: Optional[str] = None,
                 timespan: Optional[Timespan[astropy.time.Time]] = None):
        super().__init__(key=key, name=name, type=CollectionType.RUN)
        self._db = db
        self._table = table
        self._host = host
        if timespan is None:
            timespan = Timespan(begin=None, end=None)
        self._timespan = timespan
        self._keyColumnNames = keyColumnNames

    def update(self, host: Optional[str] = None,
               timespan: Optional[Timespan[astropy.time.Time]] = None) -> None:
        # Docstring inherited from RunRecord.
        if timespan is None:
            timespan = Timespan(begin=None, end=None)
        row = dict(zip(self._keyColumnNames, self.key))
        row.update({
            TIMESPAN_FIELD_SPECS.begin.name: timespan.begin,
            TIMESPAN_FIELD_SPECS.end.name: timespan.end,
            "host": host
        })
        count = self._db.update(self._table, self._keyColumnNames, row)
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
    key : `tuple`
        Unique collection identifier; the Python equivalent of a database
        primary key value.  Must be a tuple, even if the key is not compound
        in the database.
    name : `str`
        Collection name.
    table : `sqlalchemy.schema.Table`
        Table for chain relationship records.
    keyColumnNames : `tuple` [ `str` ]
        Suffixes for the names of the collection table key columns; combining
        these with "parent_" and "child_" prefixes yield the names of columns
        in the table produced by `makeCollectionChainTableSpec`.
    """
    def __init__(self, db: Database, key: CollectionKey, name: str, *,
                 keyColumnNames: Tuple[str, ...],
                 table: sqlalchemy.schema.Table):
        super().__init__(key=key, name=name)
        self._db = db
        self._table = table
        self._keyColumnNames = keyColumnNames

    def _update(self, manager: CollectionManager, children: CollectionSearch) -> None:
        # Docstring inherited from ChainedCollectionRecord.
        rows: List[Dict[str, Any]] = []
        baseParentRow = {
            f"parent_{suffix}": value for suffix, value in zip(self._keyColumnNames, self.key)
        }
        position = itertools.count()
        for child, restriction in children.iterPairs(manager, flattenChains=False):
            baseChildRow = dict(
                {f"child_{suffix}": value for suffix, value in zip(self._keyColumnNames, child.key)},
                **baseParentRow
            )
            if restriction.names is Ellipsis:
                rows.append({"position": next(position), "dataset_type_name": None, **baseChildRow})
            else:
                for name in restriction.names:
                    rows.append({"position": next(position), "dataset_type_name": name, **baseChildRow})
        with self._db.transaction():
            self._db.delete(self._table, baseParentRow.keys(), baseParentRow)
            self._db.insert(self._table, *rows)

    def _load(self, manager: CollectionManager) -> CollectionSearch:
        # Docstring inherited from ChainedCollectionRecord.
        columns = [self._table.columns[f"child_{k}"] for k in self._keyColumnNames]
        columns.append(self._table.columns.dataset_type_name)
        sql = sqlalchemy.sql.select(
            columns
        ).select_from(
            self._table
        ).where(
            sqlalchemy.sql.and_(*[self._table.columns[f"parent_{k}"] for k in self._keyColumnNames])
        ).order_by(
            self._table.columns.position
        )
        # It's fine to have consecutive rows with the same collection name
        # and different dataset type names - CollectionSearch will group those
        # up for us.
        children = []
        for row in self._db.query(sql):
            key = tuple(row[self._table.columns[f"child_{k}"]] for k in self._keyColumnNames)
            restriction = row[self._table.columns.dataset_type_name]
            if not restriction:
                restriction = ...  # we store ... as "" in the database
            record = manager[key]
            children.append((record.name, restriction))
        return CollectionSearch.fromExpression(children)


class DefaultCollectionManager(CollectionManager[CollectionKey]):
    """Default `CollectionManager` implementation.

    This implementation uses record classes defined in this module and is
    based on the same assumptions about schema outlined in the record classes.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    tables : `CollectionTablesTuple`
        Named tuple of SQLAlchemy table objects.
    keyColumnNames : `tuple` [ `str` ]
        Names of the columns in the collections table that identify it (PK).

    Notes
    -----
    Implementation uses "aggressive" pre-fetching and caching of the records
    in memory. Memory cache is synchronized from database when `refresh`
    method is called.
    """
    def __init__(self, db: Database, tables: CollectionTablesTuple, keyColumnNames: Tuple[str, ...]):
        self._db = db
        self._tables = tables
        self._keyColumnNames = keyColumnNames
        self._records: Dict[CollectionKey, CollectionRecord] = {}  # indexed by record ID

    def refresh(self) -> None:
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
            collectionKey = tuple(
                row[self._tables.collection.columns[k]] for k in self._keyColumnNames
            )
            name = row[self._tables.collection.columns.name]
            type = CollectionType(row["type"])
            record: CollectionRecord
            if type is CollectionType.RUN:
                record = DefaultRunRecord(
                    key=collectionKey,
                    name=name,
                    db=self._db,
                    table=self._tables.run,
                    keyColumnNames=self._keyColumnNames,
                    host=row[self._tables.run.columns.host],
                    timespan=Timespan(
                        begin=row[self._tables.run.columns[TIMESPAN_FIELD_SPECS.begin.name]],
                        end=row[self._tables.run.columns[TIMESPAN_FIELD_SPECS.end.name]],
                    )
                )
            elif type is CollectionType.CHAINED:
                record = DefaultChainedCollectionRecord(db=self._db,
                                                        key=collectionKey,
                                                        table=self._tables.collection_chain,
                                                        name=name,
                                                        keyColumnNames=self._keyColumnNames)
                chains.append(record)
            else:
                record = CollectionRecord(key=collectionKey, name=name, type=type)
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
                returning=self._keyColumnNames,
            )
            assert row is not None
            collectionKey = tuple(row[k] for k in self._keyColumnNames)
            if type is CollectionType.RUN:
                row, _ = self._db.sync(
                    self._tables.run,
                    keys=dict(zip(self._keyColumnNames, collectionKey)),
                    returning=["host", TIMESPAN_FIELD_SPECS.begin.name, TIMESPAN_FIELD_SPECS.end.name],
                )
                assert row is not None
                record = DefaultRunRecord(
                    db=self._db,
                    key=collectionKey,
                    name=name,
                    table=self._tables.run,
                    keyColumnNames=self._keyColumnNames,
                    host=row["host"],
                    timespan=Timespan(
                        row[TIMESPAN_FIELD_SPECS.begin.name],
                        row[TIMESPAN_FIELD_SPECS.end.name]
                    ),
                )
            elif type is CollectionType.CHAINED:
                record = DefaultChainedCollectionRecord(db=self._db, key=collectionKey, name=name,
                                                        table=self._tables.collection_chain,
                                                        keyColumnNames=self._keyColumnNames)
            else:
                record = CollectionRecord(key=collectionKey, name=name, type=type)
            self._addCachedRecord(record)
        return record

    def remove(self, name: str) -> None:
        # Docstring inherited from CollectionManager.
        record = self._getByName(name)
        if record is None:
            raise MissingCollectionError(f"No collection with name '{name}' found.")
        # This may raise
        self._db.delete(self._tables.collection, self._keyColumnNames,
                        dict(zip(self._keyColumnNames, record.key)))
        self._removeCachedRecord(record)

    def find(self, name: str) -> CollectionRecord:
        # Docstring inherited from CollectionManager.
        result = self._getByName(name)
        if result is None:
            raise MissingCollectionError(f"No collection with name '{name}' found.")
        return result

    def __getitem__(self, key: CollectionKey) -> CollectionRecord:
        # Docstring inherited from CollectionManager.
        try:
            return self._records[key]
        except KeyError as err:
            raise MissingCollectionError(f"Collection with key '{key}' not found.") from err

    def __iter__(self) -> Iterator[CollectionRecord]:
        yield from self._records.values()

    def _setRecordCache(self, records: Iterable[CollectionRecord]) -> None:
        """Set internal record cache to contain given records,
        old cached records will be removed.
        """
        self._records = {}
        for record in records:
            self._records[record.key] = record

    def _addCachedRecord(self, record: CollectionRecord) -> None:
        """Add single record to cache.
        """
        self._records[record.key] = record

    def _removeCachedRecord(self, record: CollectionRecord) -> None:
        """Remove single record from cache.
        """
        del self._records[record.key]

    @abstractmethod
    def _getByName(self, name: str) -> Optional[CollectionRecord]:
        """Find collection record given collection name.
        """
        raise NotImplementedError()
