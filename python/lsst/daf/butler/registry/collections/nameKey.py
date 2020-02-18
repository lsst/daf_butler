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

__all__ = ["AggressiveNameKeyCollectionManager"]

from collections import namedtuple
from datetime import datetime
from typing import (
    Any,
    Iterator,
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
    from .database import Database, StaticTablesContext


_TablesTuple = namedtuple("CollectionTablesTuple", ["collection", "run", "collection_chain"])

_TABLES_SPEC = _TablesTuple(
    collection=ddl.TableSpec(
        fields=[
            ddl.FieldSpec("name", dtype=sqlalchemy.String, length=64, primaryKey=True),
            ddl.FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
        ],
    ),
    run=ddl.TableSpec(
        fields=[
            ddl.FieldSpec("name", dtype=sqlalchemy.String, length=64, primaryKey=True),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            ddl.FieldSpec("host", dtype=sqlalchemy.String, length=128),
        ],
        foreignKeys=[
            ddl.ForeignKeySpec("collection", source=("name",), target=("name",), onDelete="CASCADE"),
        ],
    ),
    collection_chain=ddl.TableSpec(
        fields=[
            ddl.FieldSpec("parent", dtype=sqlalchemy.String, length=64, primaryKey=True),
            ddl.FieldSpec("index", dtype=sqlalchemy.SmallInteger, primaryKey=True),
            ddl.FieldSpec("child", dtype=sqlalchemy.String, length=64, nullable=False),
            ddl.FieldSpec("dataset_type_name", dtype=sqlalchemy.String, length=128, nullable=True),
        ],
        foreignKeys=[
            ddl.ForeignKeySpec("collection", source=("parent",), target=("name",), onDelete="CASCADE"),
            ddl.ForeignKeySpec("collection", source=("child",), target=("name",)),
        ],
    ),
)


class NameKeyCollectionRecord(CollectionRecord):
    """A `CollectionRecord` implementation that just uses the string name as
    the primary/foreign key for collections.
    """

    @property
    def key(self) -> str:
        # Docstring inherited from CollectionRecord.
        return self.name


class NameKeyRunRecord(RunRecord):
    """A `RunRecord` implementation that just uses the string name as the
    primary/foreign key for collections.
    """
    def __init__(self, db: Database, name: str, *, table: sqlalchemy.schema.Table,
                 host: Optional[str] = None, timespan: Optional[Timespan[Optional[datetime]]] = None):
        super().__init__(name=name, type=CollectionType.RUN)
        self._db = db
        self._table = table
        self._host = host
        if timespan is None:
            timespan = Timespan(begin=None, end=None)
        self._timespan = timespan

    def update(self, host: Optional[str] = None, timespan: Optional[Timespan[Optional[datetime]]] = None):
        # Docstring inherited from RunRecord.
        if timespan is None:
            timespan = Timespan(begin=None, end=None)
        row = {
            "name": self.name,
            TIMESPAN_FIELD_SPECS.begin.name: timespan.begin,
            TIMESPAN_FIELD_SPECS.end.name: timespan.end,
            "host": host,
        }
        count = self._db.update(self._table, {"name": self.name}, row)
        if count != 1:
            raise RuntimeError(f"Run update affected {count} records; expected exactly one.")
        self._host = host
        self._timespan = timespan

    @property
    def key(self) -> str:
        # Docstring inherited from CollectionRecord.
        return self.name

    @property
    def host(self) -> Optional[str]:
        # Docstring inherited from RunRecord.
        return self._host

    @property
    def timespan(self) -> Timespan[Optional[datetime]]:
        # Docstring inherited from RunRecord.
        return self._timespan


class NameKeyChainedCollectionRecord(ChainedCollectionRecord):
    """A `ChainedCollectionRecord` implementation that just uses the string
    name as the primary/foreign key for collections.
    """
    def __init__(self, db: Database, name: str, *, table: sqlalchemy.schema.Table):
        super().__init__(name=name)
        self._db = db
        self._table = table

    @property
    def key(self) -> str:
        # Docstring inherited from CollectionRecord.
        return self.name

    def _update(self, manager: CollectionManager, children: CollectionSearch):
        # Docstring inherited from ChainedCollectionRecord.
        rows = []
        i = 0
        for child, restriction in children.iter(manager, withRestrictions=True, flattenChains=False):
            if restriction.names is ...:
                rows.append({"parent": self.key, "child": child.key, "index": i,
                             "dataset_type_name": ""})
                i += 1
            else:
                for name in restriction.names:
                    rows.append({"parent": self.key, "child": child.key, "index": i,
                                 "dataset_type_name": name})
                    i += 1
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
            self._table.columns.index
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


class AggressiveNameKeyCollectionManager(CollectionManager):
    """A `CollectionManager` implementation that uses collection names for
    primary/foreign keys and aggressively loads all collection/run records in
    the database into memory.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    tables : `_TablesTuple`
        Named tuple of SQLAlchemy table objects.
    """
    def __init__(self, db: Database, tables: _TablesTuple):
        self._db = db
        self._tables = tables
        self._records = {}

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext) -> CollectionManager:
        # Docstring inherited from CollectionManager.
        return cls(db, tables=context.addTableTuple(_TABLES_SPEC))

    @classmethod
    def addCollectionForeignKey(cls, tableSpec: ddl.TableSpec, *, prefix: str = "collection",
                                onDelete: Optional[str] = None, **kwds: Any) -> ddl.FieldSpec:
        # Docstring inherited from CollectionManager.
        if prefix is None:
            prefix = "collection"
        original = _TABLES_SPEC.collection.fields["name"]
        copy = ddl.FieldSpec(cls.getCollectionForeignKeyName(prefix), dtype=original.dtype, **kwds)
        tableSpec.fields.add(copy)
        tableSpec.foreignKeys.append(ddl.ForeignKeySpec("collection", source=(copy.name,),
                                                        target=(original.name,), onDelete=onDelete))
        return copy

    @classmethod
    def addRunForeignKey(cls, tableSpec: ddl.TableSpec, *, prefix: str = "run",
                         onDelete: Optional[str] = None, **kwds: Any) -> ddl.FieldSpec:
        # Docstring inherited from CollectionManager.
        if prefix is None:
            prefix = "run"
        original = _TABLES_SPEC.run.fields["name"]
        copy = ddl.FieldSpec(cls.getRunForeignKeyName(prefix), dtype=original.dtype, **kwds)
        tableSpec.fields.add(copy)
        tableSpec.foreignKeys.append(ddl.ForeignKeySpec("run", source=(copy.name,),
                                                        target=(original.name,), onDelete=onDelete))
        return copy

    @classmethod
    def getCollectionForeignKeyName(cls, prefix: str = "collection") -> str:
        return f"{prefix}_name"

    @classmethod
    def getRunForeignKeyName(cls, prefix: str = "run") -> str:
        return f"{prefix}_name"

    def refresh(self):
        # Docstring inherited from CollectionManager.
        sql = sqlalchemy.sql.select(
            self._tables.collection.columns + self._tables.run.columns
        ).select_from(
            self._tables.collection.join(self._tables.run, isouter=True)
        )
        # Put found records into a temporary instead of updating self._records
        # in place, for exception safety.
        records = {}
        chains = []
        for row in self._db.query(sql).fetchall():
            name = row[self._tables.collection.columns.name]
            type = CollectionType(row["type"])
            if type is CollectionType.RUN:
                record = NameKeyRunRecord(
                    name=name,
                    db=self._db,
                    table=self._tables.run,
                    host=row[self._tables.run.columns.host],
                    timespan=Timespan(
                        begin=row[self._tables.run.columns[TIMESPAN_FIELD_SPECS.begin.name]],
                        end=row[self._tables.run.columns[TIMESPAN_FIELD_SPECS.end.name]],
                    )
                )
            elif type is CollectionType.CHAINED:
                record = NameKeyChainedCollectionRecord(db=self._db, table=self._tables.collection_chain,
                                                        name=name)
                chains.append(record)
            else:
                record = NameKeyCollectionRecord(type=type, name=name)
            records[record.name] = record
        self._records = records
        for chain in chains:
            chain.refresh(self)

    def register(self, name: str, type: CollectionType) -> CollectionRecord:
        # Docstring inherited from CollectionManager.
        record = self._records.get(name)
        if record is None:
            kwds = {"name": name}
            self._db.sync(
                self._tables.collection,
                keys=kwds,
                compared={"type": int(type)},
            )
            if type is CollectionType.RUN:
                row, _ = self._db.sync(
                    self._tables.run,
                    keys=kwds,
                    returning={"host", TIMESPAN_FIELD_SPECS.begin.name, TIMESPAN_FIELD_SPECS.end.name},
                )
                record = NameKeyRunRecord(
                    db=self._db,
                    table=self._tables.run,
                    host=row["host"],
                    timespan=Timespan(
                        row[TIMESPAN_FIELD_SPECS.begin.name],
                        row[TIMESPAN_FIELD_SPECS.end.name]
                    ),
                    **kwds
                )
            elif type is CollectionType.CHAINED:
                record = NameKeyChainedCollectionRecord(db=self._db, table=self._tables.collection_chain,
                                                        **kwds)
            else:
                record = NameKeyCollectionRecord(type=type, **kwds)
            self._records[record.name] = record
        return record

    def find(self, name: str) -> CollectionRecord:
        # Docstring inherited from CollectionManager.
        result = self._records.get(name)
        if result is None:
            raise MissingCollectionError(f"No collection with name '{name}' found.")
        return result

    def __getitem__(self, key: Any) -> Optional[CollectionRecord]:
        # Docstring inherited from CollectionManager.
        try:
            return self._records[key]
        except KeyError as err:
            raise MissingCollectionError(f"Collection with key '{err}' not found.") from err

    def __iter__(self) -> Iterator[CollectionRecord]:
        yield from self._records.values()
