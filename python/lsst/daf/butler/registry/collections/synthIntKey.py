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

__all__ = ["SynthIntKeyCollectionManager"]

from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    TYPE_CHECKING,
)

import sqlalchemy

from ._base import (
    CollectionTablesTuple,
    DefaultCollectionManager,
    makeRunTableSpec,
    makeCollectionChainTableSpec,
)
from ...core import ddl
from ..interfaces import CollectionRecord

if TYPE_CHECKING:
    from ..interfaces import Database, StaticTablesContext


_TABLES_SPEC = CollectionTablesTuple(
    collection=ddl.TableSpec(
        fields=[
            ddl.FieldSpec("collection_id", dtype=sqlalchemy.BigInteger, primaryKey=True, autoincrement=True),
            ddl.FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
            ddl.FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
        ],
        unique=[("name",)],
    ),
    run=makeRunTableSpec("collection_id", sqlalchemy.BigInteger),
    collection_chain=makeCollectionChainTableSpec("collection_id", sqlalchemy.BigInteger),
)


class SynthIntKeyCollectionManager(DefaultCollectionManager):
    """A `CollectionManager` implementation that uses synthetic primary key
    (auto-incremented integer) for collections table.

    Most of the logic, including caching policy, is implemented in the base
    class, this class only adds customisations specific to this particular
    table schema.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    tables : `NamedTuple`
        Named tuple of SQLAlchemy table objects.
    collectionIdName : `str`
        Name of the column in collections table that identifies it (PK).
    """
    def __init__(self, db: Database, tables: CollectionTablesTuple, collectionIdName: str):
        super().__init__(db=db, tables=tables, collectionIdName=collectionIdName)
        self._nameCache: Dict[str, CollectionRecord] = {}  # indexed by collection name

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext) -> SynthIntKeyCollectionManager:
        # Docstring inherited from CollectionManager.
        return cls(db, tables=context.addTableTuple(_TABLES_SPEC),  # type: ignore
                   collectionIdName="collection_id")

    @classmethod
    def getCollectionForeignKeyName(cls, prefix: str = "collection") -> str:
        # Docstring inherited from CollectionManager.
        return f"{prefix}_id"

    @classmethod
    def getRunForeignKeyName(cls, prefix: str = "run") -> str:
        # Docstring inherited from CollectionManager.
        return f"{prefix}_id"

    @classmethod
    def addCollectionForeignKey(cls, tableSpec: ddl.TableSpec, *, prefix: str = "collection",
                                onDelete: Optional[str] = None,
                                constraint: bool = True,
                                **kwargs: Any) -> ddl.FieldSpec:
        # Docstring inherited from CollectionManager.
        original = _TABLES_SPEC.collection.fields["collection_id"]
        copy = ddl.FieldSpec(cls.getCollectionForeignKeyName(prefix), dtype=original.dtype, **kwargs)
        tableSpec.fields.add(copy)
        if constraint:
            tableSpec.foreignKeys.append(ddl.ForeignKeySpec("collection", source=(copy.name,),
                                                            target=(original.name,), onDelete=onDelete))
        return copy

    @classmethod
    def addRunForeignKey(cls, tableSpec: ddl.TableSpec, *, prefix: str = "run",
                         onDelete: Optional[str] = None,
                         constraint: bool = True,
                         **kwargs: Any) -> ddl.FieldSpec:
        # Docstring inherited from CollectionManager.
        original = _TABLES_SPEC.run.fields["collection_id"]
        copy = ddl.FieldSpec(cls.getRunForeignKeyName(prefix), dtype=original.dtype, **kwargs)
        tableSpec.fields.add(copy)
        if constraint:
            tableSpec.foreignKeys.append(ddl.ForeignKeySpec("run", source=(copy.name,),
                                                            target=(original.name,), onDelete=onDelete))
        return copy

    def _setRecordCache(self, records: Iterable[CollectionRecord]) -> None:
        """Set internal record cache to contain given records,
        old cached records will be removed.
        """
        self._records = {}
        self._nameCache = {}
        for record in records:
            self._records[record.key] = record
            self._nameCache[record.name] = record

    def _addCachedRecord(self, record: CollectionRecord) -> None:
        """Add single record to cache.
        """
        self._records[record.key] = record
        self._nameCache[record.name] = record

    def _removeCachedRecord(self, record: CollectionRecord) -> None:
        """Remove single record from cache.
        """
        del self._records[record.key]
        del self._nameCache[record.name]

    def _getByName(self, name: str) -> Optional[CollectionRecord]:
        # Docstring inherited from DefaultCollectionManager.
        return self._nameCache.get(name)
