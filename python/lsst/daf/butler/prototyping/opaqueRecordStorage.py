from __future__ import annotations

__all__ = ["OpaqueRecordStorage", "DatabaseOpaqueRecordStorage"]

from abc import ABC, abstractmethod
from typing import (
    Any,
    Iterator,
    Optional,
)

import sqlalchemy

from ..core.schema import TableSpec, FieldSpec
from .databaseLayer import DatabaseLayer


class OpaqueRecordStorage(ABC):

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def insert(self, *data: dict):
        pass

    @abstractmethod
    def fetch(self, **where: Any) -> Iterator[dict]:
        pass

    @abstractmethod
    def delete(self, **where: Any):
        pass

    name: str


class OpaqueRecordStorageManager(ABC):

    @classmethod
    @abstractmethod
    def load(cls, db: DatabaseLayer) -> OpaqueRecordStorageManager:
        pass

    @abstractmethod
    def refresh(self):
        pass

    @abstractmethod
    def get(self, name: str) -> Optional[OpaqueRecordStorage]:
        pass

    @abstractmethod
    def register(self, name: str, spec: TableSpec) -> OpaqueRecordStorage:
        pass


class DatabaseOpaqueRecordStorage(OpaqueRecordStorage):

    def __init__(self, *, db: DatabaseLayer, name: str, table: sqlalchemy.schema.Table):
        super().__init__(name=name)
        self._db = db
        self._table = table

    def insert(self, *data: dict):
        self._db.connection.execute(self._table.insert(), *data)

    def fetch(self, **where: Any) -> Iterator[dict]:
        sql = self._table.select().where(
            sqlalchemy.sql.and_(*[self.table.columns[k] == v for k, v in where.items()])
        )
        yield from self._db.connection.execute(sql)

    def delete(self, **where: Any):
        sql = self._table.delete().where(
            sqlalchemy.sql.and_(*[self.table.columns[k] == v for k, v in where.items()])
        )
        self._db.connection.execute(sql)


class DatabaseOpaqueRecordStorageManager(OpaqueRecordStorageManager):

    _META_TABLE_NAME = "layer_meta_opaque"

    _META_TABLE_SPEC = TableSpec(
        fields=[
            FieldSpec("table_name", dtype=sqlalchemy.String, length=128, primaryKey=True),
        ],
    )

    def __init__(self, db: DatabaseLayer):
        self._db = db
        self._metaTable = db.ensureTableExists(self._META_TABLE_NAME, self._META_TABLE_SPEC)
        self._managed = {}
        self.refresh()

    @classmethod
    def load(cls, db: DatabaseLayer) -> OpaqueRecordStorageManager:
        return cls(db=db)

    def refresh(self):
        storage = {}
        for row in self._db.connection.execute(self._metaTable.select()).fetchall():
            name = row[self._metaTable.columns.table_name]
            table = self._db.getExistingTable(name)
            storage[name] = DatabaseOpaqueRecordStorage(name=name, table=table, db=self._db)
        self._managed = storage

    def get(self, name: str) -> Optional[OpaqueRecordStorage]:
        return self._managed.get(name)

    def register(self, name: str, spec: TableSpec) -> OpaqueRecordStorage:
        result = self._managed.get(name)
        if result is None:
            # Create the table itself.  If it already exists but wasn't in
            # the dict because it was added by another client since this one
            # was initialized, that's fine.
            table = self._db.ensureTableExists(name, spec)
            # Add a row to the meta table so we can find this table in the
            # future.  Also okay if that already exists, so we use sync.
            self._db.sync(self._metaTable, keys={"table_name": name})
            result = DatabaseOpaqueRecordStorage(name=name, table=table, db=self._db)
            self._managed[name] = result
        return result
