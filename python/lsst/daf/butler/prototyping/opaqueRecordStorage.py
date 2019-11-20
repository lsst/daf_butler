from __future__ import annotations

__all__ = ["OpaqueRecordStorage"]

from typing import (
    Any,
    Iterator,
)

import sqlalchemy

from ..core.schema import TableSpec
from .databaseLayer import DatabaseLayer


class OpaqueRecordStorage:

    def __init__(self, *, db: DatabaseLayer, name: str, table: sqlalchemy.schema.Table):
        self.name = name
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

    name: str
    spec: TableSpec
