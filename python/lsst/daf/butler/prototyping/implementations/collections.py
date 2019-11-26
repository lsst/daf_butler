from __future__ import annotations

__all__ = ["AggressiveRegistryLayerCollectionStorage"]

from typing import (
    Optional,
)

import sqlalchemy

from ...core.schema import TableSpec, FieldSpec, ForeignKeySpec
from ...core.timespan import TIMESPAN_FIELD_SPECS
from ..collections import Run, Collection, CollectionType

from ..interfaces import (
    Database,
    makeTableStruct,
    RegistryLayerCollectionStorage
)


@makeTableStruct
class CollectionTablesTuple:
    collection = TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
            FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
        ],
        unique={("name",)},
    )
    run = TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            FieldSpec("host", dtype=sqlalchemy.String, length=128),
        ],
        unique={("name",)},
        foreignKeys=[
            ForeignKeySpec("collection", source=("id", "origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
        ],
    )


class AggressiveRegistryLayerCollectionStorage(RegistryLayerCollectionStorage):

    def __init__(self, db: Database):
        self._db = db
        self._tables = CollectionTablesTuple(db)

    @classmethod
    def load(cls, db: Database) -> RegistryLayerCollectionStorage:
        return cls(db)

    def sync(self, collection: Collection) -> Collection:
        if collection.id is None and collection.origin is None:
            row, _ = self._db.sync(
                self._tables.collection,
                keys={"name": collection.name},
                compared={"type": collection.type},
                extra={"origin": self._db.origin},
                returning={"id", "origin"},
            )
            collection.id = row["id"]
            collection.origin = row["origin"]
        elif collection.id is not None and collection.origin is not None:
            self._db.sync(
                self._tables.collection,
                keys={"name": collection.name},
                compared={"type": collection.type, "origin": collection.origin, "id": collection.id}
            )
        else:
            raise ValueError(f"Collection with name '{collection.name}' has id={collection.id} "
                             f"but origin={collection.origin}.")

        if collection.type is CollectionType.Run:
            assert isinstance(collection, Run)
            fields = {"host", TIMESPAN_FIELD_SPECS.begin.name, TIMESPAN_FIELD_SPECS.end.name}
            compared = {}
            returning = {}
            for field in fields:
                value = getattr(collection, field)
                if value is None:
                    returning.add(field)
                else:
                    compared[field] = value
            row, _ = self.db.sync(
                self._tables.run,
                keys={"id": collection.id, "origin": collection.origin},
                compared=compared,
                returning=returning,
            )
            for field in returning.keys():
                setattr(collection, field, row[field])

        return collection

    def _fetch(self, where: sqlalchemy.sql.ColumnElement) -> Optional[Collection]:
        row = self._db.connection.execute(
            sqlalchemy.sql.select().select_from(
                self._tables.collection.join(self._tables.run, isouter=True)
            ).where(where)
        ).fetchone()
        if row is None:
            return None
        kwds = {
            "name": row[self._tables.collection.columns.name],
            "id": row[self._tables.collection.columns.id],
            "origin": row[self._tables.collection.columns.origin],
        }
        type = CollectionType(row["type"])
        if type is CollectionType.RUN:
            kwds[TIMESPAN_FIELD_SPECS.begin.name] = row[self._tables.run[TIMESPAN_FIELD_SPECS.begin.name]]
            kwds[TIMESPAN_FIELD_SPECS.end.name] = row[self._tables.run[TIMESPAN_FIELD_SPECS.end.name]]
            kwds["host"] = row[self._tables.run.host]
            return Run(**kwds)
        else:
            kwds["type"] = type
            return Collection(**kwds)

    def find(self, name: str) -> Optional[Collection]:
        return self._fetch(self._tables.collection.columns.name == name)

    def get(self, id: int, origin: int) -> Optional[Collection]:
        return self._fetch(
            sqlalchemy.sql.and_(self._tables.collection.columns.id == id,
                                self._tables.collection.columns.origin == origin)
        )

    def finish(self, run: Run):
        pass
        # TODO

    def select(self) -> sqlalchemy.sql.FromClause:
        return self._tables.collections
