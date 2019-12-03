from __future__ import annotations

__all__ = ["AggressiveCollectionManager"]

from collections import namedtuple
from datetime import datetime
from typing import (
    Any,
    Optional,
)

import sqlalchemy

from ...core.schema import TableSpec, FieldSpec, ForeignKeySpec
from ...core.dimensions.schema import TIMESPAN_FIELD_SPECS
from ...core.timespan import Timespan

from ..interfaces import (
    CollectionType,
    Database,
    CollectionManager,
    CollectionRecord,
    RunRecord,
    StaticTablesContext,
)

CollectionTablesTuple = namedtuple("CollectionTablesTuple", ["collection", "run"])

COLLECTION_TABLES_SPEC = CollectionTablesTuple(
    collection=TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
            FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
        ],
        unique={("name",)},
    ),
    run=TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            FieldSpec("host", dtype=sqlalchemy.String, length=128),
        ],
        unique={("name",)},
        foreignKeys=[
            ForeignKeySpec("collection", source=("id",), target=("id",), onDelete="CASCADE"),
        ],
    )
)


class AggressiveRunRecord(RunRecord):

    def __init__(self, *, db: Database, table: sqlalchemy.schema.Table, name: str, id: int,
                 host: Optional[str] = None, timespan: Timespan[Optional[datetime]] = None):
        super().__init__(name=name, id=id, type=CollectionType.RUN)
        self._db = db
        self._table = table
        self._host = host
        self._timespan = timespan

    def update(self, host: Optional[str] = None, timespan: Timespan[Optional[datetime]] = None):
        row = {
            "id": self.id,
            TIMESPAN_FIELD_SPECS.begin.name: timespan.begin,
            TIMESPAN_FIELD_SPECS.end.name: timespan.end,
            "host": host,
        }
        count = self._db.update(self._table, row, keys=["id"],
                                values=[TIMESPAN_FIELD_SPECS.begin.name,
                                        TIMESPAN_FIELD_SPECS.end.name,
                                        "host"])
        if count != 1:
            raise RuntimeError(f"Run update affected {count} records; expected exactly one.")
        self._host = host
        self._timespan = timespan

    @property
    def host(self) -> Optional[str]:
        return self._host

    @property
    def timespan(self) -> Timespan[Optional[datetime]]:
        return self._timespan


class AggressiveCollectionManager(CollectionManager):

    def __init__(self, db: Database, tables: CollectionTablesTuple):
        self._db = db
        self._tables = tables
        self._byName = {}
        self._byId = {}
        self.refresh()

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext) -> CollectionManager:
        return cls(db, tables=context.addTableTuple(COLLECTION_TABLES_SPEC))

    @classmethod
    def addCollectionForeignKey(cls, tableSpec: TableSpec, *, name: str = "collection",
                                onDelete: Optional[str] = None, **kwds: Any) -> FieldSpec:
        if name is None:
            name = "collection"
        original = COLLECTION_TABLES_SPEC.collection.fields["id"]
        copy = FieldSpec(f"{name}_id", dtype=original.dtype, **kwds)
        tableSpec.fields.add(copy)
        tableSpec.foreignKeys.append(ForeignKeySpec("collection", source=(copy.name,),
                                                    target=(original.name,), onDelete=onDelete))
        return copy

    @classmethod
    def addRunForeignKey(cls, tableSpec: TableSpec, *, name: str = "run",
                         onDelete: Optional[str] = None, **kwds) -> FieldSpec:
        if name is None:
            name = "run"
        original = COLLECTION_TABLES_SPEC.run.fields["id"]
        copy = FieldSpec(f"{name}_id", dtype=original.dtype, **kwds)
        tableSpec.fields.add(copy)
        tableSpec.foreignKeys.append(ForeignKeySpec("table", source=(copy.name,),
                                                    target=(original.name,), onDelete=onDelete))
        return copy

    def refresh(self):
        sql = sqlalchemy.sql.select().select_from(
            self._tables.collection.join(self._tables.run, isouter=True)
        )
        byName = {}
        byId = {}
        for row in self._db.query(sql).fetchall():
            kwds = {
                "name": row[self._tables.collection.columns.name],
                "id": row[self._tables.collection.columns.id],
            }
            type = CollectionType(row["type"])
            if type is CollectionType.RUN:
                kwds["db"] = self._db
                kwds["table"] = self.tables.run
                kwds[TIMESPAN_FIELD_SPECS.begin.name] = row[self._tables.run[TIMESPAN_FIELD_SPECS.begin.name]]
                kwds[TIMESPAN_FIELD_SPECS.end.name] = row[self._tables.run[TIMESPAN_FIELD_SPECS.end.name]]
                kwds["host"] = row[self._tables.run.host]
                record = AggressiveRunRecord(**kwds)
            else:
                kwds["type"] = type
                record = CollectionRecord(**kwds)
            byName[record.name] = record
            byId[record.id] = record
        self._byName = byName
        self._byId = byId

    def register(self, name: str, type: CollectionType) -> CollectionRecord:
        record = self._byName.get(name)
        if record is None:
            row, _ = self._db.sync(
                self._tables.collection,
                keys={"name": name},
                compared={"type": type},
                returning={"id"}
            )
            kwds = {
                "name": name,
                "id": row["id"],
            }
            if type is CollectionType.RUN:
                row, _ = self._db.sync(
                    self._tables.run,
                    keys={"id": kwds["id"]},
                    returning={"host", TIMESPAN_FIELD_SPECS.begin.name. TIMESPAN_FIELD_SPECS.end.name},
                )
                kwds["host"] = row["host"]
                kwds["timespan"] = Timespan(
                    row[TIMESPAN_FIELD_SPECS.begin.name],
                    row[TIMESPAN_FIELD_SPECS.end.name]
                )
                record = AggressiveRunRecord(**kwds)
            else:
                kwds["type"] = type
                record = CollectionRecord(**kwds)
            self._byName[record.name] = record
            self._byId[record.id] = record
        return record

    def find(self, name: str) -> Optional[CollectionRecord]:
        return self._byName.get(name)

    def get(self, id: int) -> Optional[CollectionRecord]:
        return self._byId.get(id)
