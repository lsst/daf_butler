from __future__ import annotations

__all__ = ["AggressiveCollectionManager"]

from datetime import datetime
from typing import (
    Optional,
)

import sqlalchemy

from ...core.dimensions.schema import TIMESPAN_FIELD_SPECS
from ...core.timespan import Timespan

from ..interfaces import (
    CollectionType,
    Database,
    CollectionManager,
    CollectionRecord,
    RunRecord,
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
        values = {
            TIMESPAN_FIELD_SPECS.begin.name: timespan.begin,
            TIMESPAN_FIELD_SPECS.end.name: timespan.end,
            "host": host,
        }
        self._db.connection.execute(
            self._table.update().values(values).where(self._table.columns.id == self.id)
        )
        # TODO: make sure the above update modified exactly one record.
        self._host = host
        self._timespan = timespan

    @property
    def host(self) -> Optional[str]:
        return self._host

    @property
    def timespan(self) -> Timespan[Optional[datetime]]:
        return self._timespan


class AggressiveCollectionManager(CollectionManager):

    def __init__(self, db: Database):
        self._db = db
        self._tables = self.TablesTuple(db)
        self._byName = {}
        self._byId = {}
        self.refresh()

    @classmethod
    def load(cls, db: Database) -> CollectionManager:
        return cls(db)

    def refresh(self):
        sql = sqlalchemy.sql.select().select_from(
            self._tables.collection.join(self._tables.run, isouter=True)
        )
        byName = {}
        byId = {}
        for row in self._db.connection.execute(sql).fetchall():
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

    @property
    def tables(self) -> CollectionManager.TablesTuple:
        return self._tables
