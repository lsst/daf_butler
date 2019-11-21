from __future__ import annotations

__all__ = ["DatabaseLayer"]

from abc import ABC, abstractmethod
from typing import (
    Optional,
)

import sqlalchemy

from ..core.schema import TableSpec


class DatabaseLayer(ABC):

    def __init__(self, origin: int, connection: sqlalchemy.engine.Connection):
        self.origin = origin
        self.connection = connection

    @abstractmethod
    def isWriteable(self) -> bool:
        pass

    @abstractmethod
    def ensureTableExists(self, name: str, spec: TableSpec) -> sqlalchemy.schema.Table:
        pass

    @abstractmethod
    def getExistingTable(self, name: str) -> sqlalchemy.schema.Table:
        pass

    @abstractmethod
    def sync(self, table: sqlalchemy.schema.Table,
             keys: dict,  # find the matching record with these
             compared: Optional[dict] = None,  # these values must be the same on return
             extra: Optional[dict] = None,  # these values should be used only when inserting
             ) -> sqlalchemy.engine.RowProxy:
        pass

    origin: int
    connection: sqlalchemy.engine.Connection
