from __future__ import annotations

__all__ = ["CollectionManager"]

from abc import ABC, abstractmethod
from datetime import datetime
import enum
from typing import (
    Optional,
    TYPE_CHECKING,
)

import sqlalchemy

from ...core.schema import TableSpec, FieldSpec, ForeignKeySpec
from ...core.dimensions.schema import TIMESPAN_FIELD_SPECS
from ...core.timespan import Timespan

if TYPE_CHECKING:
    from .database import Database, makeTableStruct


class CollectionType(enum.IntEnum):
    RUN = 1
    TAGGED = 2
    CALIBRATION = 3


@makeTableStruct
class CollectionTablesTuple:
    collection = TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
            FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
        ],
        unique={("name",)},
    )
    run = TableSpec(
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


class CollectionRecord(ABC):

    def __init__(self, name: str, id: int, type: CollectionType):
        self.name = name
        self.id = id
        self.type = type

    name: str
    type: CollectionType
    id: int


class RunRecord(CollectionRecord):

    @abstractmethod
    def update(self, host: Optional[str] = None, timespan: Timespan[Optional[datetime]] = None):
        pass

    @property
    @abstractmethod
    def host(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def timespan(self) -> Timespan[Optional[datetime]]:
        pass


class CollectionManager(ABC):

    TablesTuple = CollectionTablesTuple

    @classmethod
    @abstractmethod
    def load(cls, db: Database) -> CollectionManager:
        pass

    @abstractmethod
    def refresh(self):
        pass

    @abstractmethod
    def register(self, name: str, type: CollectionType) -> CollectionRecord:
        pass

    @abstractmethod
    def find(self, name: str) -> Optional[CollectionRecord]:
        pass

    @abstractmethod
    def get(self, id: int) -> Optional[CollectionRecord]:
        pass

    @property
    @abstractmethod
    def tables(self) -> CollectionTablesTuple:
        pass
