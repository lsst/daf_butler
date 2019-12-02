from __future__ import annotations

__all__ = ["CollectionManager"]

from abc import ABC, abstractmethod
from datetime import datetime
import enum
from typing import (
    Any,
    Optional,
    TYPE_CHECKING,
)

from ...core.schema import TableSpec, FieldSpec
from ...core.timespan import Timespan

if TYPE_CHECKING:
    from .database import Database, StaticTablesContext


class CollectionType(enum.IntEnum):
    RUN = 1
    TAGGED = 2
    CALIBRATION = 3


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

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, schema: StaticTablesContext) -> CollectionManager:
        pass

    @classmethod
    @abstractmethod
    def addCollectionForeignKey(cls, tableSpec: TableSpec, *, name: Optional[str] = None,
                                onDelete: Optional[str] = None, **kwds: Any) -> FieldSpec:
        pass

    @classmethod
    @abstractmethod
    def addRunForeignKey(cls, tableSpec: TableSpec, *, name: Optional[str] = None,
                         onDelete: Optional[str] = None, **kwds: Any) -> FieldSpec:
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
