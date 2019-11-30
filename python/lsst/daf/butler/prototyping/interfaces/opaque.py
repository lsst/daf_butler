from __future__ import annotations

__all__ = ["OpaqueTableManager", "OpaqueTableRecords"]

from abc import ABC, abstractmethod
from typing import (
    Any,
    Iterator,
    Optional,
)

from ...core.schema import TableSpec
from .database import Database


class OpaqueTableRecords(ABC):

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


class OpaqueTableManager(ABC):

    @classmethod
    @abstractmethod
    def load(cls, db: Database) -> OpaqueTableManager:
        pass

    @abstractmethod
    def refresh(self):
        pass

    @abstractmethod
    def get(self, name: str) -> Optional[OpaqueTableRecords]:
        pass

    @abstractmethod
    def register(self, name: str, spec: TableSpec) -> OpaqueTableRecords:
        pass
