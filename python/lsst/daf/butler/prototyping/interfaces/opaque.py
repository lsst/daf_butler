from __future__ import annotations

__all__ = ["OpaqueTableManager", "OpaqueTableRecords"]

from abc import ABC, abstractmethod
from typing import (
    Any,
    Iterator,
    Optional,
)

from ...core.schema import TableSpec
from .database import Database, StaticTablesContext


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
    def initialize(cls, db: Database, context: StaticTablesContext) -> OpaqueTableManager:
        pass

    @abstractmethod
    def get(self, name: str) -> Optional[OpaqueTableRecords]:
        pass

    @abstractmethod
    def register(self, name: str, spec: TableSpec) -> OpaqueTableRecords:
        pass
