from __future__ import annotations

__all__ = ["RegistryLayerOpaqueStorage", "RegistryLayerOpaqueRecords"]

from abc import ABC, abstractmethod
from typing import (
    Any,
    Iterator,
    Optional,
)

from ...core.schema import TableSpec
from .layer import RegistryLayer


class RegistryLayerOpaqueRecords(ABC):

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


class RegistryLayerOpaqueStorage(ABC):

    @classmethod
    @abstractmethod
    def load(cls, layer: RegistryLayer) -> RegistryLayerOpaqueStorage:
        pass

    @abstractmethod
    def refresh(self):
        pass

    @abstractmethod
    def get(self, name: str) -> Optional[RegistryLayerOpaqueRecords]:
        pass

    @abstractmethod
    def register(self, name: str, spec: TableSpec) -> RegistryLayerOpaqueRecords:
        pass
