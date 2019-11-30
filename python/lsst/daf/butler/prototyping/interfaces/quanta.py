from __future__ import annotations

__all__ = ["QuantumTableRecords", "QuantumTableManager"]

from abc import ABC, abstractmethod
from typing import (
    Optional,
    TYPE_CHECKING
)

from ...core.dimensions import DimensionGraph, DimensionUniverse

from ..quantum import Quantum

if TYPE_CHECKING:
    from .registryLayer import RegistryLayer


class QuantumTableRecords(ABC):

    def __init__(self, dimensions: DimensionGraph):
        self.dimensions = dimensions

    @abstractmethod
    def start(self, quantum: Quantum) -> Quantum:
        pass

    @abstractmethod
    def finish(self, quantum: Quantum):
        pass

    # TODO: we need methods for fetching and querying (but don't yet have the
    # high-level Registry API).

    dimensions: DimensionGraph


class QuantumTableManager(ABC):

    @classmethod
    @abstractmethod
    def load(cls, layer: RegistryLayer, *, universe: DimensionUniverse) -> QuantumTableManager:
        pass

    @abstractmethod
    def refresh(self, *, universe: DimensionUniverse):
        pass

    @abstractmethod
    def get(self, dimensions: DimensionGraph) -> Optional[QuantumTableRecords]:
        pass

    @abstractmethod
    def register(self, dimensions: DimensionGraph) -> QuantumTableRecords:
        pass
