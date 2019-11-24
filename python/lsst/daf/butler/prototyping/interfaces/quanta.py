from __future__ import annotations

__all__ = ["RegistryLayerQuantumRecords", "RegistryLayerQuantumStorage"]

from abc import ABC, abstractmethod
from typing import (
    Optional,
    TYPE_CHECKING
)

from ...core.dimensions import DimensionGraph, DimensionUniverse

from ..quantum import Quantum

if TYPE_CHECKING:
    from .registryLayer import RegistryLayer


class RegistryLayerQuantumRecords(ABC):

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


class RegistryLayerQuantumStorage(ABC):

    @classmethod
    @abstractmethod
    def load(cls, layer: RegistryLayer, *, universe: DimensionUniverse) -> RegistryLayerQuantumStorage:
        pass

    @abstractmethod
    def refresh(self, *, universe: DimensionUniverse):
        pass

    @abstractmethod
    def get(self, dimensions: DimensionGraph) -> Optional[RegistryLayerQuantumRecords]:
        pass

    @abstractmethod
    def register(self, dimensions: DimensionGraph) -> RegistryLayerQuantumRecords:
        pass
