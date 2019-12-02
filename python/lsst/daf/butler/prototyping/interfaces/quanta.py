from __future__ import annotations

__all__ = ["QuantumTableRecords", "QuantumTableManager"]

from abc import ABC, abstractmethod
from typing import (
    Any,
    Optional,
    Tuple,
    TYPE_CHECKING
)

from ...core.dimensions import DimensionGraph, DimensionUniverse
from ...core.schema import FieldSpec, TableSpec

from ..quantum import Quantum

if TYPE_CHECKING:
    from .database import Database, StaticTablesContext, DatasetTableManager, CollectionManager


class QuantumTableRecords(ABC):

    def __init__(self, dimensions: DimensionGraph):
        self.dimensions = dimensions

    @abstractmethod
    def start(self, quantum: Quantum):
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
    def initialize(cls, db: Database, context: StaticTablesContext, *, collections: CollectionManager,
                   datasets: DatasetTableManager, universe: DimensionUniverse) -> QuantumTableManager:
        pass

    @classmethod
    @abstractmethod
    def addQuantumForeignKey(cls, tableSpec: TableSpec, *, name: Optional[str] = None,
                             onDelete: Optional[str] = None, **kwds: Any) -> Tuple[FieldSpec, FieldSpec]:
        pass

    @abstractmethod
    def get(self, dimensions: DimensionGraph) -> Optional[QuantumTableRecords]:
        pass

    @abstractmethod
    def register(self, dimensions: DimensionGraph) -> QuantumTableRecords:
        pass
