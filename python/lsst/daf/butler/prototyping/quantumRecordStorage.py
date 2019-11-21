from __future__ import annotations

__all__ = ["QuantumRecordStorage", "DatabaseQuantumRecordStorage"]

from abc import ABC, abstractmethod

import sqlalchemy

from ..core.dimensions import DimensionGraph
from ..core.quantum import Quantum
from .databaseLayer import DatabaseLayer


class QuantumRecordStorage(ABC):

    def __init__(self, dimensions: DimensionGraph):
        self.dimensions = dimensions

    @abstractmethod
    def insertQuantum(self, quantum: Quantum) -> Quantum:
        pass

    @abstractmethod
    def updateQuantum(self, quantum: Quantum):
        pass

    # TODO: we need methods for fetching and querying (but don't yet have the
    # high-level Registry API).

    dimensions: DimensionGraph


class DatabaseQuantumRecordStorage(QuantumRecordStorage):

    def __init__(self, *, dimensions: DimensionGraph, db: DatabaseLayer, table: sqlalchemy.schema.Table):
        super().__init__(dimensions=dimensions)
        self._db = db
        self._table = table

    def insertQuantum(self, quantum: Quantum) -> Quantum:
        pass

    def updateQuantum(self, quantum: Quantum):
        pass

    # TODO: we need methods for fetching and querying (but don't yet have the
    # high-level Registry API).

    dimensions: DimensionGraph
