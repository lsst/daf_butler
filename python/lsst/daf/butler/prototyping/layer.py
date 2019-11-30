from __future__ import annotations

__all__ = ["RegistryLayer"]

from typing import (
    Type,
    TYPE_CHECKING,
)

from ..core.dimensions import DimensionUniverse

if TYPE_CHECKING:
    from .database import Database
    from .collections import CollectionManager
    from .opaque import OpaqueTableManager
    from .dimensions import DimensionTableManager
    from .datasets import DatasetTableManager
    from .quanta import QuantumTableManager


class RegistryLayer:

    def __init__(self, db: Database, *, universe: DimensionUniverse,
                 collections: Type[CollectionManager],
                 opaque: Type[OpaqueTableManager],
                 dimensions: Type[DimensionTableManager],
                 datasets: Type[DatasetTableManager],
                 quanta: Type[QuantumTableManager]):
        self.db = db
        self.collections = collections.load(self.db)
        self.opaque = opaque.load(self.db)
        self.dimensions = dimensions.load(self.db, universe=universe)
        self.datasets = datasets.load(self.db, self.collections, universe=universe)
        self.quanta = quanta.load(self.db, self.datasets, self.collections, universe=universe)

    collections: CollectionManager

    opaque: OpaqueTableManager

    dimensions: DimensionTableManager

    datasets: DatasetTableManager

    quanta: QuantumTableManager

    db: Database
