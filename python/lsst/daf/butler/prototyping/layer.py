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
                 quanta: Type[QuantumTableManager],
                 create: bool = True):
        self.db = db
        with db.declareStaticTables(create=create) as context:
            self.collections = collections.initialize(self.db, context)
            self.opaque = opaque.initialize(self.db, context)
            self.dimensions = dimensions.initialize(self.db, context, universe=universe)
            self.datasets = datasets.initialize(self.db, context, collections=self.collections,
                                                quanta=quanta, universe=universe)
            self.quanta = quanta.initialize(self.db, context, collections=self.collections,
                                            datasets=self.datasets, universe=universe)

    collections: CollectionManager

    opaque: OpaqueTableManager

    dimensions: DimensionTableManager

    datasets: DatasetTableManager

    quanta: QuantumTableManager

    db: Database
