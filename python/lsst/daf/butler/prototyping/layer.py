from __future__ import annotations

__all__ = ["RegistryLayer"]

from typing import (
    Type,
    TYPE_CHECKING,
)

from ..core.dimensions import DimensionUniverse

if TYPE_CHECKING:
    from .database import Database
    from .collections import RegistryLayerCollectionStorage
    from .opaque import RegistryLayerOpaqueStorage
    from .dimensions import RegistryLayerDimensionStorage
    from .datasets import RegistryLayerDatasetStorage
    from .quanta import RegistryLayerQuantumStorage


class RegistryLayer:

    def __init__(self, db: Database, *, universe: DimensionUniverse,
                 collections: Type[RegistryLayerCollectionStorage],
                 opaque: Type[RegistryLayerOpaqueStorage],
                 dimensions: Type[RegistryLayerDimensionStorage],
                 datasets: Type[RegistryLayerDatasetStorage],
                 quanta: Type[RegistryLayerQuantumStorage]):
        self.db = db
        self.collections = collections.load(self.db)
        self.opaque = opaque.load(self.db)
        self.dimensions = dimensions.load(self.db, universe=universe)
        self.datasets = datasets.load(self.db, self.collections, universe=universe)
        self.quanta = quanta.load(self.db, self.datasets, self.collections, universe=universe)

    collections: RegistryLayerCollectionStorage

    opaque: RegistryLayerOpaqueStorage

    dimensions: RegistryLayerDimensionStorage

    datasets: RegistryLayerDatasetStorage

    quanta: RegistryLayerQuantumStorage

    db: Database
