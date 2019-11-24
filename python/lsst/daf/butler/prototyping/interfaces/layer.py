from __future__ import annotations

__all__ = ["RegistryLayer"]

from abc import ABC, abstractmethod
from typing import (
    Iterator,
    Optional,
    TYPE_CHECKING,
)

import sqlalchemy

from ...core.datasets import (
    ResolvedDatasetHandle,
)
from ..iterables import DatasetIterable
from ..run import Run

if TYPE_CHECKING:
    from .opaque import RegistryLayerOpaqueStorage
    from .dimensions import RegistryLayerDimensionStorage
    from .quanta import RegistryLayerQuantumStorage
    from .database import Database


class RegistryLayer(ABC):

    def __init__(self, db: Database):
        self.db = db

    @abstractmethod
    def syncRun(self, name: str) -> Run:
        pass

    @abstractmethod
    def findRun(self, name: str) -> Optional[Run]:
        pass

    @abstractmethod
    def getRun(self, collection_id: int, origin: int) -> Optional[Run]:
        pass

    @abstractmethod
    def updateRun(self, run: Run):
        pass

    @abstractmethod
    def syncCollection(self, name: str, *, calibration: bool = False):
        pass

    @abstractmethod
    def insertDatasetLocations(self, datastoreName: str, datasets: DatasetIterable, *,
                               ephemeral: bool = False):
        pass

    @abstractmethod
    def fetchDatasetLocations(self, dataset: ResolvedDatasetHandle) -> Iterator[str]:
        pass

    @abstractmethod
    def deleteDatasetLocations(self, datastoreName: str, datasets: DatasetIterable):
        pass

    @abstractmethod
    def selectDatasetTypes(self) -> sqlalchemy.sql.FromClause:
        pass

    @abstractmethod
    def selectCollections(self) -> sqlalchemy.sql.FromClause:
        pass

    @property
    @abstractmethod
    def opaque(self) -> RegistryLayerOpaqueStorage:
        pass

    @property
    @abstractmethod
    def dimensions(self) -> RegistryLayerDimensionStorage:
        pass

    @property
    @abstractmethod
    def quanta(self) -> RegistryLayerQuantumStorage:
        pass

    db: Database
