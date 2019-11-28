from __future__ import annotations

__all__ = ["RegistryLayerDatasetStorage", "RegistryLayerDatasetRecords"]

from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Iterator,
    Optional,
    Union,
    TYPE_CHECKING,
)

import sqlalchemy

from ...core.datasets import DatasetType, ResolvedDatasetHandle
from ...core.dimensions import DimensionUniverse, DataCoordinate

from ..iterables import DataIdIterable, SingleDatasetTypeIterable, DatasetIterable
from ..quantum import Quantum

if TYPE_CHECKING:
    from ..interfaces import Database, RegistryLayerCollectionStorage


class RegistryLayerDatasetRecords(ABC):

    def __init__(self, datasetType: DatasetType):
        self.datasetType = datasetType

    @abstractmethod
    def insert(self, run: str, dataIds: DataIdIterable, *, quantum: Optional[Quantum] = None
               ) -> SingleDatasetTypeIterable:
        """Insert one or more dataset entries into the database.
        """
        pass

    @abstractmethod
    def find(self, collection: str, dataId: DataCoordinate) -> Optional[ResolvedDatasetHandle]:
        """Search a collection for a dataset.
        """
        pass

    @abstractmethod
    def get(self, id: int, origin: int) -> Optional[ResolvedDatasetHandle]:
        pass

    @abstractmethod
    def delete(self, datasets: SingleDatasetTypeIterable):
        pass

    @abstractmethod
    def associate(self, collection: str, datasets: SingleDatasetTypeIterable, *,
                  begin: Optional[datetime] = None, end: Optional[datetime] = None):
        pass

    @abstractmethod
    def disassociate(self, collection: str, datasets: SingleDatasetTypeIterable):
        pass

    @abstractmethod
    def select(self, collection: Union[str, ...],
               returnDimensions: bool = True,
               returnId: bool = True,
               returnOrigin: bool = True,
               returnRun: bool = False,
               returnQuantum: bool = False) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    datasetType: DatasetType


class RegistryLayerDatasetStorage(ABC):

    @classmethod
    @abstractmethod
    def loadTypes(cls, db: Database, collections: RegistryLayerCollectionStorage,
                  *, universe: DimensionUniverse) -> RegistryLayerDatasetStorage:
        pass

    @abstractmethod
    def refreshTypes(self, *, universe: DimensionUniverse):
        pass

    @abstractmethod
    def getType(self, datasetType: DatasetType) -> Optional[RegistryLayerDatasetRecords]:
        pass

    @abstractmethod
    def registerType(self, datasetType: DatasetType) -> RegistryLayerDatasetRecords:
        pass

    @abstractmethod
    def selectTypes(self) -> sqlalchemy.sql.FromClause:
        pass

    @abstractmethod
    def iterTypes(self) -> Iterator[RegistryLayerDatasetRecords]:
        pass

    @abstractmethod
    def getHandle(self, id: int, origin: int) -> Optional[ResolvedDatasetHandle]:
        pass

    @abstractmethod
    def insertLocations(self, datastoreName: str, datasets: DatasetIterable, *,
                        ephemeral: bool = False):
        pass

    @abstractmethod
    def fetchLocations(self, dataset: ResolvedDatasetHandle) -> Iterator[str]:
        pass

    @abstractmethod
    def deleteLocations(self, datastoreName: str, datasets: DatasetIterable):
        pass
