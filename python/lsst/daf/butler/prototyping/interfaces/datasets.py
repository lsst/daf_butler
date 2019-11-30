from __future__ import annotations

__all__ = ["DatasetTableManager", "DatasetTableRecords", "Select"]

from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Iterator,
    Optional,
    Union,
    Type,
    TypeVar,
    TYPE_CHECKING,
)

import sqlalchemy

from ...core.datasets import DatasetType, ResolvedDatasetHandle
from ...core.dimensions import DimensionUniverse, DataCoordinate
from ...core.timespan import Timespan

from ..iterables import DataIdIterable, SingleDatasetTypeIterable, DatasetIterable
from ..quantum import Quantum

if TYPE_CHECKING:
    from ..interfaces import (
        Database,
        CollectionManager,
    )


T = TypeVar("T")


class Select:
    """Tag class used to indicate that a field should be returned in
    a SELECT query.
    """
    pass


Select.Or = Union[T, Type[Select]]


class DatasetTableRecords(ABC):

    def __init__(self, datasetType: DatasetType):
        self.datasetType = datasetType

    @abstractmethod
    def insert(self, run: str, dataIds: DataIdIterable, *,
               quantum: Optional[Quantum] = None) -> SingleDatasetTypeIterable:
        """Insert one or more dataset entries into the database.
        """
        pass

    @abstractmethod
    def find(self, collection: str, dimensions: DataCoordinate,
             timespan: Optional[Timespan[Optional[datetime]]] = None
             ) -> Optional[ResolvedDatasetHandle]:
        """Search a collection for a dataset.
        """
        pass

    @abstractmethod
    def delete(self, datasets: SingleDatasetTypeIterable):
        pass

    @abstractmethod
    def associate(self, collection: str, datasets: SingleDatasetTypeIterable, *,
                  timespan: Optional[Timespan[Optional[datetime]]] = None):
        pass

    @abstractmethod
    def disassociate(self, collection: str, datasets: SingleDatasetTypeIterable):
        pass

    @abstractmethod
    def select(self, collection: Select.Or[str] = Select,
               dataId: Select.Or[DataCoordinate] = Select,
               id: Select.Or[int] = Select,
               origin: Select.Or[int] = Select,
               run: Select.Or[str] = Select,
               timespan: Optional[Select.Or[Timespan[datetime]]] = None
               ) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    datasetType: DatasetType


class DatasetTableManager(ABC):

    @classmethod
    @abstractmethod
    def loadTypes(cls, db: Database, *, collections: CollectionManager,
                  universe: DimensionUniverse) -> DatasetTableManager:
        pass

    @abstractmethod
    def refreshTypes(self, *, universe: DimensionUniverse):
        pass

    @abstractmethod
    def getType(self, datasetType: DatasetType) -> Optional[DatasetTableRecords]:
        pass

    @abstractmethod
    def registerType(self, datasetType: DatasetType) -> DatasetTableRecords:
        pass

    @abstractmethod
    def selectTypes(self) -> sqlalchemy.sql.FromClause:
        pass

    @abstractmethod
    def iterTypes(self) -> Iterator[DatasetTableRecords]:
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
