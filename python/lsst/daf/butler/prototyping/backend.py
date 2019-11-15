from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
)

import sqlalchemy

from .core.dimensions import (
    DataCoordinate,
    ExpandedDataCoordinate,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
)
from .core.datasets import DatasetType
from .core.quantum import Quantum
from .core.timespan import Timespan
from .core.utils import NamedKeyDict
from .core.queries import CollectionsExpression, Like

from .iterables import DataIdIterable, SingleDatasetTypeIterable, DatasetIterable

DatasetTypesExpression = Union[DatasetType, str, Like, List[Union[DatasetType, str, Like]], ...]


class Run:
    name: str
    collection_id: int
    origin: int
    timespan: Timespan
    host: str
    environment_id: int


class GeneralRecordStorage(ABC):

    @abstractmethod
    def isWriteable(self) -> bool:
        pass

    @abstractmethod
    def matches(self, collections: CollectionsExpression = ...,
                datasetType: DatasetTypesExpression = None) -> bool:
        pass

    @abstractmethod
    def syncRun(self, name: str) -> Run:
        pass

    @abstractmethod
    def findRun(self, name: str) -> Optional[Run]:
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
    def getDatasetLocations(self, datasets: DatasetIterable) -> Iterator[str]:
        pass

    @abstractmethod
    def deleteDatasetLocations(self, datastoreName: str, datasets: DatasetIterable):
        pass

    @abstractmethod
    def selectDatasetTypes(self, datasetTypes: DatasetTypesExpression = ..., *,
                           collections: CollectionsExpression = ...) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    @abstractmethod
    def selectCollections(self, collections: CollectionsExpression = ..., *,
                          datasetTypes: DatasetTypesExpression = ...) -> Optional[sqlalchemy.sql.FromClause]:
        pass


class OpaqueRecordStorage(ABC):

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def isWriteable(self) -> bool:
        pass

    @abstractmethod
    def insert(self, *data: dict):
        pass

    @abstractmethod
    def fetch(self, **where: Any) -> Iterator[dict]:
        pass

    @abstractmethod
    def delete(self, **where: Any):
        pass

    name: str


class DimensionRecordStorage(ABC):

    def __init__(self, element: DimensionElement):
        self.element = element

    @abstractmethod
    def isWriteable(self) -> bool:
        pass

    @abstractmethod
    def matches(self, dataId: Optional[ExpandedDataCoordinate] = None) -> bool:
        pass

    @abstractmethod
    def insert(self, *records: DimensionRecord):
        pass

    @abstractmethod
    def fetch(self, dataIds: DataIdIterable) -> Dict[DataCoordinate, DimensionRecord]:
        pass

    @abstractmethod
    def select(self, dataId: Optional[ExpandedDataCoordinate] = None) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    @abstractmethod
    def selectSkyPixOverlap(self, dataId: Optional[ExpandedDataCoordinate] = None
                            ) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    element: DimensionElement


DimensionRecordCache = NamedKeyDict[DimensionElement, Dict[DataCoordinate, DimensionRecord]]


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


class DatasetRecordStorage(ABC):

    def __init__(self, datasetType: DatasetType):
        self.datasetType = datasetType

    @abstractmethod
    def isWriteable(self) -> bool:
        pass

    @abstractmethod
    def matches(self, collections: CollectionsExpression = ...,
                dataId: Optional[ExpandedDataCoordinate] = None) -> bool:
        pass

    @abstractmethod
    def insert(self, run: Run, dataIds: DataIdIterable, *, producer: Optional[Quantum] = None
               ) -> SingleDatasetTypeIterable:
        """Insert one or more dataset entries into the database.
        """
        pass

    @abstractmethod
    def find(self, collections: List[str], dataIds: DataIdIterable = None) -> SingleDatasetTypeIterable:
        """Search one or more collections (in order) for datasets.

        Unlike select, this method requires complete data IDs and does
        not accept DatasetType or Collection expressions.
        """
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
    def select(self, collections: CollectionsExpression = ...,
               dataId: Optional[ExpandedDataCoordinate] = None,
               isResult: bool = True, addRank: bool = False) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    datasetType: DatasetType
