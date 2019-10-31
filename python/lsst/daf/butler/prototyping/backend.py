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

from .core.schema import TableSpec
from .core.dimensions import (
    DataCoordinate,
    ExpandedDataCoordinate,
    DimensionElement,
    DimensionGraph,
    DimensionRecord,
)
from .core.datasets import DatasetType
from .core.run import Run
from .core.quantum import Quantum
from .core.utils import NamedKeyDict
from .core.queries import CollectionsExpression, Like

from .iterables import DataIdIterable, SingleDatasetTypeIterable, DatasetIterable

DatasetTypesExpression = Union[DatasetType, str, Like, List[Union[DatasetType, str, Like]], ...]


class DimensionRecordStorage(ABC):

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


class DatasetRecordStorage(ABC):

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


class OpaqueRecordStorage(ABC):

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


class GeneralRecordStorage(ABC):

    @abstractmethod
    def isWriteable(self) -> bool:
        pass

    @abstractmethod
    def matches(self, collections: CollectionsExpression = ...,
                datasetType: DatasetTypesExpression = None) -> bool:
        pass

    @abstractmethod
    def ensureRun(self, name: str) -> Run:
        pass

    @abstractmethod
    def findRun(self, name: str) -> Optional[Run]:
        pass

    @abstractmethod
    def updateRun(self, run: Run):
        pass

    @abstractmethod
    def ensureCollection(self, name: str, *, calibration: bool = False):
        pass

    @abstractmethod
    def ensureDatasetType(self, datasetType: DatasetType):
        pass

    @abstractmethod
    def insertDatasetLocations(self, datastoreName: str, datasets: DatasetIterable, *,
                               ephemeral: bool = False):
        pass

    @abstractmethod
    def fetchDatasetLocations(self, datasets: DatasetIterable) -> Iterator[str]:
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


class QuantumRecordStorage(ABC):

    @abstractmethod
    def insertQuantum(self, quantum: Quantum) -> Quantum:
        pass

    @abstractmethod
    def updateQuantum(self, quantum: Quantum):
        pass

    # TODO: we need methods for fetching and querying (but don't yet have the
    # high-level Registry API).

    dimensions: DimensionGraph


class RegistryBackend(ABC):

    @abstractmethod
    def ensureWriteableGeneralRecordStorage(self):
        pass

    @abstractmethod
    def getGeneralRecordStorage(self) -> GeneralRecordStorage:
        pass

    @abstractmethod
    def registerOpaqueData(self, name: str, spec: TableSpec, *, write: bool = True, ephemeral: bool = False
                           ) -> OpaqueRecordStorage:
        pass

    @abstractmethod
    def fetchOpaqueData(self, name: str) -> OpaqueRecordStorage:
        pass

    @abstractmethod
    def registerDimensionElement(self, element: DimensionElement, *, write: bool = True
                                 ) -> DimensionRecordStorage:
        pass

    @abstractmethod
    def fetchDimensionElement(self, element: DimensionElement) -> DimensionRecordStorage:
        pass

    @abstractmethod
    def getDimensionRecordCache(self, dimensions: DimensionGraph) -> DimensionRecordCache:
        pass

    @abstractmethod
    def registerQuanta(self, dimensions: DimensionGraph, *, write: bool = True) -> QuantumRecordStorage:
        pass

    @abstractmethod
    def registerDatasetType(self, datasetType: DatasetType, *, write: bool = True) -> DatasetRecordStorage:
        pass

    @abstractmethod
    def fetchDatasetType(self, name: str) -> DatasetRecordStorage:
        pass
