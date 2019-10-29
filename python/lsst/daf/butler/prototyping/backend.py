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
    DimensionUniverse,
)
from .core.datasets import DatasetType, ResolvedDatasetHandle
from .core.run import Run
from .core.quantum import Quantum
from .core.utils import NamedKeyDict
from .core.queries import DatasetTypeExpression, CollectionsExpression

from .iterables import DataIdIterable, SingleDatasetTypeIterable


class DimensionRecordStorage(ABC):

    @abstractmethod
    def matches(self, dataId: Optional[ExpandedDataCoordinate] = None):
        pass

    @abstractmethod
    def insert(self, *records: DimensionRecord):
        pass

    @abstractmethod
    def fetch(self, dataIds: DataIdIterable) -> Iterator[DimensionRecord]:
        pass

    @abstractmethod
    def select(self, dataId: Optional[ExpandedDataCoordinate] = None) -> sqlalchemy.sql.FromClause:
        pass

    @abstractmethod
    def selectSkyPixOverlap(self, dataId: Optional[ExpandedDataCoordinate] = None
                            ) -> sqlalchemy.sql.FromClause:
        pass

    element: DimensionElement


DimensionRecordCache = NamedKeyDict[DimensionElement, Dict[DataCoordinate, DimensionRecord]]


class DatasetRecordStorage(ABC):

    @abstractmethod
    def matches(self, collections: CollectionsExpression = ...,
                dataId: Optional[ExpandedDataCoordinate] = None):
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
               isResult: bool = True, addRank: bool = False) -> sqlalchemy.sql.FromClause:
        pass

    datasetType: DatasetType


class DatastoreRecordStorage(ABC):

    @abstractmethod
    def insertOpaqueData(self, *data: dict):
        pass

    @abstractmethod
    def fetchOpaqueData(self, **where: Any) -> Iterator[dict]:
        pass

    @abstractmethod
    def deleteOpaqueData(self, **where: Any):
        pass

    @abstractmethod
    def insertLocations(self, *datasets: ResolvedDatasetHandle):
        pass

    @abstractmethod
    def fetchLocations(self, *datasets: ResolvedDatasetHandle):
        pass

    @abstractmethod
    def deleteLocations(self, *datasets: ResolvedDatasetHandle):
        pass

    name: str


class RegistryBackend(ABC):

    @abstractmethod
    @classmethod
    def construct(cls, universe: DimensionUniverse, *, write: bool = True):
        pass

    @abstractmethod
    def registerDatastore(self, name: str, spec: TableSpec, *, write: bool = True, ephemeral: bool = False
                          ) -> DatastoreRecordStorage:
        pass

    @abstractmethod
    def fetchDatastore(self, name: str) -> DatastoreRecordStorage:
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
    def ensureRun(self, name: str) -> Run:
        pass

    @abstractmethod
    def updateRun(self, run: Run):
        pass

    @abstractmethod
    def ensureCollection(self, name: str, *, calibration: bool = False):
        pass

    @abstractmethod
    def registerQuanta(self, dimensions: DimensionGraph, *, write: bool = True):
        pass

    @abstractmethod
    def insertQuantum(self, quantum: Quantum) -> Quantum:
        pass

    @abstractmethod
    def updateQuantum(self, quantum: Quantum):
        pass

    @abstractmethod
    def registerDatasetType(self, datasetType: DatasetType, *, write: bool = True) -> DatasetRecordStorage:
        pass

    @abstractmethod
    def fetchDatasetType(self, name: str) -> DatasetRecordStorage:
        pass

    @abstractmethod
    def selectDatasetTypes(self, datasetType: DatasetTypeExpression = ..., *,
                           collections: CollectionsExpression = ...,
                           dataId: Optional[ExpandedDataCoordinate] = None) -> sqlalchemy.sql.FromClause:
        pass

    @abstractmethod
    def selectCollections(self, collection: CollectionsExpression = ..., *,
                          datasetTypes: Union[DatasetTypeExpression, List[DatasetTypeExpression]] = ...,
                          dataId: Optional[ExpandedDataCoordinate] = None) -> sqlalchemy.sql.FromCLause:
        pass
