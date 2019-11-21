from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    List,
    Optional,
    Union,
)

import sqlalchemy

from .core.datasets import DatasetType
from .core.quantum import Quantum

from .iterables import DataIdIterable, SingleDatasetTypeIterable
from .run import Run


class DatasetRecordStorage(ABC):

    def __init__(self, datasetType: DatasetType):
        self.datasetType = datasetType

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
    def select(self, collections: Union[List[str], ...],
               isResult: bool = True, addRank: bool = False) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    datasetType: DatasetType
