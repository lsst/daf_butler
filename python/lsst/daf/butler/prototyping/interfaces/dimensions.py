from __future__ import annotations

__all__ = ["RegistryLayerDimensionRecords", "RegistryLayerDimensionStorage"]

from abc import ABC, abstractmethod
from typing import (
    Optional,
)

import sqlalchemy

from ...core.dimensions import (
    DataCoordinate,
    DimensionElement,
    DimensionRecord,
    DimensionUniverse,
)
from .layer import RegistryLayer


class RegistryLayerDimensionRecords(ABC):

    def __init__(self, element: DimensionElement):
        self.element = element

    @abstractmethod
    def insert(self, *records: DimensionRecord):
        pass

    @abstractmethod
    def fetch(self, dataId: DataCoordinate) -> Optional[DimensionRecord]:
        raise NotImplementedError()

    @abstractmethod
    def select(self) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    @abstractmethod
    def selectCommonSkyPixOverlap(self) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    element: DimensionElement


class RegistryLayerDimensionStorage(ABC):

    def __init__(self, *, universe: DimensionUniverse):
        self.universe = universe

    @classmethod
    @abstractmethod
    def load(cls, layer: RegistryLayer, *, universe: DimensionUniverse) -> RegistryLayerDimensionStorage:
        pass

    @abstractmethod
    def refresh(self):
        pass

    @abstractmethod
    def get(self, element: DimensionElement) -> Optional[RegistryLayerDimensionRecords]:
        pass

    @abstractmethod
    def register(self, element: DimensionElement) -> RegistryLayerDimensionRecords:
        pass

    universe: DimensionUniverse
