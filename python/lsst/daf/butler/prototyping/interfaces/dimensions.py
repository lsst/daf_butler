from __future__ import annotations

__all__ = ["DimensionTableRecords", "DimensionTableManager"]

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
from .database import Database, StaticTablesContext


class DimensionTableRecords(ABC):

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


class DimensionTableManager(ABC):

    def __init__(self, *, universe: DimensionUniverse):
        self.universe = universe

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *,
                   universe: DimensionUniverse) -> DimensionTableManager:
        pass

    @abstractmethod
    def refresh(self):
        pass

    @abstractmethod
    def get(self, element: DimensionElement) -> Optional[DimensionTableRecords]:
        pass

    @abstractmethod
    def register(self, element: DimensionElement) -> DimensionTableRecords:
        pass

    universe: DimensionUniverse
