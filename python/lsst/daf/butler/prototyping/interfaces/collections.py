from __future__ import annotations

__all__ = ["RegistryLayerCollectionStorage"]

from abc import ABC, abstractmethod
from typing import (
    Optional,
    TYPE_CHECKING,
)

import sqlalchemy

if TYPE_CHECKING:
    from ..collection import Run, Collection
    from .database import Database


class RegistryLayerCollectionStorage(ABC):

    @classmethod
    @abstractmethod
    def load(cls, db: Database) -> RegistryLayerCollectionStorage:
        pass

    @abstractmethod
    def sync(self, collection: Collection) -> Collection:
        pass

    @abstractmethod
    def find(self, name: str) -> Optional[Collection]:
        pass

    @abstractmethod
    def get(self, id: int, origin: int) -> Optional[Collection]:
        pass

    @abstractmethod
    def finish(self, run: Run):
        pass

    @abstractmethod
    def select(self) -> sqlalchemy.sql.FromClause:
        pass
