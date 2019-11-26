from __future__ import annotations

__all__ = ["ByDimensionsRegistryLayerDatasetStorage"]

from abc import abstractmethod
from typing import (
    Dict,
    Iterator,
    Optional,
    Type,
)

import sqlalchemy

from ....core.datasets import DatasetType, ResolvedDatasetHandle, DatasetUniqueness
from ....core.dimensions import DimensionGraph, DimensionUniverse
from ....core.utils import NamedKeyDict

from ...iterables import DatasetIterable
from ...interfaces import (
    Database,
    RegistryLayerCollectionStorage,
    RegistryLayerDatasetRecords,
    RegistryLayerDatasetStorage,
)
from .base import StaticDatasetTablesTuple, ByDimensionsRegistryLayerDatasetRecords


class ByDimensionsRegistryLayerDatasetStorage(RegistryLayerDatasetStorage):

    def __init__(self, db: Database,
                 RecordClasses: Dict[DatasetUniqueness, Type[ByDimensionsRegistryLayerDatasetRecords]]):
        self._db = db
        self._static = StaticDatasetTablesTuple(db)
        self._RecordClasses = RecordClasses
        self._records = NamedKeyDict({})
        self.refresh()

    @classmethod
    @abstractmethod
    def load(cls, db: Database, collections: RegistryLayerCollectionStorage,
             *, universe: DimensionUniverse) -> RegistryLayerDatasetStorage:
        # TODO: ideally we'd load RecordClasses from configuration, to make
        # this configurable all the way down.  Maybe attach a config to
        # Database?
        pass

    def refresh(self, *, universe: DimensionUniverse):
        records = {}
        c = self._static.dataset_type.columns
        for row in self._layer.db.execute(self._static.dataset_type.select()).fetchall():
            name = row[c.name]
            dimensions = DimensionGraph.decode(row[c.dimensions_encoded], universe=universe)
            uniqueness = DatasetUniqueness(row[c.uniqueness])
            datasetType = DatasetType(name, dimensions, row[c.storage_class], uniqueness=uniqueness)
            records[datasetType] = self._RecordClasses[uniqueness].load(db=self._db, datasetType=datasetType,
                                                                        static=self._static,
                                                                        id=row[c.id], origin=row[c.origin])
        self._records = records

    def get(self, datasetType: DatasetType) -> Optional[RegistryLayerDatasetRecords]:
        return self._records.get(datasetType)

    def register(self, datasetType: DatasetType) -> RegistryLayerDatasetRecords:
        result = self._records.get(datasetType)
        if result is None:
            result = self._RecordClasses[datasetType.uniqueness].register(db=self._db,
                                                                          datasetType=datasetType,
                                                                          static=self._static)
            self._records[datasetType] = result
        return result

    def selectTypes(self) -> sqlalchemy.sql.FromClause:
        return self._static.dataset_type

    def insertLocations(self, datastoreName: str, datasets: DatasetIterable, *,
                        ephemeral: bool = False):
        if ephemeral:
            raise NotImplementedError("Ephemeral datasets are not yet supported.")
        self.db.insert(
            self._static.dataset_location,
            *[{"dataset_id": dataset.id, "dataset_origin": dataset.origin, "datastore_name": datastoreName}
              for dataset in datasets]
        )

    def fetchLocations(self, dataset: ResolvedDatasetHandle) -> Iterator[str]:
        table = self._static.dataset_location
        sql = sqlalchemy.sql.select(
            [table.columns.datastore_name]
        ).select_from(table).where(
            sqlalchemy.sql.and_(
                table.columns.dataset_id == dataset.id,
                table.columns.origin == dataset.origin
            )
        )
        for row in self.db.connection.execute(sql, {"dataset_id": dataset.id,
                                                    "dataset_origin": dataset.origin}):
            yield row[table.columns.datastore_name]

    def deleteLocations(self, datastoreName: str, datasets: DatasetIterable):
        table = self._static.dataset_location
        sql = table.delete().where(
            sqlalchemy.sql.and_(
                table.columns.datastore_name == datastoreName,
                table.columns.dataset_id == sqlalchemy.sql.bindparam("dataset_id"),
                table.columns.origin == sqlalchemy.sql.bindparam("dataset_origin"),
            )
        )
        self.db.connection.execute(
            sql,
            *[{"dataset_id": dataset.id, "dataset_origin": dataset.origin} for dataset in datasets]
        )

    def __iter__(self) -> Iterator[RegistryLayerDatasetRecords]:
        yield from self._records.values()
