from __future__ import annotations

__all__ = ["ByDimensionsDatasetTableManager"]

from abc import abstractmethod
from typing import (
    Iterator,
    Optional,
    Tuple,
    Type,
)

import sqlalchemy

from ....core.datasets import DatasetType, ResolvedDatasetHandle, DatasetUniqueness
from ....core.dimensions import DimensionGraph, DimensionUniverse
from ....core.schema import TableSpec, FieldSpec

from ...iterables import DatasetIterable
from ...interfaces import (
    Database,
    CollectionManager,
    DatasetTableRecords,
    DatasetTableManager,
    QuantumTableManager,
    StaticTablesContext,
)
from . import ddl
from .records import ByDimensionsDatasetTableRecords


class ByDimensionsDatasetTableManager(DatasetTableManager):

    def __init__(self, *, db: Database, collections: CollectionManager, static: ddl.StaticDatasetTablesTuple,
                 universe: DimensionUniverse):
        self._db = db
        self._collections = collections
        self._static = static
        self._byName = {}
        self._byId = {}
        self.refresh(universe=universe)

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *, collections: CollectionManager,
                   quanta: Type[QuantumTableManager], universe: DimensionUniverse) -> DatasetTableManager:
        specs = ddl.makeStaticTableSpecs(type(collections), quanta)
        static = context.addTableTuple(specs)
        return cls(db=db, collections=collections, static=static)

    @classmethod
    def addDatasetForeignKey(cls, tableSpec: TableSpec, *, name: str = "dataset",
                             onDelete: Optional[str] = None, **kwds) -> Tuple[FieldSpec, FieldSpec]:
        return ddl.addDatasetForeignKey(tableSpec, name=name, onDelete=onDelete, **kwds)

    def refreshTypes(self, *, universe: DimensionUniverse):
        byName = {}
        byId = {}
        c = self._static.dataset_type.columns
        for row in self._layer.db.execute(self._static.dataset_type.select()).fetchall():
            name = row[c.name]
            dimensions = DimensionGraph.decode(row[c.dimensions_encoded], universe=universe)
            uniqueness = DatasetUniqueness(row[c.uniqueness])
            datasetType = DatasetType(name, dimensions, row[c.storage_class], uniqueness=uniqueness)
            dynamic = self._db.getExistingTable(ddl.makeDynamicTableName(datasetType),
                                                ddl.makeDynamicTableSpec(datasetType,
                                                                         type(self._collections)))
            records = ByDimensionsDatasetTableRecords(db=self._db, datasetType=datasetType,
                                                      static=self._static, dynamic=dynamic,
                                                      id=row["id"])
            byName[name] = records
            byId[records.id] = records
        self._byName = byName
        self._byId = byId

    def getType(self, datasetType: DatasetType) -> Optional[DatasetTableRecords]:
        return self._records._byName(datasetType.name)

    def registerType(self, datasetType: DatasetType) -> DatasetTableRecords:
        records = self._records.get(datasetType)
        if records is None:
            dynamic = self._db.ensureTableExists(
                ddl.makeDynamicTableName(datasetType),
                ddl.makeDynamicTableSpec(datasetType, type(self._collections)),
            )
            row, _ = self._db.sync(
                self._static.dataset_type,
                keys={"name": datasetType.name},
                compared={
                    "uniqueness": datasetType.uniqueness,
                    "dimensions_encoded": datasetType.dimensions.encoded(),
                    "storage_class": datasetType.storageClass.name,
                },
                returning={"id"},
            )
            records = ByDimensionsDatasetTableRecords(db=self._db, datasetType=datasetType,
                                                      static=self._static, dynamic=dynamic,
                                                      id=row["id"])
            self._byName[datasetType.name] = records
            self._byId[records.id] = records
        return records

    def selectTypes(self) -> sqlalchemy.sql.FromClause:
        return self._static.dataset_type

    def iterTypes(self) -> Iterator[DatasetType]:
        yield from self._records.keys()

    def getHandle(self, id: int, origin: int, *, collections: CollectionManager
                  ) -> Optional[ResolvedDatasetHandle]:
        sql = self._static.dataset.select().where(
            sqlalchemy.sql.and_(self._static.dataset.columns.id == id,
                                self._static.dataset.columns.origin == origin)
        )
        row = self._db.query(sql).fetchone()
        if row is None:
            return None
        recordsForType = self._byId.get(row[self._static.dataset.columns.dataset_type_id])
        if recordsForType is None:
            self.refresh()
            recordsForType = self._byId.get(row[self._static.dataset.columns.dataset_type_id])
            assert recordsForType is not None, "Should be guaranteed by foreign key constraints."
        return ResolvedDatasetHandle(
            recordsForType.datasetType,
            dataId=recordsForType.getDataId(id=id, origin=origin),
            id=id, origin=origin,
            run=collections.get(row[self._static.dataset.columns.run_id]).name
        )

    def insertLocations(self, datastoreName: str, datasets: DatasetIterable, *,
                        ephemeral: bool = False):
        if ephemeral:
            raise NotImplementedError("Ephemeral datasets are not yet supported.")
        self._db.insert(
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
        for row in self._db.query(sql, {"dataset_id": dataset.id, "dataset_origin": dataset.origin}):
            yield row[table.columns.datastore_name]

    def deleteLocations(self, datastoreName: str, datasets: DatasetIterable):
        table = self._static.dataset_location
        self._db.delete(
            table,
            *[{"dataset_id": dataset.id, "dataset_origin": dataset.origin, "datastore_name": datastoreName}
              for dataset in datasets],
            columns=["datastore_name", "dataset_id", "dataset_origin"]
        )
