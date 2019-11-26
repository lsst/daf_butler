from __future__ import annotations

__all__ = ["GlobalByDimensionsRegistryLayerDatasetRecords"]

from datetime import datetime
from typing import (
    List,
    Optional,
    Union,
    TYPE_CHECKING,
)

import sqlalchemy

from ....core.datasets import DatasetType, ResolvedDatasetHandle
from ....core.dimensions import DataCoordinate
from ....core.dimensions.schema import addDimensionForeignKey
from ....core.schema import TableSpec, FieldSpec, ForeignKeySpec

from ...iterables import DataIdIterable, SingleDatasetTypeIterable
from ...collection import Collection, Run, CollectionType
from ...quantum import Quantum

from .base import ByDimensionsRegistryLayerDatasetRecords, StaticDatasetTablesTuple

if TYPE_CHECKING:
    from ...interfaces import Database


def _makeDynamicTableName(datasetType: DatasetType) -> str:
    return f"dataset_global_{datasetType.dimensions.encode()}"


def _makeDynamicTableSpec(datasetType: DatasetType) -> TableSpec:
    tableSpec = TableSpec(
        fields=[
            FieldSpec("dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_type_id", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("dataset_type_origin", dtype=sqlalchemy.BigInteger, nullable=False),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset", source=("dataset_id", "dataset_origin"),
                           target=("id", "origin"), onDelete="CASCADE"),
            ForeignKeySpec("dataset_type", source=("dataset_type_id", "dataset_type_origin"),
                           target=("id", "origin")),
        ]
    )
    # DatasetTypes with global uniquess have a constraint on just dataset type
    # + data ID.  We only bother including the required part of the data ID, as
    # that's sufficient and saves us from worrying about nulls in the
    # constraint.
    constraint = ["dataset_type_id", "dataset_type_origin"]
    for dimension in datasetType.dimensions.required:
        fieldSpec = addDimensionForeignKey(tableSpec, dimension=dimension, nullable=False, primaryKey=False)
        constraint.append(fieldSpec.name)
    for dimension in datasetType.dimensions.implied:
        addDimensionForeignKey(tableSpec, dimension=dimension, nullable=True, primaryKey=False)
    tableSpec.unique.add(tuple(constraint))
    return tableSpec


class GlobalByDimensionsRegistryLayerDatasetRecords(ByDimensionsRegistryLayerDatasetRecords):

    def __init__(self, *, db: Database, datasetType: DatasetType, static: StaticDatasetTablesTuple,
                 id: int, origin: int, dynamic: sqlalchemy.schema.Table):
        super().__init__(datasetType=datasetType, id=id, origin=origin)
        self._db = db
        self._static = static
        self._dynamic = dynamic

    @classmethod
    def load(cls, *, db: Database, datasetType: DatasetType, static: StaticDatasetTablesTuple,
             id: int, origin: int) -> ByDimensionsRegistryLayerDatasetRecords:
        dynamic = db.getExistingTable(_makeDynamicTableName(datasetType))
        return cls(db=db, datasetType=datasetType, static=static, dynamic=dynamic, id=id, origin=origin)

    @classmethod
    def register(cls, *, db: Database, datasetType: DatasetType, static: StaticDatasetTablesTuple
                 ) -> ByDimensionsRegistryLayerDatasetRecords:
        dynamic = db.ensureTableExists(
            _makeDynamicTableName(datasetType),
            _makeDynamicTableSpec(datasetType),
        )
        row, _ = db.sync(
            static.dataset_type,
            keys={"name": datasetType.name},
            compared={
                "uniqueness": datasetType.uniqueness,
                "dimensions_encoded": datasetType.dimensions.encoded(),
                "storage_class": datasetType.storageClass.name,
            },
            extra={"origin": db.origin},
            returning={"id", "origin"},
        )
        return cls(db=db, datasetType=datasetType, static=static, dynamic=dynamic,
                   id=row["id"], origin=row["origin"])

    def insert(self, run: Run, dataIds: DataIdIterable, *, quantum: Optional[Quantum] = None
               ) -> SingleDatasetTypeIterable:
        staticRow = {
            "origin": self._db.origin,
            "dataset_type_id": self.id,
            "dataset_type_origin": self.origin,
            "run_id": run.id,
            "run_origin": run.origin,
            "quantum_id": quantum.id if quantum is not None else None,
            "quantum_origin": quantum.id if quantum is not None else None,
        }
        dataIds = list(dataIds)
        # Insert into the static dataset table, generating autoincrement
        # dataset_id values.
        datasetIdIterator = self._db.insert(self._static.dataset, *([staticRow]*len(dataIds)),
                                            returning="id")
        # Combine the generated dataset_id values and data ID fields to form
        # rows to be inserted into the dynamic table.
        protoDynamicRow = {
            "origin": self._db.origin,
            "dataset_type_id": self.id,
            "dataset_type_origin": self.origin,
        }
        dynamicRows = [
            dict(protoDynamicRow, dataset_id=dataset_id, **dataId.full.byName())
            for dataId, dataset_id in zip(dataIds, datasetIdIterator)
        ]
        # Insert those rows into the dynamic table.  This is where we'll
        # get any unique constraint violations.
        # TODO: wrap constraint violations from database with a better message.
        # TODO: make sure insertion into static table is rolled back if the
        #       insertions into the dynamic table fail.
        self._db.insert(self._dynamic, *dynamicRows)

    def find(self, collection: Collection, dataId: DataCoordinate) -> Optional[ResolvedDatasetHandle]:
        fromClause = self._static.dataset.join(self._dynamic)
        if collection.type is CollectionType.RUN:
            whereClause = sqlalchemy.sql.and_(
                self._static.dataset.columns.run_id == collection.id,
                self._static.dataset.columns.run_origin == collection.origin
            )
        else:
            # If we ever want runs to be able to have tagged datasets as well,
            # remove the else and OR whereClause with any that already exists.
            fromClause = fromClause.join(self._static.dataset_collection_unconstrained)
            whereClause = sqlalchemy.sql.and_(
                self._static.dataset_collection_unconstrained.columns.collection_id == collection.id,
                self._static.dataset_collection_unconstrained.columns.collection_origin == collection.origin
            )
        sql = sqlalchemy.sql.select(self._static.dataset.columns).select_from(fromClause).where(whereClause)
        row = self._db.connection.execute(sql).fetchone()
        if row is None:
            return row
        return ResolvedDatasetHandle(
            datasetType=self.datasetType,
            dataId=dataId,
            id=row[self._static.dataset.columns.id],
            # TODO: origin=row[self._static.dataset.columns.origin],
            # TODO: run=<get Run from CollectionStorage>
        )

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
    def select(self, collections: Union[List[Collection], ...],
               isResult: bool = True, addRank: bool = False) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    datasetType: DatasetType
