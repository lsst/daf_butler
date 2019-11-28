from __future__ import annotations

__all__ = ["StandardByDimensionsRegistryLayerDatasetRecords"]

from datetime import datetime
from typing import (
    Optional,
    Union,
    TYPE_CHECKING,
)

import sqlalchemy

from ....core.datasets import DatasetType, ResolvedDatasetHandle, DatasetUniqueness
from ....core.dimensions import DataCoordinate
from ....core.dimensions.schema import addDimensionForeignKey, TIMESPAN_FIELD_SPECS
from ....core.schema import TableSpec, FieldSpec, ForeignKeySpec

from ...iterables import DataIdIterable, SingleDatasetTypeIterable
from ...quantum import Quantum
from ...interfaces import CollectionType

from .base import ByDimensionsRegistryLayerDatasetRecords, StaticDatasetTablesTuple

if TYPE_CHECKING:
    from ...interfaces import Database, RegistryLayerCollectionStorage


def _makeDynamicTableName(datasetType: DatasetType) -> str:
    return f"dataset_collection_standard_{datasetType.dimensions.encode().hex()}"


def _makeDynamicTableSpec(datasetType: DatasetType) -> TableSpec:
    tableSpec = TableSpec(
        fields=[
            FieldSpec("dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_type_id", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("collection_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset", source=("dataset_id", "dataset_origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
            ForeignKeySpec("dataset_type", source=("dataset_type_id",), target=("id",)),
            ForeignKeySpec("collection", source=("collection_id",), target=("id",),
                           onDelete="CASCADE"),
        ]
    )
    # DatasetTypes with standard uniqueness have a constraint on dataset type +
    # data ID + collection ID.  We only bother including the required part of
    # the data ID, as that's sufficient and saves us from worrying about nulls
    # in the constraint.
    constraint = ["dataset_type_id", "collection_id"]
    for dimension in datasetType.dimensions.required:
        fieldSpec = addDimensionForeignKey(tableSpec, dimension=dimension, nullable=False, primaryKey=False)
        constraint.append(fieldSpec.name)
    for dimension in datasetType.dimensions.implied:
        addDimensionForeignKey(tableSpec, dimension=dimension, nullable=True, primaryKey=False)
    tableSpec.unique.add(tuple(constraint))
    return tableSpec


class StandardByDimensionsRegistryLayerDatasetRecords(ByDimensionsRegistryLayerDatasetRecords):

    def __init__(self, *, db: Database, datasetType: DatasetType, static: StaticDatasetTablesTuple,
                 collections: RegistryLayerCollectionStorage,
                 id: int, dynamic: sqlalchemy.schema.Table):
        super().__init__(datasetType=datasetType, id=id)
        self._db = db
        self._collections = collections
        self._static = static
        self._dynamic = dynamic

    @classmethod
    def load(cls, *, db: Database, datasetType: DatasetType, static: StaticDatasetTablesTuple,
             collections: RegistryLayerCollectionStorage,
             id: int) -> ByDimensionsRegistryLayerDatasetRecords:
        assert datasetType.uniqueness is DatasetUniqueness.GLOBAL
        dynamic = db.getExistingTable(_makeDynamicTableName(datasetType))
        return cls(db=db, datasetType=datasetType, static=static, dynamic=dynamic, id=id,
                   collections=collections)

    @classmethod
    def register(cls, *, db: Database, datasetType: DatasetType, static: StaticDatasetTablesTuple,
                 collections: RegistryLayerCollectionStorage
                 ) -> ByDimensionsRegistryLayerDatasetRecords:
        assert datasetType.uniqueness is DatasetUniqueness.GLOBAL
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
            returning={"id"},
        )
        return cls(db=db, datasetType=datasetType, static=static, dynamic=dynamic,
                   collections=collections, id=row["id"])

    def insert(self, run: str, dataIds: DataIdIterable, *, quantum: Optional[Quantum] = None
               ) -> SingleDatasetTypeIterable:
        runRecord = self._collections.get(run)
        assert runRecord.type is CollectionType.RUN
        staticRow = {
            "origin": self._db.origin,
            "dataset_type_id": self.id,
            "run_id": runRecord.id,
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
            "collection_id": runRecord.id,
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

    def find(self, collection: str, dataId: DataCoordinate) -> Optional[ResolvedDatasetHandle]:
        collectionRecord = self._collections.find(collection)
        assert collectionRecord.type is not CollectionType.CALIBRATION
        whereTerms = [
            self._dynamic.columns.collection_id == collectionRecord.id,
            self._dynamic.columns.dataset_type_id == self.id,
        ]
        for dimension, value in dataId.items():
            whereTerms.append(self._dynamic.columns[dimension.name] == value)
        whereClause = sqlalchemy.sql.and_(*whereTerms)
        sql = self._dynamic.select().where(whereClause)
        row = self._db.connection.execute(sql).fetchone()
        if row is None:
            return row
        return ResolvedDatasetHandle(
            datasetType=self.datasetType,
            dataId=dataId,
            id=row[self._static.dataset.columns.id],
            origin=row[self._static.dataset.columns.origin],
            run=self._collections.get(row[self._static.dataset.columns.run_id]).name
        )

    def getDataId(self, id: int, origin: int) -> DataCoordinate:
        # This query could return multiple rows (one for each tagged collection
        # the dataset is in, plus one for its run collection), and we don't
        # care which of those we get.
        sql = self._dynamic.select().where(
            sqlalchemy.sql.and_(self._dynamic.columns.id == id, self._dynamic.columns.origin == origin)
        ).limit(1)
        row = self._db.connection.execute(sql).fetchone()
        assert row is not None, "Should be guaranteed by caller and foreign key constraints."
        return DataCoordinate.standardize(
            {dimension: row[dimension.name] for dimension in self.datasetType.dimensions.required},
            graph=self.datasetType.dimensions
        )

    def delete(self, datasets: SingleDatasetTypeIterable):
        assert datasets.datasetType == self.datasetType
        # Only delete from common dataset table; ON DELETE CASCADE constraints
        # will handle the rest.
        sql = self._static.dataset.delete().where(
            sqlalchemy.sql.and_(
                self._static.dataset.columns.id == sqlalchemy.sql.bindparam("id"),
                self._static.dataset.columns.origin == sqlalchemy.sql.bindparam("origin"),
            )
        )
        params = [{"id": dataset.id, "origin": dataset.origin} for dataset in datasets]
        self._db.connection.execute(sql, *params)

    def associate(self, collection: str, datasets: SingleDatasetTypeIterable, *,
                  begin: Optional[datetime] = None, end: Optional[datetime] = None):
        assert datasets.datasetType == self.datasetType
        collectionRecord = self._collections.find(collection)
        protoRow = {
            "dataset_type_id": self.id,
            "collection_id": collectionRecord.id,
        }
        if collectionRecord.type is CollectionType.CALIBRATION:
            protoRow[TIMESPAN_FIELD_SPECS.begin.name] = begin
            protoRow[TIMESPAN_FIELD_SPECS.end.name] = end
            table = self._static.dataset_collection_calibration
        elif collectionRecord.type is CollectionType.RUN:
            raise TypeError(f"Cannot associate into run collection '{collection}'.")
        else:
            table = self._dynamic
            if begin is not None or end is not None:
                raise TypeError(f"'{collection}' is not a calibration collection.")
        rows = []
        for dataset in datasets:
            row = dict(protoRow, dataset_id=dataset.id, dataset_origin=dataset.origin)
            for dimension, value in dataset.dataId.items():
                row[dimension.name] = value
            rows.append(row)
        self._db.replace(table, *rows)

    def disassociate(self, collection: str, datasets: SingleDatasetTypeIterable):
        assert datasets.datasetType == self.datasetType
        collectionRecord = self._collections.find(collection)
        assert collectionRecord.type is not CollectionType.RUN
        protoRow = {
            "collection_id": collectionRecord.id,
        }
        rows = [dict(protoRow, dataset_id=dataset.id, dataset_origin=dataset.origin)
                for dataset in datasets]
        if collectionRecord.type is CollectionType.CALIBRATION:
            table = self._static.dataset_collection_calibration
        else:
            table = self._dynamic
        sql = table.delete().where(
            sqlalchemy.sql.and_(
                table.columns.dataset_id == sqlalchemy.sql.bindparam("dataset_id"),
                table.columns.dataset_origin == sqlalchemy.sql.bindparam("dataset_origin"),
            )
        )
        self._db.connection.execute(sql, *rows)

    def select(self, collection: Union[str, ...],
               returnDimensions: bool = True,
               returnId: bool = True,
               returnOrigin: bool = True,
               returnRun: bool = False,
               returnQuantum: bool = False) -> Optional[sqlalchemy.sql.FromClause]:
        fromClause = self._dynamic
        whereTerms = [self._dynamic.columns.dataset_type_id == self.id]
        if collection is not ...:
            collectionRecord = self._collections.find(collection)
            if collectionRecord is None:
                # This layer doesn't know about this collection, so we know
                # we won't find anything here.
                return None
            if collectionRecord.type is CollectionType.TAGGED or collectionRecord.type is CollectionType.RUN:
                table = self._dynamic
                fromClause = fromClause.join(table)
                whereTerms.append(table.columns.collection_id == collectionRecord.id)
            elif collectionRecord.type is CollectionType.CALIBRATION:
                table = self._static.dataset_collection_calibration
                fromClause = fromClause.join(table)  # TODO: probably need explicit onclause
                whereTerms.append(table.columns.collection_id == collectionRecord.id)
            else:
                raise RuntimeError(
                    f"Unrecognized type {collectionRecord.type} for collection '{collection}.'"
                )
        columns = []
        if returnDimensions:
            columns.extend(self._dynamic.columns[dimension.name].label(dimension.name)
                           for dimension in self.datasetType.dimensions)
        if returnQuantum:
            fromClause = fromClause.join(self._static.dataset)
            # Also constrain the dynamic table's dataset_type_id; that's
            # redundant, but it may help the query optimizer.
            whereTerms.append(self._static_dataset.columns.dataset_type_id == self.id)
            columns.append(self._static.dataset.columns.quantum_id.label("quantum_id"))
        if returnId:
            columns.append(self._dynamic.columns.id.label("dataset_id"))
        if returnOrigin:
            columns.append(self._dynamic.columns.origin.label("dataset_origin"))
        if returnRun:
            columns.append(self._dynamic.columns.run_id.label("run_id"))
        return sqlalchemy.sql.select(columns).select_from(fromClause).where(sqlalchemy.sql.and_(*whereTerms))
