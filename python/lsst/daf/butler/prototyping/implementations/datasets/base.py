from __future__ import annotations

__all__ = ["StaticDatasetTablesTuple", "ByDimensionsRegistryLayerDatasetRecords"]

from abc import abstractmethod

import sqlalchemy

from ...core.datasets import DatasetType
from ...core.schema import TableSpec, FieldSpec, ForeignKeySpec

from ...interfaces import (
    Database,
    RegistryLayerDatasetRecords,
    RegistryLayerCollectionStorage,
    makeTableStruct,
)

DATASET_TYPE_NAME_LENGTH = 128


@makeTableStruct
class StaticDatasetTablesTuple:
    dataset_type = TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("name", dtype=sqlalchemy.String, length=DATASET_TYPE_NAME_LENGTH),
            FieldSpec("storage_class", dtype=sqlalchemy.String, length=64, nullable=False),
            FieldSpec("dimensions_encoded", dtype=sqlalchemy.String, length=8, nullable=False),
            FieldSpec("uniqueness", dtype=sqlalchemy.SmallInteger, nullable=False),
        ],
    )
    dataset = TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_type_id", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("dataset_type_origin", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("run_id", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("run_origin", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("quantum_id", dtype=sqlalchemy.BigInteger),
            FieldSpec("quantum_origin", dtype=sqlalchemy.BigInteger),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset_type", source=("dataset_type_id", "dataset_type_origin"),
                           target=("id", "origin")),
            ForeignKeySpec("run", source=("run_id", "run_origin"), target=("id", "origin")),
            ForeignKeySpec("quantum", source=("quantum_id", "quantum_origin"), target=("id", "origin"),
                           onDelete="SET NULL"),
        ]
    )
    dataset_composition = TableSpec(
        fields=[
            FieldSpec("parent_dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("parent_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("component_dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("component_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("component_name", dtype=sqlalchemy.String, length=32),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset", source=("parent_dataset_id", "parent_origin"),
                           target=("id", "origin"), onDelete="CASCADE"),
            ForeignKeySpec("dataset", source=("component_dataset_id", "component_origin"),
                           target=("id", "origin"), onDelete="CASCADE"),
        ]
    )
    dataset_location = TableSpec(
        fields=[
            FieldSpec("dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("datastore_name", dtype=sqlalchemy.String, length=256, primaryKey=True),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset", source=("dataset_id", "dataset_origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
        ]
    )
    dataset_collection_unconstrained = TableSpec(
        fields=[
            FieldSpec("dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("collection_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("collection_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset", source=("dataset_id", "dataset_origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
            ForeignKeySpec("collection", source=("collection_id", "collection_origin"),
                           target=("id", "origin"),
                           onDelete="CASCADE"),
        ]
    )


class ByDimensionsRegistryLayerDatasetRecords(RegistryLayerDatasetRecords):

    def __init__(self, *, datasetType: DatasetType, id: int, origin: int):
        super().__init__(datasetType=datasetType)
        self.id = id
        self.origin = origin

    @classmethod
    @abstractmethod
    def load(cls, *, db: Database, datasetType: DatasetType, static: StaticDatasetTablesTuple,
             id: int, origin: int) -> ByDimensionsRegistryLayerDatasetRecords:
        pass

    @classmethod
    @abstractmethod
    def register(cls, *, db: Database, datasetType: DatasetType, static: StaticDatasetTablesTuple
                 ) -> ByDimensionsRegistryLayerDatasetRecords:
        pass

    id: int

    origin: int
