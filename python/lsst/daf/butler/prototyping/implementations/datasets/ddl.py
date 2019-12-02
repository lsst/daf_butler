from __future__ import annotations

__all__ = ["StaticDatasetTablesTuple", "addDatasetForeignKey", "makeStaticTableSpecs",
           "makeDynamicTableName", "makeDynamicTableSpec"]

from typing import (
    Optional,
    Tuple,
    Type,
)

from collections import namedtuple

import sqlalchemy

from ....core.datasets import DatasetType
from ....core.dimensions.schema import addDimensionForeignKey
from ....core.schema import TableSpec, FieldSpec, ForeignKeySpec
from ....core.timespan import TIMESPAN_FIELD_SPECS

from ...interfaces import CollectionManager, QuantumTableManager


DATASET_TYPE_NAME_LENGTH = 128

StaticDatasetTablesTuple = namedtuple(
    "StaticDatasetTablesTuple",
    ["dataset_type", "dataset", "dataset_composition", "dataset_location",
     "dataset_collection_nonsingular", "dataset_collection_calibration"]
)


def addDatasetForeignKey(tableSpec: TableSpec, *, name: Optional[str] = None, onDelete: Optional[str] = None,
                         **kwds) -> Tuple[FieldSpec, FieldSpec]:
    if name is None:
        name = "dataset"
    idFieldSpec = FieldSpec(f"{name}_id", dtype=sqlalchemy.BigInteger, **kwds)
    originFieldSpec = FieldSpec(f"{name}_origin", dtype=sqlalchemy.BigInteger, **kwds)
    tableSpec.fields.add(idFieldSpec)
    tableSpec.fields.add(originFieldSpec)
    tableSpec.foreignKeys.append(ForeignKeySpec("dataset", source=(idFieldSpec.name, originFieldSpec.name),
                                                target=("id", "origin"), onDelete=onDelete))
    return idFieldSpec, originFieldSpec


def makeStaticTableSpecs(collections: Type[CollectionManager], quanta: Type[QuantumTableManager]
                         ) -> StaticDatasetTablesTuple:
    specs = StaticDatasetTablesTuple(
        dataset_type=TableSpec(
            fields=[
                FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
                FieldSpec("name", dtype=sqlalchemy.String, length=DATASET_TYPE_NAME_LENGTH),
                FieldSpec("storage_class", dtype=sqlalchemy.String, length=64, nullable=False),
                FieldSpec("dimensions_encoded", dtype=sqlalchemy.String, length=8, nullable=False),
                FieldSpec("uniqueness", dtype=sqlalchemy.SmallInteger, nullable=False),
            ],
        ),
        dataset=TableSpec(
            fields=[
                FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
                FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
                FieldSpec("dataset_type_id", dtype=sqlalchemy.BigInteger, nullable=False),
                # Foreign keys to run, quantum added below.
            ],
            foreignKeys=[
                ForeignKeySpec("dataset_type", source=("dataset_type_id",), target=("id", "origin")),
            ]
        ),
        dataset_composition=TableSpec(
            fields=[
                # Foreign keys to dataset added below (these are also primary
                # keys).
                FieldSpec("component_name", dtype=sqlalchemy.String, length=32),
            ],
        ),
        dataset_location=TableSpec(
            fields=[
                # Foreign key to dataset added below (this is also part of the
                # primary key).
                FieldSpec("datastore_name", dtype=sqlalchemy.String, length=256, primaryKey=True),
            ],
        ),
        dataset_collection_nonsingular=TableSpec(
            fields=[
                # Foreign keys to dataset and collection added below (together,
                # those are the compound primary key).
            ],
        ),
        dataset_collection_calibration=TableSpec(
            fields=[
                # Foreign keys to dataset and collection added below (together,
                # those are the compound primary key).
                TIMESPAN_FIELD_SPECS.begin,
                TIMESPAN_FIELD_SPECS.end,
            ],
        )
    )
    # Add foreign key fields programmatically.
    collections.addRunForeignKey(specs.dataset, onDelete="CASCADE", nullable=False)
    quanta.addQuantumForeignKey(specs.dataset, onDelete="SET NULL", nullable=True)
    addDatasetForeignKey(specs.dataset_composition, name="parent_dataset", onDelete="CASCADE",
                         primaryKey=True)
    addDatasetForeignKey(specs.dataset_composition, name="component_dataset", onDelete="CASCADE",
                         primartKey=True)
    # No onDelete for dataset_location, because deletion from Datastore must
    # be explicit.
    addDatasetForeignKey(specs.dataset_location, primaryKey=True)
    addDatasetForeignKey(specs.dataset_collection_nonsingular, primaryKey=True, onDelete="CASCADE")
    collections.addCollectionForeignKey(specs.dataset_collection_nonsingular, primaryKey=True,
                                        onDelete="CASCADE")
    addDatasetForeignKey(specs.dataset_collection_calibration, primaryKey=True, onDelete="CASCADE")
    collections.addCollectionForeignKey(specs.dataset_collection_calibration, primaryKey=True,
                                        onDelete="CASCADE")
    return specs


def makeDynamicTableName(datasetType: DatasetType) -> str:
    return f"dataset_collection_{datasetType.dimensions.encode().hex()}"


def makeDynamicTableSpec(datasetType: DatasetType, collections: Type[CollectionManager]) -> TableSpec:
    tableSpec = TableSpec(
        fields=[
            # Foreign key fields to dataset, collection, and usually dimension
            # tables added below.
            FieldSpec("dataset_type_id", dtype=sqlalchemy.BigInteger, nullable=False),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset_type", source=("dataset_type_id",), target=("id",)),
        ]
    )
    # We'll also have a unique constraint on dataset type, collection, and data
    # ID.  We only include the required part of the data ID, as that's
    # sufficient and saves us from worrying about nulls in the constraint.
    constraint = ["dataset_type_id"]
    # Add foreign key fields to dataset table (part of the primary key)
    addDatasetForeignKey(tableSpec, primaryKey=True)
    # Add foreign key fields to collection tablee (part of the primary key and
    # the data ID unique constraint).
    fieldSpec = collections.addCollectionForeignKey(tableSpec, primaryKey=True)
    constraint.append(fieldSpec)
    for dimension in datasetType.dimensions.required:
        fieldSpec = addDimensionForeignKey(tableSpec, dimension=dimension, nullable=False, primaryKey=False)
        constraint.append(fieldSpec.name)
    for dimension in datasetType.dimensions.implied:
        addDimensionForeignKey(tableSpec, dimension=dimension, nullable=True, primaryKey=False)
    # Actually add the unique constraint.
    tableSpec.unique.add(tuple(constraint))
    return tableSpec
