from __future__ import annotations

__all__ = ["StaticDatasetTablesTuple", "makeDynamicTableName", "makeDynamicTableSpec"]

import sqlalchemy

from ....core.datasets import DatasetType
from ....core.dimensions.schema import addDimensionForeignKey
from ....core.schema import TableSpec, FieldSpec, ForeignKeySpec
from ....core.timespan import TIMESPAN_FIELD_SPECS

from ...interfaces import makeTableStruct


DATASET_TYPE_NAME_LENGTH = 128


# TODO: some of this should be part of the public API of
# DatasetTableManager, but not all - need to add flexibility to
# makeTableStruct to do that.
@makeTableStruct
class StaticDatasetTablesTuple:
    dataset_type = TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
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
            FieldSpec("run_id", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("quantum_id", dtype=sqlalchemy.BigInteger),
            FieldSpec("quantum_origin", dtype=sqlalchemy.BigInteger),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset_type", source=("dataset_type_id",), target=("id", "origin")),
            ForeignKeySpec("run", source=("run_id",), target=("id",)),
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
    dataset_collection_nonsingular = TableSpec(
        fields=[
            FieldSpec("dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("collection_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset", source=("dataset_id", "dataset_origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
            ForeignKeySpec("collection", source=("collection_id",), target=("id",),
                           onDelete="CASCADE"),
        ]
    )
    dataset_collection_calibration = TableSpec(
        fields=[
            FieldSpec("dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("collection_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
        ],
        foreignKeys=[
            ForeignKeySpec("dataset", source=("dataset_id", "dataset_origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
            ForeignKeySpec("collection", source=("collection_id",), target=("id",),
                           onDelete="CASCADE"),
        ]
    )


def makeDynamicTableName(datasetType: DatasetType) -> str:
    return f"dataset_collection_{datasetType.dimensions.encode().hex()}"


def makeDynamicTableSpec(datasetType: DatasetType) -> TableSpec:
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
    constraint = ["dataset_type_id", "collection_id"]
    for dimension in datasetType.dimensions.required:
        fieldSpec = addDimensionForeignKey(tableSpec, dimension=dimension, nullable=False, primaryKey=False)
        constraint.append(fieldSpec.name)
    for dimension in datasetType.dimensions.implied:
        addDimensionForeignKey(tableSpec, dimension=dimension, nullable=True, primaryKey=False)
    # Add a constraint on dataset type + collection + data ID.  We only include
    # the required part of the data ID, as that's sufficient and saves us from
    # worrying about nulls in the constraint.
    tableSpec.unique.add(tuple(constraint))
    return tableSpec
