from __future__ import annotations

__all__ = ["addQuantumForeignKey", "StaticQuantumTablesTuple", "makeStaticTableSpecs",
           "makeDynamicTableName", "makeDynamicTableSpec"]

from collections import namedtuple
from typing import (
    Any,
    Optional,
    Tuple,
    Type,
)

import sqlalchemy

from ....core.dimensions import DimensionGraph
from ....core.dimensions.schema import TIMESPAN_FIELD_SPECS, addDimensionForeignKey
from ....core.schema import TableSpec, FieldSpec, ForeignKeySpec

from ...interfaces import (
    CollectionManager,
    DatasetTableManager,
)


def addQuantumForeignKey(tableSpec: TableSpec, *, name: Optional[str] = None,
                         onDelete: Optional[str] = None, **kwds: Any) -> Tuple[FieldSpec, FieldSpec]:
    if name is None:
        name = "quantum"
    idFieldSpec = FieldSpec(f"{name}_id", dtype=sqlalchemy.BigInteger, **kwds)
    originFieldSpec = FieldSpec(f"{name}_origin", dtype=sqlalchemy.BigInteger, **kwds)
    tableSpec.fields.add(idFieldSpec)
    tableSpec.fields.add(originFieldSpec)
    tableSpec.foreignKeys.append(ForeignKeySpec("quantum", source=(idFieldSpec.name, originFieldSpec.name),
                                                target=("id", "origin"), onDelete=onDelete))
    return idFieldSpec, originFieldSpec


StaticQuantumTablesTuple = namedtuple("StaticQuantumTablesTuple",
                                      ["quantum_meta", "quantum", "quantum_input"])


def makeStaticTableSpecs(collections: Type[CollectionManager], datasets: Type[DatasetTableManager]
                         ) -> StaticQuantumTablesTuple:
    specs = StaticQuantumTablesTuple(
        quantum_meta=TableSpec(
            fields=[
                FieldSpec("dimensions_encoded", dtype=sqlalchemy.String, length=8, nullable=False),
            ],
        ),
        quantum=TableSpec(
            fields=[
                FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
                FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
                FieldSpec("task", dtype=sqlalchemy.String, length=256),
                TIMESPAN_FIELD_SPECS.begin,
                TIMESPAN_FIELD_SPECS.end,
                FieldSpec("host", dtype=sqlalchemy.String, length=128),
                # Foreign key to run table added below.
            ],
        ),
        quantum_input=TableSpec(
            fields=[
                FieldSpec("input_type", dtype=sqlalchemy.SmallInteger, nullable=False),
                # Foreign keys (which are also primary keys) to the quantum
                # and dataset tables are added below.
            ],
            foreignKeys=[
                ForeignKeySpec("quantum", source=("quantum_id", "quantum_origin"), target=("id", "origin"),
                               onDelete="CASCADE"),
                ForeignKeySpec("dataset", source=("dataset_id", "quantum_origin"), target=("id", "origin"),
                               onDelete="CASCADE"),
            ]
        )
    )
    collections.addRunForeignKey(specs.quantum, nullable=False, onDelete="CASCADE")
    addQuantumForeignKey(specs.quantum_input, primaryKey=True, onDelete="CASCADE")
    datasets.addDatasetForeignKey(specs.quantum_input, primaryKey=True, onDelete="CASCADE")
    return specs


def makeDynamicTableName(dimensions: DimensionGraph) -> str:
    return f"quantum_{dimensions.encode().hex()}"


def makeDynamicTableSpec(dimensions: DimensionGraph) -> TableSpec:
    spec = TableSpec(
        fields=[
            # Foreign keys to quantum table and generally dimension tables
            # added below.
        ]
    )
    addQuantumForeignKey(spec, primaryKey=True, onDelete="CASCADE")
    for dimension in dimensions:
        addDimensionForeignKey(spec, dimension, primaryKey=False,
                               nullable=(dimension in dimensions.required))
    return spec
