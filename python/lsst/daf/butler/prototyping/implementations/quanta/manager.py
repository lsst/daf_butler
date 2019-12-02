from __future__ import annotations

__all__ = ["ByDimensionsQuantumTableManager"]

from typing import (
    Any,
    Optional,
    Tuple,
)

from ....core.dimensions import DimensionGraph, DimensionUniverse
from ....core.schema import TableSpec, FieldSpec

from ...interfaces import (
    CollectionManager,
    Database,
    DatasetTableManager,
    QuantumTableRecords,
    QuantumTableManager,
    StaticTablesContext,
)

from . import ddl
from .records import ByDimensionsQuantumTableRecords


class ByDimensionsQuantumTableManager(QuantumTableManager):

    def __init__(self, db: Database, static: ddl.StaticQuantumTablesTuple, *, collections: CollectionManager,
                 datasets: DatasetTableManager):
        self._db = db
        self._static = static
        self._records = {}
        self._datasets = datasets
        self._collections = collections

    @classmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *, collections: CollectionManager,
                   datasets: DatasetTableManager, universe: DimensionUniverse) -> QuantumTableManager:
        specs = ddl.makeStaticTableSpecs(type(collections), type(datasets))
        static = context.addTableTuple(specs)
        return cls(db, static=static, collections=collections, datasets=datasets)

    @classmethod
    def addQuantumForeignKey(cls, tableSpec: TableSpec, *, name: Optional[str] = None,
                             onDelete: Optional[str] = None, **kwds: Any) -> Tuple[FieldSpec, FieldSpec]:
        return ddl.addQuantumForeignKey(tableSpec, name=name, onDelete=onDelete, **kwds)

    def get(self, dimensions: DimensionGraph) -> Optional[QuantumTableRecords]:
        return self._records.get(dimensions)

    def register(self, dimensions: DimensionGraph) -> QuantumTableRecords:
        result = self._records.get(dimensions)
        if result is None:
            tableName = ddl.makeDynamicTableName(dimensions)
            tableSpec = ddl.makeDynamicTableSpec(dimensions)
            # Create the table itself.  If it already exists but wasn't in
            # the dict because it was added by another client since this one
            # was initialized, that's fine.
            table = self._layer.db.ensureTableExists(tableName, tableSpec)
            # Add a row to the layer_meta table so we can find this table in
            # the future.  Also okay if that already exists, so we use sync.
            self._layer.db.sync(self._static.quantum_meta,
                                keys={"dimensions_encoded": dimensions.encode().hex()})
            result = ByDimensionsQuantumTableRecords(dimensions=dimensions, db=self._layer.db,
                                                     static=self._static, dynamic=table,
                                                     collections=self._collections, datasets=self._datasets)
            self._records[dimensions] = result
        return result
