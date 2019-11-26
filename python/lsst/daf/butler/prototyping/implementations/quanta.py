from __future__ import annotations

__all__ = ["ByDimensionHashRegistryLayerQuantumRecords", "ByDimensionHashRegistryLayerQuantumStorage"]

from collections import defaultdict
import enum
import hashlib
import itertools
from typing import (
    Optional,
    TYPE_CHECKING
)

import sqlalchemy

from ..core.dimensions import DimensionGraph, DimensionUniverse
from ..core.dimensions.schema import TIMESPAN_FIELD_SPECS, addDimensionForeignKey
from ..core.schema import TableSpec, FieldSpec, Base64Bytes, ForeignKeySpec

from ..interfaces import makeTableStruct, RegistryLayerQuantumRecords, RegistryLayerQuantumStorage
from ..quantum import Quantum

if TYPE_CHECKING:
    from .registryLayer import RegistryLayer


class QuantumInputType(enum.IntEnum):
    INIT = 1
    NORMAL = 2
    UNUSED = 3


@makeTableStruct
class StaticQuantumTablesTuple:
    layer_meta_quantum = TableSpec(
        fields=[
            FieldSpec("dimensions_hash", dtype=sqlalchemy.String, length=32, primaryKey=True),
            FieldSpec("dimension_name", dtype=Base64Bytes, nbytes=32, primaryKey=True),
        ],
    )
    quantum = TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("run_id", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("run_origin", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("task", dtype=sqlalchemy.String, length=256),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            FieldSpec("host", dtype=sqlalchemy.String, length=128),
        ],
        foreignKeys=[
            ForeignKeySpec("run", source=("run_id", "run_origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
        ]
    )
    quantum_input = TableSpec(
        fields=[
            FieldSpec("quantum_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("quantum_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("input_type", dtype=sqlalchemy.SmallInteger, nullable=False),
        ],
        foreignKeys=[
            ForeignKeySpec("quantum", source=("quantum_id", "quantum_origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
            ForeignKeySpec("dataset", source=("dataset_id", "quantum_origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
        ]
    )


class ByDimensionHashRegistryLayerQuantumRecords(RegistryLayerQuantumRecords):

    def __init__(self, *, dimensions: DimensionGraph, layer: RegistryLayer,
                 static: StaticQuantumTablesTuple, dynamic: sqlalchemy.schema.Table):
        super().__init__(dimensions=dimensions)
        self._layer = layer
        self._static = static
        self._dynamic = dynamic

    def start(self, quantum: Quantum) -> Quantum:
        assert quantum.id is None and quantum.origin is None
        # Insert into the main quantum table.
        c = self._static.quantum.columns
        quantum_id, = self._layer.db.insert(
            self._static.quantum,
            {
                c.origin: self._layer.db.origin,
                c.run_id: quantum.run.id,
                c.run_origin: quantum.run.origin,
                c.task: quantum.taskName,
            },
            returning={"id"},
        )
        # Insert into the dynamic table with dimension columns.
        values = {
            self._dynamic.columns.quantum_id: quantum_id,
            self._dynamic.columns.quantum_origin: self._layer.db.origin,
        }
        for k, v in quantum.dataId.items():
            values[self._dynamic.columns[k.name]] = v
        self._layer.db.insert(
            self._dynamic,
            values
        )

        # Insert into the quantum_input table.

        def categorizedInputs():
            # TODO: should we check that the datasets are resolved, and attempt
            # to resolve them ourselves if they aren't?  Depends on what the
            # usage pattern looks like in PipelineTask execution.
            # For now we just assume they are resolved.
            for dataset in quantum.initInputs.values():
                yield dataset, QuantumInputType.INIT
            for dataset in itertools.chain.from_iterable(quantum.predictedInputs.values()):
                yield dataset, QuantumInputType.NORMAL

        c = self._static.quantum_input.columns
        values = [{c.quantum_id: quantum.id,
                   c.quantum_origin: quantum.origin,
                   c.dataset_id: dataset.id,
                   c.dataset_origin: dataset.origin,
                   c.input_type: inputType}
                  for dataset, inputType in categorizedInputs()]
        self._layer.db.insert(
            self._static.quantum_input,
            *values
        )

        quantum.id = quantum_id
        quantum.origin = self._layer.db.origin
        return quantum

    def finish(self, quantum: Quantum):
        assert quantum.id is not None and quantum.origin == self._layer.db.origin
        # Update the timespan and host in the quantum table.
        c = self._static.quantum.columns
        sql = self._static.quantum.update().where(
            sqlalchemy.sql.and_(c.id == quantum.id, c.origin == quantum.origin)
        ).values({
            c[TIMESPAN_FIELD_SPECS.begin.name]: quantum.timespan.begin,
            c[TIMESPAN_FIELD_SPECS.end.name]: quantum.timespan.end,
            c.host: quantum.host,
        })
        self._layer.db.connection.execute(sql)
        c = self._static.quantum_input.columns
        predicted = set(itertools.chain.from_iterable(quantum.predictedInputs.values()))
        unused = predicted - set(itertools.chain.from_iterable(quantum.actualInputs.values()))
        sql = self._static.quantum_input.update().where(
            sqlalchemy.sql.and_(
                c.quantum_id == quantum.id,
                c.quantum_origin == quantum.origin,
                c.dataset_id == sqlalchemy.sql.bindparam("dataset_id"),
                c.dataset_origin == sqlalchemy.sql.bindparam("dataset_origin"),
            )
        ).values({
            c.input_type: QuantumInputType.UNUSED
        })
        self._layer.db.connection.execute(
            sql,
            *[{"dataset_id": dataset.id, "dataset_origin": dataset.origin} for dataset in unused]
        )

    # TODO: we need methods for fetching and querying (but don't yet have the
    # high-level Registry API).

    dimensions: DimensionGraph


def _hashQuantumDimensions(dimensions: DimensionGraph) -> bytes:
    message = hashlib.blake2b(digest_size=16)
    dimensions.fingerprint(message.update)
    return message.digest().hex()


_DYNAMIC_QUANTUM_TABLE_NAME_FORMAT = "quantum_{}"


def _makeDynamicQuantumTableSpec(dimensions: DimensionGraph) -> TableSpec:
    spec = TableSpec(
        fields=[
            FieldSpec("quantum_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
        ],
        foreignKeys=[
            ForeignKeySpec("quantum", source=("quantum_id", "origin"), target=("id", "origin")),
        ]
    )
    for dimension in dimensions:
        addDimensionForeignKey(spec, dimension, primaryKey=False,
                               nullable=(dimension in dimensions.required))
    return spec


class ByDimensionHashRegistryLayerQuantumStorage(RegistryLayerQuantumStorage):

    def __init__(self, layer: RegistryLayer, *, universe: DimensionUniverse):
        self._layer = layer
        self._static = StaticQuantumTablesTuple(layer.db)
        self._records = {}
        self.refresh(universe=universe)

    @classmethod
    def load(cls, layer: RegistryLayer, *, universe: DimensionUniverse) -> RegistryLayerQuantumStorage:
        return cls(layer=layer, universe=universe)

    def refresh(self, *, universe: DimensionUniverse):
        # Meta table stores the hashes used to identify a table along with the
        # dimensions in the table (one row for each combination).
        # Query for everything, then group by hash.
        meta = self._static.layer_meta_quantum
        dimensionsByHash = defaultdict(list)
        for row in self._layer.db.connection.execute(meta.select()).fetchall():
            dimensionsByHash[row[meta.columns.dimensions_hash]].append(row[meta.columns.dimension_name])
        # Iterate over those groups and construct managers for each table.
        records = {}
        for dimensionsHash, dimensionNames in dimensionsByHash.items():
            dimensions = DimensionGraph(universe=universe, names=dimensionNames)
            if dimensionsHash != _hashQuantumDimensions(dimensions):
                raise RuntimeError(f"Bad dimensions hash: {dimensionsHash}.  "
                                   f"Registry database may be corrupted.")
            table = self._layer.db.getExistingTable(_DYNAMIC_QUANTUM_TABLE_NAME_FORMAT.format(dimensionsHash))
            records[dimensions] = ByDimensionHashRegistryLayerQuantumRecords(dimensions=dimensions,
                                                                             layer=self._layer,
                                                                             static=self._static,
                                                                             dynamic=table)
        self._records = records

    def get(self, dimensions: DimensionGraph) -> Optional[RegistryLayerQuantumRecords]:
        return self._records.get(dimensions)

    def register(self, dimensions: DimensionGraph) -> RegistryLayerQuantumRecords:
        result = self._records.get(dimensions)
        if result is None:
            dimensionsHash = _hashQuantumDimensions(dimensions)
            tableName = _DYNAMIC_QUANTUM_TABLE_NAME_FORMAT.format(dimensionsHash)
            # Create the table itself.  If it already exists but wasn't in
            # the dict because it was added by another client since this one
            # was initialized, that's fine.
            table = self._layer.db.ensureTableExists(tableName, _makeDynamicQuantumTableSpec(dimensions))
            # Add rows to the layer_meta table so we can find this table in
            # the future.  Also okay if those already exist, so we use sync.
            for dimension in dimensions:
                self._layer.db.sync(self._static.layer_meta_quantum,
                                    keys={"dimensions_hash": dimensionsHash,
                                          "dimension_name": dimension.name})
            result = ByDimensionHashRegistryLayerQuantumRecords(dimensions=dimensions, db=self._layer.db,
                                                                static=self._static, dynamic=table)
            self._records[dimensions] = result
        return result
