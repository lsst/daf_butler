from __future__ import annotations

__all__ = ["ByDimensionsQuantumTableRecords"]

import enum
import itertools

import sqlalchemy

from ....core.dimensions import DimensionGraph
from ....core.dimensions.schema import TIMESPAN_FIELD_SPECS

from ...quantum import Quantum
from ...interfaces import (
    CollectionManager,
    CollectionType,
    Database,
    DatasetTableManager,
    QuantumTableRecords,
)

from . import ddl


class QuantumInputType(enum.IntEnum):
    INIT = 1
    NORMAL = 2
    UNUSED = 3


class ByDimensionsQuantumTableRecords(QuantumTableRecords):

    def __init__(self, *, dimensions: DimensionGraph, db: Database,
                 collections: CollectionManager, datasets: DatasetTableManager,
                 static: ddl.StaticQuantumTablesTuple, dynamic: sqlalchemy.schema.Table):
        super().__init__(dimensions=dimensions)
        self._db = db
        self._collections = collections
        self._datasets = datasets
        self._static = static
        self._dynamic = dynamic

    def start(self, quantum: Quantum):
        runRecord = self._collections.find(quantum.run)
        if runRecord is None or runRecord.type is not CollectionType.RUN:
            raise RuntimeError(f"Invalid run '{quantum.run}' in quantum.")
        if quantum.id is None and quantum.origin is None:
            # Insert into the main quantum table with db.origin and generate
            # autoincrement quantum_id.
            quantumOrigin = self._db.origin
            quantumId, = self._db.insert(
                self._static.quantum,
                {
                    "origin": quantumOrigin,
                    "run_id": runRecord.id,
                    "task": quantum.taskName,
                },
                returnIds=True,
            )
        elif quantum.id is not None and quantum.origin is not None:
            # This must be a transfer from another Registry; insert with
            # the given ID and origin.
            quantumId = quantum.id
            quantumOrigin = quantum.origin
            self._db.insert(
                self._static.quantum,
                {
                    "id": quantumId,
                    "origin": quantumOrigin,
                    "run_id": runRecord.id,
                    "task": quantum.taskName,
                },
            )
        else:
            raise RuntimeError(f"Quantum ID is {quantum.id} but origin is {quantum.origin}.")
        # Insert into the dynamic table with dimension columns.
        values = {
            "quantum_id": quantumId,
            "quantum_origin": quantumOrigin,
        }
        for k, v in quantum.dataId.items():
            values[k.name] = v
        self._db.insert(self._dynamic, values)

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

        values = [{"quantum_id": quantumId,
                   "quantum_origin": quantum.origin,
                   "dataset_id": dataset.id,
                   "dataset_origin": dataset.origin,
                   "input_type": inputType}
                  for dataset, inputType in categorizedInputs()]
        self._db.insert(self._static.quantum_input, *values)

        # Update the object we were given last for exception safety.
        quantum.id = quantumId
        quantum.origin = quantumOrigin

    def finish(self, quantum: Quantum):
        assert quantum.id is not None and quantum.origin is not None
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
