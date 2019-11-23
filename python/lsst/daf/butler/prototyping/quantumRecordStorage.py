from __future__ import annotations

__all__ = ["QuantumRecordStorage", "DatabaseQuantumRecordStorage"]

from abc import ABC, abstractmethod
from collections import defaultdict
import hashlib
from typing import (
    Optional,
    TYPE_CHECKING
)

import sqlalchemy

from ..core.dimensions import DimensionGraph, DimensionUniverse
from ..core.dimensions.schema import TIMESPAN_FIELD_SPECS, addDimensionForeignKey
from ..core.schema import TableSpec, FieldSpec, Base64Bytes, ForeignKeySpec

from .databaseLayer import makeTableStruct
from .quantum import Quantum

if TYPE_CHECKING:
    from .registryLayer import RegistryLayer
    from .databaseLayer import DatabaseLayer


class QuantumRecordStorage(ABC):

    def __init__(self, dimensions: DimensionGraph):
        self.dimensions = dimensions

    @abstractmethod
    def insertQuantum(self, quantum: Quantum) -> Quantum:
        pass

    @abstractmethod
    def updateQuantum(self, quantum: Quantum):
        pass

    # TODO: we need methods for fetching and querying (but don't yet have the
    # high-level Registry API).

    dimensions: DimensionGraph


class QuantumRecordStorageManager(ABC):

    @classmethod
    @abstractmethod
    def load(cls, parent: RegistryLayer, *, universe: DimensionUniverse) -> QuantumRecordStorageManager:
        pass

    @abstractmethod
    def refresh(self, *, universe: DimensionUniverse):
        pass

    @abstractmethod
    def get(self, dimensions: DimensionGraph) -> Optional[QuantumRecordStorage]:
        pass

    @abstractmethod
    def register(self, dimensions: DimensionGraph) -> QuantumRecordStorage:
        pass


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
            FieldSpec("run_collection_id", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("task", dtype=sqlalchemy.String, length=256),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            FieldSpec("host", dtype=sqlalchemy.String, length=128),
        ],
        foreignKeys=[
            ForeignKeySpec("run", source=("run_collection_id", "origin"), target=("collection_id", "origin"),
                           onDelete="CASCADE"),
        ]
    )
    quantum_input = TableSpec(
        fields=[
            FieldSpec("quantum_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("actual", dtype=sqlalchemy.Boolean, nullable=False),
        ],
        foreignKeys=[
            ForeignKeySpec("quantum", source=("quantum_id", "origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
            ForeignKeySpec("dataset", source=("dataset_id", "origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
        ]
    )


class DatabaseQuantumRecordStorage(QuantumRecordStorage):

    def __init__(self, *, dimensions: DimensionGraph, parent: RegistryLayer,
                 static: StaticQuantumTablesTuple, dynamic: sqlalchemy.schema.Table):
        super().__init__(dimensions=dimensions)
        self._parent = parent
        self._static = static
        self._dynamic = dynamic

    def insertQuantum(self, quantum: Quantum) -> Quantum:
        pass

    def updateQuantum(self, quantum: Quantum):
        pass

    # TODO: we need methods for fetching and querying (but don't yet have the
    # high-level Registry API).

    @property
    def db(self) -> DatabaseLayer:
        return self._parent.db

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


class DatabaseQuantumRecordStorageManager(QuantumRecordStorageManager):

    def __init__(self, parent: RegistryLayer, *, universe: DimensionUniverse):
        self._parent = parent
        self._static = StaticQuantumTablesTuple(parent.db)
        self._managed = {}
        self.refresh(universe=universe)

    @classmethod
    def load(cls, parent: RegistryLayer, *, universe: DimensionUniverse) -> QuantumRecordStorageManager:
        return cls(parent=parent, universe=universe)

    def refresh(self, *, universe: DimensionUniverse):
        # Meta table stores the hashes used to identify a table along with the
        # dimensions in the table (one row for each combination).
        # Query for everything, then group by hash.
        meta = self._static.layer_meta_quantum
        dimensionsByHash = defaultdict(list)
        for row in self.db.connection.execute(meta.select()).fetchall():
            dimensionsByHash[row[meta.columns.dimensions_hash]].append(row[meta.columns.dimension_name])
        # Iterate over those groups and construct managers for each table.
        result = {}
        for dimensionsHash, dimensionNames in dimensionsByHash.items():
            dimensions = DimensionGraph(universe=universe, names=dimensionNames)
            if dimensionsHash != _hashQuantumDimensions(dimensions):
                raise RuntimeError(f"Bad dimensions hash: {dimensionsHash}.  "
                                   f"Registry database may be corrupted.")
            table = self.db.getExistingTable(_DYNAMIC_QUANTUM_TABLE_NAME_FORMAT.format(dimensionsHash))
            result[dimensions] = DatabaseQuantumRecordStorage(dimensions=dimensions, manager=self,
                                                              table=table)
        return result

    def get(self, dimensions: DimensionGraph) -> Optional[QuantumRecordStorage]:
        return _managed.get(dimensions)

    def register(self, dimensions: DimensionGraph) -> QuantumRecordStorage:
        result = self._managed.get(dimensions)
        if result is None:
            dimensionsHash = _hashQuantumDimensions(dimensions)
            tableName = _DYNAMIC_QUANTUM_TABLE_NAME_FORMAT.format(dimensionsHash)
            # Create the table itself.  If it already exists but wasn't in
            # the dict because it was added by another client since this one
            # was initialized, that's fine.
            table = self.db.ensureTableExists(tableName, _makeDynamicQuantumTableSpec(dimensions))
            # Add rows to the layer_meta table so we can find this table in
            # the future.  Also okay if those already exist, so we use sync.
            for dimension in dimensions:
                self.db.sync(self._static.layer_meta_quantum,
                             keys={"dimensions_hash": dimensionsHash,
                                   "dimension_name": dimension.name})
            result = DatabaseQuantumRecordStorage(dimensions=dimensions, db=self.db,
                                                  static=self._static, dynamic=table)
            self.managed[dimensions] = result
        return return result

    @property
    def db(self) -> DatabaseLayer:
        return self._parent.db
