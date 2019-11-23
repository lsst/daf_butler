from __future__ import annotations

import enum
from typing import (
    Iterator,
    Optional,
    Type,
)

import sqlalchemy

from ..core.datasets import (
    ResolvedDatasetHandle,
)
from ..core.dimensions import (
    DimensionUniverse,
)
from ..core.schema import TableSpec, FieldSpec, ForeignKeySpec, Base64Bytes
from ..core.timespan import TIMESPAN_FIELD_SPECS
from .database import Database, makeTableStruct
from .iterables import DatasetIterable
from .run import Run
from .opaqueRecordStorage import OpaqueRecordStorageManager
from .dimensionRecordStorage import DimensionRecordStorageManager
from .quantumRecordStorage import QuantumRecordStorageManager


class CollectionType(enum.IntEnum):
    RUN = 1
    TAGGED = 2
    CALIBRATION = 3


class DatasetUniqueness(enum.IntEnum):
    STANDARD = 1
    NONSINGULAR = 2
    GLOBAL = 3


@makeTableStruct
class StaticLayerTablesTuple:
    collection = TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
            FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
        ],
        unique={("name",)},
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
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("datastore_name", dtype=sqlalchemy.String, length=256, primaryKey=True),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset", source=("dataset_id", "origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
        ]
    )
    dataset_type_dimension = TableSpec(
        fields=[
            FieldSpec("dataset_type_name", dtype=sqlalchemy.String, length=128, primaryKey=True),
            FieldSpec("dimension_name", dtype=sqlalchemy.String, length=32, primaryKey=True),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset_type", source=("dataset_type_name",), target=("name"),
                           onDelete="CASCADE"),
        ]
    )
    dataset_type = TableSpec(
        fields=[
            FieldSpec("name", dtype=sqlalchemy.String, length=128, primaryKey=True),
            FieldSpec("storage_class", dtype=sqlalchemy.String, length=64, nullable=False),
            FieldSpec("uniqueness", dtype=sqlalchemy.SmallInteger, nullable=False),
        ],
    )
    dataset = TableSpec(
        fields=[
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_type_name", dtype=sqlalchemy.String, length=128),
            FieldSpec("dataset_ref_hash", dtype=Base64Bytes, nbytes=32),
            FieldSpec("run_collection_id", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("quantum_id", dtype=sqlalchemy.BigInteger),
        ],
        foreignKeys=[
            ForeignKeySpec("dataset_type", source=("dataset_type_name",), target=("name")),
            ForeignKeySpec("run", source=("run_collection_id", "origin"), target=("collection_id", "origin"),
                           onDelete="CASCADE"),
        ]
    )
    run = TableSpec(
        fields=[
            FieldSpec("collection_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            FieldSpec("host", dtype=sqlalchemy.String, length=128),
        ],
        unique={("name",)},
        foreignKeys=[
            ForeignKeySpec("collection", source=("collection_id", "origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
        ],
    )


class RegistryLayer:

    def __init__(self, db: Database, *, universe: DimensionUniverse,
                 opaque: Type[OpaqueRecordStorageManager],
                 dimensions: Type[DimensionRecordStorageManager],
                 quanta: Type[QuantumRecordStorageManager]):
        self.db = db
        self._tables = StaticLayerTablesTuple(db)
        self.opaque = OpaqueRecordStorageManager.load(self.db)
        self.dimensions = DimensionRecordStorageManager.load(self.db, universe=universe)
        self.quanta = QuantumRecordStorageManager.load(self, universe=universe)

    def syncRun(self, name: str) -> Run:
        collectionRow = self.db.sync(
            self._tables.collection,
            keys={"name": name},
            compared={"origin": self.db.origin, "type": CollectionType.RUN},
        )
        runRow = self.db.sync(
            self._tables.run,
            keys={"collection_id": collectionRow["id"], "origin": self.db.origin},
            compared={"name": name}
        )
        return Run(**runRow)

    def findRun(self, name: str) -> Optional[Run]:
        sql = self._tables.run.select().where(self._tables.run.columns.name == name)
        values = self.db.connection.execute(sql).fetchone()
        if values is None:
            return None
        return Run(**values)

    def getRun(self, collection_id: int, origin: int) -> Optional[Run]:
        sql = self._tables.run.select().where(
            sqlalchemy.sql.and_(
                self._tables.run.columns.collection_id == collection_id,
                self._tables.run.columns.origin == origin
            )
        )
        values = self.db.connection.execute(sql).fetchone()
        if values is None:
            return None
        return Run(**values)

    def updateRun(self, run: Run):
        assert run.collection_id is not None
        values = {
            TIMESPAN_FIELD_SPECS.begin.name: run.timespan.begin,
            TIMESPAN_FIELD_SPECS.end.name: run.timespan.end,
            "host": run,
            "environment_id": run.environment_id,
        }
        sql = self._tables.run.update().where(
            sqlalchemy.sql.and_(self._tables.run.columns.collection_id == run.collection_id,
                                self._tables.run.columns.origin == run.origin)
        ).values(**values)
        self.db.connection.execute(sql)

    def syncCollection(self, name: str, *, calibration: bool = False):
        self.db.sync(
            self._tables.collection,
            keys={"name": name},
            compared={"type": CollectionType.CALIBRATION if calibration else CollectionType.TAGGED},
            extra={"origin": self.db.origin},
        )

    def insertDatasetLocations(self, datastoreName: str, datasets: DatasetIterable, *,
                               ephemeral: bool = False):
        if ephemeral:
            raise NotImplementedError("Ephemeral datasets are not yet supported.")
        self.db.connection.execute(
            self._tables.dataset_location.insert(),
            *[{"dataset_id": dataset.id, "origin": dataset.origin, "datastore_name": datastoreName}
              for dataset in datasets]
        )

    def fetchDatasetLocations(self, dataset: ResolvedDatasetHandle) -> Iterator[str]:
        table = self._tables.dataset_location
        sql = sqlalchemy.sql.select(
            [table.columns.datastore_name]
        ).select_from(table).where(
            sqlalchemy.sql.and_(
                table.columns.dataset_id == dataset.id,
                table.columns.origin == dataset.origin
            )
        )
        for row in self.db.connection.execute(sql, {"dataset_id": dataset.id, "origin": dataset.origin}):
            yield row[table.columns.datastore_name]

    def deleteDatasetLocations(self, datastoreName: str, datasets: DatasetIterable):
        table = self._tables.dataset_location
        sql = table.delete().where(
            sqlalchemy.sql.and_(
                table.columns.datastore_name == datastoreName,
                table.columns.dataset_id == sqlalchemy.sql.bindparam("dataset_id"),
                table.columns.origin == sqlalchemy.sql.bindparam("origin"),
            )
        )
        self.db.connection.execute(
            sql,
            *[{"dataset_id": dataset.id, "origin": dataset.origin} for dataset in datasets]
        )

    def selectDatasetTypes(self) -> sqlalchemy.sql.FromClause:
        return self._tables.dataset_type

    def selectCollections(self) -> sqlalchemy.sql.FromClause:
        return self._tables.collections

    opaque: OpaqueRecordStorageManager
    dimensions: DimensionRecordStorageManager
    quanta: QuantumRecordStorageManager
