from __future__ import annotations

from typing import (
    Dict,
    Iterator,
    Optional,
    Type,
)
from collections import defaultdict

import sqlalchemy

from ..core.datasets import (
    DatasetType,
    ResolvedDatasetHandle,
)
from ..core.dimensions import (
    DimensionGraph,
    DimensionUniverse,
)
from ..core.timespan import TIMESPAN_FIELD_SPECS
from .databaseLayer import DatabaseLayer
from .iterables import DatasetIterable
from .run import Run
from .ddl import (
    StaticTablesTuple,
    STATIC_TABLES_SPEC,
    CollectionType,
    hashQuantumDimensions,
    makeQuantumTableSpec,
    QUANTUM_TABLE_NAME_FORMAT
)
from .opaqueRecordStorage import OpaqueRecordStorageManager
from .dimensionRecordStorage import DimensionRecordStorageManager
from .quantumRecordStorage import QuantumRecordStorageManager
from .datasetRecordStorage import DatasetRecordStorageManager


class RegistryLayer:

    def __init__(self, db: DatabaseLayer, *, universe: DimensionUniverse,
                 opaque: Type[OpaqueRecordStorageManager],
                 dimensions: Type[DimensionRecordStorageManager],
                 quanta: Type[QuantumRecordStorageManager]):
        self.db = db
        self._tables = StaticTablesTuple._make(
            db.ensureTableExists(name, spec) for name, spec in STATIC_TABLES_SPEC._asdict().items()
        )
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

    def registerDatasetType(self, datasetType: DatasetType) -> DatasetRecordStorage:
        pass

    def getDatasetType(self, name: str) -> Optional[DatasetRecordStorage]:
        pass

    opaque: OpaqueRecordStorageManager
    dimensions: DimensionRecordStorageManager
    quanta: QuantumRecordStorageManager
