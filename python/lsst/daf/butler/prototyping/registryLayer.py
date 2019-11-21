from __future__ import annotations

from typing import (
    Dict,
    Iterator,
    Optional,
)
from collections import defaultdict

import sqlalchemy

from ..core.datasets import (
    DatasetType,
    ResolvedDatasetHandle,
)
from ..core.dimensions import (
    DimensionElement,
    DimensionGraph,
    DimensionUniverse,
)
from ..core.dimensions.schema import OVERLAP_TABLE_NAME_PATTERN, makeOverlapTableSpec
from ..core.schema import TableSpec
from ..core.timespan import TIMESPAN_FIELD_SPECS
from ..core.utils import NamedKeyDict
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
from .opaqueRecordStorage import OpaqueRecordStorage
from .dimensionRecordStorage import DimensionRecordStorage, DatabaseDimensionRecordStorage
from .quantumRecordStorage import QuantumRecordStorage, DatabaseQuantumRecordStorage
from .datasetRecordStorage import DatasetRecordStorage


class RegistryLayer:

    def __init__(self, db: DatabaseLayer, *, universe: DimensionUniverse):
        self._db = db
        self._tables = StaticTablesTuple._make(
            db.ensureTableExists(name, spec) for name, spec in STATIC_TABLES_SPEC._asdict().items()
        )
        self._opaqueStorage = self._findExistingOpaqueStorage()
        self._dimensionStorage = self._findExistingDimensionStorage(universe=universe)
        self._quantumStorage = self._findExistingQuantumStorage(universe=universe)

    def _findExistingOpaqueStorage(self) -> Dict[str, OpaqueRecordStorage]:
        result = {}
        meta = self._tables.layer_meta_opaque
        for row in self._db.connection.execute(meta.select()).fetchall():
            name = row["table_name"]
            result[name] = OpaqueRecordStorage(name=name, table=self._db.getExistingTable(name), db=self._db)
        return result

    def _findExistingDimensionStorage(self, *, universe: DimensionUniverse
                                      ) -> NamedKeyDict[DimensionElement, DimensionRecordStorage]:
        result = NamedKeyDict()
        meta = self._tables.layer_meta_dimension
        for row in self._db.connection.execute(meta.select()).fetchall():
            element = universe[row[meta.columns.element_name]]
            table = self._db.getExistingTable(element.name)
            if element.spatial:
                commonSkyPixOverlapTable = self._db.getExistingTable(
                    OVERLAP_TABLE_NAME_PATTERN.format(element.name, universe.commonSkyPix.name)
                )
            else:
                commonSkyPixOverlapTable = None
            result[element] = DatabaseDimensionRecordStorage(
                db=self._db, element=element, table=table,
                commonSkyPixOverlapTable=commonSkyPixOverlapTable
            )
        return result

    def _findExistingQuantumStorage(self, *, universe: DimensionUniverse
                                    ) -> Dict[DimensionGraph, QuantumRecordStorage]:
        # Query for all of the tables
        meta = self._tables.layer_meta_quantum
        dimensionsByHash = defaultdict(list)
        for row in self._db.connection.execute(meta.select()).fetchall():
            dimensionsByHash[row[meta.columns.dimensions_hash]].append(row[meta.columns.dimension_name])
        result = {}
        for dimensionsHash, dimensionNames in dimensionsByHash.items():
            dimensions = DimensionGraph(universe=universe, names=dimensionNames)
            if dimensionsHash != hashQuantumDimensions(dimensions):
                raise RuntimeError(f"Bad dimensions hash: {dimensionsHash}.  "
                                   f"Registry database may be corrupted.")
            table = self._db.getExistingTable(QUANTUM_TABLE_NAME_FORMAT.format(dimensionsHash))
            result[dimensions] = DatabaseQuantumRecordStorage(dimensions=dimensions, db=self._db, table=table)
        return result

    def syncRun(self, name: str) -> Run:
        collectionRow = self._db.sync(
            self._tables.collection,
            keys={"name": name},
            compared={"origin": self._db.origin, "type": CollectionType.RUN},
        )
        runRow = self._db.sync(
            self._tables.run,
            keys={"collection_id": collectionRow["id"], "origin": self._db.origin},
            compared={"name": name}
        )
        return Run(**runRow)

    def findRun(self, name: str) -> Optional[Run]:
        sql = self._tables.run.select().where(self._tables.run.columns.name == name)
        values = self._db.connection.execute(sql).fetchone()
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
        values = self._db.connection.execute(sql).fetchone()
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
        self._db.connection.execute(sql)

    def syncCollection(self, name: str, *, calibration: bool = False):
        self._db.sync(
            self._tables.collection,
            keys={"name": name},
            compared={"type": CollectionType.CALIBRATION if calibration else CollectionType.TAGGED},
            extra={"origin": self._db.origin},
        )

    def insertDatasetLocations(self, datastoreName: str, datasets: DatasetIterable, *,
                               ephemeral: bool = False):
        if ephemeral:
            raise NotImplementedError("Ephemeral datasets are not yet supported.")
        self._db.connection.execute(
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
        for row in self._db.connection.execute(sql, {"dataset_id": dataset.id, "origin": dataset.origin}):
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
        self._db.connection.execute(
            sql,
            *[{"dataset_id": dataset.id, "origin": dataset.origin} for dataset in datasets]
        )

    def selectDatasetTypes(self) -> sqlalchemy.sql.FromClause:
        return self._tables.dataset_type

    def selectCollections(self) -> sqlalchemy.sql.FromClause:
        return self._tables.collections

    def registerOpaqueData(self, name: str, spec: TableSpec) -> OpaqueRecordStorage:
        if name not in self._opaqueStorage:
            # Create the table itself.  If it already exists but wasn't in
            # the dict because it was added by another client since this one
            # was initialized, that's fine.
            table = self._db.ensureTableExists(name, spec)
            # Add a row to the layer_meta table so we can find this table in
            # the future.  Also okay if that already exists, so we use sync.
            self._db.sync(self._tables.layer_meta_opaque, keys={"table_name": name})
            self._opaqueStorage[name] = OpaqueRecordStorage(name=name, table=table, db=self._db)
        return self._opaqueStorage[name]

    def getOpaqueData(self, name: str) -> Optional[OpaqueRecordStorage]:
        result = self._opaqueStorage.get(name)
        if result is None:
            # Another Registry client may have added the table we're looking
            # for; refresh the list.
            refreshed = self._findExistingOpaqueStorage()
            # Keep old instances where they exist, in case those have caches
            # or something.
            refreshed.update(self._opaqueStorage)
            self._opaqueStorage = refreshed
            # Table still may not exist, so we use get to possibly return None.
            result = self._opaqueStorage.get(name)
        return result

    def registerDimensionElement(self, element: DimensionElement) -> DimensionRecordStorage:
        assert element.hasTable()
        if element not in self._dimensionStorage:
            # Create the table itself.  If it already exists but wasn't in
            # the dict because it was added by another client since this one
            # was initialized, that's fine.
            table = self._db.ensureTableExists(element.name, element.makeTableSpec())
            if element.spatial:
                # Also create a corresponding overlap table with the common
                # skypix dimension, if this is a spatial element.
                commonSkyPixOverlapTable = self._db.ensureTableExists(
                    OVERLAP_TABLE_NAME_PATTERN.format(element.name, element.universe.commonSkyPix.name),
                    makeOverlapTableSpec(element, element.universe.commonSkyPix)
                )
            else:
                commonSkyPixOverlapTable = None
            # Add a row to the layer_meta table so we can find this table in
            # the future.  Also okay if that already exists, so we use sync.
            self._db.sync(self._tables.layer_meta.dimension, keys={"element_name": element.name})
            self._dimensionStorage[element] = DatabaseDimensionRecordStorage(
                db=self._db,
                element=element,
                table=table,
                commonSkyPixOverlapTable=commonSkyPixOverlapTable
            )
        return self._dimensionStorage[element]

    def getDimensionElement(self, element: DimensionElement) -> Optional[DimensionRecordStorage]:
        result = self._dimensionStorage.get(element)
        if result is None:
            # Another Registry client may have added the table we're looking
            # for; refresh the list.
            refreshed = self._findExistingDimensionStorage(universe=element.universe)
            # Keep old instances where they exist, in case those have caches
            # or something.
            refreshed.update(self._dimensionStorage)
            self._opaqueStorage = refreshed
            # Table still may not exist, so we use get to possibly return None.
            result = self._opaqueStorage.get(element)
        return result

    def registerQuanta(self, dimensions: DimensionGraph) -> QuantumRecordStorage:
        if dimensions not in self._quantumStorage:
            dimensionsHash = hashQuantumDimensions(dimensions)
            tableName = QUANTUM_TABLE_NAME_FORMAT.format(dimensionsHash)
            # Create the table itself.  If it already exists but wasn't in
            # the dict because it was added by another client since this one
            # was initialized, that's fine.
            table = self._db.ensureTableExists(tableName, makeQuantumTableSpec(dimensions))
            # Add rows to the layer_meta table so we can find this table in
            # the future.  Also okay if those already exist, so we use sync.
            for dimension in dimensions:
                self._db.sync(self._tables.layer_meta_quantum,
                              keys={"dimensions_hash": dimensionsHash,
                                    "dimension_name": dimension.name})
            self._quantumStorage[dimensions] = DatabaseQuantumRecordStorage(dimensions=dimensions,
                                                                            db=self._db,
                                                                            table=table)
        return self._quantumStorage[dimensions]

    def getQuanta(self, dimensions: DimensionGraph) -> Optional[QuantumRecordStorage]:
        result = self._quantumStorage.get(dimensions)
        if result is None:
            # Another Registry client may have added the table we're looking
            # for; refresh the list.
            refreshed = self._findExistingQuantumStorage(universe=dimensions.universe)
            # Keep old instances where they exist, in case those have caches
            # or something.
            refreshed.update(self._quantumStorage)
            self._opaqueStorage = refreshed
            # Table still may not exist, so we use get to possibly return None.
            result = self._opaqueStorage.get(dimensions)
        return result

    def registerDatasetType(self, datasetType: DatasetType) -> DatasetRecordStorage:
        pass

    def getDatasetType(self, name: str) -> Optional[DatasetRecordStorage]:
        pass
