from __future__ import annotations

from typing import (
    Iterator,
    List,
    Optional,
)

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
from .ddl import StaticTablesTuple, STATIC_TABLES_SPEC, CollectionType
from .opaqueRecordStorage import OpaqueRecordStorage
from .dimensionRecordStorage import DimensionRecordStorage, SqlDimensionRecordStorage


def _findExistingOpaqueTables(db: DatabaseLayer, meta: sqlalchemy.schema.Table) -> List[OpaqueRecordStorage]:
    result = {}
    for row in db.connection.execute(meta.select()).fetchall():
        name = row["table_name"]
        result[name] = OpaqueRecordStorage(name=name, table=db.getExistingTable(name), db=db)
    return result


def _findExistingDimensionTables(db: DatabaseLayer, meta: sqlalchemy.schema.Table, *,
                                 universe: DimensionUniverse,
                                 ) -> List[DimensionRecordStorage]:
    result = NamedKeyDict()
    for row in db.connection.execute(meta.select()).fetchall():
        element = universe[row[meta.columns.element_name]]
        table = db.getExistingTable(element.name)
        if element.spatial:
            commonSkyPixOverlapTable = db.getExistingTable(
                OVERLAP_TABLE_NAME_PATTERN.format(element.name, universe.commonSkyPix.name)
            )
        else:
            commonSkyPixOverlapTable = None
        result[element] = SqlDimensionRecordStorage(db=db, element=element, table=table,
                                                    commonSkyPixOverlapTable=commonSkyPixOverlapTable)
    return result


class RegistryLayer:

    def __init__(self, db: DatabaseLayer, *, universe: DimensionUniverse):
        self._db = db
        self._tables = StaticTablesTuple._make(
            db.ensureTableExists(name, spec) for name, spec in STATIC_TABLES_SPEC._asdict().items()
        )
        self._opaqueStorage = _findExistingOpaqueTables(db, self._tables.layer_meta_opaque)
        self._dimensionStorage = _findExistingDimensionTables(db, self._tables.layer_meta_dimension,
                                                              universe=universe)

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
            table = self._db.ensureTableExists(name, spec)
            self._opaqueStorage[name] = OpaqueRecordStorage(name=name, table=table, db=self._db)
        return self._opaqueStorage[name]

    def getOpaqueData(self, name: str) -> Optional[OpaqueRecordStorage]:
        return self._opaqueStorage.get(name)

    def registerDimensionElement(self, element: DimensionElement) -> DimensionRecordStorage:
        assert element.hasTable()
        if element not in self._dimensionStorage:
            table = self._db.ensureTableExists(element.name, element.makeTableSpec())
            if element.spatial:
                commonSkyPixOverlapTable = self._db.ensureTableExists(
                    OVERLAP_TABLE_NAME_PATTERN.format(element.name, element.universe.commonSkyPix.name),
                    makeOverlapTableSpec(element, element.universe.commonSkyPix)
                )
            else:
                commonSkyPixOverlapTable = None
            self._dimensionStorage[element] = SqlDimensionRecordStorage(
                db=self._db,
                element=element,
                table=table,
                commonSkyPixOverlapTable=commonSkyPixOverlapTable
            )
        return self._dimensionStorage[element]

    def getDimensionElement(self, element: DimensionElement) -> Optional[DimensionRecordStorage]:
        return self._dimensionStorage.get(element)

    def registerQuanta(self, dimensions: DimensionGraph) -> QuantumRecordStorage:
        pass

    def registerDatasetType(self, datasetType: DatasetType) -> DatasetRecordStorage:
        pass

    def getDatasetType(self, name: str) -> Optional[DatasetRecordStorage]:
        pass
