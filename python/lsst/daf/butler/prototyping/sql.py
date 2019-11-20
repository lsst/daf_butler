from __future__ import annotations

import enum
from abc import abstractmethod, ABC
from namedtuple import namedtuple
from typing import (
    Any,
    Iterator,
    Optional,
)

import sqlalchemy

from .core.datasets import ResolvedDatasetHandle
from .core.dimensions import DataCoordinate, DimensionElement, DimensionRecord, ExpandedDataCoordinate
from .core.dimensions.schema import TIMESPAN_FIELD_SPECS, OVERLAP_TABLE_NAME_PATTERN, makeOverlapTableSpec
from .core.schema import TableSpec, FieldSpec, ForeignKeySpec, Base64Bytes
from .core.queries import CollectionsExpression
from .core.utils import NamedValueSet

from .iterables import DatasetIterable

from .backend import (
    DatasetTypesExpression,
    DimensionRecordStorage,
    GeneralRecordStorage,
    OpaqueRecordStorage,
    Run,
)


class Database(ABC):

    @abstractmethod
    def isWriteable(self) -> bool:
        pass

    @abstractmethod
    def ensureTableExists(self, name: str, spec: TableSpec) -> sqlalchemy.schema.Table:
        pass

    @abstractmethod
    def sync(self, table: sqlalchemy.schema.Table,
             keys: dict,  # find the matching record with these
             compared: dict,  # these values must be the same
             extra: Optional[dict] = None,  # these values need not be the same
             ) -> sqlalchemy.engine.RowProxy:
        pass

    origin: int
    connection: sqlalchemy.engine.Connection


_GeneralTableTuple = namedtuple(
    "_GeneralTableTuple",
    ["collection", "run", "dataset_type", "dataset_type_dimension",
     "dataset", "dataset_composition", "dataset_location"]
)


class CollectionType(enum.Enum):
    RUN = 1
    TAGGED = 2
    CALIBRATION = 3


class DatasetUniqueness(enum.Enum):
    STANDARD = 1
    NONSINGULAR = 2
    GLOBAL = 3


# TODO: finish populating schema specification.
GENERAL_TABLE_SCHEMA = _GeneralTableTuple(
    collection=TableSpec(
        fields=NamedValueSet([
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
            FieldSpec("type", dtype=sqlalchemy.SmallInteger, nullable=False),
        ]),
        unique={("name",)},
    ),
    run=TableSpec(
        fields=NamedValueSet([
            FieldSpec("collection_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("name", dtype=sqlalchemy.String, length=64, nullable=False),
            TIMESPAN_FIELD_SPECS.begin,
            TIMESPAN_FIELD_SPECS.end,
            FieldSpec("host", dtype=sqlalchemy.String, length=64),
            FieldSpec("environment_id", dtype=sqlalchemy.Integer),
        ]),
        unique={("name",)},
        foreignKeys=[
            ForeignKeySpec("collection", source=("collection_id", "origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
            ForeignKeySpec("dataset", source=("environment_id", "origin"), target=("id", "origin"),
                           onDelete="SET NULL"),
        ],
    ),
    dataset_type=TableSpec(
        fields=NamedValueSet([
            FieldSpec("name", dtype=sqlalchemy.String, length=128, primaryKey=True),
            FieldSpec("storage_class", dtype=sqlalchemy.String, length=64, nullable=False),
            FieldSpec("uniqueness", dtype=sqlalchemy.SmallInteger, nullable=False),
        ]),
    ),
    dataset_type_dimension=TableSpec(
        fields=NamedValueSet([
            FieldSpec("dataset_type_name", dtype=sqlalchemy.String, length=128, primaryKey=True),
            FieldSpec("dimension_name", dtype=sqlalchemy.String, length=32, primaryKey=True),
        ]),
        foreignKeys=[
            ForeignKeySpec("dataset_type", source=("dataset_type_name",), target=("name"),
                           onDelete="CASCADE"),
        ]
    ),
    dataset=TableSpec(
        fields=NamedValueSet([
            FieldSpec("id", dtype=sqlalchemy.BigInteger, autoincrement=True, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("dataset_type_name", dtype=sqlalchemy.String, length=128),
            FieldSpec("dataset_ref_hash", dtype=Base64Bytes, length=32),
            FieldSpec("run_id", dtype=sqlalchemy.BigInteger, nullable=False),
            FieldSpec("quantum_id", dtype=sqlalchemy.BigInteger),
        ]),
        foreignKeys=[
            ForeignKeySpec("dataset_type", source=("dataset_type_name",), target=("name")),
            ForeignKeySpec("run", source=("run_id", "origin"), target=("collection_id", "origin"),
                           onDelete="CASCADE"),
        ]
    ),
    dataset_composition=TableSpec(
        fields=NamedValueSet([
            FieldSpec("parent_dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("parent_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("component_dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("component_origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("component_name", dtype=sqlalchemy.String, length=32),
        ]),
        foreignKeys=[
            ForeignKeySpec("dataset", source=("parent_dataset_id", "parent_origin"),
                           target=("id", "origin"), onDelete="CASCADE"),
            ForeignKeySpec("dataset", source=("component_dataset_id", "component_origin"),
                           target=("id", "origin"), onDelete="CASCADE"),
        ]
    ),
    dataset_location=TableSpec(
        fields=NamedValueSet([
            FieldSpec("dataset_id", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("origin", dtype=sqlalchemy.BigInteger, primaryKey=True),
            FieldSpec("datastore_name", dtype=sqlalchemy.String, length=256, primaryKey=True),
        ]),
        foreignKeys=[
            ForeignKeySpec("dataset", source=("dataset_id", "origin"), target=("id", "origin"),
                           onDelete="CASCADE"),
        ]
    ),
)


class SqlGeneralRecordStorage(GeneralRecordStorage):

    def __init__(self, db: Database):
        self._db = db
        self._tables = _GeneralTableTuple._make(
            db.ensureTableExists(name, spec) for name, spec in GENERAL_TABLE_SCHEMA._asdict().items()
        )

    def isWriteable(self) -> bool:
        return self._db.isWriteable()

    def matches(self, collections: CollectionsExpression = ...,
                datasetTypes: DatasetTypesExpression = None) -> bool:
        # No point doing work here unless we're actually in a multi-namespace
        # database; we can always subclass/wrap this class to add that.
        return True

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
        assert run._collection_id is not None
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
        # TODO search ephemeral storage
        table = self._tables.dataset_location
        sql = table.select(
            [table.columns.datastore_name]
        ).where(
            sqlalchemy.sql.and_(
                table.columns.dataset_id == dataset.id,
                table.columns.origin == dataset.origin
            )
        )
        for row in self._db.connection.execute(sql, {"dataset_id": dataset.id, "origin": dataset.origin}):
            yield row[table.columns.datastore_name]

    def deleteDatasetLocations(self, datastoreName: str, datasets: DatasetIterable):
        # TODO delete from ephemeral storage
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

    def selectDatasetTypes(self, datasetTypes: DatasetTypesExpression = ..., *,
                           collections: CollectionsExpression = ...) -> sqlalchemy.sql.FromClause:
        raise NotImplementedError("TODO")

    def selectCollections(self, collections: CollectionsExpression = ..., *,
                          datasetTypes: DatasetTypesExpression = ...) -> sqlalchemy.sql.FromCLause:
        raise NotImplementedError("TODO")


class SqlOpaqueRecordStorage(OpaqueRecordStorage):

    def __init__(self, name: str, spec: TableSpec, db: Database):
        super().__init__(name=name)
        self._db = db
        self._table = self._db.ensureTableExists(name, spec)

    def isWriteable(self) -> bool:
        return self._db.isWriteable()

    def insert(self, *data: dict):
        self._db.connection.execute(self._table.insert(), *data)

    def fetch(self, **where: Any) -> Iterator[dict]:
        sql = self._table.select().where(
            sqlalchemy.sql.and_(*[self.table.columns[k] == v for k, v in where.items()])
        )
        yield from self._db.connection.execute(sql)

    def delete(self, **where: Any):
        sql = self._table.delete().where(
            sqlalchemy.sql.and_(*[self.table.columns[k] == v for k, v in where.items()])
        )
        self._db.connection.execute(sql)

    name: str


class SqlDimensionRecordStorage(DimensionRecordStorage):

    def __init__(self, element: DimensionElement, db: Database):
        super().__init__(element=element)
        self._db = db
        self._table = self._db.ensureTableExists(element.name, element.makeTableSpec())
        if element.spatial:
            self._commonSkyPixTable = self._db.ensureTableExists(
                OVERLAP_TABLE_NAME_PATTERN.format(element.name, element.universe.commonSkyPix.name),
                makeOverlapTableSpec(element, element.universe.commonSkyPix)
            )
        else:
            self._commonSkyPixTable = None

    def isWriteable(self) -> bool:
        return self._db.isWriteable()

    def matches(self, dataId: Optional[ExpandedDataCoordinate] = None) -> bool:
        # No point doing work here unless we're actually in a multi-namespace
        # database; we can always subclass/wrap this class to add that.
        return True

    def insert(self, *records: DimensionRecord):
        # Build lists of dicts to insert first, before any database operations,
        # to minimize the time spent in the transaction.
        elementRows = []
        if self.element.spatial:
            commonSkyPixRows = []
            commonSkyPix = self.element.universe.commonSkyPix
        for record in records:
            elementRows.append(record.toDict())
            if self.element.spatial:
                if record.region is None:
                    # TODO: should we warn about this case?
                    continue
                base = record.dataId.byName()
                for begin, end in commonSkyPix.pixelization.envelope(record.region):
                    for skypix in range(begin, end):
                        row = base.copy()
                        row[commonSkyPix.name] = skypix
                        commonSkyPixRows.append(row)
        # TODO: wrap the operations below in a transaction.
        self._db.connection.execute(self._elementTable.insert(), *elementRows)
        if self.element.spatial and commonSkyPixRows:
            self._db.connection.execute(self._commonSkyPixOverlapTable.insert(), *commonSkyPixRows)

    @abstractmethod
    def fetch(self, dataId: DataCoordinate) -> DimensionRecord:
        pass

    @abstractmethod
    def select(self, dataId: Optional[ExpandedDataCoordinate] = None) -> Optional[sqlalchemy.sql.FromClause]:
        pass

    @abstractmethod
    def selectSkyPixOverlap(self, dataId: Optional[ExpandedDataCoordinate] = None
                            ) -> Optional[sqlalchemy.sql.FromClause]:
        pass
