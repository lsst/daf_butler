from __future__ import annotations

__all__ = (
    "ByDimensionsDatasetRecordStorageManager",
    "ByDimensionsDatasetRecordStorageManagerUUID",
)

import copy
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Tuple, Type

import sqlalchemy
from lsst.daf.butler import DatasetId, DatasetRef, DatasetType, DimensionUniverse, ddl
from lsst.daf.butler.registry import ConflictingDefinitionError, OrphanedRecordError
from lsst.daf.butler.registry.interfaces import (
    DatasetIdGenEnum,
    DatasetRecordStorage,
    DatasetRecordStorageManager,
    VersionTuple,
)

from ...summaries import CollectionSummary
from ._storage import (
    ByDimensionsDatasetRecordStorage,
    ByDimensionsDatasetRecordStorageInt,
    ByDimensionsDatasetRecordStorageUUID,
)
from .summaries import CollectionSummaryManager
from .tables import (
    addDatasetForeignKey,
    makeCalibTableName,
    makeCalibTableSpec,
    makeStaticTableSpecs,
    makeTagTableName,
    makeTagTableSpec,
)

if TYPE_CHECKING:
    from lsst.daf.butler.registry.interfaces import (
        CollectionManager,
        CollectionRecord,
        Database,
        DimensionRecordStorageManager,
        StaticTablesContext,
    )

    from .tables import StaticDatasetTablesTuple


# This has to be updated on every schema change
_VERSION_INT = VersionTuple(1, 0, 0)
_VERSION_UUID = VersionTuple(1, 0, 0)


class MissingDatabaseTableError(RuntimeError):
    """Exception raised when a table is not found in a database."""


class ByDimensionsDatasetRecordStorageManagerBase(DatasetRecordStorageManager):
    """A manager class for datasets that uses one dataset-collection table for
    each group of dataset types that share the same dimensions.

    In addition to the table organization, this class makes a number of
    other design choices that would have been cumbersome (to say the least) to
    try to pack into its name:

     - It uses a private surrogate integer autoincrement field to identify
       dataset types, instead of using the name as the primary and foreign key
       directly.

     - It aggressively loads all DatasetTypes into memory instead of fetching
       them from the database only when needed or attempting more clever forms
       of caching.

    Alternative implementations that make different choices for these while
    keeping the same general table organization might be reasonable as well.

    This class provides complete implementation of manager logic but it is
    parametrized by few class attributes that have to be defined by
    sub-classes.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    collections : `CollectionManager`
        Manager object for the collections in this `Registry`.
    dimensions : `DimensionRecordStorageManager`
        Manager object for the dimensions in this `Registry`.
    static : `StaticDatasetTablesTuple`
        Named tuple of `sqlalchemy.schema.Table` instances for all static
        tables used by this class.
    summaries : `CollectionSummaryManager`
        Structure containing tables that summarize the contents of collections.
    """

    def __init__(
        self,
        *,
        db: Database,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
        static: StaticDatasetTablesTuple,
        summaries: CollectionSummaryManager,
    ):
        self._db = db
        self._collections = collections
        self._dimensions = dimensions
        self._static = static
        self._summaries = summaries
        self._byName: Dict[str, ByDimensionsDatasetRecordStorage] = {}
        self._byId: Dict[DatasetId, ByDimensionsDatasetRecordStorage] = {}

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
    ) -> DatasetRecordStorageManager:
        # Docstring inherited from DatasetRecordStorageManager.
        specs = cls.makeStaticTableSpecs(type(collections), universe=dimensions.universe)
        static: StaticDatasetTablesTuple = context.addTableTuple(specs)  # type: ignore
        summaries = CollectionSummaryManager.initialize(
            db,
            context,
            collections=collections,
            dimensions=dimensions,
        )
        return cls(db=db, collections=collections, dimensions=dimensions, static=static, summaries=summaries)

    @classmethod
    def currentVersion(cls) -> Optional[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return cls._version

    @classmethod
    def makeStaticTableSpecs(
        cls, collections: Type[CollectionManager], universe: DimensionUniverse
    ) -> StaticDatasetTablesTuple:
        """Construct all static tables used by the classes in this package.

        Static tables are those that are present in all Registries and do not
        depend on what DatasetTypes have been registered.

        Parameters
        ----------
        collections: `CollectionManager`
            Manager object for the collections in this `Registry`.
        universe : `DimensionUniverse`
            Universe graph containing all dimensions known to this `Registry`.

        Returns
        -------
        specs : `StaticDatasetTablesTuple`
            A named tuple containing `ddl.TableSpec` instances.
        """
        return makeStaticTableSpecs(
            collections, universe=universe, dtype=cls.getIdColumnType(), autoincrement=cls._autoincrement
        )

    @classmethod
    def getIdColumnType(cls) -> type:
        # Docstring inherited from base class.
        return cls._idColumnType

    @classmethod
    def addDatasetForeignKey(
        cls,
        tableSpec: ddl.TableSpec,
        *,
        name: str = "dataset",
        constraint: bool = True,
        onDelete: Optional[str] = None,
        **kwargs: Any,
    ) -> ddl.FieldSpec:
        # Docstring inherited from DatasetRecordStorageManager.
        return addDatasetForeignKey(
            tableSpec, cls.getIdColumnType(), name=name, onDelete=onDelete, constraint=constraint, **kwargs
        )

    def refresh(self) -> None:
        # Docstring inherited from DatasetRecordStorageManager.
        byName = {}
        byId: Dict[DatasetId, ByDimensionsDatasetRecordStorage] = {}
        c = self._static.dataset_type.columns
        for row in self._db.query(self._static.dataset_type.select()).mappings():
            name = row[c.name]
            dimensions = self._dimensions.loadDimensionGraph(row[c.dimensions_key])
            calibTableName = row[c.calibration_association_table]
            datasetType = DatasetType(
                name, dimensions, row[c.storage_class], isCalibration=(calibTableName is not None)
            )
            tags = self._db.getExistingTable(
                row[c.tag_association_table],
                makeTagTableSpec(datasetType, type(self._collections), self.getIdColumnType()),
            )
            if tags is None:
                raise MissingDatabaseTableError(
                    f"Table {row[c.tag_association_table]} is missing from database schema."
                )
            if calibTableName is not None:
                calibs = self._db.getExistingTable(
                    row[c.calibration_association_table],
                    makeCalibTableSpec(
                        datasetType,
                        type(self._collections),
                        self._db.getTimespanRepresentation(),
                        self.getIdColumnType(),
                    ),
                )
                if calibs is None:
                    raise MissingDatabaseTableError(
                        f"Table {row[c.calibration_association_table]} is missing from database schema."
                    )
            else:
                calibs = None
            storage = self._recordStorageType(
                db=self._db,
                datasetType=datasetType,
                static=self._static,
                summaries=self._summaries,
                tags=tags,
                calibs=calibs,
                dataset_type_id=row["id"],
                collections=self._collections,
            )
            byName[datasetType.name] = storage
            byId[storage._dataset_type_id] = storage
        self._byName = byName
        self._byId = byId
        self._summaries.refresh(lambda dataset_type_id: self._byId[dataset_type_id].datasetType)

    def remove(self, name: str) -> None:
        # Docstring inherited from DatasetRecordStorageManager.
        compositeName, componentName = DatasetType.splitDatasetTypeName(name)
        if componentName is not None:
            raise ValueError(f"Cannot delete a dataset type of a component of a composite (given {name})")

        # Delete the row
        try:
            self._db.delete(self._static.dataset_type, ["name"], {"name": name})
        except sqlalchemy.exc.IntegrityError as e:
            raise OrphanedRecordError(
                f"Dataset type {name} can not be removed."
                " It is associated with datasets that must be removed first."
            ) from e

        # Now refresh everything -- removal is rare enough that this does
        # not need to be fast.
        self.refresh()

    def find(self, name: str) -> Optional[DatasetRecordStorage]:
        # Docstring inherited from DatasetRecordStorageManager.
        compositeName, componentName = DatasetType.splitDatasetTypeName(name)
        storage = self._byName.get(compositeName)
        if storage is not None and componentName is not None:
            componentStorage = copy.copy(storage)
            componentStorage.datasetType = storage.datasetType.makeComponentDatasetType(componentName)
            return componentStorage
        else:
            return storage

    def register(self, datasetType: DatasetType) -> Tuple[DatasetRecordStorage, bool]:
        # Docstring inherited from DatasetRecordStorageManager.
        if datasetType.isComponent():
            raise ValueError(
                f"Component dataset types can not be stored in registry. Rejecting {datasetType.name}"
            )
        storage = self._byName.get(datasetType.name)
        if storage is None:
            dimensionsKey = self._dimensions.saveDimensionGraph(datasetType.dimensions)
            tagTableName = makeTagTableName(datasetType, dimensionsKey)
            calibTableName = (
                makeCalibTableName(datasetType, dimensionsKey) if datasetType.isCalibration() else None
            )
            # The order is important here, we want to create tables first and
            # only register them if this operation is successful. We cannot
            # wrap it into a transaction because database class assumes that
            # DDL is not transaction safe in general.
            tags = self._db.ensureTableExists(
                tagTableName,
                makeTagTableSpec(datasetType, type(self._collections), self.getIdColumnType()),
            )
            if calibTableName is not None:
                calibs = self._db.ensureTableExists(
                    calibTableName,
                    makeCalibTableSpec(
                        datasetType,
                        type(self._collections),
                        self._db.getTimespanRepresentation(),
                        self.getIdColumnType(),
                    ),
                )
            else:
                calibs = None
            row, inserted = self._db.sync(
                self._static.dataset_type,
                keys={"name": datasetType.name},
                compared={
                    "dimensions_key": dimensionsKey,
                    # Force the storage class to be loaded to ensure it
                    # exists and there is no typo in the name.
                    "storage_class": datasetType.storageClass.name,
                },
                extra={
                    "tag_association_table": tagTableName,
                    "calibration_association_table": calibTableName,
                },
                returning=["id", "tag_association_table"],
            )
            assert row is not None
            storage = self._recordStorageType(
                db=self._db,
                datasetType=datasetType,
                static=self._static,
                summaries=self._summaries,
                tags=tags,
                calibs=calibs,
                dataset_type_id=row["id"],
                collections=self._collections,
            )
            self._byName[datasetType.name] = storage
            self._byId[storage._dataset_type_id] = storage
        else:
            if datasetType != storage.datasetType:
                raise ConflictingDefinitionError(
                    f"Given dataset type {datasetType} is inconsistent "
                    f"with database definition {storage.datasetType}."
                )
            inserted = False
        return storage, bool(inserted)

    def __iter__(self) -> Iterator[DatasetType]:
        for storage in self._byName.values():
            yield storage.datasetType

    def getDatasetRef(self, id: DatasetId) -> Optional[DatasetRef]:
        # Docstring inherited from DatasetRecordStorageManager.
        sql = (
            sqlalchemy.sql.select(
                self._static.dataset.columns.dataset_type_id,
                self._static.dataset.columns[self._collections.getRunForeignKeyName()],
            )
            .select_from(self._static.dataset)
            .where(self._static.dataset.columns.id == id)
        )
        row = self._db.query(sql).mappings().fetchone()
        if row is None:
            return None
        recordsForType = self._byId.get(row[self._static.dataset.columns.dataset_type_id])
        if recordsForType is None:
            self.refresh()
            recordsForType = self._byId.get(row[self._static.dataset.columns.dataset_type_id])
            assert recordsForType is not None, "Should be guaranteed by foreign key constraints."
        return DatasetRef(
            recordsForType.datasetType,
            dataId=recordsForType.getDataId(id=id),
            id=id,
            run=self._collections[row[self._collections.getRunForeignKeyName()]].name,
        )

    def getCollectionSummary(self, collection: CollectionRecord) -> CollectionSummary:
        # Docstring inherited from DatasetRecordStorageManager.
        return self._summaries.get(collection)

    def schemaDigest(self) -> Optional[str]:
        # Docstring inherited from VersionedExtension.
        return self._defaultSchemaDigest(self._static, self._db.dialect)

    _version: VersionTuple
    """Schema version for this class."""

    _recordStorageType: Type[ByDimensionsDatasetRecordStorage]
    """Type of the storage class returned by this manager."""

    _autoincrement: bool
    """If True then PK column of the dataset table is auto-increment."""

    _idColumnType: type
    """Type of dataset column used to store dataset ID."""


class ByDimensionsDatasetRecordStorageManager(ByDimensionsDatasetRecordStorageManagerBase):
    """Implementation of ByDimensionsDatasetRecordStorageManagerBase which uses
    auto-incremental integer for dataset primary key.
    """

    _version: VersionTuple = _VERSION_INT
    _recordStorageType: Type[ByDimensionsDatasetRecordStorage] = ByDimensionsDatasetRecordStorageInt
    _autoincrement: bool = True
    _idColumnType: type = sqlalchemy.BigInteger

    @classmethod
    def supportsIdGenerationMode(cls, mode: DatasetIdGenEnum) -> bool:
        # Docstring inherited from DatasetRecordStorageManager.
        # MyPy seems confused about enum value types here.
        return mode is mode.UNIQUE  # type: ignore


class ByDimensionsDatasetRecordStorageManagerUUID(ByDimensionsDatasetRecordStorageManagerBase):
    """Implementation of ByDimensionsDatasetRecordStorageManagerBase which uses
    UUID for dataset primary key.
    """

    _version: VersionTuple = _VERSION_UUID
    _recordStorageType: Type[ByDimensionsDatasetRecordStorage] = ByDimensionsDatasetRecordStorageUUID
    _autoincrement: bool = False
    _idColumnType: type = ddl.GUID

    @classmethod
    def supportsIdGenerationMode(cls, mode: DatasetIdGenEnum) -> bool:
        # Docstring inherited from DatasetRecordStorageManager.
        return True
