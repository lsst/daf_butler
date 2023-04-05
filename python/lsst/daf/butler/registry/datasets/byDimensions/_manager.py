from __future__ import annotations

__all__ = ("ByDimensionsDatasetRecordStorageManagerUUID",)

import logging
import warnings
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import sqlalchemy
from lsst.utils.ellipsis import Ellipsis

from ....core import DatasetId, DatasetRef, DatasetType, DimensionUniverse, ddl
from ..._collection_summary import CollectionSummary
from ..._exceptions import ConflictingDefinitionError, DatasetTypeError, OrphanedRecordError
from ...interfaces import DatasetIdGenEnum, DatasetRecordStorage, DatasetRecordStorageManager, VersionTuple
from ...wildcards import DatasetTypeWildcard
from ._storage import ByDimensionsDatasetRecordStorage, ByDimensionsDatasetRecordStorageUUID
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
    from ...interfaces import (
        CollectionManager,
        CollectionRecord,
        Database,
        DimensionRecordStorageManager,
        StaticTablesContext,
    )
    from .tables import StaticDatasetTablesTuple


# This has to be updated on every schema change
_VERSION_UUID = VersionTuple(1, 0, 0)
# Starting with 2.0.0 the `ingest_date` column type uses nanoseconds instead
# of TIMESTAMP. The code supports both 1.0.0 and 2.0.0 for the duration of
# client migration period.
_VERSION_UUID_NS = VersionTuple(2, 0, 0)

_LOG = logging.getLogger(__name__)


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
        registry_schema_version: VersionTuple | None = None,
    ):
        super().__init__(registry_schema_version=registry_schema_version)
        self._db = db
        self._collections = collections
        self._dimensions = dimensions
        self._static = static
        self._summaries = summaries
        self._byName: dict[str, ByDimensionsDatasetRecordStorage] = {}
        self._byId: dict[int, ByDimensionsDatasetRecordStorage] = {}

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
        registry_schema_version: VersionTuple | None = None,
    ) -> DatasetRecordStorageManager:
        # Docstring inherited from DatasetRecordStorageManager.
        specs = cls.makeStaticTableSpecs(
            type(collections), universe=dimensions.universe, schema_version=registry_schema_version
        )
        static: StaticDatasetTablesTuple = context.addTableTuple(specs)  # type: ignore
        summaries = CollectionSummaryManager.initialize(
            db,
            context,
            collections=collections,
            dimensions=dimensions,
        )
        return cls(
            db=db,
            collections=collections,
            dimensions=dimensions,
            static=static,
            summaries=summaries,
            registry_schema_version=registry_schema_version,
        )

    @classmethod
    def currentVersions(cls) -> list[VersionTuple]:
        # Docstring inherited from VersionedExtension.
        return cls._versions

    @classmethod
    def makeStaticTableSpecs(
        cls,
        collections: type[CollectionManager],
        universe: DimensionUniverse,
        schema_version: VersionTuple | None,
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
        schema_version : `VersionTuple` or `None`
            Version of the schema that should be created, if `None` then
            default schema should be used.

        Returns
        -------
        specs : `StaticDatasetTablesTuple`
            A named tuple containing `ddl.TableSpec` instances.
        """
        schema_version = cls.clsNewSchemaVersion(schema_version)
        assert schema_version is not None, "New schema version cannot be None"
        return makeStaticTableSpecs(
            collections,
            universe=universe,
            dtype=cls.getIdColumnType(),
            autoincrement=cls._autoincrement,
            schema_version=schema_version,
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
        onDelete: str | None = None,
        **kwargs: Any,
    ) -> ddl.FieldSpec:
        # Docstring inherited from DatasetRecordStorageManager.
        return addDatasetForeignKey(
            tableSpec, cls.getIdColumnType(), name=name, onDelete=onDelete, constraint=constraint, **kwargs
        )

    def refresh(self) -> None:
        # Docstring inherited from DatasetRecordStorageManager.
        byName: dict[str, ByDimensionsDatasetRecordStorage] = {}
        byId: dict[int, ByDimensionsDatasetRecordStorage] = {}
        dataset_types: dict[int, DatasetType] = {}
        c = self._static.dataset_type.columns
        with self._db.query(self._static.dataset_type.select()) as sql_result:
            sql_rows = sql_result.mappings().fetchall()
        for row in sql_rows:
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
                use_astropy_ingest_date=self.ingest_date_dtype() is ddl.AstropyTimeNsecTai,
            )
            byName[datasetType.name] = storage
            byId[storage._dataset_type_id] = storage
            dataset_types[row["id"]] = datasetType
        self._byName = byName
        self._byId = byId
        self._summaries.refresh(dataset_types)

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

    def find(self, name: str) -> DatasetRecordStorage | None:
        # Docstring inherited from DatasetRecordStorageManager.
        return self._byName.get(name)

    def register(self, datasetType: DatasetType) -> tuple[DatasetRecordStorage, bool]:
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
                use_astropy_ingest_date=self.ingest_date_dtype() is ddl.AstropyTimeNsecTai,
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

    def resolve_wildcard(
        self,
        expression: Any,
        components: bool | None = None,
        missing: list[str] | None = None,
        explicit_only: bool = False,
        components_deprecated: bool = True,
    ) -> dict[DatasetType, list[str | None]]:
        wildcard = DatasetTypeWildcard.from_expression(expression)
        result: defaultdict[DatasetType, set[str | None]] = defaultdict(set)
        # This message can be transformed into an error on DM-36303 after v26,
        # and the components and components_deprecated arguments can be merged
        # into one on DM-36457 after v27.
        deprecation_message = (
            "Querying for component datasets via Registry query methods is deprecated in favor of using "
            "DatasetRef and DatasetType methods on parent datasets. Only components=False will be supported "
            "after v26, and the components argument will be removed after v27."
        )
        for name, dataset_type in wildcard.values.items():
            parent_name, component_name = DatasetType.splitDatasetTypeName(name)
            if component_name is not None and components_deprecated:
                warnings.warn(deprecation_message, FutureWarning)
            if (found_storage := self.find(parent_name)) is not None:
                found_parent = found_storage.datasetType
                if component_name is not None:
                    found = found_parent.makeComponentDatasetType(component_name)
                else:
                    found = found_parent
                if dataset_type is not None:
                    if dataset_type.is_compatible_with(found):
                        # Prefer the given dataset type to enable storage class
                        # conversions.
                        if component_name is not None:
                            found_parent = dataset_type.makeCompositeDatasetType()
                        else:
                            found_parent = dataset_type
                    else:
                        raise DatasetTypeError(
                            f"Dataset type definition in query expression {dataset_type} is "
                            f"not compatible with the registered type {found}."
                        )
                result[found_parent].add(component_name)
            elif missing is not None:
                missing.append(name)
        already_warned = False
        if wildcard.patterns is Ellipsis:
            if explicit_only:
                raise TypeError(
                    "Universal wildcard '...' is not permitted for dataset types in this context."
                )
            for storage in self._byName.values():
                result[storage.datasetType].add(None)
                if components:
                    try:
                        result[storage.datasetType].update(
                            storage.datasetType.storageClass.allComponents().keys()
                        )
                        if (
                            storage.datasetType.storageClass.allComponents()
                            and not already_warned
                            and components_deprecated
                        ):
                            warnings.warn(deprecation_message, FutureWarning)
                            already_warned = True
                    except KeyError as err:
                        _LOG.warning(
                            f"Could not load storage class {err} for {storage.datasetType.name}; "
                            "if it has components they will not be included in query results.",
                        )
        elif wildcard.patterns:
            if explicit_only:
                # After v26 this should raise DatasetTypeExpressionError, to
                # be implemented on DM-36303.
                warnings.warn(
                    "Passing wildcard patterns here is deprecated and will be prohibited after v26.",
                    FutureWarning,
                )
            for storage in self._byName.values():
                if any(p.fullmatch(storage.datasetType.name) for p in wildcard.patterns):
                    result[storage.datasetType].add(None)
            if components is not False:
                for storage in self._byName.values():
                    if components is None and storage.datasetType in result:
                        continue
                    try:
                        components_for_parent = storage.datasetType.storageClass.allComponents().keys()
                    except KeyError as err:
                        _LOG.warning(
                            f"Could not load storage class {err} for {storage.datasetType.name}; "
                            "if it has components they will not be included in query results."
                        )
                        continue
                    for component_name in components_for_parent:
                        if any(
                            p.fullmatch(
                                DatasetType.nameWithComponent(storage.datasetType.name, component_name)
                            )
                            for p in wildcard.patterns
                        ):
                            result[storage.datasetType].add(component_name)
                            if not already_warned and components_deprecated:
                                warnings.warn(deprecation_message, FutureWarning)
                                already_warned = True
        return {k: list(v) for k, v in result.items()}

    def getDatasetRef(self, id: DatasetId) -> DatasetRef | None:
        # Docstring inherited from DatasetRecordStorageManager.
        sql = (
            sqlalchemy.sql.select(
                self._static.dataset.columns.dataset_type_id,
                self._static.dataset.columns[self._collections.getRunForeignKeyName()],
            )
            .select_from(self._static.dataset)
            .where(self._static.dataset.columns.id == id)
        )
        with self._db.query(sql) as sql_result:
            row = sql_result.mappings().fetchone()
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

    _versions: list[VersionTuple]
    """Schema version for this class."""

    _recordStorageType: type[ByDimensionsDatasetRecordStorage]
    """Type of the storage class returned by this manager."""

    _autoincrement: bool
    """If True then PK column of the dataset table is auto-increment."""

    _idColumnType: type
    """Type of dataset column used to store dataset ID."""


class ByDimensionsDatasetRecordStorageManagerUUID(ByDimensionsDatasetRecordStorageManagerBase):
    """Implementation of ByDimensionsDatasetRecordStorageManagerBase which uses
    UUID for dataset primary key.
    """

    _versions: list[VersionTuple] = [_VERSION_UUID, _VERSION_UUID_NS]
    _recordStorageType: type[ByDimensionsDatasetRecordStorage] = ByDimensionsDatasetRecordStorageUUID
    _autoincrement: bool = False
    _idColumnType: type = ddl.GUID

    @classmethod
    def supportsIdGenerationMode(cls, mode: DatasetIdGenEnum) -> bool:
        # Docstring inherited from DatasetRecordStorageManager.
        return True

    @classmethod
    def _newDefaultSchemaVersion(cls) -> VersionTuple:
        # Docstring inherited from VersionedExtension.

        # By default return 1.0.0 so that older clients can still access new
        # registries created with a default config.
        return _VERSION_UUID

    def ingest_date_dtype(self) -> type:
        """Return type of the ``ingest_date`` column."""
        schema_version = self.newSchemaVersion()
        if schema_version is not None and schema_version.major > 1:
            return ddl.AstropyTimeNsecTai
        else:
            return sqlalchemy.TIMESTAMP
