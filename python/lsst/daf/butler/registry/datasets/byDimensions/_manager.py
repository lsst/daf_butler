from __future__ import annotations

__all__ = ("ByDimensionsDatasetRecordStorageManagerUUID",)

import dataclasses
import datetime
import logging
from collections.abc import Iterable, Mapping, Sequence, Set
from typing import TYPE_CHECKING, Any, ClassVar

import astropy.time
import sqlalchemy

from lsst.daf.relation import Relation, sql

from .... import ddl
from ...._collection_type import CollectionType
from ...._column_tags import DatasetColumnTag, DimensionKeyColumnTag
from ...._column_type_info import LogicalColumn
from ...._dataset_ref import DatasetId, DatasetIdFactory, DatasetIdGenEnum, DatasetRef
from ...._dataset_type import DatasetType, get_dataset_type_name
from ...._exceptions import CollectionTypeError, MissingDatasetTypeError
from ...._exceptions_legacy import DatasetTypeError
from ...._timespan import Timespan
from ....dimensions import DataCoordinate, DimensionGroup, DimensionUniverse
from ....direct_query_driver import SqlJoinsBuilder, SqlSelectBuilder  # new query system, server+direct only
from ....queries import tree as qt  # new query system, both clients + server
from ..._caching_context import CachingContext
from ..._collection_summary import CollectionSummary
from ..._exceptions import ConflictingDefinitionError, DatasetTypeExpressionError, OrphanedRecordError
from ...interfaces import DatasetRecordStorageManager, RunRecord, VersionTuple
from ...queries import SqlQueryContext  # old registry query system
from ...wildcards import DatasetTypeWildcard
from ._dataset_type_cache import DatasetTypeCache
from .summaries import CollectionSummaryManager
from .tables import DynamicTables, addDatasetForeignKey, makeStaticTableSpecs, makeTagTableSpec

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
# TODO: 1.0.0 can be removed once all repos were migrated to 2.0.0.
_VERSION_UUID = VersionTuple(1, 0, 0)
# Starting with 2.0.0 the `ingest_date` column type uses nanoseconds instead
# of TIMESTAMP. The code supports both 1.0.0 and 2.0.0 for the duration of
# client migration period.
_VERSION_UUID_NS = VersionTuple(2, 0, 0)

_LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class _DatasetTypeRecord:
    """Contents of a single dataset type record."""

    dataset_type: DatasetType
    dataset_type_id: int
    dimensions_key: int
    tag_table_name: str
    calib_table_name: str | None

    def make_dynamic_tables(self) -> DynamicTables:
        return DynamicTables(
            self.dataset_type.dimensions, self.dimensions_key, self.tag_table_name, self.calib_table_name
        )

    def update_dynamic_tables(self, current: DynamicTables) -> DynamicTables:
        assert self.dimensions_key == current.dimensions_key
        assert self.tag_table_name == current.tags_name
        if self.calib_table_name is not None:
            if current.calibs_name is not None:
                assert self.calib_table_name == current.calibs_name
            else:
                # Some previously-cached dataset type had the same dimensions
                # but was not a calibration.
                current = current.copy(calibs_name=self.calib_table_name)
            # If some previously-cached dataset type was a calibration but this
            # one isn't, we don't want to forget the calibs table.
        return current


@dataclasses.dataclass
class _DatasetRecordStorage:
    """Information cached about a dataset type.

    This combines information cached with different keys - the dataset type
    and its ID are cached by name, while the tables are cached by the dataset
    types dimensions (and hence shared with other dataset types that have the
    same dimensions).
    """

    dataset_type: DatasetType
    dataset_type_id: int
    dynamic_tables: DynamicTables


class ByDimensionsDatasetRecordStorageManagerUUID(DatasetRecordStorageManager):
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
    registry_schema_version : `VersionTuple` or `None`, optional
        Version of registry schema.
    _cache : `None`, optional
        For internal use only.
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
        _cache: DatasetTypeCache | None = None,
    ):
        super().__init__(registry_schema_version=registry_schema_version)
        self._db = db
        self._collections = collections
        self._dimensions = dimensions
        self._static = static
        self._summaries = summaries
        self._cache = _cache if _cache is not None else DatasetTypeCache()
        self._use_astropy_ingest_date = self.ingest_date_dtype() is ddl.AstropyTimeNsecTai
        self._run_key_column = collections.getRunForeignKeyName()

    _versions: ClassVar[list[VersionTuple]] = [_VERSION_UUID, _VERSION_UUID_NS]

    _id_maker: ClassVar[DatasetIdFactory] = DatasetIdFactory()
    """Factory for dataset IDs. In the future this factory may be shared with
    other classes (e.g. Registry).
    """

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
        caching_context: CachingContext,
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
            dataset_type_table=static.dataset_type,
            caching_context=caching_context,
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
        collections : `CollectionManager`
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
            schema_version=schema_version,
        )

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
        return addDatasetForeignKey(tableSpec, name=name, onDelete=onDelete, constraint=constraint, **kwargs)

    @classmethod
    def _newDefaultSchemaVersion(cls) -> VersionTuple:
        # Docstring inherited from VersionedExtension.
        return _VERSION_UUID_NS

    def clone(
        self,
        *,
        db: Database,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
        caching_context: CachingContext,
    ) -> ByDimensionsDatasetRecordStorageManagerUUID:
        return ByDimensionsDatasetRecordStorageManagerUUID(
            db=db,
            collections=collections,
            dimensions=dimensions,
            static=self._static,
            summaries=self._summaries.clone(db=db, collections=collections, caching_context=caching_context),
            registry_schema_version=self._registry_schema_version,
            # See notes on DatasetTypeCache.clone() about cache behavior after
            # cloning.
            _cache=self._cache.clone(),
        )

    def refresh(self) -> None:
        # Docstring inherited from DatasetRecordStorageManager.
        self._cache.clear()

    def remove_dataset_type(self, name: str) -> None:
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

    def get_dataset_type(self, name: str) -> DatasetType:
        # Docstring inherited from DatasetRecordStorageManager.
        return self._find_storage(name).dataset_type

    def register_dataset_type(self, dataset_type: DatasetType) -> bool:
        # Docstring inherited from DatasetRecordStorageManager.
        #
        # This is one of three places where we populate the dataset type cache.
        # See the comment in _fetch_dataset_types for how these are related and
        # invariants they must maintain.
        #
        if dataset_type.isComponent():
            raise ValueError(
                f"Component dataset types can not be stored in registry. Rejecting {dataset_type.name}"
            )
        record = self._fetch_dataset_type_record(dataset_type.name)
        if record is None:
            if (dynamic_tables := self._cache.get_by_dimensions(dataset_type.dimensions)) is None:
                dimensions_key = self._dimensions.save_dimension_group(dataset_type.dimensions)
                dynamic_tables = DynamicTables.from_dimensions_key(
                    dataset_type.dimensions, dimensions_key, dataset_type.isCalibration()
                )
                dynamic_tables.create(self._db, type(self._collections), self._cache.tables)
            elif dataset_type.isCalibration() and dynamic_tables.calibs_name is None:
                dynamic_tables = dynamic_tables.add_calibs(
                    self._db, type(self._collections), self._cache.tables
                )
            row, inserted = self._db.sync(
                self._static.dataset_type,
                keys={"name": dataset_type.name},
                compared={
                    "dimensions_key": dynamic_tables.dimensions_key,
                    # Force the storage class to be loaded to ensure it
                    # exists and there is no typo in the name.
                    "storage_class": dataset_type.storageClass.name,
                },
                extra={
                    "tag_association_table": dynamic_tables.tags_name,
                    "calibration_association_table": (
                        dynamic_tables.calibs_name if dataset_type.isCalibration() else None
                    ),
                },
                returning=["id", "tag_association_table"],
            )
            # Make sure that cache is updated
            if row is not None:
                self._cache.add(dataset_type, row["id"])
                self._cache.add_by_dimensions(dataset_type.dimensions, dynamic_tables)
        else:
            if dataset_type != record.dataset_type:
                raise ConflictingDefinitionError(
                    f"Given dataset type {dataset_type} is inconsistent "
                    f"with database definition {record.dataset_type}."
                )
            inserted = False
        return bool(inserted)

    def resolve_wildcard(
        self,
        expression: Any,
        missing: list[str] | None = None,
        explicit_only: bool = False,
    ) -> list[DatasetType]:
        wildcard = DatasetTypeWildcard.from_expression(expression)
        result: list[DatasetType] = []
        for name, dataset_type in wildcard.values.items():
            parent_name, component_name = DatasetType.splitDatasetTypeName(name)
            if component_name is not None:
                raise DatasetTypeError(
                    "Component dataset types are not supported in Registry methods; use DatasetRef or "
                    "DatasetType methods to obtain components from parents instead."
                )
            try:
                resolved_dataset_type = self.get_dataset_type(parent_name)
            except MissingDatasetTypeError:
                if missing is not None:
                    missing.append(name)
            else:
                if dataset_type is not None:
                    if dataset_type.is_compatible_with(resolved_dataset_type):
                        # Prefer the given dataset type to enable storage class
                        # conversions.
                        resolved_dataset_type = dataset_type
                    else:
                        raise DatasetTypeError(
                            f"Dataset type definition in query expression {dataset_type} is "
                            f"not compatible with the registered type {resolved_dataset_type}."
                        )
                result.append(resolved_dataset_type)
        if wildcard.patterns is ...:
            if explicit_only:
                raise TypeError(
                    "Universal wildcard '...' is not permitted for dataset types in this context."
                )
            for datasetType in self._fetch_dataset_types():
                result.append(datasetType)
        elif wildcard.patterns:
            if explicit_only:
                raise DatasetTypeExpressionError(
                    "Dataset type wildcard expressions are not supported in this context."
                )
            dataset_types = self._fetch_dataset_types()
            for datasetType in dataset_types:
                if any(p.fullmatch(datasetType.name) for p in wildcard.patterns):
                    result.append(datasetType)

        return result

    def getDatasetRef(self, id: DatasetId) -> DatasetRef | None:
        # Docstring inherited from DatasetRecordStorageManager.
        #
        # This is one of three places where we populate the dataset type cache.
        # See the comment in _fetch_dataset_types for how these are related and
        # invariants they must maintain.
        #
        sql = (
            sqlalchemy.sql.select(
                self._static.dataset.columns.dataset_type_id,
                self._static.dataset.columns[self._collections.getRunForeignKeyName()],
                *self._static.dataset_type.columns,
            )
            .select_from(self._static.dataset)
            .join(self._static.dataset_type)
            .where(self._static.dataset.columns.id == id)
        )
        with self._db.query(sql) as sql_result:
            row = sql_result.mappings().fetchone()
        if row is None:
            return None
        run = row[self._run_key_column]
        record = self._record_from_row(row)
        _, dataset_type_id = self._cache.get(record.dataset_type.name)
        if dataset_type_id is None:
            self._cache.add(record.dataset_type, record.dataset_type_id)
        else:
            assert record.dataset_type_id == dataset_type_id, "Two IDs for the same dataset type name!"
        dynamic_tables = self._cache.get_by_dimensions(record.dataset_type.dimensions)
        if dynamic_tables is None:
            dynamic_tables = record.make_dynamic_tables()
            self._cache.add_by_dimensions(record.dataset_type.dimensions, dynamic_tables)
        if record.dataset_type.dimensions:
            # This query could return multiple rows (one for each tagged
            # collection the dataset is in, plus one for its run collection),
            # and we don't care which of those we get.
            tags_table = self._get_tags_table(dynamic_tables)
            data_id_sql = (
                tags_table.select()
                .where(
                    sqlalchemy.sql.and_(
                        tags_table.columns.dataset_id == id,
                        tags_table.columns.dataset_type_id == record.dataset_type_id,
                    )
                )
                .limit(1)
            )
            with self._db.query(data_id_sql) as sql_result:
                data_id_row = sql_result.mappings().fetchone()
            assert data_id_row is not None, "Data ID should be present if dataset is."
            data_id = DataCoordinate.from_required_values(
                record.dataset_type.dimensions,
                tuple(data_id_row[dimension] for dimension in record.dataset_type.dimensions.required),
            )
        else:
            data_id = DataCoordinate.make_empty(self._dimensions.universe)
        return DatasetRef(
            record.dataset_type,
            dataId=data_id,
            id=id,
            run=self._collections[run].name,
        )

    def _fetch_dataset_type_record(self, name: str) -> _DatasetTypeRecord | None:
        """Retrieve all dataset types defined in database.

        Yields
        ------
        dataset_types : `_DatasetTypeRecord`
            Information from a single database record.
        """
        c = self._static.dataset_type.columns
        stmt = self._static.dataset_type.select().where(c.name == name)
        with self._db.query(stmt) as sql_result:
            row = sql_result.mappings().one_or_none()
        if row is None:
            return None
        else:
            return self._record_from_row(row)

    def _record_from_row(self, row: Mapping) -> _DatasetTypeRecord:
        name = row["name"]
        dimensions = self._dimensions.load_dimension_group(row["dimensions_key"])
        calibTableName = row["calibration_association_table"]
        datasetType = DatasetType(
            name, dimensions, row["storage_class"], isCalibration=(calibTableName is not None)
        )
        return _DatasetTypeRecord(
            dataset_type=datasetType,
            dataset_type_id=row["id"],
            dimensions_key=row["dimensions_key"],
            tag_table_name=row["tag_association_table"],
            calib_table_name=calibTableName,
        )

    def _dataset_type_from_row(self, row: Mapping) -> DatasetType:
        return self._record_from_row(row).dataset_type

    def preload_cache(self) -> None:
        self._fetch_dataset_types()

    def _fetch_dataset_types(self) -> list[DatasetType]:
        """Fetch list of all defined dataset types."""
        # This is one of three places we populate the dataset type cache:
        #
        # - This method handles almost all requests for dataset types that
        #   should already exist.  It always marks the cache as "full" in both
        #   dataset type names and dimensions.
        #
        # - register_dataset_type handles the case where the dataset type might
        #   not existing yet.  Since it can only add a single dataset type, it
        #   never changes whether the cache is full.
        #
        # - getDatasetRef is a special case for a dataset type that should
        #   already exist, but is looked up via a dataset ID rather than its
        #   name.  It also never changes whether the cache is full, and it's
        #   handles separately essentially as an optimization: we can fetch a
        #   single dataset type definition record in a join when we query for
        #   the dataset type based on the dataset ID, and this is better than
        #   blindly fetching all dataset types in a separate query.
        #
        # In all three cases, we require that the per-dimensions data be cached
        # whenever a dataset type is added to the cache by name, to reduce the
        # number of possible states the cache can be in and minimize the number
        # of queries.
        if self._cache.full:
            return [dataset_type for dataset_type, _ in self._cache.items()]
        with self._db.query(self._static.dataset_type.select()) as sql_result:
            sql_rows = sql_result.mappings().fetchall()
        records = [self._record_from_row(row) for row in sql_rows]
        # Cache everything and specify that cache is complete.
        cache_data: list[tuple[DatasetType, int]] = []
        cache_dimensions_data: dict[DimensionGroup, DynamicTables] = {}
        for record in records:
            cache_data.append((record.dataset_type, record.dataset_type_id))
            if (dynamic_tables := cache_dimensions_data.get(record.dataset_type.dimensions)) is None:
                tables = record.make_dynamic_tables()
            else:
                tables = record.update_dynamic_tables(dynamic_tables)
            cache_dimensions_data[record.dataset_type.dimensions] = tables
        self._cache.set(
            cache_data, full=True, dimensions_data=cache_dimensions_data.items(), dimensions_full=True
        )
        return [record.dataset_type for record in records]

    def _find_storage(self, name: str) -> _DatasetRecordStorage:
        """Find a dataset type and the extra information needed to work with
        it, utilizing and populating the cache as needed.
        """
        dataset_type, dataset_type_id = self._cache.get(name)
        if dataset_type is not None:
            tables = self._cache.get_by_dimensions(dataset_type.dimensions)
            assert dataset_type_id is not None and tables is not None, (
                "Dataset type cache population is incomplete."
            )
            return _DatasetRecordStorage(
                dataset_type=dataset_type, dataset_type_id=dataset_type_id, dynamic_tables=tables
            )
        else:
            # On the first cache miss populate the cache with complete list
            # of dataset types (if it was not done yet).
            if not self._cache.full:
                self._fetch_dataset_types()
                # Try again
                dataset_type, dataset_type_id = self._cache.get(name)
            if dataset_type is not None:
                tables = self._cache.get_by_dimensions(dataset_type.dimensions)
                assert dataset_type_id is not None and tables is not None, (
                    "Dataset type cache population is incomplete."
                )
                return _DatasetRecordStorage(
                    dataset_type=dataset_type, dataset_type_id=dataset_type_id, dynamic_tables=tables
                )
        record = self._fetch_dataset_type_record(name)
        if record is not None:
            self._cache.add(record.dataset_type, record.dataset_type_id)
            tables = record.make_dynamic_tables()
            self._cache.add_by_dimensions(record.dataset_type.dimensions, tables)
            return _DatasetRecordStorage(record.dataset_type, record.dataset_type_id, tables)
        raise MissingDatasetTypeError(f"Dataset type {name!r} does not exist.")

    def getCollectionSummary(self, collection: CollectionRecord) -> CollectionSummary:
        # Docstring inherited from DatasetRecordStorageManager.
        summaries = self._summaries.fetch_summaries([collection], None, self._dataset_type_from_row)
        return summaries[collection.key]

    def fetch_summaries(
        self,
        collections: Iterable[CollectionRecord],
        dataset_types: Iterable[DatasetType] | Iterable[str] | None = None,
    ) -> Mapping[Any, CollectionSummary]:
        # Docstring inherited from DatasetRecordStorageManager.
        dataset_type_names: Iterable[str] | None = None
        if dataset_types is not None:
            dataset_type_names = set(get_dataset_type_name(dt) for dt in dataset_types)
        return self._summaries.fetch_summaries(collections, dataset_type_names, self._dataset_type_from_row)

    def ingest_date_dtype(self) -> type:
        """Return type of the ``ingest_date`` column."""
        schema_version = self.newSchemaVersion()
        if schema_version is not None and schema_version.major > 1:
            return ddl.AstropyTimeNsecTai
        else:
            return sqlalchemy.TIMESTAMP

    def insert(
        self,
        dataset_type_name: str,
        run: RunRecord,
        data_ids: Iterable[DataCoordinate],
        id_generation_mode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
    ) -> list[DatasetRef]:
        # Docstring inherited from DatasetRecordStorageManager.
        if (storage := self._find_storage(dataset_type_name)) is None:
            raise MissingDatasetTypeError(f"Dataset type {dataset_type_name!r} has not been registered.")
        # Current timestamp, type depends on schema version. Use microsecond
        # precision for astropy time to keep things consistent with
        # TIMESTAMP(6) SQL type.
        timestamp: datetime.datetime | astropy.time.Time
        if self._use_astropy_ingest_date:
            # Astropy `now()` precision should be the same as `now()` which
            # should mean microsecond.
            timestamp = astropy.time.Time.now()
        else:
            timestamp = datetime.datetime.now(datetime.UTC)

        # Iterate over data IDs, transforming a possibly-single-pass iterable
        # into a list.
        data_id_list: list[DataCoordinate] = []
        rows = []
        summary = CollectionSummary()
        for dataId in summary.add_data_ids_generator(storage.dataset_type, data_ids):
            data_id_list.append(dataId)
            rows.append(
                {
                    "id": self._id_maker.makeDatasetId(
                        run.name, storage.dataset_type, dataId, id_generation_mode
                    ),
                    "dataset_type_id": storage.dataset_type_id,
                    self._run_key_column: run.key,
                    "ingest_date": timestamp,
                }
            )
        if not rows:
            # Just in case an empty collection is provided we want to avoid
            # adding dataset type to summary tables.
            return []

        with self._db.transaction():
            # Insert into the static dataset table.
            self._db.insert(self._static.dataset, *rows)
            # Update the summary tables for this collection in case this is the
            # first time this dataset type or these governor values will be
            # inserted there.
            self._summaries.update(run, [storage.dataset_type_id], summary)
            # Combine the generated dataset_id values and data ID fields to
            # form rows to be inserted into the tags table.
            protoTagsRow = {
                "dataset_type_id": storage.dataset_type_id,
                self._collections.getCollectionForeignKeyName(): run.key,
            }
            tagsRows = [
                dict(protoTagsRow, dataset_id=row["id"], **dataId.required)
                for dataId, row in zip(data_id_list, rows, strict=True)
            ]
            # Insert those rows into the tags table.
            self._db.insert(self._get_tags_table(storage.dynamic_tables), *tagsRows)

        return [
            DatasetRef(
                datasetType=storage.dataset_type,
                dataId=dataId,
                id=row["id"],
                run=run.name,
            )
            for dataId, row in zip(data_id_list, rows, strict=True)
        ]

    def import_(
        self,
        dataset_type: DatasetType,
        run: RunRecord,
        data_ids: Mapping[DatasetId, DataCoordinate],
    ) -> list[DatasetRef]:
        # Docstring inherited from DatasetRecordStorageManager.
        if not data_ids:
            # Just in case an empty mapping is provided we want to avoid
            # adding dataset type to summary tables.
            return []
        if (storage := self._find_storage(dataset_type.name)) is None:
            raise MissingDatasetTypeError(f"Dataset type {dataset_type.name!r} has not been registered.")
        # Current timestamp, type depends on schema version.
        if self._use_astropy_ingest_date:
            # Astropy `now()` precision should be the same as `now()` which
            # should mean microsecond.
            timestamp = sqlalchemy.sql.literal(astropy.time.Time.now(), type_=ddl.AstropyTimeNsecTai)
        else:
            timestamp = sqlalchemy.sql.literal(datetime.datetime.now(datetime.UTC))
        # We'll insert all new rows into a temporary table
        table_spec = makeTagTableSpec(
            storage.dataset_type.dimensions, type(self._collections), constraints=False
        )
        collection_fkey_name = self._collections.getCollectionForeignKeyName()
        proto_ags_row = {
            "dataset_type_id": storage.dataset_type_id,
            collection_fkey_name: run.key,
        }
        tmpRows = [
            dict(proto_ags_row, dataset_id=dataset_id, **data_id.required)
            for dataset_id, data_id in data_ids.items()
        ]
        with self._db.transaction(for_temp_tables=True), self._db.temporary_table(table_spec) as tmp_tags:
            # store all incoming data in a temporary table
            self._db.insert(tmp_tags, *tmpRows)
            # There are some checks that we want to make for consistency
            # of the new datasets with existing ones.
            self._validate_import(storage, tmp_tags, run)
            # Before we merge temporary table into dataset/tags we need to
            # drop datasets which are already there (and do not conflict).
            self._db.deleteWhere(
                tmp_tags,
                tmp_tags.columns.dataset_id.in_(sqlalchemy.sql.select(self._static.dataset.columns.id)),
            )
            # Copy it into dataset table, need to re-label some columns.
            self._db.insert(
                self._static.dataset,
                select=sqlalchemy.sql.select(
                    tmp_tags.columns.dataset_id.label("id"),
                    tmp_tags.columns.dataset_type_id,
                    tmp_tags.columns[collection_fkey_name].label(self._run_key_column),
                    timestamp.label("ingest_date"),
                ),
            )
            refs = [
                DatasetRef(
                    datasetType=dataset_type,
                    id=dataset_id,
                    dataId=dataId,
                    run=run.name,
                )
                for dataset_id, dataId in data_ids.items()
            ]
            # Update the summary tables for this collection in case this
            # is the first time this dataset type or these governor values
            # will be inserted there.
            summary = CollectionSummary()
            summary.add_datasets(refs)
            self._summaries.update(run, [storage.dataset_type_id], summary)
            # Copy from temp table into tags table.
            self._db.insert(self._get_tags_table(storage.dynamic_tables), select=tmp_tags.select())
        return refs

    def _validate_import(
        self, storage: _DatasetRecordStorage, tmp_tags: sqlalchemy.schema.Table, run: RunRecord
    ) -> None:
        """Validate imported refs against existing datasets.

        Parameters
        ----------
        storage : `_DatasetREcordStorage`
            Struct that holds the tables and ID for a dataset type.
        tmp_tags : `sqlalchemy.schema.Table`
            Temporary table with new datasets and the same schema as tags
            table.
        run : `RunRecord`
            The record object describing the `~CollectionType.RUN` collection.

        Raises
        ------
        ConflictingDefinitionError
            Raise if new datasets conflict with existing ones.
        """
        dataset = self._static.dataset
        tags = self._get_tags_table(storage.dynamic_tables)
        collection_fkey_name = self._collections.getCollectionForeignKeyName()

        # Check that existing datasets have the same dataset type and
        # run.
        query = (
            sqlalchemy.sql.select(
                dataset.columns.id.label("dataset_id"),
                dataset.columns.dataset_type_id.label("dataset_type_id"),
                tmp_tags.columns.dataset_type_id.label("new_dataset_type_id"),
                dataset.columns[self._run_key_column].label("run"),
                tmp_tags.columns[collection_fkey_name].label("new_run"),
            )
            .select_from(dataset.join(tmp_tags, dataset.columns.id == tmp_tags.columns.dataset_id))
            .where(
                sqlalchemy.sql.or_(
                    dataset.columns.dataset_type_id != tmp_tags.columns.dataset_type_id,
                    dataset.columns[self._run_key_column] != tmp_tags.columns[collection_fkey_name],
                )
            )
            .limit(1)
        )
        with self._db.query(query) as result:
            # Only include the first one in the exception message
            if (row := result.first()) is not None:
                existing_run = self._collections[row.run].name
                new_run = self._collections[row.new_run].name
                if row.dataset_type_id == storage.dataset_type_id:
                    if row.new_dataset_type_id == storage.dataset_type_id:
                        raise ConflictingDefinitionError(
                            f"Current run {existing_run!r} and new run {new_run!r} do not agree for "
                            f"dataset {row.dataset_id}."
                        )
                    else:
                        raise ConflictingDefinitionError(
                            f"Dataset {row.dataset_id} was provided with type {storage.dataset_type.name!r} "
                            f"in run {new_run!r}, but was already defined with type ID {row.dataset_type_id} "
                            f"in run {run!r}."
                        )
                else:
                    raise ConflictingDefinitionError(
                        f"Dataset {row.dataset_id} was provided with type ID {row.new_dataset_type_id} "
                        f"in run {new_run!r}, but was already defined with type "
                        f"{storage.dataset_type.name!r} in run {run!r}."
                    )

        # Check that matching dataset in tags table has the same DataId.
        query = (
            sqlalchemy.sql.select(
                tags.columns.dataset_id,
                tags.columns.dataset_type_id.label("type_id"),
                tmp_tags.columns.dataset_type_id.label("new_type_id"),
                *[tags.columns[dim] for dim in storage.dataset_type.dimensions.required],
                *[
                    tmp_tags.columns[dim].label(f"new_{dim}")
                    for dim in storage.dataset_type.dimensions.required
                ],
            )
            .select_from(tags.join(tmp_tags, tags.columns.dataset_id == tmp_tags.columns.dataset_id))
            .where(
                sqlalchemy.sql.or_(
                    tags.columns.dataset_type_id != tmp_tags.columns.dataset_type_id,
                    *[
                        tags.columns[dim] != tmp_tags.columns[dim]
                        for dim in storage.dataset_type.dimensions.required
                    ],
                )
            )
            .limit(1)
        )

        with self._db.query(query) as result:
            if (row := result.first()) is not None:
                # Only include the first one in the exception message
                raise ConflictingDefinitionError(
                    f"Existing dataset type or dataId do not match new dataset: {row._asdict()}"
                )

        # Check that matching run+dataId have the same dataset ID.
        query = (
            sqlalchemy.sql.select(
                *[tags.columns[dim] for dim in storage.dataset_type.dimensions.required],
                tags.columns.dataset_id,
                tmp_tags.columns.dataset_id.label("new_dataset_id"),
                tags.columns[collection_fkey_name],
                tmp_tags.columns[collection_fkey_name].label(f"new_{collection_fkey_name}"),
            )
            .select_from(
                tags.join(
                    tmp_tags,
                    sqlalchemy.sql.and_(
                        tags.columns.dataset_type_id == tmp_tags.columns.dataset_type_id,
                        tags.columns[collection_fkey_name] == tmp_tags.columns[collection_fkey_name],
                        *[
                            tags.columns[dim] == tmp_tags.columns[dim]
                            for dim in storage.dataset_type.dimensions.required
                        ],
                    ),
                )
            )
            .where(tags.columns.dataset_id != tmp_tags.columns.dataset_id)
            .limit(1)
        )
        with self._db.query(query) as result:
            # only include the first one in the exception message
            if (row := result.first()) is not None:
                data_id = {dim: getattr(row, dim) for dim in storage.dataset_type.dimensions.required}
                existing_collection = self._collections[getattr(row, collection_fkey_name)].name
                new_collection = self._collections[getattr(row, f"new_{collection_fkey_name}")].name
                raise ConflictingDefinitionError(
                    f"Dataset with type {storage.dataset_type.name!r} and data ID {data_id} "
                    f"has ID {row.dataset_id} in existing collection {existing_collection!r} "
                    f"but ID {row.new_dataset_id} in new collection {new_collection!r}."
                )

    def delete(self, datasets: Iterable[DatasetId | DatasetRef]) -> None:
        # Docstring inherited from DatasetRecordStorageManager.
        # Only delete from common dataset table; ON DELETE foreign key clauses
        # will handle the rest.
        self._db.delete(
            self._static.dataset,
            ["id"],
            *[{"id": getattr(dataset, "id", dataset)} for dataset in datasets],
        )

    def associate(
        self, dataset_type: DatasetType, collection: CollectionRecord, datasets: Iterable[DatasetRef]
    ) -> None:
        # Docstring inherited from DatasetRecordStorageManager.
        if (storage := self._find_storage(dataset_type.name)) is None:
            raise MissingDatasetTypeError(f"Dataset type {dataset_type.name!r} has not been registered.")
        if collection.type is not CollectionType.TAGGED:
            raise CollectionTypeError(
                f"Cannot associate into collection '{collection.name}' "
                f"of type {collection.type.name}; must be TAGGED."
            )
        proto_row = {
            self._collections.getCollectionForeignKeyName(): collection.key,
            "dataset_type_id": storage.dataset_type_id,
        }
        rows = []
        summary = CollectionSummary()
        for dataset in summary.add_datasets_generator(datasets):
            rows.append(dict(proto_row, dataset_id=dataset.id, **dataset.dataId.required))
        if rows:
            # Update the summary tables for this collection in case this is the
            # first time this dataset type or these governor values will be
            # inserted there.
            self._summaries.update(collection, [storage.dataset_type_id], summary)
            # Update the tag table itself.
            self._db.replace(self._get_tags_table(storage.dynamic_tables), *rows)

    def disassociate(
        self, dataset_type: DatasetType, collection: CollectionRecord, datasets: Iterable[DatasetRef]
    ) -> None:
        # Docstring inherited from DatasetRecordStorageManager.
        if (storage := self._find_storage(dataset_type.name)) is None:
            raise MissingDatasetTypeError(f"Dataset type {dataset_type.name!r} has not been registered.")
        if collection.type is not CollectionType.TAGGED:
            raise CollectionTypeError(
                f"Cannot disassociate from collection '{collection.name}' "
                f"of type {collection.type.name}; must be TAGGED."
            )
        rows = [
            {
                "dataset_id": dataset.id,
                self._collections.getCollectionForeignKeyName(): collection.key,
            }
            for dataset in datasets
        ]
        self._db.delete(
            self._get_tags_table(storage.dynamic_tables),
            ["dataset_id", self._collections.getCollectionForeignKeyName()],
            *rows,
        )

    def certify(
        self,
        dataset_type: DatasetType,
        collection: CollectionRecord,
        datasets: Iterable[DatasetRef],
        timespan: Timespan,
        context: SqlQueryContext,
    ) -> None:
        # Docstring inherited from DatasetRecordStorageManager.
        if (storage := self._find_storage(dataset_type.name)) is None:
            raise MissingDatasetTypeError(f"Dataset type {dataset_type.name!r} has not been registered.")
        if not dataset_type.isCalibration():
            raise DatasetTypeError(
                f"Cannot certify datasets of type {dataset_type.name!r}, for which "
                "DatasetType.isCalibration() is False."
            )
        if collection.type is not CollectionType.CALIBRATION:
            raise CollectionTypeError(
                f"Cannot certify into collection '{collection.name}' "
                f"of type {collection.type.name}; must be CALIBRATION."
            )
        TimespanReprClass = self._db.getTimespanRepresentation()
        proto_row = {
            self._collections.getCollectionForeignKeyName(): collection.key,
            "dataset_type_id": storage.dataset_type_id,
        }
        rows = []
        data_ids: set[DataCoordinate] | None = (
            set() if not TimespanReprClass.hasExclusionConstraint() else None
        )
        summary = CollectionSummary()
        for dataset in summary.add_datasets_generator(datasets):
            row = dict(proto_row, dataset_id=dataset.id, **dataset.dataId.required)
            TimespanReprClass.update(timespan, result=row)
            rows.append(row)
            if data_ids is not None:
                data_ids.add(dataset.dataId)
        if not rows:
            # Just in case an empty dataset collection is provided we want to
            # avoid adding dataset type to summary tables.
            return
        # Update the summary tables for this collection in case this is the
        # first time this dataset type or these governor values will be
        # inserted there.
        self._summaries.update(collection, [storage.dataset_type_id], summary)
        # Update the association table itself.
        calibs_table = self._get_calibs_table(storage.dynamic_tables)
        if TimespanReprClass.hasExclusionConstraint():
            # Rely on database constraint to enforce invariants; we just
            # reraise the exception for consistency across DB engines.
            try:
                self._db.insert(calibs_table, *rows)
            except sqlalchemy.exc.IntegrityError as err:
                raise ConflictingDefinitionError(
                    f"Validity range conflict certifying datasets of type {dataset_type.name!r} "
                    f"into {collection.name!r} for range {timespan}."
                ) from err
        else:
            # Have to implement exclusion constraint ourselves.
            # Start by building a SELECT query for any rows that would overlap
            # this one.
            relation = self._build_calib_overlap_query(dataset_type, collection, data_ids, timespan, context)
            # Acquire a table lock to ensure there are no concurrent writes
            # could invalidate our checking before we finish the inserts.  We
            # use a SAVEPOINT in case there is an outer transaction that a
            # failure here should not roll back.
            with self._db.transaction(lock=[calibs_table], savepoint=True):
                # Enter SqlQueryContext in case we need to use a temporary
                # table to include the give data IDs in the query.  Note that
                # by doing this inside the transaction, we make sure it doesn't
                # attempt to close the session when its done, since it just
                # sees an already-open session that it knows it shouldn't
                # manage.
                with context:
                    # Run the check SELECT query.
                    conflicting = context.count(context.process(relation))
                    if conflicting > 0:
                        raise ConflictingDefinitionError(
                            f"{conflicting} validity range conflicts certifying datasets of type "
                            f"{dataset_type.name} into {collection.name} for range "
                            f"[{timespan.begin}, {timespan.end})."
                        )
                # Proceed with the insert.
                self._db.insert(calibs_table, *rows)

    def decertify(
        self,
        dataset_type: DatasetType,
        collection: CollectionRecord,
        timespan: Timespan,
        *,
        data_ids: Iterable[DataCoordinate] | None = None,
        context: SqlQueryContext,
    ) -> None:
        # Docstring inherited from DatasetRecordStorageManager.
        if (storage := self._find_storage(dataset_type.name)) is None:
            raise MissingDatasetTypeError(f"Dataset type {dataset_type.name!r} has not been registered.")
        if not dataset_type.isCalibration():
            raise DatasetTypeError(
                f"Cannot certify datasets of type {dataset_type.name!r}, for which "
                "DatasetType.isCalibration() is False."
            )
        if collection.type is not CollectionType.CALIBRATION:
            raise CollectionTypeError(
                f"Cannot decertify from collection '{collection.name}' "
                f"of type {collection.type.name}; must be CALIBRATION."
            )
        TimespanReprClass = self._db.getTimespanRepresentation()
        # Construct a SELECT query to find all rows that overlap our inputs.
        data_id_set: set[DataCoordinate] | None
        if data_ids is not None:
            data_id_set = set(data_ids)
        else:
            data_id_set = None
        relation = self._build_calib_overlap_query(dataset_type, collection, data_id_set, timespan, context)
        calib_pkey_tag = DatasetColumnTag(dataset_type.name, "calib_pkey")
        dataset_id_tag = DatasetColumnTag(dataset_type.name, "dataset_id")
        timespan_tag = DatasetColumnTag(dataset_type.name, "timespan")
        data_id_tags = [(name, DimensionKeyColumnTag(name)) for name in dataset_type.dimensions.required]
        # Set up collections to populate with the rows we'll want to modify.
        # The insert rows will have the same values for collection and
        # dataset type.
        proto_insert_row = {
            self._collections.getCollectionForeignKeyName(): collection.key,
            "dataset_type_id": storage.dataset_type_id,
        }
        rows_to_delete = []
        rows_to_insert = []
        # Acquire a table lock to ensure there are no concurrent writes
        # between the SELECT and the DELETE and INSERT queries based on it.
        calibs_table = self._get_calibs_table(storage.dynamic_tables)
        with self._db.transaction(lock=[calibs_table], savepoint=True):
            # Enter SqlQueryContext in case we need to use a temporary table to
            # include the give data IDs in the query (see similar block in
            # certify for details).
            with context:
                for row in context.fetch_iterable(relation):
                    rows_to_delete.append({"id": row[calib_pkey_tag]})
                    # Construct the insert row(s) by copying the prototype row,
                    # then adding the dimension column values, then adding
                    # what's left of the timespan from that row after we
                    # subtract the given timespan.
                    new_insert_row = proto_insert_row.copy()
                    new_insert_row["dataset_id"] = row[dataset_id_tag]
                    for name, tag in data_id_tags:
                        new_insert_row[name] = row[tag]
                    row_timespan = row[timespan_tag]
                    assert row_timespan is not None, "Field should have a NOT NULL constraint."
                    for diff_timespan in row_timespan.difference(timespan):
                        rows_to_insert.append(
                            TimespanReprClass.update(diff_timespan, result=new_insert_row.copy())
                        )
            # Run the DELETE and INSERT queries.
            self._db.delete(calibs_table, ["id"], *rows_to_delete)
            self._db.insert(calibs_table, *rows_to_insert)

    def _build_calib_overlap_query(
        self,
        dataset_type: DatasetType,
        collection: CollectionRecord,
        data_ids: set[DataCoordinate] | None,
        timespan: Timespan,
        context: SqlQueryContext,
    ) -> Relation:
        relation = self.make_relation(
            dataset_type, collection, columns={"timespan", "dataset_id", "calib_pkey"}, context=context
        ).with_rows_satisfying(
            context.make_timespan_overlap_predicate(
                DatasetColumnTag(dataset_type.name, "timespan"), timespan
            ),
        )
        if data_ids is not None:
            relation = relation.join(
                context.make_data_id_relation(data_ids, dataset_type.dimensions.required).transferred_to(
                    context.sql_engine
                ),
            )
        return relation

    def make_relation(
        self,
        dataset_type: DatasetType,
        *collections: CollectionRecord,
        columns: Set[str],
        context: SqlQueryContext,
    ) -> Relation:
        # Docstring inherited from DatasetRecordStorageManager.
        if (storage := self._find_storage(dataset_type.name)) is None:
            raise MissingDatasetTypeError(f"Dataset type {dataset_type.name!r} has not been registered.")
        collection_types = {collection.type for collection in collections}
        assert CollectionType.CHAINED not in collection_types, "CHAINED collections must be flattened."
        TimespanReprClass = self._db.getTimespanRepresentation()
        #
        # There are two kinds of table in play here:
        #
        #  - the static dataset table (with the dataset ID, dataset type ID,
        #    run ID/name, and ingest date);
        #
        #  - the dynamic tags/calibs table (with the dataset ID, dataset type
        #    type ID, collection ID/name, data ID, and possibly validity
        #    range).
        #
        # That means that we might want to return a query against either table
        # or a JOIN of both, depending on which quantities the caller wants.
        # But the data ID is always included, which means we'll always include
        # the tags/calibs table and join in the static dataset table only if we
        # need things from it that we can't get from the tags/calibs table.
        #
        # Note that it's important that we include a WHERE constraint on both
        # tables for any column (e.g. dataset_type_id) that is in both when
        # it's given explicitly; not doing can prevent the query planner from
        # using very important indexes.  At present, we don't include those
        # redundant columns in the JOIN ON expression, however, because the
        # FOREIGN KEY (and its index) are defined only on dataset_id.
        tag_relation: Relation | None = None
        calib_relation: Relation | None = None
        if collection_types != {CollectionType.CALIBRATION}:
            tags_table = self._get_tags_table(storage.dynamic_tables)
            # We'll need a subquery for the tags table if any of the given
            # collections are not a CALIBRATION collection.  This intentionally
            # also fires when the list of collections is empty as a way to
            # create a dummy subquery that we know will fail.
            # We give the table an alias because it might appear multiple times
            # in the same query, for different dataset types.
            tags_parts = sql.Payload[LogicalColumn](tags_table.alias(f"{dataset_type.name}_tags"))
            if "timespan" in columns:
                tags_parts.columns_available[DatasetColumnTag(dataset_type.name, "timespan")] = (
                    TimespanReprClass.fromLiteral(Timespan(None, None))
                )
            tag_relation = self._finish_single_relation(
                storage,
                tags_parts,
                columns,
                [
                    (record, rank)
                    for rank, record in enumerate(collections)
                    if record.type is not CollectionType.CALIBRATION
                ],
                context,
            )
            assert "calib_pkey" not in columns, "For internal use only, and only for pure-calib queries."
        if CollectionType.CALIBRATION in collection_types:
            # If at least one collection is a CALIBRATION collection, we'll
            # need a subquery for the calibs table, and could include the
            # timespan as a result or constraint.
            calibs_table = self._get_calibs_table(storage.dynamic_tables)
            calibs_parts = sql.Payload[LogicalColumn](calibs_table.alias(f"{dataset_type.name}_calibs"))
            if "timespan" in columns:
                calibs_parts.columns_available[DatasetColumnTag(dataset_type.name, "timespan")] = (
                    TimespanReprClass.from_columns(calibs_parts.from_clause.columns)
                )
            if "calib_pkey" in columns:
                # This is a private extension not included in the base class
                # interface, for internal use only in _buildCalibOverlapQuery,
                # which needs access to the autoincrement primary key for the
                # calib association table.
                calibs_parts.columns_available[DatasetColumnTag(dataset_type.name, "calib_pkey")] = (
                    calibs_parts.from_clause.columns.id
                )
            calib_relation = self._finish_single_relation(
                storage,
                calibs_parts,
                columns,
                [
                    (record, rank)
                    for rank, record in enumerate(collections)
                    if record.type is CollectionType.CALIBRATION
                ],
                context,
            )
        if tag_relation is not None:
            if calib_relation is not None:
                # daf_relation's chain operation does not automatically
                # deduplicate; it's more like SQL's UNION ALL.  To get UNION
                # in SQL here, we add an explicit deduplication.
                return tag_relation.chain(calib_relation).without_duplicates()
            else:
                return tag_relation
        elif calib_relation is not None:
            return calib_relation
        else:
            raise AssertionError("Branch should be unreachable.")

    def _finish_single_relation(
        self,
        storage: _DatasetRecordStorage,
        payload: sql.Payload[LogicalColumn],
        requested_columns: Set[str],
        collections: Sequence[tuple[CollectionRecord, int]],
        context: SqlQueryContext,
    ) -> Relation:
        """Handle adding columns and WHERE terms that are not specific to
        either the tags or calibs tables.

        Helper method for `make_relation`.

        Parameters
        ----------
        storage : `ByDimensionsDatasetRecordStorageUUID`
            Struct that holds the tables and ID for the dataset type.
        payload : `lsst.daf.relation.sql.Payload`
            SQL query parts under construction, to be modified in-place and
            used to construct the new relation.
        requested_columns : `~collections.abc.Set` [ `str` ]
            Columns the relation should include.
        collections : `~collections.abc.Sequence` [ `tuple` \
                [ `CollectionRecord`, `int` ] ]
            Collections to search for the dataset and their ranks.
        context : `SqlQueryContext`
            Context that manages engines and state for the query.

        Returns
        -------
        relation : `lsst.daf.relation.Relation`
            New dataset query relation.
        """
        payload.where.append(payload.from_clause.columns.dataset_type_id == storage.dataset_type_id)
        dataset_id_col = payload.from_clause.columns.dataset_id
        collection_col = payload.from_clause.columns[self._collections.getCollectionForeignKeyName()]
        # We always constrain and optionally retrieve the collection(s) via the
        # tags/calibs table.
        if len(collections) == 1:
            payload.where.append(collection_col == collections[0][0].key)
            if "collection" in requested_columns:
                payload.columns_available[DatasetColumnTag(storage.dataset_type.name, "collection")] = (
                    sqlalchemy.sql.literal(collections[0][0].key)
                )
        else:
            assert collections, "The no-collections case should be in calling code for better diagnostics."
            payload.where.append(collection_col.in_([collection.key for collection, _ in collections]))
            if "collection" in requested_columns:
                payload.columns_available[DatasetColumnTag(storage.dataset_type.name, "collection")] = (
                    collection_col
                )
        # Add rank if requested as a CASE-based calculation the collection
        # column.
        if "rank" in requested_columns:
            payload.columns_available[DatasetColumnTag(storage.dataset_type.name, "rank")] = (
                sqlalchemy.sql.case(
                    {record.key: rank for record, rank in collections},
                    value=collection_col,
                )
            )
        # Add more column definitions, starting with the data ID.
        for dimension_name in storage.dataset_type.dimensions.required:
            payload.columns_available[DimensionKeyColumnTag(dimension_name)] = payload.from_clause.columns[
                dimension_name
            ]
        # We can always get the dataset_id from the tags/calibs table.
        if "dataset_id" in requested_columns:
            payload.columns_available[DatasetColumnTag(storage.dataset_type.name, "dataset_id")] = (
                dataset_id_col
            )
        # It's possible we now have everything we need, from just the
        # tags/calibs table.  The things we might need to get from the static
        # dataset table are the run key and the ingest date.
        need_static_table = False
        if "run" in requested_columns:
            if len(collections) == 1 and collections[0][0].type is CollectionType.RUN:
                # If we are searching exactly one RUN collection, we
                # know that if we find the dataset in that collection,
                # then that's the datasets's run; we don't need to
                # query for it.
                payload.columns_available[DatasetColumnTag(storage.dataset_type.name, "run")] = (
                    sqlalchemy.sql.literal(collections[0][0].key)
                )
            else:
                payload.columns_available[DatasetColumnTag(storage.dataset_type.name, "run")] = (
                    self._static.dataset.columns[self._run_key_column]
                )
                need_static_table = True
        # Ingest date can only come from the static table.
        if "ingest_date" in requested_columns:
            need_static_table = True
            payload.columns_available[DatasetColumnTag(storage.dataset_type.name, "ingest_date")] = (
                self._static.dataset.columns.ingest_date
            )
        # If we need the static table, join it in via dataset_id and
        # dataset_type_id
        if need_static_table:
            payload.from_clause = payload.from_clause.join(
                self._static.dataset, onclause=(dataset_id_col == self._static.dataset.columns.id)
            )
            # Also constrain dataset_type_id in static table in case that helps
            # generate a better plan.
            # We could also include this in the JOIN ON clause, but my guess is
            # that that's a good idea IFF it's in the foreign key, and right
            # now it isn't.
            payload.where.append(self._static.dataset.columns.dataset_type_id == storage.dataset_type_id)
        leaf = context.sql_engine.make_leaf(
            payload.columns_available.keys(),
            payload=payload,
            name=storage.dataset_type.name,
            parameters={record.name: rank for record, rank in collections},
        )
        return leaf

    def make_joins_builder(
        self,
        dataset_type: DatasetType,
        collections: Sequence[CollectionRecord],
        fields: Set[str],
        is_union: bool = False,
    ) -> SqlJoinsBuilder:
        if (storage := self._find_storage(dataset_type.name)) is None:
            raise MissingDatasetTypeError(f"Dataset type {dataset_type.name!r} has not been registered.")
        # This method largely mimics `make_relation`, but it uses the new query
        # system primitives instead of the old one.  In terms of the SQL
        # queries it builds, there are two more main differences:
        #
        # - Collection and run columns are now string names rather than IDs.
        #   This insulates the query result-processing code from collection
        #   caching and the collection manager subclass details.
        #
        # - The subquery always has unique rows, which is achieved by using
        #   SELECT DISTINCT when necessary.
        #
        collection_types = {collection.type for collection in collections}
        assert CollectionType.CHAINED not in collection_types, "CHAINED collections must be flattened."
        #
        # There are two kinds of table in play here:
        #
        #  - the static dataset table (with the dataset ID, dataset type ID,
        #    run ID/name, and ingest date);
        #
        #  - the dynamic tags/calibs table (with the dataset ID, dataset type
        #    type ID, collection ID/name, data ID, and possibly validity
        #    range).
        #
        # That means that we might want to return a query against either table
        # or a JOIN of both, depending on which quantities the caller wants.
        # But the data ID is always included, which means we'll always include
        # the tags/calibs table and join in the static dataset table only if we
        # need things from it that we can't get from the tags/calibs table.
        #
        # Note that it's important that we include a WHERE constraint on both
        # tables for any column (e.g. dataset_type_id) that is in both when
        # it's given explicitly; not doing can prevent the query planner from
        # using very important indexes.  At present, we don't include those
        # redundant columns in the JOIN ON expression, however, because the
        # FOREIGN KEY (and its index) are defined only on dataset_id.
        columns = qt.ColumnSet(dataset_type.dimensions)
        columns.drop_implied_dimension_keys()
        fields_key: str | qt.AnyDatasetType = qt.ANY_DATASET if is_union else dataset_type.name
        columns.dataset_fields[fields_key].update(fields)
        tags_builder: SqlSelectBuilder | None = None
        if collection_types != {CollectionType.CALIBRATION}:
            # We'll need a subquery for the tags table if any of the given
            # collections are not a CALIBRATION collection.  This intentionally
            # also fires when the list of collections is empty as a way to
            # create a dummy subquery that we know will fail.
            # We give the table an alias because it might appear multiple times
            # in the same query, for different dataset types.
            tags_table = self._get_tags_table(storage.dynamic_tables).alias(
                f"{dataset_type.name}_tags{'_union' if is_union else ''}"
            )
            tags_builder = self._finish_query_builder(
                storage,
                SqlJoinsBuilder(db=self._db, from_clause=tags_table).to_select_builder(columns),
                [record for record in collections if record.type is not CollectionType.CALIBRATION],
                fields,
                fields_key,
            )
            if "timespan" in fields:
                tags_builder.joins.timespans[fields_key] = self._db.getTimespanRepresentation().fromLiteral(
                    None
                )
        calibs_builder: SqlSelectBuilder | None = None
        if CollectionType.CALIBRATION in collection_types:
            # If at least one collection is a CALIBRATION collection, we'll
            # need a subquery for the calibs table, and could include the
            # timespan as a result or constraint.
            calibs_table = self._get_calibs_table(storage.dynamic_tables).alias(
                f"{dataset_type.name}_calibs{'_union' if is_union else ''}"
            )
            calibs_builder = self._finish_query_builder(
                storage,
                SqlJoinsBuilder(db=self._db, from_clause=calibs_table).to_select_builder(columns),
                [record for record in collections if record.type is CollectionType.CALIBRATION],
                fields,
                fields_key,
            )
            if "timespan" in fields:
                calibs_builder.joins.timespans[fields_key] = (
                    self._db.getTimespanRepresentation().from_columns(calibs_table.columns)
                )

            # In calibration collections, we need timespan as well as data ID
            # to ensure unique rows.
            calibs_builder.distinct = calibs_builder.distinct and "timespan" not in fields
        if tags_builder is not None:
            if calibs_builder is not None:
                # Need a UNION subquery.
                return tags_builder.union_subquery([calibs_builder])
            else:
                return tags_builder.into_joins_builder(postprocessing=None)
        elif calibs_builder is not None:
            return calibs_builder.into_joins_builder(postprocessing=None)
        else:
            raise AssertionError("Branch should be unreachable.")

    def _finish_query_builder(
        self,
        storage: _DatasetRecordStorage,
        sql_projection: SqlSelectBuilder,
        collections: Sequence[CollectionRecord],
        fields: Set[str],
        fields_key: str | qt.AnyDatasetType,
    ) -> SqlSelectBuilder:
        # This method plays the same role as _finish_single_relation in the new
        # query system. It is called exactly one or two times by
        # make_sql_builder, just as _finish_single_relation is called exactly
        # one or two times by make_relation.  See make_sql_builder comments for
        # what's different.
        assert sql_projection.joins.from_clause is not None
        run_collections_only = all(record.type is CollectionType.RUN for record in collections)
        sql_projection.joins.where(
            sql_projection.joins.from_clause.c.dataset_type_id == storage.dataset_type_id
        )
        dataset_id_col = sql_projection.joins.from_clause.c.dataset_id
        collection_col = sql_projection.joins.from_clause.c[self._collections.getCollectionForeignKeyName()]
        fields_provided = sql_projection.joins.fields[fields_key]
        # We always constrain and optionally retrieve the collection(s) via the
        # tags/calibs table.
        if "collection_key" in fields:
            sql_projection.joins.fields[fields_key]["collection_key"] = collection_col
        if len(collections) == 1:
            only_collection_record = collections[0]
            sql_projection.joins.where(collection_col == only_collection_record.key)
            if "collection" in fields:
                fields_provided["collection"] = sqlalchemy.literal(only_collection_record.name).cast(
                    # This cast is necessary to ensure that Postgres knows the
                    # type of this column if it is used in an aggregate
                    # function.
                    sqlalchemy.String
                )

        elif not collections:
            sql_projection.joins.where(sqlalchemy.literal(False))
            if "collection" in fields:
                fields_provided["collection"] = sqlalchemy.literal("NO COLLECTIONS")
        else:
            sql_projection.joins.where(collection_col.in_([collection.key for collection in collections]))
            if "collection" in fields:
                # Avoid a join to the collection table to get the name by using
                # a CASE statement.  The SQL will be a bit more verbose but
                # more efficient.
                fields_provided["collection"] = _create_case_expression_for_collections(
                    collections, collection_col
                )
        # Add more column definitions, starting with the data ID.
        sql_projection.joins.extract_dimensions(storage.dataset_type.dimensions.required)
        # We can always get the dataset_id from the tags/calibs table, even if
        # could also get it from the 'static' dataset table.
        if "dataset_id" in fields:
            fields_provided["dataset_id"] = dataset_id_col

        # It's possible we now have everything we need, from just the
        # tags/calibs table.  The things we might need to get from the static
        # dataset table are the run key and the ingest date.
        need_static_table = False
        need_collection_table = False
        # Ingest date can only come from the static table.
        if "ingest_date" in fields:
            fields_provided["ingest_date"] = self._static.dataset.c.ingest_date
            need_static_table = True
        if "run" in fields:
            if len(collections) == 1 and run_collections_only:
                # If we are searching exactly one RUN collection, we
                # know that if we find the dataset in that collection,
                # then that's the datasets's run; we don't need to
                # query for it.
                #
                fields_provided["run"] = sqlalchemy.literal(only_collection_record.name).cast(
                    # This cast is necessary to ensure that Postgres knows the
                    # type of this column if it is used in an aggregate
                    # function.
                    sqlalchemy.String
                )
            elif run_collections_only:
                # Once again we can avoid joining to the collection table by
                # adding a CASE statement.
                fields_provided["run"] = _create_case_expression_for_collections(
                    collections, self._static.dataset.c[self._run_key_column]
                )
                need_static_table = True
            else:
                # Here we can't avoid a join to the collection table, because
                # we might find a dataset via something other than its RUN
                # collection.
                #
                # We have to defer adding the join until after we have joined
                # in the static dataset table, because the ON clause involves
                # the run collection from the static dataset table.  Postgres
                # cares about the join ordering (though SQLite does not.)
                need_collection_table = True
                need_static_table = True
        if need_static_table:
            # If we need the static table, join it in via dataset_id.  We don't
            # use SqlJoinsBuilder.join because we're joining on dataset ID, not
            # dimensions.
            sql_projection.joins.from_clause = sql_projection.joins.from_clause.join(
                self._static.dataset, onclause=(dataset_id_col == self._static.dataset.c.id)
            )
            # Also constrain dataset_type_id in static table in case that helps
            # generate a better plan. We could also include this in the JOIN ON
            # clause, but my guess is that that's a good idea IFF it's in the
            # foreign key, and right now it isn't.
            sql_projection.joins.where(self._static.dataset.c.dataset_type_id == storage.dataset_type_id)
        if need_collection_table:
            # Join the collection table to look up the RUN collection name
            # associated with the dataset.
            (
                fields_provided["run"],
                sql_projection.joins.from_clause,
            ) = self._collections.lookup_name_sql(
                self._static.dataset.c[self._run_key_column],
                sql_projection.joins.from_clause,
            )

        sql_projection.distinct = (
            # If there are multiple collections, this subquery might have
            # non-unique rows.
            len(collections) > 1 and not fields
        )
        return sql_projection

    def refresh_collection_summaries(self, dataset_type: DatasetType) -> None:
        # Docstring inherited.
        if (storage := self._find_storage(dataset_type.name)) is None:
            raise MissingDatasetTypeError(f"Dataset type {dataset_type.name!r} has not been registered.")
        with self._db.transaction():
            # The main issue here is consistency in the presence of concurrent
            # updates (using default READ COMMITTED isolation). Regular clients
            # only add to summary tables, and we want to avoid deleting what
            # other concurrent transactions may add while we are in this
            # transaction. This ordering of operations should guarantee it:
            #  - read collections for this dataset type from summary tables,
            #  - read collections for this dataset type from dataset tables
            #    (both tags and calibs),
            #  - whatever is in the first set but not in the second can be
            #    dropped from summary tables.
            summary_collection_ids = set(self._summaries.get_collection_ids(storage.dataset_type_id))

            # Query datasets tables for associated collections.
            column_name = self._collections.getCollectionForeignKeyName()
            tags_table = self._get_tags_table(storage.dynamic_tables)
            query: sqlalchemy.sql.expression.SelectBase = (
                sqlalchemy.select(tags_table.columns[column_name])
                .where(tags_table.columns.dataset_type_id == storage.dataset_type_id)
                .distinct()
            )
            if dataset_type.isCalibration():
                calibs_table = self._get_calibs_table(storage.dynamic_tables)
                query2 = (
                    sqlalchemy.select(calibs_table.columns[column_name])
                    .where(calibs_table.columns.dataset_type_id == storage.dataset_type_id)
                    .distinct()
                )
                query = sqlalchemy.sql.expression.union(query, query2)

            with self._db.query(query) as result:
                collection_ids = set(result.scalars())

            collections_to_delete = summary_collection_ids - collection_ids
            self._summaries.delete_collections(storage.dataset_type_id, collections_to_delete)

    def _get_tags_table(self, table: DynamicTables) -> sqlalchemy.Table:
        return table.tags(self._db, type(self._collections), self._cache.tables)

    def _get_calibs_table(self, table: DynamicTables) -> sqlalchemy.Table:
        return table.calibs(self._db, type(self._collections), self._cache.tables)


def _create_case_expression_for_collections(
    collections: Iterable[CollectionRecord], id_column: sqlalchemy.ColumnElement
) -> sqlalchemy.ColumnElement:
    """Return a SQLAlchemy Case expression that converts collection IDs to
    collection names for the given set of collections.

    Parameters
    ----------
    collections : `~collections.abc.Iterable` [ `CollectionRecord` ]
        List of collections to include in conversion table.  This should be an
        exhaustive list of collections that could appear in `id_column`.
    id_column : `sqlalchemy.ColumnElement`
        The column containing the collection ID that we want to convert to a
        collection name.
    """
    mapping = {record.key: record.name for record in collections}
    if not mapping:
        # SQLAlchemy does not correctly handle an empty mapping in case() -- it
        # crashes when trying to compile the expression with an
        # "AttributeError('NoneType' object has no attribute 'dialect_impl')"
        # when trying to access the 'type' property of the Case object.  If you
        # explicitly specify a type via type_coerce it instead generates
        # invalid SQL syntax.
        #
        # We can end up with empty mappings here in certain "doomed query" edge
        # cases, e.g.  we start with a list of valid collections but they are
        # all filtered out by higher-level code on the basis of collection
        # summaries.
        return sqlalchemy.cast(sqlalchemy.null(), sqlalchemy.String)

    return sqlalchemy.case(mapping, value=id_column)
