from __future__ import annotations

__all__ = ("ByDimensionsDatasetRecordStorage",)

from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Set,
    TYPE_CHECKING,
)

import sqlalchemy

from lsst.daf.butler import (
    CollectionType,
    DataCoordinate,
    DataCoordinateSet,
    DatasetRef,
    DatasetType,
    SimpleQuery,
    Timespan,
)
from lsst.daf.butler.registry import ConflictingDefinitionError
from lsst.daf.butler.registry.interfaces import DatasetRecordStorage


if TYPE_CHECKING:
    from ...interfaces import CollectionManager, CollectionRecord, Database, RunRecord
    from .tables import CollectionSummaryTables, StaticDatasetTablesTuple


class ByDimensionsDatasetRecordStorage(DatasetRecordStorage):
    """Dataset record storage implementation paired with
    `ByDimensionsDatasetRecordStorageManager`; see that class for more
    information.

    Instances of this class should never be constructed directly; use
    `DatasetRecordStorageManager.register` instead.
    """
    def __init__(self, *, datasetType: DatasetType,
                 db: Database,
                 dataset_type_id: int,
                 collections: CollectionManager,
                 static: StaticDatasetTablesTuple,
                 summaries: CollectionSummaryTables,
                 tags: sqlalchemy.schema.Table,
                 calibs: Optional[sqlalchemy.schema.Table]):
        super().__init__(datasetType=datasetType)
        self._dataset_type_id = dataset_type_id
        self._db = db
        self._collections = collections
        self._static = static
        self._summaries = summaries
        self._tags = tags
        self._calibs = calibs
        self._runKeyColumn = collections.getRunForeignKeyName()

    def _ensureSummaries(self, collection: CollectionRecord, governorValues: Dict[str, Set[str]]) -> None:
        """Update the summary table to associate the given collection with
        ``self.datasetType`` and the given governor dimension values.

        Parameters
        ----------
        collection : `CollectionRecord`
            Collection whose summary should be updated.
        governorValues : `dict` [ `str`, `set` [ `str ` ] ]
            Mapping from `GovernorDimension` names to sets of values they may
            have in the data IDs of the dataests in this collection.
        """
        self._db.ensure(
            self._summaries.datasetType,
            {
                "dataset_type_id": self._dataset_type_id,
                self._collections.getCollectionForeignKeyName(): collection.key,
            }
        )
        for governorName, values in governorValues.items():
            self._db.ensure(
                self._summaries.dimensions[governorName],
                *[{
                    self._collections.getCollectionForeignKeyName(): collection.key,
                    governorName: v
                } for v in values],
            )

    def insert(self, run: RunRecord, dataIds: Iterable[DataCoordinate]) -> Iterator[DatasetRef]:
        # Docstring inherited from DatasetRecordStorage.
        staticRow = {
            "dataset_type_id": self._dataset_type_id,
            self._runKeyColumn: run.key,
        }
        # Iterate over data IDs, transforming a possibly-single-pass iterable
        # into a list, and remembering any governor dimension values we see.
        governorValues: Dict[str, Set[str]] = {
            name: set() for name in self.datasetType.dimensions.governors.names
        }
        dataIdList = []
        for dataId in dataIds:
            dataIdList.append(dataId)
            for governorName, values in governorValues.items():
                values.add(dataId[governorName])  # type: ignore
        with self._db.transaction():
            # Insert into the static dataset table, generating autoincrement
            # dataset_id values.
            datasetIds = self._db.insert(self._static.dataset, *([staticRow]*len(dataIdList)),
                                         returnIds=True)
            assert datasetIds is not None
            # Update the summary tables for this collection in case this is the
            # first time this dataset type or these governor values will be
            # inserted there.
            self._ensureSummaries(run, governorValues)
            # Combine the generated dataset_id values and data ID fields to
            # form rows to be inserted into the tags table.
            protoTagsRow = {
                "dataset_type_id": self._dataset_type_id,
                self._collections.getCollectionForeignKeyName(): run.key,
            }
            tagsRows = [
                dict(protoTagsRow, dataset_id=dataset_id, **dataId.byName())
                for dataId, dataset_id in zip(dataIdList, datasetIds)
            ]
            # Insert those rows into the tags table.  This is where we'll
            # get any unique constraint violations.
            self._db.insert(self._tags, *tagsRows)
        for dataId, datasetId in zip(dataIdList, datasetIds):
            yield DatasetRef(
                datasetType=self.datasetType,
                dataId=dataId,
                id=datasetId,
                run=run.name,
            )

    def find(self, collection: CollectionRecord, dataId: DataCoordinate,
             timespan: Optional[Timespan] = None) -> Optional[DatasetRef]:
        # Docstring inherited from DatasetRecordStorage.
        assert dataId.graph == self.datasetType.dimensions
        if collection.type is CollectionType.CALIBRATION and timespan is None:
            raise TypeError(f"Cannot search for dataset in CALIBRATION collection {collection.name} "
                            f"without an input timespan.")
        sql = self.select(collection=collection, dataId=dataId, id=SimpleQuery.Select,
                          run=SimpleQuery.Select, timespan=timespan).combine()
        results = self._db.query(sql)
        row = results.fetchone()
        if row is None:
            return None
        if collection.type is CollectionType.CALIBRATION:
            # For temporal calibration lookups (only!) our invariants do not
            # guarantee that the number of result rows is <= 1.
            # They would if `select` constrained the given timespan to be
            # _contained_ by the validity range in the self._calibs table,
            # instead of simply _overlapping_ it, because we do guarantee that
            # the validity ranges are disjoint for a particular dataset type,
            # collection, and data ID.  But using an overlap test and a check
            # for multiple result rows here allows us to provide a more useful
            # diagnostic, as well as allowing `select` to support more general
            # queries where multiple results are not an error.
            if results.fetchone() is not None:
                raise RuntimeError(
                    f"Multiple matches found for calibration lookup in {collection.name} for "
                    f"{self.datasetType.name} with {dataId} overlapping {timespan}. "
                )
        return DatasetRef(
            datasetType=self.datasetType,
            dataId=dataId,
            id=row["id"],
            run=self._collections[row[self._runKeyColumn]].name
        )

    def delete(self, datasets: Iterable[DatasetRef]) -> None:
        # Docstring inherited from DatasetRecordStorage.
        # Only delete from common dataset table; ON DELETE foreign key clauses
        # will handle the rest.
        self._db.delete(
            self._static.dataset,
            ["id"],
            *[{"id": dataset.getCheckedId()} for dataset in datasets],
        )

    def associate(self, collection: CollectionRecord, datasets: Iterable[DatasetRef]) -> None:
        # Docstring inherited from DatasetRecordStorage.
        if collection.type is not CollectionType.TAGGED:
            raise TypeError(f"Cannot associate into collection '{collection.name}' "
                            f"of type {collection.type.name}; must be TAGGED.")
        protoRow = {
            self._collections.getCollectionForeignKeyName(): collection.key,
            "dataset_type_id": self._dataset_type_id,
        }
        rows = []
        governorValues: Dict[str, Set[str]] = {
            name: set() for name in self.datasetType.dimensions.governors.names
        }
        for dataset in datasets:
            row = dict(protoRow, dataset_id=dataset.getCheckedId())
            for dimension, value in dataset.dataId.items():
                row[dimension.name] = value
            for governorName, values in governorValues.items():
                values.add(dataset.dataId[governorName])  # type: ignore
            rows.append(row)
        # Update the summary tables for this collection in case this is the
        # first time this dataset type or these governor values will be
        # inserted there.
        self._ensureSummaries(collection, governorValues)
        # Update the tag table itself.
        self._db.replace(self._tags, *rows)

    def disassociate(self, collection: CollectionRecord, datasets: Iterable[DatasetRef]) -> None:
        # Docstring inherited from DatasetRecordStorage.
        if collection.type is not CollectionType.TAGGED:
            raise TypeError(f"Cannot disassociate from collection '{collection.name}' "
                            f"of type {collection.type.name}; must be TAGGED.")
        rows = [
            {
                "dataset_id": dataset.getCheckedId(),
                self._collections.getCollectionForeignKeyName(): collection.key
            }
            for dataset in datasets
        ]
        self._db.delete(self._tags, ["dataset_id", self._collections.getCollectionForeignKeyName()],
                        *rows)

    def _buildCalibOverlapQuery(self, collection: CollectionRecord,
                                dataIds: Optional[DataCoordinateSet],
                                timespan: Timespan) -> SimpleQuery:
        assert self._calibs is not None
        # Start by building a SELECT query for any rows that would overlap
        # this one.
        query = SimpleQuery()
        query.join(self._calibs)
        # Add a WHERE clause matching the dataset type and collection.
        query.where.append(self._calibs.columns.dataset_type_id == self._dataset_type_id)
        query.where.append(
            self._calibs.columns[self._collections.getCollectionForeignKeyName()] == collection.key
        )
        # Add a WHERE clause matching any of the given data IDs.
        if dataIds is not None:
            dataIds.constrain(
                query,
                lambda name: self._calibs.columns[name],  # type: ignore
            )
        # Add WHERE clause for timespan overlaps.
        tsRepr = self._db.getTimespanRepresentation()
        query.where.append(tsRepr.fromSelectable(self._calibs).overlaps(tsRepr.fromLiteral(timespan)))
        return query

    def certify(self, collection: CollectionRecord, datasets: Iterable[DatasetRef],
                timespan: Timespan) -> None:
        # Docstring inherited from DatasetRecordStorage.
        if self._calibs is None:
            raise TypeError(f"Cannot certify datasets of type {self.datasetType.name}, for which "
                            f"DatasetType.isCalibration() is False.")
        if collection.type is not CollectionType.CALIBRATION:
            raise TypeError(f"Cannot certify into collection '{collection.name}' "
                            f"of type {collection.type.name}; must be CALIBRATION.")
        tsRepr = self._db.getTimespanRepresentation()
        protoRow = {
            self._collections.getCollectionForeignKeyName(): collection.key,
            "dataset_type_id": self._dataset_type_id,
        }
        rows = []
        governorValues: Dict[str, Set[str]] = {
            name: set() for name in self.datasetType.dimensions.governors.names
        }
        dataIds: Optional[Set[DataCoordinate]] = set() if not tsRepr.hasExclusionConstraint() else None
        for dataset in datasets:
            row = dict(protoRow, dataset_id=dataset.getCheckedId())
            for dimension, value in dataset.dataId.items():
                row[dimension.name] = value
            tsRepr.update(timespan, result=row)
            for governorName, values in governorValues.items():
                values.add(dataset.dataId[governorName])  # type: ignore
            rows.append(row)
            if dataIds is not None:
                dataIds.add(dataset.dataId)
        # Update the summary tables for this collection in case this is the
        # first time this dataset type or these governor values will be
        # inserted there.
        self._ensureSummaries(collection, governorValues)
        # Update the association table itself.
        if tsRepr.hasExclusionConstraint():
            # Rely on database constraint to enforce invariants; we just
            # reraise the exception for consistency across DB engines.
            try:
                self._db.insert(self._calibs, *rows)
            except sqlalchemy.exc.IntegrityError as err:
                raise ConflictingDefinitionError(
                    f"Validity range conflict certifying datasets of type {self.datasetType.name} "
                    f"into {collection.name} for range [{timespan.begin}, {timespan.end})."
                ) from err
        else:
            # Have to implement exclusion constraint ourselves.
            # Start by building a SELECT query for any rows that would overlap
            # this one.
            query = self._buildCalibOverlapQuery(
                collection,
                DataCoordinateSet(dataIds, graph=self.datasetType.dimensions),  # type: ignore
                timespan
            )
            query.columns.append(sqlalchemy.sql.func.count())
            sql = query.combine()
            # Acquire a table lock to ensure there are no concurrent writes
            # could invalidate our checking before we finish the inserts.  We
            # use a SAVEPOINT in case there is an outer transaction that a
            # failure here should not roll back.
            with self._db.transaction(lock=[self._calibs], savepoint=True):
                # Run the check SELECT query.
                conflicting = self._db.query(sql).scalar()
                if conflicting > 0:
                    raise ConflictingDefinitionError(
                        f"{conflicting} validity range conflicts certifying datasets of type "
                        f"{self.datasetType.name} into {collection.name} for range "
                        f"[{timespan.begin}, {timespan.end})."
                    )
                # Proceed with the insert.
                self._db.insert(self._calibs, *rows)

    def decertify(self, collection: CollectionRecord, timespan: Timespan, *,
                  dataIds: Optional[Iterable[DataCoordinate]] = None) -> None:
        # Docstring inherited from DatasetRecordStorage.
        if self._calibs is None:
            raise TypeError(f"Cannot decertify datasets of type {self.datasetType.name}, for which "
                            f"DatasetType.isCalibration() is False.")
        if collection.type is not CollectionType.CALIBRATION:
            raise TypeError(f"Cannot decertify from collection '{collection.name}' "
                            f"of type {collection.type.name}; must be CALIBRATION.")
        tsRepr = self._db.getTimespanRepresentation()
        # Construct a SELECT query to find all rows that overlap our inputs.
        dataIdSet: Optional[DataCoordinateSet]
        if dataIds is not None:
            dataIdSet = DataCoordinateSet(set(dataIds), graph=self.datasetType.dimensions)
        else:
            dataIdSet = None
        query = self._buildCalibOverlapQuery(collection, dataIdSet, timespan)
        query.columns.extend(self._calibs.columns)
        sql = query.combine()
        # Set up collections to populate with the rows we'll want to modify.
        # The insert rows will have the same values for collection and
        # dataset type.
        protoInsertRow = {
            self._collections.getCollectionForeignKeyName(): collection.key,
            "dataset_type_id": self._dataset_type_id,
        }
        rowsToDelete = []
        rowsToInsert = []
        # Acquire a table lock to ensure there are no concurrent writes
        # between the SELECT and the DELETE and INSERT queries based on it.
        with self._db.transaction(lock=[self._calibs], savepoint=True):
            for row in self._db.query(sql):
                rowsToDelete.append({"id": row["id"]})
                # Construct the insert row(s) by copying the prototype row,
                # then adding the dimension column values, then adding what's
                # left of the timespan from that row after we subtract the
                # given timespan.
                newInsertRow = protoInsertRow.copy()
                newInsertRow["dataset_id"] = row["dataset_id"]
                for name in self.datasetType.dimensions.required.names:
                    newInsertRow[name] = row[name]
                rowTimespan = tsRepr.extract(row)
                assert rowTimespan is not None, "Field should have a NOT NULL constraint."
                for diffTimespan in rowTimespan.difference(timespan):
                    rowsToInsert.append(tsRepr.update(diffTimespan, result=newInsertRow.copy()))
            # Run the DELETE and INSERT queries.
            self._db.delete(self._calibs, ["id"], *rowsToDelete)
            self._db.insert(self._calibs, *rowsToInsert)

    def select(self, collection: CollectionRecord,
               dataId: SimpleQuery.Select.Or[DataCoordinate] = SimpleQuery.Select,
               id: SimpleQuery.Select.Or[Optional[int]] = SimpleQuery.Select,
               run: SimpleQuery.Select.Or[None] = SimpleQuery.Select,
               timespan: SimpleQuery.Select.Or[Optional[Timespan]] = SimpleQuery.Select,
               ingestDate: SimpleQuery.Select.Or[Optional[Timespan]] = None,
               ) -> SimpleQuery:
        # Docstring inherited from DatasetRecordStorage.
        assert collection.type is not CollectionType.CHAINED
        query = SimpleQuery()
        # We always include the _static.dataset table, and we can always get
        # the id and run fields from that; passing them as kwargs here tells
        # SimpleQuery to handle them whether they're constraints or results.
        # We always constraint the dataset_type_id here as well.
        static_kwargs = {self._runKeyColumn: run}
        if ingestDate is not None:
            static_kwargs["ingest_date"] = SimpleQuery.Select
        query.join(
            self._static.dataset,
            id=id,
            dataset_type_id=self._dataset_type_id,
            **static_kwargs
        )
        # If and only if the collection is a RUN, we constrain it in the static
        # table (and also the tags or calibs table below)
        if collection.type is CollectionType.RUN:
            query.where.append(self._static.dataset.columns[self._runKeyColumn]
                               == collection.key)
        # We get or constrain the data ID from the tags/calibs table, but
        # that's multiple columns, not one, so we need to transform the one
        # Select.Or argument into a dictionary of them.
        kwargs: Dict[str, Any]
        if dataId is SimpleQuery.Select:
            kwargs = {dim.name: SimpleQuery.Select for dim in self.datasetType.dimensions.required}
        else:
            kwargs = dict(dataId.byName())
        # We always constrain (never retrieve) the collection from the tags
        # table.
        kwargs[self._collections.getCollectionForeignKeyName()] = collection.key
        # constrain ingest time
        if isinstance(ingestDate, Timespan):
            # Tmespan is astropy Time (usually in TAI) and ingest_date is
            # TIMESTAMP, convert values to Python datetime for sqlalchemy.
            if ingestDate.isEmpty():
                raise RuntimeError("Empty timespan constraint provided for ingest_date.")
            if ingestDate.begin is not None:
                begin = ingestDate.begin.utc.datetime  # type: ignore
                query.where.append(self._static.dataset.ingest_date >= begin)
            if ingestDate.end is not None:
                end = ingestDate.end.utc.datetime  # type: ignore
                query.where.append(self._static.dataset.ingest_date < end)
        # And now we finally join in the tags or calibs table.
        if collection.type is CollectionType.CALIBRATION:
            assert self._calibs is not None, \
                "DatasetTypes with isCalibration() == False can never be found in a CALIBRATION collection."
            tsRepr = self._db.getTimespanRepresentation()
            # Add the timespan column(s) to the result columns, or constrain
            # the timespan via an overlap condition.
            if timespan is SimpleQuery.Select:
                kwargs.update({k: SimpleQuery.Select for k in tsRepr.getFieldNames()})
            elif timespan is not None:
                query.where.append(
                    tsRepr.fromSelectable(self._calibs).overlaps(tsRepr.fromLiteral(timespan))
                )
            query.join(
                self._calibs,
                onclause=(self._static.dataset.columns.id == self._calibs.columns.dataset_id),
                **kwargs
            )
        else:
            query.join(
                self._tags,
                onclause=(self._static.dataset.columns.id == self._tags.columns.dataset_id),
                **kwargs
            )
        return query

    def getDataId(self, id: int) -> DataCoordinate:
        # Docstring inherited from DatasetRecordStorage.
        # This query could return multiple rows (one for each tagged collection
        # the dataset is in, plus one for its run collection), and we don't
        # care which of those we get.
        sql = self._tags.select().where(
            sqlalchemy.sql.and_(
                self._tags.columns.dataset_id == id,
                self._tags.columns.dataset_type_id == self._dataset_type_id
            )
        ).limit(1)
        row = self._db.query(sql).fetchone()
        assert row is not None, "Should be guaranteed by caller and foreign key constraints."
        return DataCoordinate.standardize(
            {dimension.name: row[dimension.name] for dimension in self.datasetType.dimensions.required},
            graph=self.datasetType.dimensions
        )
