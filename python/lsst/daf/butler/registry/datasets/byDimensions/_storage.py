# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


from __future__ import annotations

__all__ = ("ByDimensionsDatasetRecordStorage",)

import uuid
from collections.abc import Iterable, Iterator, Sequence
from typing import TYPE_CHECKING, Any

import sqlalchemy
from deprecated.sphinx import deprecated

from ....core import (
    DataCoordinate,
    DataCoordinateSet,
    DatasetId,
    DatasetRef,
    DatasetType,
    SimpleQuery,
    Timespan,
    ddl,
)
from ..._collection_summary import CollectionSummary
from ..._collectionType import CollectionType
from ..._exceptions import CollectionTypeError, ConflictingDefinitionError, UnsupportedIdGeneratorError
from ...interfaces import DatasetIdFactory, DatasetIdGenEnum, DatasetRecordStorage
from .tables import makeTagTableSpec

if TYPE_CHECKING:
    from ...interfaces import CollectionManager, CollectionRecord, Database, RunRecord
    from .summaries import CollectionSummaryManager
    from .tables import StaticDatasetTablesTuple


class ByDimensionsDatasetRecordStorage(DatasetRecordStorage):
    """Dataset record storage implementation paired with
    `ByDimensionsDatasetRecordStorageManager`; see that class for more
    information.

    Instances of this class should never be constructed directly; use
    `DatasetRecordStorageManager.register` instead.
    """

    def __init__(
        self,
        *,
        datasetType: DatasetType,
        db: Database,
        dataset_type_id: int,
        collections: CollectionManager,
        static: StaticDatasetTablesTuple,
        summaries: CollectionSummaryManager,
        tags: sqlalchemy.schema.Table,
        calibs: sqlalchemy.schema.Table | None,
    ):
        super().__init__(datasetType=datasetType)
        self._dataset_type_id = dataset_type_id
        self._db = db
        self._collections = collections
        self._static = static
        self._summaries = summaries
        self._tags = tags
        self._calibs = calibs
        self._runKeyColumn = collections.getRunForeignKeyName()

    def find(
        self, collection: CollectionRecord, dataId: DataCoordinate, timespan: Timespan | None = None
    ) -> DatasetRef | None:
        # Docstring inherited from DatasetRecordStorage.
        assert dataId.graph == self.datasetType.dimensions
        if collection.type is CollectionType.CALIBRATION and timespan is None:
            raise TypeError(
                f"Cannot search for dataset in CALIBRATION collection {collection.name} "
                f"without an input timespan."
            )
        sql = self.select(
            collection, dataId=dataId, id=SimpleQuery.Select, run=SimpleQuery.Select, timespan=timespan
        )
        with self._db.query(sql) as results:
            row = results.fetchone()
            if row is None:
                return None
            if collection.type is CollectionType.CALIBRATION:
                # For temporal calibration lookups (only!) our invariants do
                # not guarantee that the number of result rows is <= 1.  They
                # would if `select` constrained the given timespan to be
                # _contained_ by the validity range in the self._calibs table,
                # instead of simply _overlapping_ it, because we do guarantee
                # that the validity ranges are disjoint for a particular
                # dataset type, collection, and data ID.  But using an overlap
                # test and a check for multiple result rows here allows us to
                # provide a more useful diagnostic, as well as allowing
                # `select` to support more general queries where multiple
                # results are not an error.
                if results.fetchone() is not None:
                    raise RuntimeError(
                        f"Multiple matches found for calibration lookup in {collection.name} for "
                        f"{self.datasetType.name} with {dataId} overlapping {timespan}. "
                    )
        return DatasetRef(
            datasetType=self.datasetType,
            dataId=dataId,
            id=row.id,
            run=self._collections[row._mapping[self._runKeyColumn]].name,
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
            raise TypeError(
                f"Cannot associate into collection '{collection.name}' "
                f"of type {collection.type.name}; must be TAGGED."
            )
        protoRow = {
            self._collections.getCollectionForeignKeyName(): collection.key,
            "dataset_type_id": self._dataset_type_id,
        }
        rows = []
        summary = CollectionSummary()
        for dataset in summary.add_datasets_generator(datasets):
            row = dict(protoRow, dataset_id=dataset.getCheckedId())
            for dimension, value in dataset.dataId.items():
                row[dimension.name] = value
            rows.append(row)
        # Update the summary tables for this collection in case this is the
        # first time this dataset type or these governor values will be
        # inserted there.
        self._summaries.update(collection, [self._dataset_type_id], summary)
        # Update the tag table itself.
        self._db.replace(self._tags, *rows)

    def disassociate(self, collection: CollectionRecord, datasets: Iterable[DatasetRef]) -> None:
        # Docstring inherited from DatasetRecordStorage.
        if collection.type is not CollectionType.TAGGED:
            raise TypeError(
                f"Cannot disassociate from collection '{collection.name}' "
                f"of type {collection.type.name}; must be TAGGED."
            )
        rows = [
            {
                "dataset_id": dataset.getCheckedId(),
                self._collections.getCollectionForeignKeyName(): collection.key,
            }
            for dataset in datasets
        ]
        self._db.delete(self._tags, ["dataset_id", self._collections.getCollectionForeignKeyName()], *rows)

    def _buildCalibOverlapQuery(
        self, collection: CollectionRecord, dataIds: DataCoordinateSet | None, timespan: Timespan
    ) -> SimpleQuery:
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
        TimespanReprClass = self._db.getTimespanRepresentation()
        query.where.append(
            TimespanReprClass.from_columns(self._calibs.columns).overlaps(
                TimespanReprClass.fromLiteral(timespan)
            )
        )
        return query

    def certify(
        self, collection: CollectionRecord, datasets: Iterable[DatasetRef], timespan: Timespan
    ) -> None:
        # Docstring inherited from DatasetRecordStorage.
        if self._calibs is None:
            raise CollectionTypeError(
                f"Cannot certify datasets of type {self.datasetType.name}, for which "
                f"DatasetType.isCalibration() is False."
            )
        if collection.type is not CollectionType.CALIBRATION:
            raise CollectionTypeError(
                f"Cannot certify into collection '{collection.name}' "
                f"of type {collection.type.name}; must be CALIBRATION."
            )
        TimespanReprClass = self._db.getTimespanRepresentation()
        protoRow = {
            self._collections.getCollectionForeignKeyName(): collection.key,
            "dataset_type_id": self._dataset_type_id,
        }
        rows = []
        dataIds: set[DataCoordinate] | None = (
            set() if not TimespanReprClass.hasExclusionConstraint() else None
        )
        summary = CollectionSummary()
        for dataset in summary.add_datasets_generator(datasets):
            row = dict(protoRow, dataset_id=dataset.getCheckedId())
            for dimension, value in dataset.dataId.items():
                row[dimension.name] = value
            TimespanReprClass.update(timespan, result=row)
            rows.append(row)
            if dataIds is not None:
                dataIds.add(dataset.dataId)
        # Update the summary tables for this collection in case this is the
        # first time this dataset type or these governor values will be
        # inserted there.
        self._summaries.update(collection, [self._dataset_type_id], summary)
        # Update the association table itself.
        if TimespanReprClass.hasExclusionConstraint():
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
                timespan,
            )
            query.columns.append(sqlalchemy.sql.func.count())
            sql = query.combine()
            # Acquire a table lock to ensure there are no concurrent writes
            # could invalidate our checking before we finish the inserts.  We
            # use a SAVEPOINT in case there is an outer transaction that a
            # failure here should not roll back.
            with self._db.transaction(lock=[self._calibs], savepoint=True):
                # Run the check SELECT query.
                with self._db.query(sql) as sql_result:
                    conflicting = sql_result.scalar()
                if conflicting > 0:
                    raise ConflictingDefinitionError(
                        f"{conflicting} validity range conflicts certifying datasets of type "
                        f"{self.datasetType.name} into {collection.name} for range "
                        f"[{timespan.begin}, {timespan.end})."
                    )
                # Proceed with the insert.
                self._db.insert(self._calibs, *rows)

    def decertify(
        self,
        collection: CollectionRecord,
        timespan: Timespan,
        *,
        dataIds: Iterable[DataCoordinate] | None = None,
    ) -> None:
        # Docstring inherited from DatasetRecordStorage.
        if self._calibs is None:
            raise CollectionTypeError(
                f"Cannot decertify datasets of type {self.datasetType.name}, for which "
                f"DatasetType.isCalibration() is False."
            )
        if collection.type is not CollectionType.CALIBRATION:
            raise CollectionTypeError(
                f"Cannot decertify from collection '{collection.name}' "
                f"of type {collection.type.name}; must be CALIBRATION."
            )
        TimespanReprClass = self._db.getTimespanRepresentation()
        # Construct a SELECT query to find all rows that overlap our inputs.
        dataIdSet: DataCoordinateSet | None
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
            with self._db.query(sql) as sql_result:
                sql_rows = sql_result.mappings().fetchall()
            for row in sql_rows:
                rowsToDelete.append({"id": row["id"]})
                # Construct the insert row(s) by copying the prototype row,
                # then adding the dimension column values, then adding what's
                # left of the timespan from that row after we subtract the
                # given timespan.
                newInsertRow = protoInsertRow.copy()
                newInsertRow["dataset_id"] = row["dataset_id"]
                for name in self.datasetType.dimensions.required.names:
                    newInsertRow[name] = row[name]
                rowTimespan = TimespanReprClass.extract(row)
                assert rowTimespan is not None, "Field should have a NOT NULL constraint."
                for diffTimespan in rowTimespan.difference(timespan):
                    rowsToInsert.append(TimespanReprClass.update(diffTimespan, result=newInsertRow.copy()))
            # Run the DELETE and INSERT queries.
            self._db.delete(self._calibs, ["id"], *rowsToDelete)
            self._db.insert(self._calibs, *rowsToInsert)

    def select(
        self,
        *collections: CollectionRecord,
        dataId: SimpleQuery.Select.Or[DataCoordinate] = SimpleQuery.Select,
        id: SimpleQuery.Select.Or[int | None] = SimpleQuery.Select,
        run: SimpleQuery.Select.Or[None] = SimpleQuery.Select,
        timespan: SimpleQuery.Select.Or[Timespan | None] = SimpleQuery.Select,
        ingestDate: SimpleQuery.Select.Or[Timespan | None] = None,
        rank: SimpleQuery.Select.Or[None] = None,
    ) -> sqlalchemy.sql.Selectable:
        # Docstring inherited from DatasetRecordStorage.
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
        # But this method is documented/typed such that ``dataId`` is never
        # `None` - i.e. we always constrain or retreive the data ID.  That
        # means we'll always include the tags/calibs table and join in the
        # static dataset table only if we need things from it that we can't get
        # from the tags/calibs table.
        #
        # Note that it's important that we include a WHERE constraint on both
        # tables for any column (e.g. dataset_type_id) that is in both when
        # it's given explicitly; not doing can prevent the query planner from
        # using very important indexes.  At present, we don't include those
        # redundant columns in the JOIN ON expression, however, because the
        # FOREIGN KEY (and its index) are defined only on dataset_id.
        #
        # We'll start by accumulating kwargs to pass to SimpleQuery.join when
        # we bring in the tags/calibs table.  We get the data ID or constrain
        # it in the tags/calibs table(s), but that's multiple columns, not one,
        # so we need to transform the one Select.Or argument into a dictionary
        # of them.
        kwargs: dict[str, Any]
        if dataId is SimpleQuery.Select:
            kwargs = {dim.name: SimpleQuery.Select for dim in self.datasetType.dimensions.required}
        else:
            kwargs = dict(dataId.byName())
        # We always constrain (never retrieve) the dataset type in at least the
        # tags/calibs table.
        kwargs["dataset_type_id"] = self._dataset_type_id
        # Join in the tags and/or calibs tables, turning those 'kwargs' entries
        # into WHERE constraints or SELECT columns as appropriate.
        if collection_types != {CollectionType.CALIBRATION}:
            # We'll need a subquery for the tags table if any of the given
            # collections are not a CALIBRATION collection.  This intentionally
            # also fires when the list of collections is empty as a way to
            # create a dummy subquery that we know will fail.
            tags_query = SimpleQuery()
            tags_query.join(self._tags, **kwargs)
            # If the timespan is requested, simulate a potentially compound
            # column whose values are the maximum and minimum timespan
            # bounds.
            # If the timespan is constrained, ignore the constraint, since
            # it'd be guaranteed to evaluate to True.
            if timespan is SimpleQuery.Select:
                tags_query.columns.extend(TimespanReprClass.fromLiteral(Timespan(None, None)).flatten())
            self._finish_single_select(
                tags_query,
                self._tags,
                collections,
                id=id,
                run=run,
                ingestDate=ingestDate,
                rank=rank,
            )
        else:
            tags_query = None
        if CollectionType.CALIBRATION in collection_types:
            # If at least one collection is a CALIBRATION collection, we'll
            # need a subquery for the calibs table, and could include the
            # timespan as a result or constraint.
            calibs_query = SimpleQuery()
            assert (
                self._calibs is not None
            ), "DatasetTypes with isCalibration() == False can never be found in a CALIBRATION collection."
            calibs_query.join(self._calibs, **kwargs)
            # Add the timespan column(s) to the result columns, or constrain
            # the timespan via an overlap condition.
            if timespan is SimpleQuery.Select:
                calibs_query.columns.extend(TimespanReprClass.from_columns(self._calibs.columns).flatten())
            elif timespan is not None:
                calibs_query.where.append(
                    TimespanReprClass.from_columns(self._calibs.columns).overlaps(
                        TimespanReprClass.fromLiteral(timespan)
                    )
                )
            self._finish_single_select(
                calibs_query,
                self._calibs,
                collections,
                id=id,
                run=run,
                ingestDate=ingestDate,
                rank=rank,
            )
        else:
            calibs_query = None
        if calibs_query is not None:
            if tags_query is not None:
                return tags_query.combine().union(calibs_query.combine())
            else:
                return calibs_query.combine()
        else:
            assert tags_query is not None, "Earlier logic should guaranteed at least one is not None."
            return tags_query.combine()

    def _finish_single_select(
        self,
        query: SimpleQuery,
        table: sqlalchemy.schema.Table,
        collections: Sequence[CollectionRecord],
        id: SimpleQuery.Select.Or[int | None],
        run: SimpleQuery.Select.Or[None],
        ingestDate: SimpleQuery.Select.Or[Timespan | None],
        rank: SimpleQuery.Select.Or[None],
    ) -> None:
        dataset_id_col = table.columns.dataset_id
        collection_col = table.columns[self._collections.getCollectionForeignKeyName()]
        # We always constrain (never retrieve) the collection(s) in the
        # tags/calibs table.
        if len(collections) == 1:
            query.where.append(collection_col == collections[0].key)
        elif len(collections) == 0:
            # We support the case where there are no collections as a way to
            # generate a valid SQL query that can't yield results.  This should
            # never get executed, but lots of downstream code will still try
            # to access the SQLAlchemy objects representing the columns in the
            # subquery.  That's not ideal, but it'd take a lot of refactoring
            # to fix it (DM-31725).
            query.where.append(sqlalchemy.sql.literal(False))
        else:
            query.where.append(collection_col.in_([collection.key for collection in collections]))
        # Add rank if requested as a CASE-based calculation the collection
        # column.
        if rank is not None:
            assert rank is SimpleQuery.Select, "Cannot constraint rank, only select it."
            query.columns.append(
                sqlalchemy.sql.case(
                    {record.key: n for n, record in enumerate(collections)},
                    value=collection_col,
                ).label("rank")
            )
        # We can always get the dataset_id from the tags/calibs table or
        # constrain it there.  Can't use kwargs for that because we need to
        # alias it to 'id'.
        if id is SimpleQuery.Select:
            query.columns.append(dataset_id_col.label("id"))
        elif id is not None:
            query.where.append(dataset_id_col == id)
        # It's possible we now have everything we need, from just the
        # tags/calibs table.  The things we might need to get from the static
        # dataset table are the run key and the ingest date.
        need_static_table = False
        static_kwargs: dict[str, Any] = {}
        if run is not None:
            assert run is SimpleQuery.Select, "To constrain the run name, pass a RunRecord as a collection."
            if len(collections) == 1 and collections[0].type is CollectionType.RUN:
                # If we are searching exactly one RUN collection, we
                # know that if we find the dataset in that collection,
                # then that's the datasets's run; we don't need to
                # query for it.
                query.columns.append(sqlalchemy.sql.literal(collections[0].key).label(self._runKeyColumn))
            else:
                static_kwargs[self._runKeyColumn] = SimpleQuery.Select
                need_static_table = True
        # Ingest date can only come from the static table.
        if ingestDate is not None:
            need_static_table = True
            if ingestDate is SimpleQuery.Select:
                static_kwargs["ingest_date"] = SimpleQuery.Select
            else:
                assert isinstance(ingestDate, Timespan)
                # Timespan is astropy Time (usually in TAI) and ingest_date is
                # TIMESTAMP, convert values to Python datetime for sqlalchemy.
                if ingestDate.isEmpty():
                    raise RuntimeError("Empty timespan constraint provided for ingest_date.")
                if ingestDate.begin is not None:
                    begin = ingestDate.begin.utc.datetime  # type: ignore
                    query.where.append(self._static.dataset.columns.ingest_date >= begin)
                if ingestDate.end is not None:
                    end = ingestDate.end.utc.datetime  # type: ignore
                    query.where.append(self._static.dataset.columns.ingest_date < end)
        # If we need the static table, join it in via dataset_id and
        # dataset_type_id
        if need_static_table:
            query.join(
                self._static.dataset,
                onclause=(dataset_id_col == self._static.dataset.columns.id),
                **static_kwargs,
            )
            # Also constrain dataset_type_id in static table in case that helps
            # generate a better plan.
            # We could also include this in the JOIN ON clause, but my guess is
            # that that's a good idea IFF it's in the foreign key, and right
            # now it isn't.
            query.where.append(self._static.dataset.columns.dataset_type_id == self._dataset_type_id)

    def getDataId(self, id: DatasetId) -> DataCoordinate:
        """Return DataId for a dataset.

        Parameters
        ----------
        id : `DatasetId`
            Unique dataset identifier.

        Returns
        -------
        dataId : `DataCoordinate`
            DataId for the dataset.
        """
        # This query could return multiple rows (one for each tagged collection
        # the dataset is in, plus one for its run collection), and we don't
        # care which of those we get.
        sql = (
            self._tags.select()
            .where(
                sqlalchemy.sql.and_(
                    self._tags.columns.dataset_id == id,
                    self._tags.columns.dataset_type_id == self._dataset_type_id,
                )
            )
            .limit(1)
        )
        with self._db.query(sql) as sql_result:
            row = sql_result.mappings().fetchone()
        assert row is not None, "Should be guaranteed by caller and foreign key constraints."
        return DataCoordinate.standardize(
            {dimension.name: row[dimension.name] for dimension in self.datasetType.dimensions.required},
            graph=self.datasetType.dimensions,
        )


@deprecated(
    "Integer dataset IDs are deprecated in favor of UUIDs; support will be removed after v26. "
    "Please migrate or re-create this data repository.",
    version="v25.0",
    category=FutureWarning,
)
class ByDimensionsDatasetRecordStorageInt(ByDimensionsDatasetRecordStorage):
    """Implementation of ByDimensionsDatasetRecordStorage which uses integer
    auto-incremented column for dataset IDs.
    """

    def insert(
        self,
        run: RunRecord,
        dataIds: Iterable[DataCoordinate],
        idMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
    ) -> Iterator[DatasetRef]:
        # Docstring inherited from DatasetRecordStorage.

        # We only support UNIQUE mode for integer dataset IDs
        if idMode != DatasetIdGenEnum.UNIQUE:
            raise UnsupportedIdGeneratorError("Only UNIQUE mode can be used with integer dataset IDs.")

        # Transform a possibly-single-pass iterable into a list.
        dataIdList = list(dataIds)
        yield from self._insert(run, dataIdList)

    def import_(
        self,
        run: RunRecord,
        datasets: Iterable[DatasetRef],
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
        reuseIds: bool = False,
    ) -> Iterator[DatasetRef]:
        # Docstring inherited from DatasetRecordStorage.

        # We only support UNIQUE mode for integer dataset IDs
        if idGenerationMode != DatasetIdGenEnum.UNIQUE:
            raise UnsupportedIdGeneratorError("Only UNIQUE mode can be used with integer dataset IDs.")

        # Make a list of dataIds and optionally dataset IDs.
        dataIdList: list[DataCoordinate] = []
        datasetIdList: list[int] = []
        for dataset in datasets:
            dataIdList.append(dataset.dataId)

            # We only accept integer dataset IDs, but also allow None.
            datasetId = dataset.id
            if datasetId is None:
                # if reuseIds is set then all IDs must be known
                if reuseIds:
                    raise TypeError("All dataset IDs must be known if `reuseIds` is set")
            elif isinstance(datasetId, int):
                if reuseIds:
                    datasetIdList.append(datasetId)
            else:
                raise TypeError(f"Unsupported type of dataset ID: {type(datasetId)}")

        yield from self._insert(run, dataIdList, datasetIdList)

    def _insert(
        self, run: RunRecord, dataIdList: list[DataCoordinate], datasetIdList: list[int] | None = None
    ) -> Iterator[DatasetRef]:
        """Common part of implementation of `insert` and `import_` methods."""

        # Remember any governor dimension values we see.
        summary = CollectionSummary()
        summary.add_data_ids(self.datasetType, dataIdList)

        staticRow = {
            "dataset_type_id": self._dataset_type_id,
            self._runKeyColumn: run.key,
        }
        with self._db.transaction():
            # Insert into the static dataset table, generating autoincrement
            # dataset_id values.
            if datasetIdList:
                # reuse existing IDs
                rows = [dict(staticRow, id=datasetId) for datasetId in datasetIdList]
                self._db.insert(self._static.dataset, *rows)
            else:
                # use auto-incremented IDs
                datasetIdList = self._db.insert(
                    self._static.dataset, *([staticRow] * len(dataIdList)), returnIds=True
                )
                assert datasetIdList is not None
            # Update the summary tables for this collection in case this is the
            # first time this dataset type or these governor values will be
            # inserted there.
            self._summaries.update(run, [self._dataset_type_id], summary)
            # Combine the generated dataset_id values and data ID fields to
            # form rows to be inserted into the tags table.
            protoTagsRow = {
                "dataset_type_id": self._dataset_type_id,
                self._collections.getCollectionForeignKeyName(): run.key,
            }
            tagsRows = [
                dict(protoTagsRow, dataset_id=dataset_id, **dataId.byName())
                for dataId, dataset_id in zip(dataIdList, datasetIdList)
            ]
            # Insert those rows into the tags table.  This is where we'll
            # get any unique constraint violations.
            self._db.insert(self._tags, *tagsRows)

        for dataId, datasetId in zip(dataIdList, datasetIdList):
            yield DatasetRef(
                datasetType=self.datasetType,
                dataId=dataId,
                id=datasetId,
                run=run.name,
            )


class ByDimensionsDatasetRecordStorageUUID(ByDimensionsDatasetRecordStorage):
    """Implementation of ByDimensionsDatasetRecordStorage which uses UUID for
    dataset IDs.
    """

    idMaker = DatasetIdFactory()
    """Factory for dataset IDs. In the future this factory may be shared with
    other classes (e.g. Registry)."""

    def insert(
        self,
        run: RunRecord,
        dataIds: Iterable[DataCoordinate],
        idMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
    ) -> Iterator[DatasetRef]:
        # Docstring inherited from DatasetRecordStorage.

        # Iterate over data IDs, transforming a possibly-single-pass iterable
        # into a list.
        dataIdList = []
        rows = []
        summary = CollectionSummary()
        for dataId in summary.add_data_ids_generator(self.datasetType, dataIds):
            dataIdList.append(dataId)
            rows.append(
                {
                    "id": self.idMaker.makeDatasetId(run.name, self.datasetType, dataId, idMode),
                    "dataset_type_id": self._dataset_type_id,
                    self._runKeyColumn: run.key,
                }
            )

        with self._db.transaction():
            # Insert into the static dataset table.
            self._db.insert(self._static.dataset, *rows)
            # Update the summary tables for this collection in case this is the
            # first time this dataset type or these governor values will be
            # inserted there.
            self._summaries.update(run, [self._dataset_type_id], summary)
            # Combine the generated dataset_id values and data ID fields to
            # form rows to be inserted into the tags table.
            protoTagsRow = {
                "dataset_type_id": self._dataset_type_id,
                self._collections.getCollectionForeignKeyName(): run.key,
            }
            tagsRows = [
                dict(protoTagsRow, dataset_id=row["id"], **dataId.byName())
                for dataId, row in zip(dataIdList, rows)
            ]
            # Insert those rows into the tags table.
            self._db.insert(self._tags, *tagsRows)

        for dataId, row in zip(dataIdList, rows):
            yield DatasetRef(
                datasetType=self.datasetType,
                dataId=dataId,
                id=row["id"],
                run=run.name,
            )

    def import_(
        self,
        run: RunRecord,
        datasets: Iterable[DatasetRef],
        idGenerationMode: DatasetIdGenEnum = DatasetIdGenEnum.UNIQUE,
        reuseIds: bool = False,
    ) -> Iterator[DatasetRef]:
        # Docstring inherited from DatasetRecordStorage.

        # Iterate over data IDs, transforming a possibly-single-pass iterable
        # into a list.
        dataIds = {}
        summary = CollectionSummary()
        for dataset in summary.add_datasets_generator(datasets):
            # Ignore unknown ID types, normally all IDs have the same type but
            # this code supports mixed types or missing IDs.
            datasetId = dataset.id if isinstance(dataset.id, uuid.UUID) else None
            if datasetId is None:
                datasetId = self.idMaker.makeDatasetId(
                    run.name, self.datasetType, dataset.dataId, idGenerationMode
                )
            dataIds[datasetId] = dataset.dataId

        # We'll insert all new rows into a temporary table
        tableSpec = makeTagTableSpec(self.datasetType, type(self._collections), ddl.GUID, constraints=False)
        collFkName = self._collections.getCollectionForeignKeyName()
        protoTagsRow = {
            "dataset_type_id": self._dataset_type_id,
            collFkName: run.key,
        }
        tmpRows = [
            dict(protoTagsRow, dataset_id=dataset_id, **dataId.byName())
            for dataset_id, dataId in dataIds.items()
        ]
        with self._db.transaction(for_temp_tables=True):
            with self._db.temporary_table(tableSpec) as tmp_tags:
                # store all incoming data in a temporary table
                self._db.insert(tmp_tags, *tmpRows)

                # There are some checks that we want to make for consistency
                # of the new datasets with existing ones.
                self._validateImport(tmp_tags, run)

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
                        tmp_tags.columns[collFkName].label(self._runKeyColumn),
                    ),
                )

                # Update the summary tables for this collection in case this
                # is the first time this dataset type or these governor values
                # will be inserted there.
                self._summaries.update(run, [self._dataset_type_id], summary)

                # Copy it into tags table.
                self._db.insert(self._tags, select=tmp_tags.select())

        # Return refs in the same order as in the input list.
        for dataset_id, dataId in dataIds.items():
            yield DatasetRef(
                datasetType=self.datasetType,
                id=dataset_id,
                dataId=dataId,
                run=run.name,
            )

    def _validateImport(self, tmp_tags: sqlalchemy.schema.Table, run: RunRecord) -> None:
        """Validate imported refs against existing datasets.

        Parameters
        ----------
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
        tags = self._tags
        collFkName = self._collections.getCollectionForeignKeyName()

        # Check that existing datasets have the same dataset type and
        # run.
        query = (
            sqlalchemy.sql.select(
                dataset.columns.id.label("dataset_id"),
                dataset.columns.dataset_type_id.label("dataset_type_id"),
                tmp_tags.columns.dataset_type_id.label("new dataset_type_id"),
                dataset.columns[self._runKeyColumn].label("run"),
                tmp_tags.columns[collFkName].label("new run"),
            )
            .select_from(dataset.join(tmp_tags, dataset.columns.id == tmp_tags.columns.dataset_id))
            .where(
                sqlalchemy.sql.or_(
                    dataset.columns.dataset_type_id != tmp_tags.columns.dataset_type_id,
                    dataset.columns[self._runKeyColumn] != tmp_tags.columns[collFkName],
                )
            )
            .limit(1)
        )
        with self._db.query(query) as result:
            if (row := result.first()) is not None:
                # Only include the first one in the exception message
                raise ConflictingDefinitionError(
                    f"Existing dataset type or run do not match new dataset: {row._asdict()}"
                )

        # Check that matching dataset in tags table has the same DataId.
        query = (
            sqlalchemy.sql.select(
                tags.columns.dataset_id,
                tags.columns.dataset_type_id.label("type_id"),
                tmp_tags.columns.dataset_type_id.label("new type_id"),
                *[tags.columns[dim] for dim in self.datasetType.dimensions.required.names],
                *[
                    tmp_tags.columns[dim].label(f"new {dim}")
                    for dim in self.datasetType.dimensions.required.names
                ],
            )
            .select_from(tags.join(tmp_tags, tags.columns.dataset_id == tmp_tags.columns.dataset_id))
            .where(
                sqlalchemy.sql.or_(
                    tags.columns.dataset_type_id != tmp_tags.columns.dataset_type_id,
                    *[
                        tags.columns[dim] != tmp_tags.columns[dim]
                        for dim in self.datasetType.dimensions.required.names
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
                tags.columns.dataset_type_id.label("dataset_type_id"),
                *[tags.columns[dim] for dim in self.datasetType.dimensions.required.names],
                tags.columns.dataset_id,
                tmp_tags.columns.dataset_id.label("new dataset_id"),
                tags.columns[collFkName],
                tmp_tags.columns[collFkName].label(f"new {collFkName}"),
            )
            .select_from(
                tags.join(
                    tmp_tags,
                    sqlalchemy.sql.and_(
                        tags.columns.dataset_type_id == tmp_tags.columns.dataset_type_id,
                        tags.columns[collFkName] == tmp_tags.columns[collFkName],
                        *[
                            tags.columns[dim] == tmp_tags.columns[dim]
                            for dim in self.datasetType.dimensions.required.names
                        ],
                    ),
                )
            )
            .where(tags.columns.dataset_id != tmp_tags.columns.dataset_id)
            .limit(1)
        )
        with self._db.query(query) as result:
            if (row := result.first()) is not None:
                # only include the first one in the exception message
                raise ConflictingDefinitionError(
                    f"Existing dataset type and dataId does not match new dataset: {row._asdict()}"
                )
