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
from collections.abc import Iterable, Iterator, Sequence, Set
from typing import TYPE_CHECKING

import sqlalchemy
from deprecated.sphinx import deprecated
from lsst.daf.relation import Relation, sql

from ....core import (
    DataCoordinate,
    DatasetColumnTag,
    DatasetId,
    DatasetRef,
    DatasetType,
    DimensionKeyColumnTag,
    LogicalColumn,
    Timespan,
    ddl,
)
from ..._collection_summary import CollectionSummary
from ..._collectionType import CollectionType
from ..._exceptions import CollectionTypeError, ConflictingDefinitionError, UnsupportedIdGeneratorError
from ...interfaces import DatasetIdFactory, DatasetIdGenEnum, DatasetRecordStorage
from ...queries import SqlQueryContext
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
        self,
        collection: CollectionRecord,
        data_ids: set[DataCoordinate] | None,
        timespan: Timespan,
        context: SqlQueryContext,
    ) -> Relation:
        relation = self.make_relation(
            collection, columns={"timespan", "dataset_id", "calib_pkey"}, context=context
        ).with_rows_satisfying(
            context.make_timespan_overlap_predicate(
                DatasetColumnTag(self.datasetType.name, "timespan"), timespan
            ),
        )
        if data_ids is not None:
            relation = relation.join(
                context.make_data_id_relation(
                    data_ids, self.datasetType.dimensions.required.names
                ).transferred_to(context.sql_engine),
            )
        return relation

    def certify(
        self,
        collection: CollectionRecord,
        datasets: Iterable[DatasetRef],
        timespan: Timespan,
        context: SqlQueryContext,
    ) -> None:
        # Docstring inherited from DatasetRecordStorage.
        if self._calibs is None:
            raise CollectionTypeError(
                f"Cannot certify datasets of type {self.datasetType.name}, for which "
                "DatasetType.isCalibration() is False."
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
            relation = self._buildCalibOverlapQuery(collection, dataIds, timespan, context)
            # Acquire a table lock to ensure there are no concurrent writes
            # could invalidate our checking before we finish the inserts.  We
            # use a SAVEPOINT in case there is an outer transaction that a
            # failure here should not roll back.
            with self._db.transaction(lock=[self._calibs], savepoint=True):
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
        context: SqlQueryContext,
    ) -> None:
        # Docstring inherited from DatasetRecordStorage.
        if self._calibs is None:
            raise CollectionTypeError(
                f"Cannot decertify datasets of type {self.datasetType.name}, for which "
                "DatasetType.isCalibration() is False."
            )
        if collection.type is not CollectionType.CALIBRATION:
            raise CollectionTypeError(
                f"Cannot decertify from collection '{collection.name}' "
                f"of type {collection.type.name}; must be CALIBRATION."
            )
        TimespanReprClass = self._db.getTimespanRepresentation()
        # Construct a SELECT query to find all rows that overlap our inputs.
        dataIdSet: set[DataCoordinate] | None
        if dataIds is not None:
            dataIdSet = set(dataIds)
        else:
            dataIdSet = None
        relation = self._buildCalibOverlapQuery(collection, dataIdSet, timespan, context)
        calib_pkey_tag = DatasetColumnTag(self.datasetType.name, "calib_pkey")
        dataset_id_tag = DatasetColumnTag(self.datasetType.name, "dataset_id")
        timespan_tag = DatasetColumnTag(self.datasetType.name, "timespan")
        data_id_tags = [
            (name, DimensionKeyColumnTag(name)) for name in self.datasetType.dimensions.required.names
        ]
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
            # Enter SqlQueryContext in case we need to use a temporary table to
            # include the give data IDs in the query (see similar block in
            # certify for details).
            with context:
                for row in context.fetch_iterable(relation):
                    rowsToDelete.append({"id": row[calib_pkey_tag]})
                    # Construct the insert row(s) by copying the prototype row,
                    # then adding the dimension column values, then adding
                    # what's left of the timespan from that row after we
                    # subtract the given timespan.
                    newInsertRow = protoInsertRow.copy()
                    newInsertRow["dataset_id"] = row[dataset_id_tag]
                    for name, tag in data_id_tags:
                        newInsertRow[name] = row[tag]
                    rowTimespan = row[timespan_tag]
                    assert rowTimespan is not None, "Field should have a NOT NULL constraint."
                    for diffTimespan in rowTimespan.difference(timespan):
                        rowsToInsert.append(
                            TimespanReprClass.update(diffTimespan, result=newInsertRow.copy())
                        )
            # Run the DELETE and INSERT queries.
            self._db.delete(self._calibs, ["id"], *rowsToDelete)
            self._db.insert(self._calibs, *rowsToInsert)

    def make_relation(
        self,
        *collections: CollectionRecord,
        columns: Set[str],
        context: SqlQueryContext,
    ) -> Relation:
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
            # We'll need a subquery for the tags table if any of the given
            # collections are not a CALIBRATION collection.  This intentionally
            # also fires when the list of collections is empty as a way to
            # create a dummy subquery that we know will fail.
            # We give the table an alias because it might appear multiple times
            # in the same query, for different dataset types.
            tags_parts = sql.Payload[LogicalColumn](self._tags.alias(f"{self.datasetType.name}_tags"))
            if "timespan" in columns:
                tags_parts.columns_available[
                    DatasetColumnTag(self.datasetType.name, "timespan")
                ] = TimespanReprClass.fromLiteral(Timespan(None, None))
            tag_relation = self._finish_single_relation(
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
            assert (
                self._calibs is not None
            ), "DatasetTypes with isCalibration() == False can never be found in a CALIBRATION collection."
            calibs_parts = sql.Payload[LogicalColumn](self._calibs.alias(f"{self.datasetType.name}_calibs"))
            if "timespan" in columns:
                calibs_parts.columns_available[
                    DatasetColumnTag(self.datasetType.name, "timespan")
                ] = TimespanReprClass.from_columns(calibs_parts.from_clause.columns)
            if "calib_pkey" in columns:
                # This is a private extension not included in the base class
                # interface, for internal use only in _buildCalibOverlapQuery,
                # which needs access to the autoincrement primary key for the
                # calib association table.
                calibs_parts.columns_available[
                    DatasetColumnTag(self.datasetType.name, "calib_pkey")
                ] = calibs_parts.from_clause.columns.id
            calib_relation = self._finish_single_relation(
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
        payload: sql.Payload[LogicalColumn],
        requested_columns: Set[str],
        collections: Sequence[tuple[CollectionRecord, int]],
        context: SqlQueryContext,
    ) -> Relation:
        """Helper method for `make_relation`.

        This handles adding columns and WHERE terms that are not specific to
        either the tags or calibs tables.

        Parameters
        ----------
        payload : `lsst.daf.relation.sql.Payload`
            SQL query parts under construction, to be modified in-place and
            used to construct the new relation.
        requested_columns : `~collections.abc.Set` [ `str` ]
            Columns the relation should include.
        collections : `Sequence` [ `tuple` [ `CollectionRecord`, `int` ] ]
            Collections to search for the dataset and their ranks.
        context : `SqlQueryContext`
            Context that manages engines and state for the query.

        Returns
        -------
        relation : `lsst.daf.relation.Relation`
            New dataset query relation.
        """
        payload.where.append(payload.from_clause.columns.dataset_type_id == self._dataset_type_id)
        dataset_id_col = payload.from_clause.columns.dataset_id
        collection_col = payload.from_clause.columns[self._collections.getCollectionForeignKeyName()]
        # We always constrain and optionally retrieve the collection(s) via the
        # tags/calibs table.
        if len(collections) == 1:
            payload.where.append(collection_col == collections[0][0].key)
            if "collection" in requested_columns:
                payload.columns_available[
                    DatasetColumnTag(self.datasetType.name, "collection")
                ] = sqlalchemy.sql.literal(collections[0][0].key)
        else:
            assert collections, "The no-collections case should be in calling code for better diagnostics."
            payload.where.append(collection_col.in_([collection.key for collection, _ in collections]))
            if "collection" in requested_columns:
                payload.columns_available[
                    DatasetColumnTag(self.datasetType.name, "collection")
                ] = collection_col
        # Add rank if requested as a CASE-based calculation the collection
        # column.
        if "rank" in requested_columns:
            payload.columns_available[DatasetColumnTag(self.datasetType.name, "rank")] = sqlalchemy.sql.case(
                {record.key: rank for record, rank in collections},
                value=collection_col,
            )
        # Add more column definitions, starting with the data ID.
        for dimension_name in self.datasetType.dimensions.required.names:
            payload.columns_available[DimensionKeyColumnTag(dimension_name)] = payload.from_clause.columns[
                dimension_name
            ]
        # We can always get the dataset_id from the tags/calibs table.
        if "dataset_id" in requested_columns:
            payload.columns_available[DatasetColumnTag(self.datasetType.name, "dataset_id")] = dataset_id_col
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
                payload.columns_available[
                    DatasetColumnTag(self.datasetType.name, "run")
                ] = sqlalchemy.sql.literal(collections[0][0].key)
            else:
                payload.columns_available[
                    DatasetColumnTag(self.datasetType.name, "run")
                ] = self._static.dataset.columns[self._runKeyColumn]
                need_static_table = True
        # Ingest date can only come from the static table.
        if "ingest_date" in requested_columns:
            need_static_table = True
            payload.columns_available[
                DatasetColumnTag(self.datasetType.name, "ingest_date")
            ] = self._static.dataset.columns.ingest_date
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
            payload.where.append(self._static.dataset.columns.dataset_type_id == self._dataset_type_id)
        leaf = context.sql_engine.make_leaf(
            payload.columns_available.keys(),
            payload=payload,
            name=self.datasetType.name,
            parameters={record.name: rank for record, rank in collections},
        )
        return leaf

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
