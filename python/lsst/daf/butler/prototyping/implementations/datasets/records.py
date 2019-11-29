from __future__ import annotations

__all__ = ["ByDimensionsRegistryLayerDatasetRecords"]

from datetime import datetime
from typing import (
    List,
    Optional,
)

import sqlalchemy

from ....core.datasets import DatasetType, ResolvedDatasetHandle, DatasetUniqueness
from ....core.dimensions import DataCoordinate
from ....core.timespan import Timespan, TIMESPAN_FIELD_SPECS, extractColumnTimespan

from ...iterables import DataIdIterable, SingleDatasetTypeIterable
from ...quantum import Quantum

from ...interfaces import (
    CollectionType,
    Database,
    RegistryLayerCollectionStorage,
    RegistryLayerDatasetRecords,
    Select,
)

from .ddl import StaticDatasetTablesTuple


class DatasetSelect:

    def __init__(self):
        self.columns = []
        self.where = []
        self._from = None

    def join(self, table: sqlalchemy.sql.FromClause, *,
             onclause: Optional[sqlalchemy.sql.ColumnElement] = None,
             isouter: Optional[bool] = False,
             full: Optional[bool] = False,
             **kwds: Select.Or):
        if self._from is None:
            self._from = table
        else:
            self._from = self._from.join(table, onclause=onclause, isouter=isouter, full=full)
        for name, arg in kwds.items():
            if arg is Select:
                self.columns.append(table.columns[name].label(name))
            else:
                self.where.append(table.columns[name] == arg)

    def finish(self) -> sqlalchemy.sql.FromClause:
        return sqlalchemy.sql.select(
            self.columns
        ).select_from(
            self._from
        ).where(
            sqlalchemy.sql.and_(*self.where)
        )

    columns: List[sqlalchemy.sql.ColumnElement]
    where: List[sqlalchemy.sql.ColumnElement]


class ByDimensionsRegistryLayerDatasetRecords(RegistryLayerDatasetRecords):

    def __init__(self, *, datasetType: DatasetType, db: Database, id: int,
                 collections: RegistryLayerCollectionStorage,
                 static: StaticDatasetTablesTuple, dynamic: sqlalchemy.sql.Table):
        super().__init__(datasetType=datasetType)
        self._id = id
        self._db = db
        self._collections = collections
        self._static = static
        self._dynamic = dynamic

    def insert(self, run: str, dataIds: DataIdIterable, *,
               quantum: Optional[Quantum] = None) -> SingleDatasetTypeIterable:
        runRecord = self._collections.get(run)
        assert runRecord.type is CollectionType.RUN
        staticRow = {
            "origin": self._db.origin,
            "dataset_type_id": self.id,
            "run_id": runRecord.id,
            "quantum_id": quantum.id if quantum is not None else None,
            "quantum_origin": quantum.id if quantum is not None else None,
        }
        dataIds = list(dataIds)
        # Insert into the static dataset table, generating autoincrement
        # dataset_id values.
        datasetIdIterator = self._db.insert(self._static.dataset, *([staticRow]*len(dataIds)),
                                            returning="id")
        # Combine the generated dataset_id values and data ID fields to form
        # rows to be inserted into the dynamic table.
        protoDynamicRow = {
            "origin": self._db.origin,
            "dataset_type_id": self.id,
            "collection_id": runRecord.id,
        }
        dynamicRows = [
            dict(protoDynamicRow, dataset_id=dataset_id, **dataId.full.byName())
            for dataId, dataset_id in zip(dataIds, datasetIdIterator)
        ]
        # Insert those rows into the dynamic table.  This is where we'll
        # get any unique constraint violations.
        # TODO: wrap constraint violations from database with a better message.
        # TODO: make sure insertion into static table is rolled back if the
        #       insertions into the dynamic table fail.
        self._db.insert(self._dynamic, *dynamicRows)

    def find(self, collection: str, dataId: DataCoordinate,
             timespan: Optional[Timespan[Optional[datetime]]] = None
             ) -> Optional[ResolvedDatasetHandle]:
        assert dataId.graph == self.datasetType.dimensions
        sql = self.select(collection=collection, dataId=dataId, timespan=timespan,
                          id=Select, origin=Select, run=Select)
        row = self._db.connection.execute(sql).fetchone()
        if row is None:
            return row
        return ResolvedDatasetHandle(
            datasetType=self.datasetType,
            dataId=dataId,
            id=row["id"],
            origin=row["origin"],
            run=self._collections.get(row["run_id"]).name
        )

    def delete(self, datasets: SingleDatasetTypeIterable):
        assert datasets.datasetType == self.datasetType
        # Only delete from common dataset table; ON DELETE CASCADE constraints
        # will handle the rest.
        sql = self._static.dataset.delete().where(
            sqlalchemy.sql.and_(
                self._static.dataset.columns.id == sqlalchemy.sql.bindparam("id"),
                self._static.dataset.columns.origin == sqlalchemy.sql.bindparam("origin"),
            )
        )
        params = [{"id": dataset.id, "origin": dataset.origin} for dataset in datasets]
        self._db.connection.execute(sql, *params)

    def associate(self, collection: str, datasets: SingleDatasetTypeIterable, *,
                  timespan: Optional[Timespan[Optional[datetime]]] = None):
        assert datasets.datasetType == self.datasetType
        collectionRecord = self._collections.find(collection)
        if collectionRecord.type is CollectionType.TAGGED:
            if timespan is not None:
                raise TypeError(f"Timespan provided but '{collection}' is not a calibration collection.")
            if self.datasetType.uniqueness is DatasetUniqueness.NONSINGULAR:
                table = self._static.dataset_collection_nonsingular
                protoRow = {"collection_id": collectionRecord.id}
                rows = [dict(protoRow, dataset_id=dataset.id, dataset_origin=dataset.origin)
                        for dataset in datasets]
            elif self.datasetType.uniqueness is DatasetUniqueness.STANDARD:
                table = self._dynamic
                protoRow = {
                    "collection_id": collectionRecord.id,
                    "dataset_type_id": self.id,
                }
                rows = []
                for dataset in datasets:
                    row = dict(protoRow, dataset_id=dataset.id, dataset_origin=dataset.origin)
                    for dimension, value in dataset.dataId.items():
                        row[dimension.name] = value
                    rows.append(row)
            else:
                raise TypeError(f"Cannot associate dataset '{self.datasetType.name}' "
                                f"with uniqueness {self.datsaetType.uniqueness}.")
        elif collectionRecord.type is CollectionType.CALIBRATION:
            table = self._static.dataset_collection_calibration
            protoRow = {"collection_id": collectionRecord.id}
            rows = [dict(protoRow, dataset_id=dataset.id, dataset_origin=dataset.origin)
                    for dataset in datasets]
        else:
            raise TypeError(f"Cannot associate into collection '{collection}' "
                            f"of type {collectionRecord.type}.")
        self._db.replace(table, *rows)

    def disassociate(self, collection: str, datasets: SingleDatasetTypeIterable):
        assert datasets.datasetType == self.datasetType
        collectionRecord = self._collections.find(collection)
        if collectionRecord.type is CollectionType.RUN:
            raise TypeError(f"Cannot disassociate from run {collection}.")
        elif collectionRecord.type is CollectionType.TAGGED:
            if self.datasetType.uniqueness is DatasetUniqueness.NONSINGULAR:
                table = self._static.dataset_collection_nonsingular
            else:
                table = self._dynamic
        elif collectionRecord.type is CollectionType.CALIBRATION:
            table = self._static.dataset_collection_calibration
        else:
            raise TypeError(f"Unrecognized CollectionType value {collectionRecord.type}.")
        rows = [dict(dataset_id=dataset.id, dataset_origin=dataset.origin) for dataset in datasets]
        sql = table.delete().where(
            sqlalchemy.sql.and_(
                table.columns.collection_id == collectionRecord.id,
                table.columns.dataset_id == sqlalchemy.sql.bindparam("dataset_id"),
                table.columns.dataset_origin == sqlalchemy.sql.bindparam("dataset_origin"),
            )
        )
        self._db.connection.execute(sql, *rows)

    def select(self, collection: Select.Or[str] = Select,
               dataId: Select.Or[DataCoordinate] = Select,
               id: Select.Or[int] = Select,
               origin: Select.Or[int] = Select,
               run: Select.Or[str] = Select,
               timespan: Optional[Select.Or[Timespan[datetime]]] = None
               ) -> Optional[sqlalchemy.sql.FromClause]:

        def joinDynamic(query: DatasetSelect, joinOnRun: bool = False, **kwds: Select.Or):
            if joinOnRun:
                query.join(
                    self._dynamic,
                    onclause=sqlalchemy.sql.and_(
                        self._static.dataset.columns.id == self._dynamic.columns.dataset_id,
                        self._static.dataset.columns.origin == self._dynamic.columns.dataset_origin,
                        self._static.dataset.columns.run_id == self._dynamic.columns.collection_id
                    )
                )
            else:
                query.join(self._dynamic)  # default onclause is just id and origin
            query.where.append(self._dynamic.columns.dataset_type_id == self.id)

        def buildQuery(ctype: CollectionType, cid: Select.Or[int]) -> Optional[DatasetSelect]:
            query = DatasetSelect()
            # We always include the _static.dataset table, and we can always
            # get the id and origin fields from that; passing them as kwargs
            # here tells DatasetSelect to handle them.
            query.join(self._static.dataset, id=id, origin=origin)
            query.where.append(self._static.dataset.columns.dataset_type_id == self.id)
            # How we join in the _dynamic table and whether it provides the
            # collection_id field depends on both the CollectionType and the
            # DatasetUniqueness.
            if ctype is CollectionType.CALIBRATION:
                joinDynamic(query, onRun=True)
                table = self._static.dataset_collection_calibration
                query.join(table, collection_id=cid)
                if timespan is not None:
                    if timespan is Select:
                        for spec in TIMESPAN_FIELD_SPECS:
                            query.columns.append(table.columns[spec.name].label(spec.name))
                    else:
                        query.where.append(
                            extractColumnTimespan(table).overlaps(timespan, ops=sqlalchemy.sql)
                        )
            elif ctype is CollectionType.RUN:
                joinDynamic(query, onRun=True, collection_id=cid)
            elif self.datasetType.uniqueness is DatasetUniqueness.NONSINGULAR:
                assert ctype is CollectionType.TAGGED
                joinDynamic(query, onRun=True)
                query.join(self._static.dataset_collection_nonsingular, collection_id=cid)
            else:
                assert ctype is CollectionType.TAGGED
                assert self.datasetType.uniqueness is DatasetUniqueness.STANDARD
                joinDynamic(query, onRun=False, collection_id=cid)
            # Handle other arguments that we can't just delegate to
            # DatasetSelect.join kwargs.
            if dataId is Select:
                query.columns.extend(self._dynamic.columns[dimension.name]
                                     for dimension in self.datasetType.dimensions)
            else:
                query.where.extend(self._dynamic.columns[dimension.name] == value
                                   for dimension, value in dataId.items())
            if run is Select:
                query.columns.append(self._static.dataset.columns.run_id.label("run_id"))
            else:
                runRecord = self._collections.find(run)
                if runRecord is None:
                    # Given run doesn't exist in this layer, so we know the
                    # query will yield no results.
                    return None
                if runRecord.type is not CollectionType.RUN:
                    raise TypeError(f"{run} is {runRecord.type} collection, not a run.")
                query.where.append(self._static.dataset.columns.run_id == runRecord.id)

            return query

        if collection is not Select:
            collectionRecord = self._collections.find(collection)
            if collectionRecord is None:
                # Given collection not found in this layer, so we know the
                # query would have no results.
                return None
            if collectionRecord.type is not CollectionType.CALIBRATION and timespan is not None:
                raise TypeError(f"Timespan passed, but '{collection}' is not a calibration collection.")
            query = buildQuery(collectionRecord.type, collectionRecord.id)
            if query is None:
                return None
            return query.finish()
        else:
            sql = []
            for ctype in CollectionType:
                query = buildQuery(ctype, Select)
                if query is not None:
                    sql.append(query.finish())
            if sql:
                return sqlalchemy.sql.union_all(sql)
            else:
                return None

    def getDataId(self, id: int, origin: int) -> DataCoordinate:
        # This query could return multiple rows (one for each tagged collection
        # the dataset is in, plus one for its run collection), and we don't
        # care which of those we get.
        sql = self._dynamic.select().where(
            sqlalchemy.sql.and_(
                self._dynamic.columns.id == id,
                self._dynamic.columns.origin == origin,
                self._dynamic.columns.dataset_type_id == self._id
            )
        ).limit(1)
        row = self._db.connection.execute(sql).fetchone()
        assert row is not None, "Should be guaranteed by caller and foreign key constraints."
        return DataCoordinate.standardize(
            {dimension: row[dimension.name] for dimension in self.datasetType.dimensions.required},
            graph=self.datasetType.dimensions
        )
