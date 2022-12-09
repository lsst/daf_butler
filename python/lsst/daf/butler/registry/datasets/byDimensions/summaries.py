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

__all__ = ("CollectionSummaryManager",)

from collections.abc import Callable, Iterable
from typing import Any, Generic, TypeVar

import sqlalchemy

from ....core import (
    DatasetType,
    GovernorDimension,
    NamedKeyDict,
    NamedKeyMapping,
    addDimensionForeignKey,
    ddl,
)
from ..._collection_summary import CollectionSummary
from ..._collectionType import CollectionType
from ...interfaces import (
    ChainedCollectionRecord,
    CollectionManager,
    CollectionRecord,
    Database,
    DimensionRecordStorageManager,
    StaticTablesContext,
)

_T = TypeVar("_T")


class CollectionSummaryTables(Generic[_T]):
    """Structure that holds the table or table specification objects that
    summarize the contents of collections.

    Parameters
    ----------
    datasetType
        Table [specification] that summarizes which dataset types are in each
        collection.
    dimensions
        Mapping of table [specifications] that summarize which governor
        dimension values are present in the data IDs of each collection.
    """

    def __init__(
        self,
        datasetType: _T,
        dimensions: NamedKeyMapping[GovernorDimension, _T],
    ):
        self.datasetType = datasetType
        self.dimensions = dimensions

    @classmethod
    def makeTableSpecs(
        cls,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
    ) -> CollectionSummaryTables[ddl.TableSpec]:
        """Create specifications for all summary tables.

        Parameters
        ----------
        collections: `CollectionManager`
            Manager object for the collections in this `Registry`.
        dimensions : `DimensionRecordStorageManager`
            Manager object for the dimensions in this `Registry`.

        Returns
        -------
        tables : `CollectionSummaryTables` [ `ddl.TableSpec` ]
            Structure containing table specifications.
        """
        # Spec for collection_summary_dataset_type.
        datasetTypeTableSpec = ddl.TableSpec(fields=[])
        collections.addCollectionForeignKey(datasetTypeTableSpec, primaryKey=True, onDelete="CASCADE")
        datasetTypeTableSpec.fields.add(
            ddl.FieldSpec("dataset_type_id", dtype=sqlalchemy.BigInteger, primaryKey=True)
        )
        datasetTypeTableSpec.foreignKeys.append(
            ddl.ForeignKeySpec(
                "dataset_type", source=("dataset_type_id",), target=("id",), onDelete="CASCADE"
            )
        )
        # Specs for collection_summary_<dimension>.
        dimensionTableSpecs = NamedKeyDict[GovernorDimension, ddl.TableSpec]()
        for dimension in dimensions.universe.getGovernorDimensions():
            tableSpec = ddl.TableSpec(fields=[])
            collections.addCollectionForeignKey(tableSpec, primaryKey=True, onDelete="CASCADE")
            addDimensionForeignKey(tableSpec, dimension, primaryKey=True)
            dimensionTableSpecs[dimension] = tableSpec
        return CollectionSummaryTables(
            datasetType=datasetTypeTableSpec,
            dimensions=dimensionTableSpecs.freeze(),
        )


class CollectionSummaryManager:
    """Object manages the summaries of what dataset types and governor
    dimension values are present in a collection.

    Parameters
    ----------
    db : `Database`
        Interface to the underlying database engine and namespace.
    collections: `CollectionManager`
        Manager object for the collections in this `Registry`.
    dimensions : `DimensionRecordStorageManager`
        Manager object for the dimensions in this `Registry`.
    tables : `CollectionSummaryTables`
        Struct containing the tables that hold collection summaries.
    """

    def __init__(
        self,
        db: Database,
        *,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
        tables: CollectionSummaryTables[sqlalchemy.sql.Table],
    ):
        self._db = db
        self._collections = collections
        self._collectionKeyName = collections.getCollectionForeignKeyName()
        self._dimensions = dimensions
        self._tables = tables
        self._cache: dict[Any, CollectionSummary] = {}

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
    ) -> CollectionSummaryManager:
        """Create all summary tables (or check that they have been created),
        returning an object to manage them.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present.
        collections: `CollectionManager`
            Manager object for the collections in this `Registry`.
        dimensions : `DimensionRecordStorageManager`
            Manager object for the dimensions in this `Registry`.

        Returns
        -------
        manager : `CollectionSummaryManager`
            New manager object for collection summaries.
        """
        specs = CollectionSummaryTables.makeTableSpecs(collections, dimensions)
        tables = CollectionSummaryTables(
            datasetType=context.addTable("collection_summary_dataset_type", specs.datasetType),
            dimensions=NamedKeyDict[GovernorDimension, sqlalchemy.schema.Table](
                {
                    dimension: context.addTable(f"collection_summary_{dimension.name}", spec)
                    for dimension, spec in specs.dimensions.items()
                }
            ).freeze(),
        )
        return cls(
            db=db,
            collections=collections,
            dimensions=dimensions,
            tables=tables,
        )

    def update(
        self,
        collection: CollectionRecord,
        dataset_type_ids: Iterable[int],
        summary: CollectionSummary,
    ) -> None:
        """Update the summary tables to associate the given collection with
        a dataset type and governor dimension values.

        Parameters
        ----------
        collection : `CollectionRecord`
            Collection whose summary should be updated.
        dataset_type_ids : `Iterable` [ `int` ]
            Integer IDs for the dataset types to associate with this
            collection.
        summary : `CollectionSummary`
            Summary to store.  Dataset types must correspond to
            ``dataset_type_ids``.

        Notes
        -----
        This method should only be called inside the transaction context of
        another operation that inserts or associates datasets.
        """
        self._db.ensure(
            self._tables.datasetType,
            *[
                {
                    "dataset_type_id": dataset_type_id,
                    self._collectionKeyName: collection.key,
                }
                for dataset_type_id in dataset_type_ids
            ],
        )
        for dimension, values in summary.governors.items():
            if values:
                self._db.ensure(
                    self._tables.dimensions[dimension],
                    *[{self._collectionKeyName: collection.key, dimension: v} for v in values],
                )
        # Update the in-memory cache, too.  These changes will remain even if
        # the database inserts above are rolled back by some later exception in
        # the same transaction, but that's okay: we never promise that a
        # CollectionSummary has _just_ the dataset types and governor dimension
        # values that are actually present, only that it is guaranteed to
        # contain any dataset types or governor dimension values that _may_ be
        # present.
        # That guarantee (and the possibility of rollbacks) means we can't get
        # away with checking the cache before we try the database inserts,
        # however; if someone had attempted to insert datasets of some dataset
        # type previously, and that rolled back, and we're now trying to insert
        # some more datasets of that same type, it would not be okay to skip
        # the DB summary table insertions because we found entries in the
        # in-memory cache.
        self.get(collection).update(summary)

    def refresh(self, get_dataset_type: Callable[[int], DatasetType]) -> None:
        """Load all collection summary information from the database.

        Parameters
        ----------
        get_dataset_type : `Callable`
            Function that takes an `int` dataset_type_id value and returns a
            `DatasetType` instance.
        """
        # Set up the SQL query we'll use to fetch all of the summary
        # information at once.
        columns = [
            self._tables.datasetType.columns[self._collectionKeyName].label(self._collectionKeyName),
            self._tables.datasetType.columns.dataset_type_id.label("dataset_type_id"),
        ]
        fromClause = self._tables.datasetType
        for dimension, table in self._tables.dimensions.items():
            columns.append(table.columns[dimension.name].label(dimension.name))
            fromClause = fromClause.join(
                table,
                onclause=(
                    self._tables.datasetType.columns[self._collectionKeyName]
                    == table.columns[self._collectionKeyName]
                ),
                isouter=True,
            )
        sql = sqlalchemy.sql.select(*columns).select_from(fromClause)
        # Run the query and construct CollectionSummary objects from the result
        # rows.  This will never include CHAINED collections or collections
        # with no datasets.
        summaries: dict[Any, CollectionSummary] = {}
        with self._db.query(sql) as sql_result:
            sql_rows = sql_result.mappings().fetchall()
        for row in sql_rows:
            # Collection key should never be None/NULL; it's what we join on.
            # Extract that and then turn it into a collection name.
            collectionKey = row[self._collectionKeyName]
            # dataset_type_id should also never be None/NULL; it's in the first
            # table we joined.
            datasetType = get_dataset_type(row["dataset_type_id"])
            # See if we have a summary already for this collection; if not,
            # make one.
            summary = summaries.get(collectionKey)
            if summary is None:
                summary = CollectionSummary()
                summaries[collectionKey] = summary
            # Update the dimensions with the values in this row that aren't
            # None/NULL (many will be in general, because these enter the query
            # via LEFT OUTER JOIN).
            summary.dataset_types.add(datasetType)
            for dimension in self._tables.dimensions:
                value = row[dimension.name]
                if value is not None:
                    summary.governors.setdefault(dimension.name, set()).add(value)
        self._cache = summaries

    def get(self, collection: CollectionRecord) -> CollectionSummary:
        """Return a summary for the given collection.

        Parameters
        ----------
        collection : `CollectionRecord`
            Record describing the collection for which a summary is to be
            retrieved.

        Returns
        -------
        summary : `CollectionSummary`
            Summary of the dataset types and governor dimension values in
            this collection.
        """
        summary = self._cache.get(collection.key)
        if summary is None:
            # When we load the summary information from the database, we don't
            # create summaries for CHAINED collections; those are created here
            # as needed, and *never* cached - we have no good way to update
            # those summaries when some a new dataset is added to a child
            # colletion.
            if collection.type is CollectionType.CHAINED:
                assert isinstance(collection, ChainedCollectionRecord)
                child_summaries = [self.get(self._collections.find(child)) for child in collection.children]
                if child_summaries:
                    summary = CollectionSummary.union(*child_summaries)
                else:
                    summary = CollectionSummary()
            else:
                # Either this collection doesn't have any datasets yet, or the
                # only datasets it has were created by some other process since
                # the last call to refresh.  We assume the former; the user is
                # responsible for calling refresh if they want to read
                # concurrently-written things.  We do remember this in the
                # cache.
                summary = CollectionSummary()
                self._cache[collection.key] = summary
        return summary
