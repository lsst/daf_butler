# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from .... import ddl

__all__ = ("CollectionSummaryManager",)

import logging
from collections.abc import Callable, Iterable, Mapping
from typing import Any, Generic, TypeVar

import sqlalchemy

from lsst.utils.iteration import chunk_iterable

from ...._collection_type import CollectionType
from ...._dataset_type import DatasetType
from ...._named import NamedKeyDict, NamedKeyMapping
from ....dimensions import GovernorDimension, addDimensionForeignKey
from ..._caching_context import CachingContext
from ..._collection_summary import CollectionSummary
from ...interfaces import (
    CollectionManager,
    CollectionRecord,
    Database,
    DimensionRecordStorageManager,
    StaticTablesContext,
)
from ...wildcards import CollectionWildcard

_T = TypeVar("_T")


_LOG = logging.getLogger(__name__)


class CollectionSummaryTables(Generic[_T]):
    """Structure that holds the table or table specification objects that
    summarize the contents of collections.

    Parameters
    ----------
    datasetType : _T
        Table [specification] that summarizes which dataset types are in each
        collection.
    dimensions : `NamedKeyMapping`
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
        collections : `CollectionManager`
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
        for dimension in dimensions.universe.governor_dimensions:
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
    collections : `CollectionManager`
        Manager object for the collections in this `Registry`.
    tables : `CollectionSummaryTables`
        Struct containing the tables that hold collection summaries.
    dataset_type_table : `sqlalchemy.schema.Table`
        Table containing dataset type definitions.
    caching_context : `CachingContext`
        Object controlling caching of information returned by managers.
    """

    def __init__(
        self,
        db: Database,
        *,
        collections: CollectionManager,
        tables: CollectionSummaryTables[sqlalchemy.schema.Table],
        dataset_type_table: sqlalchemy.schema.Table,
        caching_context: CachingContext,
    ):
        self._db = db
        self._collections = collections
        self._collectionKeyName = collections.getCollectionForeignKeyName()
        self._tables = tables
        self._dataset_type_table = dataset_type_table
        self._caching_context = caching_context

    def clone(
        self,
        *,
        db: Database,
        collections: CollectionManager,
        caching_context: CachingContext,
    ) -> CollectionSummaryManager:
        """Make an independent copy of this manager instance bound to new
        instances of `Database` and other managers.

        Parameters
        ----------
        db : `Database`
            New `Database` object to use when instantiating the manager.
        collections : `CollectionManager`
            New `CollectionManager` object to use when instantiating the
            manager.
        caching_context : `CachingContext`
            New `CachingContext` object to use when instantiating the manager.

        Returns
        -------
        instance : `CollectionSummaryManager`
            New manager instance with the same configuration as this instance,
            but bound to a new Database object.
        """
        return CollectionSummaryManager(
            db=db,
            collections=collections,
            tables=self._tables,
            dataset_type_table=self._dataset_type_table,
            caching_context=caching_context,
        )

    @classmethod
    def initialize(
        cls,
        db: Database,
        context: StaticTablesContext,
        *,
        collections: CollectionManager,
        dimensions: DimensionRecordStorageManager,
        dataset_type_table: sqlalchemy.schema.Table,
        caching_context: CachingContext,
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
        collections : `CollectionManager`
            Manager object for the collections in this `Registry`.
        dimensions : `DimensionRecordStorageManager`
            Manager object for the dimensions in this `Registry`.
        dataset_type_table : `sqlalchemy.schema.Table`
            Table containing dataset type definitions.
        caching_context : `CachingContext`
            Object controlling caching of information returned by managers.

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
            tables=tables,
            dataset_type_table=dataset_type_table,
            caching_context=caching_context,
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
        dataset_type_ids : `~collections.abc.Iterable` [ `int` ]
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

    def fetch_summaries(
        self,
        collections: Iterable[CollectionRecord],
        dataset_type_names: Iterable[str] | None,
        dataset_type_factory: Callable[[sqlalchemy.engine.RowMapping], DatasetType],
    ) -> Mapping[Any, CollectionSummary]:
        """Fetch collection summaries given their names and dataset types.

        Parameters
        ----------
        collections : `~collections.abc.Iterable` [`CollectionRecord`]
            Collection records to query.
        dataset_type_names : `~collections.abc.Iterable` [`str`]
            Names of dataset types to include into returned summaries. If
            `None` then all dataset types will be included.
        dataset_type_factory : `Callable`
            Method that takes a table row and make `DatasetType` instance out
            of it.

        Returns
        -------
        summaries : `~collections.abc.Mapping` [`Any`, `CollectionSummary`]
            Collection summaries indexed by collection record key. This mapping
            will also contain all nested non-chained collections of the chained
            collections.
        """
        summaries: dict[Any, CollectionSummary] = {}
        # Check what we have in cache first.
        if self._caching_context.collection_summaries is not None:
            summaries, missing_keys = self._caching_context.collection_summaries.find_summaries(
                [record.key for record in collections]
            )
            if not missing_keys:
                return summaries
            else:
                collections = [record for record in collections if record.key in missing_keys]

        # Need to expand all chained collections first.
        non_chains: list[CollectionRecord] = []
        chains: dict[CollectionRecord, list[CollectionRecord]] = {}
        for collection in collections:
            if collection.type is CollectionType.CHAINED:
                children = self._collections.resolve_wildcard(
                    CollectionWildcard.from_names([collection.name]),
                    flatten_chains=True,
                    include_chains=False,
                )
                non_chains += children
                chains[collection] = children
            else:
                non_chains.append(collection)

        _LOG.debug("Fetching summaries for collections %s.", [record.name for record in non_chains])

        # Set up the SQL query we'll use to fetch all of the summary
        # information at once.
        coll_col = self._tables.datasetType.columns[self._collectionKeyName].label(self._collectionKeyName)
        dataset_type_id_col = self._tables.datasetType.columns.dataset_type_id.label("dataset_type_id")
        columns = [coll_col, dataset_type_id_col] + list(self._dataset_type_table.columns)
        fromClause: sqlalchemy.sql.expression.FromClause = self._tables.datasetType.join(
            self._dataset_type_table
        )
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
        sql = sql.where(coll_col.in_([coll.key for coll in non_chains]))
        # For caching we need to fetch complete summaries.
        if self._caching_context.collection_summaries is None:
            if dataset_type_names is not None:
                sql = sql.where(self._dataset_type_table.columns["name"].in_(dataset_type_names))

        # Run the query and construct CollectionSummary objects from the result
        # rows.  This will never include CHAINED collections or collections
        # with no datasets.
        with self._db.query(sql) as sql_result:
            sql_rows = sql_result.mappings().fetchall()
        dataset_type_ids: dict[int, DatasetType] = {}
        for row in sql_rows:
            # Collection key should never be None/NULL; it's what we join on.
            # Extract that and then turn it into a collection name.
            collectionKey = row[self._collectionKeyName]
            # dataset_type_id should also never be None/NULL; it's in the first
            # table we joined.
            dataset_type_id = row["dataset_type_id"]
            if (dataset_type := dataset_type_ids.get(dataset_type_id)) is None:
                dataset_type_ids[dataset_type_id] = dataset_type = dataset_type_factory(row)
            # See if we have a summary already for this collection; if not,
            # make one.
            summary = summaries.get(collectionKey)
            if summary is None:
                summary = CollectionSummary()
                summaries[collectionKey] = summary
            # Update the dimensions with the values in this row that
            # aren't None/NULL (many will be in general, because these
            # enter the query via LEFT OUTER JOIN).
            summary.dataset_types.add(dataset_type)
            for dimension in self._tables.dimensions:
                value = row[dimension.name]
                if value is not None:
                    summary.governors.setdefault(dimension.name, set()).add(value)

        # Add empty summary for any missing collection.
        for collection in non_chains:
            if collection.key not in summaries:
                summaries[collection.key] = CollectionSummary()

        # Merge children into their chains summaries.
        for chain, children in chains.items():
            summaries[chain.key] = CollectionSummary.union(*(summaries[child.key] for child in children))

        if self._caching_context.collection_summaries is not None:
            self._caching_context.collection_summaries.update(summaries)

        return summaries

    def get_collection_ids(self, dataset_type_id: int) -> Iterable[str] | Iterable[int]:
        """Get collection IDs for a given dataset type ID.

        Parameters
        ----------
        dataset_type_id : `int`
            Integer ID for the dataset type.

        Returns
        -------
        collection_ids : `~collections.abc.Iterable`
            Collection IDs (ints or strings) associated with the dataset type.
        """
        query = sqlalchemy.select(self._tables.datasetType.columns[self._collectionKeyName])
        query = query.where(self._tables.datasetType.columns.dataset_type_id == dataset_type_id)
        with self._db.query(query) as result:
            return list(result.scalars())

    def delete_collections(self, dataset_type_id: int, collection_ids: Iterable) -> None:
        """Delete collection from summaries for a given dataset type.

        Parameters
        ----------
        dataset_type_id : `int`
            Integer ID for the dataset type.
        collection_ids : `~collections.abc.Iterable`
            Collection IDs (integer or string) to remove from summaries for
            this dataset type.

        Notes
        -----
        This method should only be called inside the transaction context of
        another operation that selects collection information.
        """
        for collections_chunk in chunk_iterable(collection_ids, 1000):
            to_delete = [
                {"dataset_type_id": dataset_type_id, self._collectionKeyName: collection_id}
                for collection_id in collections_chunk
            ]
            self._db.delete(
                self._tables.datasetType, ["dataset_type_id", self._collectionKeyName], *to_delete
            )
