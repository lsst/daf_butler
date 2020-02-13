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

__all__ = ("DatasetRegistryStorage", "DatasetTypeExpression")

from typing import Mapping, Iterator, Sequence, Union

import sqlalchemy

from ...core import (
    DatasetType,
    DimensionGraph,
    DimensionUniverse,
)
from .._collectionType import CollectionType
from ..interfaces import CollectionManager, CollectionRecord
from ..wildcards import WildcardExpression, CategorizedWildcard


DatasetTypeExpression = Union[DatasetType, Sequence[DatasetType], WildcardExpression]


class DatasetRegistryStorage:
    """An object managing ``dataset`` and related tables in a `Registry`.

    Parameters
    ----------
    connection : `sqlalchemy.engine.Connection`
        A SQLAlchemy connection object, typically shared with the `Registry`
        that will own the storage instances.
    universe : `DimensionUniverse`
        The set of all dimensions for which storage instances should be
        constructed.
    tables : `dict`
        A dictionary mapping table name to a `sqlalchemy.sql.FromClause`
        representing that table.

    Notes
    -----
    Future changes will convert this concrete class into a polymorphic
    hierarchy modeled after `DimensionRecordStorage`, with many more
    `SqlRegistry` method implementations delegating to it.  Its interface
    may change significantly at the same time.  At present, this functionality
    has been factored out of `SqlRegistry` (with a bit of duplication) to
    allow the initial `QueryBuilder` design and implementation to be more
    forward-looking.
    """
    def __init__(self, connection: sqlalchemy.engine.Connection, universe: DimensionUniverse,
                 tables: Mapping[str, sqlalchemy.sql.FromClause], *,
                 collections: CollectionManager):
        self._connection = connection
        self._universe = universe
        self._collections = collections
        self._datasetTypeTable = tables["dataset_type"]
        self._datasetTypeDimensionsTable = tables["dataset_type_dimensions"]
        self._datasetTable = tables["dataset"]
        self._datasetCollectionTable = tables["dataset_collection"]

    def fetchDatasetTypes(self, datasetType: DatasetTypeExpression = ...) -> Iterator[DatasetType]:
        """Retrieve `DatasetType` instances from the database matching an
        expression.

        Parameters
        ----------
        datasetType : `DatasetType`, `str`, `Like`, sequence thereof, or `...`
            An expression indicating the dataset type(s) to fetch.  See
            `WildcardExpression` for more information.

        Yields
        -------
        datasetType
            A dataset matching the given argument.
        """
        query = sqlalchemy.sql.select([
            self._datasetTypeTable.columns.dataset_type_name,
            self._datasetTypeTable.columns.storage_class,
            self._datasetTypeDimensionsTable.columns.dimension_name,
        ]).select_from(
            self._datasetTypeTable.join(self._datasetTypeDimensionsTable)
        )
        wildcard = CategorizedWildcard.categorize(datasetType)
        if wildcard is not None:
            for item in wildcard.other:
                if isinstance(item, DatasetType):
                    yield item
                else:
                    raise TypeError(f"Object of unsupported type in dataset type expression: '{item}'.")
            where = wildcard.makeWhereExpression(self._datasetTypeTable.columns.dataset_type_name)
            if where is None:
                return
            query = query.where(where)
        # Run the query and group by dataset type name.
        grouped = {}
        for row in self._connection.execute(query).fetchall():
            datasetTypeName, storageClassName, dimensionName = row
            _, dimensionNames = grouped.setdefault(datasetTypeName, (storageClassName, set()))
            dimensionNames.add(dimensionName)
        for datasetTypeName, (storageClassName, dimensionNames) in grouped.items():
            yield DatasetType(datasetTypeName,
                              dimensions=DimensionGraph(self._universe, names=dimensionNames),
                              storageClass=storageClassName)

    def getDatasetSubquery(self, datasetType: DatasetType, *,
                           collections: WildcardExpression,
                           isResult: bool = True,
                           addRank: bool = False) -> sqlalchemy.sql.FromClause:
        """Return a SQL expression that searches for a dataset of a particular
        type in one or more collections.

        Parameters
        ----------
        datasetType : `DatasetType`
            Type of dataset to search for.  Must be a true `DatasetType`;
            call `fetchDatasetTypes` first to expand an expression if desired.
        collections : `str`, `Like`, `list` thereof, or `...`
            An expression describing the collections in which to search for
            the datasets.  See `WildcardExpression` for more information.
        isResult : `bool`, optional
            If `True` (default), include the ``dataset_id`` column in the
            result columns of the query.
        addRank : `bool`, optional
            If `True` (`False` is default), also include a calculated column
            that ranks the collection in which the dataset was found (lower
            is better).  Requires that ``collections`` be a `list` of `str`
            regular strings, so there is a clear search order.  Ignored if
            ``isResult`` is `False`.

        Returns
        -------
        subquery : `sqlalchemy.sql.FromClause`
            Named subquery or table that can be used in the FROM clause of
            a SELECT query.  Has at least columns for all dimensions in
            ``datasetType.dimensions``; may have additional columns depending
            on the values of ``isResult`` and ``addRank``.
        """
        wildcard = CategorizedWildcard.categorize(collections)
        # Always include dimension columns, because that's what we use to
        # join against other tables.
        columns = [self._datasetTable.columns[dimension.name] for dimension in datasetType.dimensions]

        def finishSubquery(select: sqlalchemy.sql.Select, collectionRecord: CollectionRecord):
            if collectionRecord.type is CollectionType.TAGGED:
                collectionColumn = \
                    self._datasetCollectionTable.columns[self._collections.getCollectionForeignKeyName()]
                fromClause = self._datasetTable.join(self._datasetCollectionTable)
            elif collectionRecord.type is CollectionType.RUN:
                collectionColumn = self._datasetTable.columns[self._collections.getRunForeignKeyName()]
                fromClause = self._datasetTable
            else:
                raise NotImplementedError(f"Unrecognized CollectionType: '{collectionRecord.type}'.")
            return select.select_from(
                fromClause
            ).where(
                sqlalchemy.sql.and_(self._datasetTable.columns.dataset_type_name == datasetType.name,
                                    collectionColumn == collectionRecord.key)
            )

        wildcard = CategorizedWildcard.categorize(collections)
        if wildcard is not None and wildcard.other:
            raise TypeError(f"Unsupported objects in collections expression: '{wildcard.other}'.")

        # A list of single-collection queries that we'll UNION together.
        subsubqueries = []

        # Only include dataset_id and the rank of the collection in the given
        # list if caller has indicated that they're going to be actually
        # selecting columns from this subquery in the larger query.
        if isResult:
            columns.append(self._datasetTable.columns.dataset_id)
            if addRank:
                # If we're adding ranks, we need explicit collection names in
                # the list, so we can't use self._collections.query.
                # This is one code path for the rest of this function.
                if wildcard is None or wildcard.patterns:
                    raise TypeError("Cannot rank collections when expression includes wildcards.")
                for n, name in enumerate(wildcard.strings):
                    record = self._collections.find(name)
                    if record is None:
                        # This collection doesn't exist at all.  Arguable
                        # whether we should raise here instead of just taking
                        # advantage of the fact that we know there are no
                        # datasets with this collection, but this preserves the
                        # old behavior.
                        continue
                    subsubqueries.append(
                        finishSubquery(
                            sqlalchemy.sql.select(columns + [sqlalchemy.sql.literal(n).label("rank")]),
                            record
                        )
                    )
                return sqlalchemy.sql.union_all(*subsubqueries).alias(datasetType.name)

        # The code path for not adding ranks is similar, but we can get the
        # records from self._collections.query, and we don't need to add the
        # literal rank column.
        for record in self._collections.query(wildcard):
            subsubqueries.append(finishSubquery(sqlalchemy.sql.select(columns), record))
        return sqlalchemy.sql.union_all(*subsubqueries).alias(datasetType.name)
