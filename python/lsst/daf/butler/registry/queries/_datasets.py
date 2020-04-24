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

__all__ = ["DatasetRegistryStorage"]

from typing import Any, Mapping, Iterator, Optional

import sqlalchemy

from ...core import (
    DatasetType,
    DimensionGraph,
    DimensionUniverse,
)
from .._collectionType import CollectionType
from ..interfaces import CollectionManager, CollectionRecord
from ..wildcards import CategorizedWildcard, CollectionSearch, CollectionQuery


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

    def fetchDatasetTypes(self, expression: Any = ...) -> Iterator[DatasetType]:
        """Retrieve `DatasetType` instances from the database matching an
        expression.

        Parameters
        ----------
        expression
            An expression indicating the dataset type(s) to fetch.
            See :ref:`daf_butler_dataset_type_expressions` for more
            information.

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
        wildcard = CategorizedWildcard.fromExpression(expression, coerceUnrecognized=lambda d: d.name)
        if wildcard is not ...:
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
                           collections: Any,
                           isResult: bool = True,
                           addRank: bool = False) -> Optional[sqlalchemy.sql.FromClause]:
        """Return a SQL expression that searches for a dataset of a particular
        type in one or more collections.

        Parameters
        ----------
        datasetType : `DatasetType`
            Type of dataset to search for.  Must be a true `DatasetType`;
            call `fetchDatasetTypes` first to expand an expression if desired.
        collections
            An expression describing the collections to search and any
            restrictions on the dataset types to search within them.
            See :ref:`daf_butler_collection_expressions` for more information.
        isResult : `bool`, optional
            If `True` (default), include the ``dataset_id`` column in the
            result columns of the query.
        addRank : `bool`, optional
            If `True` (`False` is default), also include a calculated column
            that ranks the collection in which the dataset was found (lower
            is better).  Requires that ``collections`` must be an *ordered*
            expression (regular expressions and `...` are not allowed).

        Returns
        -------
        subquery : `sqlalchemy.sql.FromClause` or `None`
            Named subquery or table that can be used in the FROM clause of
            a SELECT query.  Has at least columns for all dimensions in
            ``datasetType.dimensions``; may have additional columns depending
            on the values of ``isResult`` and ``addRank``.  May be `None` if
            it is known that the query would return no results.
        """
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

        # A list of single-collection queries that we'll UNION together.
        subsubqueries = []

        # Only include dataset_id and the rank of the collection in the given
        # list if caller has indicated that they're going to be actually
        # selecting columns from this subquery in the larger query.
        if isResult:
            columns.append(self._datasetTable.columns.dataset_id)
            if addRank:
                collections = CollectionSearch.fromExpression(collections)
                for n, record in enumerate(collections.iter(self._collections, datasetType=datasetType)):
                    subsubqueries.append(
                        finishSubquery(
                            sqlalchemy.sql.select(
                                columns + [sqlalchemy.sql.literal(n).label("rank")]
                            ),
                            record
                        )
                    )
                return sqlalchemy.sql.union_all(*subsubqueries).alias(datasetType.name)

        # The code path for not adding ranks is similar, but we don't need to
        # add the literal rank column, and we transform the collections
        # expression into a CollectionQuery instead of a CollectionSearch.
        collections = CollectionQuery.fromExpression(collections)
        for record in collections.iter(self._collections, datasetType=datasetType):
            subsubqueries.append(finishSubquery(sqlalchemy.sql.select(columns), record))
        if not subsubqueries:
            return None
        return sqlalchemy.sql.union_all(*subsubqueries).alias(datasetType.name)
