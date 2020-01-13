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

__all__ = ("DatasetRegistryStorage", "Like", "DatasetTypeExpression", "CollectionsExpression")

from dataclasses import dataclass
from typing import Mapping, Optional, Sequence, List, Union

from sqlalchemy.sql import FromClause, select, case, and_, or_, ColumnElement
from sqlalchemy.engine import Connection

from ...core import (
    DatasetType,
    ExpandedDataCoordinate,
    DimensionGraph,
    DimensionUniverse,
)


@dataclass(frozen=True)
class Like:
    """Simple wrapper around a string pattern used to indicate that a string is
    a pattern to be used with the SQL ``LIKE`` operator rather than a complete
    name.
    """

    pattern: str
    """The string pattern, in SQL ``LIKE`` syntax.
    """


DatasetTypeExpression = Union[DatasetType, str, Like, type(...)]
"""Type annotation alias for the types accepted when querying for a dataset
type.

Ellipsis (``...``) is used as a full wildcard, indicating that any
`DatasetType` will be matched.
"""

CollectionsExpression = Union[Sequence[Union[str, Like]], type(...)]
"""Type annotation alias for the types accepted to describe the collections to
be searched for a dataset.

Ellipsis (``...``) is used as a full wildcard, indicating that all
collections will be searched.
"""


def makeCollectionsWhereExpression(column: ColumnElement,
                                   collections: CollectionsExpression) -> Optional[ColumnElement]:
    """Construct a boolean SQL expression corresponding to a Python expression
    for the collections to search for one or more datasets.

    Parameters
    ----------
    column : `sqlalchemy.sql.ColumnElement`
        The "collection" name column from a dataset subquery or table.
    collections : `list` of `str` or `Like`, or ``...``
        An expression indicating the collections to be searched.  This may
        be a sequence containing complete collection names (`str` values),
        wildcard expressions (`Like` instances) or the special value ``...``,
        indicating all collections.

    Returns
    -------
    where : `sqlalchemy.sql.ColumnElement` or `None`
        A boolean SQL expression object, or `None` if all collections are to
        be searched and hence there is no WHERE expression for the given Python
        expression (or, more precisely, the WHERE expression is the literal
        "true", but we don't want to pollute our SQL queries with those when
        we can avoid it).
    """
    if collections is ...:
        return None
    terms = []
    equalities = []
    for collection in collections:
        if isinstance(collection, Like):
            terms.append(column.like(collection.pattern))
        else:
            equalities.append(collection)
    if len(equalities) == 1:
        terms.append(column == equalities[0])
    if len(equalities) > 1:
        terms.append(column.in_(equalities))
    return or_(*terms)


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
    def __init__(self, connection: Connection, universe: DimensionUniverse,
                 tables: Mapping[str, FromClause]):
        self._connection = connection
        self._universe = universe
        self._datasetTypeTable = tables["dataset_type"]
        self._datasetTypeDimensionsTable = tables["dataset_type_dimensions"]
        self._datasetTable = tables["dataset"]
        self._datasetCollectionTable = tables["dataset_collection"]

    def fetchDatasetTypes(self, datasetType: DatasetTypeExpression = ..., *,
                          collections: CollectionsExpression = ...,
                          dataId: Optional[ExpandedDataCoordinate] = None) -> List[DatasetType]:
        """Retrieve `DatasetType` instances from the database matching an
        expression.

        Parameters
        ----------
        datasetType : `str`, `Like`, `DatasetType`, or ``...``
            An expression indicating the dataset type(s) to fetch.  If this is
            a true `DatasetType` instance, it will be returned directly without
            querying the database.  If this is a `str`, the `DatasetType`
            matching that name will be returned if it exists.  If it is a
            `Like` expression, dataset types whose name match the expression
            will be returned.  The special value ``...`` fetches all dataset
            types.  If no dataset types match, an empty `list` is returned.
        collections : sequence of `str` or `Like`, or ``...``
            An expression indicating collections that *may* be used to limit
            the dataset types returned to only those that might have datasets
            in these collections.  This is intended as an optimization for
            higher-level functionality; it may simply be ignored, and cannot
            be relied upon to filter the returned dataset types.
        dataId : `ExpandedDataCoordinate`, optional
            A data ID that *may* be used to limit the dataset types returned
            to only those with datasets matching the given data ID.  This is
            intended as an optimization for higher-level functionality; it may
            simply be ignored, and cannot be relied upon to filter the returned
            dataset types.

        Returns
        -------
        datasetTypes : `list` of `DatasetType`
            All datasets in the registry matching the given arguments.
        """
        if isinstance(datasetType, DatasetType):
            # This *could* return an empty list if we could determine
            # efficiently that there are no entries of this dataset type
            # that match the given data ID or collections, but we are not
            # required to do that filtering.
            return [datasetType]
        whereTerms = []
        if datasetType is ...:
            # "..." means no restriction on the dataset types; get all
            # of them.
            pass
        elif isinstance(datasetType, str):
            whereTerms.append(self._datasetTypeTable.columns.dataset_type_name == datasetType)
        elif isinstance(datasetType, Like):
            whereTerms.append(self._datasetTypeTable.columns.dataset_type_name.like(datasetType.pattern))
        else:
            raise TypeError(f"Unexpected dataset type expression '{datasetType}' in query.")
        query = select([
            self._datasetTypeTable.columns.dataset_type_name,
            self._datasetTypeTable.columns.storage_class,
            self._datasetTypeDimensionsTable.columns.dimension_name,
        ]).select_from(
            self._datasetTypeTable.join(self._datasetTypeDimensionsTable)
        )
        if whereTerms:
            query = query.where(*whereTerms)
        # Collections and dataId arguments are currently ignored; they are
        # provided so future code *may* restrict the list of returned dataset
        # types, but are not required to be used.
        grouped = {}
        for row in self._connection.execute(query).fetchall():
            datasetTypeName, storageClassName, dimensionName = row
            _, dimensionNames = grouped.setdefault(datasetTypeName, (storageClassName, set()))
            dimensionNames.add(dimensionName)
        return [DatasetType(datasetTypeName,
                            dimensions=DimensionGraph(self._universe, names=dimensionNames),
                            storageClass=storageClassName)
                for datasetTypeName, (storageClassName, dimensionNames) in grouped.items()]

    def getDatasetSubquery(self, datasetType: DatasetType, *,
                           collections: CollectionsExpression,
                           dataId: Optional[ExpandedDataCoordinate] = None,
                           isResult: bool = True,
                           addRank: bool = False) -> FromClause:
        """Return a SQL expression that searches for a dataset of a particular
        type in one or more collections.

        Parameters
        ----------
        datasetType : `DatasetType`
            Type of dataset to search for.  Must be a true `DatasetType`;
            call `fetchDatasetTypes` first to expand an expression if desired.
        collections : sequence of `str` or `Like`, or ``...``
            An expression describing the collections in which to search for
            the datasets.  ``...`` indicates that all collections should be
            searched.  Returned datasets are guaranteed to be from one of the
            given collections (unlike the behavior of the same argument in
            `fetchDatasetTypes`).
        dataId : `ExpandedDataCoordinate`, optional
            A data ID that *may* be used to limit the datasets returned
            to only those matching the given data ID.  This is intended as an
            optimization for higher-level functionality; it may simply be
            ignored, and cannot be relied upon to filter the returned dataset
            types.
        isResult : `bool`, optional
            If `True` (default), include the ``dataset_id`` column in the
            result columns of the query.
        addRank : `bool`, optional
            If `True` (`False` is default), also include a calculated column
            that ranks the collection in which the dataset was found (lower
            is better).  Requires that all entries in ``collections`` be
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
        # Always include dimension columns, because that's what we use to
        # join against other tables.
        columns = [self._datasetTable.columns[dimension.name] for dimension in datasetType.dimensions]
        # Only include dataset_id and the rank of the collection in the given
        # list if caller has indicated that they're going to be actually
        # selecting columns from this subquery in the larger query.
        if isResult:
            columns.append(self._datasetTable.columns.dataset_id)
            if addRank:
                if collections is ...:
                    raise TypeError("Cannot rank collections when no collections are provided.")
                ranks = {}
                for n, collection in enumerate(collections):
                    if isinstance(collection, Like):
                        raise TypeError(
                            f"Cannot rank collections that include LIKE pattern '{collection.pattern}'."
                        )
                    ranks[collection] = n
                columns.append(
                    case(
                        ranks,
                        value=self._datasetCollectionTable.columns.collection
                    ).label("rank")
                )
        whereTerms = [self._datasetTable.columns.dataset_type_name == datasetType.name]
        collectionsTerm = makeCollectionsWhereExpression(self._datasetCollectionTable.columns.collection,
                                                         collections)
        if collectionsTerm is not None:
            whereTerms.append(collectionsTerm)
        return select(
            columns
        ).select_from(
            self._datasetTable.join(self._datasetCollectionTable)
        ).where(
            and_(*whereTerms)
        ).alias(datasetType.name)
