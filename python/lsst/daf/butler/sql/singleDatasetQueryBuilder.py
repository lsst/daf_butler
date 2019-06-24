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

__all__ = ("SingleDatasetQueryBuilder",)

import logging
from dataclasses import dataclass
from typing import Set
from sqlalchemy.sql import select, and_, functions, case
from ..core import DimensionJoin
from .queryBuilder import QueryBuilder

_LOG = logging.getLogger(__name__)


@dataclass
class RelateCandidate:
    """Helper struct for `SingleDatasetQueryBuilder.relateDimensions`.
    """

    join: DimensionJoin
    """The dimension join being considered as a way to relate the dataset's
    dimensions to the given dimensions.
    """

    linksSharedWithDataset: Set[str]
    """The set of dimension links this join shares with the dataset's
    dimensions."""

    linksSharedWithOther: Set[str]
    """The set of dimension links this join shares with the given dimensions.
    """

    @property
    def score(self):
        """A combination of the number of links shared used as a preliminary
        comparison between candidates.
        """
        return len(self.linksSharedWithDataset)*len(self.linksSharedWithOther)

    def isBetterThan(self, other):
        """Return `True` if this candidate is clearly better than the other
        (`False` is returned for both ties and when `other` is clearly better).
        """
        if other is None:
            return True
        if self.score > other.score:
            return True
        elif self.score == other.score:
            return self.join in other.join.summarizes
        return False


class SingleDatasetQueryBuilder(QueryBuilder):
    """Specialization of `QueryBuilder` that includes a single join
    to the Dataset table corresponding to a single `DatasetType`.

    Most users should call `fromCollections` to construct an instance of this
    class rather than invoking the constructor directly.

    Parameters
    ----------
    registry : `SqlRegistry`
        Registry instance the query is being run against.
    datasetType : `DatasetType`
        `DatasetType` of the datasets this query searches for.
    selectableForDataset : `sqlalchemy.sql.expression.FromClause`
        SQLAlchemy object representing the Dataset table or a subquery
        equivalent.
    fromClause : `sqlalchemy.sql.expression.FromClause`, optional
        Initial FROM clause for the query.
    whereClause : SQLAlchemy boolean expression, optional
        Expression to use as the initial WHERE clause.
    addResultColumns : `bool`
        If `True` (default), add result columns to ``self.resultColumns``
        for the dataset ID and dimension links used to identify this
        `DatasetType.
    """

    def __init__(self, registry, *, datasetType, selectableForDataset, fromClause=None, whereClause=None,
                 addResultColumns=True):
        super().__init__(registry, fromClause=fromClause, whereClause=whereClause)
        self._datasetType = datasetType
        self._selectableForDataset = selectableForDataset
        if addResultColumns:
            self.selectDatasetId()
            for link in datasetType.dimensions.links():
                self.resultColumns.addDimensionLink(selectableForDataset, link)

    @classmethod
    def fromSingleCollection(cls, registry, datasetType, collection, addResultColumns=True):
        """Construct a builder that searches a single collection for datasets
        of a single dataset type.

        Parameters
        ----------
        registry : `SqlRegistry`
            Registry instance the query is being run against.
        datasetType : `DatasetType`
            `DatasetType` of the datasets this query searches for.
        collection : `str`
            Name of the collection to search in.
        addResultColumns : `bool`
            If `True` (default), add result columns to ``self.resultColumns``
            for the dataset ID and dimension links used to identify this
            `DatasetType.

        Returns
        -------
        builder : `SingleDatasetQueryBuilder`
            New query builder instance initialized with a
            `~QueryBuilder.fromClause` that either directly includes the
            dataset table or includes a subquery equivalent.

        Notes
        -----
        If there is only one collection, then there is a guarantee that
        data IDs are all unique (by data ID we mean the combination of all link
        values relevant for this dataset); in that case the dataset query can
        be written as:

            SELECT
                dataset.dataset_id AS dataset_id,
                dataset.link1 AS link1,
                ...
                dataset.link1 AS linkN
            FROM dataset JOIN dataset_collection
                ON dataset.dataset_id = dataset_collection.dataset_id
            WHERE dataset.dataset_type_name = :dsType_name
                AND dataset_collection.collection = :collection_name
        """
        datasetTable = registry._schema.tables["dataset"]
        datasetCollectionTable = registry._schema.tables["dataset_collection"]
        fromClause = datasetTable.join(
            datasetCollectionTable,
            datasetTable.columns.dataset_id == datasetCollectionTable.columns.dataset_id
        )
        whereClause = and_(datasetTable.columns.dataset_type_name == datasetType.name,
                           datasetCollectionTable.columns.collection == collection)
        return cls(registry, fromClause=fromClause, whereClause=whereClause, datasetType=datasetType,
                   selectableForDataset=datasetTable, addResultColumns=addResultColumns)

    @classmethod
    def fromCollections(cls, registry, datasetType, collections, addResultColumns=True):
        """Construct a builder that searches a multiple collections for
        datasets single dataset type.

        Parameters
        ----------
        registry : `SqlRegistry`
            Registry instance the query is being run against.
        datasetType : `DatasetType`
            `DatasetType` of the datasets this query searches for.
        collections : `list` of `str`
            List of collections to search, ordered from highest-priority to
            lowest.
        addResultColumns : `bool`
            If `True` (default), add result columns to ``self.resultColumns``
            for the dataset ID and dimension links used to identify this
            `DatasetType.

        Returns
        -------
        builder : `SingleDatasetQueryBuilder`
            New query builder instance initialized with a
            `~QueryBuilder.fromClause` that either directly includes the
            dataset table or includes a subquery equivalent.

        Notes
        -----
        If ``len(collections)==1``, this method simply calls
        `fromSingleCollection`.

        If there are multiple collections, then there can be multiple matching
        Datasets for the same DataId. In that case we need only one Dataset
        record, which comes from earliest collection (in the user-provided
        order). Here things become complicated; we have to:
        - replace collection names with their order in input list
        - select all combinations of rows from dataset and dataset_collection
          which match collection names and dataset type name
        - from those only select rows with lowest collection position if
          there are multiple collections for the same DataId

        Replacing collection names with positions is easy:

            SELECT dataset_id,
                CASE collection
                    WHEN 'collection1' THEN 0
                    WHEN 'collection2' THEN 1
                    ...
                END AS collorder
            FROM dataset_collection

        Combined query will look like (CASE ... END is as above):

            SELECT dataset.dataset_id AS dataset_id,
                CASE dataset_collection.collection ... END AS collorder,
                dataset.link1,
                ...
                dataset.linkN
            FROM dataset JOIN dataset_collection
                ON dataset.dataset_id = dataset_collection.dataset_id
            WHERE dataset.dataset_type_name = <dsType.name>
                AND dataset_collection.collection IN (<collections>)

        Filtering is complicated; it would be simpler to use Common Table
        Expressions (WITH clause) but not all databases support CTEs, so we
        will have to do with the repeating sub-queries. We use GROUP BY for
        the data ID (link columns) and MIN(collorder) to find ``collorder``
        for a particular DataId, then join it with previous combined selection:

            SELECT
                DS.dataset_id AS dataset_id,
                DS.link1 AS link1,
                ...
                DS.linkN AS linkN
            FROM (
                SELECT dataset.dataset_id AS dataset_id,
                    CASE ... END AS collorder,
                    dataset.link1,
                    ...
                    dataset.linkN
                FROM dataset JOIN dataset_collection
                    ON dataset.dataset_id = dataset_collection.dataset_id
                WHERE dataset.dataset_type_name = <dsType.name>
                    AND dataset_collection.collection IN (<collections>)
                ) DS
            INNER JOIN (
                SELECT
                    MIN(CASE ... END AS) collorder,
                    dataset.link1,
                    ...
                    dataset.linkN
                FROM dataset JOIN dataset_collection
                    ON dataset.dataset_id = dataset_collection.dataset_id
                WHERE dataset.dataset_type_name = <dsType.name>
                   AND dataset_collection.collection IN (<collections>)
                GROUP BY (
                    dataset.link1,
                    ...
                    dataset.linkN
                    )
                ) DSG
            ON (DS.colpos = DSG.colpos
                    AND
                DS.link1 = DSG.link1
                    AND
                ...
                    AND
                DS.linkN = DSG.linkN)
        """
        if len(collections) == 1:
            return cls.fromSingleCollection(registry, datasetType, collections[0],
                                            addResultColumns=addResultColumns)

        # helper method
        def _columns(selectable, names):
            """Return list of columns for given column names"""
            return [selectable.columns[name].label(name) for name in names]

        datasetTable = registry._schema.tables["dataset"]
        datasetCollectionTable = registry._schema.tables["dataset_collection"]

        # full set of link names for this DatasetType
        links = list(datasetType.dimensions.links())

        # Starting point for both subqueries below: a join of dataset to
        # dataset_collection
        subJoin = datasetTable.join(
            datasetCollectionTable,
            datasetTable.columns.dataset_id == datasetCollectionTable.columns.dataset_id
        )
        subWhere = and_(datasetTable.columns.dataset_type_name == datasetType.name,
                        datasetCollectionTable.columns.collection.in_(collections))

        # CASE clause that transforms collection name to position in the given
        # list of collections
        collorder = case([
            (datasetCollectionTable.columns.collection == coll, pos) for pos, coll in enumerate(collections)
        ])

        # first GROUP BY sub-query, find minimum `collorder` for each DataId
        columns = [functions.min(collorder).label("collorder")] + _columns(datasetTable, links)
        groupSubq = select(columns).select_from(subJoin).where(subWhere)
        groupSubq = groupSubq.group_by(*links)
        groupSubq = groupSubq.alias("sub1" + datasetType.name)

        # next combined sub-query
        columns = [collorder.label("collorder")] + _columns(datasetTable, ["dataset_id"] + links)
        combined = select(columns).select_from(subJoin).where(subWhere)
        combined = combined.alias("sub2" + datasetType.name)

        # now join these two
        joinsOn = [groupSubq.columns.collorder == combined.columns.collorder] + \
                  [groupSubq.columns[colName] == combined.columns[colName] for colName in links]

        return cls(registry, fromClause=combined.join(groupSubq, and_(*joinsOn)),
                   datasetType=datasetType, selectableForDataset=combined, addResultColumns=addResultColumns)

    @property
    def datasetType(self):
        """The dataset type this query searches for (`DatasetType`).
        """
        return self._datasetType

    def relateDimensions(self, otherDimensions, addResultColumns=True):
        """Add the dimension tables/views necessary to map the dimensions
        of this query's `DatasetType` to another set of dimensions.

        Parameters
        ----------
        otherDimensions : `DimensionGraph` or `DimensionSet`
            The dimensions we need to relate the dataset type to.  One or more
            `DimensionJoin` tables/views will be added to the query for each
            `Dimension` in ``self.datasetType.dimensions`` that is not
            in ``otherDimensions``.
        addResultColumns : `bool`
            If `True` (default), add result columns to ``self.resultColumns``
            for the dataset ID and dimension links used to identify this
            `DatasetType.

        Returns
        -------
        newLinks : `set` of `str`
            The names of additional dimension link columns provided by the
            subquery via the added joins.  This is a subset of
            ``otherDimensions.links()`` and disjoint from
            ``self.datasetType.dimensions.links()``.

        Notes
        -----
        This method can currently only handle a single level of indirection -
        for any "missing" dimension (one that is in
        ``self.datasetType.dimensions`` but not in ``otherDimensions``), there
        must be a single `DimensionJoin` that relates that dimension to one or
        more dimensions in ``otherDimensions``.
        """
        allDimensions = self.datasetType.dimensions.union(otherDimensions)
        missingLinks = set(self.datasetType.dimensions.links() - otherDimensions.links())
        newLinks = set()
        while missingLinks:
            best = None
            for join in allDimensions.joins():
                if join.asNeeded or missingLinks.isdisjoint(join.links()):
                    continue
                candidate = RelateCandidate(
                    join=join,
                    linksSharedWithDataset=self.datasetType.dimensions.links() & join.links(),
                    linksSharedWithOther=otherDimensions.links() & join.links(),
                )
                if candidate.isBetterThan(best):
                    best = candidate
            if best is None:
                raise ValueError(f"No join found relating links {missingLinks} to {otherDimensions.links()}")
            self.joinDimensionElement(best.join)
            missingLinks -= best.join.links()
            newLinks.update(best.join.links())
        newLinks -= self.datasetType.dimensions.links()
        if addResultColumns:
            for link in newLinks:
                self.selectDimensionLink(link)
        return newLinks

    def selectDatasetId(self):
        """Add the ``dataset_id`` column to the SELECT clause of the query.
        """
        self.resultColumns.addDatasetId(self._selectableForDataset, self.datasetType)

    def findSelectableForLink(self, link):
        # Docstring inherited from QueryBuilder.findSelectableForLink
        result = super().findSelectableForLink(link)
        if result is None and link in self.datasetType.dimensions.links():
            result = self._selectableForDataset
        return result

    def findSelectableByName(self, name):
        # Docstring inherited from QueryBuilder.findSelectableByName
        result = super().findSelectableByName(name)
        if result is None and (name == self.datasetType.name or name == "Dataset"):
            result = self._selectableForDataset
        return result

    def convertResultRow(self, managed, *, expandDataId=True):
        """Convert a result row for this query to a `DatasetRef`.

        Parameters
        ----------
        managed : `ResultsColumnsManager.ManagedRow`
            Intermediate result row object to convert.
        expandDataId : `bool`
            If `True` (default), query the registry again to fully populate
            the `DataId` associated with the returned `DatasetRef`.

        Returns
        -------
        ref : `DatasetRef`
            Reference to a dataset identified by the query.
        """
        return managed.makeDatasetRef(self.datasetType, expandDataId=expandDataId)
