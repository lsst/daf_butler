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
from sqlalchemy.sql import select, and_, func, functions, case
from ..core import DimensionJoin
from .queryBuilder import QueryBuilder

_LOG = logging.getLogger(__name__)


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
        order). Here things become complicated. In previous version of this
        code we used a MIN(collection_order) with GROUP BY the dataset link
        values and then joined that with another sub-query to select dataset
        rows. In the new implementation we use window function to avoid
        self-joining. The sub-query with window function looks like

            SELECT ds.dataset_id, ds.link1, ds.link2, ...,
                dc.collection,
                MIN(<coll_order>) OVER (PARTITION BY ds.link1, ds.link2, ...)
            FROM dataset ds
                JOIN dataset_collection dc ON ds.dataset_id = dc.dataset_id
            WHERE ds.dataset_type_name = <dsType.name>
                AND dc.collection IN (<collections>)

        where <coll_order> is some value which reflects order of the
        collection in the collection list. The expression that gives that
        value can be a CASE like this:

                CASE dc.collection
                    WHEN 'collection1' THEN '000collection1'
                    WHEN 'collection2' THEN '001collection2'
                    ...
                END

        This special format when resulting value is the name of the collection
        itself prefixed with its order value has the advantage that prefix can
        be stripped to obtain the name of the collection again. Above SELECT
        can be written now as:

            SELECT ds.dataset_id dataset_id,
                ds.link1 link1, ds.link2 link2, ...,
                dc.collection collection,
                SUBSTR(
                    MIN(CASE dc.collection
                        WHEN 'collection1' THEN '000collection1'
                        WHEN 'collection2' THEN '001collection2'
                        ...
                    END) OVER (PARTITION BY ds.link1, ds.link2, ...),
                    5) first_collection
            FROM dataset ds
                JOIN dataset_collection dc ON ds.dataset_id = dc.dataset_id
            WHERE ds.dataset_type_name = <dsType.name>
                AND dc.collection IN (<collections>)

        Now we need to filter rows for wich ``collection`` is the same as
        ``first_collection``, for that we need to run this as a subquery:

            SELECT dataset_id, link1, link2, ...
            FROM (<above SELECT>)
            WHERE collection = first_collection;

        and the same can be written using Common Table Expression (CTE):

            WITH subq AS (<above SELECT>)
            SELECT subq.dataset_id, subq.link1, subq.link2, ...
            WHERE subq.collection = subq.first_collection;

        CTE may be preferred for reasons of query analysis and optimization
        but we cannot use it now due to the way our code is structured (the
        complete query is built seprately from pieces defined here). If CTE
        is needed then some restructuring of the code should be done.
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

        # Starting point is a join of dataset to dataset_collection
        subJoin = datasetTable.join(
            datasetCollectionTable,
            datasetTable.columns.dataset_id == datasetCollectionTable.columns.dataset_id
        )
        subWhere = and_(datasetTable.columns.dataset_type_name == datasetType.name,
                        datasetCollectionTable.columns.collection.in_(collections))

        # CASE clause that transforms collection name to prefixed name, plus
        # MIN window function, plus SUBSTR
        nDigits = 3
        collFmt = f"{{:0{nDigits}d}}{{}}"
        collCase = case({coll: collFmt.format(pos, coll) for pos, coll in enumerate(collections)},
                        value=datasetCollectionTable.columns.collection)
        collMin = functions.min(collCase).over(partition_by=_columns(datasetTable, links))
        firstColl = func.substr(collMin, nDigits+1).label("first_collection")

        columns = _columns(datasetTable, ["dataset_id"] + links) + \
            _columns(datasetCollectionTable, ["collection"]) + [firstColl]

        subq = select(columns).select_from(subJoin).where(subWhere)
        # subquery needs a unique alias name
        subq = subq.alias("mcsubq_" + datasetType.name)
        whereClause = subq.columns.collection == subq.columns.first_collection

        return cls(registry, fromClause=subq, whereClause=whereClause, datasetType=datasetType,
                   selectableForDataset=subq, addResultColumns=addResultColumns)

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
            ``otherDimensions.link()`` and disjoint from
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
        missingLinks = set(self.datasetType.dimensions.links().difference(otherDimensions.links()))
        newLinks = set()
        while missingLinks:
            missingLink = missingLinks.pop()
            related = False
            for element in self.registry.dimensions.withLink(missingLink):
                if (isinstance(element, DimensionJoin) and not element.asNeeded and
                        element.links().issubset(allDimensions.links())):
                    self.joinDimensionElement(element)
                    missingLinks -= element.links()
                    newLinks.update(element.links())
                    related = True
            if not related:
                raise ValueError(f"No join found relating link {missingLink} to {otherDimensions.links()}")
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
