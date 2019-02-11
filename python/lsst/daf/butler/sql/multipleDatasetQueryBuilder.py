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

__all__ = ("MultipleDatasetQueryBuilder",)

import itertools
import logging
from collections import namedtuple

from .resultColumnsManager import SearchDeferred
from .queryBuilder import QueryBuilder
from .singleDatasetQueryBuilder import SingleDatasetQueryBuilder

_LOG = logging.getLogger(__name__)


_SubqueryData = namedtuple("_SubqueryData", ("subquery", "optional"))


class MultipleDatasetQueryBuilder(QueryBuilder):
    r"""Specialization of `QueryBuilder` that relates multiple `DatasetType`s
    via their `Dimensions`.

    Most users should call `fromDatasetTypes` to construct an instance of this
    class, rather than invoking the constructor and calling
    `~QueryBuilder.joinDimensionElement` or `joinDataset` directly.

    Parameters
    ----------
    registry : `SqlRegistry`
        Registry instance the query is being run against.
    fromClause : `sqlalchemy.sql.expression.FromClause`, optional
        Initial FROM clause for the query.
    whereClause : SQLAlchemy boolean expression, optional
        Expression to use as the initial WHERE clause.
    """

    def __init__(self, registry, *, fromClause=None, whereClause=None):
        super().__init__(registry, fromClause=fromClause, whereClause=whereClause)
        self._subqueries = {}

    @classmethod
    def fromDatasetTypes(cls, registry, originInfo, required=(), optional=(), addResultColumns=True,
                         deferOptionalDatasetQueries=False):
        r"""Build a query that relates multiple `DatasetType`s via their
        dimensions.

        This method ensures that all `Dimension` and `DimensionJoin` tables
        necessary to relate the given datasets are also included.

        Parameters
        ----------
        registry : `SqlRegistry`
            Registry instance the query is being run against.
        originInfo : `DatasetOriginInfo`
            Information about which collections to search for different
            `DatasetType`s.
        required : iterable of `DatasetType`
            `DatasetType`s whose presence or absence constrains the query
            results; these are added to the query with an INNER JOIN.
        optional : iterable of `DatasetType`
            `DatasetType`s whose presence or absence does not constrain the
            query results; these are added to the query with a LEFT OUTER
            JOIN. Note that this does nothing unless the ID for this dataset
            is actually requested in the results, via either
            ``addResultColumns`` here or `selectDatasetId`.
        addResultColumns : `bool`
            If `True` (default), add result columns to the SELECT clause for
            all dataset IDs and dimension links.
        deferOptionalDatasetQueries : `bool`
            If `True`, defer queries for optional dataset IDs until row-by-row
            processing of the main query's results.
        """
        resultDimensions = registry.dimensions.extract(
            itertools.chain(
                itertools.chain.from_iterable(dsType.dimensions.names for dsType in required),
                itertools.chain.from_iterable(dsType.dimensions.names for dsType in optional),
            )
        )
        _LOG.debug("Input dimensions (needed by DatasetTypes): %s", resultDimensions)
        allDimensions = resultDimensions.union(resultDimensions.implied())
        _LOG.debug("All dimensions (expanded to include implied): %s", allDimensions)

        self = cls.fromDimensions(registry, dimensions=allDimensions, addResultColumns=addResultColumns)

        for datasetType in required:
            self.joinDataset(datasetType, originInfo.getInputCollections(datasetType.name),
                             addResultColumns=addResultColumns)
        for datasetType in optional:
            collections = [originInfo.getOutputCollection(datasetType.name)]
            self.joinDataset(datasetType, collections, optional=True, defer=deferOptionalDatasetQueries,
                             addResultColumns=addResultColumns)
        return self

    @property
    def datasetTypes(self):
        """The dataset types this query searches for (`~collections.abc.Set` of
        `DatasetType`).
        """
        return self._subqueries.keys()

    def joinDataset(self, datasetType, collections, optional=False, defer=False, addResultColumns=True):
        """Join an aliased subquery of the dataset table for a particular
        `DatasetType` into the query.

        This method attempts to join the dataset subquery on the dimension
        link columns that identify that `DatasetType`, which in general means
        at least one `Dimension` table for all of those types should be present
        in the query first.  This can be guaranteed by calling
        `fromDatasetTypes` to construct the `QueryBuilder` instead of calling
        this method directly.

        Parameters
        ----------
        datasetType : `DatasetType`
            Object representing the type of dataset to query for.
        collections : `list` of `str`
            String names of the collections in which to search for the dataset,
            ordered from the first to be searched to the last to be searched.
        optional : `bool`
            If `True`, a dataset of this type does not need to be present to
            generate a query result row; it should be added with a
            LEFT OUTER JOIN.
        defer : `bool`
            If `True`, defer querying for the IDs for this dataset until
            processing the main query results.  Must be `False` unless
            ``optional`` is `True`.  Note that this does nothing unless
            the ID for this dataset is actually requested in the results,
            via either ``addResultColumns`` here or `selectDatasetId`.
        addResultColumns : `bool`
            If `True` (default), add the ``dataset_id`` for this `DatasetType`
            to the result columns in the SELECT clause of the query.
        """
        if datasetType in self._subqueries:
            raise ValueError(f"DatasetType {datasetType.name} already included in query.")
        if defer:
            subquery = SearchDeferred(collections)
        else:
            builder = SingleDatasetQueryBuilder.fromCollections(self.registry, datasetType, collections)
            subquery = builder.build().alias(datasetType.name)
            self.join(subquery, datasetType.dimensions.links(), isOuter=optional)
        self._subqueries[datasetType] = _SubqueryData(subquery=subquery, optional=optional)
        if addResultColumns:
            self.resultColumns.addDatasetId(subquery, datasetType)
        return subquery

    def selectDatasetId(self, datasetType):
        """Add the ``dataset_id`` for the given `DatasetType` to the result
        columns in the SELECT clause of the query.

        Parameters
        ----------
        datasetType : `DatasetType`
            Dataset type for which output IDs should be returned by the query.
            A subquery for this `DatasetType` must have already been added to
            the query via `fromDatasetTypes` or `joinDatasetType`.
        """
        self.resultColumns.addDatasetId(self._subqueries[datasetType].subquery, datasetType)

    def findSelectableForLink(self, link):
        # Docstring inherited from QueryBuilder.findSelectableForLink
        result = super().findSelectableForLink(link)
        if result is None:
            for datasetType, data in self._subqueries.items():
                if not data.optional and link in datasetType.dimensions().links():
                    result = data.subquery
                    break
        return result

    def findSelectableByName(self, name):
        # Docstring inherited from QueryBuilder.findSelectableByName
        result = super().findSelectableByName(name)
        if result is None:
            for datasetType, data in self._subqueries.items():
                if name == datasetType.name:
                    result = data.subquery
                    break
        return result
