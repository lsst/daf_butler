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

__all__ = ("DataIdQueryBuilder",)

import logging

from sqlalchemy.sql import and_, select

from .queryBuilder import QueryBuilder

_LOG = logging.getLogger(__name__)


class DataIdQueryBuilder(QueryBuilder):
    r"""Specialization of `QueryBuilder` that yields data IDs consistent
    with dimension relationships.

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

    def addDimension(self, dimension, addResultColumns=True):
        """Add a `Dimension` table to the query.

        This is a thin wrapper around `QueryBuilder.joinDimensionElement`
        that updates the SELECT clause and hence requires that the dimension to
        be added has a table.

        Parameters
        ----------
        dimensions : `Dimension`
            Dimension to add.
        addResultColumns : `bool`
            If `True`, add the dimension's links to the SELECT clause so
            they are included in the query results.  Any links already selected
            from other dimensions will be ignored.

        Raises
        ------
        ValueError
            Raised if the dimension has no table or view.
        """
        table = self.joinDimensionElement(dimension)
        if table is None:
            raise ValueError(f"Dimension '{dimension}' has no table.")
        if addResultColumns:
            for link in dimension.links():
                self.selectDimensionLink(link)

    def requireDataset(self, datasetType, collections):
        """Constrain the query to return only data IDs for which at least one
        instance of the given dataset exists in one of the given collections.

        The dimensions joined into the query and the dimensions used to
        identify the `DatasetType` need not be identical, but they should
        overlap.

        To ensure any dimensions that might relate to a `DatasetType` are
        present, `requireDataset` should generally only be called after
        all calls to `addDimension` have been made.

        Parameters
        ----------
        datasetType : `DatasetType`
            `DatasetType` for which instances must exist in order for the
            query to yield related data IDs.
        collections : `~collections.abc.Iterable` of `str`
            The names of collections to search for the dataset, in any order.
        """
        datasetTable = self.registry._schema.tables["dataset"]
        datasetCollectionTable = self.registry._schema.tables["dataset_collection"]
        links = [link for link in datasetType.dimensions.links()
                 if self.findSelectableForLink(link) is not None]
        subquery = select(
            [datasetTable.columns[link] for link in links]
        ).select_from(
            datasetTable.join(
                datasetCollectionTable,
                datasetTable.columns.dataset_id == datasetCollectionTable.columns.dataset_id
            )
        ).where(
            and_(datasetTable.columns.dataset_type_name == datasetType.name,
                 datasetCollectionTable.columns.collection.in_(list(collections)))
        )
        self.join(subquery.alias(datasetType.name), links)

    def convertResultRow(self, managed):
        """Convert a result row for this query to a `DataId`.

        Parameters
        ----------
        managed : `ResultsColumnsManager.ManagedRow`
            Intermediate result row object to convert.

        Returns
        -------
        dataId : `DataId`
            Dictionary-like object with a set of related dimension values.
        """
        return managed.makeDataId()
