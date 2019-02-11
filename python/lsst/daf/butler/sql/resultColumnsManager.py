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

__all__ = ("ResultColumnsManager", "SearchDeferred")

import logging
import itertools

from sqlalchemy.sql import select

from lsst.sphgeom import Region, DISJOINT
from .. import DatasetRef, PreFlightDimensionsRow

from .. import DataId


_LOG = logging.getLogger(__name__)


class SearchDeferred:

    def __init__(self, collections):
        if len(collections) != 1:
            # TODO: remove this limitation, probably by making `Registry.find`
            # search multiple collections.
            raise NotImplementedError("Deferred searches in multiple collections is not yet supported.")
        self.collections = collections


class ResultColumnsManager:
    """Helper class that manages the columns in the selectDimensions query.

    Parameters
    ----------
    registry : `SqlRegistry`
        Registry instance the query is being run against.
    """

    def __init__(self, registry):
        self.registry = registry
        self._columns = []
        self._indicesForDimensionLinks = {}
        self._indicesForRegions = {}
        self._indicesForDatasetIds = {}

    def logState(self):
        """Log the state of ``self`` at debug level.
        """
        _LOG.debug("ResultColumnsManager columns: %s", self._columns)
        if self._indicesForDimensionLinks:
            _LOG.debug("ResultColumnsManager dimensions: %s", self._indicesForDimensionLinks)
        if self._indicesForRegions:
            _LOG.debug("ResultColumnsManager regions: %s", self._indicesForRegions)
        if self._indicesForDatasetIds:
            _LOG.debug("ResultColumnsManager datasets: %s", self._indicesForDatasetIds)

    def addDimensionLink(self, selectable, link):
        """Add a column containing a dimension link value.

        Parameters
        ----------
        selectable : `sqlalchemy.FromClause`
            SQLAlchemy object representing a part of the query from which
            a column can be selected.
        link : `str`
            String name of a dimension link column.  If this link is
            already in the selected columns, this method does nothing.
        """
        if link in self._indicesForDimensionLinks:
            return
        column = selectable.c[link]
        self._indicesForDimensionLinks[link] = len(self._columns)
        self._columns.append(column)

    def addRegion(self, selectable, holder):
        """Add a column containing a region.

        Parameters
        ----------
        selectable : `sqlalchemy.FromClause`
            SQLAlchemy object representing a part of the query from which
            a column can be selected.
        holder : `DimensionElement`
            `Dimension` or `DimensionElement` the region is associated with.
            If this holder is already in the selected columns, this method does
            nothing.
        """
        if holder in self._indicesForRegions:
            return
        if holder.name == "SkyPix":
            self._indicesForRegions[holder] = None
        else:
            column = selectable.c.region
            self._indicesForRegions[holder] = len(self._columns)
            self._columns.append(column)

    def addDatasetId(self, selectable, datasetType):
        """Add a column containing a dataset ID.

        Parameters
        ----------
        selectable : `sqlalchemy.FromClause` or `SearchDeferreed`
            SQLAlchemy object representing a part of the query from which
            a column can be selected, or a `SearchDeferred` instance
            indicating that a follow-up query must be performed later.
        datasetType : `DatasetType`
            `DatasetType` this ID column is for.  If this `DatasetType` is
            already in the selected columns, this method does nothing.
        """
        if datasetType in self._indicesForDatasetIds:
            return
        if isinstance(selectable, SearchDeferred):
            self._indicesForDatasetIds[datasetType] = selectable
        else:
            column = selectable.c["dataset_id"]
            self._indicesForDatasetIds[datasetType] = len(self._columns)
            self._columns.append(column)

    def apply(self, fromClause):
        """Return a select query that extracts the managed columns from the
        given from clause.

        Parameters
        ----------
        fromClause : `sqlalchemy.FromClause`
            SQLAlchemy object representing the full FROM clause for the query.

        Returns
        -------
        select : `sqlalchemy.Select`
            SQLAlchemy object representing the SELECT and FROM clauses of the
            query.
        """
        return select(self._columns, use_labels=True).select_from(fromClause)

    def extractRegions(self, row):
        """Return a dictionary of regions from a query result row.

        Parameters
        ----------
        row : `sqlalchemy.RowProxy`

        Returns
        -------
        regions : `dict`
            Dictionary mapping region holders (`DimensionElement`) to
            regions (`sphgeom.Region`).
        """
        result = {}
        for holder, index in self._indicesForRegions.items():
            if index is None:
                assert holder.name == "SkyPix"
                skypix = row[self._indicesForDimensionLinks["skypix"]]
                result[holder] = self.registry.pixelization.pixel(skypix)
            else:
                result[holder] = Region.decode(row[index])
        return result

    def extractDatasetIds(self, row):
        """Return a dictionary of dataset IDs from a query result row.

        Parameters
        ----------
        row : `sqlalchemy.RowProxy`

        Returns
        -------
        ids : `dict`
            Dictionary mapping `DatasetType` to integer dataset ID or
            `SearchStatus` enum value.
        """
        result = {}
        for datasetType, index in self._indicesForDatasetIds.items():
            if isinstance(index, SearchDeferred):
                result[datasetType] = index
            else:
                result[datasetType] = row[index]
        return result

    def extractDataId(self, row):
        """Return a `DataId` from a query result row.

        Parameters
        ----------
        row : `sqlalchemy.RowProxy`

        Returns
        -------
        dataId : `DataId`
            Dictionary-like object that identifies a set of dimensions.
        """
        return DataId({link: row[index] for link, index in self._indicesForDimensionLinks.items()},
                      universe=self.registry.dimensions)

    def convertQueryResults(self, rowIter, expandDataIds=True):
        """Convert query result rows into `PreFlightDimensionsRow` instances.

        Parameters
        ----------
        rowIter : iterable
            Iterator for rows returned by a query created with the selected
            columns managed by ``self``.
        expandDataIds : `bool`
            If `True` (default), expand per-`DatasetType` data IDs when
            returning them.

        Yields
        ------
        row : `PreFlightDimensionsRow`
        """

        total = 0
        count = 0
        for row in rowIter:

            total += 1

            # Filter result rows that have non-overlapping regions. Result set
            # generated by query in selectDimensions() method can include set
            # of regions in each row (encoded as bytes). Due to pixel-based
            # matching some regions may not overlap, this generator method
            # filters rows that have disjoint regions. If result row contains
            # more than two regions (this should not happen with our current
            # schema) then row is filtered if any of two regions are disjoint.
            disjoint = False
            regions = self.extractRegions(row)
            for reg1, reg2 in itertools.combinations(regions.values(), 2):
                if reg1.relate(reg2) == DISJOINT:
                    disjoint = True
                    break
            if disjoint:
                continue

            dataId = self.extractDataId(row)

            # row-wide Data IDs are never expanded, even if
            # expandDataIds=True; this is slightly confusing, but we don't
            # actually need them expanded, and it's actually quite slow.

            # get DatasetRef for each DatasetType
            datasetRefs = {}
            for datasetType, datasetId in self.extractDatasetIds(row).items():

                holder = datasetType.dimensions.getRegionHolder()
                if holder is not None:
                    region = regions.get(holder)
                else:
                    region = None

                dsDataId = DataId(dataId, dimensions=datasetType.dimensions, region=region)

                if expandDataIds:
                    self.registry.expandDataId(dsDataId)

                if datasetId is None:
                    # Dataset does not exist yet.
                    datasetRefs[datasetType] = DatasetRef(datasetType, dataId, id=None)
                elif isinstance(datasetId, SearchDeferred):
                    # We haven't searched for the dataset yet, because we've
                    # deferred these queries
                    ref = self.registry.find(
                        collection=datasetId.collections[0],
                        datasetType=datasetType,
                        dataId=dsDataId
                    )
                    datasetRefs[datasetType] = ref if ref is not None else DatasetRef(datasetType, dsDataId,
                                                                                      id=None)
                else:
                    datasetRefs[datasetType] = self.registry.getDataset(id=datasetId,
                                                                        datasetType=datasetType,
                                                                        dataId=dsDataId)
            count += 1
            yield PreFlightDimensionsRow(dataId, datasetRefs)

        _LOG.debug("Total %d rows in result set, %d after region filtering", total, count)
