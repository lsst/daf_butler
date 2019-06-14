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

__all__ = ("ResultColumnsManager",)

import logging
import itertools
from collections import defaultdict

from sqlalchemy.sql import select

from lsst.sphgeom import DISJOINT
from .. import DatasetRef, DataId


_LOG = logging.getLogger(__name__)


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
        self._indicesForPerDatasetTypeDimensionLinks = defaultdict(dict)
        self._indicesForRegions = {}
        self._indicesForDatasetIds = {}
        self._needSkyPixRegion = False

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

    def addDimensionLink(self, selectable, link, datasetType=None):
        """Add a column containing a dimension link value.

        Parameters
        ----------
        selectable : `sqlalchemy.FromClause`
            SQLAlchemy object representing a part of the query from which
            a column can be selected.
        link : `str`
            String name of a dimension link column.  If this link is
            already in the selected columns, this method does nothing.
        datasetType : `DatasetType`, optional
            If not `None`, this link corresponds to a "per-DatasetType"
            dimension in a query for this `DatasetType`.
        """
        column = selectable.columns[link]
        if datasetType is None:
            if link in self._indicesForDimensionLinks:
                return
            self._indicesForDimensionLinks[link] = len(self._columns)
        else:
            if link in self._indicesForPerDatasetTypeDimensionLinks[datasetType]:
                return
            column = column.label(f"{datasetType.name}_{link}")
            self._indicesForPerDatasetTypeDimensionLinks[datasetType][link] = len(self._columns)
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
        if holder.name == "skypix":
            # We obtain these regions directly from the registry, but require
            # that the "skypix" dimension link be present to do so.  We can't
            # add that ourselves (we don't know what selectable to obtain it
            # from), and we can't assume it's *already* been added.
            # Instead we'll remember that we do need it, and raise when trying
            # to create the full query (in `selectFrom`) if it's not present.
            self._needskypixRegion = True
            return
        else:
            column = selectable.columns.region
            self._indicesForRegions[holder] = len(self._columns)
            self._columns.append(column)

    def addDatasetId(self, selectable, datasetType):
        """Add a column containing a dataset ID.

        Parameters
        ----------
        selectable : `sqlalchemy.FromClause`
            SQLAlchemy object representing a part of the query from which
            a column can be selected.
        datasetType : `DatasetType`
            `DatasetType` this ID column is for.  If this `DatasetType` is
            already in the selected columns, this method does nothing.
        """
        if datasetType in self._indicesForDatasetIds:
            return
        column = selectable.columns["dataset_id"]
        self._indicesForDatasetIds[datasetType] = len(self._columns)
        self._columns.append(column)

    def selectFrom(self, fromClause, distinct=False):
        """Return a select query that extracts the managed columns from the
        given from clause.

        Parameters
        ----------
        fromClause : `sqlalchemy.FromClause`
            SQLAlchemy object representing the full FROM clause for the query.
        distinct : `bool`
            If `True`, perform a ``SELECT DISTINCT`` query.

        Returns
        -------
        select : `sqlalchemy.Select`
            SQLAlchemy object representing the SELECT and FROM clauses of the
            query.
        """
        if self._needSkyPixRegion and "skypix" not in self._indicesForDimensionLinks:
            raise RuntimeError("skypix region added to query without associated link.")
        return select(self._columns, distinct=distinct).select_from(fromClause)

    def manageRow(self, row):
        """Return an object that manages raw query result row.

        Parameters
        ----------
        row : `sqlalchemy.sql.RowProxy`, optional
            Direct SQLAlchemy row result object.

        Returns
        -------
        managed : `ManagedRow` or `None`
            Proxy for the row that understands the columns managed by ``self``.
            Will be `None` if and only if ``row`` is `None`.
        """
        if row is None:
            return None
        else:
            return self.ManagedRow(self, row)

    class ManagedRow:
        """An intermediate query result row class that understands the columns
        managed by a `ResultColumnsManager`.

        Parameters
        ----------
        manager : `ResultColumnsManager`
            Object that manages the columns in the query's SELECT clause.
        row : `sqlalchemy.sql.RowProxy`
            Direct SQLAlchemy row result object.
        """

        __slots__ = ("registry", "_dimensionLinks", "_perDatasetTypeDimensionLinks", "_regions",
                     "_datasetIds")

        def __init__(self, manager, row):
            self.registry = manager.registry
            self._dimensionLinks = {
                link: row[index] for link, index in manager._indicesForDimensionLinks.items()
            }
            self._perDatasetTypeDimensionLinks = {
                datasetType: {link: row[index] for link, index in indices.items()}
                for datasetType, indices in manager._indicesForPerDatasetTypeDimensionLinks.items()
            }
            self._regions = {
                holder: row[index] for holder, index in manager._indicesForRegions.items()
            }
            self._datasetIds = {
                datasetType: row[index] for datasetType, index in manager._indicesForDatasetIds.items()
            }
            skypix = self._dimensionLinks.get("skypix", None)
            if skypix is not None:
                self._regions[self.registry.dimensions["skypix"]] = self.registry.pixelization.pixel(skypix)

        def areRegionsDisjoint(self):
            """Test whether the regions in this result row are disjoint.

            Returns
            -------
            disjoint : `bool`
                `True` if any region in the result row is disjoint with any
                other region in the result row.
            """
            for reg1, reg2 in itertools.combinations(self._regions.values(), 2):
                if reg1.relate(reg2) == DISJOINT:
                    return True
            return False

        def makeDataId(self, *, datasetType=None, expandDataId=True, **kwds):
            """Construct a `DataId` from the result row.

            Parameters
            ----------
            datasetType : `DatasetType`, optional
                If provided, the `DatasetType` this data ID will describe.
                This will enable per-DatasetType dimension link values to be
                used and set the `~DataId.dimensions` to be those of the
                `DatasetType`. If not provided, the returned data ID will
                cover all common dimensions in the graph.
            expandDataId : `bool`
                If `True` (default), query the `Registry` to further expand
                the data ID to include additional information.
            kwds
                Additional keyword arguments passed to the `DataId`
                constructor.

            Returns
            -------
            dataId : `DataId`
                A new `DataId` instance.
            """
            if datasetType is None:
                result = DataId(self._dimensionLinks, universe=self.registry.dimensions, **kwds)
            else:
                d = self._dimensionLinks.copy()
                d.update(self._perDatasetTypeDimensionLinks.get(datasetType, {}))
                result = DataId(d, dimensions=datasetType.dimensions, **kwds)
            if result.region is None:
                holder = result.dimensions().getRegionHolder()
                if holder is not None:
                    result.region = self._regions.get(holder)
            if expandDataId:
                self.registry.expandDataId(result)
            return result

        def makeDatasetRef(self, datasetType, *, expandDataId=True, **kwds):
            """Construct a `DatasetRef` from the result row.

            Parameters
            ----------
            datasetType : `DatasetType`
                The `DatasetType` the returned `DatasetRef` will identify.
            expandDataId : `bool`
                If `True` (default), query the `Registry` to further expand
                the data ID to include additional information.
            kwds
                Additional keyword arguments passed to the `DataId`
                constructor.

            Returns
            -------
            ref : `DatasetRef`
                New `DatasetRef` instance.  `DatasetRef.id` will be `None` if
                the ``dataset_id`` for this dataset was either not included in
                the result columns or NULL in the result row.  If
                `DatasetRef.id` is not `None`, any component dataset references
                will also be present.
            """
            dataId = self.makeDataId(datasetType=datasetType, expandDataId=expandDataId, **kwds)
            datasetId = self._datasetIds.get(datasetType)
            if datasetId is None:
                return DatasetRef(datasetType, dataId)
            else:
                # Need to ask registry to expand to include components if they
                # exist.
                return self.registry.getDataset(id=datasetId, dataId=dataId, datasetType=datasetType)
