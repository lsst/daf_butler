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

__all__ = ("SqlPreFlight",)

import itertools
import logging
from sqlalchemy.sql import select, and_, functions, text, literal, case, between

from lsst.sphgeom import Region
from lsst.sphgeom.relationship import DISJOINT
from lsst.daf.butler import DatasetRef, PreFlightDimensionsRow, DimensionJoin, DataId


_LOG = logging.getLogger(__name__)


class SqlPreFlight:
    """Class implementing part of preflight solver which extracts dimension
    data from registry.

    This is an implementation detail only to be used by SqlRegistry class,
    not supposed to be used anywhere else.

    Parameters
    ----------
    registry : `SqlRegistry``
        Registry instance
    """
    def __init__(self, registry):
        self.registry = registry

    def _joinOnForeignKey(self, fromClause, dimension, otherDimensions):
        """Add new table for join clause.

        Assumption here is that this Dimension table has a foreign key to all
        other tables and names of columns are the same in both tables, so we
        just get primary key columns from other tables and join on them.

        Parameters
        ----------
        fromClause : `sqlalchemy.FromClause`
            May be `None`, in that case ``otherDimensions`` is expected to be
            empty and is ignored.
        dimension : `DimensionElement`
            `Dimension` or `DimensionJoin` to join with ``fromClause``.
        otherDimensions : iterable of `Dimension`
            Dimensions whose tables have PKs for ``dimension`` table's FK.
            These must be in ``fromClause`` already.

        Returns
        -------
        fromClause : `sqlalchemy.FromClause`
            SQLAlchemy FROM clause extended with new join.
        """
        if fromClause is None:
            # starting point, first table in JOIN
            return self.registry._schema.tables[dimension.name]
        else:
            joinOn = []
            for otherDimension in otherDimensions:
                primaryKeyColumns = {name: self.registry._schema.tables[otherDimension.name].c[name]
                                     for name in otherDimension.links()}
                for name, col in primaryKeyColumns.items():
                    joinOn.append(self.registry._schema.tables[dimension.name].c[name] == col)
                _LOG.debug("join %s with %s on columns %s", dimension.name,
                           dimension.name, list(primaryKeyColumns.keys()))
            if joinOn:
                return fromClause.join(self.registry._schema.tables[dimension.name], and_(*joinOn))
            else:
                # Completely unrelated tables, e.g. joining SkyMap and Instrument.
                # We need a cross join here but SQLAlchemy does not have specific
                # method for that. Using join() without `onclause` will try to
                # join on FK and will raise an exception for unrelated tables,
                # so we have to use `onclause` which is always true.
                return fromClause.join(self.registry._schema.tables[dimension.name], literal(True))

    def selectDimensions(self, originInfo, expression, neededDatasetTypes, futureDatasetTypes):
        """Evaluate a filter expression and lists of
        `DatasetTypes <DatasetType>` and return a set of dimension values.

        Returned set consists of combinations of dimensions participating in
        data transformation from ``neededDatasetTypes`` to
        ``futureDatasetTypes``, restricted by existing data and filter
        expression.

        Parameters
        ----------
        originInfo : `DatasetOriginInfo`
            Object which provides names of the input/output collections.
        expression : `str`
            An expression that limits the `Dimensions <Dimension>` and
            (indirectly) the Datasets returned.
        neededDatasetTypes : `list` of `DatasetType`
            The `list` of `DatasetTypes <DatasetType>` whose Dimensions will
            be included in the returned column set. Output is limited to the
            the Datasets of these DatasetTypes which already exist in the
            registry.
        futureDatasetTypes : `list` of `DatasetType`
            The `list` of `DatasetTypes <DatasetType>` whose Dimensions will
            be included in the returned column set. It is expected that
            Datasets for these DatasetTypes do not exist in the registry,
            but presently this is not checked.

        Yields
        ------
        row : `PreFlightDimensionsRow`
            Single row is a unique combination of dimensions in a transform.
        """

        # Brief overview of the code below:
        #  - extract all Dimensions used by all input/output dataset types
        #  - build a complex SQL query to run against registry database:
        #    - first do (natural) join for all tables for all Dimensions
        #      involved based on their foreign keys
        #    - then add Join tables to the mix, only use Join tables which
        #      have their lhs/rhs links in the above Dimensions set, also
        #      ignore Joins which summarize other Joins
        #    - next join with Dataset for each input dataset type, this
        #      limits result only to existing input dataset
        #    - also do outer join with Dataset for each output dataset type
        #      to see which output datasets are already there
        #    - append user filter expression
        #    - query returns all Dimension values, regions for region-based
        #      joins, and dataset IDs for all existing datasets
        #  - run this query
        #  - filter out records whose regions do not overlap
        #  - return result as iterator of records containing Dimension values

        # Collect dimensions from both input and output dataset types
        dimensions = self.registry.dimensions.extract(
            itertools.chain(
                itertools.chain.from_iterable(dsType.dimensions.names for dsType in neededDatasetTypes),
                itertools.chain.from_iterable(dsType.dimensions.names for dsType in futureDatasetTypes),
            )
        )
        _LOG.debug("dimensions: %s", dimensions)

        # Build select column list
        selectColumns = []
        linkColumnIndices = {}
        for dimension in dimensions:
            table = self.registry._schema.tables.get(dimension.name)
            if table is not None:
                # take link column names, usually there is one
                for link in dimension.links(expand=False):
                    linkColumnIndices[link] = len(selectColumns)
                    selectColumns.append(table.c[link])
        _LOG.debug("selectColumns: %s", selectColumns)
        _LOG.debug("linkColumnIndices: %s", linkColumnIndices)

        # Extend dimensions with the "implied" superset, so that joins work
        # correctly. This may bring more tables into query than really needed,
        # potential for optimization.
        dimensions = dimensions.union(dimensions.implied())

        fromJoin = None
        for dimension in dimensions:
            _LOG.debug("processing Dimension: %s", dimension.name)
            if dimension.name in self.registry._schema.tables:
                fromJoin = self._joinOnForeignKey(fromJoin, dimension, dimension.dependencies(implied=True))

        joinedRegionTables = set()
        regionColumnIndices = {}
        for dimensionJoin in dimensions.joins(summaries=False):
            _LOG.debug("processing DimensionJoin: %s", dimensionJoin.name)
            # Some `DimensionJoin`s have an associated region in that case
            # they shouldn't be joined separately in the region lookup.
            if dimensionJoin.hasRegion:
                _LOG.debug("%s has a region, skipping", dimensionJoin.name)
                continue

            # Look at each side of the DimensionJoin and join it with
            # corresponding Dimension tables, including making all necessary
            # joins for special multi-Dimension region table(s).
            regionHolders = []
            for connection in (dimensionJoin.lhs, dimensionJoin.rhs):
                graph = self.registry.dimensions.extract(connection)
                try:
                    regionHolder = graph.getRegionHolder()
                except KeyError:
                    # means there is no region for these dimensions, want to skip it
                    _LOG.debug("Dimensions %s are not spatial, skipping", connection)
                    break
                if isinstance(regionHolder, DimensionJoin):
                    # If one of the connections is with a DimensionJoin, then
                    # it must be one with a region (and hence one we skip
                    # in the outermost 'for' loop).
                    # Bring that join in now, but (unlike the logic in the
                    # outermost 'for' loop) bring the region along too.
                    assert regionHolder.hasRegion, "Spatial join with a join that has no region."
                    if regionHolder.name in joinedRegionTables:
                        _LOG.debug("region table already joined: %s", regionHolder.name)
                    else:
                        _LOG.debug("joining region table: %s", regionHolder.name)
                        joinedRegionTables.add(regionHolder.name)

                        fromJoin = self._joinOnForeignKey(fromJoin, regionHolder, connection)

                # add to the list of tables this join table joins against
                regionHolders.append(regionHolder)

                # We also have to include regions from each side of the join
                # into resultset so that we can filter-out non-overlapping
                # regions.
                regionColumnIndices[regionHolder.name] = len(selectColumns)
                regionColumn = self.registry._schema.tables[regionHolder.name].c.region
                selectColumns.append(regionColumn)

            if regionHolders:
                fromJoin = self._joinOnForeignKey(fromJoin, dimensionJoin, regionHolders)

        # join with input datasets to restrict to existing inputs
        dsIdColumns = {}
        allDsTypes = [(dsType, False) for dsType in neededDatasetTypes] + \
                     [(dsType, True) for dsType in futureDatasetTypes]
        for dsType, isOutput in allDsTypes:

            _LOG.debug("joining %s dataset type: %s",
                       "output" if isOutput else "input", dsType.name)

            # Build a sub-query.
            subquery = self._buildDatasetSubquery(dsType, originInfo, isOutput)
            if subquery is None:
                # If there nothing to join (e.g. we know that output
                # collection is empty) then just pass None as column
                # index for this dataset type to the code below.
                dsIdColumns[dsType] = None
                continue

            # Join sub-query with all dimensions on their link names,
            # OUTER JOIN is used for output datasets (they don't usually exist)
            joinOn = []
            for dimension in dsType.dimensions:
                if dimension.name == "ExposureRange":
                    # very special handling of ExposureRange
                    # TODO: try to generalize this in some way, maybe using
                    # sql from ExposureRangeJoin
                    _LOG.debug("  joining on dimension: %s", dimension.name)
                    exposureTable = self.registry._schema.tables["Exposure"]
                    joinOn.append(between(exposureTable.c.datetime_begin,
                                          subquery.c.valid_first,
                                          subquery.c.valid_last))
                    linkColumnIndices[dsType.name + ".valid_first"] = len(selectColumns)
                    selectColumns.append(subquery.c.valid_first)
                    linkColumnIndices[dsType.name + ".valid_last"] = len(selectColumns)
                    selectColumns.append(subquery.c.valid_last)
                else:
                    for link in dimension.links():
                        _LOG.debug("  joining on link: %s", link)
                        joinOn.append(subquery.c[link] ==
                                      self.registry._schema.tables[dimension.name].c[link])
            fromJoin = fromJoin.join(subquery, and_(*joinOn), isouter=isOutput)

            # remember dataset_id column index for this dataset
            dsIdColumns[dsType] = len(selectColumns)
            selectColumns.append(subquery.c.dataset_id)

        # build full query
        q = select(selectColumns).select_from(fromJoin)
        if expression:
            # TODO: potentially transform query from user-friendly expression
            where = text(expression)
            _LOG.debug("full where: %s", where)
            q = q.where(where)
        _LOG.debug("full query: %s",
                   q.compile(bind=self.registry._connection.engine,
                             compile_kwargs={"literal_binds": True}))

        # execute and return result iterator
        return self._convertResultRows(rows, dimensions, linkColumnIndices, regionColumnIndices, dsIdColumns)
        rows = self.registry._connection.execute(q).fetchall()

    def _buildDatasetSubquery(self, dsType, originInfo, isOutput):
        """Build a sub-query for a dataset type to be joined with "big join".

        If there is only one collection then there is a guarantee that
        DataIds are all unique (by DataId I mean combination of all link
        values relevant for this dataset), in that case subquery can be
        written as:

            SELECT Dataset.dataset_id AS dataset_id, Dataset.link1 AS link1 ...
            FROM Dataset JOIN DatasetCollection
                ON Dataset.dataset_id = DatasetCollection.dataset_id
            WHERE Dataset.dataset_type_name = :dsType_name
                AND DatasetCollection.collection = :collection_name

        We only have single collection for output DatasetTypes so for them
        subqueries always look like above.

        If there are multiple collections then there can be multiple matching
        Datasets for the same DataId. In that case we need only one Dataset
        record which comes from earliest collection (in the user-provided
        order). Here things become complicated, we have to:
        - replace collection names with their order in input list
        - select all combinations of rows from Dataset and DatasetCollection
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
            FROM DatasetCollection

        Combined query will look like (CASE ... END is as  above):

            SELECT Dataset.dataset_id AS dataset_id,
                CASE DatasetCollection.collection ... END AS collorder,
                Dataset.DataId
            FROM Dataset JOIN DatasetCollection
                ON Dataset.dataset_id = DatasetCollection.dataset_id
            WHERE Dataset.dataset_type_name = <dsType.name>
                AND DatasetCollection.collection IN (<collections>)

        (here ``Dataset.DataId`` means ``Dataset.link1, Dataset.link2, etc.``)

        Filtering is complicated, it is simpler to use Common Table Expression
        (WITH clause) but not all databases support CTEs so we will have to do
        with the repeating sub-queries. Use GROUP BY for DataId and
        MIN(collorder) to find ``collorder`` for given DataId, then join it
        with previous combined selection:

            SELECT DS.dataset_id AS dataset_id, DS.link1 AS link1 ...
            FROM (SELECT Dataset.dataset_id AS dataset_id,
                    CASE ... END AS collorder,
                    Dataset.DataId
                FROM Dataset JOIN DatasetCollection
                    ON Dataset.dataset_id = DatasetCollection.dataset_id
                WHERE Dataset.dataset_type_name = <dsType.name>
                    AND DatasetCollection.collection IN (<collections>)) DS
            INNER JOIN
                (SELECT MIN(CASE ... END AS) collorder, Dataset.DataId
                FROM Dataset JOIN DatasetCollection
                    ON Dataset.dataset_id = DatasetCollection.dataset_id
                WHERE Dataset.dataset_type_name = <dsType.name>
                   AND DatasetCollection.collection IN (<collections>)
                GROUP BY Dataset.DataId) DSG
            ON DS.colpos = DSG.colpos AND DS.DataId = DSG.DataId

        Parameters
        ----------
        dsType : `DatasetType`
        originInfo : `DatasetOriginInfo`
            Object which provides names of the input/output collections.
        isOutput : `bool`
            `True` for output datasets.

        Returns
        -------
        subquery : `sqlalchemy.FromClause` or `None`
        """

        # helper method
        def _columns(selectable, names):
            """Return list of columns for given column names"""
            return [selectable.c[name].label(name) for name in names]

        if isOutput:

            outputCollection = originInfo.getOutputCollection(dsType.name)
            if not outputCollection:
                # No output collection means no output datasets exist, we do
                # not need to do any joins here.
                return None

            dsCollections = [outputCollection]
        else:
            dsCollections = originInfo.getInputCollections(dsType.name)

        _LOG.debug("using collections: %s", dsCollections)

        # full set of link names for this DatasetType
        links = list(dsType.dimensions.links())

        dsTable = self.registry._schema.tables["Dataset"]
        dsCollTable = self.registry._schema.tables["DatasetCollection"]

        if len(dsCollections) == 1:

            # single collection, easy-peasy
            subJoin = dsTable.join(dsCollTable, dsTable.c.dataset_id == dsCollTable.c.dataset_id)
            subWhere = and_(dsTable.c.dataset_type_name == dsType.name,
                            dsCollTable.c.collection == dsCollections[0])

            columns = _columns(dsTable, ["dataset_id"] + links)
            subquery = select(columns).select_from(subJoin).where(subWhere)

        else:

            # multiple collections
            subJoin = dsTable.join(dsCollTable, dsTable.c.dataset_id == dsCollTable.c.dataset_id)
            subWhere = and_(dsTable.c.dataset_type_name == dsType.name,
                            dsCollTable.c.collection.in_(dsCollections))

            # CASE caluse
            collorder = case([
                (dsCollTable.c.collection == coll, pos) for pos, coll in enumerate(dsCollections)
            ])

            # first GROUP BY sub-query, find minimum `collorder` for each DataId
            columns = [functions.min(collorder).label("collorder")] + _columns(dsTable, links)
            groupSubq = select(columns).select_from(subJoin).where(subWhere)
            groupSubq = groupSubq.group_by(*links)
            groupSubq = groupSubq.alias("sub1" + dsType.name)

            # next combined sub-query
            columns = [collorder.label("collorder")] + _columns(dsTable, ["dataset_id"] + links)
            combined = select(columns).select_from(subJoin).where(subWhere)
            combined = combined.alias("sub2" + dsType.name)

            # now join these two
            joinsOn = [groupSubq.c.collorder == combined.c.collorder] + \
                      [groupSubq.c[colName] == combined.c[colName] for colName in links]
            subJoin = combined.join(groupSubq, and_(*joinsOn))
            columns = _columns(combined, ["dataset_id"] + links)
            subquery = select(columns).select_from(subJoin)

        # need a unique alias name for it, otherwise we'll see name conflicts
        subquery = subquery.alias("ds" + dsType.name)
        return subquery

    def _convertResultRows(self, rowIter, dimensions, linkColumnIndices, regionColumnIndices, dsIdColumns):
        """Convert query result rows into `PreFlightDimensionsRow` instances.

        Parameters
        ----------
        rowIter : iterable
            Iterator for rows returned by the query on registry
        dimensions : `DimensionGraph`
            All Dimensions included in this query.
        linkColumnIndices : `dict`
            Dictionary of {dimension link name: column index} for the column
            that contains the link value
        regionColumnIndices : `dict`
            Dictionary of (Dimension name, column index), column contains
            encoded region data
        dsIdColumns : `dict`
            Dictionary of (DatasetType, column index), column contains
            dataset Id, or None if dataset does not exist

        Yields
        ------
        row : `PreFlightDimensionsRow`
        """

        total = 0
        count = 0
        for row in rowIter:

            total += 1

            # Filter result rows that have non-overlapping regions.
            # Result set generated by query in selectDimensions() method can include
            # set of regions in each row (encoded as bytes). Due to pixel-based
            # matching some regions may not overlap, this generator method filters
            # rows that have disjoint regions. If result row contains more than two
            # regions (this should not happen with our current schema) then row is
            # filtered if any of two regions are disjoint.
            disjoint = False
            regions = {holder: Region.decode(row[col]) for holder, col in regionColumnIndices.items()}
            for reg1, reg2 in itertools.combinations(regions.values(), 2):
                if reg1.relate(reg2) == DISJOINT:
                    disjoint = True
                    break
            if disjoint:
                continue

            def extractRegion(dims):
                try:
                    holder = dims.getRegionHolder()
                except ValueError:
                    return None
                if holder is not None:
                    return regions.get(holder.name)
                return None

            # Find all of the link columns that aren't NULL.
            rowDataIdDict = {link: row[col] for link, col in linkColumnIndices.items()
                             if row[col] is not None}
            # Find all of the Dimensions we can uniquely identify with the
            # non-NULL link columns.
            rowDimensions = dimensions.extract(dim for dim in dimensions
                                               if dim.links().issubset(rowDataIdDict.keys()))
            # Remove all of the link columns that weren't needed by the
            # Dimensions we selected (in practice this is just ExposureRange
            # links right now, so this step might not be needed once we make
            # that less of a special case).
            dataId = DataId(
                {k: v for k, v in rowDataIdDict.items() if k in rowDimensions.links()},
                dimensions=rowDimensions,
                region=extractRegion(rowDimensions)
            )

            # for each dataset get ids DataRef
            datasetRefs = {}
            for dsType, col in dsIdColumns.items():
                linkNames = {}  # maps full link name in linkColumnIndices to dataId key
                for dimension in dsType.dimensions:
                    if dimension.name == "ExposureRange":
                        # special case of ExposureRange, its columns come from
                        # Dataset table instead of Dimension
                        linkNames[dsType.name + ".valid_first"] = "valid_first"
                        linkNames[dsType.name + ".valid_last"] = "valid_last"
                    else:
                        if self.registry._schema.tables.get(dimension.name) is not None:
                            linkNames.update((s, s) for s in dimension.links(expand=False))
                dsDataId = DataId({val: row[linkColumnIndices[key]] for key, val in linkNames.items()},
                                  dimensions=dsType.dimensions,
                                  region=extractRegion(dsType.dimensions))
                dsId = None if col is None else row[col]
                datasetRefs[dsType] = DatasetRef(dsType, dsDataId, dsId)

            count += 1
            yield PreFlightDimensionsRow(dataId, datasetRefs)

        _LOG.debug("Total %d rows in result set, %d after region filtering", total, count)
