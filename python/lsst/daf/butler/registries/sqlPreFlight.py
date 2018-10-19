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

__all__ = ("SqlPreFlight")

import itertools
import logging
from sqlalchemy.sql import select, and_, functions, text, literal, case

from lsst.sphgeom import Region
from lsst.sphgeom.relationship import DISJOINT
from lsst.daf.butler import DatasetRef, PreFlightUnitsRow


_LOG = logging.getLogger(__name__)


def _scanDataUnits(dataUnits):
    """Recursively scan units and their optional dependencies, return their
    names"""
    for dataUnit in dataUnits:
        yield dataUnit.name
        yield from _scanDataUnits(dataUnit.optionalDependencies)


def _filterSummarizes(dataUnitJoins):
    """Filter out DataUnitJoins that summarize other DataUnitJoins.

    Parameters
    ----------
    dataUnitJoins : iterable of `DataUnitJoin`

    Yields
    ------
    dataUnitJoin : `DataUnitJoin`
        DataUnitJoin which do not summarize any of the DataUnitJoins in the
        input set.
    """
    dataUnitJoins = list(dataUnitJoins)
    dataUnitJoinNames = set(join.name for join in dataUnitJoins)
    for dataUnitJoin in dataUnitJoins:
        summarizes = set(dataUnitJoin.summarizes or [])
        # If it summarizes some other joins and all those joins are in the
        # set of joins then we do not need it.
        if summarizes and summarizes.issubset(dataUnitJoinNames):
            continue
        yield dataUnitJoin


def _unitsTopologicalSort(dataUnits):
    """Return topologically sorted DataUnits.

    Ordering is based on dependencies, units with no dependencies
    on other units are returned first.

    Parameters
    ----------
    dataUnits : iterable of `DataUnit`
    """
    dataUnits = set(dataUnits)
    while dataUnits:
        for dataUnit in dataUnits:
            if dataUnits.isdisjoint(dataUnit.dependencies):
                dataUnits.remove(dataUnit)
                yield dataUnit
                break


class SqlPreFlight:
    """Class implementing part of preflight solver which extracts
    units data from registry.

    This is an implementation detail only to be used by SqlRegistry class,
    not supposed to be used anywhere else.

    Parameters
    ----------
    schema : `Schema`
        Schema instance
    dataUnits : `DataUnitRegistry`
        Description of DataUnit dimensions and joins.
    connection : `sqlalchmey.Connection`
        Connection to use for database access.
    """
    def __init__(self, schema, dataUnits, connection):
        self._schema = schema
        self._dataUnits = dataUnits
        self._connection = connection

    def _joinOnForeignKey(self, fromClause, dataUnit, otherDataUnits):
        """Add new table for join clause.

        Assumption here is that this unit table has a foreign key to all other
        tables and names of columns are the same in both tables, so we just get
        primary key columns from other tables and join on them.

        Parameters
        ----------
        fromClause : `sqlalchemy.FromClause`
            May be `None`, in that case ``otherDataUnits`` is expected to be
            empty and is ignored.
        dataUnit : `DataUnit`
            DataUnit to join with ``fromClause``.
        otherDataUnits : iterable of `DataUnit`
            DataUnits whose tables have PKs for ``dataUnit`` table's FK. They all
            have to be in ``fromClause`` already.

        Returns
        -------
        fromClause : `sqlalchemy.FromClause`
            SQLAlchemy FROM clause extended with new join.
        """
        if fromClause is None:
            # starting point, first table in JOIN
            return self._schema.tables[dataUnit.name]
        else:
            joinOn = []
            for otherUnit in otherDataUnits:
                primaryKeyColumns = {name: self._schema.tables[otherUnit.name].c[name]
                                     for name in otherUnit.primaryKey}
                for name, col in primaryKeyColumns.items():
                    joinOn.append(self._schema.tables[dataUnit.name].c[name] == col)
                _LOG.debug("join %s with %s on columns %s", dataUnit.name,
                           otherUnit.name, list(primaryKeyColumns.keys()))
            if joinOn:
                return fromClause.join(self._schema.tables[dataUnit.name], and_(*joinOn))
            else:
                # Completely unrelated tables, e.g. joining SkyMap and Instrument.
                # We need a cross join here but SQLAlchemy does not have specific
                # method for that. Using join() without `onclause` will try to
                # join on FK and will raise an exception for unrelated tables,
                # so we have to use `onclause` which is always true.
                return fromClause.join(self._schema.tables[dataUnit.name], literal(True))

    def selectDataUnits(self, originInfo, expression, neededDatasetTypes, futureDatasetTypes):
        """Evaluate a filter expression and lists of
        `DatasetTypes <DatasetType>` and return a set of data unit values.

        Returned set consists of combinations of units participating in data
        transformation from ``neededDatasetTypes`` to ``futureDatasetTypes``,
        restricted by existing data and filter expression.

        Parameters
        ----------
        originInfo : `DatasetOriginInfo`
            Object which provides names of the input/output collections.
        expression : `str`
            An expression that limits the `DataUnits <DataUnit>` and
            (indirectly) the Datasets returned.
        neededDatasetTypes : `list` of `DatasetType`
            The `list` of `DatasetTypes <DatasetType>` whose DataUnits will
            be included in the returned column set. Output is limited to the
            the Datasets of these DatasetTypes which already exist in the
            registry.
        futureDatasetTypes : `list` of `DatasetType`
            The `list` of `DatasetTypes <DatasetType>` whose DataUnits will
            be included in the returned column set. It is expected that
            Datasets for these DatasetTypes do not exist in the registry,
            but presently this is not checked.

        Yields
        ------
        row : `PreFlightUnitsRow`
            Single row is a unique combination of units in a transform.
        """

        # Brief overview of the code below:
        #  - extract all DataUnits used by all input/output dataset types
        #  - build a complex SQL query to run against registry database:
        #    - first do (natural) join for all tables for all DataUnits
        #      involved based on their foreign keys
        #    - then add Join tables to the mix, only use Join tables which
        #      have their lhs/rhs links in the above DataUnits set, also
        #      ignore Joins which summarize other Joins
        #    - next join with Dataset for each input dataset type, this
        #      limits result only to existing input dataset
        #    - also do outer join with Dataset for each output dataset type
        #      to see which output datasets are already there
        #    - append user filter expression
        #    - query returns all DataUnit values, regions for region-based
        #      joins, and dataset IDs for all existing datasets
        #  - run this query
        #  - filter out records whose regions do not overlap
        #  - return result as iterator of records containing DataUnit values

        # Collect unit names in both input and output dataset types
        allUnitNames = set(itertools.chain.from_iterable(dsType.dataUnits for dsType in neededDatasetTypes))
        allUnitNames.update(itertools.chain.from_iterable(dsType.dataUnits for dsType in futureDatasetTypes))
        _LOG.debug("allUnitNames: %s", allUnitNames)

        # Build select column list
        selectColumns = []
        unitLinkColumns = {}
        for unitName in allUnitNames:
            dataUnit = self._dataUnits[unitName]
            if self._schema.tables[unitName] is not None:
                # take link column names, usually there is one
                for link in dataUnit.link:
                    unitLinkColumns[link] = len(selectColumns)
                    selectColumns.append(self._schema.tables[unitName].c[link])
        _LOG.debug("selectColumns: %s", selectColumns)
        _LOG.debug("unitLinkColumns: %s", unitLinkColumns)

        # Extend units set with the "optional" superset from schema, so that
        # joins work correctly. This may bring more tables into query than
        # really needed, potential for optimization.
        allUnitNames = set(_scanDataUnits(self._dataUnits[unitName] for unitName in allUnitNames))

        # All DataUnit instances in a subset that we need
        allDataUnits = {unitName: self._dataUnits[unitName] for unitName in allUnitNames}

        # joins for all unit tables
        fromJoin = None
        for dataUnit in _unitsTopologicalSort(allDataUnits.values()):
            if self._schema.tables[dataUnit.name] is None:
                continue
            _LOG.debug("add dataUnit: %s", dataUnit.name)
            fromJoin = self._joinOnForeignKey(fromJoin, dataUnit, dataUnit.dependencies)

        # joins between skymap and instrument units
        dataUnitJoins = [dataUnitJoin for dataUnitJoin in self._dataUnits.joins.values()
                         if dataUnitJoin.lhs.issubset(allUnitNames) and
                         dataUnitJoin.rhs.issubset(allUnitNames)]
        _LOG.debug("all dataUnitJoins: %s", [join.name for join in dataUnitJoins])

        # only use most specific joins
        dataUnitJoins = list(_filterSummarizes(dataUnitJoins))
        _LOG.debug("filtered dataUnitJoins: %s", [join.name for join in dataUnitJoins])

        joinedRegionTables = set()
        regionColumns = {}
        for dataUnitJoin in dataUnitJoins:
            # Some `DataUnitJoin`s have an associated region (e.g. they are spatial)
            # in that case they shouldn't be joined separately in the region lookup.
            if dataUnitJoin.spatial:
                continue

            # TODO: do not know yet how to handle MultiInstrumentExposureJoin,
            # skip it for now
            if dataUnitJoin.lhs == dataUnitJoin.rhs:
                continue

            # Look at each side of the DataUnitJoin and join it with
            # corresponding DataUnit tables, including making all necessary
            # joins for special multi-DataUnit region table(s).
            regionHolders = []
            for connection in (dataUnitJoin.lhs, dataUnitJoin.rhs):
                # For DataUnits like Patch we need to extend list with their required
                # units which are also spatial.
                units = []
                for dataUnitName in connection:
                    units.append(dataUnitName)
                    dataUnit = self._dataUnits[dataUnitName]
                    units += [d.name for d in dataUnit.requiredDependencies if d.spatial]
                regionHolder = self._dataUnits.getRegionHolder(*units)
                if len(connection) > 1:
                    # if one of the joins is with Visit/Detector then also bring
                    # VisitDetectorRegion table in and join it with the units
                    # TODO: need a better way to recognize this special case
                    if regionHolder.name in joinedRegionTables:
                        _LOG.debug("region table already joined with units: %s", regionHolder.name)
                    else:
                        _LOG.debug("joining region table with units: %s", regionHolder.name)
                        joinedRegionTables.add(regionHolder.name)

                        dataUnits = [self._dataUnits[dataUnitName] for dataUnitName in connection]
                        fromJoin = self._joinOnForeignKey(fromJoin, regionHolder, dataUnits)

                # add to the list of tables that we need to join with
                regionHolders.append(regionHolder)

                # We also have to include regions from each side of the join
                # into resultset so that we can filter-out non-overlapping
                # regions.
                regionColumns[regionHolder.name] = len(selectColumns)
                regionColumn = self._schema.tables[regionHolder.name].c.region
                selectColumns.append(regionColumn)

            fromJoin = self._joinOnForeignKey(fromJoin, dataUnitJoin, regionHolders)

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

            # Join sub-query with all units on their link names,
            # OUTER JOIN is used for output datasets (they don't usually exist)
            joinOn = []
            for unitName in dsType.dataUnits:
                dataUnit = allDataUnits[unitName]
                for link in dataUnit.link:
                    _LOG.debug("  joining on link: %s", link)
                    joinOn.append(subquery.c[link] == self._schema.tables[dataUnit.name].c[link])
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
        _LOG.debug("full query: %s", q)

        # execute and return result iterator
        rows = self._connection.execute(q).fetchall()
        return self._convertResultRows(rows, unitLinkColumns, regionColumns, dsIdColumns)

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
        links = set()
        for unitName in dsType.dataUnits:
            dataUnit = self._dataUnits[unitName]
            links.update(dataUnit.link)
        links = list(links)

        dsTable = self._schema.tables["Dataset"]
        dsCollTable = self._schema.tables["DatasetCollection"]

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

    def _convertResultRows(self, rowIter, unitLinkColumns, regionColumns, dsIdColumns):
        """Convert query result rows into `PreFlightUnitsRow` instances.

        Parameters
        ----------
        rowIter : iterable
            Iterator for rows returned by the query on registry
        unitLinkColumns : `dict`
            Dictionary of (unit link name, column index), column contains
            DataUnit value
        regionColumns : `dict`
            Dictionary of (DataUnit name, column index), column contains
            encoded region data
        dsIdColumns : `dict`
            Dictionary of (DatasetType, column index), column contains
            dataset Id, or None if dataset does not exist

        Yields
        ------
        row : `PreFlightUnitsRow`
        """
        total = 0
        count = 0
        for row in rowIter:

            total += 1

            # Filter result rows that have non-overlapping regions.
            # Result set generated by query in selectDataUnits() method can include
            # set of regions in each row (encoded as bytes). Due to pixel-based
            # matching some regions may not overlap, this generator method filters
            # rows that have disjoint regions. If result row contains more than two
            # regions (this should not happen with our current schema) then row is
            # filtered if any of two regions are disjoint.
            disjoint = False
            regions = [Region.decode(row[col]) for col in regionColumns.values()]
            for reg1, reg2 in itertools.combinations(regions, 2):
                if reg1.relate(reg2) == DISJOINT:
                    disjoint = True
                    break
            if disjoint:
                continue

            # for each dataset get ids DataRef
            datasetRefs = {}
            for dsType, col in dsIdColumns.items():
                linkNames = set()
                for unitName in dsType.dataUnits:
                    dataUnit = self._dataUnits[unitName]
                    if self._schema.tables[dataUnit.name] is not None:
                        linkNames.update(dataUnit.link)
                dsDataId = dict((link, row[unitLinkColumns[link]]) for link in linkNames)
                dsId = None if col is None else row[col]
                datasetRefs[dsType] = DatasetRef(dsType, dsDataId, dsId)

            dataId = dict((link, row[col]) for link, col in unitLinkColumns.items())

            count += 1
            yield PreFlightUnitsRow(dataId, datasetRefs)

        _LOG.debug("Total %d rows in result set, %d after region filtering", total, count)
