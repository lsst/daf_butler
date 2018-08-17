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
from sqlalchemy.sql import select, and_, text, literal

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

    Returns
    -------
    Iterator for DataUnitJoin which do not summarize any of the DataUnitJoins
    in the input set.
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

    Oredering is based on dependencies, units with no dependencies
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


def _joinOnForeignKey(fromClause, dataUnit, otherDataUnits):
    """Add new table for join clause.

    Assumption here is that this unit table has a foreign key to all other
    tables and names of columns are the same in both tables, so we just get
    primary key columns from other tables and join on them.

    Parameters
    ----------
    fromClause : `sqlalchemy.FromClause`
        May be ``None``, in that case ``otherDataUnits`` is expected to be
        empty and is ignored.
    dataUnit : `DataUnit`
        DataUnit to join with ``fromClause``.
    otherDataUnits : iterable of `DataUnit`
        DataUnits whose tableshave PKs for ``dataUnit`` table's FK. They all
        have to be in ``fromClause`` already.

    Returns
    -------
    fromClause : `sqlalchemy.FromClause`
        SQLAlchemy FROM clause extended with new join.
    """
    if fromClause is None:
        # starting point, first table in JOIN
        return dataUnit.table
    else:
        joinOn = []
        for otherUnit in otherDataUnits:
            for name, col in otherUnit.primaryKeyColumns.items():
                joinOn.append(dataUnit.table.c[name] == col)
            _LOG.debug("join %s with %s on columns %s", dataUnit.name,
                       otherUnit.name, list(otherUnit.primaryKeyColumns.keys()))
        if joinOn:
            return fromClause.join(dataUnit.table, and_(*joinOn))
        else:
            # need explicit cross join here, ugly with SQLAlchemy
            return fromClause.join(dataUnit.table, literal(True))


class SqlPreFlight:
    """Class implementing part of preflight solver which extracts
    units data from registry.

    This is an implementation detail only to be used by SqlRegistry class,
    not supposed to be used anywhere else.

    Parameters
    ----------
    schema : `Schema`
        Schema instance
    connection : `sqlalchmey.Connection`
        Connection to use for database access.
    """
    def __init__(self, schema, connection):
        self._schema = schema
        self._connection = connection

    def selectDataUnits(self, collections, expr, neededDatasetTypes, futureDatasetTypes):
        """Evaluate a filter expression and lists of
        `DatasetTypes <DatasetType>` and return a set of data unit values.

        Returned set consists of combinations of units participating in data
        transformation from ``neededDatasetTypes`` to ``futureDatasetTypes``,
        restricted by existing data and filter expression.

        Parameters
        ----------
        collections : `list` of `str`
            An ordered `list` of collections indicating the Collections to
            search for Datasets.
        expr : `str`
            An expression that limits the `DataUnits <DataUnit>` and
            (indirectly) the Datasets returned.
        neededDatasetTypes : `list` of `DatasetType`
            The `list` of `DatasetTypes <DatasetType>` whose DataUnits will
            be included in the returned column set. Output is limited by the
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
        #  - build a complex SQL query to run agains registry database:
        #    - first do (natural) join for all tables for all DataUnits
        #      involved based on their foreing keys
        #    - then add Join tables to the mix, only use Join tables which
        #      have their lhs/rhs links in the above DataUnits set, also
        #      ignore Joins which summarize other Joins
        #    - next join with Dataset for each input dataset type, this
        #      limits resukt only to existing input dataset
        #    - append user filter expression
        #    - query returns all DataUnit values and regions for
        #      region-based Joins
        #  - run this query
        #  - filter out records whose regions do not overlap
        #  - return result as iterator of records containing DataUnit values

        # for now only a single input collection is supported
        if len(collections) != 1:
            raise ValueError("Only single collection is supported by makeDataGraph()")
        collection = collections[0]

        # Collect unit names in both input and output dataset types
        allUnitNames = set(itertools.chain.from_iterable(dsType.dataUnits for dsType in neededDatasetTypes))
        allUnitNames.update(itertools.chain.from_iterable(dsType.dataUnits for dsType in futureDatasetTypes))
        _LOG.debug("allUnitNames: %s", allUnitNames)

        # Build select column list
        selectColumns = []
        unitLinkColumns = {}
        for unitName in allUnitNames:
            dataUnit = self._schema.dataUnits[unitName]
            if dataUnit.table is not None:
                # take link column names, usually there is one
                for link in dataUnit.link:
                    unitLinkColumns[link] = len(selectColumns)
                    selectColumns.append(dataUnit.table.c[link])
        _LOG.debug("selectColumns: %s", selectColumns)
        _LOG.debug("unitLinkColumns: %s", unitLinkColumns)

        # Extend units set with the "optional" superset from schema, so that
        # joins work correctly. This may bring more tables into query than
        # really needed, potential for optimization.
        allUnitNames = set(_scanDataUnits(self._schema.dataUnits[unitName] for unitName in allUnitNames))

        # All DataUnit instances in a subset that we need
        allDataUnits = {unitName: self._schema.dataUnits[unitName] for unitName in allUnitNames}

        # joins for all unit tables
        fromJoin = None
        where = []
        for dataUnit in _unitsTopologicalSort(allDataUnits.values()):
            if dataUnit.table is None:
                continue
            _LOG.debug("add dataUnit: %s", dataUnit.name)
            fromJoin = _joinOnForeignKey(fromJoin, dataUnit, dataUnit.dependencies)

        # joins between skymap and camera units
        dataUnitJoins = [dataUnitJoin for dataUnitJoin in self._schema.dataUnits.joins.values()
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

            # TODO: do not know yet how to handle MultiCameraExposureJoin,
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
                    dataUnit = self._schema.dataUnits[dataUnitName]
                    units += [d.name for d in dataUnit.requiredDependencies if d.spatial]
                regionHolder = self._schema.dataUnits.getRegionHolder(*units)
                if len(connection) > 1:
                    # if one of the joins is with Visit/Sensor then also bring
                    # VisitSensorRegion table in and join it with the units
                    # TODO: need a better way to recognize this special case
                    if regionHolder.name in joinedRegionTables:
                        _LOG.debug("region table already joined with units: %s", regionHolder.name)
                    else:
                        _LOG.debug("joining region table with units: %s", regionHolder.name)
                        joinedRegionTables.add(regionHolder.name)

                        dataUnits = [self._schema.dataUnits[dataUnitName] for dataUnitName in connection]
                        fromJoin = _joinOnForeignKey(fromJoin, regionHolder, dataUnits)

                # add to the list of tables that we need to join with
                regionHolders.append(regionHolder)

                # We also have to include regions from each side of the join
                # into resultset so that we can filter-out non-overlapping
                # regions.
                regionColumns[regionHolder.name] = len(selectColumns)
                selectColumns.append(regionHolder.regionColumn)

            fromJoin = _joinOnForeignKey(fromJoin, dataUnitJoin, regionHolders)

        _LOG.debug("units where: %s", [str(x) for x in where])

        # join with input datasets to restrict to existing inputs
        dsIdColumns = {}
        dsTable = self._schema._metadata.tables["Dataset"]
        dsCollTable = self._schema._metadata.tables["DatasetCollection"]
        for dsType in neededDatasetTypes:
            _LOG.debug("joining dataset: %s", dsType.name)
            dsAlias = dsTable.alias("ds" + dsType.name)
            dsCollAlias = dsCollTable.alias("dsColl" + dsType.name)

            joinOn = []
            for unitName in dsType.dataUnits:
                dataUnit = allDataUnits[unitName]
                for link in dataUnit.link:
                    _LOG.debug("joining on link: %s", link)
                    joinOn.append(dsAlias.c[link] == dataUnit.table.c[link])
            fromJoin = fromJoin.join(dsAlias, and_(*joinOn))
            fromJoin = fromJoin.join(dsCollAlias,
                                     dsAlias.c["dataset_id"] == dsCollAlias.c["dataset_id"])

            where += [dsAlias.c["dataset_type_name"] == dsType.name,
                      dsCollAlias.c["collection"] == collection]

            dsIdColumns[dsType] = len(selectColumns)
            selectColumns.append(dsAlias.c["dataset_id"])

        _LOG.debug("datasets where: %s", [str(x) for x in where])

        # For output datasets we need them in dsIdColumns so that
        # _convertResultRows knows what to do. For now we don't have their ID's
        # so we set column index to None, in the future we'll try to find IDs
        # of existing output datasets if there are any.
        for dsType in futureDatasetTypes:
            dsIdColumns[dsType] = None

        # build full query
        q = select(selectColumns).select_from(fromJoin)
        if expr:
            # TODO: potentially transform query from user-friendly expression
            where += [text(expr)]
        if where:
            where = and_(*where)
            _LOG.debug("full where: %s", where)
            q = q.where(where)
        _LOG.debug("full query: %s", q)

        # execute and return result iterator
        rows = self._connection.execute(q).fetchall()
        return self._convertResultRows(rows, unitLinkColumns, regionColumns, dsIdColumns)

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
                    dataUnit = self._schema.dataUnits[unitName]
                    if dataUnit.table is not None:
                        linkNames.update(dataUnit.link)
                dsDataId = dict((link, row[unitLinkColumns[link]]) for link in linkNames)
                dsId = None if col is None else row[col]
                datasetRefs[dsType] = DatasetRef(dsType, dsDataId, dsId)

            dataId = dict((link, row[col]) for link, col in unitLinkColumns.items())

            count += 1
            yield PreFlightUnitsRow(dataId, datasetRefs)

        _LOG.debug("Total %d rows in result set, %d after region filtering", total, count)
