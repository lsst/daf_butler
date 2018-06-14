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
import copy

from sqlalchemy.sql import select, and_, text

import lsst.log as lsstLog


_LOG = lsstLog.Log.getLogger(__name__)


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

        Returns
        -------
        header : `tuple` of `tuple`
            Length of tuple equals the number of columns in the returned
            result set. Each item is a tuple with two elements - DataUnit
            name (e.g. "Visit") and unit value name (e.g. "visit").
        rows : iterable of `tuple`
            Result set, this can be a single-pass iterator. Each tuple
            contains unit values corresponding to units in a header.
        """

        # for now only a single collection is supported
        if len(collections) != 1:
            raise ValueError("Only single collection is supported by makeDataGraph()")
        collection = collections[0]

        # Collect unit names in both input and output dataset types
        allUnits = set(itertools.chain.from_iterable(dsType.dataUnits for dsType in neededDatasetTypes))
        allUnits.update(itertools.chain.from_iterable(dsType.dataUnits for dsType in futureDatasetTypes))
        _LOG.debug("allUnits: %s", allUnits)

        # All DataUnit instances in a subset that we need
        allDataUnits = {unitName: self._schema.dataUnits[unitName] for unitName in allUnits}

        # potentially we have to extend units set with the "required" superset
        # from schema, for now just assume that set is full.

        # Temporary workaround for AbstractFilter "view" which is not defined in schema yet,
        # make a subquery for it and use it as an alias for that sub-query
        dataUnit = allDataUnits.get("AbstractFilter")
        if dataUnit and dataUnit.table is None:
            # make a copy, do not change object in schema
            dataUnit = copy.copy(dataUnit)
            filterTable = self._schema.dataUnits["PhysicalFilter"].table
            subquery = select([filterTable.c["abstract_filter"]]).distinct()
            dataUnit._table = subquery.alias("AbstractFilter")
            allDataUnits["AbstractFilter"] = dataUnit

        # Build select column list
        selectColumns = []
        header = []            # tuple (UnitName, link_name) for each returned column
        for dataUnit in allDataUnits.values():
            if dataUnit.table is not None:
                # take link column names, usually there is one
                for link in dataUnit.link:
                    header.append((dataUnit.name, link))
                    selectColumns.append(dataUnit.table.c[link])
        _LOG.debug("selectColumns: %s", selectColumns)
        _LOG.debug("header: %s", header)

        # joins for all unit tables
        where = []
        for dataUnit in allDataUnits.values():
            table = dataUnit.table
            if table is None:
                continue
            _LOG.debug("add unit table: %s", table)

            for other in dataUnit.requiredDependencies:
                _LOG.debug("joining to: %s", other)
                for link in other.link:
                    _LOG.debug("joining on link: %s", link)
                    where.append(table.c[link] == other.table.c[link])

            for other in dataUnit.optionalDependencies:
                if other.name in allDataUnits:
                    _LOG.debug("joining to: %s", other)
                    for link in other.link:
                        _LOG.debug("joining on link: %s", link)
                        where.append(table.c[link] == other.table.c[link])
                else:
                    _LOG.debug("not joining to: %s", other)
                    pass

        _LOG.debug("units where: %s", [str(x) for x in where])

        # join with input datasets to restrict to existing inputs
        dsTable = self._schema._metadata.tables['Dataset']
        dsCollTable = self._schema._metadata.tables['DatasetCollection']
        for dsType in neededDatasetTypes:
            _LOG.debug("joining dataset: %s", dsType.name)
            dsAlias = dsTable.alias("ds" + dsType.name)
            dsCollAlias = dsCollTable.alias("dsColl" + dsType.name)

            for unitName in dsType.dataUnits:
                dataUnit = allDataUnits[unitName]
                for link in dataUnit.link:
                    _LOG.debug("joining on link: %s", link)
                    where.append(dsAlias.c[link] == dataUnit.table.c[link])

            where += [dsAlias.c['dataset_id'] == dsCollAlias.c['dataset_id'],
                      dsAlias.c['dataset_type_name'] == dsType.name,
                      dsCollAlias.c['collection'] == collection]
        _LOG.debug("datasets where: %s", [str(x) for x in where])

        # build full query
        q = select(selectColumns)
        if expr:
            # TODO: potentially transform query from user-friendly expression
            where += [text(expr)]
        if where:
            where = and_(*where)
            _LOG.debug("full where: %s", where)
            q = q.where(where)
        _LOG.debug("full query: %s", q)

        # execute and return header and result iterator
        rows = self._connection.execute(q).fetchall()
        return tuple(header), (tuple(row) for row in rows)
