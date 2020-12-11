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
from __future__ import annotations

from astropy.table import Table as AstropyTable
from collections import defaultdict, namedtuple
from typing import Any, Dict
import numpy as np

from .. import Butler
from ..core.utils import globToRegex
from ..cli.utils import sortAstropyTable


_RefInfo = namedtuple("RefInfo", "datasetRef uri")


class _Table:
    """Aggregates rows for a single dataset type, and creates an astropy table
    with the aggregated data. Eliminates duplicate rows.
    """

    def __init__(self):
        self.datasetRefs = set()

    def add(self, datasetRef, uri=None):
        """Add a row of information to the table.

        ``uri`` is optional but must be the consistent; provided or not, for
        every call to a ``_Table`` instance.

        Parameters
        ----------
        datasetRef : ``DatasetRef``
            A dataset ref that will be added as a row in the table.
        uri : ``ButlerURI``, optional
            The URI to show as a file location in the table, by default None
        """
        if uri:
            uri = str(uri)
        self.datasetRefs.add(_RefInfo(datasetRef, uri))

    def getAstropyTable(self, datasetTypeName):
        """Get the table as an astropy table.

        Parameters
        ----------
        datasetTypeName : `str`
            The dataset type name to show in the ``type`` column of the table.

        Returns
        -------
        table : `astropy.table._Table`
            The table with the provided column names and rows.
        """
        # Should never happen; adding a dataset should be the action that
        # causes a _Table to be created.
        if not self.datasetRefs:
            raise RuntimeError(f"No DatasetRefs were provided for dataset type {datasetTypeName}")

        refInfo = next(iter(self.datasetRefs))
        dimensions = list(refInfo.datasetRef.dataId.full.keys())
        columnNames = ["type", "run", "id",
                       *[str(item) for item in dimensions]]

        # Need to hint the column types for numbers since the per-row
        # constructor of Table does not work this out on its own and sorting
        # will not work properly without.
        typeMap = {float: np.float, int: np.int64}
        columnTypes = [None, None, np.int64,
                       *[typeMap.get(type(value)) for value in refInfo.datasetRef.dataId.full.values()]]
        if refInfo.uri:
            columnNames.append("URI")
            columnTypes.append(None)

        rows = []
        for refInfo in self.datasetRefs:
            row = [datasetTypeName,
                   refInfo.datasetRef.run,
                   refInfo.datasetRef.id,
                   *[value for value in refInfo.datasetRef.dataId.full.values()]]
            if refInfo.uri:
                row.append(refInfo.uri)
            rows.append(row)

        dataset_table = AstropyTable(np.array(rows), names=columnNames, dtype=columnTypes)
        return sortAstropyTable(dataset_table, dimensions, ["type", "run"])


class QueryDatasets:
    """Get dataset refs from a repository.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    glob : iterable [`str`]
        A list of glob-style search string that fully or partially identify
        the dataset type names to search for.
    collections : iterable [`str`]
        A list of glob-style search string that fully or partially identify
        the collections to search for.
    where : `str`
        A string expression similar to a SQL WHERE clause.  May involve any
        column of a dimension table or (as a shortcut for the primary key
        column of a dimension table) dimension name.
    find_first : `bool`
        For each result data ID, only yield one DatasetRef of each DatasetType,
        from the first collection in which a dataset of that dataset type
        appears (according to the order of `collections` passed in).  If used,
        `collections` must specify at least one expression and must not contain
        wildcards.
    show_uri : `bool`
        If True, include the dataset URI in the output.
    """

    def __init__(self, repo, glob, collections, where, find_first, show_uri):
        self.butler = Butler(repo)
        self._getDatasets(glob, collections, where, find_first)
        self.showUri = show_uri

    def _getDatasets(self, glob, collections, where, find_first):
        dataset: Any = globToRegex(glob)
        if not dataset:
            dataset = ...

        if collections and not find_first:
            collections = globToRegex(collections)
        elif not collections:
            collections = ...

        self.datasets = self.butler.registry.queryDatasets(datasetType=dataset,
                                                           collections=collections,
                                                           where=where,
                                                           findFirst=find_first)

    def getTables(self):
        """Get the datasets as a list of astropy tables.

        Returns
        -------
        datasetTables : `list` [``astropy.table._Table``]
            A list of astropy tables, one for each dataset type.
        """
        tables: Dict[str, _Table] = defaultdict(_Table)
        for datasetRef in self.datasets:
            if not self.showUri:
                tables[datasetRef.datasetType.name].add(datasetRef)
            else:
                primaryURI, componentURIs = self.butler.getURIs(datasetRef, collections=datasetRef.run)
                if primaryURI:
                    tables[datasetRef.datasetType.name].add(datasetRef, primaryURI)
                for name, uri in componentURIs.items():
                    tables[datasetRef.datasetType.componentTypeName(name)].add(datasetRef, uri)

        return [table.getAstropyTable(datasetTypeName) for datasetTypeName, table in tables.items()]

    def getDatasets(self):
        """Get the datasets as a list of ``DatasetQueryResults``.

        Returns
        -------
        refs : ``queries.DatasetQueryResults``
            Dataset references matching the given query criteria.
        """
        return self.datasets
