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

from astropy.table import Table as AstropyTable
from collections import defaultdict, namedtuple
from numpy import array

from .. import Butler
from ..core.utils import globToRegex

_RefInfo = namedtuple("RefInfo", "datasetRef uri")


class _Table:
    """Aggregates rows for a single dataset type, and creates an astropy table
    with the aggregated data. Eliminates duplicate rows.

    Parameters
    ----------
    columnNames : `list` [`str`]
        The names of columns.
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
        columnNames = ["type", "run", "id",
                       *[str(item) for item in refInfo.datasetRef.dataId.keys()]]
        if refInfo.uri:
            columnNames.append("URI")

        rows = []
        for refInfo in sorted(self.datasetRefs):
            row = [datasetTypeName,
                   refInfo.datasetRef.run,
                   refInfo.datasetRef.id,
                   *[str(value) for value in refInfo.datasetRef.dataId.values()]]
            if refInfo.uri:
                row.append(refInfo.uri)
            rows.append(row)

        return AstropyTable(array(rows), names=columnNames)


def queryDatasets(repo, glob, collections, where, find_first, show_uri):
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
    Returns
    -------
    datasetTables : `list` [``astropy.table._Table``]
        A list of astropy tables, one for each dataset type.
    """
    butler = Butler(repo)

    dataset = globToRegex(glob)
    if not dataset:
        dataset = ...

    if collections and not find_first:
        collections = globToRegex(collections)
    elif not collections:
        collections = ...

    datasets = butler.registry.queryDatasets(datasetType=dataset,
                                             collections=collections,
                                             where=where,
                                             deduplicate=find_first)

    tables = defaultdict(_Table)

    for datasetRef in datasets:
        if not show_uri:
            tables[datasetRef.datasetType.name].add(datasetRef)
        else:
            primaryURI, componentURIs = butler.getURIs(datasetRef, collections=datasetRef.run)
            if primaryURI:
                tables[datasetRef.datasetType.name].add(datasetRef, primaryURI)
            for name, uri in componentURIs.items():
                tables[datasetRef.datasetType.componentTypeName(name)].add(datasetRef, uri)

    return [table.getAstropyTable(datasetTypeName) for datasetTypeName, table in tables.items()]
