# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

import dataclasses
from collections import defaultdict
from collections.abc import Iterable
from types import EllipsisType
from typing import TYPE_CHECKING

import numpy as np
from astropy.table import Table as AstropyTable

from .._butler import Butler
from ..cli.utils import sortAstropyTable

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef
    from lsst.daf.butler.registry.queries import DatasetQueryResults
    from lsst.resources import ResourcePath


@dataclasses.dataclass(frozen=True)
class _RefInfo:
    datasetRef: DatasetRef
    uri: str | None


class _Table:
    """Aggregates rows for a single dataset type, and creates an astropy table
    with the aggregated data. Eliminates duplicate rows.
    """

    datasetRefs: set[_RefInfo]

    def __init__(self) -> None:
        self.datasetRefs = set()

    def add(self, datasetRef: DatasetRef, uri: ResourcePath | None = None) -> None:
        """Add a row of information to the table.

        ``uri`` is optional but must be the consistent; provided or not, for
        every call to a ``_Table`` instance.

        Parameters
        ----------
        datasetRef : `DatasetRef`
            A dataset ref that will be added as a row in the table.
        uri : `lsst.resources.ResourcePath`, optional
            The URI to show as a file location in the table, by default None
        """
        uri_str = str(uri) if uri else None
        self.datasetRefs.add(_RefInfo(datasetRef, uri_str))

    def getAstropyTable(self, datasetTypeName: str) -> AstropyTable:
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
        dimensions = [
            refInfo.datasetRef.dataId.universe.dimensions[k]
            for k in refInfo.datasetRef.dataId.dimensions.data_coordinate_keys
        ]
        columnNames = ["type", "run", "id", *[str(item) for item in dimensions]]

        # Need to hint the column types for numbers since the per-row
        # constructor of Table does not work this out on its own and sorting
        # will not work properly without.
        typeMap = {float: np.float64, int: np.int64}
        columnTypes = [
            None,
            None,
            str,
            *[typeMap.get(type(value)) for value in refInfo.datasetRef.dataId.full_values],
        ]
        if refInfo.uri:
            columnNames.append("URI")
            columnTypes.append(None)

        rows = []
        for refInfo in self.datasetRefs:
            row = [
                datasetTypeName,
                refInfo.datasetRef.run,
                str(refInfo.datasetRef.id),
                *refInfo.datasetRef.dataId.full_values,
            ]
            if refInfo.uri:
                row.append(refInfo.uri)
            rows.append(row)

        dataset_table = AstropyTable(np.array(rows), names=columnNames, dtype=columnTypes)
        return sortAstropyTable(dataset_table, dimensions, ["type", "run"])


class QueryDatasets:
    """Get dataset refs from a repository.

    Parameters
    ----------
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
    repo : `str` or `None`
        URI to the location of the repo or URI to a config file describing the
        repo and its location. One of `repo` and `butler` must be `None` and
        the other must not be `None`.
    butler : `lsst.daf.butler.Butler` or `None`
        The butler to use to query. One of `repo` and `butler` must be `None`
        and the other must not be `None`.

    """

    def __init__(
        self,
        glob: Iterable[str],
        collections: Iterable[str],
        where: str,
        find_first: bool,
        show_uri: bool,
        repo: str | None = None,
        butler: Butler | None = None,
    ):
        if (repo and butler) or (not repo and not butler):
            raise RuntimeError("One of repo and butler must be provided and the other must be None.")
        # show_uri requires a datastore.
        without_datastore = not show_uri
        self.butler = butler or Butler.from_config(repo, without_datastore=without_datastore)
        self._getDatasets(glob, collections, where, find_first)
        self.showUri = show_uri

    def _getDatasets(
        self, glob: Iterable[str], collections: Iterable[str], where: str, find_first: bool
    ) -> None:
        datasetTypes = glob or ...
        query_collections: Iterable[str] | EllipsisType = collections or ...

        self.datasets = self.butler.registry.queryDatasets(
            datasetType=datasetTypes, collections=query_collections, where=where, findFirst=find_first
        ).expanded()

    def getTables(self) -> list[AstropyTable]:
        """Get the datasets as a list of astropy tables.

        Returns
        -------
        datasetTables : `list` [``astropy.table._Table``]
            A list of astropy tables, one for each dataset type.
        """
        tables: dict[str, _Table] = defaultdict(_Table)
        if not self.showUri:
            for dataset_ref in self.datasets:
                tables[dataset_ref.datasetType.name].add(dataset_ref)
        else:
            d = list(self.datasets)
            ref_uris = self.butler.get_many_uris(d, predict=True)
            for ref, uris in ref_uris.items():
                if uris.primaryURI:
                    tables[ref.datasetType.name].add(ref, uris.primaryURI)
                for name, uri in uris.componentURIs.items():
                    tables[ref.datasetType.componentTypeName(name)].add(ref, uri)

        return [table.getAstropyTable(datasetTypeName) for datasetTypeName, table in tables.items()]

    def getDatasets(self) -> DatasetQueryResults:
        """Get the datasets as a list of ``DatasetQueryResults``.

        Returns
        -------
        refs : `lsst.daf.butler.registry.queries.DatasetQueryResults`
            Dataset references matching the given query criteria.
        """
        return self.datasets
