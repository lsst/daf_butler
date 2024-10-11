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

import logging
import warnings
from collections import defaultdict
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any

import numpy as np
from astropy.table import Table as AstropyTable

from .._butler import Butler
from ..cli.utils import sortAstropyTable
from ..utils import has_globs

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef
    from lsst.resources import ResourcePath


_LOG = logging.getLogger(__name__)


class _Table:
    """Aggregates rows for a single dataset type, and creates an astropy table
    with the aggregated data. Eliminates duplicate rows.
    """

    datasetRefs: dict[DatasetRef, str | None]

    def __init__(self) -> None:
        self.datasetRefs = {}

    def add(self, datasetRef: DatasetRef, uri: ResourcePath | None = None) -> None:
        """Add a row of information to the table.

        ``uri`` is optional but must be the consistent; provided or not, for
        every call to a ``_Table`` instance.

        Parameters
        ----------
        datasetRef : `DatasetRef`
            A dataset ref that will be added as a row in the table.
        uri : `lsst.resources.ResourcePath`, optional
            The URI to show as a file location in the table, by default `None`.
        """
        uri_str = str(uri) if uri else None
        # Use a dict to retain ordering.
        self.datasetRefs[datasetRef] = uri_str

    def getAstropyTable(self, datasetTypeName: str, sort: bool = True) -> AstropyTable:
        """Get the table as an astropy table.

        Parameters
        ----------
        datasetTypeName : `str`
            The dataset type name to show in the ``type`` column of the table.
        sort : `bool`, optional
            If `True` the table will be sorted.

        Returns
        -------
        table : `astropy.table._Table`
            The table with the provided column names and rows.
        """
        # Should never happen; adding a dataset should be the action that
        # causes a _Table to be created.
        if not self.datasetRefs:
            raise RuntimeError(f"No DatasetRefs were provided for dataset type {datasetTypeName}")

        ref = next(iter(self.datasetRefs))
        dimensions = [ref.dataId.universe.dimensions[k] for k in ref.dataId.dimensions.data_coordinate_keys]
        columnNames = ["type", "run", "id", *[str(item) for item in dimensions]]

        # Need to hint the column types for numbers since the per-row
        # constructor of Table does not work this out on its own and sorting
        # will not work properly without.
        typeMap = {float: np.float64, int: np.int64}
        columnTypes = [
            None,
            None,
            str,
            *[typeMap.get(type(value)) for value in ref.dataId.full_values],
        ]
        if self.datasetRefs[ref]:
            columnNames.append("URI")
            columnTypes.append(None)

        rows = []
        for ref, uri in self.datasetRefs.items():
            row = [
                datasetTypeName,
                ref.run,
                str(ref.id),
                *ref.dataId.full_values,
            ]
            if uri:
                row.append(uri)
            rows.append(row)

        dataset_table = AstropyTable(np.array(rows), names=columnNames, dtype=columnTypes)
        if sort:
            return sortAstropyTable(dataset_table, dimensions, ["type", "run"])
        else:
            return dataset_table


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
    limit : `int`, optional
        Limit the number of results to be returned. A value of 0 means
        unlimited. A negative value is used to specify a cap where a warning
        is issued if that cap is hit.
    order_by : `tuple` of `str`
        Dimensions to use for sorting results. If no ordering is given the
        results of ``limit`` are undefined and default sorting of the resulting
        datasets will be applied. It is an error if the requested ordering
        is inconsistent with the dimensions of the dataset type being queried.
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
        limit: int = 0,
        order_by: tuple[str, ...] = (),
        repo: str | None = None,
        butler: Butler | None = None,
    ):
        if (repo and butler) or (not repo and not butler):
            raise RuntimeError("One of repo and butler must be provided and the other must be None.")
        collections = list(collections)
        if not collections:
            warnings.warn(
                "No --collections specified.  The --collections argument will become mandatory after v28.",
                FutureWarning,
            )
        glob = list(glob)
        if not glob:
            warnings.warn(
                "No dataset types specified.  Explicitly specifying dataset types will become mandatory"
                " after v28. Specify '*' to match the current behavior of querying all dataset types.",
                FutureWarning,
            )

        # show_uri requires a datastore.
        without_datastore = not show_uri
        self.butler = butler or Butler.from_config(repo, without_datastore=without_datastore)
        self.showUri = show_uri
        self._dataset_type_glob = glob
        self._collections_wildcard = collections
        self._where = where
        self._find_first = find_first
        self._limit = limit
        self._order_by = order_by

    def getTables(self) -> Iterator[AstropyTable]:
        """Get the datasets as a list of astropy tables.

        Yields
        ------
        datasetTables : `collections.abc.Iterator` [``astropy.table._Table``]
            Astropy tables, one for each dataset type.
        """
        # Sort if we haven't been told to enforce an order.
        sort_table = not bool(self._order_by)

        if not self.showUri:
            for refs in self.getDatasets():
                table = _Table()
                for ref in refs:
                    table.add(ref)
                if refs:
                    yield table.getAstropyTable(refs[0].datasetType.name, sort=sort_table)
        else:
            for refs in self.getDatasets():
                if not refs:
                    continue
                # For URIs of disassembled composites we create a table per
                # component.
                tables: dict[str, _Table] = defaultdict(_Table)
                dataset_type_name = refs[0].datasetType.name
                ref_uris = self.butler.get_many_uris(refs, predict=True)
                for ref, uris in ref_uris.items():
                    if uris.primaryURI:
                        tables[dataset_type_name].add(ref, uris.primaryURI)
                    for name, uri in uris.componentURIs.items():
                        tables[ref.datasetType.componentTypeName(name)].add(ref, uri)
                for name in sorted(tables):
                    yield tables[name].getAstropyTable(name, sort=sort_table)
        return

    def getDatasets(self) -> Iterator[list[DatasetRef]]:
        """Get the datasets as a list of lists.

        Yields
        ------
        refs : `collections.abc.Iterator` [ `list [ `DatasetRef` ] ]
            Dataset references matching the given query criteria grouped
            by dataset type.
        """
        datasetTypes = self._dataset_type_glob
        query_collections: Iterable[str] = self._collections_wildcard or ["*"]

        # Currently need to use old interface to get all the matching
        # dataset types and loop over the dataset types executing a new
        # query each time.
        dataset_types = set(self.butler.registry.queryDatasetTypes(datasetTypes or ...))
        n_dataset_types = len(dataset_types)
        if n_dataset_types == 0:
            _LOG.info("The given dataset type, %s, is not known to this butler.", datasetTypes)
            return

        # Expand the collections query and include summary information.
        query_collections_info = self.butler.collections.query_info(
            query_collections,
            include_summary=True,
            flatten_chains=True,
            include_chains=False,
            summary_datasets=dataset_types,
        )
        expanded_query_collections = [c.name for c in query_collections_info]
        if self._find_first and has_globs(query_collections):
            raise RuntimeError("Can not use wildcards in collections when find_first=True")
        query_collections = expanded_query_collections

        # Only iterate over dataset types that are relevant for the query.
        dataset_type_names = {dataset_type.name for dataset_type in dataset_types}
        dataset_type_collections = self.butler.collections._group_by_dataset_type(
            dataset_type_names, query_collections_info
        )

        if (n_filtered := len(dataset_type_collections)) != n_dataset_types:
            _LOG.info("Filtered %d dataset types down to %d", n_dataset_types, n_filtered)
        else:
            _LOG.info("Processing %d dataset type%s", n_dataset_types, "" if n_dataset_types == 1 else "s")

        # Accumulate over dataset types.
        limit = self._limit
        warn_limit = False
        unlimited = True if limit == 0 else False
        if limit < 0:
            # Must track this limit in the loop rather than relying on
            # butler.query_datasets() because this loop knows there are more
            # possible dataset types to query.
            warn_limit = True
            limit = abs(limit) + 1  # +1 to tell us we hit the limit.
        for dt, collections in sorted(dataset_type_collections.items()):
            kwargs: dict[str, Any] = {}
            if self._where:
                kwargs["where"] = self._where
            # API uses 0 to mean "check query but return nothing" and None
            # to mean "unlimited".
            kwargs["limit"] = None if unlimited else limit
            _LOG.debug("Querying dataset type %s with %s", dt, kwargs)
            results = self.butler.query_datasets(
                dt,
                collections=collections,
                find_first=self._find_first,
                with_dimension_records=True,
                order_by=self._order_by,
                explain=False,
                **kwargs,
            )
            if not unlimited:
                limit -= len(results)
                if warn_limit and limit == 0 and results:
                    # We asked for one too many so must remove that from
                    # the list.
                    results.pop(-1)
            _LOG.debug("Got %d results for dataset type %s", len(results), dt)
            yield results

            if not unlimited and limit == 0:
                if warn_limit:
                    _LOG.warning(
                        "Requested limit of %d hit for number of datasets returned. "
                        "Use --limit to increase this limit.",
                        self._limit,
                    )
                break
