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

from collections.abc import Iterable

from astropy.table import Table
from numpy import array

from .._butler import Butler


def queryDatasetTypes(
    repo: str, verbose: bool, glob: Iterable[str], collections: Iterable[str] | None = None
) -> Table:
    """Get the dataset types in a repository.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    verbose : `bool`
        If false only return the name of the dataset types. If false return
        name, dimensions, and storage class of each dataset type.
    glob : iterable [`str`]
        A list of glob-style search string that fully or partially identify
        the dataset type names to search for.
    collections : iterable [`str`] or `None`, optional
        Constrains resulting dataset types such that only dataset type
        found (at some point) in these collections will be returned.

    Returns
    -------
    dataset_types_table : `astropy.table.Table`
        A dict whose key is "datasetTypes" and whose value is a list of
        collection names.
    """
    butler = Butler.from_config(repo, without_datastore=True)
    expression = glob or ...
    datasetTypes = butler.registry.queryDatasetTypes(expression=expression)

    if collections:
        collections_info = butler.collections.query_info(collections, include_summary=True)
        filtered_dataset_types = set(
            butler.collections._filter_dataset_types([d.name for d in datasetTypes], collections_info)
        )
        datasetTypes = [d for d in datasetTypes if d.name in filtered_dataset_types]

    if verbose:
        table = Table(
            array(
                [(d.name, str(list(d.dimensions.names)) or "None", d.storageClass_name) for d in datasetTypes]
            ),
            names=("name", "dimensions", "storage class"),
        )
    else:
        rows = ([d.name for d in datasetTypes],)
        table = Table(rows, names=("name",))
    table.sort("name")
    return table
