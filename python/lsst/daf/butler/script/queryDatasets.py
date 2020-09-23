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

from .. import Butler
from ..core.utils import globToRegex


def queryDatasets(repo, glob, collections, where, deduplicate, components):
    """Get dataset refs from a repository.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    glob : iterable [`str`]
        A list of glob-style search string that fully or partially identify
        the dataset type names to search for.
    collections : iterabe [`str`]
        A list of glob-style search string that fully or partially identify
        the collections to search for.
    where : `str`
        A string expression similar to a SQL WHERE clause.  May involve any
        column of a dimension table or (as a shortcut for the primary key
        column of a dimension table) dimension name.
    deduplicate : `bool`
        For each result data ID, only yield one DatasetRef of each DatasetType,
        from the first collection in which a dataset of that dataset type
        appears (according to the order of `collections` passed in).  If used,
        `collections` must specify at least one expression and must not contain
        wildcards.
    components : `str`
        One of "ALL", "NONE", or "UNMATCHED". If "UNMATCHED": apply patterns to
        components only if their parent datasets were not matched by the
        expression. If "ALL": apply all dataset expression patterns to
        component. If "NONE": never apply patterns to components.
        Fully-specified component datasets are always included.
    Returns
    -------
    refs : a generator `queries.DatasetQueryResults`
        Dataset references matching the given query criteria.
    """
    butler = Butler(repo)
    dataset = globToRegex(glob)

    if collections and not deduplicate:
        collections = globToRegex(collections)
    elif not collections:
        collections = ...

    if components == "ALL":
        components = True
    elif components == "NONE":
        components = False
    elif components == "UNMATCHED":
        components = None
    else:
        raise RuntimeError(f"Unrecognized value for components: {components}")

    if not dataset:
        dataset = ...
    return butler.registry.queryDatasets(datasetType=dataset,
                                         collections=collections,
                                         where=where,
                                         deduplicate=deduplicate,
                                         components=components)
