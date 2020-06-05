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

import fnmatch
import re

from .. import Butler


def _translateExpr(expr):
    """Translate glob-style search terms to regex.

    Parameters
    ----------
    expr : `str` or `...`
        A glob-style pattern string to convert, or an Ellipsis.

    Returns
    -------
    expressions : [`str` or `...`]
        A list of expressions that are either regex or Ellipsis.
    """
    if expr == ...:
        return expr
    return re.compile(fnmatch.translate(expr))


def queryDatasetTypes(repo, verbose, glob):
    """Get the dataset types in a repository.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    verbose : `bool`
        If false only return the name of the dataset types. If false return
        name, dimensions, and storage class of each dataset type.
    glob : [`str`]
        A list of glob-style search string that fully or partially identify
        the dataset type names to search for.

    Returns
    -------
    collections : `dict` [`str`, [`str`]]
        A dict whose key is 'datasetTypes' and whose value is a list of
        collection names.
    """
    butler = Butler(repo)
    kwargs = dict()
    if glob:
        kwargs['expression'] = [_translateExpr(g) for g in glob]
    datasetTypes = butler.registry.queryDatasetTypes(**kwargs)
    if verbose:
        info = [dict(name=datasetType.name,
                     dimensions=list(datasetType.dimensions.names),
                     storageClass=datasetType.storageClass.name)
                for datasetType in datasetTypes]
    else:
        info = [datasetType.name for datasetType in datasetTypes]
    return {'datasetTypes': info}
