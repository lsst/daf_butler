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

from . import QueryDatasets
from .. import Butler


def exportCollection(butler, repoExportContext, names):
    """Add collection names to a butler export context.
    Parameters
    ----------
    butler : ``lsst.daf.butler.Butler``
        The butler that datasets are being exported from.
    repoExportContext : ``lsst.daf.butler.transfers.RepoExportContext``
        The current export context.
    names : `list` [`str`]
        A list of collection names to add to the export context.
    """
    for name in names:
        repoExportContext.saveCollection(name)


def exportDataIds(butler, repoExportContext, dimensions, collections, datasets, where):
    """Add data ids to a butler export context.

    Parameters
    ----------
    butler : ``lsst.daf.butler.Butler``
        The butler that datasets are being exported from.
    repoExportContext : ``lsst.daf.butler.transfers.RepoExportContext``
        The current export context.

    Other parameters are the same as ``Butler.Registry.queryDataIds``.
    """

    dataIds = butler.registry.queryDataIds(dimensions,
                                            collections=collections,
                                            datasets=datasets,
                                            where=where)
    repoExportContext.saveDataIds(dataIds)


def exportDatasets(butler, repoExportContext, glob, collections, where, find_first):
    """Add datasets to a butler export context.

    Parameters
    ----------
    butler : ``lsst.daf.butler.Butler``
        The butler that datasets are being exported from.
    repoExportContext : ``lsst.daf.butler.transfers.RepoExportContext``
        The current export context.

    Other parameters are the same as ``lsst.daf.butler.cli.script.QueryDatasets``.
    """
    query = QueryDatasets(butler=butler, glob=glob, collections=collections, where=where,
                          find_first=find_first, show_uri=False, repo=None)
    repoExportContext.saveDatasets(query.getDatasets())
