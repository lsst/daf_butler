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

__all__ = ("retrieveArtifacts",)

import logging

from .._butler import Butler

log = logging.getLogger(__name__)


def retrieveArtifacts(
    repo, destination, dataset_type, collections, where, find_first, transfer, preserve_path, clobber
):
    """Parameters are those required for querying datasets plus a destination
    URI.

    Parameters
    ----------
    repo : `str`
        URI string of the Butler repo to use.
    destination : `str`
        URI string of the directory to write the artifacts.
    dataset_type : `tuple` of `str`
        Dataset type names. An empty tuple implies all dataset types.
    collections : `tuple` of `str`
        Names of collection globs to match. An empty tuple implies all
        collections.
    where : `str`
        Query modification string.
    find_first : `bool`
        Whether only the first match should be used.
    transfer : `str`
        Transfer mode to use when placing artifacts in the destination.
    preserve_path : `bool`
        If `True` the full datastore path will be retained within the
        destination directory, else only the filename will be used.
    clobber : `bool`
        If `True` allow transfers to overwrite files at the destination.

    Returns
    -------
    transferred : `list` of `lsst.resources.ResourcePath`
        The destination URIs of every transferred artifact.
    """
    if not dataset_type:
        dataset_type = ...

    if not collections:
        collections = ...

    butler = Butler(repo, writeable=False)

    # Need to store in list so we can count the number to give some feedback
    # to caller.
    refs = list(
        butler.registry.queryDatasets(
            datasetType=dataset_type, collections=collections, where=where, findFirst=find_first
        )
    )

    log.info("Number of datasets matching query: %d", len(refs))

    transferred = butler.retrieveArtifacts(
        refs, destination=destination, transfer=transfer, preserve_path=preserve_path, overwrite=clobber
    )
    return transferred
