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

__all__ = ("retrieveArtifacts",)

import itertools
import logging
from typing import TYPE_CHECKING

from .._butler import Butler
from .queryDatasets import QueryDatasets

if TYPE_CHECKING:
    from lsst.resources import ResourcePath

log = logging.getLogger(__name__)


def retrieveArtifacts(
    repo: str,
    destination: str,
    dataset_type: tuple[str, ...],
    collections: tuple[str, ...],
    where: str,
    find_first: bool,
    limit: int,
    order_by: tuple[str, ...],
    transfer: str,
    preserve_path: bool,
    clobber: bool,
) -> list[ResourcePath]:
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
    limit : `int`
        Limit the number of results to be returned. A value of 0 means
        unlimited. A negative value is used to specify a cap where a warning
        is issued if that cap is hit.
    order_by : `tuple` of `str`
        Dimensions to use for sorting results. If no ordering is given the
        results of ``limit`` are undefined and default sorting of the resulting
        datasets will be applied. It is an error if the requested ordering
        is inconsistent with the dimensions of the dataset type being queried.
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
    query_types = dataset_type or "*"
    query_collections: tuple[str, ...] = collections or ("*",)

    butler = Butler.from_config(repo, writeable=False)

    # Need to store in list so we can count the number to give some feedback
    # to caller.
    query = QueryDatasets(
        butler=butler,
        glob=query_types,
        collections=query_collections,
        where=where,
        find_first=find_first,
        limit=limit,
        order_by=order_by,
        show_uri=False,
    )
    refs = list(itertools.chain(*query.getDatasets()))
    log.info("Number of datasets matching query: %d", len(refs))

    transferred = butler.retrieveArtifacts(
        refs, destination=destination, transfer=transfer, preserve_path=preserve_path, overwrite=clobber
    )
    return transferred
