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

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial

from astropy.table import Table

from .._butler import Butler
from ..registry import CollectionType, MissingCollectionError


@dataclass
class RemoveCollectionResult:
    """Container to return to the cli command; holds tables describing the
    collections that will be removed, as well as any found RUN collections
    which can not be removed by this command. Also holds the callback funciton
    to execute the remove upon user confirmation.
    """

    # the callback function to do the removal
    onConfirmation: Callable[[], None]
    # astropy table describing data that will be removed.
    removeCollectionsTable: Table
    # astropy table describing any run collections that will NOT be removed.
    runsTable: Table


@dataclass
class CollectionInfo:
    """Lightweight container to hold the name and type of non-run
    collections, as well as the names of run collections.
    """

    nonRunCollections: Table
    runCollections: Table


def _getCollectionInfo(
    repo: str,
    collection: str,
) -> CollectionInfo:
    """Get the names and types of collections that match the collection
    string.

    Parameters
    ----------
    repo : `str`
        The URI to the repostiory.
    collection : `str`
        The collection string to search for. Same as the `expression`
        argument to `registry.queryCollections`.

    Returns
    -------
    collectionInfo : `CollectionInfo`
        Contains tables with run and non-run collection info.
    """
    butler = Butler.from_config(repo, without_datastore=True)
    try:
        names = sorted(
            butler.registry.queryCollections(
                collectionTypes=frozenset(
                    (
                        CollectionType.RUN,
                        CollectionType.TAGGED,
                        CollectionType.CHAINED,
                        CollectionType.CALIBRATION,
                    )
                ),
                expression=collection,
                includeChains=True,
            )
        )
    except MissingCollectionError:
        names = []
    collections = Table(names=("Collection", "Collection Type"), dtype=(str, str))
    runCollections = Table(names=("Collection",), dtype=(str,))
    for name in names:
        collectionType = butler.registry.getCollectionType(name).name
        if collectionType == "RUN":
            runCollections.add_row((name,))
        else:
            collections.add_row((name, collectionType))

    return CollectionInfo(collections, runCollections)


def removeCollections(
    repo: str,
    collection: str,
) -> Table:
    """Remove collections.

    Parameters
    ----------
    repo : `str`
        Same as the ``config`` argument to ``Butler.__init__``.
    collection : `str`
        Same as the ``name`` argument to ``Registry.removeCollection``.

    Returns
    -------
    collections : `RemoveCollectionResult`
        Contains tables describing what will be removed, and
        run collections that *will not* be removed.
    """
    collectionInfo = _getCollectionInfo(repo, collection)

    def _doRemove(collections: Table) -> None:
        """Perform the prune collection step."""
        butler = Butler.from_config(repo, writeable=True, without_datastore=True)
        for name in collections["Collection"]:
            butler.registry.removeCollection(name)

    result = RemoveCollectionResult(
        onConfirmation=partial(_doRemove, collectionInfo.nonRunCollections),
        removeCollectionsTable=collectionInfo.nonRunCollections,
        runsTable=collectionInfo.runCollections,
    )
    return result
