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

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from astropy.table import Table

from .._butler import (
    Butler,
    PurgeUnsupportedPruneCollectionsError,
    PurgeWithoutUnstorePruneCollectionsError,
    RunWithoutPurgePruneCollectionsError,
)
from ..registry import CollectionType
from .queryDatasets import QueryDatasets


class PruneCollectionResult:
    def __init__(self, confirm: bool) -> None:
        # if `confirm == True`, will contain the astropy table describing data
        # that will be removed.
        self.removeTable: None | Table = None
        # the callback function to do the work
        self.onConfirmation: None | Callable[[], None] = None
        # true if the user should be shown what will be removed before pruning
        # the collection.
        self.confirm: bool = confirm


def pruneCollection(
    repo: str, collection: str, purge: bool, unstore: bool, unlink: list[str], confirm: bool
) -> Table:
    """Remove a collection and possibly prune datasets within it.

    Parameters
    ----------
    repo : `str`
        Same as the ``config`` argument to ``Butler.__init__``
    collection : `str`
        Same as the ``name`` argument to ``Butler.pruneCollection``.
    purge : `bool`, optional
        Same as the ``purge`` argument to ``Butler.pruneCollection``.
    unstore: `bool`, optional
        Same as the ``unstore`` argument to ``Butler.pruneCollection``.
    unlink: `list` [`str`]
        Same as the ``unlink`` argument to ``Butler.pruneCollection``.
    confirm : `bool`
        If `True` will produce a table of collections that will be removed for
        display to the user.

    Returns
    -------
    collections : `astropy.table.Table`
        The table containing collections that will be removed, their type, and
        the number of datasets in the collection if applicable.
    """

    @dataclass
    class CollectionInfo:
        """Lightweight container to hold the type of collection and the number
        of datasets in the collection if applicable."""

        count: int | None
        type: str

    result = PruneCollectionResult(confirm)
    if confirm:
        print("Searching collections...")
        butler = Butler(repo)
        collectionNames = list(
            butler.registry.queryCollections(
                collectionTypes=frozenset(
                    (
                        CollectionType.RUN,
                        CollectionType.TAGGED,
                        CollectionType.CHAINED,
                        CollectionType.CALIBRATION,
                    )
                ),
                expression=(collection,),
                includeChains=True,
            )
        )

        collections: dict[str, CollectionInfo] = {}

        def addCollection(name: str) -> None:
            """Add a collection to the collections, recursive if the collection
            being added can contain collections."""
            collectionType = butler.registry.getCollectionType(name).name
            collections[name] = CollectionInfo(0 if collectionType == "RUN" else None, collectionType)
            if collectionType == "CHAINED":
                for c in butler.registry.getCollectionChain(name):
                    addCollection(c)

        for name in collectionNames:
            addCollection(name)

        collections = {k: collections[k] for k in sorted(collections.keys())}

        queryDatasets = QueryDatasets(
            repo=repo,
            glob=[],
            collections=[collection],
            where="",
            find_first=True,
            show_uri=False,
        )
        for datasetRef in queryDatasets.getDatasets():
            assert datasetRef.run is not None, "This must be a resolved dataset ref"
            collectionInfo = collections[datasetRef.run]
            if collectionInfo.count is None:
                raise RuntimeError(f"Unexpected dataset in collection of type {collectionInfo.type}")
            collectionInfo.count += 1

        result.removeTable = Table(
            [
                list(collections.keys()),
                [v.type for v in collections.values()],
                [v.count if v.count is not None else "-" for v in collections.values()],
            ],
            names=("Collection", "Collection Type", "Number of Datasets"),
        )

    def doRemove() -> None:
        """Perform the prune collection step."""
        butler = Butler(repo, writeable=True)
        try:
            butler.pruneCollection(collection, purge, unstore, unlink)
        except PurgeWithoutUnstorePruneCollectionsError as e:
            raise TypeError("Cannot pass --purge without --unstore.") from e
        except RunWithoutPurgePruneCollectionsError as e:
            raise TypeError(f"Cannot prune RUN collection {e.collectionType.name} without --purge.") from e
        except PurgeUnsupportedPruneCollectionsError as e:
            raise TypeError(
                f"Cannot prune {e.collectionType} collection {e.collectionType.name} with --purge."
            ) from e

    result.onConfirmation = doRemove
    return result
