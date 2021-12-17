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


from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from typing import Callable, Dict, List, Mapping, Sequence, Tuple

from .. import Butler
from ..registry import CollectionType, MissingCollectionError
from ..registry.queries import DatasetQueryResults


@dataclass
class RemoveRunsResult:
    """Container to return to the cli command.

    Contains the names of runs that will be deleted, and a map of dataset type
    to how many of that dataset will be deleted. Also contains the callback
    function to execute the remove upon user confirmation.
    """
    # the callback function to do the removal
    onConfirmation: Callable[[], None]
    # list of the run collections that will be removed
    runs: Sequence[str]
    # mapping of dataset type name to how many will be removed.
    datasets: Mapping[str, int]


def _getCollectionInfo(
    repo: str,
    collection: str,
) -> Tuple[List[str], Mapping[str, int]]:
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
    runs : `list` of `str`
        The runs that will be removed.
    datasets : `dict` [`str`, `int`]
        The dataset types and and how many will be removed.
    """
    butler = Butler(repo)
    try:
        collectionNames = list(
            butler.registry.queryCollections(
                collectionTypes=frozenset((CollectionType.RUN,)),
                expression=collection,
                includeChains=False,
            )
        )
    except MissingCollectionError:
        collectionNames = list()
    runs = []
    datasets: Dict[str, int] = defaultdict(int)
    for collectionName in collectionNames:
        assert butler.registry.getCollectionType(collectionName).name == "RUN"
        runs.append(collectionName)
        all_results = butler.registry.queryDatasets(..., collections=collectionName)
        assert isinstance(all_results, DatasetQueryResults)
        for r in all_results.byParentDatasetType():
            datasets[r.parentDatasetType.name] += r.count(exact=False)
    return runs, datasets


def removeRuns(
    repo: str,
    collection: str,
) -> RemoveRunsResult:
    """Remove collections.

    Parameters
    ----------
    repo : `str`
        Same as the ``config`` argument to ``Butler.__init__``
    collection : `str`
        Same as the ``name`` argument to ``Butler.pruneCollection``.

    Returns
    -------
    collections : `RemoveRunsResult`
        Contains information describing what will be removed.
    """
    runs, datasets = _getCollectionInfo(repo, collection)

    def doRemove(runs: Sequence[str]) -> None:
        """Perform the remove step."""
        butler = Butler(repo, writeable=True)
        butler.removeRuns(runs, unstore=True)

    result = RemoveRunsResult(
        onConfirmation=partial(doRemove, runs),
        runs=runs,
        datasets=datasets,
    )
    return result
