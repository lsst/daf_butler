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

from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial

from .._butler import Butler
from ..registry import CollectionType, MissingCollectionError
from ..registry.queries import DatasetQueryResults


@dataclass
class RemoveRun:
    """Represents a RUN collection to remove."""

    # the name of the run:
    name: str
    # parent CHAINED collections the RUN belongs to:
    parents: list[str]


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
    runs: Sequence[RemoveRun]
    # mapping of dataset type name to how many will be removed.
    datasets: Mapping[str, int]


def _getCollectionInfo(
    repo: str,
    collection: str,
) -> tuple[list[RemoveRun], Mapping[str, int]]:
    """Get the names and types of collections that match the collection
    string.

    Parameters
    ----------
    repo : `str`
        The URI to the repository.
    collection : `str`
        The collection string to search for. Same as the `expression`
        argument to `registry.queryCollections`.

    Returns
    -------
    runs : `list` of `RemoveRun`
        Describes the runs that will be removed.
    datasets : `dict` [`str`, `int`]
        The dataset types and and how many will be removed.
    """
    butler = Butler.from_config(repo)
    try:
        collectionNames = list(
            butler.registry.queryCollections(
                collectionTypes=frozenset((CollectionType.RUN,)),
                expression=collection,
                includeChains=False,
            )
        )
    except MissingCollectionError:
        collectionNames = []
    runs = []
    datasets: dict[str, int] = defaultdict(int)
    for collectionName in collectionNames:
        assert butler.registry.getCollectionType(collectionName).name == "RUN"
        parents = butler.registry.getCollectionParentChains(collectionName)
        runs.append(RemoveRun(collectionName, list(parents)))
        all_results = butler.registry.queryDatasets(..., collections=collectionName)
        assert isinstance(all_results, DatasetQueryResults)
        for r in all_results.byParentDatasetType():
            if r.any(exact=False, execute=False):
                datasets[r.parentDatasetType.name] += r.count(exact=False)
    return runs, {k: datasets[k] for k in sorted(datasets.keys())}


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
        Same as the ``name`` argument to ``Butler.removeRuns``.

    Returns
    -------
    collections : `RemoveRunsResult`
        Contains information describing what will be removed.
    """
    runs, datasets = _getCollectionInfo(repo, collection)

    def doRemove(runs: Sequence[RemoveRun]) -> None:
        """Perform the remove step."""
        butler = Butler.from_config(repo, writeable=True)
        with butler.transaction():
            for run in runs:
                for parent in run.parents:
                    children = list(butler.registry.getCollectionChain(parent))
                    children.remove(run.name)
                    butler.registry.setCollectionChain(parent, children, flatten=False)
            butler.removeRuns([r.name for r in runs], unstore=True)

    result = RemoveRunsResult(
        onConfirmation=partial(doRemove, runs),
        runs=runs,
        datasets=datasets,
    )
    return result
