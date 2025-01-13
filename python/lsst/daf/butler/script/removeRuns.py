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
from .._collection_type import CollectionType
from ..registry import MissingCollectionError


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
    with butler.registry.caching_context():
        try:
            collections = butler.collections.query_info(
                collection,
                CollectionType.RUN,
                include_chains=False,
                include_parents=True,
                include_summary=True,
            )
        except MissingCollectionError:
            # Act as if no collections matched.
            collections = []
        dataset_types = [dt.name for dt in butler.registry.queryDatasetTypes(...)]
        dataset_types = list(butler.collections._filter_dataset_types(dataset_types, collections))

        runs = []
        datasets: dict[str, int] = defaultdict(int)
        for collection_info in collections:
            assert collection_info.type == CollectionType.RUN and collection_info.parents is not None
            runs.append(RemoveRun(collection_info.name, list(collection_info.parents)))
            with butler.query() as query:
                for dt in dataset_types:
                    results = query.datasets(dt, collections=collection_info.name)
                    count = results.count(exact=False)
                    if count:
                        datasets[dt] += count

        return runs, {k: datasets[k] for k in sorted(datasets.keys())}


def removeRuns(
    repo: str,
    collection: str,
) -> RemoveRunsResult:
    """Remove collections.

    Parameters
    ----------
    repo : `str`
        Same as the ``config`` argument to ``Butler.__init__``.
    collection : `str`
        Same as the ``name`` argument to ``Butler.removeRuns``.

    Returns
    -------
    collections : `RemoveRunsResult`
        Contains information describing what will be removed.
    """
    runs, datasets = _getCollectionInfo(repo, collection)

    def _doRemove(runs: Sequence[RemoveRun]) -> None:
        """Perform the remove step."""
        butler = Butler.from_config(repo, writeable=True)
        with butler.transaction():
            for run in runs:
                for parent in run.parents:
                    butler.collections.remove_from_chain(parent, run.name)
            butler.removeRuns([r.name for r in runs], unstore=True)

    result = RemoveRunsResult(
        onConfirmation=partial(_doRemove, runs),
        runs=runs,
        datasets=datasets,
    )
    return result
