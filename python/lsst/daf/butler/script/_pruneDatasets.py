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

from collections.abc import Callable, Iterable
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from .._butler import Butler
from ..registry import CollectionType
from .queryDatasets import QueryDatasets

if TYPE_CHECKING:
    from astropy.table import Table


class PruneDatasetsResult:
    """Contains the results of a prune-datasets action.

    The action may not be complete if the caller requested a confirmation, in
    which case calling ``onConfirmation`` will perform the action.

    Parameters
    ----------
    tables : `list` [`astropy.table.Table`], optional
        The astropy tables that will be or were deleted, by default None.
    state : `PruneDatasetsResult.State`, optional
        The initial state of execution of the action, if `None` the result
        state is ``INIT``, by default None.

    Attributes
    ----------
    tables
        Same as in Parameters.
    state : ``PruneDatasetsResult.State``
        The current state of the action.
    onConfirmation : `Callable[None, None]`
        The function to call to perform the action if the caller wants to
        confirm the tables before performing the action.
    """

    action: dict[str, Any] | None
    onConfirmation: Callable | None

    class State(Enum):
        """State associated with dataset pruning request."""

        INIT = auto()
        DRY_RUN_COMPLETE = auto()
        AWAITING_CONFIRMATION = auto()
        FINISHED = auto()
        ERR_PURGE_AND_DISASSOCIATE = auto()
        ERR_NO_COLLECTION_RESTRICTION = auto()
        ERR_PRUNE_ON_NOT_RUN = auto()
        ERR_NO_OP = auto()

    def __init__(
        self,
        tables: list[Table] | None = None,
        state: State | None = None,
        errDict: dict[str, str] | None = None,
    ):
        self.state = state or self.State.INIT
        if tables is None:
            tables = []
        self.tables = tables
        self.onConfirmation = None
        # Action describes the removal action for dry-run, will be a dict with
        # keys disassociate, unstore, purge, and collections.
        self.action = None
        # errDict is a container for variables related to the error that may be
        # substituted into a user-visible string.
        self.errDict = errDict or {}

    @property
    def dryRun(self) -> bool:
        return self.state is self.State.DRY_RUN_COMPLETE

    @property
    def confirm(self) -> bool:
        return self.state is self.State.AWAITING_CONFIRMATION

    @property
    def finished(self) -> bool:
        return self.state is self.State.FINISHED

    @property
    def errPurgeAndDisassociate(self) -> bool:
        return self.state is self.State.ERR_PURGE_AND_DISASSOCIATE

    @property
    def errNoCollectionRestriction(self) -> bool:
        return self.state is self.State.ERR_NO_COLLECTION_RESTRICTION

    @property
    def errPruneOnNotRun(self) -> bool:
        return self.state is self.State.ERR_PRUNE_ON_NOT_RUN

    @property
    def errNoOp(self) -> bool:
        return self.state is self.State.ERR_NO_OP


def pruneDatasets(
    repo: str,
    collections: Iterable[str],
    datasets: Iterable[str],
    where: str,
    disassociate_tags: Iterable[str],
    unstore: bool,
    purge_run: str,
    dry_run: bool,
    confirm: bool,
    find_all: bool,
) -> PruneDatasetsResult:
    """Prune datasets from a repository.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    collections : iterable [`str`]
        A list of glob-style search string that identify the collections to
        search for.
    datasets : iterable [`str`]
        A list of glob-style search string that identify the dataset type names
        to search for.
    where : `str`
        A string expression similar to a SQL WHERE clause.  May involve any
        column of a dimension table or (as a shortcut for the primary key
        column of a dimension table) dimension name.
    disassociate_tags : `list` [`str`]
        TAGGED collections to disassociate the datasets from. If not `None`
        then ``purge_run`` must be `None`.
    unstore : `bool`
        Same as the unstore argument to ``Butler.pruneDatasets``.
    purge_run : `str`
        Completely remove datasets from the ``Registry``. Note that current
        implementation accepts any RUN-type collection, but will remove
        datasets from all collections in ``collections`` if it is non-empty.
    dry_run : `bool`
        Get results for what would be removed but do not remove.
    confirm : `bool`
        Get results for what would be removed and return the results for
        display & confirmation, with a completion function to run after
        confirmation.
    find_all : `bool`
        If False, for each result data ID, will only delete the dataset from
        the first collection in which a dataset of that dataset type appears
        (according to the order of ``collections`` passed in).  If used,
        ``collections`` must specify at least one expression and must not
        contain wildcards. This is the inverse of ``QueryDataset``'s find_first
        option.

    Notes
    -----
    The matrix of legal & illegal combinations of purge, unstore, and
    disassociate is this:
    - none of (purge, unstore, disassociate): error, nothing to do
    - purge only: ok
    - unstore only: ok
    - disassociate only: ok
    - purge+unstore: ok, just ignore unstore (purge effectively implies
      unstore)
    - purge+disassociate: this is an error (instead of ignoring disassociate),
      because that comes with a collection argument that we can't respect, and
      that might be confusing (purge will disassociate from all TAGGED
      collections, not just the one given)
    - purge+unstore+disassociate: an error, for the same reason as just
      purge+disassociate
    - unstore+disassociate: ok; these operations are unrelated to each other

    Returns
    -------
    results : `PruneDatasetsResult`
        A data structure that contains information about datasets for removal,
        removal status, and options to continue in some cases.
    """
    if not disassociate_tags and not unstore and not purge_run:
        return PruneDatasetsResult(state=PruneDatasetsResult.State.ERR_NO_OP)

    if disassociate_tags and purge_run:
        return PruneDatasetsResult(state=PruneDatasetsResult.State.ERR_PURGE_AND_DISASSOCIATE)

    # If collections is not specified and a purge_run is, use the purge_run for
    # collections, or if disassociate_tags is then use that.
    if not collections:
        if purge_run:
            collections = (purge_run,)
        elif disassociate_tags:
            collections = disassociate_tags

    if not collections:
        return PruneDatasetsResult(state=PruneDatasetsResult.State.ERR_NO_COLLECTION_RESTRICTION)

    butler = Butler.from_config(repo)

    # If purging, verify that the collection to purge is RUN type collection.
    if purge_run:
        collectionType = butler.registry.getCollectionType(purge_run)
        if collectionType is not CollectionType.RUN:
            return PruneDatasetsResult(
                state=PruneDatasetsResult.State.ERR_PRUNE_ON_NOT_RUN, errDict=dict(collection=purge_run)
            )

    datasets_found = QueryDatasets(
        repo=repo,
        glob=datasets,
        collections=collections,
        where=where,
        # By default we want find_first to be True if collections are provided
        # (else False) (find_first requires collections to be provided).
        # But the user may specify that they want to find all (thus forcing
        # find_first to be False)
        find_first=not find_all,
        show_uri=False,
    )

    result = PruneDatasetsResult(datasets_found.getTables())

    disassociate = bool(disassociate_tags) or bool(purge_run)
    purge = bool(purge_run)
    unstore = unstore or bool(purge_run)

    if dry_run:
        result.state = PruneDatasetsResult.State.DRY_RUN_COMPLETE
        result.action = dict(disassociate=disassociate, purge=purge, unstore=unstore, collections=collections)
        return result

    def doPruneDatasets() -> PruneDatasetsResult:
        butler = Butler.from_config(repo, writeable=True)
        butler.pruneDatasets(
            refs=datasets_found.getDatasets(),
            disassociate=disassociate,
            tags=disassociate_tags or (),
            purge=purge,
            unstore=unstore,
        )
        result.state = PruneDatasetsResult.State.FINISHED
        return result

    if confirm:
        result.state = PruneDatasetsResult.State.AWAITING_CONFIRMATION
        result.onConfirmation = doPruneDatasets
        return result

    return doPruneDatasets()
