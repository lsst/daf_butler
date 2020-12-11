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


from enum import Enum, auto

from . import QueryDatasets
from .. import Butler


class PruneDatasetsResult:
    """Contains the results of a prune-datasets action.

    The action may not be complete if the caller requested a confirmation, in
    which case calling ``onConfirmation`` will perform the action.

    Parameters
    ----------
    tables : `list` [``astropy.table.table``], optional
        The astropy tables that will be or were deleted, by default None.
    state : ``PruneDatasetsResult.State``, optional
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

    class State(Enum):
        INIT = auto()
        DRY_RUN_COMPLETE = auto()
        AWAITING_CONFIRMATION = auto()
        FINISHED = auto()
        ERR_PURGE_AND_DISASSOCIATE = auto()
        ERR_FIND_ALL_WITHOUT_COLLECTIONS = auto()

    def __init__(self, tables=None, state=None):
        self.state = state or self.State.INIT
        self.tables = tables
        self.onConfirmation = None

    @property
    def dryRun(self):
        return self.state is self.State.DRY_RUN_COMPLETE

    @property
    def confirm(self):
        return self.state is self.State.AWAITING_CONFIRMATION

    @property
    def finished(self):
        return self.state is self.State.FINISHED

    @property
    def errPurgeAndDisassociate(self):
        return self.state is self.State.ERR_PURGE_AND_DISASSOCIATE

    @property
    def errFindAllWithoutCollections(self):
        return self.state is self.State.ERR_FIND_ALL_WITHOUT_COLLECTIONS


def pruneDatasets(repo, glob, collections, where, disassociate_tags, unstore, purge_run, dry_run, confirm,
                  find_all):
    """Prune datasets from a repository.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    glob : iterable [`str`]
        A list of glob-style search string that fully or partially identify
        the dataset type names to search for.
    collections : iterable [`str`]
        A list of glob-style search string that fully or partially identify
        the collections to search for.
    where : `str`
        A string expression similar to a SQL WHERE clause.  May involve any
        column of a dimension table or (as a shortcut for the primary key
        column of a dimension table) dimension name.
    find_all : `bool`
        If False, for each result data ID, will only delete the dataset from
        the first collection in which a dataset of that dataset type appears
        (according to the order of ``collections`` passed in).  If used,
        ``collections`` must specify at least one expression and must not
        contain wildcards. This is the inverse of ``QueryDataset``'s find_first
        option.
    disassociate_tags : `list` [`str`]
        TAGGED collections to disassociate the datasets from. If not `None`
        then ``purge_run`` must be `None`.
    unstore : `bool`
        Same as the unstore argument to ``Butler.pruneDatasets``.
    purge_run : `str`
        Completely remove the dataset from this run in the ``Registry``.
    dry_run : `bool`
        Get results for what would be removed but do not remove.
    confirm : `bool`
        Get results for what would be removed and return the results for
        display & confirmation, with a completion function to run after
        confirmation.

    Returns
    -------
    results : ``PruneDatasetsResult``
        A data structure that contains information about datasets for removal,
        removal status, and options to continue in some cases.
    """

    if disassociate_tags and purge_run:
        return PruneDatasetsResult(state=PruneDatasetsResult.State.ERR_PURGE_AND_DISASSOCIATE)

    if find_all and not collections:
        return PruneDatasetsResult(state=PruneDatasetsResult.State.ERR_FIND_ALL_WITHOUT_COLLECTIONS)

    # If collections is not specified and a purge_run is, use the purge_run for
    # collections, or if disassociate_tags is then use that.
    if not collections:
        if purge_run:
            collections = (purge_run,)
        elif disassociate_tags:
            collections = disassociate_tags

    datasets = QueryDatasets(
        repo=repo,
        glob=glob,
        collections=collections,
        where=where,
        # By default we want find_first to be True if collections are provided
        # (else False) (find_first requires collections to be provided).
        # But the user may specify that they want to find all (thus forcing
        # find_first to be False)
        find_first=False if find_all else bool(collections),
        show_uri=False
    )

    result = PruneDatasetsResult(datasets.getTables())

    if dry_run:
        result.state = PruneDatasetsResult.State.DRY_RUN_COMPLETE
        return result

    kwargs = dict()

    if disassociate_tags:
        kwargs["disassociate"] = True
        kwargs["tags"] = disassociate_tags

    if purge_run:
        kwargs["disassociate"] = True
        kwargs["purge"] = True
        kwargs["run"] = purge_run
        kwargs["unstore"] = True

    if unstore:
        kwargs["unstore"] = True

    def doPruneDatasets():
        butler = Butler(repo, writeable=True)
        butler.pruneDatasets(refs=datasets.getDatasets(), **kwargs)
        result.state = PruneDatasetsResult.State.FINISHED
        return result

    if confirm:
        result.state = PruneDatasetsResult.State.AWAITING_CONFIRMATION
        result.onConfirmation = doPruneDatasets
        return result

    return doPruneDatasets()
