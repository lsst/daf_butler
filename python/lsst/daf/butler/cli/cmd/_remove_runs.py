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

__all__ = ["remove_runs"]

from collections.abc import Mapping, Sequence
from typing import Any

import click

from ... import script
from ..opt import collection_argument, confirm_option, options_file_option, repo_argument
from ..utils import ButlerCommand
from .commands import existingRepoHelp

# messages emitted by remove-runs, defined separately for use in unit
# tests.
noRunCollectionsMsg = "No RUN collections were found."
willRemoveRunsMsg = "The following RUN collections will be removed:"
willRemoveDatasetsMsg = "The following datasets will be removed:"
didRemoveRunsMsg = "The following RUN collections were removed:"
didRemoveDatasetsMsg = "The following datasets were removed:"
removedRunsMsg = "Removed collections"
abortedMsg = "Aborted."
requiresConfirmationMsg = (
    "Removing runs that are in parent CHAINED collections requires confirmation. "
    "\nTry again without --no-confirm to confirm removal of RUN collections from parents, "
    "or add the --force flag to skip confirmation."
)
willUnlinkMsg = "{run}: will be unlinked from {parents}"
didUnlinkMsg = "{run}: was removed and unlinked from {parents}"
mustBeUnlinkedMsg = "{run}: must be unlinked from {parents}"


def _quoted(items: Sequence[str]) -> list[str]:
    return [f'"{i}"' for i in items]


def _print_remove(will: bool, runs: Sequence[script.RemoveRun], datasets: Mapping[str, int]) -> None:
    """Print the formatted remove statement.

    Parameters
    ----------
    will : `bool`
        True if remove "will" happen, False if the remove "did" happen.
    runs : `~collections.abc.Sequence` [`str`]
        The RUNs that will be or were removed.
    datasets : `~collections.abc.Mapping` [`str`, `int`]
        The dataset types & count that will be or were removed.
    """
    print(willRemoveRunsMsg if will else didRemoveRunsMsg)
    unlinkMsg = willUnlinkMsg if will else didUnlinkMsg
    for run in runs:
        if run.parents:
            print(unlinkMsg.format(run=run.name, parents=", ".join(_quoted(run.parents))))
        else:
            print(run.name)
    print("\n" + willRemoveDatasetsMsg if will else didRemoveDatasetsMsg)
    print(", ".join([f"{i[0]}({i[1]})" for i in datasets.items()]))


def _print_requires_confirmation(runs: Sequence[script.RemoveRun], datasets: Mapping[str, int]) -> None:
    print(requiresConfirmationMsg)
    for run in runs:
        if run.parents:
            print(mustBeUnlinkedMsg.format(run=run.name, parents=", ".join(_quoted(run.parents))))


@click.command(cls=ButlerCommand)
@click.pass_context
@repo_argument(
    help=existingRepoHelp,
    required=True,
)
@collection_argument(
    help="COLLECTION is a glob-style expression that identifies the RUN collection(s) to remove."
)
@confirm_option()
@click.option(
    "--force",
    is_flag=True,
    help="Required to remove RUN collections from parent collections if using --no-confirm.",
)
@options_file_option()
def remove_runs(context: click.Context, confirm: bool, force: bool, **kwargs: Any) -> None:
    """Remove one or more RUN collections.

    This command can be used to remove RUN collections and the datasets within
    them.

    Parameters
    ----------
    context : `click.Context`
        Context provided by Click.
    confirm : `bool`
        Confirmation for removal of the run.
    force : `bool`
        Force removal.
    **kwargs : `dict` [`str`, `str`]
        The parameters to pass to `~lsst.daf.butler.script.removeRuns`.
    """
    result = script.removeRuns(**kwargs)
    canRemoveRuns = len(result.runs)
    if not canRemoveRuns:
        print(noRunCollectionsMsg)
        return
    if confirm:
        _print_remove(True, result.runs, result.datasets)
        doContinue = click.confirm(text="Continue?", default=False)
        if doContinue:
            result.onConfirmation()
            print(removedRunsMsg)
        else:
            print(abortedMsg)
    else:
        # if the user opted out of confirmation but there are runs with
        # parent collections then they must confirm; print a message
        # and exit.
        if any(run.parents for run in result.runs) and not force:
            _print_requires_confirmation(result.runs, result.datasets)
            context.exit(1)
        result.onConfirmation()
        _print_remove(False, result.runs, result.datasets)
