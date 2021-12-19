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


from typing import Mapping, Sequence

import click

from ... import script
from ..opt import collection_argument, confirm_option, options_file_option, repo_argument
from ..utils import ButlerCommand

# messages emitted by remove-runs, defined separately for use in unit
# tests.
noRunCollectionsMsg = "No RUN collections were found."
willRemoveRunsMsg = "The following RUN collections will be removed:"
willRemoveDatasetsMsg = "The following datasets will be removed:"
didRemoveRunsMsg = "The following RUN collections were removed:"
didRemoveDatasetsMsg = "The following datasets were removed:"
removedRunsMsg = "Removed collections"
abortedMsg = "Aborted."


def _print_remove(will: bool, runs: Sequence[str], datasets: Mapping[str, int]):
    """Print the formatted remove statement.

    Parameters
    ----------
    will : bool
        True if remove "will" happen, False if the remove "did" happen.
    runs : Sequence[str]
        The RUNs that will be or were removed.
    datasets : Mapping[str, int]
        The dataset types & count that will be or were removed.
    """
    print(willRemoveRunsMsg if will else didRemoveRunsMsg)
    print(", ".join(runs))
    print(willRemoveDatasetsMsg if will else didRemoveDatasetsMsg)
    print(", ".join([f"{i[0]}({i[1]})" for i in datasets.items()]))


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@collection_argument(
    help="COLLECTION is a glob-style expression that identifies the RUN collection(s) to remove."
)
@confirm_option()
@options_file_option()
def remove_runs(**kwargs):
    """Remove one or more RUN collections.

    This command can be used to remove RUN collections and the datasets within
    them.
    """
    confirm = kwargs.pop("confirm")
    result = script.removeRuns(**kwargs)
    canRemoveRuns = len(result.runs)
    if not canRemoveRuns:
        print(noRunCollectionsMsg)
        return
    if confirm:
        _print_remove(True, result.runs, result.datasets)
        doContinue = click.confirm("Continue?", default=False)
        if doContinue:
            result.onConfirmation()
            print(removedRunsMsg)
        else:
            print(abortedMsg)
    else:
        result.onConfirmation()
        _print_remove(False, result.runs, result.datasets)
