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

__all__ = ["remove_collections"]

from typing import Any

import click

from ... import script
from ..opt import collection_argument, confirm_option, options_file_option, repo_argument
from ..utils import ButlerCommand

# messages emitted by remove-collections, defined separately for use in unit
# tests.
noNonRunCollectionsMsg = "No non-RUN collections were found."
willRemoveCollectionMsg = "The following collections will be removed:"
removedCollectionsMsg = "Removed collections"
canNotRemoveFoundRuns = "The following RUN collections were found but can NOT be removed by this command:"
didNotRemoveFoundRuns = "Found RUN collections but they can NOT be removed by this command:"
abortedMsg = "Aborted."


@click.command(cls=ButlerCommand)
@repo_argument(required=True)
@collection_argument(
    help="COLLECTION is a glob-style expression that identifies the collection(s) to remove."
)
@confirm_option()
@options_file_option()
def remove_collections(**kwargs: Any) -> None:
    """Remove one or more non-RUN collections.

    This command can be used to remove only non-RUN collections. If RUN
    collections are found when searching for collections (and the --no-confirm
    flag is not used), then they will be shown in a separate table during
    confirmation, but they will not be removed.

    Use the remove-runs subcommand to remove RUN collections.
    """
    confirm: bool = kwargs.pop("confirm")
    result = script.removeCollections(**kwargs)
    canRemoveCollections = len(result.removeCollectionsTable)
    doContinue = canRemoveCollections
    if confirm:
        if canRemoveCollections:
            print("\n" + willRemoveCollectionMsg)
            result.removeCollectionsTable.pprint_all(align="<")
        else:
            print("\n" + noNonRunCollectionsMsg)
        if len(result.runsTable):
            print("\n" + canNotRemoveFoundRuns)
            result.runsTable.pprint_all(align="<")
            print()
        if canRemoveCollections:
            doContinue = click.confirm(text="Continue?", default=False)
    if doContinue:
        result.onConfirmation()
        if confirm:
            print("\n" + removedCollectionsMsg + ".\n")
        else:
            print("\n" + removedCollectionsMsg + ":\n")
            result.removeCollectionsTable.pprint_all(align="<")
            if len(result.runsTable):
                print("\n" + didNotRemoveFoundRuns)
                result.runsTable.pprint_all(align="<")
                print()
    elif canRemoveCollections and not doContinue:
        print("\n" + abortedMsg + "\n")
