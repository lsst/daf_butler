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


import click

from ..utils import addArgumentHelp


class repo_argument:  # noqa: N801
    """Decorator to add a repo argument to a click command.

    Parameters
    ----------
    required : bool, optional
        Indicates if the caller must pass this argument to the command, by
        default True.
    help : str, optional
        The help text for this argument to append to the command's help text.
        If None or '' then nothing will be appended to the help text (in which
        case the command should document this argument directly in its help
        text). By default the value of repo_argument.existing_repo.
    """

    will_create_repo = "REPO is the URI or path to the new repository. " \
                       "Will be created if it does not exist."
    existing_repo = "REPO is the URI or path to an existing data repository root " \
                    "or configuration file."

    def __init__(self, required=False, help=existing_repo):
        self.required = required
        self.helpText = help

    def __call__(self, f):
        f.__doc__ = addArgumentHelp(f.__doc__, self.helpText)
        return click.argument("repo", required=self.required)(f)
