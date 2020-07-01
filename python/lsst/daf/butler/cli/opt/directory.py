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


class directory_argument:  # noqa: N801
    """Decorator to add a directory argument to a click command.

    Parameters
    ----------
    help : `str`, optional
        The help text to use for the option.
    required : bool, optional
        If True, the option is required to be passed in on the command line, by
        default False.
    """

    defaultHelp = "DIRECTORY is the folder containing dataset files."

    def __init__(self, help=defaultHelp, required=False):
        self.required = required
        self.helpText = help

    def __call__(self, f):
        f.__doc__ = addArgumentHelp(f.__doc__, self.helpText)
        return click.argument("directory", required=self.required)(f)
