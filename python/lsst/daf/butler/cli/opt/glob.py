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

from ..utils import addArgumentHelp, ParameterType, textTypeStr


class glob_parameter:  # noqa: N801
    """Decorator to add an glob option or argument to a click command.
    """

    ARGUMENT = ParameterType.ARGUMENT
    OPTION = ParameterType.OPTION

    defaultHelp = "GLOB is a string to apply to the search."
    defaultHelpMultiple = "GLOB is one or more strings to apply to the search."

    def __init__(self, parameterType=ParameterType.OPTION, required=False, help=defaultHelp,
                 multiple=False):
        self.parameterType = parameterType
        self.required = required
        self.help = help
        self.multiple = multiple
        if self.help == self.defaultHelp and self.multiple:
            self.help = self.defaultHelpMultiple

    def __call__(self, f):
        if self.parameterType == ParameterType.OPTION:
            return click.option("--glob",
                                multiple=self.multiple,
                                required=self.required,
                                help=self.help,
                                metavar=textTypeStr(self.multiple))(f)
        else:
            f.__doc__ = addArgumentHelp(f.__doc__, self.help)
            return click.argument("glob",
                                  required=self.required,
                                  nargs=-1 if self.multiple else 1,
                                  metavar="GLOB ..." if self.multiple else "GLOB")(f)
