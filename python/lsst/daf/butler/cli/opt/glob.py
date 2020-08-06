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

from ..utils import addArgumentHelp, MWOption, MWOptionDecorator, split_commas, ParameterType, textTypeStr


class glob_parameter(MWOptionDecorator):  # noqa: N801
    """Decorator to add an glob option or argument to a click command.

    Parameters
    ----------
    help : `str`, optional
        The help text to use for the option.
    multiple : `bool`, optional
        If True, multiple instances of the option may be passed in on the
        command line, by default False.
    parameterType : `ParameterType`, optional
        Indicates if the decorator should add a `click.Argument` or a
        `click.Option`, by default ParameterType.OPTION
    required : bool, optional
        If True, the option is required to be passed in on the command line, by
        default False.
    """

    defaultHelpMultiple = "GLOB is one or more strings to apply to the search."

    @staticmethod
    def defaultHelp():
        return "GLOB is a string to apply to the search."

    @staticmethod
    def optionFlags():
        return ("--glob",)

    def __init__(self, help=None, multiple=False, parameterType=ParameterType.OPTION, required=False):
        self.help = help or self.defaultHelp()
        self.callback = split_commas if multiple else None
        self.multiple = multiple
        self.parameterType = parameterType
        self.required = required

        if self.help == self.defaultHelp and self.multiple:
            self.help = self.defaultHelpMultiple

    def __call__(self, f):
        if self.parameterType == ParameterType.OPTION:
            return click.option(*self.optionFlags(), cls=MWOption,
                                callback=self.callback,
                                help=self.help,
                                metavar=textTypeStr(self.multiple),
                                multiple=self.multiple,
                                required=self.required)(f)
        else:
            f.__doc__ = addArgumentHelp(f.__doc__, self.help)
            return click.argument("glob",
                                  callback=self.callback,
                                  metavar="GLOB ..." if self.multiple else "GLOB",
                                  nargs=-1 if self.multiple else 1,
                                  required=self.required)(f)
