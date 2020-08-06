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

from ..utils import MWOption, MWOptionDecorator, split_commas, unwrap


class config_file_option(MWOptionDecorator):  # noqa: N801
    """A decorator that adds a config-file option to a click command.

    Parameters
    ----------
    help : `str`, optional
        The help text to use for the option.
    metavar : `str`, optional
        How the type is represented in the help page.
    multiple : `bool`, optional
        If true, multiple instances of the option may be passed in on the
        command line, by default False.
    required : bool, optional
        If True, the option is required to be passed in on the command line, by
        default False.
    type : a python type or a `click.ParamType`, optional
        The type that should be used for the value. Python types will be
        converted into a `click.ParamType` automatically if supported.
    """

    @staticmethod
    def defaultHelp():
        return unwrap("""Path to a pex config override to be included after the Instrument config overrides
                      are applied.""")

    @staticmethod
    def optionFlags():
        return ("-C", "--config-file")

    def __init__(self, help=None, metavar=None, multiple=False, required=False, type=None):
        self.callback = split_commas if multiple else None
        self.help = help or self.defaultHelp()
        self.metavar = metavar
        self.multiple = multiple
        self.required = required
        self.type = type

    def __call__(self, f):
        return click.option(*self.optionFlags(), cls=MWOption,
                            callback=self.callback,
                            help=self.help,
                            metavar=self.metavar,
                            multiple=self.multiple,
                            required=self.required,
                            type=self.type)(f)
