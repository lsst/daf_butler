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
from functools import partial

from ..utils import MWOption, MWOptionDecorator, split_kv
from ...core.utils import iterable


class log_level_option(MWOptionDecorator):  # noqa: N801
    """A decorator to add a log_level option to a click.Command.

    Parameters
    ----------
    help : str, optional
        The help text to use, by default defaultHelp
    defaultValue : str, optional
        The default value to use if this option is not required. By default
        `log_level_option.defaultValue`.
    required : bool, optional
        If true then the caller must pass this option when calling the
        command. By default False.
    """

    choices = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]
    defaultValue = "WARNING"

    @staticmethod
    def defaultHelp():
        return "The Python log level to use."

    @staticmethod
    def optionFlags():
        return ("--log-level",)

    def __init__(self, defaultValue=defaultValue, help=None, required=False):
        self.help = help or self.defaultHelp()
        self.isEager = True
        self.multiple = True
        self.required = required
        self.default = None if required else defaultValue

    def __call__(self, f):
        return click.option(*self.optionFlags(), cls=MWOption,
                            callback=partial(split_kv,
                                             choice=click.Choice(self.choices, case_sensitive=False),
                                             normalize=True,
                                             unseparated_okay=True),
                            default=iterable(self.default) if self.default is not None else None,
                            is_eager=self.isEager,
                            help=self.help,
                            multiple=self.multiple,
                            required=self.required)(f)
