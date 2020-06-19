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

from ..utils import (split_commas,
                     split_kv as split_kv_func,
                     split_kv_separator)


class config_option:  # noqa: N801
    """A decorator that adds a config option to a click command.

    Parameters
    ----------
    help : `str`, optional
        The help text to use for the option, by default `defaultHelp`
    multiple : `bool`, optional
        If true, multiple instances of the option may be passed in on the
        command line, by default False
    required : bool, optional
        If true, the option is required to be passed in on the command line, by
        default False
    split_kv : `bool`, optional
        If true, values that contain commas will be split into multiple values
        at the comma, by default False.
    """

    defaultHelp = "Config override, as a key-value pair."

    def __init__(self, help=defaultHelp, multiple=False, required=False, split_kv=False):
        self.help = help
        self.multiple = multiple
        self.required = required
        if split_kv:
            self.callback = partial(split_kv_func,
                                    separator=split_kv_separator,
                                    multiple=multiple)
        elif multiple is True:
            self.callback = split_commas
        else:
            self.callback = None

    def __call__(self, f):
        return click.option("-c", "--config",
                            callback=self.callback,
                            help=self.help,
                            multiple=self.multiple,
                            required=self.required)(f)
