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

from ..utils import MWOption, MWOptionDecorator, split_commas


class dataset_type_option(MWOptionDecorator):  # noqa: N801
    """A decorator that adds a datsety-type option to a click command.

    Parameters
    ----------
    help : `str`, optional
        The help text to use for the option.
    multiple : `bool`, optional
        If true, multiple instances of the option may be passed in on the
        command line, by default False.
    required : bool, optional
        If True, the option is required to be passed in on the command line, by
        default False.
    """

    @staticmethod
    def defaultHelp():
        return "Dataset types(s)"

    @staticmethod
    def optionFlags():
        return ("-d", "--dataset-type")

    def __init__(self, help=None, multiple=False, required=False):
        self.callback = split_commas if multiple else None
        self.help = help or self.defaultHelp()
        self.multiple = multiple
        self.required = required

    def __call__(self, f):
        return click.option(*self.optionFlags(), cls=MWOption,
                            callback=self.callback,
                            help=self.help,
                            multiple=self.multiple,
                            required=self.required)(f)
