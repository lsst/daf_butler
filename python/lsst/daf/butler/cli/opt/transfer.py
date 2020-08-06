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
from ..utils import MWOption, MWOptionDecorator


class transfer_option(MWOptionDecorator):  # noqa: N801
    """A decorator to add a transfer option to a click.Command.

    Parameters
    ----------
    help : `str`, optional
        The help text to use for the option.
    required : bool, optional
        If True, the option is required to be passed in on the command line, by
        default False.
    """

    choices = ["auto", "link", "symlink", "hardlink", "copy", "move", "relsymlink"]

    @staticmethod
    def defaultHelp():
        return "The external data transfer mode."

    @staticmethod
    def optionFlags():
        return ("-t", "--transfer")

    def __init__(self, help=None, required=False):
        self.help = help or self.defaultHelp()
        self.required = required

    def __call__(self, f):
        kwargs = dict(help=self.help,
                      type=click.Choice(self.choices, case_sensitive=False))
        # If the option is required, do not provide a default value (it
        # prevents the CLI execution from failing if the option is not
        # provided).
        if self.required:
            kwargs["required"] = True
        else:
            kwargs["default"] = "auto"
        return click.option(*self.optionFlags(), cls=MWOption, **kwargs)(f)
