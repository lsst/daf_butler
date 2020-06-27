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

from ..utils import MWOption


class long_log_option:  # noqa: N801

    defaultHelp = "Make log messages appear in long format."

    def __init__(self, help=defaultHelp):
        """A decorator to add a long_log option to a click.Command.

        Parameters
        ----------
        help : str, optional
            The help text to use, by default defaultHelp
        """
        self.help = help

    def __call__(self, f):
        return click.option("--long-log", cls=MWOption,
                            help=self.help,
                            is_flag=True)(f)
