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

from ..utils import to_upper


class log_level_option:  # noqa: N801

    defaultHelp = "The Python log level to use."

    # the default default value
    defaultValue = "WARNING"

    # the allowed values
    choices = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]

    optionKey = "log_level"

    def __init__(self, help=defaultHelp, required=False, defaultValue=defaultValue):
        """A decorator to add a log_level option to a click.Command.

        Parameters
        ----------
        help : str, optional
            The help text to use, by default defaultHelp
        required : bool, optional
            If true then the caller must pass this option when calling the
            command. By default False
        defaultValue : str, optional
            The default value to use if this option is not required. By default
            `log_level_option.defaultValue`
        """
        self.help = help
        self.isEager = True
        self.required = required
        self.default = None if required else defaultValue

    def __call__(self, f):
        return click.option("--log-level",
                            callback=to_upper,
                            default=self.default,
                            is_eager=self.isEager,
                            help=self.help,
                            required=self.required,
                            type=click.Choice(self.choices, case_sensitive=False))(f)
