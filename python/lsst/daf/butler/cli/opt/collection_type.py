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

from lsst.daf.butler.cli.utils import MWOption
from lsst.daf.butler.registry import CollectionType


class collection_type_option:  # noqa: N801
    """Decorator to add a collection type option to a click command.

    Converts the type option from string to a CollectionType enum value.
    """

    defaultHelp = "If provided, only list collections of this type."

    choices = ["CHAINED", "RUN", "TAGGED"]

    def makeCollectionType(self, context, param, value):
        if value is None:
            return value
        value = value.upper()
        if value == "CHAINED":
            return CollectionType.CHAINED
        if value == "RUN":
            return CollectionType.RUN
        if value == "TAGGED":
            return CollectionType.TAGGED

    def __init__(self, required=False, help=defaultHelp):
        self.required = required
        self.help = help

    def __call__(self, f):
        return click.option("--collection-type", cls=MWOption,
                            required=self.required,
                            type=click.Choice(self.choices, case_sensitive=False),
                            callback=self.makeCollectionType,
                            help=self.help)(f)
