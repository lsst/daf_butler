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

from ..opt import repo_argument
from ..utils import cli_handle_exception
from ...script import configDump


@click.command()
@repo_argument(required=True)
@click.option("--subset", "-s", type=str,
              help="Subset of a configuration to report. This can be any key in the hierarchy such as "
              "'.datastore.root' where the leading '.' specified the delimiter for the hierarchy.")
@click.option("--searchpath", "-p", type=str, multiple=True,
              help="Additional search paths to use for configuration overrides")
@click.option("--file", "outfile", type=click.File("w"), default="-",
              help="Print the (possibly-expanded) configuration for a repository to a file, or to stdout "
              "by default.")
def config_dump(*args, **kwargs):
    """Dump either a subset or full Butler configuration to standard output."""
    cli_handle_exception(configDump, *args, **kwargs)
