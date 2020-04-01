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
import logging

from lsst.daf.butler import ButlerConfig
from lsst.daf.butler.script.opt import verbose


@click.command(name='dump-config')
@click.argument('repo')
@click.option('--subset', '-s', type=str, help='Subset of a configuration to report. This can be any key in '
              "the hierarchy such as '.datastore.root' where the leading '.' specified the delimiter for "
              'the hierarchy.')
@click.option('--searchpath', '-p', type=str, multiple=True,
              help='Additional search paths to use for configuration overrides')
@click.option('--file', 'outfile', type=click.File('w'), default='-',
              help='Print the (possibly-expanded) configuration for a repository to a file, or to stdout '
              'by default.')
@verbose.verbose_option
@verbose.pass_verbosity
def dump_config(verbosity, repo, subset, searchpath, outfile):
    '''Dump either a subset or full Butler configuration to standard output.

    REPO is the filesystem path for an existing Butler repository or path to
    config file.
    '''
    if verbosity.verbose:
        logging.basicConfig(level=logging.DEBUG)

    config = ButlerConfig(repo, searchPaths=searchpath)

    if subset is not None:
        config = config[subset]

    try:
        config.dump(outfile)
    except AttributeError:
        print(config, file=outfile)
