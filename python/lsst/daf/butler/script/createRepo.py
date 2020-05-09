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

from .. import Butler, Config


def createRepo(repo, config_file=None, standalone=False, override=False, outfile=None):
    """Create an empty Gen3 Butler repository.

    Parameters
    ----------
    repo : `str`
        URI to the location to create the repo.
    config_file : `str` or `None`
        Path to a config yaml file, by default None
    standalone : `bool`
        Include all the defaults in the config file in the repo if True.
        Insulates the the repo from changes to package defaults. By default
        False.
    override : `bool`
        Allow values in the config file to override any repo settings, by
        default False.
    outfile : `str` or None
        Name of output file to receive repository configuration. Default is to
        write butler.yaml into the specified repo, by default False.
    """
    config = Config(config_file) if config_file is not None else None
    Butler.makeRepo(repo, config=config, standalone=standalone, forceConfigRoot=not override,
                    outfile=outfile)
