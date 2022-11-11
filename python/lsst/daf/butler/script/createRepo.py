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
from __future__ import annotations

from .._butler import Butler
from ..core import Config


def createRepo(
    repo: str,
    seed_config: str | None = None,
    dimension_config: str | None = None,
    standalone: bool = False,
    override: bool = False,
    outfile: str | None = None,
) -> None:
    """Create an empty Gen3 Butler repository.

    Parameters
    ----------
    repo : `str`
        URI to the location to create the repo.
    seed_config : `str` or `None`
        Path to an existing YAML config file to apply (on top of defaults).
    dimension_config : `str` or `None`
        Path to an existing YAML config file with dimensions configuration.
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
    config = Config(seed_config) if seed_config is not None else None
    Butler.makeRepo(
        repo,
        config=config,
        dimensionConfig=dimension_config,
        standalone=standalone,
        forceConfigRoot=not override,
        outfile=outfile,
    )
