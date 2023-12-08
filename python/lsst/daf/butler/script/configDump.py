# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from typing import IO

from .._butler_config import ButlerConfig


def configDump(repo: str, subset: str, searchpath: str, outfile: IO) -> None:
    """Dump either a subset or full Butler configuration to standard output.

    Parameters
    ----------
    repo : `str`
        URI to the location to create the repo.
    subset : `str`
        Subset of a configuration to report. This can be any key in the
        hierarchy such as '.datastore.root' where the leading '.' specified the
        delimiter for the hierarchy.
    searchpath : `str`
        Additional search paths to use for configuration overrides.
    outfile : file-like object
        File to which the configuration should be printed.

    Raises
    ------
    KeyError
        If a subset is specified but does not exist in the configuration.
    AttributeError
        If there is an issue dumping the configuration.
    """
    config = ButlerConfig(repo, searchPaths=searchpath)
    if subset is not None:
        try:
            config = config[subset]
        except KeyError:
            raise KeyError(f"{subset} not found in config at {repo} (has {config.names()})") from None
    if hasattr(config, "dump"):
        config.dump(outfile)
    else:
        print(f"{subset}: {config}", file=outfile)
