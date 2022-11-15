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
from ..core import ValidationError


def configValidate(repo: str, quiet: bool, dataset_type: list[str], ignore: list[str]) -> bool:
    """Validate the configuration files for a Gen3 Butler repository.

    Parameters
    ----------
    repo : `str`
        URI to the location to create the repo.
    quiet : `bool`
        Do not report individual failures if True.
    dataset_type : `list`[`str`]
        Specific DatasetTypes to validate.
    ignore : `list`[`str`]
        "DatasetTypes to ignore for validation."

    Returns
    -------
    is_good : `bool`
        `True` if validation was okay. `False` if there was a validation
        error.
    """
    logFailures = not quiet
    butler = Butler(config=repo)
    is_good = True
    try:
        butler.validateConfiguration(logFailures=logFailures, datasetTypeNames=dataset_type, ignore=ignore)
    except ValidationError:
        is_good = False
    else:
        print("No problems encountered with configuration.")
    return is_good
