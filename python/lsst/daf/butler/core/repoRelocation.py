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

"""Routines to support relocation of a file-based Butler repository."""

from __future__ import annotations

__all__ = ("BUTLER_ROOT_TAG", "replaceRoot")

import os.path
from typing import Optional

from lsst.resources import ResourcePath, ResourcePathExpression

BUTLER_ROOT_TAG = "<butlerRoot>"
"""The special string to be used in configuration files to indicate that
the butler root location should be used."""


def replaceRoot(configRoot: str, butlerRoot: Optional[ResourcePathExpression]) -> str:
    """Update a configuration root with the butler root location.

    No changes are made if the special root string is not found in the
    configuration entry.  The name of the tag is defined in
    the module variable `~lsst.daf.butler.core.repoRelocation.BUTLER_ROOT_TAG`.

    Parameters
    ----------
    configRoot : `str`
        Directory root location as specified in a configuration file.
    butlerRoot : `lsst.resources.ResourcePathExpression` or `None`
        Butler root directory.  Absolute path is inserted into the
        ``configRoot`` where the
        `~lsst.daf.butler.core.repoRelocation.BUTLER_ROOT_TAG` string is
        found.  Passing `None` here is allowed only as a convenient way to
        raise an exception (`ValueError`).

    Returns
    -------
    newRoot : `str`
        New configuration string, with the root tag replaced with the butler
        root if that tag was present in the supplied configuration root.

    Raises
    ------
    ValueError
        Raised if ``butlerRoot`` is not set but a value is required.
    """
    # Do nothing if there is nothing to be done
    if BUTLER_ROOT_TAG not in configRoot:
        return configRoot

    # None or empty string indicate a problem
    if not butlerRoot:
        raise ValueError(
            f"Required to replace {BUTLER_ROOT_TAG} in '{configRoot}' "
            "but a replacement has not been defined"
        )

    # Use absolute file path if this refers to a local file, else use
    # unchanged since all other URI schemes are absolute
    uri = ResourcePath(butlerRoot)
    if uri.isLocal:
        # This will be a local file with URI quoting removed
        butlerRoot = os.path.abspath(uri.ospath)

    assert butlerRoot is not None
    return configRoot.replace(BUTLER_ROOT_TAG, str(butlerRoot))
