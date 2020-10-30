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

import contextlib
import posixpath
import logging
import os

from pathlib import Path, PurePath, PurePosixPath


from typing import (
    Any,
    Callable,
    Iterator,
    Union,
)

# Determine if the path separator for the OS looks like POSIX
IS_POSIX = os.sep == posixpath.sep

# Root path for this operating system
OS_ROOT_PATH = Path().resolve().root

log = logging.getLogger(__name__)


def os2posix(ospath: str) -> str:
    """Convert a local path description to a POSIX path description.

    Parameters
    ----------
    ospath : `str`
        Path using the local path separator.

    Returns
    -------
    posix : `str`
        Path using POSIX path separator
    """
    if IS_POSIX:
        return ospath

    posix = PurePath(ospath).as_posix()

    # PurePath strips trailing "/" from paths such that you can no
    # longer tell if a path is meant to be referring to a directory
    # Try to fix this.
    if ospath.endswith(os.sep) and not posix.endswith(posixpath.sep):
        posix += posixpath.sep

    return posix


def posix2os(posix: Union[PurePath, str]) -> str:
    """Convert a POSIX path description to a local path description.

    Parameters
    ----------
    posix : `str`, `PurePath`
        Path using the POSIX path separator.

    Returns
    -------
    ospath : `str`
        Path using OS path separator
    """
    if IS_POSIX:
        return str(posix)

    posixPath = PurePosixPath(posix)
    paths = list(posixPath.parts)

    # Have to convert the root directory after splitting
    if paths[0] == posixPath.root:
        paths[0] = OS_ROOT_PATH

    # Trailing "/" is stripped so we need to add back an empty path
    # for consistency
    if str(posix).endswith(posixpath.sep):
        paths.append("")

    return os.path.join(*paths)


class NoTransaction:
    """A simple emulation of the `DatastoreTransaction` class.

    Does nothing.
    """

    def __init__(self) -> None:
        return

    @contextlib.contextmanager
    def undoWith(self, name: str, undoFunc: Callable, *args: Any, **kwargs: Any) -> Iterator[None]:
        """No-op context manager to replace `DatastoreTransaction`
        """
        yield None
