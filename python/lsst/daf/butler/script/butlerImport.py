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

from collections.abc import Iterable
from typing import TextIO

from .._butler import Butler


def butlerImport(
    repo: str,
    directory: str | None,
    export_file: str | TextIO | None,
    transfer: str | None,
    skip_dimensions: Iterable[str] | None,
    reuse_ids: bool,
) -> None:
    """Import data into a butler repository.

    Parameters
    ----------
    repo : `str`
        URI to the location of the repo or URI to a config file describing the
        repo and its location.
    directory : `str`, or None
        Directory containing dataset files.  If `None`, all file paths must be
        absolute.
    export_file : `TextIO`, or None
        Name for the file that contains database information associated with
        the exported datasets.  If this is not an absolute path, does not exist
        in the current working directory, and `directory` is not `None`, it is
        assumed to be in `directory`.  Defaults to "export.{format}".
    transfer : `str`, or None
        The external data transfer type.
    skip_dimensions : `list`, or `None`
        Dimensions that should be skipped.
    reuse_ids : `bool`
        If `True` forces re-use of imported dataset IDs for integer IDs.
    """
    butler = Butler(repo, writeable=True)

    if skip_dimensions is not None:
        skip_dimensions = set(skip_dimensions)

    butler.import_(
        directory=directory,
        filename=export_file,
        transfer=transfer,
        format="yaml",
        skip_dimensions=skip_dimensions,
        reuseIds=reuse_ids,
    )
