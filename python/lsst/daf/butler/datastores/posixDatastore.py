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

"""POSIX datastore."""

__all__ = ("PosixDatastore", )

import logging
from deprecated.sphinx import deprecated

from typing import (
    ClassVar,
    Optional,
)

from .fileLikeDatastore import FileLikeDatastore

log = logging.getLogger(__name__)


@deprecated(reason="PosixDatastore no longer necessary. Please switch to"
            " lsst.daf.butler.datastores.fileLikeDatastore.FileLikeDatastore and rename configuration file."
            " Will soon be removed.")
class PosixDatastore(FileLikeDatastore):
    """Basic POSIX filesystem backed Datastore.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration. A string should refer to the name of the config file.
    bridgeManager : `DatastoreRegistryBridgeManager`
        Object that manages the interface between `Registry` and datastores.
    butlerRoot : `str`, optional
        New datastore root to use to override the configuration value.

    Raises
    ------
    ValueError
        If root location does not exist and ``create`` is `False` in the
        configuration.

    Notes
    -----
    PosixDatastore supports all transfer modes for file-based ingest:
    `"move"`, `"copy"`, `"symlink"`, `"hardlink"`, `"relsymlink"`
    and `None` (no transfer).

    For PosixDatastore, the `"auto"` transfer mode will operate in-place (like
    ``transfer=None``) if the file is already within the datastore root, and
    fall back to `"link"` otherwise.

    See `Datastore.ingest` for more information on transfer modes.
    """

    defaultConfigFile: ClassVar[Optional[str]] = "datastores/posixDatastore.yaml"
    """Path to configuration defaults. Accessed within the ``configs`` resource
    or relative to a search path. Can be None if no defaults specified.
    """
