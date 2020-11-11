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

"""Webdav datastore."""

__all__ = ("WebdavDatastore", )

from deprecated.sphinx import deprecated
from .fileDatastore import FileDatastore


@deprecated(reason="S3Datastore no longer necessary. Please switch to"
            " lsst.daf.butler.datastores.fileDatastore.FileDatastore and rename configuration file."
            " Will soon be removed.")
class WebdavDatastore(FileDatastore):
    """Basic Webdav Storage backed Datastore.

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
    """

    defaultConfigFile = "datastores/webdavDatastore.yaml"
    """Path to configuration defaults. Accessed within the ``configs`` resource
    or relative to a search path. Can be None if no defaults specified.
    """
