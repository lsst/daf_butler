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

import logging
import os.path
import tempfile

from typing import (
    Any,
)

from .fileLikeDatastore import FileLikeDatastore

from lsst.daf.butler import (
    DatasetRef,
    Location,
    StoredFileInfo,
)

log = logging.getLogger(__name__)


class RemoteFileDatastore(FileLikeDatastore):
    """A datastore designed for files at remote locations.

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
    Datastore supports non-link transfer modes for file-based ingest:
    `"move"`, `"copy"`, and `None` (no transfer).
    """

    defaultConfigFile = "datastores/remoteFileDatastore.yaml"
    """Path to configuration defaults. Accessed within the ``config`` resource
    or relative to a search path. Can be None if no defaults specified.
    """

    def _write_in_memory_to_artifact(self, inMemoryDataset: Any, ref: DatasetRef) -> StoredFileInfo:
        location, formatter = self._prepare_for_put(inMemoryDataset, ref)

        if location.uri.exists():
            # Assume that by this point if registry thinks the file should
            # not exist then the file should not exist and therefore we can
            # overwrite it. This can happen if a put was interrupted by
            # an external interrupt. The only time this could be problematic is
            # if the file template is incomplete and multiple dataset refs
            # result in identical filenames.
            # Eventually we should remove the check completely (it takes
            # non-zero time for network).
            log.warning("Object %s exists in datastore for ref %s", location.uri, ref)

        if not location.uri.dirname().exists():
            log.debug("Folder %s does not exist yet.", location.uri.dirname())
            location.uri.dirname().mkdir()

        if self._transaction is None:
            raise RuntimeError("Attempting to write artifact without transaction enabled")

        # upload the file directly from bytes or by using a temporary file if
        # _toBytes is not implemented
        try:
            serializedDataset = formatter.toBytes(inMemoryDataset)
            log.debug("Writing bytes directly to %s", location.uri)
            location.uri.write(serializedDataset, overwrite=True)
            log.debug("Successfully wrote bytes directly to %s", location.uri)
        except NotImplementedError:
            with tempfile.NamedTemporaryFile(suffix=location.getExtension()) as tmpFile:
                tmpLocation = Location(*os.path.split(tmpFile.name))
                formatter._fileDescriptor.location = tmpLocation
                log.debug("Writing dataset to temporary directory at %s", tmpLocation.uri)
                formatter.write(inMemoryDataset)
                location.uri.transfer_from(tmpLocation.uri, transfer="copy", overwrite=True)
            log.debug("Successfully wrote dataset to %s via a temporary file.", location.uri)

        # Register a callback to try to delete the uploaded data if
        # the ingest fails below
        self._transaction.registerUndo("remoteWrite", location.uri.remove)

        # URI is needed to resolve what ingest case are we dealing with
        return self._extractIngestInfo(location.uri, ref, formatter=formatter)
