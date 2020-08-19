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
    TYPE_CHECKING,
    Any,
    Union,
)

from .fileLikeDatastore import FileLikeDatastore

from lsst.daf.butler import (
    DatasetRef,
    Location,
    StoredFileInfo,
)

if TYPE_CHECKING:
    from .fileLikeDatastore import DatastoreFileGetInformation
    from lsst.daf.butler import DatastoreConfig
    from lsst.daf.butler.registry.interfaces import DatastoreRegistryBridgeManager

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

    def __init__(self, config: Union[DatastoreConfig, str],
                 bridgeManager: DatastoreRegistryBridgeManager, butlerRoot: str = None):
        super().__init__(config, bridgeManager, butlerRoot)
        if not self.root.exists():
            try:
                self.root.mkdir()
            except ValueError as e:
                raise ValueError(f"Can not create datastore root '{self.root}', check permissions.") from e

    def _read_artifact_into_memory(self, getInfo: DatastoreFileGetInformation,
                                   ref: DatasetRef, isComponent: bool = False) -> Any:
        location = getInfo.location

        log.debug("Downloading data from %s", location.uri)
        serializedDataset = location.uri.read()

        storedFileInfo = getInfo.info
        if len(serializedDataset) != storedFileInfo.file_size:
            raise RuntimeError("Integrity failure in Datastore. "
                               f"Size of file {location.path} ({len(serializedDataset)}) "
                               f"does not match recorded size of {storedFileInfo.file_size}")

        # format the downloaded bytes into appropriate object directly, or via
        # tempfile (when formatter does not support to/from/Bytes). This is
        # equivalent of PosixDatastore formatter.read try-except block.
        formatter = getInfo.formatter
        try:
            result = formatter.fromBytes(serializedDataset,
                                         component=getInfo.component if isComponent else None)
        except NotImplementedError:
            # formatter might not always have an extension so mypy complains
            # We can either ignore the complaint or use a temporary location
            tmpLoc = Location(".", "temp")
            tmpLoc = formatter.makeUpdatedLocation(tmpLoc)
            with tempfile.NamedTemporaryFile(suffix=tmpLoc.getExtension()) as tmpFile:
                tmpFile.write(serializedDataset)
                # Flush the write. Do not close the file because that
                # will delete it.
                tmpFile.flush()
                formatter._fileDescriptor.location = Location(*os.path.split(tmpFile.name))
                result = formatter.read(component=getInfo.component if isComponent else None)
        except Exception as e:
            raise ValueError(f"Failure from formatter '{formatter.name()}' for dataset {ref.id}"
                             f" ({ref.datasetType.name} from {location.uri}): {e}") from e

        return self._post_process_get(result, getInfo.readStorageClass, getInfo.assemblerParams,
                                      isComponent=isComponent)

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
                log.debug("Writing dataset to temporary directory at ", tmpLocation.uri)
                formatter.write(inMemoryDataset)
                location.uri.transfer_from(tmpLocation.uri, transfer="copy", overwrite=True)
            log.debug("Successfully wrote dataset to %s via a temporary file.", location.uri)

        # Register a callback to try to delete the uploaded data if
        # the ingest fails below
        self._transaction.registerUndo("remoteWrite", location.uri.remove)

        # URI is needed to resolve what ingest case are we dealing with
        return self._extractIngestInfo(location.uri, ref, formatter=formatter)
