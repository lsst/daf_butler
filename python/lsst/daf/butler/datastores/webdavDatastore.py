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

import logging
import urllib3
import os
import pathlib
import tempfile

from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Type,
    Union,
)

from lsst.daf.butler import (
    ButlerURI,
    DatasetRef,
    Formatter,
    Location,
    StoredFileInfo,
)

from .fileLikeDatastore import FileLikeDatastore
from webdav3.exceptions import WebDavException

from lsst.daf.butler.core.webdavutils import (
    getWebdavClient,
    getHttpSession,
    webdavCheckFileExists,
    folderExists,
)

if TYPE_CHECKING:
    from .fileLikeDatastore import DatastoreFileGetInformation
    from lsst.daf.butler import DatastoreConfig
    from lsst.daf.butler.registry.interfaces import DatastoreRegistryBridgeManager

log = logging.getLogger(__name__)
urllib3.disable_warnings()


class WebdavDatastore(FileLikeDatastore):
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

    Notes
    -----
    """

    defaultConfigFile = "datastores/webdavDatastore.yaml"
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    def __init__(self, config: Union[DatastoreConfig, str],
                 bridgeManager: DatastoreRegistryBridgeManager, butlerRoot: str = None):
        super().__init__(config, bridgeManager, butlerRoot)
        root = ButlerURI(self.root)
        self.client = getWebdavClient()
        self.session = getHttpSession()
        if not folderExists(root.relativeToPathRoot):
            try:
                self.client.mkdir(root.relativeToPathRoot)
            except WebDavException:
                raise IOError(f"Folder {root.relativeToPathRoot} could not be created, \
                            check that you have sufficient permission.")

    def _artifact_exists(self, location: Location) -> bool:
        """Check that an artifact exists in this datastore at the specified
        location.

        Parameters
        ----------
        location : `Location`
            Expected location of the artifact associated with this datastore.

        Returns
        -------
        exists : `bool`
            True if the location can be found, false otherwise.
        """
        exists, _ = webdavCheckFileExists(location.relativeToPathRoot,
                                          client=self.client)
        return exists

    def _delete_artifact(self, location: Location) -> None:
        """Delete the artifact from the datastore.

        Parameters
        ----------
        location : `Location`
            Location of the artifact associated with this datastore.
        """
        self.client.clean(location.relativeToPathRoot)

    def _read_artifact_into_memory(self, getInfo: DatastoreFileGetInformation,
                                   ref: DatasetRef, isComponent: bool = False) -> Any:
        location = getInfo.location

        response = self.session.get(location.uri)
        if response.status_code != 200:
            errorcode = response.status_code
            if errorcode == 403:
                raise FileNotFoundError(f"Dataset with Id {ref.id} not accessible at "
                                        f"expected location {location.uri}. Forbidden "
                                        "operation error occured. Verify permissions are granted for "
                                        "your user and that file exists. ")
            if errorcode == 404:
                errmsg = f"Dataset with Id {ref.id} does not exists at expected location {location.uri}."
                raise FileNotFoundError(errmsg)
            raise FileNotFoundError(f"There was an error getting file at {location.uri}, \
                                    status code : {errorcode}")

        storedFileInfo = getInfo.info
        if len(response.content) != storedFileInfo.file_size:
            raise RuntimeError("Integrity failure in Datastore. Size of file {} ({}) does not"
                               " match recorded size of {}".format(location.path, len(response.content),
                                                                   storedFileInfo.file_size))

        serializedDataset = response.content

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

        if webdavCheckFileExists(location, client=self.client,)[0]:
            raise FileExistsError(f"Cannot write file for ref {ref} as "
                                  f"output file {location.uri} exists.")

        try:
            serializedDataset = formatter.toBytes(inMemoryDataset)
            self.session.put(location.uri, data=serializedDataset)
            log.debug("Wrote file directly to %s", location.uri)
        except NotImplementedError:
            with tempfile.NamedTemporaryFile(suffix=location.getExtension()) as tmpFile:
                formatter._fileDescriptor.location = Location(*os.path.split(tmpFile.name))
                formatter.write(inMemoryDataset)
                with open(tmpFile.name, 'rb') as f:
                    self.session.put(location.uri, data=f)
                log.debug("Wrote file to %s via a temporary directory.", location.uri)

        if self._transaction is None:
            raise RuntimeError("Attempting to write artifact without transaction enabled")

        # Register a callback to try to delete the uploaded data if
        # the ingest fails below
        self._transaction.registerUndo("write", self.client.clean, location.relativeToPathRoot)

        # URI is needed to resolve what ingest case are we dealing with
        return self._extractIngestInfo(location.uri, ref, formatter=formatter)

    def _overrideTransferMode(self, *datasets: Any, transfer: Optional[str] = None) -> Optional[str]:
        # Docstring inherited from base class
        if transfer != "auto":
            return transfer
        return "copy"

    def _standardizeIngestPath(self, path: str, *, transfer: Optional[str] = None) -> str:
        # Docstring inherited from FileLikeDatastore._standardizeIngestPath.
        if transfer not in (None, "move", "copy"):
            raise NotImplementedError(f"Transfer mode {transfer} not supported.")
        srcUri = ButlerURI(path)
        if srcUri.scheme == 'file' or not srcUri.scheme:
            if not os.path.exists(srcUri.ospath):
                raise FileNotFoundError(f"File at '{srcUri}' does not exist.")
        elif srcUri.scheme == 'https':
            if not webdavCheckFileExists(srcUri, client=self.client)[0]:
                raise FileNotFoundError(f"File at '{srcUri}' does not exist.")
        else:
            raise NotImplementedError(f"Scheme type {srcUri.scheme} not supported.")

        if transfer is None:
            rootUri = ButlerURI(self.root)
            if srcUri.scheme == "file":
                raise RuntimeError(f"'{srcUri}' is not inside repository root '{rootUri}'. "
                                   "Ingesting local data to WebdavDatastore without upload "
                                   "to Webdav is not allowed.")
            elif srcUri.scheme == "https":
                if not srcUri.path.startswith(rootUri.path):
                    raise RuntimeError(f"'{srcUri}' is not inside repository root '{rootUri}'.")
        return path

    def _extractIngestInfo(self, path: str, ref: DatasetRef, *,
                           formatter: Union[Formatter, Type[Formatter]],
                           transfer: Optional[str] = None) -> StoredFileInfo:
        # Docstring inherited from FileLikeDatastore._extractIngestInfo.
        srcUri = ButlerURI(path)

        if transfer is None:
            rootUri = ButlerURI(self.root)
            p = pathlib.PurePosixPath(srcUri.relativeToPathRoot)
            pathInStore = str(p.relative_to(rootUri.relativeToPathRoot))
            tgtLocation = self.locationFactory.fromPath(pathInStore)
        else:
            assert transfer == "move" or transfer == "copy", "Should be guaranteed by _standardizeIngestPath"

            # Work out the name we want this ingested file to have
            # inside the datastore
            tgtLocation = self._calculate_ingested_datastore_name(srcUri, ref, formatter)

            if srcUri.scheme == "file":
                # source is on local disk.
                with open(srcUri.ospath, 'rb') as f:
                    self.session.put(tgtLocation.uri, data=f)
                if transfer == "move":
                    os.remove(srcUri.ospath)
            elif srcUri.scheme == "https":
                relpath = srcUri.relativeToPathRoot
                self.client.copy(remote_path_from=relpath,
                                 remote_path_to=tgtLocation.relativeToPathRoot)
                if transfer == "move":
                    self.client.clean(relpath)

        # the file should exist on the bucket by now
        exists, size = webdavCheckFileExists(tgtLocation, client=self.client)

        return StoredFileInfo(formatter=formatter, path=tgtLocation.pathInStore,
                              storageClass=ref.datasetType.storageClass,
                              component=ref.datasetType.component(),
                              file_size=size, checksum=None)
