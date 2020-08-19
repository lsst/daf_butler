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
import os
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Optional,
    Union
)

from .fileLikeDatastore import FileLikeDatastore
from lsst.daf.butler.core.utils import safeMakeDir
from lsst.daf.butler import StoredFileInfo, DatasetRef

if TYPE_CHECKING:
    from .fileLikeDatastore import DatastoreFileGetInformation
    from lsst.daf.butler import DatastoreConfig
    from lsst.daf.butler.registry.interfaces import DatastoreRegistryBridgeManager

log = logging.getLogger(__name__)


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
    """

    defaultConfigFile: ClassVar[Optional[str]] = "datastores/posixDatastore.yaml"
    """Path to configuration defaults. Accessed within the ``config`` resource
    or relative to a search path. Can be None if no defaults specified.
    """

    def __init__(self, config: Union[DatastoreConfig, str],
                 bridgeManager: DatastoreRegistryBridgeManager, butlerRoot: str = None):
        super().__init__(config, bridgeManager, butlerRoot)

        # Check that root is a valid URI for this datastore
        if self.root.scheme and self.root.scheme != "file":
            raise ValueError(f"Root location must only be a file URI not {self.root}")

        if not self.root.exists():
            if "create" not in self.config or not self.config["create"]:
                raise ValueError(f"No valid root and not allowed to create one at: {self.root}")
            self.root.mkdir()

    def _read_artifact_into_memory(self, getInfo: DatastoreFileGetInformation,
                                   ref: DatasetRef, isComponent: bool = False) -> Any:
        location = getInfo.location

        # Too expensive to recalculate the checksum on fetch
        # but we can check size and existence
        if not os.path.exists(location.path):
            raise FileNotFoundError("Dataset with Id {} does not seem to exist at"
                                    " expected location of {}".format(ref.id, location.path))
        stat = os.stat(location.path)
        size = stat.st_size
        storedFileInfo = getInfo.info
        if size != storedFileInfo.file_size:
            raise RuntimeError("Integrity failure in Datastore. Size of file {} ({}) does not"
                               " match recorded size of {}".format(location.path, size,
                                                                   storedFileInfo.file_size))

        formatter = getInfo.formatter
        try:
            log.debug("Reading %s from location %s with formatter %s",
                      f"component {getInfo.component}" if isComponent else "",
                      location.uri, type(formatter).__name__)
            result = formatter.read(component=getInfo.component if isComponent else None)
        except Exception as e:
            raise ValueError(f"Failure from formatter '{formatter.name()}' for dataset {ref.id}"
                             f" ({ref.datasetType.name} from {location.path}): {e}") from e

        return self._post_process_get(result, getInfo.readStorageClass, getInfo.assemblerParams,
                                      isComponent=isComponent)

    def _write_in_memory_to_artifact(self, inMemoryDataset: Any, ref: DatasetRef) -> StoredFileInfo:
        # Inherit docstring

        location, formatter = self._prepare_for_put(inMemoryDataset, ref)

        storageDir = os.path.dirname(location.path)
        if not os.path.isdir(storageDir):
            # Never try to remove this after creating it since there might
            # be a butler ingest process running concurrently that will
            # already think this directory exists.
            safeMakeDir(storageDir)

        # Write the file
        predictedFullPath = os.path.join(self.root.ospath, formatter.predictPath())

        if os.path.exists(predictedFullPath):
            # Assume that by this point if registry thinks the file should
            # not exist then the file should not exist and therefore we can
            # overwrite it. This can happen if a put was interrupted by
            # an external interrupt. The only time this could be problematic is
            # if the file template is incomplete and multiple dataset refs
            # result in identical filenames.
            log.warning("Object %s exists in datastore for ref %s", location.uri, ref)

        def _removeFileExists(path: str) -> None:
            """Remove a file and do not complain if it is not there.

            This is important since a formatter might fail before the file
            is written and we should not confuse people by writing spurious
            error messages to the log.
            """
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

        if self._transaction is None:
            raise RuntimeError("Attempting to write dataset without transaction enabled")

        formatter_exception = None
        with self._transaction.undoWith("write", _removeFileExists, predictedFullPath):
            try:
                path = formatter.write(inMemoryDataset)
                log.debug("Wrote file to %s", path)
            except Exception as e:
                formatter_exception = e

        if formatter_exception:
            raise formatter_exception

        assert predictedFullPath == os.path.join(self.root.ospath, path)

        return self._extractIngestInfo(path, ref, formatter=formatter)

    def _standardizeIngestPath(self, path: str, *, transfer: Optional[str] = None) -> str:
        # Docstring inherited from FileLikeDatastore._standardizeIngestPath.
        fullPath = os.path.normpath(os.path.join(self.root.ospath, path))
        if not os.path.exists(fullPath):
            raise FileNotFoundError(f"File at '{fullPath}' does not exist; note that paths to ingest "
                                    f"are assumed to be relative to self.root unless they are absolute.")
        if transfer is None:
            # Can not reuse path var because of typing
            pathx = self._pathInStore(path)
            if pathx is None:
                raise RuntimeError(f"'{path}' is not inside repository root '{self.root}'.")
            path = pathx
        return path
