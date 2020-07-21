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

import hashlib
import logging
import os
import shutil
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Iterable,
    Optional,
    Type,
    Union
)

from .fileLikeDatastore import FileLikeDatastore
from lsst.daf.butler.core.utils import safeMakeDir
from lsst.daf.butler import ButlerURI, FileDataset, StoredFileInfo, Formatter, DatasetRef

if TYPE_CHECKING:
    from .fileLikeDatastore import DatastoreFileGetInformation
    from lsst.daf.butler import DatastoreConfig, Location
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
        root = ButlerURI(self.root)
        if root.scheme and root.scheme != "file":
            raise ValueError(f"Root location must only be a file URI not {self.root}")

        self.root = root.path
        if not os.path.isdir(self.root):
            if "create" not in self.config or not self.config["create"]:
                raise ValueError(f"No valid root at: {self.root}")
            safeMakeDir(self.root)

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
        return os.path.exists(location.path)

    def _delete_artifact(self, location: Location) -> None:
        """Delete the artifact from the datastore.

        Parameters
        ----------
        location : `Location`
            Location of the artifact associated with this datastore.
        """
        os.remove(location.path)

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
        predictedFullPath = os.path.join(self.root, formatter.predictPath())

        if os.path.exists(predictedFullPath):
            raise FileExistsError(f"Cannot write file for ref {ref} as "
                                  f"output file {predictedFullPath} already exists")

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

        assert predictedFullPath == os.path.join(self.root, path)

        return self._extractIngestInfo(path, ref, formatter=formatter)

    def _overrideTransferMode(self, *datasets: FileDataset, transfer: Optional[str] = None) -> Optional[str]:
        # Docstring inherited from base class
        if transfer != "auto":
            return transfer

        # See if the paths are within the datastore or not
        inside = [self._pathInStore(d.path) is not None for d in datasets]

        if all(inside):
            transfer = None
        elif not any(inside):
            transfer = "link"
        else:
            raise ValueError("Some datasets are inside the datastore and some are outside."
                             " Please use an explicit transfer mode and not 'auto'.")

        return transfer

    def _pathInStore(self, path: str) -> Optional[str]:
        """Return path relative to datastore root

        Parameters
        ----------
        path : `str`
            Path to dataset. Can be absolute path. Returns path in datastore
            or raises an exception if the path it outside.

        Returns
        -------
        inStore : `str`
            Path relative to datastore root. Returns `None` if the file is
            outside the root.
        """
        if os.path.isabs(path):
            absRoot = os.path.abspath(self.root)
            if os.path.commonpath([absRoot, path]) != absRoot:
                return None
            return os.path.relpath(path, absRoot)
        elif path.startswith(os.path.pardir):
            return None
        return path

    def _standardizeIngestPath(self, path: str, *, transfer: Optional[str] = None) -> str:
        # Docstring inherited from FileLikeDatastore._standardizeIngestPath.
        fullPath = os.path.normpath(os.path.join(self.root, path))
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

    def _extractIngestInfo(self, path: str, ref: DatasetRef, *,
                           formatter: Union[Formatter, Type[Formatter]],
                           transfer: Optional[str] = None) -> StoredFileInfo:
        # Docstring inherited from FileLikeDatastore._extractIngestInfo.
        if self._transaction is None:
            raise RuntimeError("Ingest called without transaction enabled")

        fullPath = os.path.normpath(os.path.join(self.root, path))
        if transfer is not None:
            # Work out the name we want this ingested file to have
            # inside the datastore
            location = self._calculate_ingested_datastore_name(ButlerURI(fullPath), ref, formatter)

            newPath = location.pathInStore
            newFullPath = location.path
            if os.path.exists(newFullPath):
                raise FileExistsError(f"File '{newFullPath}' already exists.")
            storageDir = os.path.dirname(newFullPath)
            if not os.path.isdir(storageDir):
                # Do not attempt to reverse directory creation
                # because of race conditions with other processes running
                # ingest in parallel.
                safeMakeDir(storageDir)
            if transfer == "move":
                with self._transaction.undoWith("move", shutil.move, newFullPath, fullPath):
                    shutil.move(fullPath, newFullPath)
            elif transfer == "copy":
                with self._transaction.undoWith("copy", os.remove, newFullPath):
                    shutil.copy(fullPath, newFullPath)
            elif transfer == "link":
                with self._transaction.undoWith("link", os.unlink, newFullPath):
                    realPath = os.path.realpath(fullPath)
                    # Try hard link and if that fails use a symlink
                    try:
                        os.link(realPath, newFullPath)
                    except OSError:
                        # Read through existing symlinks
                        os.symlink(realPath, newFullPath)
            elif transfer == "hardlink":
                with self._transaction.undoWith("hardlink", os.unlink, newFullPath):
                    os.link(os.path.realpath(fullPath), newFullPath)
            elif transfer == "symlink":
                with self._transaction.undoWith("symlink", os.unlink, newFullPath):
                    # Read through existing symlinks
                    os.symlink(os.path.realpath(fullPath), newFullPath)
            elif transfer == "relsymlink":
                # This is a standard symlink but using a relative path
                fullPath = os.path.realpath(fullPath)

                # Need the directory name to give to relative root
                # A full file path confuses it into an extra ../
                newFullPathRoot, _ = os.path.split(newFullPath)
                relPath = os.path.relpath(fullPath, newFullPathRoot)
                with self._transaction.undoWith("relsymlink", os.unlink, newFullPath):
                    os.symlink(relPath, newFullPath)
            else:
                raise NotImplementedError("Transfer type '{}' not supported.".format(transfer))
            path = newPath
            fullPath = newFullPath
        checksum = self.computeChecksum(fullPath) if self.useChecksum else None
        stat = os.stat(fullPath)
        size = stat.st_size
        return StoredFileInfo(formatter=formatter, path=path, storageClass=ref.datasetType.storageClass,
                              component=ref.datasetType.component(),
                              file_size=size, checksum=checksum)

    @staticmethod
    def computeChecksum(filename: str, algorithm: str = "blake2b", block_size: int = 8192) -> str:
        """Compute the checksum of the supplied file.

        Parameters
        ----------
        filename : `str`
            Name of file to calculate checksum from.
        algorithm : `str`, optional
            Name of algorithm to use. Must be one of the algorithms supported
            by :py:class`hashlib`.
        block_size : `int`
            Number of bytes to read from file at one time.

        Returns
        -------
        hexdigest : `str`
            Hex digest of the file.
        """
        if algorithm not in hashlib.algorithms_guaranteed:
            raise NameError("The specified algorithm '{}' is not supported by hashlib".format(algorithm))

        hasher = hashlib.new(algorithm)

        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(block_size), b""):
                hasher.update(chunk)

        return hasher.hexdigest()

    def export(self, refs: Iterable[DatasetRef], *,
               directory: Optional[str] = None, transfer: Optional[str] = None) -> Iterable[FileDataset]:
        # Docstring inherited from Datastore.export.
        if transfer is not None and directory is None:
            raise RuntimeError(f"Cannot export using transfer mode {transfer} with no "
                               "export directory given")
        if transfer is not None and directory is not None:
            # mypy needs the second test
            if not os.path.exists(directory):
                raise FileNotFoundError(f"Export directory {directory} does not exist")

        if transfer == "auto":
            transfer = "link"

        for ref in refs:
            fileLocations = self._get_dataset_locations_info(ref)
            if not fileLocations:
                raise FileNotFoundError(f"Could not retrieve dataset {ref}.")
            # For now we can not export disassembled datasets
            if len(fileLocations) > 1:
                raise NotImplementedError(f"Can not export disassembled datasets such as {ref}")
            location, storedFileInfo = fileLocations[0]
            exportPath = None
            if transfer is None:
                # TODO: do we also need to return the readStorageClass somehow?
                # We will use the path in store directly
                pass
            elif transfer in ("copy", "link", "symlink", "hardlink"):
                # We are going to transfer the file using the same
                # path as datastore uses but in the export directory
                assert directory is not None  # mypy needs this
                exportPath = os.path.join(directory, location.pathInStore)
                exportDir = os.path.dirname(exportPath)
                # Should not be a softlink but play it safe
                storePath = os.path.realpath(location.path)
                if not os.path.exists(exportDir):
                    safeMakeDir(exportDir)
                if transfer == "copy":
                    shutil.copy(location.path, exportPath)
                elif transfer == "symlink":
                    os.symlink(storePath, exportPath)
                elif transfer == "hardlink":
                    os.link(storePath, exportPath)
                elif transfer == "link":
                    try:
                        os.link(storePath, exportPath)
                    except OSError:
                        os.symlink(storePath, exportPath)
                else:
                    raise RuntimeError(f"Unexpected transfer mode encountered: {transfer}")

            else:
                # TODO: add support for other transfer modes.  If we support
                # moving, this method should become transactional.
                raise NotImplementedError(f"Transfer mode '{transfer}' not yet supported.")

            yield FileDataset(refs=[ref], path=location.pathInStore, formatter=storedFileInfo.formatter)
