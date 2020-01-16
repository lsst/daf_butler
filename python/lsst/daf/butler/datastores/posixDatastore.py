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

"""POSIX datastore."""

from __future__ import annotations

__all__ = ("PosixDatastore", )

import hashlib
import logging
import os
import shutil
from typing import TYPE_CHECKING, Iterable, Optional, Type

from .fileLikeDatastore import FileLikeDatastore
from lsst.daf.butler.core.safeFileIo import safeMakeDir
from lsst.daf.butler.core.utils import transactional
from lsst.daf.butler import FileDataset, StoredFileInfo, Formatter

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef

log = logging.getLogger(__name__)


class PosixDatastore(FileLikeDatastore):
    """Basic POSIX filesystem backed Datastore.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration. A string should refer to the name of the config file.
    registry : `Registry`
        Registry to use for storing internal information about the datasets.
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
    `"move"`, `"copy"`, `"symlink"`, `"hardlink"`, and `None` (no transfer).
    """

    defaultConfigFile = "datastores/posixDatastore.yaml"
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    def __init__(self, config, registry, butlerRoot=None):
        super().__init__(config, registry, butlerRoot)

        if not os.path.isdir(self.root):
            if "create" not in self.config or not self.config["create"]:
                raise ValueError(f"No valid root at: {self.root}")
            safeMakeDir(self.root)

    def exists(self, ref):
        """Check if the dataset exists in the datastore.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required dataset.

        Returns
        -------
        exists : `bool`
            `True` if the entity exists in the `Datastore`.
        """
        location, _ = self._get_dataset_location_info(ref)
        if location is None:
            return False
        return os.path.exists(location.path)

    def get(self, ref, parameters=None):
        """Load an InMemoryDataset from the store.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
        parameters : `dict`
            `StorageClass`-specific parameters that specify, for example,
            a slice of the Dataset to be loaded.

        Returns
        -------
        inMemoryDataset : `object`
            Requested Dataset or slice thereof as an InMemoryDataset.

        Raises
        ------
        FileNotFoundError
            Requested dataset can not be retrieved.
        TypeError
            Return value from formatter has unexpected type.
        ValueError
            Formatter failed to process the dataset.
        """
        getInfo = self._prepare_for_get(ref, parameters)
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
            result = formatter.read(component=getInfo.component)
        except Exception as e:
            raise ValueError(f"Failure from formatter '{formatter.name()}' for Dataset {ref.id}") from e

        return self._post_process_get(result, getInfo.readStorageClass, getInfo.assemblerParams)

    @transactional
    def put(self, inMemoryDataset, ref):
        """Write a InMemoryDataset with a given `DatasetRef` to the store.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.
        ref : `DatasetRef`
            Reference to the associated Dataset.

        Raises
        ------
        TypeError
            Supplied object and storage class are inconsistent.
        DatasetTypeNotSupportedError
            The associated `DatasetType` is not handled by this datastore.

        Notes
        -----
        If the datastore is configured to reject certain dataset types it
        is possible that the put will fail and raise a
        `DatasetTypeNotSupportedError`.  The main use case for this is to
        allow `ChainedDatastore` to put to multiple datastores without
        requiring that every datastore accepts the dataset.
        """
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

        def _removeFileExists(path):
            """Remove a file and do not complain if it is not there.

            This is important since a formatter might fail before the file
            is written and we should not confuse people by writing spurious
            error messages to the log.
            """
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

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

        info = self._extractIngestInfo(path, ref, formatter=formatter)
        self._register_datasets([(ref, info)])

    def _standardizeIngestPath(self, path: str, *, transfer: Optional[str] = None) -> str:
        # Docstring inherited from FileLikeDatastore._standardizeIngestPath.
        fullPath = os.path.normpath(os.path.join(self.root, path))
        if not os.path.exists(fullPath):
            raise FileNotFoundError(f"File at '{fullPath}' does not exist; note that paths to ingest "
                                    f"are assumed to be relative to self.root unless they are absolute.")
        if transfer is None:
            if os.path.isabs(path):
                absRoot = os.path.abspath(self.root)
                if os.path.commonpath([absRoot, path]) != absRoot:
                    raise RuntimeError(f"'{path}' is not inside repository root '{self.root}'.")
                return os.path.relpath(path, absRoot)
            elif path.startswith(os.path.pardir):
                raise RuntimeError(f"'{path}' is outside repository root '{self.root}.'")
        return path

    def _extractIngestInfo(self, path: str, ref: DatasetRef, *, formatter: Type[Formatter],
                           transfer: Optional[str] = None) -> StoredFileInfo:
        # Docstring inherited from FileLikeDatastore._extractIngestInfo.
        fullPath = os.path.normpath(os.path.join(self.root, path))
        if transfer is not None:
            template = self.templates.getTemplate(ref)
            location = self.locationFactory.fromPath(template.format(ref))
            newPath = formatter.predictPathFromLocation(location)
            newFullPath = os.path.join(self.root, newPath)
            if os.path.exists(newFullPath):
                raise FileExistsError(f"File '{newFullPath}' already exists.")
            storageDir = os.path.dirname(newFullPath)
            if not os.path.isdir(storageDir):
                with self._transaction.undoWith("mkdir", os.rmdir, storageDir):
                    safeMakeDir(storageDir)
            if transfer == "move":
                with self._transaction.undoWith("move", shutil.move, newFullPath, fullPath):
                    shutil.move(fullPath, newFullPath)
            elif transfer == "copy":
                with self._transaction.undoWith("copy", os.remove, newFullPath):
                    shutil.copy(fullPath, newFullPath)
            elif transfer == "hardlink":
                with self._transaction.undoWith("hardlink", os.unlink, newFullPath):
                    os.link(fullPath, newFullPath)
            elif transfer == "symlink":
                with self._transaction.undoWith("symlink", os.unlink, newFullPath):
                    os.symlink(fullPath, newFullPath)
            else:
                raise NotImplementedError("Transfer type '{}' not supported.".format(transfer))
            path = newPath
            fullPath = newFullPath
        checksum = self.computeChecksum(fullPath)
        stat = os.stat(fullPath)
        size = stat.st_size
        return StoredFileInfo(formatter=formatter, path=path, storageClass=ref.datasetType.storageClass,
                              file_size=size, checksum=checksum)

    def remove(self, ref):
        """Indicate to the Datastore that a Dataset can be removed.

        .. warning::

            This method does not support transactions; removals are
            immediate, cannot be undone, and are not guaranteed to
            be atomic if deleting either the file or the internal
            database records fails.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.

        Raises
        ------
        FileNotFoundError
            Attempt to remove a dataset that does not exist.
        """
        # Get file metadata and internal metadata
        location, _ = self._get_dataset_location_info(ref)
        if location is None:
            raise FileNotFoundError(f"Requested dataset ({ref}) does not exist")

        if not os.path.exists(location.path):
            raise FileNotFoundError(f"No such file: {location.uri}")

        if self._can_remove_dataset_artifact(ref):
            # Only reference to this path so we can remove it
            os.remove(location.path)

        # Remove rows from registries
        self._remove_from_registry(ref)

    @staticmethod
    def computeChecksum(filename, algorithm="blake2b", block_size=8192):
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
        for ref in refs:
            location, storedFileInfo = self._get_dataset_location_info(ref)
            if location is None:
                raise FileNotFoundError(f"Could not retrieve Dataset {ref}.")
            if transfer is None:
                # TODO: do we also need to return the readStorageClass somehow?
                yield FileDataset(refs=[ref], path=location.pathInStore, formatter=storedFileInfo.formatter)
            else:
                # TODO: add support for other transfer modes.  If we support
                # moving, this method should become transactional.
                raise NotImplementedError(f"Transfer mode '{transfer}' not yet supported.")
