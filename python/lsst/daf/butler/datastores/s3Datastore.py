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

"""S3 datastore."""

__all__ = ("S3Datastore", )

import logging
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
from lsst.daf.butler.core.s3utils import getS3Client, s3CheckFileExists, bucketExists

if TYPE_CHECKING:
    from .fileLikeDatastore import DatastoreFileGetInformation
    from lsst.daf.butler import DatastoreConfig
    from lsst.daf.butler.registry.interfaces import DatastoreRegistryBridgeManager

log = logging.getLogger(__name__)


class S3Datastore(FileLikeDatastore):
    """Basic S3 Object Storage backed Datastore.

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
    S3Datastore supports non-link transfer modes for file-based ingest:
    `"move"`, `"copy"`, and `None` (no transfer).
    """

    defaultConfigFile = "datastores/s3Datastore.yaml"
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    def __init__(self, config: Union[DatastoreConfig, str],
                 bridgeManager: DatastoreRegistryBridgeManager, butlerRoot: str = None):
        super().__init__(config, bridgeManager, butlerRoot)

        self.client = getS3Client()
        if not bucketExists(self.locationFactory.netloc):
            # PosixDatastore creates the root directory if one does not exist.
            # Calling s3 client.create_bucket is possible but also requires
            # ACL LocationConstraints, Permissions and other configuration
            # parameters, so for now we do not create a bucket if one is
            # missing. Further discussion can make this happen though.
            raise IOError(f"Bucket {self.locationFactory.netloc} does not exist!")

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
        log.debug("Checking if file exists: %s", location.uri)
        exists, _ = s3CheckFileExists(location, client=self.client)
        return exists

    def _delete_artifact(self, location: Location) -> None:
        """Delete the artifact from the datastore.

        Parameters
        ----------
        location : `Location`
            Location of the artifact associated with this datastore.
        """
        log.debug("Deleting file: %s", location.uri)
        self.client.delete_object(Bucket=location.netloc, Key=location.relativeToPathRoot)
        log.debug("Successfully deleted file: %s", location.uri)

    def _read_artifact_into_memory(self, getInfo: DatastoreFileGetInformation,
                                   ref: DatasetRef, isComponent: bool = False) -> Any:
        location = getInfo.location

        # since we have to make a GET request to S3 anyhow (for download) we
        # might as well use the HEADER metadata for size comparison instead.
        # s3CheckFileExists would just duplicate GET/LIST charges in this case.
        try:
            log.debug("Reading file: %s", location.uri)
            response = self.client.get_object(Bucket=location.netloc,
                                              Key=location.relativeToPathRoot)
            log.debug("Successfully read file: %s", location.uri)
        except self.client.exceptions.ClientError as err:
            errorcode = err.response["ResponseMetadata"]["HTTPStatusCode"]
            # head_object returns 404 when object does not exist only when user
            # has s3:ListBucket permission. If list permission does not exist a
            # 403 is returned. In practical terms this usually means that the
            # file does not exist, but it could also mean user lacks GetObject
            # permission. It's hard to tell which case is it.
            # docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
            # Unit tests right now demand FileExistsError is raised, but this
            # should be updated to PermissionError like in s3CheckFileExists.
            if errorcode == 403:
                raise FileNotFoundError(f"Dataset with Id {ref.id} not accessible at "
                                        f"expected location {location}. Forbidden HEAD "
                                        "operation error occured. Verify s3:ListBucket "
                                        "and s3:GetObject permissions are granted for "
                                        "your IAM user and that file exists. ") from err
            if errorcode == 404:
                errmsg = f"Dataset with Id {ref.id} does not exists at expected location {location}."
                raise FileNotFoundError(errmsg) from err
            # other errors are reraised also, but less descriptively
            raise err

        storedFileInfo = getInfo.info
        if response["ContentLength"] != storedFileInfo.file_size:
            raise RuntimeError("Integrity failure in Datastore. Size of file {} ({}) does not"
                               " match recorded size of {}".format(location.path, response["ContentLength"],
                                                                   storedFileInfo.file_size))

        # download the data as bytes
        serializedDataset = response["Body"].read()

        # format the downloaded bytes into appropriate object directly, or via
        # tempfile (when formatter does not support to/from/Bytes). This is S3
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

        # in PosixDatastore a directory can be created by `safeMakeDir`. In S3
        # `Keys` instead only look like directories, but are not. We check if
        # an *exact* full key already exists before writing instead. The insert
        # key operation is equivalent to creating the dir and the file.
        if s3CheckFileExists(location, client=self.client,)[0]:
            raise FileExistsError(f"Cannot write file for ref {ref} as "
                                  f"output file {location.uri} exists.")

        # upload the file directly from bytes or by using a temporary file if
        # _toBytes is not implemented
        try:
            serializedDataset = formatter.toBytes(inMemoryDataset)
            log.debug("Writing file directly to %s", location.uri)
            self.client.put_object(Bucket=location.netloc, Key=location.relativeToPathRoot,
                                   Body=serializedDataset)
            log.debug("Successfully wrote file directly to %s", location.uri)
        except NotImplementedError:
            with tempfile.NamedTemporaryFile(suffix=location.getExtension()) as tmpFile:
                formatter._fileDescriptor.location = Location(*os.path.split(tmpFile.name))
                formatter.write(inMemoryDataset)
                with open(tmpFile.name, 'rb') as f:
                    log.debug("Writing file to %s via a temporary directory.", location.uri)
                    self.client.put_object(Bucket=location.netloc,
                                           Key=location.relativeToPathRoot, Body=f)
                log.debug("Successfully wrote file to %s via a temporary directory.", location.uri)

        if self._transaction is None:
            raise RuntimeError("Attempting to write artifact without transaction enabled")

        # Register a callback to try to delete the uploaded data if
        # the ingest fails below
        self._transaction.registerUndo("write", self.client.delete_object,
                                       Bucket=location.netloc, Key=location.relativeToPathRoot)

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
        # ingest can occur from file->s3 and s3->s3 (source can be file or s3,
        # target will always be s3). File has to exist at target location. Two
        # Schemeless URIs are assumed to obey os.path rules. Equivalent to
        # os.path.exists(fullPath) check in PosixDatastore.
        srcUri = ButlerURI(path)
        if srcUri.scheme == 'file' or not srcUri.scheme:
            if not os.path.exists(srcUri.ospath):
                raise FileNotFoundError(f"File at '{srcUri}' does not exist.")
        elif srcUri.scheme == 's3':
            if not s3CheckFileExists(srcUri, client=self.client)[0]:
                raise FileNotFoundError(f"File at '{srcUri}' does not exist.")
        else:
            raise NotImplementedError(f"Scheme type {srcUri.scheme} not supported.")

        if transfer is None:
            rootUri = ButlerURI(self.root)
            if srcUri.scheme == "file":
                raise RuntimeError(f"'{srcUri}' is not inside repository root '{rootUri}'. "
                                   "Ingesting local data to S3Datastore without upload "
                                   "to S3 is not allowed.")
            elif srcUri.scheme == "s3":
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
                    self.client.put_object(Bucket=tgtLocation.netloc,
                                           Key=tgtLocation.relativeToPathRoot, Body=f)
                if transfer == "move":
                    os.remove(srcUri.ospath)
            elif srcUri.scheme == "s3":
                # source is another S3 Bucket
                relpath = srcUri.relativeToPathRoot
                copySrc = {"Bucket": srcUri.netloc, "Key": relpath}
                self.client.copy(copySrc, self.locationFactory.netloc,
                                 tgtLocation.relativeToPathRoot)
                if transfer == "move":
                    # https://github.com/boto/boto3/issues/507 - there is no
                    # way of knowing if the file was actually deleted except
                    # for checking all the keys again, reponse is  HTTP 204 OK
                    # response all the time
                    self.client.delete(Bucket=srcUri.netloc, Key=relpath)

        # the file should exist on the bucket by now
        exists, size = s3CheckFileExists(path=tgtLocation.relativeToPathRoot,
                                         bucket=tgtLocation.netloc,
                                         client=self.client)

        return StoredFileInfo(formatter=formatter, path=tgtLocation.pathInStore,
                              storageClass=ref.datasetType.storageClass,
                              component=ref.datasetType.component(),
                              file_size=size, checksum=None)
