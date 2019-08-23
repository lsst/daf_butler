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

"""S3 datastore."""

__all__ = ("S3Datastore", )

import boto3
import logging
import os
import pathlib
import tempfile

from lsst.daf.butler import (
    ButlerURI,
    DatasetTypeNotSupportedError,
    Location,
)

from .fileLikeDatastore import FileLikeDatastore
from lsst.daf.butler.core.s3utils import s3CheckFileExists, bucketExists
from lsst.daf.butler.core.utils import transactional

log = logging.getLogger(__name__)


class S3Datastore(FileLikeDatastore):
    """Basic S3 Object Storage backed Datastore.

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
    """

    defaultConfigFile = "datastores/s3Datastore.yaml"
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    def __init__(self, config, registry, butlerRoot=None):
        super().__init__(config, registry, butlerRoot)

        self.client = boto3.client("s3")
        if not bucketExists(self.locationFactory.netloc):
            # PosixDatastore creates the root directory if one does not exist.
            # Calling s3 client.create_bucket is possible but also requires
            # ACL LocationConstraints, Permissions and other configuration
            # parameters, so for now we do not create a bucket if one is
            # missing. Further discussion can make this happen though.
            raise IOError(f"Bucket {self.locationFactory.netloc} does not exist!")

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
        return s3CheckFileExists(location, client=self.client)[0]

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
        location, formatter, storedFileInfo, assemblerParams, \
            component, readStorageClass = self._prepare_for_get(ref,
                                                                parameters)

        # since we have to make a GET request to S3 anyhow (for download) we
        # might as well use the HEADER metadata for size comparison instead.
        # s3CheckFileExists would just duplicate GET/LIST charges in this case.
        try:
            response = self.client.get_object(Bucket=location.netloc,
                                              Key=location.relativeToPathRoot)
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

        if response["ContentLength"] != storedFileInfo.file_size:
            raise RuntimeError("Integrity failure in Datastore. Size of file {} ({}) does not"
                               " match recorded size of {}".format(location.path, response["ContentLength"],
                                                                   storedFileInfo.file_size))

        # download the data as bytes
        serializedDataset = response["Body"].read()

        # format the downloaded bytes into appropriate object directly, or via
        # tempfile (when formatter does not support to/from/Bytes). This is S3
        # equivalent of PosixDatastore formatter.read try-except block.
        try:
            result = formatter.fromBytes(serializedDataset, component=component)
        except NotImplementedError:
            with tempfile.NamedTemporaryFile(suffix=formatter.extension) as tmpFile:
                tmpFile.file.write(serializedDataset)
                formatter._fileDescriptor.location = Location(*os.path.split(tmpFile.name))
                result = formatter.read(component=component)
        except Exception as e:
            raise ValueError(f"Failure from formatter for Dataset {ref.id}: {e}") from e

        return self._post_process_get(result, readStorageClass, assemblerParams)

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

        # in PosixDatastore a directory can be created by `safeMakeDir`. In S3
        # `Keys` instead only look like directories, but are not. We check if
        # an *exact* full key already exists before writing instead. The insert
        # key operation is equivalent to creating the dir and the file.
        location.updateExtension(formatter.extension)
        if s3CheckFileExists(location, client=self.client,)[0]:
            raise FileExistsError(f"Cannot write file for ref {ref} as "
                                  f"output file {location.uri} exists.")

        # upload the file directly from bytes or by using a temporary file if
        # _toBytes is not implemented
        try:
            serializedDataset = formatter.toBytes(inMemoryDataset)
            self.client.put_object(Bucket=location.netloc, Key=location.relativeToPathRoot,
                                   Body=serializedDataset)
            log.debug("Wrote file directly to %s", location.uri)
        except NotImplementedError:
            with tempfile.NamedTemporaryFile(suffix=formatter.extension) as tmpFile:
                formatter._fileDescriptor.location = Location(*os.path.split(tmpFile.name))
                formatter.write(inMemoryDataset)
                self.client.upload_file(Bucket=location.netloc, Key=location.relativeToPathRoot,
                                        Filename=tmpFile.name)
                log.debug("Wrote file to %s via a temporary directory.", location.uri)

        # URI is needed to resolve what ingest case are we dealing with
        self.ingest(location.uri, ref, formatter=formatter)

    @transactional
    def ingest(self, path, ref, formatter=None, transfer=None):
        """Add an on-disk file with the given `DatasetRef` to the store,
        possibly transferring it.

        The caller is responsible for ensuring that the given (or predicted)
        Formatter is consistent with how the file was written; `ingest` will
        in general silently ignore incorrect formatters (as it cannot
        efficiently verify their correctness), deferring errors until ``get``
        is first called on the ingested dataset.

        Parameters
        ----------
        path : `str`
            File path.  Treated as relative to the repository root if not
            absolute.
        ref : `DatasetRef`
            Reference to the associated Dataset.
        formatter : `Formatter` (optional)
            Formatter that should be used to retreive the Dataset.  If not
            provided, the formatter will be constructed according to
            Datastore configuration.
        transfer : str (optional)
            If not None, must be one of 'move' or 'copy' indicating how to
            transfer the file.  The new filename and location will be
            determined via template substitution, as with ``put``.  If the file
            is outside the datastore root, it must be transferred somehow.

        Raises
        ------
        RuntimeError
            Raised if ``transfer is None`` and path is outside the repository
            root.
        FileNotFoundError
            Raised if the file at ``path`` does not exist.
        FileExistsError
            Raised if ``transfer is not None`` but a file already exists at the
            location computed from the template.
        PermissionError
            Raised when check if file exists at target location in S3 can not
            be made because IAM user used lacks s3:GetObject or s3:ListBucket
            permissions.
        """
        if not self.constraints.isAcceptable(ref):
            # Raise rather than use boolean return value.
            raise DatasetTypeNotSupportedError(f"Dataset {ref} has been rejected by this datastore via"
                                               " configuration.")

        if formatter is None:
            formatter = self.formatterFactory.getFormatterClass(ref)

        # ingest can occur from file->s3 and s3->s3 (source can be file or s3,
        # target will always be s3). File has to exist at target location. Two
        # Schemeless URIs are assumed to obey os.path rules. Equivalent to
        # os.path.exists(fullPath) check in PosixDatastore.
        srcUri = ButlerURI(path)
        if srcUri.scheme == 'file' or not srcUri.scheme:
            if not os.path.exists(srcUri.ospath):
                raise FileNotFoundError(f"File at '{srcUri}' does not exist; note that paths to ingest are "
                                        "assumed to be relative to self.root unless they are absolute.")
        elif srcUri.scheme == 's3':
            if not s3CheckFileExists(srcUri, client=self.client)[0]:
                raise FileNotFoundError("File at '{}' does not exist; note that paths to ingest are "
                                        "assumed to be relative to self.root unless they are absolute."
                                        .format(srcUri))
        else:
            raise NotImplementedError(f"Scheme type {srcUri.scheme} not supported.")

        # Transfer is generaly None when put calls ingest. In that case file is
        # uploaded in put, or already in proper location, so source location
        # must be inside repository. In other cases, created target location
        # must be inside root and source file must be deleted when 'move'd.
        if transfer is None:
            rootUri = ButlerURI(self.root)
            if srcUri.scheme == "file":
                raise RuntimeError(f"'{srcUri}' is not inside repository root '{rootUri}'. "
                                   "Ingesting local data to S3Datastore without upload "
                                   "to S3 is not allowed.")
            elif srcUri.scheme == "s3":
                if not srcUri.path.startswith(rootUri.path):
                    raise RuntimeError(f"'{srcUri}' is not inside repository root '{rootUri}'.")
            p = pathlib.PurePosixPath(srcUri.relativeToPathRoot)
            pathInStore = str(p.relative_to(rootUri.relativeToPathRoot))
            tgtLocation = self.locationFactory.fromPath(pathInStore)
        elif transfer == "move" or transfer == "copy":
            if srcUri.scheme == "file":
                # source is on local disk.
                template = self.templates.getTemplate(ref)
                location = self.locationFactory.fromPath(template.format(ref))
                tgtPathInStore = formatter.predictPathFromLocation(location)
                tgtLocation = self.locationFactory.fromPath(tgtPathInStore)
                self.client.upload_file(Bucket=tgtLocation.netloc, Key=tgtLocation.relativeToPathRoot,
                                        Filename=srcUri.ospath)
                if transfer == "move":
                    os.remove(srcUri.ospath)
            elif srcUri.scheme == "s3":
                # source is another S3 Bucket
                relpath = srcUri.relativeToPathRoot
                copySrc = {"Bucket": srcUri.netloc, "Key": relpath}
                self.client.copy(copySrc, self.locationFactory.netloc, relpath)
                if transfer == "move":
                    # https://github.com/boto/boto3/issues/507 - there is no
                    # way of knowing if the file was actually deleted except
                    # for checking all the keys again, reponse is  HTTP 204 OK
                    # response all the time
                    self.client.delete(Bucket=srcUri.netloc, Key=relpath)
                p = pathlib.PurePosixPath(srcUri.relativeToPathRoot)
                relativeToDatastoreRoot = str(p.relative_to(rootUri.relativeToPathRoot))
                tgtLocation = self.locationFactory.fromPath(relativeToDatastoreRoot)
        else:
            raise NotImplementedError(f"Transfer type '{transfer}' not supported.")

        # the file should exist on the bucket by now
        exists, size = s3CheckFileExists(path=tgtLocation.relativeToPathRoot,
                                         bucket=tgtLocation.netloc,
                                         client=self.client)

        # Update the registry
        self._register_dataset_file(ref, formatter,
                                    tgtLocation.pathInStore,
                                    size, None)

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
        location, storefFileInfo = self._get_dataset_location_info(ref)
        if location is None:
            raise FileNotFoundError(f"Requested dataset ({ref}) does not exist")

        if not s3CheckFileExists(location, client=self.client):
            raise FileNotFoundError(f"No such file: {location.uri}")

        # https://github.com/boto/boto3/issues/507 - there is no way of knowing
        # if the file was actually deleted
        self.client.delete_object(Bucket=location.netloc, Key=location.relativeToPathRoot)

        # Remove rows from registries
        self._remove_from_registry(ref)
