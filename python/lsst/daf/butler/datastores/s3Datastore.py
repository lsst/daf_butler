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

import os
import logging
from collections import namedtuple
import tempfile

import boto3

from lsst.daf.butler import (Config, Datastore, DatastoreConfig, LocationFactory, ButlerURI,
                             Location, FileDescriptor, FormatterFactory, FileTemplates,
                             StoredFileInfo, StorageClassFactory, DatasetTypeNotSupportedError,
                             DatabaseDict, DatastoreValidationError, FileTemplateValidationError,
                             Constraints)
from lsst.daf.butler.core.utils import transactional, getInstanceOf
from lsst.daf.butler.core.s3utils import s3CheckFileExists, bucketExists
from lsst.daf.butler.core.repoRelocation import replaceRoot

log = logging.getLogger(__name__)


class S3Datastore(Datastore):
    """Basic S3 Object Storage backed Datastore.

    Attributes
    ----------
    config : `DatastoreConfig`
        Configuration used to create Datastore.
    registry : `Registry`
        `Registry` to use when recording the writing of Datasets.
    root : `str`
        Root directory of this `Datastore`.
    locationFactory : `LocationFactory`
        Factory for creating locations relative to S3 bucket.
    locationFactory : `LocationFactory`
        Factory for creating locations relative to datastore root
        as if it was file:// .
    formatterFactory : `FormatterFactory`
        Factory for creating instances of formatters.
    storageClassFactory : `StorageClassFactory`
        Factory for creating storage class instances from name.
    templates : `FileTemplates`
        File templates that can be used by this `Datastore`.
    name : `str`
        Label associated with this Datastore.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration.

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

    RecordTuple = namedtuple("s3datastorerecord", ["formatter", "path",
                                                   "storage_class", "checksum",
                                                   "file_size"])

    @classmethod
    def setConfigRoot(cls, root, config, full, overwrite=True):
        """Set any filesystem-dependent config options for this Datastore to
        be appropriate for a new empty repository with the given root.
        Parameters
        ----------
        root : `str`
            URI to the root of the data repository.
        config : `Config`
            A `Config` to update. Only the subset understood by
            this component will be updated. Will not expand
            defaults.
        full : `Config`
            A complete config with all defaults expanded that can be
            converted to a `DatastoreConfig`. Read-only and will not be
            modified by this method.
            Repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from ``full`` to ``config``.
        overwrite : `bool`, optional
            If `False`, do not modify a value in ``config`` if the value
            already exists.  Default is always to overwrite with the provided
            ``root``.
        Notes
        -----
        If a keyword is explicitly defined in the supplied ``config`` it
        will not be overridden by this method if ``overwrite`` is `False`.
        This allows explicit values set in external configs to be retained.
        """
        Config.updateParameters(DatastoreConfig, config, full,
                                toUpdate={"root": root},
                                toCopy=("cls", ("records", "table")), overwrite=overwrite)

    def __init__(self, config, registry, butlerRoot=None):
        super().__init__(config, registry)
        if "root" not in self.config:
            raise ValueError("No root directory specified in configuration")

        # Name ourselves either using an explicit name or a name
        # derived from the (unexpanded) root
        if "name" in self.config:
            self.name = self.config["name"]
        else:
            self.name = "S3Datastore@{}".format(self.config["root"])

        # Support repository relocation in config
        self.root = replaceRoot(self.config["root"], butlerRoot)
        self.locationFactory = LocationFactory(self.root)

        self.client = boto3.client("s3")
        if not bucketExists(self.locationFactory.netloc):
            # PosixDatastore creates the root directory if one does not exist.
            # Calling s3 client.create_bucket is possible but also requires
            # ACL LocationConstraints, Permissions and other configuration
            # parameters, so for now we do not create a bucket if one is
            # missing. Further discussion can make this happen though.
            raise IOError(f"Bucket {self.locationFactory.netloc} does not exists!")

        self.formatterFactory = FormatterFactory()
        self.storageClassFactory = StorageClassFactory()

        # Now associate formatters with storage classes
        self.formatterFactory.registerFormatters(self.config["formatters"],
                                                 universe=self.registry.dimensions)

        # Read the file naming templates
        self.templates = FileTemplates(self.config["templates"],
                                       universe=self.registry.dimensions)

        # And read the constraints list
        constraintsConfig = self.config.get("constraints")
        self.constraints = Constraints(constraintsConfig, universe=self.registry.dimensions)

        # Storage of paths and formatters, keyed by dataset_id
        types = {"path": str, "formatter": str, "storage_class": str,
                 "file_size": int, "checksum": str, "dataset_id": int}
        lengths = {"path": 256, "formatter": 128, "storage_class": 64,
                   "checksum": 128}
        self.records = DatabaseDict.fromConfig(self.config["records"], types=types,
                                               value=self.RecordTuple, key="dataset_id",
                                               lengths=lengths, registry=registry)

    def __str__(self):
        return self.root

    def addStoredFileInfo(self, ref, info):
        """Record internal storage information associated with this
        `DatasetRef`
        Parameters
        ----------
        ref : `DatasetRef`
            The Dataset that has been stored.
        info : `StoredFileInfo`
            Metadata associated with the stored Dataset.
        """
        self.records[ref.id] = self.RecordTuple(formatter=info.formatter, path=info.path,
                                                storage_class=info.storageClass.name,
                                                checksum=info.checksum, file_size=info.size)

    def removeStoredFileInfo(self, ref):
        """Remove information about the file associated with this dataset.
        Parameters
        ----------
        ref : `DatasetRef`
            The Dataset that has been removed.
        """
        del self.records[ref.id]

    def getStoredFileInfo(self, ref):
        """Retrieve information associated with file stored in this
        `Datastore`.
        Parameters
        ----------
        ref : `DatasetRef`
            The Dataset that is to be queried.
        Returns
        -------
        info : `StoredFileInfo`
            Stored information about this file and its formatter.
        Raises
        ------
        KeyError
            Dataset with that id can not be found.
        """
        record = self.records.get(ref.id, None)
        if record is None:
            raise KeyError("Unable to retrieve formatter associated with Dataset {}".format(ref.id))
        # Convert name of StorageClass to instance
        storageClass = self.storageClassFactory.getStorageClass(record.storage_class)
        return StoredFileInfo(record.formatter, record.path, storageClass,
                              checksum=record.checksum, size=record.file_size)

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
        # Get the file information (this will fail if no file)
        try:
            storedFileInfo = self.getStoredFileInfo(ref)
        except KeyError:
            return False

        loc = self.locationFactory.fromPath(storedFileInfo.path)
        return s3CheckFileExists(self.client, loc)[0]

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
        log.debug("Retrieve %s from %s with parameters %s", ref, self.name,
                  parameters)

        # Get file metadata and internal metadata
        try:
            storedFileInfo = self.getStoredFileInfo(ref)
        except KeyError:
            raise FileNotFoundError(f"Could not retrieve Dataset {ref}.")

        location = self.locationFactory.fromPath(storedFileInfo.path)

        # since we have to make a GET request to S3 anyhow (for download) we
        # might as well use the HEADER metadata for size comparison instead.
        # s3CheckFileExists would just duplicate GET/LIST charges in this case.
        try:
            response = self.client.get_object(Bucket=location.netloc,
                                              Key=location.relativeToNetloc)
        except self.client.exceptions.ClientError as err:
            errorcode = err.response["ResponseMetadata"]["HTTPStatusCode"]
            # the HTTPS code 403 should be reserved for authorization failures.
            # For some reason it's 403 that is raised when paths, that begin
            # with '/', are being asked for. So `path/file` raises a 404 but
            # `/path/file` raises a 403. Unit tests right now demand a
            # FileExistsError be raised, so I do, but this should be updated.
            # This has been the way boto3 does errors since at least 2015
            # https://github.com/boto/boto3/issues/167
#            if errorcode == 403:
#                errmsg = "Insufficient, or invalid, permissions."
#                raise PermissionError(errmsg) from err
            if errorcode == 404 or errorcode == 403:
                errmsg = "Dataset with Id {} does not exists at expected location {}."
                raise FileNotFoundError(errmsg.format(ref.id, location)) from err
            # other errors are reraised also, but less descriptively
            raise err

        if response["ContentLength"] != storedFileInfo.size:
            errstr = ("Integrity failure in Datastore. Size of file {} ({}) "
                      "does not match recorded size of {}")
            raise RuntimeError(errstr.format(location.path,
                                             response["ContentLength"],
                                             storedFileInfo.size))

        # We have a write storage class and a read storage class and they
        # can be different for concrete composites.
        readStorageClass = ref.datasetType.storageClass
        writeStorageClass = storedFileInfo.storageClass

        # Check that the supplied parameters are suitable for the type read
        readStorageClass.validateParameters(parameters)

        # Is this a component request?
        component = ref.datasetType.component()
        formatter = getInstanceOf(storedFileInfo.formatter)
        formatterParams, assemblerParams = formatter.segregateParameters(parameters)

        # download the data as bytes
        serializedDataset = response["Body"].read()
        fileDescriptor = FileDescriptor(location,
                                        readStorageClass=readStorageClass,
                                        storageClass=writeStorageClass,
                                        parameters=parameters)

        # format the downloaded bytes into appropriate object directly, or via
        # tempfile (when formatter does not support to/from/Bytes)
        try:
            result = formatter.fromBytes(serializedDataset, fileDescriptor,
                                         component=component)
        except NotImplementedError:
            with tempfile.NamedTemporaryFile(suffix=formatter.extension) as tmpFile:
                tmpFile.file.write(serializedDataset)
                fileDescriptor.location = Location(*os.path.split(tmpFile.name))
                result = formatter.read(fileDescriptor, component=component)
        except Exception as e:
            raise ValueError(f"Failure from formatter for Dataset {ref.id}: {e}") from e

        # Process any left over parameters
        if parameters:
            result = readStorageClass.assembler().handleParameters(result, assemblerParams)

        # Validate the returned data type matches the expected data type
        pytype = readStorageClass.pytype
        if pytype and not isinstance(result, pytype):
            raise TypeError(f"Got type {type(result)} from formatter but expected {pytype}")

        return result

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
        datasetType = ref.datasetType
        storageClass = datasetType.storageClass

        # Sanity check
        if not isinstance(inMemoryDataset, storageClass.pytype):
            raise TypeError("Inconsistency between supplied object ({}) "
                            "and storage class type ({})".format(type(inMemoryDataset),
                                                                 storageClass.pytype))

        # Confirm that we can accept this dataset
        if not self.constraints.isAcceptable(ref):
            # Raise rather than use boolean return value.
            raise DatasetTypeNotSupportedError(f"Dataset {ref} has been rejected by this datastore via"
                                               " configuration.")

        # Work out output file name
        try:
            template = self.templates.getTemplate(ref)
        except KeyError as e:
            raise DatasetTypeNotSupportedError(f"Unable to find template for {ref}") from e

        location = self.locationFactory.fromPath(template.format(ref))

        # Get the formatter based on the storage class
        try:
            formatter = self.formatterFactory.getFormatter(ref)
        except KeyError as e:
            raise DatasetTypeNotSupportedError(f"Unable to find formatter for {ref}") from e

        # in PosixDatastore a directory can be created by `safeMakeDir` and
        # then the file is written if checks against overwriting an existing
        # file pass. But in S3 `Keys` instead only look like directories, but
        # are not. We check if an exact full key already exists before writing
        # instead, if it does not the insert key operation will be equivalent
        # to creating a directory and the file together.
        location.updateExtension(formatter.extension)
        if s3CheckFileExists(self.client, location)[0]:
            raise FileExistsError(f"Cannot write file for ref {ref} as "
                                  f"output file {location.uri} exists.")

        # upload the file directly from bytes or by using a temporary file if
        # _toBytes is not implemented for the formatter in question
        fileDescriptor = FileDescriptor(location, storageClass=storageClass)
        try:
            serializedDataset = formatter.toBytes(inMemoryDataset, fileDescriptor)
            self.client.put_object(Bucket=location.netloc, Key=location.relativeToNetloc,
                                   Body=serializedDataset)
            log.debug("Wrote file directly to %s", location.uri)
        except NotImplementedError:
            with tempfile.NamedTemporaryFile(suffix=formatter.extension) as tmpFile:
                fileDescriptor.location = Location(*os.path.split(tmpFile.name))
                formatter.write(inMemoryDataset, fileDescriptor)
                self.client.upload_file(Bucket=location.netloc, Key=location.relativeToNetloc,
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
        """
        if formatter is None:
            formatter = self.formatterFactory.getFormatter(ref)

        # we can not assume that root is the same as self.root when ingesting
        uri = ButlerURI(path)
        if (uri.scheme != "file") and (uri.scheme != "s3"):
            raise NotImplementedError(f"Scheme type {uri.scheme} not supported.")

        if transfer is None:
            if uri.scheme == "file":
                # ingest a local file, but not transfer it to object storage
                errmsg = (f"'{uri}' is not inside repository root '{self.root}'. "
                          "Ingesting local data to S3Datastore without upload "
                          "to S3 is not allowed.")
                raise RuntimeError(errmsg.format(uri, self.root))
            elif uri.scheme == "s3":
                # if transfer is None and scheme is s3, put already uploaded it
                rooturi = ButlerURI(self.root)
                if not uri.path.startswith(rooturi.path):
                    raise RuntimeError(f"'{uri}' is not inside repository root '{rooturi}'.")
        elif transfer == "move" or transfer == "copy":
            if uri.scheme == "file":
                # uploading file from local disk and potentially deleting it
                if s3CheckFileExists(self.client, uri)[0]:
                    raise FileExistsError(f"File '{path}' exists!")

                template = self.templates.getTemplate(ref)
                location = self.locationFactory.fromPath(template.format(ref))
                location.updateExtension(formatter.extension)
                self.client.upload_file(Bucket=location.netloc, Key=location.relativeToNetloc,
                                        Filename=path)
                if transfer == "move":
                    os.remove(path)
            elif uri.scheme == "s3":
                # copying files between buckets, potentially deleting src files
                if s3CheckFileExists(self.client, uri)[0]:
                    raise FileExistsError(f"File '{uri}' exists.")

                relpath = uri.relativeToNetloc
                copySrc = {"Bucket": uri.netloc, "Key": relpath}
                self.client.copy(copySrc, self.locationFactory.netloc,
                                 relpath)
                if transfer == "move":
                    # https://github.com/boto/boto3/issues/507 - there is no
                    # way of knowing if the file was actually deleted except
                    # for checking all the keys again, reponse just ends up
                    # being HTTP 204 OK request all the time
                    self.client.delete(Bucket=uri.netloc, Key=relpath)
        else:
            raise NotImplementedError(f"Transfer type '{transfer}' not supported.")

        if path.startswith(self.root):
            path = path[len(self.root):].lstrip("/")
        location = self.locationFactory.fromPath(path)

        # the file should exist on the bucket by now
        exists, size = s3CheckFileExists(self.client, bucket=location.netloc,
                                         filepath=uri.relativeToNetloc)
        self.registry.addDatasetLocation(ref, self.name)

        # Associate this dataset with the formatter for later read.
        fileInfo = StoredFileInfo(formatter, path,
                                  ref.datasetType.storageClass, size=size)
        # TODO: this is only transactional if the DatabaseDict uses
        #       self.registry internally.  Probably need to add
        #       transactions to DatabaseDict to do better than that.
        self.addStoredFileInfo(ref, fileInfo)

        # Register all components with same information
        for compRef in ref.components.values():
            self.registry.addDatasetLocation(compRef, self.name)
            self.addStoredFileInfo(compRef, fileInfo)

    def getUri(self, ref, predict=False):
        """URI to the Dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
        predict : `bool`
            If `True`, allow URIs to be returned of datasets that have not
            been written.

        Returns
        -------
        uri : `str`
            URI string pointing to the Dataset within the datastore. If the
            Dataset does not exist in the datastore, and if ``predict`` is
            `True`, the URI will be a prediction and will include a URI
            fragment "#predicted".
            If the datastore does not have entities that relate well
            to the concept of a URI the returned URI string will be
            descriptive. The returned URI is not guaranteed to be obtainable.

        Raises
        ------
        FileNotFoundError
            A URI has been requested for a dataset that does not exist and
            guessing is not allowed.

        """
        # if this has never been written then we have to guess
        if not self.exists(ref):
            if not predict:
                raise FileNotFoundError(f"Dataset {ref} not in this datastore")

            template = self.templates.getTemplate(ref)
            location = self.s3locationFactory.fromPath(template.format(ref) +
                                                       "#predicted")
        else:
            # If this is a ref that we have written we can get the path.
            # Get file metadata and internal metadata
            storedFileInfo = self.getStoredFileInfo(ref)

            # Use the path to determine the location
            location = self.s3locationFactory.fromPath(storedFileInfo.path)

        return location.uri

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
        try:
            storedFileInfo = self.getStoredFileInfo(ref)
        except KeyError:
            raise FileNotFoundError(f"Requested dataset ({ref}) does not exist")
        location = self.locationFactory.fromPath(storedFileInfo.path)
        if not s3CheckFileExists(self.client, location):
            raise FileNotFoundError("No such file: {0}".format(location.uri))

        # https://github.com/boto/boto3/issues/507 - there is no way of knowing
        # if the file was actually deleted
        self.client.delete_object(Bucket=location.netloc, Key=location.relativeToNetloc)

        # Remove rows from registries
        self.removeStoredFileInfo(ref)
        self.registry.removeDatasetLocation(self.name, ref)
        for compRef in ref.components.values():
            self.registry.removeDatasetLocation(self.name, compRef)
            self.removeStoredFileInfo(compRef)

    def transfer(self, inputDatastore, ref):
        """Retrieve a Dataset from an input `Datastore`,
        and store the result in this `Datastore`.

        Parameters
        ----------
        inputDatastore : `Datastore`
            The external `Datastore` from which to retreive the Dataset.
        ref : `DatasetRef`
            Reference to the required Dataset in the input data store.

        """
        assert inputDatastore is not self  # unless we want it for renames?
        inMemoryDataset = inputDatastore.get(ref)
        return self.put(inMemoryDataset, ref)

    def validateConfiguration(self, entities, logFailures=False):
        """Validate some of the configuration for this datastore.

        Parameters
        ----------
        entities : iterable of `DatasetRef`, `DatasetType`, or `StorageClass`
            Entities to test against this configuration.  Can be differing
            types.
        logFailures : `bool`, optional
            If `True`, output a log message for every validation error
            detected.

        Raises
        ------
        DatastoreValidationError
            Raised if there is a validation problem with a configuration.
            All the problems are reported in a single exception.

        Notes
        -----
        This method checks that all the supplied entities have valid file
        templates and also have formatters defined.
        """

        templateFailed = None
        try:
            self.templates.validateTemplates(entities, logFailures=logFailures)
        except FileTemplateValidationError as e:
            templateFailed = str(e)

        formatterFailed = []
        for entity in entities:
            try:
                self.formatterFactory.getFormatter(entity)
            except KeyError as e:
                formatterFailed.append(str(e))
                if logFailures:
                    log.fatal("Formatter failure: %s", e)

        if templateFailed or formatterFailed:
            messages = []
            if templateFailed:
                messages.append(templateFailed)
            if formatterFailed:
                messages.append(",".join(formatterFailed))
            msg = ";\n".join(messages)
            raise DatastoreValidationError(msg)

    def getLookupKeys(self):
        # Docstring is inherited from base class
        return self.templates.getLookupKeys() | self.formatterFactory.getLookupKeys()

    def validateKey(self, lookupKey, entity):
        # Docstring is inherited from base class
        # The key can be valid in either formatters or templates so we can
        # only check the template if it exists
        if lookupKey in self.templates:
            try:
                self.templates[lookupKey].validateTemplate(entity)
            except FileTemplateValidationError as e:
                raise DatastoreValidationError(e) from e
