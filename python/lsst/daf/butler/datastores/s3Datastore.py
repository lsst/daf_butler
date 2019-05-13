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
import shutil
import hashlib
import logging
from collections import namedtuple
from urllib.parse import urlparse
import tempfile

import boto3

from lsst.daf.butler import (Config, Datastore, DatastoreConfig, LocationFactory, S3LocationFactory,
                             Location, FileDescriptor, FormatterFactory, FileTemplates, StoredFileInfo,
                             StorageClassFactory, DatasetTypeNotSupportedError, DatabaseDict,
                             DatastoreValidationError, FileTemplateValidationError)
from lsst.daf.butler.core.utils import transactional, getInstanceOf, s3CheckFileExists, parsePathToUriElements
from lsst.daf.butler.core.safeFileIo import safeMakeDir
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
    s3locationFactory : `LocationFactory`
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

    RecordTuple = namedtuple("S3DatastoreRecord", ["formatter", "path", "storage_class",
                                                   "checksum", "file_size"])

    @classmethod
    def setConfigRoot(cls, root, config, full):
        """Set any filesystem-dependent config options for this Datastore to
        be appropriate for a new empty repository with the given root.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the data repository.
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
            should be copied from `full` to `Config`.
        """
        if 'butlerRoot' in full['.datastore.root']:
            Config.overrideParameters(DatastoreConfig, config, full,
                                      toUpdate={"root": root},
                                      toCopy=("cls", ("records", "table")))
        else:
            Config.overrideParameters(DatastoreConfig, config, full,
                                      toCopy=("cls", ("records", "table")))

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

        parsed = urlparse(self.root)
        self.s3locationFactory = S3LocationFactory(parsed.netloc, parsed.path)

        # It is practical to carry the client with your datastore - it can cover
        # all the if schema == s3 things that were needed in Butler and ButlerConfig
        # instantiation. By default it will intialize to boto3.Session('DEFAULT')
        # profile in ~/.aws/credentials. This needs to be better. Realistically also
        # we don't need the self.bucket anywhere, EXCEPT in the test suite so I left it in
        # but it should probably be removed at some point sooner or later, preferentially
        # getting the bucket name from S3Location or somewhere else. After checking if
        # the named bucket doesn't exist, ideally there would be a create flag(?) and a
        # bucket could be created through DataStore for the user and no online clicking
        # would be required - but generalizations are hard? Lot of options when opening.
        self.client = boto3.client('s3')
        self.bucket = self.s3locationFactory._bucket
        self.client.get_bucket_location(Bucket=self.bucket)

        self.formatterFactory = FormatterFactory()
        self.storageClassFactory = StorageClassFactory()

        # Now associate formatters with storage classes
        self.formatterFactory.registerFormatters(self.config["formatters"])
        self.formatterFactory.normalizeDimensions(self.registry.dimensions)

        # Read the file naming templates
        self.templates = FileTemplates(self.config["templates"])
        self.templates.normalizeDimensions(self.registry.dimensions)

        # Name ourselves
        self.name = "S3Datastore@{}".format(self.root)

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

        # Use the path to determine the location instead of self.bucket
        location = self.s3locationFactory.fromPath(storedFileInfo.path)
        return s3CheckFileExists(self.client, location.bucket, location.path)[0]

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
        log.debug("Retrieve %s from %s with parameters %s", ref, self.name, parameters)

        # Get file metadata and internal metadata
        try:
            storedFileInfo = self.getStoredFileInfo(ref)
        except KeyError:
            raise FileNotFoundError("Could not retrieve Dataset {}".format(ref))

        location = self.s3locationFactory.fromPath(storedFileInfo.path)

        # Too expensive to recalculate the checksum on fetch
        # but we can check size and existence
        exists, size = s3CheckFileExists(self.client, location.bucket, location.path)
        if not exists:
            raise FileNotFoundError("Dataset with Id {} does not seem to exist at"
                                    " expected location of {}".format(ref.id, location.path))
        if size != storedFileInfo.size:
            raise RuntimeError("Integrity failure in Datastore. Size of file {} ({}) does not"
                               " match recorded size of {}".format(location.path, size, storedFileInfo.size))

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

        # I don't know how to make this prettier. If I stick this under the if-else condition that tests
        # whether a particular formatter has a _fromBytes method then it looks really bad. But its pointless
        # to make them methods or functions because the signature is large - everything is needed. This at
        # least makes a visual distinction without descending 4 indentations deep. I don't know what
        # is the accepted style here.
        def downloadBytes():
            """Downloads a dataset from an object storage as a bytestring and returns it as an
            appropriate python object.
            """
            try:
                response = self.client.get_object(Bucket=location.bucket, Key=location.path)
            except self.client.exceptions.ClientError as err:
                if err['Error']['Code'] == '404':
                    raise FileNotFoundError('Could not find dataset {}'.format(location.uri)) from err

            pickledDataset = response['Body'].read()
            fileDescriptor = FileDescriptor(location, readStorageClass=readStorageClass,
                                            storageClass=writeStorageClass, parameters=parameters)
            try:
                result = formatter.fromBytes(pickledDataset, fileDescriptor, component=component)
            except Exception as e:
                raise ValueError("Failure from formatter for Dataset {}: {}".format(ref.id, e))

            return result, fileDescriptor

        def downloadFile():
            """Downloads a dataset from an object storage into a temporary file from which
            it is read as an appropriate python object.
            """
            with tempfile.TemporaryDirectory() as tmpRootDir:
                datasetName = location.path.split('/')[-1]
                tmpLoc = Location(tmpRootDir, datasetName)

                if os.path.exists(tmpLoc.path):
                    raise FileExistsError(f"Cannot write file for ref {ref} as "
                                          f"output file {predictedFullPath} already exists")

                # removing the tmpdir not neccesary? Just a rollback
                with self.transaction() as transaction:
                    self.client.download_file(location.bucket, location.path, tmpLoc.path)
                    fileDescriptor = FileDescriptor(tmpLoc, readStorageClass=readStorageClass,
                                                    storageClass=writeStorageClass, parameters=parameters)
                    try:
                        result = formatter.read(fileDescriptor, component=component)
                    except Exception as e:
                        raise ValueError("Failure from formatter for Dataset {}: {}".format(ref.id, e))

            return result, fileDescriptor

        if hasattr(formatter, '_fromBytes'):
            result, fileDescriptor = downloadBytes()
        else:
            result, fileDescriptor = downloadFile()

        # Process any left over parameters
        if parameters:
            result = readStorageClass.assembler().handleParameters(result, assemblerParams)

        # Validate the returned data type matches the expected data type
        pytype = readStorageClass.pytype
        if pytype and not isinstance(result, pytype):
            raise TypeError("Got type {} from formatter but expected {}".format(type(result), pytype))

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
        """
        datasetType = ref.datasetType
        storageClass = datasetType.storageClass

        # Sanity check
        if not isinstance(inMemoryDataset, storageClass.pytype):
            raise TypeError("Inconsistency between supplied object ({}) "
                            "and storage class type ({})".format(type(inMemoryDataset),
                                                                 storageClass.pytype))

        # Work out output file name
        try:
            template = self.templates.getTemplate(ref)
        except KeyError as e:
            raise DatasetTypeNotSupportedError(f"Unable to find template for {ref}") from e

        location = self.s3locationFactory.fromPath(template.format(ref))

        # Get the formatter based on the storage class
        try:
            formatter = self.formatterFactory.getFormatter(ref)
        except KeyError as e:
            raise DatasetTypeNotSupportedError(f"Unable to find formatter for {ref}") from e

        location.updateExtension(formatter.extension)
        if s3CheckFileExists(self.client, location.bucket, location.path)[0]:
            raise FileExistsError(f"Cannot write file for ref {ref} as "
                                  f"output file {location.uri} already exists")

        # unlike get, the existance of the bucket has been verified at init and ButlerConfig
        # downloads - therefore not a lot of error handling is needed?
        if hasattr(formatter, '_toBytes'):
            with self.transaction() as transaction:
                fileDescriptor = FileDescriptor(location, storageClass=storageClass)
                pickledDataset = formatter.toBytes(inMemoryDataset,  fileDescriptor)
                self.client.put_object(Bucket=location.bucket, Key=location.path, Body=pickledDataset)
            log.debug("Wrote file to %s", location.uri)
        else:
            # we can't change location attrs, so we need to make up a new one that we clean up afterwards.
            with tempfile.TemporaryDirectory() as tmpRootDir:
                datasetName = location.path.split('/')[-1]
                tmpLoc = Location(tmpRootDir, datasetName)
                # this should never happen
                if os.path.exists(tmpLoc.path):
                    raise FileExistsError(f"Cannot write file for ref {ref} as "
                                          f"output file {tmpLoc.uri} already exists")
                with self.transaction() as transaction:
                    tmpPath = formatter.write(inMemoryDataset, FileDescriptor(tmpLoc, storageClass=storageClass))
                    assert tmpLoc.path == os.path.join(tmpRootDir, tmpPath)
                    self.client.upload_file(tmpLoc.path, location.bucket, location.path)
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
            If not None, must be one of 'move', 'copy', 'hardlink', or
            'symlink' indicating how to transfer the file.  The new
            filename and location will be determined via template substitution,
            as with ``put``.  If the file is outside the datastore root, it
            must be transferred somehow.

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

        # if I understood the point of ingest correctly - put is for putting python objects in store,
        # ingest is for putting file-like stuff in store. That makes an undefined and 4 defined cases:
        # 1) filesystem-bucket with file upload - allowed
        # 2) filesystem-bucket without upload - not allowed, registry and datastore not synched
        # 3) bucket-bucket with copying - allowed
        # 4) bucket-bucket without copying - not allowed
        #        - filesystem equivalent of link does not exist
        #        - if someone priviledged to a bucket made a transfer, to another bucket from
        #          which an unauthorized user can read, without copy they don't have the data even
        #          if I would have added extra columns in the DB to describe each location individually.
        # Practically, there is an additional case, when ingest is called by put - the file was already
        # uploaded in put (it exists in bucket). This ends up being equal to the case where download and
        # upload locations are the same - as long as the location is verifiably within store root its fine.
        scheme, root, relpath = parsePathToUriElements(path)
        if (scheme != 'file://') and (scheme != 's3://'):
            raise NotImplementedError('Scheme type {} not supported.'.format(scheme))

        if transfer is None:
            if scheme == 'file://':
                # someone wants to ingest a local file, but not transfer it to object storage
                abspath = os.path.join(root, relpath)
                raise RuntimeError(("'{}' is not inside repository root '{}'. Ingesting local data to"
                                    " S3Datastore without upload to S3 not allowed.").format(abspath, self.root))
            if scheme == 's3://':
                # if both transfer is None and scheme s3, file was already uploaded in put
                # rootdir is sometimes sent as s3://bucketname/root and sometimes s3://bucketname/root/
                # There is no equivalent of os.isdir/os.isfile for the S3 - problem for parsing.
                # The string comparisons will fail since one of them will have the '/' and the other not.
                #I hope I just don't know how to use urllib or something because this is crap.
                rootSchema, bucketname, rootDir = parsePathToUriElements(self.root)
                topDir = relpath.split('/')[0] + '/'
                rootDir = rootDir + '/' if rootDir[-1] != '/' else rootDir
                if (bucketname != root) or (rootDir != topDir):
                    raise RuntimeError("'{}' is not inside repository root '{}'".format(path, self.root))
        elif transfer == 'upload' or transfer == 'copy':
            if scheme == 'file://':
                # reuploads not allowed?
                if s3CheckFileExists(self.client, root, relpath)[0]:
                    raise FileExistsError("File '{}' already exists".format(path))

                template = self.templates.getTemplate(ref)
                location = self.s3locationFactory.fromPath(template.format(ref))
                location.updateExtension(formatter.extension)

                with self.transaction() as transaction:
                    self.client.upload_file(path, location.bucket, location.path)
            if scheme == 's3://':
                # ingesting is done from another bucket - not tested
                if s3CheckFileExists(self.client, root, relpath)[0]:
                    fullpath = os.path.join(root, relpath)
                    raise FileExistsError("File '{}' already exists".format(schema+fullpath))

                copySrc = {'Bucket': root, 'Key': relpath}
                self.client.copy(copySrc, self.bucket, relpath)
        else:
            raise NotImplementedError("Transfer type '{}' not supported.".format(transfer))

        path = os.path.join(self.bucket, relpath)
        location = self.s3locationFactory.fromPath(path)

        # Create Storage information in the registry
        # TODO: fix the checksum calcs already
        #checksum = self.computeChecksum(fullPath)

        # the file should exist on the bucket by now
        exists, size = s3CheckFileExists(self.client, location.bucket, relpath)
        self.registry.addDatasetLocation(ref, self.name)

        # Associate this dataset with the formatter for later read.
        fileInfo = StoredFileInfo(formatter, path, ref.datasetType.storageClass,
                                  size=size)
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
                raise FileNotFoundError("Dataset {} not in this datastore".format(ref))

            template = self.templates.getTemplate(ref)
            location = self.s3locationFactory.fromPath(template.format(ref) + "#predicted")
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
        # Get file metadata and internal metadata

        try:
            storedFileInfo = self.getStoredFileInfo(ref)
        except KeyError:
            raise FileNotFoundError("Requested dataset ({}) does not exist".format(ref))
        location = self.s3locationFactory.fromPath(storedFileInfo.path)
        if not s3CheckFileExists(self.client, location.bucket, location.path ): # os.path.exists(location.path):
            raise FileNotFoundError("No such file: {0}".format(location.uri))
        self.client.delete_object(Bucket=location.bucket, Key=location.path)
        #os.remove(location.path)

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

        tmpdir = "/home/dinob/uni/lsstspark/simple_repo/s3_repo"
        scheme, root, relpath = parsePathToUriElements(filename)
        dirpath, name = os.path.split(relpath)
        potentialloc = os.path.join(tmpdir, name)
        if os.path.exists(potentialloc):
            filename = potentialloc
        else:
            client = boto3.client('s3')
            client.download_file(root, relpath, potentialloc)
            filename = potentialloc

        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(block_size), b""):
                hasher.update(chunk)

        return hasher.hexdigest()
