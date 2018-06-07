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

import os
import hashlib
from collections import namedtuple

from lsst.daf.butler.core.safeFileIo import safeMakeDir
from lsst.daf.butler.core.datastore import Datastore
from lsst.daf.butler.core.datastore import DatastoreConfig  # noqa F401
from lsst.daf.butler.core.location import LocationFactory
from lsst.daf.butler.core.fileDescriptor import FileDescriptor
from lsst.daf.butler.core.formatter import FormatterFactory
from lsst.daf.butler.core.fileTemplates import FileTemplates
from lsst.daf.butler.core.storageInfo import StorageInfo
from lsst.daf.butler.core.storedFileInfo import StoredFileInfo
from lsst.daf.butler.core.utils import getInstanceOf
from lsst.daf.butler.core.storageClass import StorageClassFactory
from ..core.databaseDict import DatabaseDict

__all__ = ("PosixDatastore", )


class PosixDatastore(Datastore):
    """Basic POSIX filesystem backed Datastore.

    Attributes
    ----------
    config : `DatastoreConfig`
        Configuration used to create Datastore.
    registry : `Registry`
        `Registry` to use when recording the writing of Datasets.
    root : `str`
        Root directory of this `Datastore`.
    locationFactory : `LocationFactory`
        Factory for creating locations relative to this root.
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

    defaultConfigFile = "datastores/posixDatastore.yaml"
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    RecordTuple = namedtuple("PosixDatastoreRecord", ["formatter", "path", "storage_class"])

    @classmethod
    def setConfigRoot(cls, root, config, full):
        """Set any filesystem-dependent config options for this Datastore to
        be appropriate for a new empty repository with the given root.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the data repository.
        config : `Config`
            A Butler-level config object to update (but not a
            `ButlerConfig`, to avoid included expanded defaults).
        full : `ButlerConfig`
            A complete Butler config with all defaults expanded;
            repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from `full` to `Config`.
        """
        config["datastore.root"] = root
        for key in ("datastore.cls", "datastore.records.table"):
            config[key] = full[key]

    def __init__(self, config, registry):
        super().__init__(config, registry)
        if "root" not in self.config:
            raise ValueError("No root directory specified in configuration")
        self.root = self.config['root']
        if not os.path.isdir(self.root):
            if 'create' not in self.config or not self.config['create']:
                raise ValueError("No valid root at: {0}".format(self.root))
            safeMakeDir(self.root)

        self.locationFactory = LocationFactory(self.root)
        self.formatterFactory = FormatterFactory()
        self.storageClassFactory = StorageClassFactory()

        # Now associate formatters with storage classes
        for name, f in self.config["formatters"].items():
            self.formatterFactory.registerFormatter(name, f)

        # Read the file naming templates
        self.templates = FileTemplates(self.config["templates"])

        # Name ourselves
        self.name = "POSIXDatastore@{}".format(self.root)

        # Storage of paths and formatters, keyed by dataset_id
        types = {"path": str, "formatter": str, "storage_class": str, "dataset_id": int}
        self.records = DatabaseDict.fromConfig(self.config["records"], types=types,
                                               value=self.RecordTuple, key="dataset_id",
                                               registry=registry)

    def addStoredFileInfo(self, ref, info):
        """Record formatter information associated with this `DatasetRef`

        Parameters
        ----------
        ref : `DatasetRef`
            The Dataset that has been stored.
        info : `StoredFileInfo`
            Metadata associated with the stored Dataset.
        """
        self.records[ref.id] = self.RecordTuple(formatter=info.formatter, path=info.path,
                                                storage_class=info.storageClass.name)

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
            Stored information about the internal location of this file
            and its formatter.

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
        return StoredFileInfo(record.formatter, record.path, storageClass)

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

        # Use the path to determine the location
        location = self.locationFactory.fromPath(storedFileInfo.path)
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

        # Get file metadata and internal metadata
        try:
            storageInfo = self.registry.getStorageInfo(ref, self.name)
            storedFileInfo = self.getStoredFileInfo(ref)
        except KeyError:
            raise FileNotFoundError("Could not retrieve Dataset {}".format(ref))

        # Use the path to determine the location
        location = self.locationFactory.fromPath(storedFileInfo.path)

        # Too expensive to recalculate the checksum on fetch
        # but we can check size and existence
        if not os.path.exists(location.path):
            raise FileNotFoundError("Dataset with Id {} does not seem to exist at"
                                    " expected location of {}".format(ref.id, location.path))
        stat = os.stat(location.path)
        size = stat.st_size
        if size != storageInfo.size:
            raise RuntimeError("Integrity failure in Datastore. Size of file {} ({}) does not"
                               " match recorded size of {}".format(location.path, size, storageInfo.size))

        # We have a write storage class and a read storage class and they
        # can be different for concrete composites.
        readStorageClass = ref.datasetType.storageClass
        writeStorageClass = storedFileInfo.storageClass

        # Is this a component request?
        comp = ref.datasetType.component()

        formatter = getInstanceOf(storedFileInfo.formatter)
        try:
            result = formatter.read(FileDescriptor(location, readStorageClass=readStorageClass,
                                                   storageClass=writeStorageClass, parameters=parameters),
                                    comp)
        except Exception as e:
            raise ValueError("Failure from formatter for Dataset {}: {}".format(ref.id, e))

        # Validate the returned data type matches the expected data type
        pytype = readStorageClass.pytype
        if pytype and not isinstance(result, pytype):
            raise TypeError("Got type {} from formatter but expected {}".format(type(result), pytype))

        return result

    def put(self, inMemoryDataset, ref):
        """Write a InMemoryDataset with a given `DatasetRef` to the store.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.
        ref : `DatasetRef`
            Reference to the associated Dataset.
        """

        datasetType = ref.datasetType
        typeName = datasetType.name
        storageClass = datasetType.storageClass

        # Sanity check
        if not isinstance(inMemoryDataset, storageClass.pytype):
            raise ValueError("Inconsistency between supplied object ({}) "
                             "and storage class type ({})".format(type(inMemoryDataset), storageClass.pytype))

        # Work out output file name
        template = self.templates.getTemplate(typeName)
        location = self.locationFactory.fromPath(template.format(ref))

        # Get the formatter based on the storage class
        formatter = self.formatterFactory.getFormatter(datasetType.storageClass, typeName)

        storageDir = os.path.dirname(location.path)
        if not os.path.isdir(storageDir):
            safeMakeDir(storageDir)

        # Write the file
        path = formatter.write(inMemoryDataset, FileDescriptor(location,
                                                               storageClass=storageClass))

        self.ingest(path, ref, formatter=formatter)

        if self._transaction is not None:
            self._transaction.registerUndo('put', self.remove, ref)

    def ingest(self, path, ref, formatter=None):
        """Record that a Dataset with the given `DatasetRef` exists in the store.

        Parameters
        ----------
        path : `str`
            File path, relative to the repository root.
        ref : `DatasetRef`
            Reference to the associated Dataset.
        formatter : `Formatter` (optional)
            Formatter that should be used to retreive the Dataset.
        """
        if formatter is None:
            formatter = self.formatterFactory.getFormatter(ref.datasetType.storageClass,
                                                           ref.datasetType.name)
        # Create Storage information in the registry
        ospath = os.path.join(self.root, path)
        checksum = self.computeChecksum(ospath)
        stat = os.stat(ospath)
        size = stat.st_size
        info = StorageInfo(self.name, checksum, size)
        self.registry.addStorageInfo(ref, info)

        # Associate this dataset with the formatter for later read.
        fileInfo = StoredFileInfo(formatter, path, ref.datasetType.storageClass)
        self.addStoredFileInfo(ref, fileInfo)

        # Register all components with same information
        for compRef in ref.components.values():
            self.registry.addStorageInfo(compRef, info)
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
            `True`, the URI will be a prediction and will include a URI fragment
            "#predicted".
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

            datasetType = ref.datasetType
            typeName = datasetType.name
            template = self.templates.getTemplate(typeName)
            location = self.locationFactory.fromPath(template.format(ref) + "#predicted")
        else:
            # If this is a ref that we have written we can get the path.
            # Get file metadata and internal metadata
            storedFileInfo = self.getStoredFileInfo(ref)

            # Use the path to determine the location
            location = self.locationFactory.fromPath(storedFileInfo.path)

        return location.uri

    def remove(self, ref):
        """Indicate to the Datastore that a Dataset can be removed.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.

        Raises
        ------
        FileNotFoundError
            Attempt to remove a dataset that does not exist.

        Notes
        -----
        Some Datastores may implement this method as a silent no-op to
        disable Dataset deletion through standard interfaces.
        """
        # Get file metadata and internal metadata

        try:
            storedFileInfo = self.getStoredFileInfo(ref)
        except KeyError:
            raise FileNotFoundError("Requested dataset ({}) does not exist".format(ref))
        location = self.locationFactory.fromPath(storedFileInfo.path)
        if not os.path.exists(location.path):
            raise FileNotFoundError("No such file: {0}".format(location.uri))
        os.remove(location.path)

        # Remove rows from registries
        self.removeStoredFileInfo(ref)
        self.registry.removeStorageInfo(self.name, ref)
        for compRef in ref.components.values():
            self.registry.removeStorageInfo(self.name, compRef)
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
            raise NameError('The specified algorithm "{}" is not supported by hashlib'.format(algorithm))

        hasher = hashlib.new(algorithm)

        with open(filename, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                hasher.update(chunk)

        return hasher.hexdigest()
