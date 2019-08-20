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

"""Generic file-based datastore code."""

__all__ = ("FileLikeDatastore", )

import logging

from collections import namedtuple

from lsst.daf.butler import (
    Config,
    Constraints,
    DatabaseDict,
    DatasetTypeNotSupportedError,
    Datastore,
    DatastoreConfig,
    DatastoreValidationError,
    FileDescriptor,
    FileTemplates,
    FileTemplateValidationError,
    FormatterFactory,
    LocationFactory,
    StorageClassFactory,
    StoredFileInfo,
)

from lsst.daf.butler.core.repoRelocation import replaceRoot
from lsst.daf.butler.core.utils import getInstanceOf

log = logging.getLogger(__name__)


class FileLikeDatastore(Datastore):
    """Generic Datastore for file-based implementations.

    Should be sub-classed.

    Attributes
    ----------
    config : `DatastoreConfig`
        Configuration used to create Datastore.
    registry : `Registry`
        `Registry` to use when recording the writing of Datasets.
    root : `str`
        Root directory of this `Datastore`.
    locationFactory : `LocationFactory`
        Factory for creating locations relative to the datastore
        root.
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

    defaultConfigFile = None
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    RecordTuple = namedtuple("DatastoreRecord", ["formatter", "path",
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
            self.name = "{}@{}".format(type(self).__name__,
                                       self.config["root"])

        # Support repository relocation in config
        # Existence of self.root is checked in subclass
        self.root = replaceRoot(self.config["root"], butlerRoot)

        self.locationFactory = LocationFactory(self.root)
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

    def _get_dataset_location_info(self, ref):
        """Find the `Location` of the requested dataset in the
        `Datastore` and the associated stored file information.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required `Dataset`.

        Returns
        -------
        location : `Location`
            Location of the dataset within the datastore.
            Returns `None` if the dataset can not be located.
        info : `StoredFileInfo`
            Stored information about this file and its formatter.
        """
        # Get the file information (this will fail if no file)
        try:
            storedFileInfo = self.getStoredFileInfo(ref)
        except KeyError:
            return None, None

        # Use the path to determine the location
        location = self.locationFactory.fromPath(storedFileInfo.path)

        return location, storedFileInfo

    def _prepare_for_get(self, ref, parameters=None):
        """Check parameters for ``get`` and obtain formatter and
        location.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
        parameters : `dict`
            `StorageClass`-specific parameters that specify, for example,
            a slice of the Dataset to be loaded.

        Returns
        -------
        location : `Location`
            The location from which to read the dataset.
        formatter : `Formatter`
            The `Formatter` to use to deserialize the dataset.
        info : `StoredFileInfo`
            Stored information about this file and its formatter.
        assemblerParams : `dict`
            Parameters to use for post-processing the retrieved
            dataset.
        component : `str`
            The component to be retrieved (can be `None`).
        readStorageClass : `StorageClass`
            The `StorageClass` of the dataset being read.
        """
        log.debug("Retrieve %s from %s with parameters %s", ref, self.name, parameters)

        # Get file metadata and internal metadata
        location, storedFileInfo = self._get_dataset_location_info(ref)
        if location is None:
            raise FileNotFoundError(f"Could not retrieve Dataset {ref}.")

        # We have a write storage class and a read storage class and they
        # can be different for concrete composites.
        readStorageClass = ref.datasetType.storageClass
        writeStorageClass = storedFileInfo.storageClass

        # Check that the supplied parameters are suitable for the type read
        readStorageClass.validateParameters(parameters)

        # Is this a component request?
        component = ref.datasetType.component()

        formatter = getInstanceOf(storedFileInfo.formatter,
                                  FileDescriptor(location, readStorageClass=readStorageClass,
                                                 storageClass=writeStorageClass, parameters=parameters))
        formatterParams, assemblerParams = formatter.segregateParameters()

        return location, formatter, storedFileInfo, assemblerParams, component, readStorageClass

    def _post_process_get(self, inMemoryDataset, readStorageClass, assemblerParams=None):
        """Given the Python object read from the datastore, manipulate
        it based on the supplied parameters and ensure the Python
        type is correct.

        Parameters
        ----------
        inMemoryDataset : `object`
            Dataset to check.
        readStorageClass: `StorageClass`
            The `StorageClass` used to obtain the assembler and to
            check the python type.
        assemblerParams : `dict`
            Parameters to pass to the assembler.  Can be `None`.
        """
        # Process any left over parameters
        if assemblerParams:
            inMemoryDataset = readStorageClass.assembler().handleParameters(inMemoryDataset, assemblerParams)

        # Validate the returned data type matches the expected data type
        pytype = readStorageClass.pytype
        if pytype and not isinstance(inMemoryDataset, pytype):
            raise TypeError("Got type {} from formatter but expected {}".format(type(inMemoryDataset),
                                                                                pytype))

        return inMemoryDataset

    def _prepare_for_put(self, inMemoryDataset, ref):
        """Check the arguments for ``put`` and obtain formatter and
        location.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.
        ref : `DatasetRef`
            Reference to the associated Dataset.

        Returns
        -------
        location : `Location`
            The location to write the dataset.
        formatter : `Formatter`
            The `Formatter` to use to write the dataset.

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
            formatter = self.formatterFactory.getFormatter(ref,
                                                           FileDescriptor(location,
                                                                          storageClass=storageClass))
        except KeyError as e:
            raise DatasetTypeNotSupportedError(f"Unable to find formatter for {ref}") from e

        return location, formatter

    def _register_dataset(self, ref, formatter, path, size, checksum=None):
        """Update registry to indicate that this dataset has been stored.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to register.
        formatter : `Formatter`
            Formatter to use to read this dataset.
        path : `str`
            Path to dataset relative to datastore root.
        size : `int`
            Size of the serialized dataset.
        checksum : `str`, optional
            Checksum of the serialized dataset. Can be `None`.
        """
        self.registry.addDatasetLocation(ref, self.name)

        # Associate this dataset with the formatter for later read.
        fileInfo = StoredFileInfo(formatter, path, ref.datasetType.storageClass,
                                  size=size, checksum=checksum)
        # TODO: this is only transactional if the DatabaseDict uses
        #       self.registry internally.  Probably need to add
        #       transactions to DatabaseDict to do better than that.
        self.addStoredFileInfo(ref, fileInfo)

        # Register all components with same information
        for compRef in ref.components.values():
            self.registry.addDatasetLocation(compRef, self.name)
            self.addStoredFileInfo(compRef, fileInfo)

    def _remove_from_registry(self, ref):
        """Remove rows from registry.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to remove from registry.
        """
        self.removeStoredFileInfo(ref)
        self.registry.removeDatasetLocation(self.name, ref)
        for compRef in ref.components.values():
            self.registry.removeDatasetLocation(self.name, compRef)
            self.removeStoredFileInfo(compRef)

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
            location = self.locationFactory.fromPath(template.format(ref) + "#predicted")
        else:
            # If this is a ref that we have written we can get the path.
            # Get file metadata and internal metadata
            storedFileInfo = self.getStoredFileInfo(ref)

            # Use the path to determine the location
            location = self.locationFactory.fromPath(storedFileInfo.path)

        return location.uri

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
                self.formatterFactory.getFormatterClass(entity)
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
        return self.templates.getLookupKeys() | self.formatterFactory.getLookupKeys() | \
            self.constraints.getLookupKeys()

    def validateKey(self, lookupKey, entity):
        # Docstring is inherited from base class
        # The key can be valid in either formatters or templates so we can
        # only check the template if it exists
        if lookupKey in self.templates:
            try:
                self.templates[lookupKey].validateTemplate(entity)
            except FileTemplateValidationError as e:
                raise DatastoreValidationError(e) from e
