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

from dataclasses import dataclass
from typing import ClassVar, Type, Optional

from lsst.daf.butler import (
    Config,
    DatabaseDict,
    DatabaseDictRecordBase,
    DatasetTypeNotSupportedError,
    DatastoreConfig,
    DatastoreValidationError,
    FileDescriptor,
    FileTemplates,
    FileTemplateValidationError,
    Formatter,
    FormatterFactory,
    Location,
    LocationFactory,
    StorageClass,
    StoredFileInfo,
)

from lsst.daf.butler.core.repoRelocation import replaceRoot
from lsst.daf.butler.core.utils import getInstanceOf
from .genericDatastore import GenericBaseDatastore

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatastoreRecord(DatabaseDictRecordBase):
    """Describes the contents of a datastore record of a dataset in the
    registry.

    The record is usually populated by a `StoredFileInfo` object.
    """
    __slots__ = {"path", "formatter", "storage_class", "file_size", "checksum"}
    path: str
    formatter: str
    storage_class: str
    file_size: int
    checksum: str

    lengths = {"path": 256, "formatter": 128, "storage_class": 64, "checksum": 128}
    """Lengths of string fields."""


@dataclass(frozen=True)
class DatastoreFileGetInformation:
    """Collection of useful parameters needed to retrieve a file from
    a Datastore.
    """

    location: Location
    """The location from which to read the dataset."""

    formatter: Formatter
    """The `Formatter` to use to deserialize the dataset."""

    info: StoredFileInfo
    """Stored information about this file and its formatter."""

    assemblerParams: dict
    """Parameters to use for post-processing the retrieved dataset."""

    component: Optional[str]
    """The component to be retrieved (can be `None`)."""

    readStorageClass: StorageClass
    """The `StorageClass` of the dataset being read."""


class FileLikeDatastore(GenericBaseDatastore):
    """Generic Datastore for file-based implementations.

    Should always be sub-classed since key abstract methods are missing.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration as either a `Config` object or URI to file.

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

    Record: ClassVar[Type] = DatastoreRecord
    """Class to use to represent datastore records."""

    root: str
    """Root directory or URI of this `Datastore`."""

    locationFactory: LocationFactory
    """Factory for creating locations relative to the datastore root."""

    formatterFactory: FormatterFactory
    """Factory for creating instances of formatters."""

    templates: FileTemplates
    """File templates that can be used by this `Datastore`."""

    records: DatabaseDict
    """Place to store internal records about datasets."""

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

        # Now associate formatters with storage classes
        self.formatterFactory.registerFormatters(self.config["formatters"],
                                                 universe=self.registry.dimensions)

        # Read the file naming templates
        self.templates = FileTemplates(self.config["templates"],
                                       universe=self.registry.dimensions)

        # Storage of paths and formatters, keyed by dataset_id
        self.records = DatabaseDict.fromConfig(self.config["records"],
                                               value=self.Record, key="dataset_id",
                                               registry=registry)

    def __str__(self):
        return self.root

    def _info_to_record(self, info):
        """Convert a `StoredFileInfo` to a suitable database record.

        Parameters
        ----------
        info : `StoredFileInfo`
            Metadata associated with the stored Dataset.

        Returns
        -------
        record : `DatastoreRecord`
            Record to be stored.
        """
        return self.Record(formatter=info.formatter, path=info.path,
                           storage_class=info.storageClass.name,
                           checksum=info.checksum, file_size=info.file_size)

    def _record_to_info(self, record):
        """Convert a record associated with this dataset to a `StoredItemInfo`

        Parameters
        ----------
        record : `DatastoreRecord`
            Object stored in the record table.

        Returns
        -------
        info : `StoredFileInfo`
            The information associated with this dataset record as a Python
            class.
        """
        # Convert name of StorageClass to instance
        storageClass = self.storageClassFactory.getStorageClass(record.storage_class)
        return StoredFileInfo(formatter=record.formatter,
                              path=record.path,
                              storageClass=storageClass,
                              checksum=record.checksum,
                              file_size=record.file_size)

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
            storedFileInfo = self.getStoredItemInfo(ref)
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
        getInfo : `DatastoreFileGetInformation`
            Parameters needed to retrieve the file.
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

        return DatastoreFileGetInformation(location, formatter, storedFileInfo,
                                           assemblerParams, component, readStorageClass)

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
        self._validate_put_parameters(inMemoryDataset, ref)

        # Work out output file name
        try:
            template = self.templates.getTemplate(ref)
        except KeyError as e:
            raise DatasetTypeNotSupportedError(f"Unable to find template for {ref}") from e

        location = self.locationFactory.fromPath(template.format(ref))

        # Get the formatter based on the storage class
        storageClass = ref.datasetType.storageClass
        try:
            formatter = self.formatterFactory.getFormatter(ref,
                                                           FileDescriptor(location,
                                                                          storageClass=storageClass))
        except KeyError as e:
            raise DatasetTypeNotSupportedError(f"Unable to find formatter for {ref}") from e

        return location, formatter

    def _register_dataset_file(self, ref, formatter, path, size, checksum=None):
        """Update registry to indicate that this dataset has been stored,
        specifying file metadata.

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
        # Associate this dataset with the formatter for later read.
        fileInfo = StoredFileInfo(formatter, path, ref.datasetType.storageClass,
                                  file_size=size, checksum=checksum)
        self._register_dataset(ref, fileInfo)

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
            storedFileInfo = self.getStoredItemInfo(ref)

            # Use the path to determine the location
            location = self.locationFactory.fromPath(storedFileInfo.path)

        return location.uri

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
