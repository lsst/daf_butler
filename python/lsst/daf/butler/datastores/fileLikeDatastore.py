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
import itertools
from abc import abstractmethod

from sqlalchemy import Integer, String

from dataclasses import dataclass
from typing import Optional, List, Type

from lsst.daf.butler import (
    Config,
    FileDataset,
    DatasetRef,
    DatasetTypeNotSupportedError,
    Datastore,
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

from lsst.daf.butler import ddl

from lsst.daf.butler.core.repoRelocation import replaceRoot
from lsst.daf.butler.core.utils import getInstanceOf, NamedValueSet, getClassOf, transactional
from .genericDatastore import GenericBaseDatastore

log = logging.getLogger(__name__)


class _IngestPrepData(Datastore.IngestPrepData):
    """Helper class for FileLikeDatastore ingest implementation.

    Parameters
    ----------
    datasets : `list` of `FileDataset`
        Files to be ingested by this datastore.
    """
    def __init__(self, datasets: List[FileDataset]):
        super().__init__(ref for dataset in datasets for ref in dataset.refs)
        self.datasets = datasets


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

    root: str
    """Root directory or URI of this `Datastore`."""

    locationFactory: LocationFactory
    """Factory for creating locations relative to the datastore root."""

    formatterFactory: FormatterFactory
    """Factory for creating instances of formatters."""

    templates: FileTemplates
    """File templates that can be used by this `Datastore`."""

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

    @classmethod
    def makeTableSpec(cls):
        return ddl.TableSpec(
            fields=NamedValueSet([
                ddl.FieldSpec(name="dataset_id", dtype=Integer, primaryKey=True),
                ddl.FieldSpec(name="path", dtype=String, length=256, nullable=False),
                ddl.FieldSpec(name="formatter", dtype=String, length=128, nullable=False),
                ddl.FieldSpec(name="storage_class", dtype=String, length=64, nullable=False),
                # TODO: should checksum be Base64Bytes instead?
                ddl.FieldSpec(name="checksum", dtype=String, length=128, nullable=True),
                ddl.FieldSpec(name="file_size", dtype=Integer, nullable=True),
            ]),
            unique=frozenset(),
            foreignKeys=[ddl.ForeignKeySpec(table="dataset", source=("dataset_id",), target=("dataset_id",),
                                            onDelete="CASCADE")]
        )

    def __init__(self, config, registry, butlerRoot=None):
        super().__init__(config, registry)
        if "root" not in self.config:
            raise ValueError("No root directory specified in configuration")

        # Name ourselves either using an explicit name or a name
        # derived from the (unexpanded) root
        if "name" in self.config:
            self.name = self.config["name"]
        else:
            # We use the unexpanded root in the name to indicate that this
            # datastore can be moved without having to update registry.
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
        self._tableName = self.config["records", "table"]
        registry.registerOpaqueTable(self._tableName, self.makeTableSpec())

    def __str__(self):
        return self.root

    def addStoredItemInfo(self, refs, infos):
        # Docstring inherited from GenericBaseDatastore
        records = []
        for ref, info in zip(refs, infos):
            records.append(
                dict(dataset_id=ref.id, formatter=info.formatter, path=info.path,
                     storage_class=info.storageClass.name,
                     checksum=info.checksum, file_size=info.file_size)
            )
        self.registry.insertOpaqueData(self._tableName, *records)

    def getStoredItemInfo(self, ref):
        # Docstring inherited from GenericBaseDatastore
        records = list(self.registry.fetchOpaqueData(self._tableName, dataset_id=ref.id))
        if len(records) == 0:
            raise KeyError(f"Unable to retrieve location associated with Dataset {ref}.")
        assert len(records) == 1, "Primary key constraint should make more than one result impossible."
        record = records[0]
        # Convert name of StorageClass to instance
        storageClass = self.storageClassFactory.getStorageClass(record["storage_class"])
        return StoredFileInfo(formatter=record["formatter"],
                              path=record["path"],
                              storageClass=storageClass,
                              checksum=record["checksum"],
                              file_size=record["file_size"])

    def _registered_refs_per_artifact(self, pathInStore):
        """Return all dataset refs associated with the supplied path.

        Parameters
        ----------
        pathInStore : `str`
            Path of interest in the data store.

        Returns
        -------
        ids : `set` of `int`
            All `DatasetRef` IDs associated with this path.
        """
        records = list(self.registry.fetchOpaqueData(self._tableName, path=pathInStore))
        ids = {r["dataset_id"] for r in records}
        return ids

    def removeStoredItemInfo(self, ref):
        # Docstring inherited from GenericBaseDatastore
        self.registry.deleteOpaqueData(self._tableName, dataset_id=ref.id)

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

    def _can_remove_dataset_artifact(self, ref):
        """Check that there is only one dataset associated with the
        specified artifact.

        Parameters
        ----------
        ref : `DatasetRef`
            Dataset to be removed.

        Returns
        -------
        can_remove : `Bool`
            True if the artifact can be safely removed.
        """
        storedFileInfo = self.getStoredItemInfo(ref)

        # Get all entries associated with this path
        allRefs = self._registered_refs_per_artifact(storedFileInfo.path)
        if not allRefs:
            raise RuntimeError(f"Datastore inconsistency error. {storedFileInfo.path} not in registry")

        # Get all the refs associated with this dataset if it is a composite
        theseRefs = {r.id for r in itertools.chain([ref], ref.components.values())}

        # Remove these refs from all the refs and if there is nothing left
        # then we can delete
        remainingRefs = allRefs - theseRefs

        if remainingRefs:
            return False
        return True

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
                                                 storageClass=writeStorageClass, parameters=parameters),
                                  ref.dataId)
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
                                                                          storageClass=storageClass),
                                                           ref.dataId)
        except KeyError as e:
            raise DatasetTypeNotSupportedError(f"Unable to find formatter for {ref}") from e

        return location, formatter

    @abstractmethod
    def _standardizeIngestPath(self, path: str, *, transfer: Optional[str] = None) -> str:
        """Standardize the path of a to-be-ingested file.

        Parameters
        ----------
        path : `str`
            Path of a file to be ingested.
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            If `None` (default), the file must already be in a location
            appropriate for the datastore (e.g. within its root directory),
            and will not be moved.  Other choices include "move", "copy",
            "symlink", and "hardlink".  This is provided only so
            `NotImplementedError` can be raised if the mode is not supported;
            actual transfers are deferred to `_extractIngestInfo`.

        Returns
        -------
        path : `str`
            New path in what the datastore considers standard form.

        Notes
        -----
        Subclasses of `FileLikeDatastore` should implement this method instead
        of `_prepIngest`.  It should not modify the data repository or given
        file in any way.

        Raises
        ------
        NotImplementedError
            Raised if the datastore does not support the given transfer mode
            (including the case where ingest is not supported at all).
        FileNotFoundError
            Raised if one of the given files does not exist.
        """
        raise NotImplementedError("Must be implemented by subclasses.")

    @abstractmethod
    def _extractIngestInfo(self, path: str, ref: DatasetRef, *, formatter: Type[Formatter],
                           transfer: Optional[str] = None) -> StoredFileInfo:
        """Relocate (if necessary) and extract `StoredFileInfo` from a
        to-be-ingested file.

        Parameters
        ----------
        path : `str`
            Path of a file to be ingested.
        ref : `DatasetRef`
            Reference for the dataset being ingested.  Guaranteed to have
            ``dataset_id not None`.
        formatter : `type`
            `Formatter` subclass to use for this dataset.
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            If `None` (default), the file must already be in a location
            appropriate for the datastore (e.g. within its root directory),
            and will not be modified.  Other choices include "move", "copy",
            "symlink", and "hardlink".

        Returns
        -------
        info : `StoredFileInfo`
            Internal datastore record for this file.  This will be inserted by
            the caller; the `_extractIngestInfo` is only resposible for
            creating and populating the struct.

        Raises
        ------
        FileNotFoundError
            Raised if one of the given files does not exist.
        FileExistsError
            Raised if transfer is not `None` but the (internal) location the
            file would be moved to is already occupied.
        """
        raise NotImplementedError("Must be implemented by subclasses.")

    def _prepIngest(self, *datasets: FileDataset, transfer: Optional[str] = None) -> _IngestPrepData:
        # Docstring inherited from Datastore._prepIngest.
        filtered = []
        for dataset in datasets:
            acceptable = [ref for ref in dataset.refs if self.constraints.isAcceptable(ref)]
            if not acceptable:
                continue
            else:
                dataset.refs = acceptable
            if dataset.formatter is None:
                dataset.formatter = self.formatterFactory.getFormatterClass(dataset.refs[0])
            else:
                dataset.formatter = getClassOf(dataset.formatter)
            dataset.path = self._standardizeIngestPath(dataset.path, transfer=transfer)
            filtered.append(dataset)
        return _IngestPrepData(filtered)

    @transactional
    def _finishIngest(self, prepData: Datastore.IngestPrepData, *, transfer: Optional[str] = None):
        # Docstring inherited from Datastore._finishIngest.
        refsAndInfos = []
        for dataset in prepData.datasets:
            # Do ingest as if the first dataset ref is associated with the file
            info = self._extractIngestInfo(dataset.path, dataset.refs[0], formatter=dataset.formatter,
                                           transfer=transfer)
            refsAndInfos.extend([(ref, info) for ref in dataset.refs])
        self._register_datasets(refsAndInfos)

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

        Notes
        -----
        When a predicted URI is requested an attempt will be made to form
        a reasonable URI based on file templates and the expected formatter.
        """
        # if this has never been written then we have to guess
        if not self.exists(ref):
            if not predict:
                raise FileNotFoundError("Dataset {} not in this datastore".format(ref))

            template = self.templates.getTemplate(ref)
            location = self.locationFactory.fromPath(template.format(ref))
            storageClass = ref.datasetType.storageClass
            formatter = self.formatterFactory.getFormatter(ref, FileDescriptor(location,
                                                                               storageClass=storageClass))
            # Try to use the extension attribute but ignore problems if the
            # formatter does not define one.
            try:
                location = formatter.makeUpdatedLocation(location)
            except Exception:
                # Use the default extension
                pass

            # Add a URI fragment to indicate this is a guess
            return location.uri + "#predicted"

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
