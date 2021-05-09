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

"""Generic file-based datastore code."""

__all__ = ("FileDatastore", )

import hashlib
import logging
import os
import tempfile

from sqlalchemy import BigInteger, String

from collections import defaultdict
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from lsst.daf.butler import (
    ButlerURI,
    CompositesMap,
    Config,
    FileDataset,
    DatasetId,
    DatasetRef,
    DatasetType,
    DatasetTypeNotSupportedError,
    Datastore,
    DatastoreCacheManager,
    DatastoreDisabledCacheManager,
    DatastoreConfig,
    DatastoreValidationError,
    FileDescriptor,
    FileTemplates,
    FileTemplateValidationError,
    Formatter,
    FormatterFactory,
    Location,
    LocationFactory,
    Progress,
    StorageClass,
    StoredFileInfo,
)

from lsst.daf.butler import ddl
from lsst.daf.butler.registry.interfaces import (
    ReadOnlyDatabaseError,
    DatastoreRegistryBridge,
)

from lsst.daf.butler.core.repoRelocation import replaceRoot
from lsst.daf.butler.core.utils import getInstanceOf, getClassOf, transactional
from .genericDatastore import GenericBaseDatastore

if TYPE_CHECKING:
    from lsst.daf.butler import LookupKey, AbstractDatastoreCacheManager
    from lsst.daf.butler.registry.interfaces import DatasetIdRef, DatastoreRegistryBridgeManager

log = logging.getLogger(__name__)


class _IngestPrepData(Datastore.IngestPrepData):
    """Helper class for FileDatastore ingest implementation.

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

    assemblerParams: Dict[str, Any]
    """Parameters to use for post-processing the retrieved dataset."""

    formatterParams: Dict[str, Any]
    """Parameters that were understood by the associated formatter."""

    component: Optional[str]
    """The component to be retrieved (can be `None`)."""

    readStorageClass: StorageClass
    """The `StorageClass` of the dataset being read."""


class FileDatastore(GenericBaseDatastore):
    """Generic Datastore for file-based implementations.

    Should always be sub-classed since key abstract methods are missing.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration as either a `Config` object or URI to file.
    bridgeManager : `DatastoreRegistryBridgeManager`
        Object that manages the interface between `Registry` and datastores.
    butlerRoot : `str`, optional
        New datastore root to use to override the configuration value.

    Raises
    ------
    ValueError
        If root location does not exist and ``create`` is `False` in the
        configuration.
    """

    defaultConfigFile: ClassVar[Optional[str]] = None
    """Path to configuration defaults. Accessed within the ``config`` resource
    or relative to a search path. Can be None if no defaults specified.
    """

    root: ButlerURI
    """Root directory URI of this `Datastore`."""

    locationFactory: LocationFactory
    """Factory for creating locations relative to the datastore root."""

    formatterFactory: FormatterFactory
    """Factory for creating instances of formatters."""

    templates: FileTemplates
    """File templates that can be used by this `Datastore`."""

    composites: CompositesMap
    """Determines whether a dataset should be disassembled on put."""

    defaultConfigFile = "datastores/fileDatastore.yaml"
    """Path to configuration defaults. Accessed within the ``config`` resource
    or relative to a search path. Can be None if no defaults specified.
    """

    @classmethod
    def setConfigRoot(cls, root: str, config: Config, full: Config, overwrite: bool = True) -> None:
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
    def makeTableSpec(cls, datasetIdColumnType: type) -> ddl.TableSpec:
        return ddl.TableSpec(
            fields=[
                ddl.FieldSpec(name="dataset_id", dtype=datasetIdColumnType, primaryKey=True),
                ddl.FieldSpec(name="path", dtype=String, length=256, nullable=False),
                ddl.FieldSpec(name="formatter", dtype=String, length=128, nullable=False),
                ddl.FieldSpec(name="storage_class", dtype=String, length=64, nullable=False),
                # Use empty string to indicate no component
                ddl.FieldSpec(name="component", dtype=String, length=32, primaryKey=True),
                # TODO: should checksum be Base64Bytes instead?
                ddl.FieldSpec(name="checksum", dtype=String, length=128, nullable=True),
                ddl.FieldSpec(name="file_size", dtype=BigInteger, nullable=True),
            ],
            unique=frozenset(),
            indexes=[tuple(["path"])],
        )

    def __init__(self, config: Union[DatastoreConfig, str],
                 bridgeManager: DatastoreRegistryBridgeManager, butlerRoot: str = None):
        super().__init__(config, bridgeManager)
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
        self.root = ButlerURI(replaceRoot(self.config["root"], butlerRoot),
                              forceDirectory=True, forceAbsolute=True)

        self.locationFactory = LocationFactory(self.root)
        self.formatterFactory = FormatterFactory()

        # Now associate formatters with storage classes
        self.formatterFactory.registerFormatters(self.config["formatters"],
                                                 universe=bridgeManager.universe)

        # Read the file naming templates
        self.templates = FileTemplates(self.config["templates"],
                                       universe=bridgeManager.universe)

        # See if composites should be disassembled
        self.composites = CompositesMap(self.config["composites"],
                                        universe=bridgeManager.universe)

        tableName = self.config["records", "table"]
        try:
            # Storage of paths and formatters, keyed by dataset_id
            self._table = bridgeManager.opaque.register(
                tableName, self.makeTableSpec(bridgeManager.datasetIdColumnType))
            # Interface to Registry.
            self._bridge = bridgeManager.register(self.name)
        except ReadOnlyDatabaseError:
            # If the database is read only and we just tried and failed to
            # create a table, it means someone is trying to create a read-only
            # butler client for an empty repo.  That should be okay, as long
            # as they then try to get any datasets before some other client
            # creates the table.  Chances are they'rejust validating
            # configuration.
            pass

        # Determine whether checksums should be used - default to False
        self.useChecksum = self.config.get("checksum", False)

        # Determine whether we can fall back to configuration if a
        # requested dataset is not known to registry
        self.trustGetRequest = self.config.get("trust_get_request", False)

        # Create a cache manager
        self.cacheManager: AbstractDatastoreCacheManager
        if "cached" in self.config:
            self.cacheManager = DatastoreCacheManager(self.config["cached"],
                                                      universe=bridgeManager.universe)
        else:
            self.cacheManager = DatastoreDisabledCacheManager("",
                                                              universe=bridgeManager.universe)

        # Check existence and create directory structure if necessary
        if not self.root.exists():
            if "create" not in self.config or not self.config["create"]:
                raise ValueError(f"No valid root and not allowed to create one at: {self.root}")
            try:
                self.root.mkdir()
            except Exception as e:
                raise ValueError(f"Can not create datastore root '{self.root}', check permissions."
                                 f" Got error: {e}") from e

    def __str__(self) -> str:
        return str(self.root)

    @property
    def bridge(self) -> DatastoreRegistryBridge:
        return self._bridge

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
        log.debug("Checking if resource exists: %s", location.uri)
        return location.uri.exists()

    def _delete_artifact(self, location: Location) -> None:
        """Delete the artifact from the datastore.

        Parameters
        ----------
        location : `Location`
            Location of the artifact associated with this datastore.
        """
        if location.pathInStore.isabs():
            raise RuntimeError(f"Cannot delete artifact with absolute uri {location.uri}.")

        try:
            location.uri.remove()
        except Exception:
            log.critical("Failed to delete file: %s", location.uri)
            raise
        log.debug("Successfully deleted file: %s", location.uri)

    def addStoredItemInfo(self, refs: Iterable[DatasetRef], infos: Iterable[StoredFileInfo]) -> None:
        # Docstring inherited from GenericBaseDatastore
        records = [info.to_record(ref) for ref, info in zip(refs, infos)]
        self._table.insert(*records)

    def getStoredItemsInfo(self, ref: DatasetIdRef) -> List[StoredFileInfo]:
        # Docstring inherited from GenericBaseDatastore

        # Look for the dataset_id -- there might be multiple matches
        # if we have disassembled the dataset.
        records = list(self._table.fetch(dataset_id=ref.id))
        return [StoredFileInfo.from_record(record) for record in records]

    def _refs_associated_with_artifacts(self, paths: List[Union[str, ButlerURI]]) -> Dict[str,
                                                                                          Set[DatasetId]]:
        """Return paths and associated dataset refs.

        Parameters
        ----------
        paths : `list` of `str` or `ButlerURI`
            All the paths to include in search.

        Returns
        -------
        mapping : `dict` of [`str`, `set` [`DatasetId`]]
            Mapping of each path to a set of associated database IDs.
        """
        records = list(self._table.fetch(path=[str(path) for path in paths]))
        result = defaultdict(set)
        for row in records:
            result[row["path"]].add(row["dataset_id"])
        return result

    def _registered_refs_per_artifact(self, pathInStore: ButlerURI) -> Set[DatasetId]:
        """Return all dataset refs associated with the supplied path.

        Parameters
        ----------
        pathInStore : `ButlerURI`
            Path of interest in the data store.

        Returns
        -------
        ids : `set` of `int`
            All `DatasetRef` IDs associated with this path.
        """
        records = list(self._table.fetch(path=str(pathInStore)))
        ids = {r["dataset_id"] for r in records}
        return ids

    def removeStoredItemInfo(self, ref: DatasetIdRef) -> None:
        # Docstring inherited from GenericBaseDatastore
        self._table.delete(["dataset_id"], {"dataset_id": ref.id})

    def _get_dataset_locations_info(self, ref: DatasetIdRef) -> List[Tuple[Location, StoredFileInfo]]:
        r"""Find all the `Location`\ s  of the requested dataset in the
        `Datastore` and the associated stored file information.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required `Dataset`.

        Returns
        -------
        results : `list` [`tuple` [`Location`, `StoredFileInfo` ]]
            Location of the dataset within the datastore and
            stored information about each file and its formatter.
        """
        # Get the file information (this will fail if no file)
        records = self.getStoredItemsInfo(ref)

        # Use the path to determine the location -- we need to take
        # into account absolute URIs in the datastore record
        return [(r.file_location(self.locationFactory), r) for r in records]

    def _can_remove_dataset_artifact(self, ref: DatasetIdRef, location: Location) -> bool:
        """Check that there is only one dataset associated with the
        specified artifact.

        Parameters
        ----------
        ref : `DatasetRef` or `FakeDatasetRef`
            Dataset to be removed.
        location : `Location`
            The location of the artifact to be removed.

        Returns
        -------
        can_remove : `Bool`
            True if the artifact can be safely removed.
        """
        # Can't ever delete absolute URIs.
        if location.pathInStore.isabs():
            return False

        # Get all entries associated with this path
        allRefs = self._registered_refs_per_artifact(location.pathInStore)
        if not allRefs:
            raise RuntimeError(f"Datastore inconsistency error. {location.pathInStore} not in registry")

        # Remove these refs from all the refs and if there is nothing left
        # then we can delete
        remainingRefs = allRefs - {ref.id}

        if remainingRefs:
            return False
        return True

    def _get_expected_dataset_locations_info(self, ref: DatasetRef) -> List[Tuple[Location,
                                                                                  StoredFileInfo]]:
        """Predict the location and related file information of the requested
        dataset in this datastore.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required `Dataset`.

        Returns
        -------
        results : `list` [`tuple` [`Location`, `StoredFileInfo` ]]
            Expected Location of the dataset within the datastore and
            placeholder information about each file and its formatter.

        Notes
        -----
        Uses the current configuration to determine how we would expect the
        datastore files to have been written if we couldn't ask registry.
        This is safe so long as there has been no change to datastore
        configuration between writing the dataset and wanting to read it.
        Will not work for files that have been ingested without using the
        standard file template or default formatter.
        """

        # If we have a component ref we always need to ask the questions
        # of the composite.  If the composite is disassembled this routine
        # should return all components.  If the composite was not
        # disassembled the composite is what is stored regardless of
        # component request. Note that if the caller has disassembled
        # a composite there is no way for this guess to know that
        # without trying both the composite and component ref and seeing
        # if there is something at the component Location even without
        # disassembly being enabled.
        if ref.datasetType.isComponent():
            ref = ref.makeCompositeRef()

        # See if the ref is a composite that should be disassembled
        doDisassembly = self.composites.shouldBeDisassembled(ref)

        all_info: List[Tuple[Location, Formatter, StorageClass, Optional[str]]] = []

        if doDisassembly:
            for component, componentStorage in ref.datasetType.storageClass.components.items():
                compRef = ref.makeComponentRef(component)
                location, formatter = self._determine_put_formatter_location(compRef)
                all_info.append((location, formatter, componentStorage, component))

        else:
            # Always use the composite ref if no disassembly
            location, formatter = self._determine_put_formatter_location(ref)
            all_info.append((location, formatter, ref.datasetType.storageClass, None))

        # Convert the list of tuples to have StoredFileInfo as second element
        return [(location, StoredFileInfo(formatter=formatter,
                                          path=location.pathInStore.path,
                                          storageClass=storageClass,
                                          component=component,
                                          checksum=None,
                                          file_size=-1))
                for location, formatter, storageClass, component in all_info]

    def _prepare_for_get(self, ref: DatasetRef,
                         parameters: Optional[Mapping[str, Any]] = None) -> List[DatastoreFileGetInformation]:
        """Check parameters for ``get`` and obtain formatter and
        location.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
        parameters : `dict`
            `StorageClass`-specific parameters that specify, for example,
            a slice of the dataset to be loaded.

        Returns
        -------
        getInfo : `list` [`DatastoreFileGetInformation`]
            Parameters needed to retrieve each file.
        """
        log.debug("Retrieve %s from %s with parameters %s", ref, self.name, parameters)

        # Get file metadata and internal metadata
        fileLocations = self._get_dataset_locations_info(ref)
        if not fileLocations:
            if not self.trustGetRequest:
                raise FileNotFoundError(f"Could not retrieve dataset {ref}.")
            # Assume the dataset is where we think it should be
            fileLocations = self._get_expected_dataset_locations_info(ref)

        # The storage class we want to use eventually
        refStorageClass = ref.datasetType.storageClass

        if len(fileLocations) > 1:
            disassembled = True
        else:
            disassembled = False

        # Is this a component request?
        refComponent = ref.datasetType.component()

        fileGetInfo = []
        for location, storedFileInfo in fileLocations:

            # The storage class used to write the file
            writeStorageClass = storedFileInfo.storageClass

            # If this has been disassembled we need read to match the write
            if disassembled:
                readStorageClass = writeStorageClass
            else:
                readStorageClass = refStorageClass

            formatter = getInstanceOf(storedFileInfo.formatter,
                                      FileDescriptor(location, readStorageClass=readStorageClass,
                                                     storageClass=writeStorageClass, parameters=parameters),
                                      ref.dataId)

            formatterParams, notFormatterParams = formatter.segregateParameters()

            # Of the remaining parameters, extract the ones supported by
            # this StorageClass (for components not all will be handled)
            assemblerParams = readStorageClass.filterParameters(notFormatterParams)

            # The ref itself could be a component if the dataset was
            # disassembled by butler, or we disassembled in datastore and
            # components came from the datastore records
            component = storedFileInfo.component if storedFileInfo.component else refComponent

            fileGetInfo.append(DatastoreFileGetInformation(location, formatter, storedFileInfo,
                                                           assemblerParams, formatterParams,
                                                           component, readStorageClass))

        return fileGetInfo

    def _prepare_for_put(self, inMemoryDataset: Any, ref: DatasetRef) -> Tuple[Location, Formatter]:
        """Check the arguments for ``put`` and obtain formatter and
        location.

        Parameters
        ----------
        inMemoryDataset : `object`
            The dataset to store.
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
        return self._determine_put_formatter_location(ref)

    def _determine_put_formatter_location(self, ref: DatasetRef) -> Tuple[Location, Formatter]:
        """Calculate the formatter and output location to use for put.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the associated Dataset.

        Returns
        -------
        location : `Location`
            The location to write the dataset.
        formatter : `Formatter`
            The `Formatter` to use to write the dataset.
        """
        # Work out output file name
        try:
            template = self.templates.getTemplate(ref)
        except KeyError as e:
            raise DatasetTypeNotSupportedError(f"Unable to find template for {ref}") from e

        # Validate the template to protect against filenames from different
        # dataIds returning the same and causing overwrite confusion.
        template.validateTemplate(ref)

        location = self.locationFactory.fromPath(template.format(ref))

        # Get the formatter based on the storage class
        storageClass = ref.datasetType.storageClass
        try:
            formatter = self.formatterFactory.getFormatter(ref,
                                                           FileDescriptor(location,
                                                                          storageClass=storageClass),
                                                           ref.dataId)
        except KeyError as e:
            raise DatasetTypeNotSupportedError(f"Unable to find formatter for {ref} in datastore "
                                               f"{self.name}") from e

        # Now that we know the formatter, update the location
        location = formatter.makeUpdatedLocation(location)

        return location, formatter

    def _overrideTransferMode(self, *datasets: FileDataset, transfer: Optional[str] = None) -> Optional[str]:
        # Docstring inherited from base class
        if transfer != "auto":
            return transfer

        # See if the paths are within the datastore or not
        inside = [self._pathInStore(d.path) is not None for d in datasets]

        if all(inside):
            transfer = None
        elif not any(inside):
            # Allow ButlerURI to use its own knowledge
            transfer = "auto"
        else:
            raise ValueError("Some datasets are inside the datastore and some are outside."
                             " Please use an explicit transfer mode and not 'auto'.")

        return transfer

    def _pathInStore(self, path: Union[str, ButlerURI]) -> Optional[str]:
        """Return path relative to datastore root

        Parameters
        ----------
        path : `str` or `ButlerURI`
            Path to dataset. Can be absolute URI. If relative assumed to
            be relative to the datastore. Returns path in datastore
            or raises an exception if the path it outside.

        Returns
        -------
        inStore : `str`
            Path relative to datastore root. Returns `None` if the file is
            outside the root.
        """
        # Relative path will always be relative to datastore
        pathUri = ButlerURI(path, forceAbsolute=False)
        return pathUri.relative_to(self.root)

    def _standardizeIngestPath(self, path: Union[str, ButlerURI], *,
                               transfer: Optional[str] = None) -> Union[str, ButlerURI]:
        """Standardize the path of a to-be-ingested file.

        Parameters
        ----------
        path : `str` or `ButlerURI`
            Path of a file to be ingested.
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            See `ingest` for details of transfer modes.
            This implementation is provided only so
            `NotImplementedError` can be raised if the mode is not supported;
            actual transfers are deferred to `_extractIngestInfo`.

        Returns
        -------
        path : `str` or `ButlerURI`
            New path in what the datastore considers standard form. If an
            absolute URI was given that will be returned unchanged.

        Notes
        -----
        Subclasses of `FileDatastore` can implement this method instead
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
        if transfer not in (None, "direct") + self.root.transferModes:
            raise NotImplementedError(f"Transfer mode {transfer} not supported.")

        # A relative URI indicates relative to datastore root
        srcUri = ButlerURI(path, forceAbsolute=False)
        if not srcUri.isabs():
            srcUri = self.root.join(path)

        if not srcUri.exists():
            raise FileNotFoundError(f"Resource at {srcUri} does not exist; note that paths to ingest "
                                    f"are assumed to be relative to {self.root} unless they are absolute.")

        if transfer is None:
            relpath = srcUri.relative_to(self.root)
            if not relpath:
                raise RuntimeError(f"Transfer is none but source file ({srcUri}) is not "
                                   f"within datastore ({self.root})")

            # Return the relative path within the datastore for internal
            # transfer
            path = relpath

        return path

    def _extractIngestInfo(self, path: Union[str, ButlerURI], ref: DatasetRef, *,
                           formatter: Union[Formatter, Type[Formatter]],
                           transfer: Optional[str] = None) -> StoredFileInfo:
        """Relocate (if necessary) and extract `StoredFileInfo` from a
        to-be-ingested file.

        Parameters
        ----------
        path : `str` or `ButlerURI`
            URI or path of a file to be ingested.
        ref : `DatasetRef`
            Reference for the dataset being ingested.  Guaranteed to have
            ``dataset_id not None`.
        formatter : `type` or `Formatter`
            `Formatter` subclass to use for this dataset or an instance.
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            See `ingest` for details of transfer modes.

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
        if self._transaction is None:
            raise RuntimeError("Ingest called without transaction enabled")

        # Create URI of the source path, do not need to force a relative
        # path to absolute.
        srcUri = ButlerURI(path, forceAbsolute=False)

        # Track whether we have read the size of the source yet
        have_sized = False

        tgtLocation: Optional[Location]
        if transfer is None:
            # A relative path is assumed to be relative to the datastore
            # in this context
            if not srcUri.isabs():
                tgtLocation = self.locationFactory.fromPath(srcUri.ospath)
            else:
                # Work out the path in the datastore from an absolute URI
                # This is required to be within the datastore.
                pathInStore = srcUri.relative_to(self.root)
                if pathInStore is None:
                    raise RuntimeError(f"Unexpectedly learned that {srcUri} is "
                                       f"not within datastore {self.root}")
                tgtLocation = self.locationFactory.fromPath(pathInStore)
        elif transfer == "direct":
            # Want to store the full URI to the resource directly in
            # datastore. This is useful for referring to permanent archive
            # storage for raw data.
            # Trust that people know what they are doing.
            tgtLocation = None
        else:
            # Work out the name we want this ingested file to have
            # inside the datastore
            tgtLocation = self._calculate_ingested_datastore_name(srcUri, ref, formatter)
            if not tgtLocation.uri.dirname().exists():
                log.debug("Folder %s does not exist yet.", tgtLocation.uri.dirname())
                tgtLocation.uri.dirname().mkdir()

            # if we are transferring from a local file to a remote location
            # it may be more efficient to get the size and checksum of the
            # local file rather than the transferred one
            if not srcUri.scheme or srcUri.scheme == "file":
                size = srcUri.size()
                checksum = self.computeChecksum(srcUri) if self.useChecksum else None
                have_sized = True

            # transfer the resource to the destination
            tgtLocation.uri.transfer_from(srcUri, transfer=transfer, transaction=self._transaction)

        if tgtLocation is None:
            # This means we are using direct mode
            targetUri = srcUri
            targetPath = str(srcUri)
        else:
            targetUri = tgtLocation.uri
            targetPath = tgtLocation.pathInStore.path

        # the file should exist in the datastore now
        if not have_sized:
            size = targetUri.size()
            checksum = self.computeChecksum(targetUri) if self.useChecksum else None

        return StoredFileInfo(formatter=formatter, path=targetPath,
                              storageClass=ref.datasetType.storageClass,
                              component=ref.datasetType.component(),
                              file_size=size, checksum=checksum)

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
                assert isinstance(dataset.formatter, (type, str))
                dataset.formatter = getClassOf(dataset.formatter)
            dataset.path = self._standardizeIngestPath(dataset.path, transfer=transfer)
            filtered.append(dataset)
        return _IngestPrepData(filtered)

    @transactional
    def _finishIngest(self, prepData: Datastore.IngestPrepData, *, transfer: Optional[str] = None) -> None:
        # Docstring inherited from Datastore._finishIngest.
        refsAndInfos = []
        progress = Progress("lsst.daf.butler.datastores.FileDatastore.ingest", level=logging.DEBUG)
        for dataset in progress.wrap(prepData.datasets, desc="Ingesting dataset files"):
            # Do ingest as if the first dataset ref is associated with the file
            info = self._extractIngestInfo(dataset.path, dataset.refs[0], formatter=dataset.formatter,
                                           transfer=transfer)
            refsAndInfos.extend([(ref, info) for ref in dataset.refs])
        self._register_datasets(refsAndInfos)

    def _calculate_ingested_datastore_name(self, srcUri: ButlerURI, ref: DatasetRef,
                                           formatter: Union[Formatter, Type[Formatter]]) -> Location:
        """Given a source URI and a DatasetRef, determine the name the
        dataset will have inside datastore.

        Parameters
        ----------
        srcUri : `ButlerURI`
            URI to the source dataset file.
        ref : `DatasetRef`
            Ref associated with the newly-ingested dataset artifact.  This
            is used to determine the name within the datastore.
        formatter : `Formatter` or Formatter class.
            Formatter to use for validation. Can be a class or an instance.

        Returns
        -------
        location : `Location`
            Target location for the newly-ingested dataset.
        """
        # Ingesting a file from outside the datastore.
        # This involves a new name.
        template = self.templates.getTemplate(ref)
        location = self.locationFactory.fromPath(template.format(ref))

        # Get the extension
        ext = srcUri.getExtension()

        # Update the destination to include that extension
        location.updateExtension(ext)

        # Ask the formatter to validate this extension
        formatter.validateExtension(location)

        return location

    def _write_in_memory_to_artifact(self, inMemoryDataset: Any, ref: DatasetRef) -> StoredFileInfo:
        """Write out in memory dataset to datastore.

        Parameters
        ----------
        inMemoryDataset : `object`
            Dataset to write to datastore.
        ref : `DatasetRef`
            Registry information associated with this dataset.

        Returns
        -------
        info : `StoredFileInfo`
            Information describin the artifact written to the datastore.
        """
        location, formatter = self._prepare_for_put(inMemoryDataset, ref)
        uri = location.uri

        if not uri.dirname().exists():
            log.debug("Folder %s does not exist yet so creating it.", uri.dirname())
            uri.dirname().mkdir()

        if self._transaction is None:
            raise RuntimeError("Attempting to write artifact without transaction enabled")

        def _removeFileExists(uri: ButlerURI) -> None:
            """Remove a file and do not complain if it is not there.

            This is important since a formatter might fail before the file
            is written and we should not confuse people by writing spurious
            error messages to the log.
            """
            try:
                uri.remove()
            except FileNotFoundError:
                pass

        # Register a callback to try to delete the uploaded data if
        # something fails below
        self._transaction.registerUndo("artifactWrite", _removeFileExists, uri)

        # For a local file, simply use the formatter directly
        if uri.isLocal:
            try:
                formatter.write(inMemoryDataset)
            except Exception as e:
                raise RuntimeError(f"Failed to serialize dataset {ref} of type {type(inMemoryDataset)} "
                                   f"to location {uri}") from e
            log.debug("Successfully wrote python object to local file at %s", uri)
        else:
            # This is a remote URI, so first try bytes and write directly else
            # fallback to a temporary file
            try:
                serializedDataset = formatter.toBytes(inMemoryDataset)
            except NotImplementedError:
                with tempfile.NamedTemporaryFile(suffix=uri.getExtension()) as tmpFile:
                    # Need to configure the formatter to write to a different
                    # location and that needs us to overwrite internals
                    tmpLocation = Location(*os.path.split(tmpFile.name))
                    log.debug("Writing dataset to temporary location at %s", tmpLocation.uri)
                    with formatter._updateLocation(tmpLocation):
                        try:
                            formatter.write(inMemoryDataset)
                        except Exception as e:
                            raise RuntimeError(f"Failed to serialize dataset {ref} of type"
                                               f" {type(inMemoryDataset)} to "
                                               f"temporary location {tmpLocation.uri}") from e
                    uri.transfer_from(tmpLocation.uri, transfer="copy", overwrite=True)

                    # Cache if required
                    self.cacheManager.move_to_cache(tmpLocation.uri, ref)

                log.debug("Successfully wrote dataset to %s via a temporary file.", uri)
            except Exception as e:
                raise RuntimeError(f"Failed to serialize dataset {ref} to bytes.") from e
            else:
                log.debug("Writing bytes directly to %s", uri)
                uri.write(serializedDataset, overwrite=True)
                log.debug("Successfully wrote bytes directly to %s", uri)

        # URI is needed to resolve what ingest case are we dealing with
        return self._extractIngestInfo(uri, ref, formatter=formatter)

    def _read_artifact_into_memory(self, getInfo: DatastoreFileGetInformation,
                                   ref: DatasetRef, isComponent: bool = False) -> Any:
        """Read the artifact from datastore into in memory object.

        Parameters
        ----------
        getInfo : `DatastoreFileGetInformation`
            Information about the artifact within the datastore.
        ref : `DatasetRef`
            The registry information associated with this artifact.
        isComponent : `bool`
            Flag to indicate if a component is being read from this artifact.

        Returns
        -------
        inMemoryDataset : `object`
            The artifact as a python object.
        """
        location = getInfo.location
        uri = location.uri
        log.debug("Accessing data from %s", uri)

        # Cannot recalculate checksum but can compare size as a quick check
        # Do not do this if the size is negative since that indicates
        # we do not know.
        recorded_size = getInfo.info.file_size
        resource_size = uri.size()
        if recorded_size >= 0 and resource_size != recorded_size:
            raise RuntimeError("Integrity failure in Datastore. "
                               f"Size of file {uri} ({resource_size}) "
                               f"does not match size recorded in registry of {recorded_size}")

        # For the general case we have choices for how to proceed.
        # 1. Always use a local file (downloading the remote resource to a
        #    temporary file if needed).
        # 2. Use a threshold size and read into memory and use bytes.
        # Use both for now with an arbitrary hand off size.
        # This allows small datasets to be downloaded from remote object
        # stores without requiring a temporary file.

        formatter = getInfo.formatter
        nbytes_max = 10_000_000  # Arbitrary number that we can tune
        if resource_size <= nbytes_max and formatter.can_read_bytes():
            serializedDataset = uri.read()
            log.debug("Deserializing %s from %d bytes from location %s with formatter %s",
                      f"component {getInfo.component}" if isComponent else "",
                      len(serializedDataset), uri, formatter.name())
            try:
                result = formatter.fromBytes(serializedDataset,
                                             component=getInfo.component if isComponent else None)
            except Exception as e:
                raise ValueError(f"Failure from formatter '{formatter.name()}' for dataset {ref.id}"
                                 f" ({ref.datasetType.name} from {uri}): {e}") from e
        else:
            # Read from file.

            # Have to update the Location associated with the formatter
            # because formatter.read does not allow an override.
            # This could be improved.
            location_updated = False
            msg = ""

            # First check in cache for local version.
            # The cache will only be relevant for remote resources.
            if not uri.isLocal:
                cached_file = self.cacheManager.find_in_cache(ref, uri.getExtension())
                if cached_file is not None:
                    msg = f"(via cache read of remote file {uri})"
                    uri = cached_file
                    location_updated = True

            with uri.as_local() as local_uri:

                # URI was remote and file was downloaded
                if uri != local_uri:
                    cache_msg = ""
                    location_updated = True

                    # Cache the downloaded file if needed.
                    cached_uri = self.cacheManager.move_to_cache(local_uri, ref)
                    if cached_uri is not None:
                        local_uri = cached_uri
                        cache_msg = " and cached"

                    msg = f"(via download to local file{cache_msg})"

                # Calculate the (possibly) new location for the formatter
                # to use.
                newLocation = Location(*local_uri.split()) if location_updated else None

                log.debug("Reading%s from location %s %s with formatter %s",
                          f" component {getInfo.component}" if isComponent else "",
                          uri, msg, formatter.name())
                try:
                    with formatter._updateLocation(newLocation):
                        result = formatter.read(component=getInfo.component if isComponent else None)
                except Exception as e:
                    raise ValueError(f"Failure from formatter '{formatter.name()}' for dataset {ref.id}"
                                     f" ({ref.datasetType.name} from {uri}): {e}") from e

        return self._post_process_get(result, getInfo.readStorageClass, getInfo.assemblerParams,
                                      isComponent=isComponent)

    def exists(self, ref: DatasetRef) -> bool:
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
        fileLocations = self._get_dataset_locations_info(ref)

        # if we are being asked to trust that registry might not be correct
        # we ask for the expected locations and check them explicitly
        if not fileLocations:
            if not self.trustGetRequest:
                return False
            fileLocations = self._get_expected_dataset_locations_info(ref)
        for location, _ in fileLocations:
            if not self._artifact_exists(location):
                return False

        return True

    def getURIs(self, ref: DatasetRef,
                predict: bool = False) -> Tuple[Optional[ButlerURI], Dict[str, ButlerURI]]:
        """Return URIs associated with dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required dataset.
        predict : `bool`, optional
            If the datastore does not know about the dataset, should it
            return a predicted URI or not?

        Returns
        -------
        primary : `ButlerURI`
            The URI to the primary artifact associated with this dataset.
            If the dataset was disassembled within the datastore this
            may be `None`.
        components : `dict`
            URIs to any components associated with the dataset artifact.
            Can be empty if there are no components.
        """

        primary: Optional[ButlerURI] = None
        components: Dict[str, ButlerURI] = {}

        # if this has never been written then we have to guess
        if not self.exists(ref):
            if not predict:
                raise FileNotFoundError("Dataset {} not in this datastore".format(ref))

            doDisassembly = self.composites.shouldBeDisassembled(ref)

            if doDisassembly:

                for component, componentStorage in ref.datasetType.storageClass.components.items():
                    compRef = ref.makeComponentRef(component)
                    compLocation, _ = self._determine_put_formatter_location(compRef)

                    # Add a URI fragment to indicate this is a guess
                    components[component] = ButlerURI(compLocation.uri.geturl() + "#predicted")

            else:

                location, _ = self._determine_put_formatter_location(ref)

                # Add a URI fragment to indicate this is a guess
                primary = ButlerURI(location.uri.geturl() + "#predicted")

            return primary, components

        # If this is a ref that we have written we can get the path.
        # Get file metadata and internal metadata
        fileLocations = self._get_dataset_locations_info(ref)

        guessing = False
        if not fileLocations:
            if not self.trustGetRequest:
                raise RuntimeError(f"Unexpectedly got no artifacts for dataset {ref}")
            fileLocations = self._get_expected_dataset_locations_info(ref)
            guessing = True

        if len(fileLocations) == 1:
            # No disassembly so this is the primary URI
            uri = fileLocations[0][0].uri
            if guessing and not uri.exists():
                raise FileNotFoundError(f"Expected URI ({uri}) does not exist")
            primary = uri

        else:
            for location, storedFileInfo in fileLocations:
                if storedFileInfo.component is None:
                    raise RuntimeError(f"Unexpectedly got no component name for a component at {location}")
                uri = location.uri
                if guessing and not uri.exists():
                    raise FileNotFoundError(f"Expected URI ({uri}) does not exist")
                components[storedFileInfo.component] = uri

        return primary, components

    def getURI(self, ref: DatasetRef, predict: bool = False) -> ButlerURI:
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
            URI pointing to the dataset within the datastore. If the
            dataset does not exist in the datastore, and if ``predict`` is
            `True`, the URI will be a prediction and will include a URI
            fragment "#predicted".
            If the datastore does not have entities that relate well
            to the concept of a URI the returned URI will be
            descriptive. The returned URI is not guaranteed to be obtainable.

        Raises
        ------
        FileNotFoundError
            Raised if a URI has been requested for a dataset that does not
            exist and guessing is not allowed.
        RuntimeError
            Raised if a request is made for a single URI but multiple URIs
            are associated with this dataset.

        Notes
        -----
        When a predicted URI is requested an attempt will be made to form
        a reasonable URI based on file templates and the expected formatter.
        """
        primary, components = self.getURIs(ref, predict)
        if primary is None or components:
            raise RuntimeError(f"Dataset ({ref}) includes distinct URIs for components. "
                               "Use Dataastore.getURIs() instead.")
        return primary

    def retrieveArtifacts(self, refs: Iterable[DatasetRef],
                          destination: ButlerURI, transfer: str = "auto",
                          preserve_path: bool = True,
                          overwrite: bool = False) -> List[ButlerURI]:
        """Retrieve the file artifacts associated with the supplied refs.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets for which file artifacts are to be retrieved.
            A single ref can result in multiple files. The refs must
            be resolved.
        destination : `ButlerURI`
            Location to write the file artifacts.
        transfer : `str`, optional
            Method to use to transfer the artifacts. Must be one of the options
            supported by `ButlerURI.transfer_from()`. "move" is not allowed.
        preserve_path : `bool`, optional
            If `True` the full path of the file artifact within the datastore
            is preserved. If `False` the final file component of the path
            is used.
        overwrite : `bool`, optional
            If `True` allow transfers to overwrite existing files at the
            destination.

        Returns
        -------
        targets : `list` of `ButlerURI`
            URIs of file artifacts in destination location. Order is not
            preserved.
        """
        if not destination.isdir():
            raise ValueError(f"Destination location must refer to a directory. Given {destination}")

        if transfer == "move":
            raise ValueError("Can not move artifacts out of datastore. Use copy instead.")

        # Source -> Destination
        # This also helps filter out duplicate DatasetRef in the request
        # that will map to the same underlying file transfer.
        to_transfer: Dict[ButlerURI, ButlerURI] = {}

        for ref in refs:
            locations = self._get_dataset_locations_info(ref)
            for location, _ in locations:
                source_uri = location.uri
                target_path: Union[str, ButlerURI]
                if preserve_path:
                    target_path = location.pathInStore
                    if target_path.isabs():
                        # This is an absolute path to an external file.
                        # Use the full path.
                        target_path = target_path.relativeToPathRoot
                else:
                    target_path = source_uri.basename()
                target_uri = destination.join(target_path)
                to_transfer[source_uri] = target_uri

        # In theory can now parallelize the transfer
        log.debug("Number of artifacts to transfer to %s: %d",
                  str(destination), len(to_transfer))
        for source_uri, target_uri in to_transfer.items():
            target_uri.transfer_from(source_uri, transfer=transfer, overwrite=overwrite)

        return list(to_transfer.values())

    def get(self, ref: DatasetRef, parameters: Optional[Mapping[str, Any]] = None) -> Any:
        """Load an InMemoryDataset from the store.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
        parameters : `dict`
            `StorageClass`-specific parameters that specify, for example,
            a slice of the dataset to be loaded.

        Returns
        -------
        inMemoryDataset : `object`
            Requested dataset or slice thereof as an InMemoryDataset.

        Raises
        ------
        FileNotFoundError
            Requested dataset can not be retrieved.
        TypeError
            Return value from formatter has unexpected type.
        ValueError
            Formatter failed to process the dataset.
        """
        allGetInfo = self._prepare_for_get(ref, parameters)
        refComponent = ref.datasetType.component()

        # Supplied storage class for the component being read
        refStorageClass = ref.datasetType.storageClass

        # Create mapping from component name to related info
        allComponents = {i.component: i for i in allGetInfo}

        # By definition the dataset is disassembled if we have more
        # than one record for it.
        isDisassembled = len(allGetInfo) > 1

        # Look for the special case where we are disassembled but the
        # component is a derived component that was not written during
        # disassembly. For this scenario we need to check that the
        # component requested is listed as a derived component for the
        # composite storage class
        isDisassembledReadOnlyComponent = False
        if isDisassembled and refComponent:
            # The composite storage class should be accessible through
            # the component dataset type
            compositeStorageClass = ref.datasetType.parentStorageClass

            # In the unlikely scenario where the composite storage
            # class is not known, we can only assume that this is a
            # normal component. If that assumption is wrong then the
            # branch below that reads a persisted component will fail
            # so there is no need to complain here.
            if compositeStorageClass is not None:
                isDisassembledReadOnlyComponent = refComponent in compositeStorageClass.derivedComponents

        if isDisassembled and not refComponent:
            # This was a disassembled dataset spread over multiple files
            # and we need to put them all back together again.
            # Read into memory and then assemble

            # Check that the supplied parameters are suitable for the type read
            refStorageClass.validateParameters(parameters)

            # We want to keep track of all the parameters that were not used
            # by formatters.  We assume that if any of the component formatters
            # use a parameter that we do not need to apply it again in the
            # assembler.
            usedParams = set()

            components: Dict[str, Any] = {}
            for getInfo in allGetInfo:
                # assemblerParams are parameters not understood by the
                # associated formatter.
                usedParams.update(set(getInfo.formatterParams))

                component = getInfo.component

                if component is None:
                    raise RuntimeError(f"Internal error in datastore assembly of {ref}")

                # We do not want the formatter to think it's reading
                # a component though because it is really reading a
                # standalone dataset -- always tell reader it is not a
                # component.
                components[component] = self._read_artifact_into_memory(getInfo, ref, isComponent=False)

            inMemoryDataset = ref.datasetType.storageClass.delegate().assemble(components)

            # Any unused parameters will have to be passed to the assembler
            if parameters:
                unusedParams = {k: v for k, v in parameters.items() if k not in usedParams}
            else:
                unusedParams = {}

            # Process parameters
            return ref.datasetType.storageClass.delegate().handleParameters(inMemoryDataset,
                                                                            parameters=unusedParams)

        elif isDisassembledReadOnlyComponent:

            compositeStorageClass = ref.datasetType.parentStorageClass
            if compositeStorageClass is None:
                raise RuntimeError(f"Unable to retrieve derived component '{refComponent}' since"
                                   "no composite storage class is available.")

            if refComponent is None:
                # Mainly for mypy
                raise RuntimeError(f"Internal error in datastore {self.name}: component can not be None here")

            # Assume that every derived component can be calculated by
            # forwarding the request to a single read/write component.
            # Rather than guessing which rw component is the right one by
            # scanning each for a derived component of the same name,
            # we ask the storage class delegate directly which one is best to
            # use.
            compositeDelegate = compositeStorageClass.delegate()
            forwardedComponent = compositeDelegate.selectResponsibleComponent(refComponent,
                                                                              set(allComponents))

            # Select the relevant component
            rwInfo = allComponents[forwardedComponent]

            # For now assume that read parameters are validated against
            # the real component and not the requested component
            forwardedStorageClass = rwInfo.formatter.fileDescriptor.readStorageClass
            forwardedStorageClass.validateParameters(parameters)

            # Unfortunately the FileDescriptor inside the formatter will have
            # the wrong write storage class so we need to create a new one
            # given the immutability constraint.
            writeStorageClass = rwInfo.info.storageClass

            # We may need to put some thought into parameters for read
            # components but for now forward them on as is
            readFormatter = type(rwInfo.formatter)(FileDescriptor(rwInfo.location,
                                                                  readStorageClass=refStorageClass,
                                                                  storageClass=writeStorageClass,
                                                                  parameters=parameters),
                                                   ref.dataId)

            # The assembler can not receive any parameter requests for a
            # derived component at this time since the assembler will
            # see the storage class of the derived component and those
            # parameters will have to be handled by the formatter on the
            # forwarded storage class.
            assemblerParams: Dict[str, Any] = {}

            # Need to created a new info that specifies the derived
            # component and associated storage class
            readInfo = DatastoreFileGetInformation(rwInfo.location, readFormatter,
                                                   rwInfo.info, assemblerParams, {},
                                                   refComponent, refStorageClass)

            return self._read_artifact_into_memory(readInfo, ref, isComponent=True)

        else:
            # Single file request or component from that composite file
            for lookup in (refComponent, None):
                if lookup in allComponents:
                    getInfo = allComponents[lookup]
                    break
            else:
                raise FileNotFoundError(f"Component {refComponent} not found "
                                        f"for ref {ref} in datastore {self.name}")

            # Do not need the component itself if already disassembled
            if isDisassembled:
                isComponent = False
            else:
                isComponent = getInfo.component is not None

            # For a disassembled component we can validate parametersagainst
            # the component storage class directly
            if isDisassembled:
                refStorageClass.validateParameters(parameters)
            else:
                # For an assembled composite this could be a derived
                # component derived from a real component. The validity
                # of the parameters is not clear. For now validate against
                # the composite storage class
                getInfo.formatter.fileDescriptor.storageClass.validateParameters(parameters)

            return self._read_artifact_into_memory(getInfo, ref, isComponent=isComponent)

    @transactional
    def put(self, inMemoryDataset: Any, ref: DatasetRef) -> None:
        """Write a InMemoryDataset with a given `DatasetRef` to the store.

        Parameters
        ----------
        inMemoryDataset : `object`
            The dataset to store.
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

        doDisassembly = self.composites.shouldBeDisassembled(ref)
        # doDisassembly = True

        artifacts = []
        if doDisassembly:
            components = ref.datasetType.storageClass.delegate().disassemble(inMemoryDataset)
            for component, componentInfo in components.items():
                # Don't recurse because we want to take advantage of
                # bulk insert -- need a new DatasetRef that refers to the
                # same dataset_id but has the component DatasetType
                # DatasetType does not refer to the types of components
                # So we construct one ourselves.
                compRef = ref.makeComponentRef(component)
                storedInfo = self._write_in_memory_to_artifact(componentInfo.component, compRef)
                artifacts.append((compRef, storedInfo))
        else:
            # Write the entire thing out
            storedInfo = self._write_in_memory_to_artifact(inMemoryDataset, ref)
            artifacts.append((ref, storedInfo))

        self._register_datasets(artifacts)

    @transactional
    def trash(self, ref: DatasetRef, ignore_errors: bool = True) -> None:
        """Indicate to the datastore that a dataset can be removed.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
        ignore_errors : `bool`
            If `True` return without error even if something went wrong.
            Problems could occur if another process is simultaneously trying
            to delete.

        Raises
        ------
        FileNotFoundError
            Attempt to remove a dataset that does not exist.
        """
        # Get file metadata and internal metadata
        log.debug("Trashing %s in datastore %s", ref, self.name)

        fileLocations = self._get_dataset_locations_info(ref)

        if not fileLocations:
            err_msg = f"Requested dataset to trash ({ref}) is not known to datastore {self.name}"
            if ignore_errors:
                log.warning(err_msg)
                return
            else:
                raise FileNotFoundError(err_msg)

        for location, storedFileInfo in fileLocations:
            if not self._artifact_exists(location):
                err_msg = f"Dataset is known to datastore {self.name} but " \
                          f"associated artifact ({location.uri}) is missing"
                if ignore_errors:
                    log.warning(err_msg)
                    return
                else:
                    raise FileNotFoundError(err_msg)

        # Mark dataset as trashed
        try:
            self._move_to_trash_in_registry(ref)
        except Exception as e:
            if ignore_errors:
                log.warning(f"Attempted to mark dataset ({ref}) to be trashed in datastore {self.name} "
                            f"but encountered an error: {e}")
                pass
            else:
                raise

    @transactional
    def emptyTrash(self, ignore_errors: bool = True) -> None:
        """Remove all datasets from the trash.

        Parameters
        ----------
        ignore_errors : `bool`
            If `True` return without error even if something went wrong.
            Problems could occur if another process is simultaneously trying
            to delete.
        """
        log.debug("Emptying trash in datastore %s", self.name)

        # Context manager will empty trash iff we finish it without raising.
        # It will also automatically delete the relevant rows from the
        # trash table and the records table.
        with self.bridge.emptyTrash(self._table, record_class=StoredFileInfo,
                                    record_column="path") as trash_data:
            # Removing the artifacts themselves requires that the files are
            # not also associated with refs that are not to be trashed.
            # Therefore need to do a query with the file paths themselves
            # and return all the refs associated with them. Can only delete
            # a file if the refs to be trashed are the only refs associated
            # with the file.
            # This requires multiple copies of the trashed items
            trashed, artifacts_to_keep = trash_data

            if artifacts_to_keep is None:
                # The bridge is not helping us so have to work it out
                # ourselves. This is not going to be as efficient.
                trashed = list(trashed)

                # The instance check is for mypy since up to this point it
                # does not know the type of info.
                path_map = self._refs_associated_with_artifacts([info.path for _, info in trashed
                                                                 if isinstance(info, StoredFileInfo)])

                for ref, info in trashed:
                    path_map[info.path].remove(ref.id)
                    if not path_map[info.path]:
                        del path_map[info.path]

                artifacts_to_keep = set(path_map)

            for ref, info in trashed:

                # Should not happen for this implementation but need
                # to keep mypy happy.
                assert info is not None, f"Internal logic error in emptyTrash with ref {ref}."

                # Mypy needs to know this is not the base class
                assert isinstance(info, StoredFileInfo), f"Unexpectedly got info of class {type(info)}"

                # Check for mypy
                assert ref.id is not None, f"Internal logic error in emptyTrash with ref {ref}/{info}"

                # Only trashed refs still known to datastore will be returned.
                location = info.file_location(self.locationFactory)

                # If the file itself has been deleted there is nothing we
                # can do about it. It is possible that trash has been run
                # in parallel in another process or someone decided to delete
                # the file. It is unlikely to come back and so we should still
                # continue with the removal of the entry from the trash
                # table.
                if not self._artifact_exists(location):
                    log.debug("Dataset at %s with id %s no longer present in datastore %s. Continuing.",
                              location.uri, ref.id, self.name)
                    continue

                if info.path not in artifacts_to_keep:
                    # Point of no return for this artifact
                    log.debug("Removing artifact %s from datastore %s", location.uri, self.name)
                    try:
                        self._delete_artifact(location)
                    except Exception as e:
                        if ignore_errors:
                            # Use a debug message here even though it's not
                            # a good situation. In some cases this can be
                            # caused by a race between user A and user B
                            # and neither of them has permissions for the
                            # other's files. Butler does not know about users
                            # and trash has no idea what collections these
                            # files were in (without guessing from a path).
                            log.debug("Encountered error removing artifact %s from datastore %s: %s",
                                      location.uri, self.name, e)
                        else:
                            raise

    @transactional
    def forget(self, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited.
        refs = list(refs)
        self.bridge.forget(refs)
        self._table.delete(["dataset_id"], *[{"dataset_id": ref.getCheckedId()} for ref in refs])

    def validateConfiguration(self, entities: Iterable[Union[DatasetRef, DatasetType, StorageClass]],
                              logFailures: bool = False) -> None:
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
                    log.critical("Formatter failure: %s", e)

        if templateFailed or formatterFailed:
            messages = []
            if templateFailed:
                messages.append(templateFailed)
            if formatterFailed:
                messages.append(",".join(formatterFailed))
            msg = ";\n".join(messages)
            raise DatastoreValidationError(msg)

    def getLookupKeys(self) -> Set[LookupKey]:
        # Docstring is inherited from base class
        return self.templates.getLookupKeys() | self.formatterFactory.getLookupKeys() | \
            self.constraints.getLookupKeys()

    def validateKey(self, lookupKey: LookupKey,
                    entity: Union[DatasetRef, DatasetType, StorageClass]) -> None:
        # Docstring is inherited from base class
        # The key can be valid in either formatters or templates so we can
        # only check the template if it exists
        if lookupKey in self.templates:
            try:
                self.templates[lookupKey].validateTemplate(entity)
            except FileTemplateValidationError as e:
                raise DatastoreValidationError(e) from e

    def export(self, refs: Iterable[DatasetRef], *,
               directory: Optional[Union[ButlerURI, str]] = None,
               transfer: Optional[str] = "auto") -> Iterable[FileDataset]:
        # Docstring inherited from Datastore.export.
        if transfer is not None and directory is None:
            raise RuntimeError(f"Cannot export using transfer mode {transfer} with no "
                               "export directory given")

        # Force the directory to be a URI object
        directoryUri: Optional[ButlerURI] = None
        if directory is not None:
            directoryUri = ButlerURI(directory, forceDirectory=True)

        if transfer is not None and directoryUri is not None:
            # mypy needs the second test
            if not directoryUri.exists():
                raise FileNotFoundError(f"Export location {directory} does not exist")

        progress = Progress("lsst.daf.butler.datastores.FileDatastore.export", level=logging.DEBUG)
        for ref in progress.wrap(refs, "Exporting dataset files"):
            fileLocations = self._get_dataset_locations_info(ref)
            if not fileLocations:
                raise FileNotFoundError(f"Could not retrieve dataset {ref}.")
            # For now we can not export disassembled datasets
            if len(fileLocations) > 1:
                raise NotImplementedError(f"Can not export disassembled datasets such as {ref}")
            location, storedFileInfo = fileLocations[0]

            pathInStore = location.pathInStore.path
            if transfer is None:
                # TODO: do we also need to return the readStorageClass somehow?
                # We will use the path in store directly
                pass
            elif transfer == "direct":
                # Use full URIs to the remote store in the export
                pathInStore = str(location.uri)
            else:
                # mypy needs help
                assert directoryUri is not None, "directoryUri must be defined to get here"
                storeUri = ButlerURI(location.uri)

                # if the datastore has an absolute URI to a resource, we
                # have two options:
                # 1. Keep the absolute URI in the exported YAML
                # 2. Allocate a new name in the local datastore and transfer
                #    it.
                # For now go with option 2
                if location.pathInStore.isabs():
                    template = self.templates.getTemplate(ref)
                    newURI = ButlerURI(template.format(ref), forceAbsolute=False)
                    pathInStore = str(newURI.updatedExtension(location.pathInStore.getExtension()))

                exportUri = directoryUri.join(pathInStore)
                exportUri.transfer_from(storeUri, transfer=transfer)

            yield FileDataset(refs=[ref], path=pathInStore, formatter=storedFileInfo.formatter)

    @staticmethod
    def computeChecksum(uri: ButlerURI, algorithm: str = "blake2b", block_size: int = 8192) -> Optional[str]:
        """Compute the checksum of the supplied file.

        Parameters
        ----------
        uri : `ButlerURI`
            Name of resource to calculate checksum from.
        algorithm : `str`, optional
            Name of algorithm to use. Must be one of the algorithms supported
            by :py:class`hashlib`.
        block_size : `int`
            Number of bytes to read from file at one time.

        Returns
        -------
        hexdigest : `str`
            Hex digest of the file.

        Notes
        -----
        Currently returns None if the URI is for a remote resource.
        """
        if algorithm not in hashlib.algorithms_guaranteed:
            raise NameError("The specified algorithm '{}' is not supported by hashlib".format(algorithm))

        if not uri.isLocal:
            return None

        hasher = hashlib.new(algorithm)

        with uri.as_local() as local_uri:
            with open(local_uri.ospath, "rb") as f:
                for chunk in iter(lambda: f.read(block_size), b""):
                    hasher.update(chunk)

        return hasher.hexdigest()

    def needs_expanded_data_ids(
        self,
        transfer: Optional[str],
        entity: Optional[Union[DatasetRef, DatasetType, StorageClass]] = None,
    ) -> bool:
        # Docstring inherited.
        # This _could_ also use entity to inspect whether the filename template
        # involves placeholders other than the required dimensions for its
        # dataset type, but that's not necessary for correctness; it just
        # enables more optimizations (perhaps only in theory).
        return transfer not in ("direct", None)
