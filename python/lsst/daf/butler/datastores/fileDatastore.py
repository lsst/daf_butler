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

__all__ = ("FileDatastore",)

import hashlib
import logging
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
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

from lsst.daf.butler import (
    CompositesMap,
    Config,
    DatasetId,
    DatasetRef,
    DatasetRefURIs,
    DatasetType,
    DatasetTypeNotSupportedError,
    Datastore,
    DatastoreCacheManager,
    DatastoreConfig,
    DatastoreDisabledCacheManager,
    DatastoreRecordData,
    DatastoreValidationError,
    FileDataset,
    FileDescriptor,
    FileTemplates,
    FileTemplateValidationError,
    Formatter,
    FormatterFactory,
    Location,
    LocationFactory,
    Progress,
    StorageClass,
    StoredDatastoreItemInfo,
    StoredFileInfo,
    ddl,
)
from lsst.daf.butler.core.repoRelocation import replaceRoot
from lsst.daf.butler.core.utils import transactional
from lsst.daf.butler.registry.interfaces import DatastoreRegistryBridge, ReadOnlyDatabaseError
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import get_class_of, get_instance_of
from lsst.utils.iteration import chunk_iterable

# For VERBOSE logging usage.
from lsst.utils.logging import VERBOSE, getLogger
from lsst.utils.timer import time_this
from sqlalchemy import BigInteger, String

from ..registry.interfaces import FakeDatasetRef
from .genericDatastore import GenericBaseDatastore

if TYPE_CHECKING:
    from lsst.daf.butler import AbstractDatastoreCacheManager, LookupKey
    from lsst.daf.butler.registry.interfaces import DatasetIdRef, DatastoreRegistryBridgeManager

log = getLogger(__name__)


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

    assemblerParams: Mapping[str, Any]
    """Parameters to use for post-processing the retrieved dataset."""

    formatterParams: Mapping[str, Any]
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

    root: ResourcePath
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
        Config.updateParameters(
            DatastoreConfig,
            config,
            full,
            toUpdate={"root": root},
            toCopy=("cls", ("records", "table")),
            overwrite=overwrite,
        )

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
            indexes=[ddl.IndexSpec("path")],
        )

    def __init__(
        self,
        config: Union[DatastoreConfig, str],
        bridgeManager: DatastoreRegistryBridgeManager,
        butlerRoot: str | None = None,
    ):
        super().__init__(config, bridgeManager)
        if "root" not in self.config:
            raise ValueError("No root directory specified in configuration")

        self._bridgeManager = bridgeManager

        # Name ourselves either using an explicit name or a name
        # derived from the (unexpanded) root
        if "name" in self.config:
            self.name = self.config["name"]
        else:
            # We use the unexpanded root in the name to indicate that this
            # datastore can be moved without having to update registry.
            self.name = "{}@{}".format(type(self).__name__, self.config["root"])

        # Support repository relocation in config
        # Existence of self.root is checked in subclass
        self.root = ResourcePath(
            replaceRoot(self.config["root"], butlerRoot), forceDirectory=True, forceAbsolute=True
        )

        self.locationFactory = LocationFactory(self.root)
        self.formatterFactory = FormatterFactory()

        # Now associate formatters with storage classes
        self.formatterFactory.registerFormatters(self.config["formatters"], universe=bridgeManager.universe)

        # Read the file naming templates
        self.templates = FileTemplates(self.config["templates"], universe=bridgeManager.universe)

        # See if composites should be disassembled
        self.composites = CompositesMap(self.config["composites"], universe=bridgeManager.universe)

        tableName = self.config["records", "table"]
        try:
            # Storage of paths and formatters, keyed by dataset_id
            self._table = bridgeManager.opaque.register(
                tableName, self.makeTableSpec(bridgeManager.datasetIdColumnType)
            )
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
            self.cacheManager = DatastoreCacheManager(self.config["cached"], universe=bridgeManager.universe)
        else:
            self.cacheManager = DatastoreDisabledCacheManager("", universe=bridgeManager.universe)

        # Check existence and create directory structure if necessary
        if not self.root.exists():
            if "create" not in self.config or not self.config["create"]:
                raise ValueError(f"No valid root and not allowed to create one at: {self.root}")
            try:
                self.root.mkdir()
            except Exception as e:
                raise ValueError(
                    f"Can not create datastore root '{self.root}', check permissions. Got error: {e}"
                ) from e

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
        except FileNotFoundError:
            log.debug("File %s did not exist and so could not be deleted.", location.uri)
            raise
        except Exception as e:
            log.critical("Failed to delete file: %s (%s)", location.uri, e)
            raise
        log.debug("Successfully deleted file: %s", location.uri)

    def addStoredItemInfo(self, refs: Iterable[DatasetRef], infos: Iterable[StoredFileInfo]) -> None:
        # Docstring inherited from GenericBaseDatastore
        records = [info.rebase(ref).to_record() for ref, info in zip(refs, infos)]
        self._table.insert(*records, transaction=self._transaction)

    def getStoredItemsInfo(self, ref: DatasetIdRef) -> List[StoredFileInfo]:
        # Docstring inherited from GenericBaseDatastore

        # Look for the dataset_id -- there might be multiple matches
        # if we have disassembled the dataset.
        records = self._table.fetch(dataset_id=ref.id)
        return [StoredFileInfo.from_record(record) for record in records]

    def _get_stored_records_associated_with_refs(
        self, refs: Iterable[DatasetIdRef]
    ) -> Dict[DatasetId, List[StoredFileInfo]]:
        """Retrieve all records associated with the provided refs.

        Parameters
        ----------
        refs : iterable of `DatasetIdRef`
            The refs for which records are to be retrieved.

        Returns
        -------
        records : `dict` of [`DatasetId`, `list` of `StoredFileInfo`]
            The matching records indexed by the ref ID.  The number of entries
            in the dict can be smaller than the number of requested refs.
        """
        records = self._table.fetch(dataset_id=[ref.id for ref in refs])

        # Uniqueness is dataset_id + component so can have multiple records
        # per ref.
        records_by_ref = defaultdict(list)
        for record in records:
            records_by_ref[record["dataset_id"]].append(StoredFileInfo.from_record(record))
        return records_by_ref

    def _refs_associated_with_artifacts(
        self, paths: List[Union[str, ResourcePath]]
    ) -> Dict[str, Set[DatasetId]]:
        """Return paths and associated dataset refs.

        Parameters
        ----------
        paths : `list` of `str` or `lsst.resources.ResourcePath`
            All the paths to include in search.

        Returns
        -------
        mapping : `dict` of [`str`, `set` [`DatasetId`]]
            Mapping of each path to a set of associated database IDs.
        """
        records = self._table.fetch(path=[str(path) for path in paths])
        result = defaultdict(set)
        for row in records:
            result[row["path"]].add(row["dataset_id"])
        return result

    def _registered_refs_per_artifact(self, pathInStore: ResourcePath) -> Set[DatasetId]:
        """Return all dataset refs associated with the supplied path.

        Parameters
        ----------
        pathInStore : `lsst.resources.ResourcePath`
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

    def _get_expected_dataset_locations_info(self, ref: DatasetRef) -> List[Tuple[Location, StoredFileInfo]]:
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
        return [
            (
                location,
                StoredFileInfo(
                    formatter=formatter,
                    path=location.pathInStore.path,
                    storageClass=storageClass,
                    component=component,
                    checksum=None,
                    file_size=-1,
                    dataset_id=ref.getCheckedId(),
                ),
            )
            for location, formatter, storageClass, component in all_info
        ]

    def _prepare_for_get(
        self, ref: DatasetRef, parameters: Optional[Mapping[str, Any]] = None
    ) -> List[DatastoreFileGetInformation]:
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

            # If trust is involved it is possible that there will be
            # components listed here that do not exist in the datastore.
            # Explicitly check for file artifact existence and filter out any
            # that are missing.
            if self.trustGetRequest:
                fileLocations = [loc for loc in fileLocations if loc[0].uri.exists()]

                # For now complain only if we have no components at all. One
                # component is probably a problem but we can punt that to the
                # assembler.
                if not fileLocations:
                    raise FileNotFoundError(f"None of the component files for dataset {ref} exist.")

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

            formatter = get_instance_of(
                storedFileInfo.formatter,
                FileDescriptor(
                    location,
                    readStorageClass=readStorageClass,
                    storageClass=writeStorageClass,
                    parameters=parameters,
                ),
                ref.dataId,
            )

            formatterParams, notFormatterParams = formatter.segregateParameters()

            # Of the remaining parameters, extract the ones supported by
            # this StorageClass (for components not all will be handled)
            assemblerParams = readStorageClass.filterParameters(notFormatterParams)

            # The ref itself could be a component if the dataset was
            # disassembled by butler, or we disassembled in datastore and
            # components came from the datastore records
            component = storedFileInfo.component if storedFileInfo.component else refComponent

            fileGetInfo.append(
                DatastoreFileGetInformation(
                    location,
                    formatter,
                    storedFileInfo,
                    assemblerParams,
                    formatterParams,
                    component,
                    readStorageClass,
                )
            )

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
            formatter = self.formatterFactory.getFormatter(
                ref, FileDescriptor(location, storageClass=storageClass), ref.dataId
            )
        except KeyError as e:
            raise DatasetTypeNotSupportedError(
                f"Unable to find formatter for {ref} in datastore {self.name}"
            ) from e

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
            # Allow ResourcePath to use its own knowledge
            transfer = "auto"
        else:
            # This can happen when importing from a datastore that
            # has had some datasets ingested using "direct" mode.
            # Also allow ResourcePath to sort it out but warn about it.
            # This can happen if you are importing from a datastore
            # that had some direct transfer datasets.
            log.warning(
                "Some datasets are inside the datastore and some are outside. Using 'split' "
                "transfer mode. This assumes that the files outside the datastore are "
                "still accessible to the new butler since they will not be copied into "
                "the target datastore."
            )
            transfer = "split"

        return transfer

    def _pathInStore(self, path: ResourcePathExpression) -> Optional[str]:
        """Return path relative to datastore root

        Parameters
        ----------
        path : `lsst.resources.ResourcePathExpression`
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
        pathUri = ResourcePath(path, forceAbsolute=False)
        return pathUri.relative_to(self.root)

    def _standardizeIngestPath(
        self, path: Union[str, ResourcePath], *, transfer: Optional[str] = None
    ) -> Union[str, ResourcePath]:
        """Standardize the path of a to-be-ingested file.

        Parameters
        ----------
        path : `str` or `lsst.resources.ResourcePath`
            Path of a file to be ingested. This parameter is not expected
            to be all the types that can be used to construct a
            `~lsst.resources.ResourcePath`.
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            See `ingest` for details of transfer modes.
            This implementation is provided only so
            `NotImplementedError` can be raised if the mode is not supported;
            actual transfers are deferred to `_extractIngestInfo`.

        Returns
        -------
        path : `str` or `lsst.resources.ResourcePath`
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
        if transfer not in (None, "direct", "split") + self.root.transferModes:
            raise NotImplementedError(f"Transfer mode {transfer} not supported.")

        # A relative URI indicates relative to datastore root
        srcUri = ResourcePath(path, forceAbsolute=False)
        if not srcUri.isabs():
            srcUri = self.root.join(path)

        if not srcUri.exists():
            raise FileNotFoundError(
                f"Resource at {srcUri} does not exist; note that paths to ingest "
                f"are assumed to be relative to {self.root} unless they are absolute."
            )

        if transfer is None:
            relpath = srcUri.relative_to(self.root)
            if not relpath:
                raise RuntimeError(
                    f"Transfer is none but source file ({srcUri}) is not within datastore ({self.root})"
                )

            # Return the relative path within the datastore for internal
            # transfer
            path = relpath

        return path

    def _extractIngestInfo(
        self,
        path: ResourcePathExpression,
        ref: DatasetRef,
        *,
        formatter: Union[Formatter, Type[Formatter]],
        transfer: Optional[str] = None,
        record_validation_info: bool = True,
    ) -> StoredFileInfo:
        """Relocate (if necessary) and extract `StoredFileInfo` from a
        to-be-ingested file.

        Parameters
        ----------
        path : `lsst.resources.ResourcePathExpression`
            URI or path of a file to be ingested.
        ref : `DatasetRef`
            Reference for the dataset being ingested.  Guaranteed to have
            ``dataset_id not None`.
        formatter : `type` or `Formatter`
            `Formatter` subclass to use for this dataset or an instance.
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            See `ingest` for details of transfer modes.
        record_validation_info : `bool`, optional
            If `True`, the default, the datastore can record validation
            information associated with the file. If `False` the datastore
            will not attempt to track any information such as checksums
            or file sizes. This can be useful if such information is tracked
            in an external system or if the file is to be compressed in place.
            It is up to the datastore whether this parameter is relevant.

        Returns
        -------
        info : `StoredFileInfo`
            Internal datastore record for this file.  This will be inserted by
            the caller; the `_extractIngestInfo` is only responsible for
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
        srcUri = ResourcePath(path, forceAbsolute=False)

        # Track whether we have read the size of the source yet
        have_sized = False

        tgtLocation: Optional[Location]
        if transfer is None or transfer == "split":
            # A relative path is assumed to be relative to the datastore
            # in this context
            if not srcUri.isabs():
                tgtLocation = self.locationFactory.fromPath(srcUri.ospath)
            else:
                # Work out the path in the datastore from an absolute URI
                # This is required to be within the datastore.
                pathInStore = srcUri.relative_to(self.root)
                if pathInStore is None and transfer is None:
                    raise RuntimeError(
                        f"Unexpectedly learned that {srcUri} is not within datastore {self.root}"
                    )
                if pathInStore:
                    tgtLocation = self.locationFactory.fromPath(pathInStore)
                elif transfer == "split":
                    # Outside the datastore but treat that as a direct ingest
                    # instead.
                    tgtLocation = None
                else:
                    raise RuntimeError(f"Unexpected transfer mode encountered: {transfer} for URI {srcUri}")
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
            if record_validation_info and srcUri.isLocal:
                size = srcUri.size()
                checksum = self.computeChecksum(srcUri) if self.useChecksum else None
                have_sized = True

            # Transfer the resource to the destination.
            # Allow overwrite of an existing file. This matches the behavior
            # of datastore.put() in that it trusts that registry would not
            # be asking to overwrite unless registry thought that the
            # overwrite was allowed.
            tgtLocation.uri.transfer_from(
                srcUri, transfer=transfer, transaction=self._transaction, overwrite=True
            )

        if tgtLocation is None:
            # This means we are using direct mode
            targetUri = srcUri
            targetPath = str(srcUri)
        else:
            targetUri = tgtLocation.uri
            targetPath = tgtLocation.pathInStore.path

        # the file should exist in the datastore now
        if record_validation_info:
            if not have_sized:
                size = targetUri.size()
                checksum = self.computeChecksum(targetUri) if self.useChecksum else None
        else:
            # Not recording any file information.
            size = -1
            checksum = None

        return StoredFileInfo(
            formatter=formatter,
            path=targetPath,
            storageClass=ref.datasetType.storageClass,
            component=ref.datasetType.component(),
            file_size=size,
            checksum=checksum,
            dataset_id=ref.getCheckedId(),
        )

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
                formatter_class = get_class_of(dataset.formatter)
                if not issubclass(formatter_class, Formatter):
                    raise TypeError(f"Requested formatter {dataset.formatter} is not a Formatter class.")
                dataset.formatter = formatter_class
            dataset.path = self._standardizeIngestPath(dataset.path, transfer=transfer)
            filtered.append(dataset)
        return _IngestPrepData(filtered)

    @transactional
    def _finishIngest(
        self,
        prepData: Datastore.IngestPrepData,
        *,
        transfer: Optional[str] = None,
        record_validation_info: bool = True,
    ) -> None:
        # Docstring inherited from Datastore._finishIngest.
        refsAndInfos = []
        progress = Progress("lsst.daf.butler.datastores.FileDatastore.ingest", level=logging.DEBUG)
        for dataset in progress.wrap(prepData.datasets, desc="Ingesting dataset files"):
            # Do ingest as if the first dataset ref is associated with the file
            info = self._extractIngestInfo(
                dataset.path,
                dataset.refs[0],
                formatter=dataset.formatter,
                transfer=transfer,
                record_validation_info=record_validation_info,
            )
            refsAndInfos.extend([(ref, info) for ref in dataset.refs])
        self._register_datasets(refsAndInfos)

    def _calculate_ingested_datastore_name(
        self, srcUri: ResourcePath, ref: DatasetRef, formatter: Union[Formatter, Type[Formatter]]
    ) -> Location:
        """Given a source URI and a DatasetRef, determine the name the
        dataset will have inside datastore.

        Parameters
        ----------
        srcUri : `lsst.resources.ResourcePath`
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
            Information describing the artifact written to the datastore.
        """
        # May need to coerce the in memory dataset to the correct
        # python type.
        inMemoryDataset = ref.datasetType.storageClass.coerce_type(inMemoryDataset)

        location, formatter = self._prepare_for_put(inMemoryDataset, ref)
        uri = location.uri

        if not uri.dirname().exists():
            log.debug("Folder %s does not exist yet so creating it.", uri.dirname())
            uri.dirname().mkdir()

        if self._transaction is None:
            raise RuntimeError("Attempting to write artifact without transaction enabled")

        def _removeFileExists(uri: ResourcePath) -> None:
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

        data_written = False
        if not uri.isLocal:
            # This is a remote URI. Some datasets can be serialized directly
            # to bytes and sent to the remote datastore without writing a
            # file. If the dataset is intended to be saved to the cache
            # a file is always written and direct write to the remote
            # datastore is bypassed.
            if not self.cacheManager.should_be_cached(ref):
                try:
                    serializedDataset = formatter.toBytes(inMemoryDataset)
                except NotImplementedError:
                    # Fallback to the file writing option.
                    pass
                except Exception as e:
                    raise RuntimeError(
                        f"Failed to serialize dataset {ref} of type {type(inMemoryDataset)} to bytes."
                    ) from e
                else:
                    log.debug("Writing bytes directly to %s", uri)
                    uri.write(serializedDataset, overwrite=True)
                    log.debug("Successfully wrote bytes directly to %s", uri)
                    data_written = True

        if not data_written:
            # Did not write the bytes directly to object store so instead
            # write to temporary file. Always write to a temporary even if
            # using a local file system -- that gives us atomic writes.
            # If a process is killed as the file is being written we do not
            # want it to remain in the correct place but in corrupt state.
            # For local files write to the output directory not temporary dir.
            prefix = uri.dirname() if uri.isLocal else None
            with ResourcePath.temporary_uri(suffix=uri.getExtension(), prefix=prefix) as temporary_uri:
                # Need to configure the formatter to write to a different
                # location and that needs us to overwrite internals
                log.debug("Writing dataset to temporary location at %s", temporary_uri)
                with formatter._updateLocation(Location(None, temporary_uri)):
                    try:
                        formatter.write(inMemoryDataset)
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to serialize dataset {ref} of type"
                            f" {type(inMemoryDataset)} to "
                            f"temporary location {temporary_uri}"
                        ) from e

                # Use move for a local file since that becomes an efficient
                # os.rename. For remote resources we use copy to allow the
                # file to be cached afterwards.
                transfer = "move" if uri.isLocal else "copy"

                uri.transfer_from(temporary_uri, transfer=transfer, overwrite=True)

                if transfer == "copy":
                    # Cache if required
                    self.cacheManager.move_to_cache(temporary_uri, ref)

            log.debug("Successfully wrote dataset to %s via a temporary file.", uri)

        # URI is needed to resolve what ingest case are we dealing with
        return self._extractIngestInfo(uri, ref, formatter=formatter)

    def _read_artifact_into_memory(
        self,
        getInfo: DatastoreFileGetInformation,
        ref: DatasetRef,
        isComponent: bool = False,
        cache_ref: Optional[DatasetRef] = None,
    ) -> Any:
        """Read the artifact from datastore into in memory object.

        Parameters
        ----------
        getInfo : `DatastoreFileGetInformation`
            Information about the artifact within the datastore.
        ref : `DatasetRef`
            The registry information associated with this artifact.
        isComponent : `bool`
            Flag to indicate if a component is being read from this artifact.
        cache_ref : `DatasetRef`, optional
            The DatasetRef to use when looking up the file in the cache.
            This ref must have the same ID as the supplied ref but can
            be a parent ref or component ref to indicate to the cache whether
            a composite file is being requested from the cache or a component
            file. Without this the cache will default to the supplied ref but
            it can get confused with read-only derived components for
            disassembled composites.

        Returns
        -------
        inMemoryDataset : `object`
            The artifact as a python object.
        """
        location = getInfo.location
        uri = location.uri
        log.debug("Accessing data from %s", uri)

        if cache_ref is None:
            cache_ref = ref
        if cache_ref.id != ref.id:
            raise ValueError(
                "The supplied cache dataset ref refers to a different dataset than expected:"
                f" {ref.id} != {cache_ref.id}"
            )

        # Cannot recalculate checksum but can compare size as a quick check
        # Do not do this if the size is negative since that indicates
        # we do not know.
        recorded_size = getInfo.info.file_size
        resource_size = uri.size()
        if recorded_size >= 0 and resource_size != recorded_size:
            raise RuntimeError(
                "Integrity failure in Datastore. "
                f"Size of file {uri} ({resource_size}) "
                f"does not match size recorded in registry of {recorded_size}"
            )

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
            with self.cacheManager.find_in_cache(cache_ref, uri.getExtension()) as cached_file:
                if cached_file is not None:
                    desired_uri = cached_file
                    msg = f" (cached version of {uri})"
                else:
                    desired_uri = uri
                    msg = ""
                with time_this(log, msg="Reading bytes from %s%s", args=(desired_uri, msg)):
                    serializedDataset = desired_uri.read()
            log.debug(
                "Deserializing %s from %d bytes from location %s with formatter %s",
                f"component {getInfo.component}" if isComponent else "",
                len(serializedDataset),
                uri,
                formatter.name(),
            )
            try:
                result = formatter.fromBytes(
                    serializedDataset, component=getInfo.component if isComponent else None
                )
            except Exception as e:
                raise ValueError(
                    f"Failure from formatter '{formatter.name()}' for dataset {ref.id}"
                    f" ({ref.datasetType.name} from {uri}): {e}"
                ) from e
        else:
            # Read from file.

            # Have to update the Location associated with the formatter
            # because formatter.read does not allow an override.
            # This could be improved.
            location_updated = False
            msg = ""

            # First check in cache for local version.
            # The cache will only be relevant for remote resources but
            # no harm in always asking. Context manager ensures that cache
            # file is not deleted during cache expiration.
            with self.cacheManager.find_in_cache(cache_ref, uri.getExtension()) as cached_file:
                if cached_file is not None:
                    msg = f"(via cache read of remote file {uri})"
                    uri = cached_file
                    location_updated = True

                with uri.as_local() as local_uri:

                    can_be_cached = False
                    if uri != local_uri:
                        # URI was remote and file was downloaded
                        cache_msg = ""
                        location_updated = True

                        if self.cacheManager.should_be_cached(cache_ref):
                            # In this scenario we want to ask if the downloaded
                            # file should be cached but we should not cache
                            # it until after we've used it (to ensure it can't
                            # be expired whilst we are using it).
                            can_be_cached = True

                            # Say that it is "likely" to be cached because
                            # if the formatter read fails we will not be
                            # caching this file.
                            cache_msg = " and likely cached"

                        msg = f"(via download to local file{cache_msg})"

                    # Calculate the (possibly) new location for the formatter
                    # to use.
                    newLocation = Location(*local_uri.split()) if location_updated else None

                    log.debug(
                        "Reading%s from location %s %s with formatter %s",
                        f" component {getInfo.component}" if isComponent else "",
                        uri,
                        msg,
                        formatter.name(),
                    )
                    try:
                        with formatter._updateLocation(newLocation):
                            with time_this(
                                log,
                                msg="Reading%s from location %s %s with formatter %s",
                                args=(
                                    f" component {getInfo.component}" if isComponent else "",
                                    uri,
                                    msg,
                                    formatter.name(),
                                ),
                            ):
                                result = formatter.read(component=getInfo.component if isComponent else None)
                    except Exception as e:
                        raise ValueError(
                            f"Failure from formatter '{formatter.name()}' for dataset {ref.id}"
                            f" ({ref.datasetType.name} from {uri}): {e}"
                        ) from e

                    # File was read successfully so can move to cache
                    if can_be_cached:
                        self.cacheManager.move_to_cache(local_uri, cache_ref)

        return self._post_process_get(
            result, getInfo.readStorageClass, getInfo.assemblerParams, isComponent=isComponent
        )

    def knows(self, ref: DatasetRef) -> bool:
        """Check if the dataset is known to the datastore.

        Does not check for existence of any artifact.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required dataset.

        Returns
        -------
        exists : `bool`
            `True` if the dataset is known to the datastore.
        """
        fileLocations = self._get_dataset_locations_info(ref)
        if fileLocations:
            return True
        return False

    def knows_these(self, refs: Iterable[DatasetRef]) -> dict[DatasetRef, bool]:
        # Docstring inherited from the base class.

        # The records themselves. Could be missing some entries.
        records = self._get_stored_records_associated_with_refs(refs)

        return {ref: ref.id in records for ref in refs}

    def _process_mexists_records(
        self,
        id_to_ref: Dict[DatasetId, DatasetRef],
        records: Dict[DatasetId, List[StoredFileInfo]],
        all_required: bool,
        artifact_existence: Optional[Dict[ResourcePath, bool]] = None,
    ) -> Dict[DatasetRef, bool]:
        """Helper function for mexists that checks the given records.

        Parameters
        ----------
        id_to_ref : `dict` of [`DatasetId`, `DatasetRef`]
            Mapping of the dataset ID to the dataset ref itself.
        records : `dict` of [`DatasetId`, `list` of `StoredFileInfo`]
            Records as generally returned by
            ``_get_stored_records_associated_with_refs``.
        all_required : `bool`
            Flag to indicate whether existence requires all artifacts
            associated with a dataset ID to exist or not for existence.
        artifact_existence : `dict` [`lsst.resources.ResourcePath`, `bool`]
            Optional mapping of datastore artifact to existence. Updated by
            this method with details of all artifacts tested. Can be `None`
            if the caller is not interested.

        Returns
        -------
        existence : `dict` of [`DatasetRef`, `bool`]
            Mapping from dataset to boolean indicating existence.
        """
        # The URIs to be checked and a mapping of those URIs to
        # the dataset ID.
        uris_to_check: List[ResourcePath] = []
        location_map: Dict[ResourcePath, DatasetId] = {}

        location_factory = self.locationFactory

        uri_existence: Dict[ResourcePath, bool] = {}
        for ref_id, infos in records.items():
            # Key is the dataset Id, value is list of StoredItemInfo
            uris = [info.file_location(location_factory).uri for info in infos]
            location_map.update({uri: ref_id for uri in uris})

            # Check the local cache directly for a dataset corresponding
            # to the remote URI.
            if self.cacheManager.file_count > 0:
                ref = id_to_ref[ref_id]
                for uri, storedFileInfo in zip(uris, infos):
                    check_ref = ref
                    if not ref.datasetType.isComponent() and (component := storedFileInfo.component):
                        check_ref = ref.makeComponentRef(component)
                    if self.cacheManager.known_to_cache(check_ref, uri.getExtension()):
                        # Proxy for URI existence.
                        uri_existence[uri] = True
                    else:
                        uris_to_check.append(uri)
            else:
                # Check all of them.
                uris_to_check.extend(uris)

        if artifact_existence is not None:
            # If a URI has already been checked remove it from the list
            # and immediately add the status to the output dict.
            filtered_uris_to_check = []
            for uri in uris_to_check:
                if uri in artifact_existence:
                    uri_existence[uri] = artifact_existence[uri]
                else:
                    filtered_uris_to_check.append(uri)
            uris_to_check = filtered_uris_to_check

        # Results.
        dataset_existence: Dict[DatasetRef, bool] = {}

        uri_existence.update(ResourcePath.mexists(uris_to_check))
        for uri, exists in uri_existence.items():
            dataset_id = location_map[uri]
            ref = id_to_ref[dataset_id]

            # Disassembled composite needs to check all locations.
            # all_required indicates whether all need to exist or not.
            if ref in dataset_existence:
                if all_required:
                    exists = dataset_existence[ref] and exists
                else:
                    exists = dataset_existence[ref] or exists
            dataset_existence[ref] = exists

        if artifact_existence is not None:
            artifact_existence.update(uri_existence)

        return dataset_existence

    def mexists(
        self, refs: Iterable[DatasetRef], artifact_existence: Optional[Dict[ResourcePath, bool]] = None
    ) -> Dict[DatasetRef, bool]:
        """Check the existence of multiple datasets at once.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets to be checked.
        artifact_existence : `dict` [`lsst.resources.ResourcePath`, `bool`]
            Optional mapping of datastore artifact to existence. Updated by
            this method with details of all artifacts tested. Can be `None`
            if the caller is not interested.

        Returns
        -------
        existence : `dict` of [`DatasetRef`, `bool`]
            Mapping from dataset to boolean indicating existence.

        Notes
        -----
        To minimize potentially costly remote existence checks, the local
        cache is checked as a proxy for existence. If a file for this
        `DatasetRef` does exist no check is done for the actual URI. This
        could result in possibly unexpected behavior if the dataset itself
        has been removed from the datastore by another process whilst it is
        still in the cache.
        """
        chunk_size = 10_000
        dataset_existence: Dict[DatasetRef, bool] = {}
        log.debug("Checking for the existence of multiple artifacts in datastore in chunks of %d", chunk_size)
        n_found_total = 0
        n_checked = 0
        n_chunks = 0
        for chunk in chunk_iterable(refs, chunk_size=chunk_size):
            chunk_result = self._mexists(chunk, artifact_existence)
            if log.isEnabledFor(VERBOSE):
                n_results = len(chunk_result)
                n_checked += n_results
                # Can treat the booleans as 0, 1 integers and sum them.
                n_found = sum(chunk_result.values())
                n_found_total += n_found
                log.verbose(
                    "Number of datasets found in datastore for chunk %d = %d/%d (running total: %d/%d)",
                    n_chunks,
                    n_found,
                    n_results,
                    n_found_total,
                    n_checked,
                )
            dataset_existence.update(chunk_result)
            n_chunks += 1

        return dataset_existence

    def _mexists(
        self, refs: Sequence[DatasetRef], artifact_existence: Optional[Dict[ResourcePath, bool]] = None
    ) -> Dict[DatasetRef, bool]:
        """Check the existence of multiple datasets at once.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets to be checked.
        artifact_existence : `dict` [`lsst.resources.ResourcePath`, `bool`]
            Optional mapping of datastore artifact to existence. Updated by
            this method with details of all artifacts tested. Can be `None`
            if the caller is not interested.

        Returns
        -------
        existence : `dict` of [`DatasetRef`, `bool`]
            Mapping from dataset to boolean indicating existence.
        """
        # Need a mapping of dataset_id to dataset ref since the API
        # works with dataset_id
        id_to_ref = {ref.getCheckedId(): ref for ref in refs}

        # Set of all IDs we are checking for.
        requested_ids = set(id_to_ref.keys())

        # The records themselves. Could be missing some entries.
        records = self._get_stored_records_associated_with_refs(refs)

        dataset_existence = self._process_mexists_records(
            id_to_ref, records, True, artifact_existence=artifact_existence
        )

        # Set of IDs that have been handled.
        handled_ids = {ref.id for ref in dataset_existence.keys()}

        missing_ids = requested_ids - handled_ids
        if missing_ids:
            dataset_existence.update(
                self._mexists_check_expected(
                    [id_to_ref[missing] for missing in missing_ids], artifact_existence
                )
            )

        return dataset_existence

    def _mexists_check_expected(
        self, refs: Sequence[DatasetRef], artifact_existence: Optional[Dict[ResourcePath, bool]] = None
    ) -> Dict[DatasetRef, bool]:
        """Check existence of refs that are not known to datastore.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets to be checked. These are assumed not to be known
            to datastore.
        artifact_existence : `dict` [`lsst.resources.ResourcePath`, `bool`]
            Optional mapping of datastore artifact to existence. Updated by
            this method with details of all artifacts tested. Can be `None`
            if the caller is not interested.

        Returns
        -------
        existence : `dict` of [`DatasetRef`, `bool`]
            Mapping from dataset to boolean indicating existence.
        """
        dataset_existence: Dict[DatasetRef, bool] = {}
        if not self.trustGetRequest:
            # Must assume these do not exist
            for ref in refs:
                dataset_existence[ref] = False
        else:
            log.debug(
                "%d datasets were not known to datastore during initial existence check.",
                len(refs),
            )

            # Construct data structure identical to that returned
            # by _get_stored_records_associated_with_refs() but using
            # guessed names.
            records = {}
            id_to_ref = {}
            for missing_ref in refs:
                expected = self._get_expected_dataset_locations_info(missing_ref)
                dataset_id = missing_ref.getCheckedId()
                records[dataset_id] = [info for _, info in expected]
                id_to_ref[dataset_id] = missing_ref

            dataset_existence.update(
                self._process_mexists_records(
                    id_to_ref,
                    records,
                    False,
                    artifact_existence=artifact_existence,
                )
            )

        return dataset_existence

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

        Notes
        -----
        The local cache is checked as a proxy for existence in the remote
        object store. It is possible that another process on a different
        compute node could remove the file from the object store even
        though it is present in the local cache.
        """
        fileLocations = self._get_dataset_locations_info(ref)

        # if we are being asked to trust that registry might not be correct
        # we ask for the expected locations and check them explicitly
        if not fileLocations:
            if not self.trustGetRequest:
                return False

            # First check the cache. If it is not found we must check
            # the datastore itself. Assume that any component in the cache
            # means that the dataset does exist somewhere.
            if self.cacheManager.known_to_cache(ref):
                return True

            # When we are guessing a dataset location we can not check
            # for the existence of every component since we can not
            # know if every component was written. Instead we check
            # for the existence of any of the expected locations.
            for location, _ in self._get_expected_dataset_locations_info(ref):
                if self._artifact_exists(location):
                    return True
            return False

        # All listed artifacts must exist.
        for location, storedFileInfo in fileLocations:
            # Checking in cache needs the component ref.
            check_ref = ref
            if not ref.datasetType.isComponent() and (component := storedFileInfo.component):
                check_ref = ref.makeComponentRef(component)
            if self.cacheManager.known_to_cache(check_ref, location.getExtension()):
                continue

            if not self._artifact_exists(location):
                return False

        return True

    def getURIs(self, ref: DatasetRef, predict: bool = False) -> DatasetRefURIs:
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
        uris : `DatasetRefURIs`
            The URI to the primary artifact associated with this dataset (if
            the dataset was disassembled within the datastore this may be
            `None`), and the URIs to any components associated with the dataset
            artifact. (can be empty if there are no components).
        """
        many = self.getManyURIs([ref], predict=predict, allow_missing=False)
        return many[ref]

    def getURI(self, ref: DatasetRef, predict: bool = False) -> ResourcePath:
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
            raise RuntimeError(
                f"Dataset ({ref}) includes distinct URIs for components. Use Datastore.getURIs() instead."
            )
        return primary

    def _predict_URIs(
        self,
        ref: DatasetRef,
    ) -> DatasetRefURIs:
        """Predict the URIs of a dataset ref.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.

        Returns
        -------
        URI : DatasetRefUris
            Primary and component URIs. URIs will contain a URI fragment
            "#predicted".
        """
        uris = DatasetRefURIs()

        if self.composites.shouldBeDisassembled(ref):

            for component, _ in ref.datasetType.storageClass.components.items():
                comp_ref = ref.makeComponentRef(component)
                comp_location, _ = self._determine_put_formatter_location(comp_ref)

                # Add the "#predicted" URI fragment to indicate this is a
                # guess
                uris.componentURIs[component] = ResourcePath(comp_location.uri.geturl() + "#predicted")

        else:

            location, _ = self._determine_put_formatter_location(ref)

            # Add the "#predicted" URI fragment to indicate this is a guess
            uris.primaryURI = ResourcePath(location.uri.geturl() + "#predicted")

        return uris

    def getManyURIs(
        self,
        refs: Iterable[DatasetRef],
        predict: bool = False,
        allow_missing: bool = False,
    ) -> Dict[DatasetRef, DatasetRefURIs]:
        # Docstring inherited

        uris: Dict[DatasetRef, DatasetRefURIs] = {}

        records = self._get_stored_records_associated_with_refs(refs)
        records_keys = records.keys()

        existing_refs = tuple(ref for ref in refs if ref.id in records_keys)
        missing_refs = tuple(ref for ref in refs if ref.id not in records_keys)

        # Have to handle trustGetRequest mode by checking for the existence
        # of the missing refs on disk.
        if missing_refs:
            dataset_existence = self._mexists_check_expected(missing_refs, None)
            really_missing = set()
            not_missing = set()
            for ref, exists in dataset_existence.items():
                if exists:
                    not_missing.add(ref)
                else:
                    really_missing.add(ref)

            if not_missing:
                # Need to recalculate the missing/existing split.
                existing_refs = existing_refs + tuple(not_missing)
                missing_refs = tuple(really_missing)

        for ref in missing_refs:
            # if this has never been written then we have to guess
            if not predict:
                if not allow_missing:
                    raise FileNotFoundError("Dataset {} not in this datastore.".format(ref))
            else:
                uris[ref] = self._predict_URIs(ref)

        for ref in existing_refs:
            file_infos = records[ref.getCheckedId()]
            file_locations = [(i.file_location(self.locationFactory), i) for i in file_infos]
            uris[ref] = self._locations_to_URI(ref, file_locations)

        return uris

    def _locations_to_URI(
        self,
        ref: DatasetRef,
        file_locations: Sequence[Tuple[Location, StoredFileInfo]],
    ) -> DatasetRefURIs:
        """Convert one or more file locations associated with a DatasetRef
        to a DatasetRefURIs.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the dataset.
        file_locations : Sequence[Tuple[Location, StoredFileInfo]]
            Each item in the sequence is the location of the dataset within the
            datastore and stored information about the file and its formatter.
            If there is only one item in the sequence then it is treated as the
            primary URI. If there is more than one item then they are treated
            as component URIs. If there are no items then an error is raised
            unless ``self.trustGetRequest`` is `True`.

        Returns
        -------
        uris: DatasetRefURIs
            Represents the primary URI or component URIs described by the
            inputs.

        Raises
        ------
        RuntimeError
            If no file locations are passed in and ``self.trustGetRequest`` is
            `False`.
        FileNotFoundError
            If the a passed-in URI does not exist, and ``self.trustGetRequest``
            is `False`.
        RuntimeError
            If a passed in `StoredFileInfo`'s ``component`` is `None` (this is
            unexpected).
        """

        guessing = False
        uris = DatasetRefURIs()

        if not file_locations:
            if not self.trustGetRequest:
                raise RuntimeError(f"Unexpectedly got no artifacts for dataset {ref}")
            file_locations = self._get_expected_dataset_locations_info(ref)
            guessing = True

        if len(file_locations) == 1:
            # No disassembly so this is the primary URI
            uris.primaryURI = file_locations[0][0].uri
            if guessing and not uris.primaryURI.exists():
                raise FileNotFoundError(f"Expected URI ({uris.primaryURI}) does not exist")
        else:
            for location, file_info in file_locations:
                if file_info.component is None:
                    raise RuntimeError(f"Unexpectedly got no component name for a component at {location}")
                if guessing and not location.uri.exists():
                    # If we are trusting then it is entirely possible for
                    # some components to be missing. In that case we skip
                    # to the next component.
                    if self.trustGetRequest:
                        continue
                    raise FileNotFoundError(f"Expected URI ({location.uri}) does not exist")
                uris.componentURIs[file_info.component] = location.uri

        return uris

    def retrieveArtifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePath,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool = False,
    ) -> List[ResourcePath]:
        """Retrieve the file artifacts associated with the supplied refs.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets for which file artifacts are to be retrieved.
            A single ref can result in multiple files. The refs must
            be resolved.
        destination : `lsst.resources.ResourcePath`
            Location to write the file artifacts.
        transfer : `str`, optional
            Method to use to transfer the artifacts. Must be one of the options
            supported by `lsst.resources.ResourcePath.transfer_from()`.
            "move" is not allowed.
        preserve_path : `bool`, optional
            If `True` the full path of the file artifact within the datastore
            is preserved. If `False` the final file component of the path
            is used.
        overwrite : `bool`, optional
            If `True` allow transfers to overwrite existing files at the
            destination.

        Returns
        -------
        targets : `list` of `lsst.resources.ResourcePath`
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
        to_transfer: Dict[ResourcePath, ResourcePath] = {}

        for ref in refs:
            locations = self._get_dataset_locations_info(ref)
            for location, _ in locations:
                source_uri = location.uri
                target_path: ResourcePathExpression
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
        log.debug("Number of artifacts to transfer to %s: %d", str(destination), len(to_transfer))
        for source_uri, target_uri in to_transfer.items():
            target_uri.transfer_from(source_uri, transfer=transfer, overwrite=overwrite)

        return list(to_transfer.values())

    def get(
        self,
        ref: DatasetRef,
        parameters: Optional[Mapping[str, Any]] = None,
        storageClass: Optional[Union[StorageClass, str]] = None,
    ) -> Any:
        """Load an InMemoryDataset from the store.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
        parameters : `dict`
            `StorageClass`-specific parameters that specify, for example,
            a slice of the dataset to be loaded.
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type.

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
        # Supplied storage class for the component being read is either
        # from the ref itself or some an override if we want to force
        # type conversion.
        if storageClass is not None:
            ref = ref.overrideStorageClass(storageClass)
        refStorageClass = ref.datasetType.storageClass

        allGetInfo = self._prepare_for_get(ref, parameters)
        refComponent = ref.datasetType.component()

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
                components[component] = self._read_artifact_into_memory(
                    getInfo, ref.makeComponentRef(component), isComponent=False
                )

            inMemoryDataset = ref.datasetType.storageClass.delegate().assemble(components)

            # Any unused parameters will have to be passed to the assembler
            if parameters:
                unusedParams = {k: v for k, v in parameters.items() if k not in usedParams}
            else:
                unusedParams = {}

            # Process parameters
            return ref.datasetType.storageClass.delegate().handleParameters(
                inMemoryDataset, parameters=unusedParams
            )

        elif isDisassembledReadOnlyComponent:

            compositeStorageClass = ref.datasetType.parentStorageClass
            if compositeStorageClass is None:
                raise RuntimeError(
                    f"Unable to retrieve derived component '{refComponent}' since"
                    "no composite storage class is available."
                )

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
            forwardedComponent = compositeDelegate.selectResponsibleComponent(
                refComponent, set(allComponents)
            )

            # Select the relevant component
            rwInfo = allComponents[forwardedComponent]

            # For now assume that read parameters are validated against
            # the real component and not the requested component
            forwardedStorageClass = rwInfo.formatter.fileDescriptor.readStorageClass
            forwardedStorageClass.validateParameters(parameters)

            # The reference to use for the caching must refer to the forwarded
            # component and not the derived component.
            cache_ref = ref.makeCompositeRef().makeComponentRef(forwardedComponent)

            # Unfortunately the FileDescriptor inside the formatter will have
            # the wrong write storage class so we need to create a new one
            # given the immutability constraint.
            writeStorageClass = rwInfo.info.storageClass

            # We may need to put some thought into parameters for read
            # components but for now forward them on as is
            readFormatter = type(rwInfo.formatter)(
                FileDescriptor(
                    rwInfo.location,
                    readStorageClass=refStorageClass,
                    storageClass=writeStorageClass,
                    parameters=parameters,
                ),
                ref.dataId,
            )

            # The assembler can not receive any parameter requests for a
            # derived component at this time since the assembler will
            # see the storage class of the derived component and those
            # parameters will have to be handled by the formatter on the
            # forwarded storage class.
            assemblerParams: Dict[str, Any] = {}

            # Need to created a new info that specifies the derived
            # component and associated storage class
            readInfo = DatastoreFileGetInformation(
                rwInfo.location,
                readFormatter,
                rwInfo.info,
                assemblerParams,
                {},
                refComponent,
                refStorageClass,
            )

            return self._read_artifact_into_memory(readInfo, ref, isComponent=True, cache_ref=cache_ref)

        else:
            # Single file request or component from that composite file
            for lookup in (refComponent, None):
                if lookup in allComponents:
                    getInfo = allComponents[lookup]
                    break
            else:
                raise FileNotFoundError(
                    f"Component {refComponent} not found for ref {ref} in datastore {self.name}"
                )

            # Do not need the component itself if already disassembled
            if isDisassembled:
                isComponent = False
            else:
                isComponent = getInfo.component is not None

            # For a component read of a composite we want the cache to
            # be looking at the composite ref itself.
            cache_ref = ref.makeCompositeRef() if isComponent else ref

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

            return self._read_artifact_into_memory(getInfo, ref, isComponent=isComponent, cache_ref=cache_ref)

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
            if components is None:
                raise RuntimeError(
                    f"Inconsistent configuration: dataset type {ref.datasetType.name} "
                    f"with storage class {ref.datasetType.storageClass.name} "
                    "is configured to be disassembled, but cannot be."
                )
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
    def trash(self, ref: Union[DatasetRef, Iterable[DatasetRef]], ignore_errors: bool = True) -> None:
        # At this point can safely remove these datasets from the cache
        # to avoid confusion later on. If they are not trashed later
        # the cache will simply be refilled.
        self.cacheManager.remove_from_cache(ref)

        # If we are in trust mode there will be nothing to move to
        # the trash table and we will have to try to delete the file
        # immediately.
        if self.trustGetRequest:
            # Try to keep the logic below for a single file trash.
            if isinstance(ref, DatasetRef):
                refs = {ref}
            else:
                # Will recreate ref at the end of this branch.
                refs = set(ref)

            # Determine which datasets are known to datastore directly.
            id_to_ref = {ref.getCheckedId(): ref for ref in refs}
            existing_ids = self._get_stored_records_associated_with_refs(refs)
            existing_refs = {id_to_ref[ref_id] for ref_id in existing_ids}

            missing = refs - existing_refs
            if missing:
                # Do an explicit existence check on these refs.
                # We only care about the artifacts at this point and not
                # the dataset existence.
                artifact_existence: Dict[ResourcePath, bool] = {}
                _ = self.mexists(missing, artifact_existence)
                uris = [uri for uri, exists in artifact_existence.items() if exists]

                # FUTURE UPGRADE: Implement a parallelized bulk remove.
                log.debug("Removing %d artifacts from datastore that are unknown to datastore", len(uris))
                for uri in uris:
                    try:
                        uri.remove()
                    except Exception as e:
                        if ignore_errors:
                            log.debug("Artifact %s could not be removed: %s", uri, e)
                            continue
                        raise

            # There is no point asking the code below to remove refs we
            # know are missing so update it with the list of existing
            # records. Try to retain one vs many logic.
            if not existing_refs:
                # Nothing more to do since none of the datasets were
                # known to the datastore record table.
                return
            ref = list(existing_refs)
            if len(ref) == 1:
                ref = ref[0]

        # Get file metadata and internal metadata
        if not isinstance(ref, DatasetRef):
            log.debug("Doing multi-dataset trash in datastore %s", self.name)
            # Assumed to be an iterable of refs so bulk mode enabled.
            try:
                self.bridge.moveToTrash(ref, transaction=self._transaction)
            except Exception as e:
                if ignore_errors:
                    log.warning("Unexpected issue moving multiple datasets to trash: %s", e)
                else:
                    raise
            return

        log.debug("Trashing dataset %s in datastore %s", ref, self.name)

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
                err_msg = (
                    f"Dataset is known to datastore {self.name} but "
                    f"associated artifact ({location.uri}) is missing"
                )
                if ignore_errors:
                    log.warning(err_msg)
                    return
                else:
                    raise FileNotFoundError(err_msg)

        # Mark dataset as trashed
        try:
            self.bridge.moveToTrash([ref], transaction=self._transaction)
        except Exception as e:
            if ignore_errors:
                log.warning(
                    "Attempted to mark dataset (%s) to be trashed in datastore %s "
                    "but encountered an error: %s",
                    ref,
                    self.name,
                    e,
                )
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
        with self.bridge.emptyTrash(
            self._table, record_class=StoredFileInfo, record_column="path"
        ) as trash_data:
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
                path_map = self._refs_associated_with_artifacts(
                    [info.path for _, info in trashed if isinstance(info, StoredFileInfo)]
                )

                for ref, info in trashed:

                    # Mypy needs to know this is not the base class
                    assert isinstance(info, StoredFileInfo), f"Unexpectedly got info of class {type(info)}"

                    # Check for mypy
                    assert ref.id is not None, f"Internal logic error in emptyTrash with ref {ref}/{info}"

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

                if info.path in artifacts_to_keep:
                    # This is a multi-dataset artifact and we are not
                    # removing all associated refs.
                    continue

                # Only trashed refs still known to datastore will be returned.
                location = info.file_location(self.locationFactory)

                # Point of no return for this artifact
                log.debug("Removing artifact %s from datastore %s", location.uri, self.name)
                try:
                    self._delete_artifact(location)
                except FileNotFoundError:
                    # If the file itself has been deleted there is nothing
                    # we can do about it. It is possible that trash has
                    # been run in parallel in another process or someone
                    # decided to delete the file. It is unlikely to come
                    # back and so we should still continue with the removal
                    # of the entry from the trash table. It is also possible
                    # we removed it in a previous iteration if it was
                    # a multi-dataset artifact. The delete artifact method
                    # will log a debug message in this scenario.
                    # Distinguishing file missing before trash started and
                    # file already removed previously as part of this trash
                    # is not worth the distinction with regards to potential
                    # memory cost.
                    pass
                except Exception as e:
                    if ignore_errors:
                        # Use a debug message here even though it's not
                        # a good situation. In some cases this can be
                        # caused by a race between user A and user B
                        # and neither of them has permissions for the
                        # other's files. Butler does not know about users
                        # and trash has no idea what collections these
                        # files were in (without guessing from a path).
                        log.debug(
                            "Encountered error removing artifact %s from datastore %s: %s",
                            location.uri,
                            self.name,
                            e,
                        )
                    else:
                        raise

    @transactional
    def transfer_from(
        self,
        source_datastore: Datastore,
        refs: Iterable[DatasetRef],
        local_refs: Optional[Iterable[DatasetRef]] = None,
        transfer: str = "auto",
        artifact_existence: Optional[Dict[ResourcePath, bool]] = None,
    ) -> None:
        # Docstring inherited
        if type(self) is not type(source_datastore):
            raise TypeError(
                f"Datastore mismatch between this datastore ({type(self)}) and the "
                f"source datastore ({type(source_datastore)})."
            )

        # Be explicit for mypy
        if not isinstance(source_datastore, FileDatastore):
            raise TypeError(
                "Can only transfer to a FileDatastore from another FileDatastore, not"
                f" {type(source_datastore)}"
            )

        # Stop early if "direct" transfer mode is requested. That would
        # require that the URI inside the source datastore should be stored
        # directly in the target datastore, which seems unlikely to be useful
        # since at any moment the source datastore could delete the file.
        if transfer in ("direct", "split"):
            raise ValueError(
                f"Can not transfer from a source datastore using {transfer} mode since"
                " those files are controlled by the other datastore."
            )

        # Empty existence lookup if none given.
        if artifact_existence is None:
            artifact_existence = {}

        # We will go through the list multiple times so must convert
        # generators to lists.
        refs = list(refs)

        if local_refs is None:
            local_refs = refs
        else:
            local_refs = list(local_refs)

        # In order to handle disassembled composites the code works
        # at the records level since it can assume that internal APIs
        # can be used.
        # - If the record already exists in the destination this is assumed
        #   to be okay.
        # - If there is no record but the source and destination URIs are
        #   identical no transfer is done but the record is added.
        # - If the source record refers to an absolute URI currently assume
        #   that that URI should remain absolute and will be visible to the
        #   destination butler. May need to have a flag to indicate whether
        #   the dataset should be transferred. This will only happen if
        #   the detached Butler has had a local ingest.

        # What we really want is all the records in the source datastore
        # associated with these refs. Or derived ones if they don't exist
        # in the source.
        source_records = source_datastore._get_stored_records_associated_with_refs(refs)

        # The source dataset_ids are the keys in these records
        source_ids = set(source_records)
        log.debug("Number of datastore records found in source: %d", len(source_ids))

        # The not None check is to appease mypy
        requested_ids = set(ref.id for ref in refs if ref.id is not None)
        missing_ids = requested_ids - source_ids

        # Missing IDs can be okay if that datastore has allowed
        # gets based on file existence. Should we transfer what we can
        # or complain about it and warn?
        if missing_ids and not source_datastore.trustGetRequest:
            raise ValueError(
                f"Some datasets are missing from source datastore {source_datastore}: {missing_ids}"
            )

        # Need to map these missing IDs to a DatasetRef so we can guess
        # the details.
        if missing_ids:
            log.info(
                "Number of expected datasets missing from source datastore records: %d out of %d",
                len(missing_ids),
                len(requested_ids),
            )
            id_to_ref = {ref.id: ref for ref in refs if ref.id in missing_ids}

            # This should be chunked in case we end up having to check
            # the file store since we need some log output to show
            # progress.
            for missing_ids_chunk in chunk_iterable(missing_ids, chunk_size=10_000):
                records = {}
                for missing in missing_ids_chunk:
                    # Ask the source datastore where the missing artifacts
                    # should be.  An execution butler might not know about the
                    # artifacts even if they are there.
                    expected = source_datastore._get_expected_dataset_locations_info(id_to_ref[missing])
                    records[missing] = [info for _, info in expected]

                # Call the mexist helper method in case we have not already
                # checked these artifacts such that artifact_existence is
                # empty. This allows us to benefit from parallelism.
                # datastore.mexists() itself does not give us access to the
                # derived datastore record.
                log.verbose("Checking existence of %d datasets unknown to datastore", len(records))
                ref_exists = source_datastore._process_mexists_records(
                    id_to_ref, records, False, artifact_existence=artifact_existence
                )

                # Now go through the records and propagate the ones that exist.
                location_factory = source_datastore.locationFactory
                for missing, record_list in records.items():
                    # Skip completely if the ref does not exist.
                    ref = id_to_ref[missing]
                    if not ref_exists[ref]:
                        log.warning("Asked to transfer dataset %s but no file artifacts exist for it.", ref)
                        continue
                    # Check for file artifact to decide which parts of a
                    # disassembled composite do exist. If there is only a
                    # single record we don't even need to look because it can't
                    # be a composite and must exist.
                    if len(record_list) == 1:
                        dataset_records = record_list
                    else:
                        dataset_records = [
                            record
                            for record in record_list
                            if artifact_existence[record.file_location(location_factory).uri]
                        ]
                        assert len(dataset_records) > 0, "Disassembled composite should have had some files."

                    # Rely on source_records being a defaultdict.
                    source_records[missing].extend(dataset_records)

        # See if we already have these records
        target_records = self._get_stored_records_associated_with_refs(local_refs)

        # The artifacts to register
        artifacts = []

        # Refs that already exist
        already_present = []

        # Now can transfer the artifacts
        for source_ref, target_ref in zip(refs, local_refs):
            if target_ref.id in target_records:
                # Already have an artifact for this.
                already_present.append(target_ref)
                continue

            # mypy needs to know these are always resolved refs
            for info in source_records[source_ref.getCheckedId()]:
                source_location = info.file_location(source_datastore.locationFactory)
                target_location = info.file_location(self.locationFactory)
                if source_location == target_location:
                    # Either the dataset is already in the target datastore
                    # (which is how execution butler currently runs) or
                    # it is an absolute URI.
                    if source_location.pathInStore.isabs():
                        # Just because we can see the artifact when running
                        # the transfer doesn't mean it will be generally
                        # accessible to a user of this butler. For now warn
                        # but assume it will be accessible.
                        log.warning(
                            "Transfer request for an outside-datastore artifact has been found at %s",
                            source_location,
                        )
                else:
                    # Need to transfer it to the new location.
                    # Assume we should always overwrite. If the artifact
                    # is there this might indicate that a previous transfer
                    # was interrupted but was not able to be rolled back
                    # completely (eg pre-emption) so follow Datastore default
                    # and overwrite.
                    target_location.uri.transfer_from(
                        source_location.uri, transfer=transfer, overwrite=True, transaction=self._transaction
                    )

                artifacts.append((target_ref, info))

        self._register_datasets(artifacts)

        if already_present:
            n_skipped = len(already_present)
            log.info(
                "Skipped transfer of %d dataset%s already present in datastore",
                n_skipped,
                "" if n_skipped == 1 else "s",
            )

    @transactional
    def forget(self, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited.
        refs = list(refs)
        self.bridge.forget(refs)
        self._table.delete(["dataset_id"], *[{"dataset_id": ref.getCheckedId()} for ref in refs])

    def validateConfiguration(
        self, entities: Iterable[Union[DatasetRef, DatasetType, StorageClass]], logFailures: bool = False
    ) -> None:
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
        return (
            self.templates.getLookupKeys()
            | self.formatterFactory.getLookupKeys()
            | self.constraints.getLookupKeys()
        )

    def validateKey(self, lookupKey: LookupKey, entity: Union[DatasetRef, DatasetType, StorageClass]) -> None:
        # Docstring is inherited from base class
        # The key can be valid in either formatters or templates so we can
        # only check the template if it exists
        if lookupKey in self.templates:
            try:
                self.templates[lookupKey].validateTemplate(entity)
            except FileTemplateValidationError as e:
                raise DatastoreValidationError(e) from e

    def export(
        self,
        refs: Iterable[DatasetRef],
        *,
        directory: Optional[ResourcePathExpression] = None,
        transfer: Optional[str] = "auto",
    ) -> Iterable[FileDataset]:
        # Docstring inherited from Datastore.export.
        if transfer == "auto" and directory is None:
            transfer = None

        if transfer is not None and directory is None:
            raise TypeError(f"Cannot export using transfer mode {transfer} with no export directory given")

        if transfer == "move":
            raise TypeError("Can not export by moving files out of datastore.")
        elif transfer == "direct":
            # For an export, treat this as equivalent to None. We do not
            # want an import to risk using absolute URIs to datasets owned
            # by another datastore.
            log.info("Treating 'direct' transfer mode as in-place export.")
            transfer = None

        # Force the directory to be a URI object
        directoryUri: Optional[ResourcePath] = None
        if directory is not None:
            directoryUri = ResourcePath(directory, forceDirectory=True)

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
                # We will use the path in store directly. If this is an
                # absolute URI, preserve it.
                if location.pathInStore.isabs():
                    pathInStore = str(location.uri)
            elif transfer == "direct":
                # Use full URIs to the remote store in the export
                pathInStore = str(location.uri)
            else:
                # mypy needs help
                assert directoryUri is not None, "directoryUri must be defined to get here"
                storeUri = ResourcePath(location.uri)

                # if the datastore has an absolute URI to a resource, we
                # have two options:
                # 1. Keep the absolute URI in the exported YAML
                # 2. Allocate a new name in the local datastore and transfer
                #    it.
                # For now go with option 2
                if location.pathInStore.isabs():
                    template = self.templates.getTemplate(ref)
                    newURI = ResourcePath(template.format(ref), forceAbsolute=False)
                    pathInStore = str(newURI.updatedExtension(location.pathInStore.getExtension()))

                exportUri = directoryUri.join(pathInStore)
                exportUri.transfer_from(storeUri, transfer=transfer)

            yield FileDataset(refs=[ref], path=pathInStore, formatter=storedFileInfo.formatter)

    @staticmethod
    def computeChecksum(
        uri: ResourcePath, algorithm: str = "blake2b", block_size: int = 8192
    ) -> Optional[str]:
        """Compute the checksum of the supplied file.

        Parameters
        ----------
        uri : `lsst.resources.ResourcePath`
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

    def import_records(self, data: Mapping[str, DatastoreRecordData]) -> None:
        # Docstring inherited from the base class.
        record_data = data.get(self.name)
        if not record_data:
            return

        self._bridge.insert(FakeDatasetRef(dataset_id) for dataset_id in record_data.records.keys())

        # TODO: Verify that there are no unexpected table names in the dict?
        unpacked_records = []
        for dataset_data in record_data.records.values():
            records = dataset_data.get(self._table.name)
            if records:
                for info in records:
                    assert isinstance(info, StoredFileInfo), "Expecting StoredFileInfo records"
                    unpacked_records.append(info.to_record())
        if unpacked_records:
            self._table.insert(*unpacked_records, transaction=self._transaction)

    def export_records(self, refs: Iterable[DatasetIdRef]) -> Mapping[str, DatastoreRecordData]:
        # Docstring inherited from the base class.
        exported_refs = list(self._bridge.check(refs))
        ids = {ref.getCheckedId() for ref in exported_refs}
        records: defaultdict[DatasetId, defaultdict[str, List[StoredDatastoreItemInfo]]] = defaultdict(
            lambda: defaultdict(list), {id: defaultdict(list) for id in ids}
        )
        for row in self._table.fetch(dataset_id=ids):
            info: StoredDatastoreItemInfo = StoredFileInfo.from_record(row)
            records[info.dataset_id][self._table.name].append(info)

        record_data = DatastoreRecordData(records=records)
        return {self.name: record_data}
