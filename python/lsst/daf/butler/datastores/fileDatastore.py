# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from __future__ import annotations

__all__ = ("FileDatastore",)

import contextlib
import hashlib
import logging
from collections import defaultdict
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, cast

from lsst.daf.butler import (
    Config,
    DatasetDatastoreRecords,
    DatasetId,
    DatasetRef,
    DatasetType,
    DatasetTypeNotSupportedError,
    FileDataset,
    FileDescriptor,
    Formatter,
    FormatterFactory,
    FormatterV1inV2,
    FormatterV2,
    Location,
    LocationFactory,
    Progress,
    StorageClass,
    ddl,
)
from lsst.daf.butler.datastore import (
    DatasetRefURIs,
    Datastore,
    DatastoreConfig,
    DatastoreOpaqueTable,
    DatastoreValidationError,
)
from lsst.daf.butler.datastore.cache_manager import (
    AbstractDatastoreCacheManager,
    DatastoreCacheManager,
    DatastoreDisabledCacheManager,
)
from lsst.daf.butler.datastore.composites import CompositesMap
from lsst.daf.butler.datastore.file_templates import FileTemplates, FileTemplateValidationError
from lsst.daf.butler.datastore.generic_base import GenericBaseDatastore
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from lsst.daf.butler.datastore.stored_file_info import StoredDatastoreItemInfo, StoredFileInfo
from lsst.daf.butler.datastores.file_datastore.get import (
    DatasetLocationInformation,
    DatastoreFileGetInformation,
    generate_datastore_get_information,
    get_dataset_as_python_object_from_get_info,
)
from lsst.daf.butler.datastores.file_datastore.retrieve_artifacts import (
    determine_destination_for_retrieved_artifact,
)
from lsst.daf.butler.datastores.fileDatastoreClient import (
    FileDatastoreGetPayload,
    FileDatastoreGetPayloadFileInfo,
)
from lsst.daf.butler.registry.interfaces import (
    DatabaseInsertMode,
    DatastoreRegistryBridge,
    FakeDatasetRef,
    ReadOnlyDatabaseError,
)
from lsst.daf.butler.repo_relocation import replaceRoot
from lsst.daf.butler.utils import transactional
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import get_class_of, get_full_type_name
from lsst.utils.iteration import chunk_iterable

# For VERBOSE logging usage.
from lsst.utils.logging import VERBOSE, getLogger
from lsst.utils.timer import time_this
from sqlalchemy import BigInteger, String

if TYPE_CHECKING:
    from lsst.daf.butler import LookupKey
    from lsst.daf.butler.registry.interfaces import DatasetIdRef, DatastoreRegistryBridgeManager

log = getLogger(__name__)


class _IngestPrepData(Datastore.IngestPrepData):
    """Helper class for FileDatastore ingest implementation.

    Parameters
    ----------
    datasets : `~collections.abc.Iterable` of `FileDataset`
        Files to be ingested by this datastore.
    """

    def __init__(self, datasets: Iterable[FileDataset]):
        super().__init__(ref for dataset in datasets for ref in dataset.refs)
        self.datasets = datasets


class FileDatastore(GenericBaseDatastore[StoredFileInfo]):
    """Generic Datastore for file-based implementations.

    Should always be sub-classed since key abstract methods are missing.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration as either a `Config` object or URI to file.
    bridgeManager : `DatastoreRegistryBridgeManager`
        Object that manages the interface between `Registry` and datastores.
    root : `ResourcePath`
        Root directory URI of this `Datastore`.
    formatterFactory : `FormatterFactory`
        Factory for creating instances of formatters.
    templates : `FileTemplates`
        File templates that can be used by this `Datastore`.
    composites : `CompositesMap`
        Determines whether a dataset should be disassembled on put.
    trustGetRequest : `bool`
        Determine whether we can fall back to configuration if a requested
        dataset is not known to registry.

    Raises
    ------
    ValueError
        If root location does not exist and ``create`` is `False` in the
        configuration.
    """

    defaultConfigFile: ClassVar[str | None] = None
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

    _retrieve_dataset_method: Callable[[str], DatasetType | None] | None = None
    """Callable that is used in trusted mode to retrieve registry definition
    of a named dataset type.
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
    def makeTableSpec(cls) -> ddl.TableSpec:
        return ddl.TableSpec(
            fields=[
                ddl.FieldSpec(name="dataset_id", dtype=ddl.GUID, primaryKey=True),
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
        config: DatastoreConfig,
        bridgeManager: DatastoreRegistryBridgeManager,
        root: ResourcePath,
        formatterFactory: FormatterFactory,
        templates: FileTemplates,
        composites: CompositesMap,
        trustGetRequest: bool,
    ):
        super().__init__(config, bridgeManager)
        self.root = ResourcePath(root)
        self.formatterFactory = formatterFactory
        self.templates = templates
        self.composites = composites
        self.trustGetRequest = trustGetRequest

        # Name ourselves either using an explicit name or a name
        # derived from the (unexpanded) root
        if "name" in self.config:
            self.name = self.config["name"]
        else:
            # We use the unexpanded root in the name to indicate that this
            # datastore can be moved without having to update registry.
            self.name = "{}@{}".format(type(self).__name__, self.config["root"])

        self.locationFactory = LocationFactory(self.root)

        self._opaque_table_name = self.config["records", "table"]
        try:
            # Storage of paths and formatters, keyed by dataset_id
            self._table = bridgeManager.opaque.register(self._opaque_table_name, self.makeTableSpec())
            # Interface to Registry.
            self._bridge = bridgeManager.register(self.name)
        except ReadOnlyDatabaseError:
            # If the database is read only and we just tried and failed to
            # create a table, it means someone is trying to create a read-only
            # butler client for an empty repo.  That should be okay, as long
            # as they then try to get any datasets before some other client
            # creates the table.  Chances are they're just validating
            # configuration.
            pass

        # Determine whether checksums should be used - default to False
        self.useChecksum = self.config.get("checksum", False)

        # Create a cache manager
        self.cacheManager: AbstractDatastoreCacheManager
        if "cached" in self.config:
            self.cacheManager = DatastoreCacheManager(self.config["cached"], universe=bridgeManager.universe)
        else:
            self.cacheManager = DatastoreDisabledCacheManager("", universe=bridgeManager.universe)

    @classmethod
    def _create_from_config(
        cls,
        config: DatastoreConfig,
        bridgeManager: DatastoreRegistryBridgeManager,
        butlerRoot: ResourcePathExpression | None,
    ) -> FileDatastore:
        if "root" not in config:
            raise ValueError("No root directory specified in configuration")

        # Support repository relocation in config
        # Existence of self.root is checked in subclass
        root = ResourcePath(replaceRoot(config["root"], butlerRoot), forceDirectory=True, forceAbsolute=True)

        # Now associate formatters with storage classes
        formatterFactory = FormatterFactory()
        formatterFactory.registerFormatters(config["formatters"], universe=bridgeManager.universe)

        # Read the file naming templates
        templates = FileTemplates(config["templates"], universe=bridgeManager.universe)

        # See if composites should be disassembled
        composites = CompositesMap(config["composites"], universe=bridgeManager.universe)

        # Determine whether we can fall back to configuration if a
        # requested dataset is not known to registry
        trustGetRequest = config.get("trust_get_request", False)

        self = FileDatastore(
            config, bridgeManager, root, formatterFactory, templates, composites, trustGetRequest
        )

        # Check existence and create directory structure if necessary.
        #
        # The concept of a 'root directory' is problematic for some resource
        # path types that don't necessarily support the concept of a directory
        # (http, s3, gs... basically anything that isn't a local filesystem or
        # WebDAV.)
        # On these resource paths an object representing the
        # "root" directory may not exist even though files under the root do,
        # and in a read-only repository we will be unable to create it.
        # So we only immediately verify the root for local filesystems,
        # the only case where this check will definitely not give a false
        # negative.
        if self.root.isLocal and not self.root.exists():
            if "create" not in self.config or not self.config["create"]:
                raise ValueError(f"No valid root and not allowed to create one at: {self.root}")
            try:
                self.root.mkdir()
            except Exception as e:
                raise ValueError(
                    f"Can not create datastore root '{self.root}', check permissions. Got error: {e}"
                ) from e

        return self

    def clone(self, bridgeManager: DatastoreRegistryBridgeManager) -> Datastore:
        return FileDatastore(
            self.config,
            bridgeManager,
            self.root,
            self.formatterFactory,
            self.templates,
            self.composites,
            self.trustGetRequest,
        )

    def __str__(self) -> str:
        return str(self.root)

    @property
    def bridge(self) -> DatastoreRegistryBridge:
        return self._bridge

    @property
    def roots(self) -> dict[str, ResourcePath | None]:
        # Docstring inherited.
        return {self.name: self.root}

    def _set_trust_mode(self, mode: bool) -> None:
        self.trustGetRequest = mode

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

    def addStoredItemInfo(
        self,
        refs: Iterable[DatasetRef],
        infos: Iterable[StoredFileInfo],
        insert_mode: DatabaseInsertMode = DatabaseInsertMode.INSERT,
    ) -> None:
        """Record internal storage information associated with one or more
        datasets.

        Parameters
        ----------
        refs : sequence of `DatasetRef`
            The datasets that have been stored.
        infos : sequence of `StoredDatastoreItemInfo`
            Metadata associated with the stored datasets.
        insert_mode : `~lsst.daf.butler.registry.interfaces.DatabaseInsertMode`
            Mode to use to insert the new records into the table. The
            options are ``INSERT`` (error if pre-existing), ``REPLACE``
            (replace content with new values), and ``ENSURE`` (skip if the row
            already exists).
        """
        records = [
            info.rebase(ref).to_record(dataset_id=ref.id) for ref, info in zip(refs, infos, strict=True)
        ]
        match insert_mode:
            case DatabaseInsertMode.INSERT:
                self._table.insert(*records, transaction=self._transaction)
            case DatabaseInsertMode.ENSURE:
                self._table.ensure(*records, transaction=self._transaction)
            case DatabaseInsertMode.REPLACE:
                self._table.replace(*records, transaction=self._transaction)
            case _:
                raise ValueError(f"Unknown insert mode of '{insert_mode}'")

    def getStoredItemsInfo(
        self, ref: DatasetIdRef, ignore_datastore_records: bool = False
    ) -> list[StoredFileInfo]:
        """Retrieve information associated with files stored in this
        `Datastore` associated with this dataset ref.

        Parameters
        ----------
        ref : `DatasetRef`
            The dataset that is to be queried.
        ignore_datastore_records : `bool`
            If `True` then do not use datastore records stored in refs.

        Returns
        -------
        items : `~collections.abc.Iterable` [`StoredDatastoreItemInfo`]
            Stored information about the files and associated formatters
            associated with this dataset. Only one file will be returned
            if the dataset has not been disassembled. Can return an empty
            list if no matching datasets can be found.
        """
        # Try to get them from the ref first.
        if ref._datastore_records is not None and not ignore_datastore_records:
            ref_records = ref._datastore_records.get(self._table.name, [])
            # Need to make sure they have correct type.
            for record in ref_records:
                if not isinstance(record, StoredFileInfo):
                    raise TypeError(f"Datastore record has unexpected type {record.__class__.__name__}")
            return cast(list[StoredFileInfo], ref_records)

        # Look for the dataset_id -- there might be multiple matches
        # if we have disassembled the dataset.
        records = self._table.fetch(dataset_id=ref.id)
        return [StoredFileInfo.from_record(record) for record in records]

    def _register_datasets(
        self,
        refsAndInfos: Iterable[tuple[DatasetRef, StoredFileInfo]],
        insert_mode: DatabaseInsertMode = DatabaseInsertMode.INSERT,
    ) -> None:
        """Update registry to indicate that one or more datasets have been
        stored.

        Parameters
        ----------
        refsAndInfos : sequence `tuple` [`DatasetRef`,
                                         `StoredDatastoreItemInfo`]
            Datasets to register and the internal datastore metadata associated
            with them.
        insert_mode : `str`, optional
            Indicate whether the new records should be new ("insert", default),
            or allowed to exists ("ensure") or be replaced if already present
            ("replace").
        """
        expandedRefs: list[DatasetRef] = []
        expandedItemInfos: list[StoredFileInfo] = []

        for ref, itemInfo in refsAndInfos:
            expandedRefs.append(ref)
            expandedItemInfos.append(itemInfo)

        # Dataset location only cares about registry ID so if we have
        # disassembled in datastore we have to deduplicate. Since they
        # will have different datasetTypes we can't use a set
        registryRefs = {r.id: r for r in expandedRefs}
        if insert_mode == DatabaseInsertMode.INSERT:
            self.bridge.insert(registryRefs.values())
        else:
            # There are only two columns and all that matters is the
            # dataset ID.
            self.bridge.ensure(registryRefs.values())
        self.addStoredItemInfo(expandedRefs, expandedItemInfos, insert_mode=insert_mode)

    def _get_stored_records_associated_with_refs(
        self, refs: Iterable[DatasetIdRef], ignore_datastore_records: bool = False
    ) -> dict[DatasetId, list[StoredFileInfo]]:
        """Retrieve all records associated with the provided refs.

        Parameters
        ----------
        refs : iterable of `DatasetIdRef`
            The refs for which records are to be retrieved.
        ignore_datastore_records : `bool`
            If `True` then do not use datastore records stored in refs.

        Returns
        -------
        records : `dict` of [`DatasetId`, `list` of `StoredFileInfo`]
            The matching records indexed by the ref ID.  The number of entries
            in the dict can be smaller than the number of requested refs.
        """
        # Check datastore records in refs first.
        records_by_ref: defaultdict[DatasetId, list[StoredFileInfo]] = defaultdict(list)
        refs_with_no_records = []
        for ref in refs:
            if ignore_datastore_records or ref._datastore_records is None:
                refs_with_no_records.append(ref)
            else:
                if (ref_records := ref._datastore_records.get(self._table.name)) is not None:
                    # Need to make sure they have correct type.
                    for ref_record in ref_records:
                        if not isinstance(ref_record, StoredFileInfo):
                            raise TypeError(
                                f"Datastore record has unexpected type {ref_record.__class__.__name__}"
                            )
                        records_by_ref[ref.id].append(ref_record)

        # If there were any refs without datastore records, check opaque table.
        records = self._table.fetch(dataset_id=[ref.id for ref in refs_with_no_records])

        # Uniqueness is dataset_id + component so can have multiple records
        # per ref.
        for record in records:
            records_by_ref[record["dataset_id"]].append(StoredFileInfo.from_record(record))
        return records_by_ref

    def _refs_associated_with_artifacts(self, paths: list[str | ResourcePath]) -> dict[str, set[DatasetId]]:
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

    def _registered_refs_per_artifact(self, pathInStore: ResourcePath) -> set[DatasetId]:
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
        """Remove information about the file associated with this dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            The dataset that has been removed.
        """
        # Note that this method is actually not used by this implementation,
        # we depend on bridge to delete opaque records. But there are some
        # tests that check that this method works, so we keep it for now.
        self._table.delete(["dataset_id"], {"dataset_id": ref.id})

    def _get_dataset_locations_info(
        self, ref: DatasetIdRef, ignore_datastore_records: bool = False
    ) -> list[DatasetLocationInformation]:
        r"""Find all the `Location`\ s  of the requested dataset in the
        `Datastore` and the associated stored file information.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required `Dataset`.
        ignore_datastore_records : `bool`
            If `True` then do not use datastore records stored in refs.

        Returns
        -------
        results : `list` [`tuple` [`Location`, `StoredFileInfo` ]]
            Location of the dataset within the datastore and
            stored information about each file and its formatter.
        """
        # Get the file information (this will fail if no file)
        records = self.getStoredItemsInfo(ref, ignore_datastore_records)

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

    def _get_expected_dataset_locations_info(self, ref: DatasetRef) -> list[tuple[Location, StoredFileInfo]]:
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

        all_info: list[tuple[Location, Formatter | FormatterV2, StorageClass, str | None]] = []

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
                ),
            )
            for location, formatter, storageClass, component in all_info
        ]

    def _prepare_for_direct_get(
        self, ref: DatasetRef, parameters: Mapping[str, Any] | None = None
    ) -> list[DatastoreFileGetInformation]:
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

        # The storage class we want to use eventually
        refStorageClass = ref.datasetType.storageClass

        # For trusted mode need to reset storage class.
        ref = self._cast_storage_class(ref)

        # Get file metadata and internal metadata
        fileLocations = self._get_dataset_locations_info(ref)
        if not fileLocations:
            if not self.trustGetRequest:
                raise FileNotFoundError(f"Could not retrieve dataset {ref}.")
            # Assume the dataset is where we think it should be
            fileLocations = self._get_expected_dataset_locations_info(ref)

        if len(fileLocations) > 1:
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

        return generate_datastore_get_information(
            fileLocations,
            readStorageClass=refStorageClass,
            ref=ref,
            parameters=parameters,
        )

    def _determine_put_formatter_location(self, ref: DatasetRef) -> tuple[Location, Formatter | FormatterV2]:
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

        location = self.locationFactory.fromPath(template.format(ref), trusted_path=True)

        # Get the formatter based on the storage class
        storageClass = ref.datasetType.storageClass
        try:
            formatter = self.formatterFactory.getFormatter(
                ref,
                FileDescriptor(location, storageClass=storageClass, component=ref.datasetType.component()),
                dataId=ref.dataId,
                ref=ref,
            )
        except KeyError as e:
            raise DatasetTypeNotSupportedError(
                f"Unable to find formatter for {ref} in datastore {self.name}"
            ) from e

        # Now that we know the formatter, update the location
        location = formatter.make_updated_location(location)

        return location, formatter

    def _overrideTransferMode(self, *datasets: FileDataset, transfer: str | None = None) -> str | None:
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

    def _pathInStore(self, path: ResourcePathExpression) -> str | None:
        """Return path relative to datastore root.

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
        pathUri = ResourcePath(path, forceAbsolute=False, forceDirectory=False)
        return pathUri.relative_to(self.root)

    def _standardizeIngestPath(
        self,
        path: str | ResourcePath,
        *,
        transfer: str | None = None,
        check_existence: bool = False,
    ) -> str | ResourcePath:
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
        check_existence : `bool`, optional
            If `True` the existence of the file will be checked, otherwise
            no check will be made.

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
        """
        if transfer not in (None, "direct", "split") + self.root.transferModes:
            raise NotImplementedError(f"Transfer mode {transfer} not supported.")

        # A relative URI indicates relative to datastore root
        srcUri = ResourcePath(path, forceAbsolute=False, forceDirectory=False)
        if not srcUri.isabs():
            srcUri = self.root.join(path)

        if check_existence and not srcUri.exists():
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
        formatter: Formatter | FormatterV2 | type[Formatter | FormatterV2],
        transfer: str | None = None,
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
        srcUri = ResourcePath(path, forceAbsolute=False, forceDirectory=False)

        # Track whether we have read the size of the source yet
        have_sized = False

        tgtLocation: Location | None
        if transfer is None or transfer == "split":
            # A relative path is assumed to be relative to the datastore
            # in this context
            if not srcUri.isabs():
                tgtLocation = self.locationFactory.fromPath(srcUri.ospath, trusted_path=False)
            else:
                # Work out the path in the datastore from an absolute URI
                # This is required to be within the datastore.
                pathInStore = srcUri.relative_to(self.root)
                if pathInStore is None and transfer is None:
                    raise RuntimeError(
                        f"Unexpectedly learned that {srcUri} is not within datastore {self.root}"
                    )
                if pathInStore:
                    tgtLocation = self.locationFactory.fromPath(pathInStore, trusted_path=True)
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
        )

    def _prepIngest(self, *datasets: FileDataset, transfer: str | None = None) -> _IngestPrepData:
        # Docstring inherited from Datastore._prepIngest.
        filtered = []

        # Ingest could be given tens of thousands of files. It is not efficient
        # to check for the existence of every single file (especially if they
        # are remote URIs) but in some transfer modes the files will be checked
        # anyhow when they are relocated. For direct or None transfer modes
        # it is possible to not know if the file is accessible at all.
        # Therefore limit number of files that will be checked (but always
        # include the first one).
        max_checks = 200
        n_datasets = len(datasets)
        if n_datasets <= max_checks:
            check_every_n = 1
        elif transfer in ("direct", None):
            check_every_n = int(n_datasets / max_checks + 1)  # +1 so that if n < max_checks the answer is 1.
        else:
            check_every_n = 0

        for count, dataset in enumerate(datasets):
            acceptable = [ref for ref in dataset.refs if self.constraints.isAcceptable(ref)]
            if not acceptable:
                continue
            else:
                dataset.refs = acceptable
            if dataset.formatter is None:
                dataset.formatter = self.formatterFactory.getFormatterClass(dataset.refs[0])
            else:
                assert isinstance(dataset.formatter, type | str)
                formatter_class = get_class_of(dataset.formatter)
                if not issubclass(formatter_class, Formatter | FormatterV2):
                    raise TypeError(f"Requested formatter {dataset.formatter} is not a Formatter class.")
                dataset.formatter = formatter_class

            # Decide whether the file should be checked.
            check_existence = False
            if check_every_n != 0:
                # First time through count is 0 so we guarantee to check
                # the first file but not necessarily the final one.
                check_existence = count % check_every_n == 0

            if check_existence:
                log.debug(
                    "Checking file existence: %s (%d/%d) [%s]",
                    check_existence,
                    count + 1,
                    n_datasets,
                    transfer,
                )

            dataset.path = self._standardizeIngestPath(
                dataset.path, transfer=transfer, check_existence=check_existence
            )
            filtered.append(dataset)
        return _IngestPrepData(filtered)

    @transactional
    def _finishIngest(
        self,
        prepData: Datastore.IngestPrepData,
        *,
        transfer: str | None = None,
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

        # In direct mode we can allow repeated ingests of the same thing
        # if we are sure that the external dataset is immutable. We use
        # UUIDv5 to indicate this. If there is a mix of v4 and v5 they are
        # separated.
        refs_and_infos_replace = []
        refs_and_infos_insert = []
        if transfer == "direct":
            for entry in refsAndInfos:
                if entry[0].id.version == 5:
                    refs_and_infos_replace.append(entry)
                else:
                    refs_and_infos_insert.append(entry)
        else:
            refs_and_infos_insert = refsAndInfos

        if refs_and_infos_insert:
            self._register_datasets(refs_and_infos_insert, insert_mode=DatabaseInsertMode.INSERT)
        if refs_and_infos_replace:
            self._register_datasets(refs_and_infos_replace, insert_mode=DatabaseInsertMode.REPLACE)

    def _calculate_ingested_datastore_name(
        self,
        srcUri: ResourcePath,
        ref: DatasetRef,
        formatter: Formatter | FormatterV2 | type[Formatter | FormatterV2] | None = None,
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
            No validation of the file extension is performed if the
            ``formatter`` is `None`. This can be used if the caller knows
            that the source URI and target URI will use the same formatter.

        Returns
        -------
        location : `Location`
            Target location for the newly-ingested dataset.
        """
        # Ingesting a file from outside the datastore.
        # This involves a new name.
        template = self.templates.getTemplate(ref)
        location = self.locationFactory.fromPath(template.format(ref), trusted_path=True)

        # Get the extension
        ext = srcUri.getExtension()

        # Update the destination to include that extension
        location.updateExtension(ext)

        # Ask the formatter to validate this extension
        if formatter is not None:
            formatter.validate_extension(location)

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
        # python type, but first we need to make sure the storage class
        # reflects the one defined in the data repository.
        ref = self._cast_storage_class(ref)

        # Confirm that we can accept this dataset
        if not self.constraints.isAcceptable(ref):
            # Raise rather than use boolean return value.
            raise DatasetTypeNotSupportedError(
                f"Dataset {ref} has been rejected by this datastore via configuration."
            )

        location, formatter = self._determine_put_formatter_location(ref)

        # The external storage class can differ from the registry storage
        # class AND the given in-memory dataset might not match any of the
        # storage class definitions.
        if formatter.can_accept(inMemoryDataset):
            # Do not need to coerce. Must assume that the formatter can handle
            # it without further checking of types.
            pass
        else:
            # Coerce to a type that it can accept.
            inMemoryDataset = ref.datasetType.storageClass.coerce_type(inMemoryDataset)
            required_pytype = ref.datasetType.storageClass.pytype

            if not isinstance(inMemoryDataset, required_pytype):
                raise TypeError(
                    f"Inconsistency between supplied object ({type(inMemoryDataset)}) "
                    f"and storage class type ({required_pytype})"
                )

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
            with contextlib.suppress(FileNotFoundError):
                uri.remove()

        # Register a callback to try to delete the uploaded data if
        # something fails below
        self._transaction.registerUndo("artifactWrite", _removeFileExists, uri)

        # Need to record the specified formatter but if this is a V1 formatter
        # we need to convert it to a V2 compatible shim to do the write.
        if not isinstance(formatter, Formatter):
            formatter_compat = formatter
        else:
            formatter_compat = FormatterV1inV2(
                formatter.file_descriptor,
                ref=ref,
                formatter=formatter,
                write_parameters=formatter.write_parameters,
                write_recipes=formatter.write_recipes,
            )

        assert isinstance(formatter_compat, FormatterV2)

        with time_this(log, msg="Writing dataset %s with formatter %s", args=(ref, formatter.name())):
            try:
                formatter_compat.write(inMemoryDataset, cache_manager=self.cacheManager)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to serialize dataset {ref} of type {get_full_type_name(inMemoryDataset)} "
                    f"using formatter {formatter.name()}."
                ) from e

        # URI is needed to resolve what ingest case are we dealing with
        return self._extractIngestInfo(uri, ref, formatter=formatter)

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
        records = self._get_stored_records_associated_with_refs(refs, ignore_datastore_records=True)

        return {ref: ref.id in records for ref in refs}

    def _process_mexists_records(
        self,
        id_to_ref: dict[DatasetId, DatasetRef],
        records: dict[DatasetId, list[StoredFileInfo]],
        all_required: bool,
        artifact_existence: dict[ResourcePath, bool] | None = None,
    ) -> dict[DatasetRef, bool]:
        """Check given records for existence.

        Helper function for `mexists()`.

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
        uris_to_check: list[ResourcePath] = []
        location_map: dict[ResourcePath, DatasetId] = {}

        location_factory = self.locationFactory

        uri_existence: dict[ResourcePath, bool] = {}
        for ref_id, infos in records.items():
            # Key is the dataset Id, value is list of StoredItemInfo
            uris = [info.file_location(location_factory).uri for info in infos]
            location_map.update({uri: ref_id for uri in uris})

            # Check the local cache directly for a dataset corresponding
            # to the remote URI.
            if self.cacheManager.file_count > 0:
                ref = id_to_ref[ref_id]
                for uri, storedFileInfo in zip(uris, infos, strict=True):
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
        dataset_existence: dict[DatasetRef, bool] = {}

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
        self, refs: Iterable[DatasetRef], artifact_existence: dict[ResourcePath, bool] | None = None
    ) -> dict[DatasetRef, bool]:
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
        dataset_existence: dict[DatasetRef, bool] = {}
        log.debug("Checking for the existence of multiple artifacts in datastore in chunks of %d", chunk_size)
        n_found_total = 0
        n_checked = 0
        n_chunks = 0
        for chunk in chunk_iterable(refs, chunk_size=chunk_size):
            chunk_result = self._mexists(chunk, artifact_existence)

            # The log message level and content depend on how many
            # datasets we are processing.
            n_results = len(chunk_result)

            # Use verbose logging to ensure that messages can be seen
            # easily if many refs are being checked.
            log_threshold = VERBOSE
            n_checked += n_results

            # This sum can take some time so only do it if we know the
            # result is going to be used.
            n_found = 0
            if log.isEnabledFor(log_threshold):
                # Can treat the booleans as 0, 1 integers and sum them.
                n_found = sum(chunk_result.values())
                n_found_total += n_found

            # We are deliberately not trying to count the number of refs
            # provided in case it's in the millions. This means there is a
            # situation where the number of refs exactly matches the chunk
            # size and we will switch to the multi-chunk path even though
            # we only have a single chunk.
            if n_results < chunk_size and n_chunks == 0:
                # Single chunk will be processed so we can provide more detail.
                if n_results == 1:
                    ref = list(chunk_result)[0]
                    # Use debug logging to be consistent with `exists()`.
                    log.debug(
                        "Calling mexists() with single ref that does%s exist (%s).",
                        "" if chunk_result[ref] else " not",
                        ref,
                    )
                else:
                    # Single chunk but multiple files. Summarize.
                    log.log(
                        log_threshold,
                        "Number of datasets found in datastore: %d out of %d datasets checked.",
                        n_found,
                        n_checked,
                    )

            else:
                # Use incremental verbose logging when we have multiple chunks.
                log.log(
                    log_threshold,
                    "Number of datasets found in datastore for chunk %d: %d out of %d checked "
                    "(running total from all chunks so far: %d found out of %d checked)",
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
        self, refs: Sequence[DatasetRef], artifact_existence: dict[ResourcePath, bool] | None = None
    ) -> dict[DatasetRef, bool]:
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
        # Make a mapping from refs with the internal storage class to the given
        # refs that may have a different one.  We'll use the internal refs
        # throughout this method and convert back at the very end.
        internal_ref_to_input_ref = {self._cast_storage_class(ref): ref for ref in refs}

        # Need a mapping of dataset_id to (internal) dataset ref since some
        # internal APIs work with dataset_id.
        id_to_ref = {ref.id: ref for ref in internal_ref_to_input_ref}

        # Set of all IDs we are checking for.
        requested_ids = set(id_to_ref.keys())

        # The records themselves. Could be missing some entries.
        records = self._get_stored_records_associated_with_refs(
            id_to_ref.values(), ignore_datastore_records=True
        )

        dataset_existence = self._process_mexists_records(
            id_to_ref, records, True, artifact_existence=artifact_existence
        )

        # Set of IDs that have been handled.
        handled_ids = {ref.id for ref in dataset_existence}

        missing_ids = requested_ids - handled_ids
        if missing_ids:
            dataset_existence.update(
                self._mexists_check_expected(
                    [id_to_ref[missing] for missing in missing_ids], artifact_existence
                )
            )

        return {
            internal_ref_to_input_ref[internal_ref]: existence
            for internal_ref, existence in dataset_existence.items()
        }

    def _mexists_check_expected(
        self, refs: Sequence[DatasetRef], artifact_existence: dict[ResourcePath, bool] | None = None
    ) -> dict[DatasetRef, bool]:
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
        dataset_existence: dict[DatasetRef, bool] = {}
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
                dataset_id = missing_ref.id
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
        ref = self._cast_storage_class(ref)
        # We cannot trust datastore records from ref, as many unit tests delete
        # datasets and check their existence.
        fileLocations = self._get_dataset_locations_info(ref, ignore_datastore_records=True)

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
            If the datastore does not know about the dataset, controls whether
            it should return a predicted URI or not.

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
                uris.componentURIs[component] = ResourcePath(
                    comp_location.uri.geturl() + "#predicted", forceDirectory=comp_location.uri.dirLike
                )

        else:
            location, _ = self._determine_put_formatter_location(ref)

            # Add the "#predicted" URI fragment to indicate this is a guess
            uris.primaryURI = ResourcePath(
                location.uri.geturl() + "#predicted", forceDirectory=location.uri.dirLike
            )

        return uris

    def getManyURIs(
        self,
        refs: Iterable[DatasetRef],
        predict: bool = False,
        allow_missing: bool = False,
    ) -> dict[DatasetRef, DatasetRefURIs]:
        # Docstring inherited

        uris: dict[DatasetRef, DatasetRefURIs] = {}

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
                    raise FileNotFoundError(f"Dataset {ref} not in this datastore.")
            else:
                uris[ref] = self._predict_URIs(ref)

        for ref in existing_refs:
            file_infos = records[ref.id]
            file_locations = [(i.file_location(self.locationFactory), i) for i in file_infos]
            uris[ref] = self._locations_to_URI(ref, file_locations)

        return uris

    def _locations_to_URI(
        self,
        ref: DatasetRef,
        file_locations: Sequence[tuple[Location, StoredFileInfo]],
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
    ) -> list[ResourcePath]:
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
        to_transfer: dict[ResourcePath, ResourcePath] = {}

        for ref in refs:
            locations = self._get_dataset_locations_info(ref)
            for location, _ in locations:
                source_uri = location.uri
                target_uri = determine_destination_for_retrieved_artifact(
                    destination, location.pathInStore, preserve_path
                )
                to_transfer[source_uri] = target_uri

        # In theory can now parallelize the transfer
        log.debug("Number of artifacts to transfer to %s: %d", str(destination), len(to_transfer))
        for source_uri, target_uri in to_transfer.items():
            target_uri.transfer_from(source_uri, transfer=transfer, overwrite=overwrite)

        return list(to_transfer.values())

    def get(
        self,
        ref: DatasetRef,
        parameters: Mapping[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
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

        allGetInfo = self._prepare_for_direct_get(ref, parameters)
        return get_dataset_as_python_object_from_get_info(
            allGetInfo, ref=ref, parameters=parameters, cache_manager=self.cacheManager
        )

    def prepare_get_for_external_client(self, ref: DatasetRef) -> FileDatastoreGetPayload | None:
        # Docstring inherited

        # 1 hour.  Chosen somewhat arbitrarily -- this is long enough that the
        # client should have time to download a large file with retries if
        # needed, but short enough that it will become obvious quickly that
        # these URLs expire.
        # From a strictly technical standpoint there is no reason this
        # shouldn't be a day or more, but there seems to be a political issue
        # where people think there is a risk of end users posting presigned
        # URLs for people without access rights to download.
        url_expiration_time_seconds = 1 * 60 * 60

        locations = self._get_dataset_locations_info(ref)
        if len(locations) == 0:
            return None

        return FileDatastoreGetPayload(
            datastore_type="file",
            file_info=[_to_file_info_payload(info, url_expiration_time_seconds) for info in locations],
        )

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

        self._register_datasets(artifacts, insert_mode=DatabaseInsertMode.INSERT)

    @transactional
    def put_new(self, in_memory_dataset: Any, ref: DatasetRef) -> Mapping[str, DatasetRef]:
        doDisassembly = self.composites.shouldBeDisassembled(ref)
        # doDisassembly = True

        artifacts = []
        if doDisassembly:
            components = ref.datasetType.storageClass.delegate().disassemble(in_memory_dataset)
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
            storedInfo = self._write_in_memory_to_artifact(in_memory_dataset, ref)
            artifacts.append((ref, storedInfo))

        ref_records: DatasetDatastoreRecords = {self._opaque_table_name: [info for _, info in artifacts]}
        ref = ref.replace(datastore_records=ref_records)
        return {self.name: ref}

    @transactional
    def trash(self, ref: DatasetRef | Iterable[DatasetRef], ignore_errors: bool = True) -> None:
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
            id_to_ref = {ref.id: ref for ref in refs}
            existing_ids = self._get_stored_records_associated_with_refs(refs, ignore_datastore_records=True)
            existing_refs = {id_to_ref[ref_id] for ref_id in existing_ids}

            missing = refs - existing_refs
            if missing:
                # Do an explicit existence check on these refs.
                # We only care about the artifacts at this point and not
                # the dataset existence.
                artifact_existence: dict[ResourcePath, bool] = {}
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

        for location, _ in fileLocations:
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
        refs: Collection[DatasetRef],
        transfer: str = "auto",
        artifact_existence: dict[ResourcePath, bool] | None = None,
        dry_run: bool = False,
    ) -> tuple[set[DatasetRef], set[DatasetRef]]:
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
        source_records = source_datastore._get_stored_records_associated_with_refs(
            refs, ignore_datastore_records=True
        )

        # The source dataset_ids are the keys in these records
        source_ids = set(source_records)
        log.debug("Number of datastore records found in source: %d", len(source_ids))

        requested_ids = {ref.id for ref in refs}
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
            log.verbose("Completed scan for missing data files")

        # See if we already have these records
        target_records = self._get_stored_records_associated_with_refs(refs, ignore_datastore_records=True)

        # The artifacts to register
        artifacts = []

        # Refs that already exist
        already_present = []

        # Refs that were rejected by this datastore.
        rejected = set()

        # Refs that were transferred successfully.
        accepted = set()

        # Record each time we have done a "direct" transfer.
        direct_transfers = []

        # Now can transfer the artifacts
        for ref in refs:
            if not self.constraints.isAcceptable(ref):
                # This datastore should not be accepting this dataset.
                rejected.add(ref)
                continue

            accepted.add(ref)

            if ref.id in target_records:
                # Already have an artifact for this.
                already_present.append(ref)
                continue

            # mypy needs to know these are always resolved refs
            for info in source_records[ref.id]:
                source_location = info.file_location(source_datastore.locationFactory)
                target_location = info.file_location(self.locationFactory)
                if source_location == target_location and not source_location.pathInStore.isabs():
                    # Artifact is already in the target location.
                    # (which is how execution butler currently runs)
                    pass
                else:
                    if target_location.pathInStore.isabs():
                        # Just because we can see the artifact when running
                        # the transfer doesn't mean it will be generally
                        # accessible to a user of this butler. Need to decide
                        # what to do about an absolute path.
                        if transfer == "auto":
                            # For "auto" transfers we allow the absolute URI
                            # to be recorded in the target datastore.
                            direct_transfers.append(source_location)
                        else:
                            # The user is explicitly requesting a transfer
                            # even for an absolute URI. This requires us to
                            # calculate the target path.
                            template_ref = ref
                            if info.component:
                                template_ref = ref.makeComponentRef(info.component)
                            target_location = self._calculate_ingested_datastore_name(
                                source_location.uri,
                                template_ref,
                            )

                            info = info.update(path=target_location.pathInStore.path)

                    # Need to transfer it to the new location.
                    # Assume we should always overwrite. If the artifact
                    # is there this might indicate that a previous transfer
                    # was interrupted but was not able to be rolled back
                    # completely (eg pre-emption) so follow Datastore default
                    # and overwrite. Do not copy if we are in dry-run mode.
                    if not dry_run:
                        target_location.uri.transfer_from(
                            source_location.uri,
                            transfer=transfer,
                            overwrite=True,
                            transaction=self._transaction,
                        )

                artifacts.append((ref, info))

        if direct_transfers:
            log.info(
                "Transfer request for an outside-datastore artifact with absolute URI done %d time%s",
                len(direct_transfers),
                "" if len(direct_transfers) == 1 else "s",
            )

        # We are overwriting previous datasets that may have already
        # existed. We therefore should ensure that we force the
        # datastore records to agree. Note that this can potentially lead
        # to difficulties if the dataset has previously been ingested
        # disassembled and is somehow now assembled, or vice versa.
        if not dry_run:
            self._register_datasets(artifacts, insert_mode=DatabaseInsertMode.REPLACE)

        if already_present:
            n_skipped = len(already_present)
            log.info(
                "Skipped transfer of %d dataset%s already present in datastore",
                n_skipped,
                "" if n_skipped == 1 else "s",
            )

        return accepted, rejected

    @transactional
    def forget(self, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited.
        refs = list(refs)
        self.bridge.forget(refs)
        self._table.delete(["dataset_id"], *[{"dataset_id": ref.id} for ref in refs])

    def validateConfiguration(
        self, entities: Iterable[DatasetRef | DatasetType | StorageClass], logFailures: bool = False
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

    def getLookupKeys(self) -> set[LookupKey]:
        # Docstring is inherited from base class
        return (
            self.templates.getLookupKeys()
            | self.formatterFactory.getLookupKeys()
            | self.constraints.getLookupKeys()
        )

    def validateKey(self, lookupKey: LookupKey, entity: DatasetRef | DatasetType | StorageClass) -> None:
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
        directory: ResourcePathExpression | None = None,
        transfer: str | None = "auto",
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
        directoryUri: ResourcePath | None = None
        if directory is not None:
            directoryUri = ResourcePath(directory, forceDirectory=True)

        if transfer is not None and directoryUri is not None and not directoryUri.exists():
            # mypy needs the second test
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
                storeUri = ResourcePath(location.uri, forceDirectory=False)

                # if the datastore has an absolute URI to a resource, we
                # have two options:
                # 1. Keep the absolute URI in the exported YAML
                # 2. Allocate a new name in the local datastore and transfer
                #    it.
                # For now go with option 2
                if location.pathInStore.isabs():
                    template = self.templates.getTemplate(ref)
                    newURI = ResourcePath(template.format(ref), forceAbsolute=False, forceDirectory=False)
                    pathInStore = str(newURI.updatedExtension(location.pathInStore.getExtension()))

                exportUri = directoryUri.join(pathInStore)
                exportUri.transfer_from(storeUri, transfer=transfer)

            yield FileDataset(refs=[ref], path=pathInStore, formatter=storedFileInfo.formatter)

    @staticmethod
    def computeChecksum(uri: ResourcePath, algorithm: str = "blake2b", block_size: int = 8192) -> str | None:
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
            raise NameError(f"The specified algorithm '{algorithm}' is not supported by hashlib")

        if not uri.isLocal:
            return None

        hasher = hashlib.new(algorithm)

        with uri.as_local() as local_uri, open(local_uri.ospath, "rb") as f:
            for chunk in iter(lambda: f.read(block_size), b""):
                hasher.update(chunk)

        return hasher.hexdigest()

    def needs_expanded_data_ids(
        self,
        transfer: str | None,
        entity: DatasetRef | DatasetType | StorageClass | None = None,
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

        self._bridge.insert(FakeDatasetRef(dataset_id) for dataset_id in record_data.records)

        # TODO: Verify that there are no unexpected table names in the dict?
        unpacked_records = []
        for dataset_id, dataset_data in record_data.records.items():
            records = dataset_data.get(self._table.name)
            if records:
                for info in records:
                    assert isinstance(info, StoredFileInfo), "Expecting StoredFileInfo records"
                    unpacked_records.append(info.to_record(dataset_id=dataset_id))
        if unpacked_records:
            self._table.insert(*unpacked_records, transaction=self._transaction)

    def export_records(self, refs: Iterable[DatasetIdRef]) -> Mapping[str, DatastoreRecordData]:
        # Docstring inherited from the base class.
        exported_refs = list(self._bridge.check(refs))
        ids = {ref.id for ref in exported_refs}
        records: dict[DatasetId, dict[str, list[StoredDatastoreItemInfo]]] = {id: {} for id in ids}
        for row in self._table.fetch(dataset_id=ids):
            info: StoredDatastoreItemInfo = StoredFileInfo.from_record(row)
            dataset_records = records.setdefault(row["dataset_id"], {})
            dataset_records.setdefault(self._table.name, []).append(info)

        record_data = DatastoreRecordData(records=records)
        return {self.name: record_data}

    def set_retrieve_dataset_type_method(self, method: Callable[[str], DatasetType | None] | None) -> None:
        # Docstring inherited from the base class.
        self._retrieve_dataset_method = method

    def _cast_storage_class(self, ref: DatasetRef) -> DatasetRef:
        """Update dataset reference to use the storage class from registry."""
        if self._retrieve_dataset_method is None:
            # We could raise an exception here but unit tests do not define
            # this method.
            return ref
        dataset_type = self._retrieve_dataset_method(ref.datasetType.name)
        if dataset_type is not None:
            ref = ref.overrideStorageClass(dataset_type.storageClass)
        return ref

    def get_opaque_table_definitions(self) -> Mapping[str, DatastoreOpaqueTable]:
        # Docstring inherited from the base class.
        return {self._opaque_table_name: DatastoreOpaqueTable(self.makeTableSpec(), StoredFileInfo)}


def _to_file_info_payload(
    info: DatasetLocationInformation, url_expiration_time_seconds: int
) -> FileDatastoreGetPayloadFileInfo:
    location, file_info = info

    # Make sure that we send only relative paths, to avoid leaking
    # details of our configuration to the client.
    path = location.pathInStore
    if path.isabs():
        relative_path = path.relativeToPathRoot
    else:
        relative_path = str(path)

    datastoreRecords = file_info.to_simple()
    datastoreRecords.path = relative_path

    return FileDatastoreGetPayloadFileInfo(
        url=location.uri.generate_presigned_get_url(expiration_time_seconds=url_expiration_time_seconds),
        datastoreRecords=datastoreRecords,
    )
