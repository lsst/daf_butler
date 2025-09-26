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

"""In-memory datastore."""

from __future__ import annotations

__all__ = ("InMemoryDatastore", "StoredMemoryItemInfo")

import logging
import time
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

from lsst.daf.butler import DatasetId, DatasetRef, StorageClass
from lsst.daf.butler._exceptions import DatasetTypeNotSupportedError
from lsst.daf.butler.datastore import DatasetRefURIs, DatastoreConfig
from lsst.daf.butler.datastore.generic_base import GenericBaseDatastore, post_process_get
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from lsst.daf.butler.datastore.stored_file_info import StoredDatastoreItemInfo
from lsst.daf.butler.utils import transactional
from lsst.resources import ResourcePath, ResourcePathExpression

if TYPE_CHECKING:
    from lsst.daf.butler import Config, DatasetProvenance, DatasetType, LookupKey
    from lsst.daf.butler.datastore import DatastoreOpaqueTable
    from lsst.daf.butler.datastores.file_datastore.retrieve_artifacts import ArtifactIndexInfo
    from lsst.daf.butler.registry.interfaces import DatasetIdRef, DatastoreRegistryBridgeManager

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class StoredMemoryItemInfo(StoredDatastoreItemInfo):
    """Internal InMemoryDatastore Metadata associated with a stored
    DatasetRef.
    """

    timestamp: float
    """Unix timestamp indicating the time the dataset was stored."""

    storageClass: StorageClass
    """StorageClass associated with the dataset."""

    parentID: DatasetId
    """ID of the parent `DatasetRef` if this entry is a concrete
    composite. Not used if the dataset being stored is not a
    virtual component of a composite
    """


class InMemoryDatastore(GenericBaseDatastore[StoredMemoryItemInfo]):
    """Basic Datastore for writing to an in memory cache.

    This datastore is ephemeral in that the contents of the datastore
    disappear when the Python process completes.  This also means that
    other processes can not access this datastore.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration.
    bridgeManager : `DatastoreRegistryBridgeManager`
        Object that manages the interface between `Registry` and datastores.

    Notes
    -----
    InMemoryDatastore does not support any file-based ingest.
    """

    defaultConfigFile = "datastores/inMemoryDatastore.yaml"
    """Path to configuration defaults. Accessed within the ``configs`` resource
    or relative to a search path. Can be None if no defaults specified.
    """

    isEphemeral = True
    """A new datastore is created every time and datasets disappear when
    the process shuts down."""

    datasets: dict[DatasetId, Any]
    """Internal storage of datasets indexed by dataset ID."""

    records: dict[DatasetId, StoredMemoryItemInfo]
    """Internal records about stored datasets."""

    def __init__(
        self,
        config: DatastoreConfig,
        bridgeManager: DatastoreRegistryBridgeManager,
    ):
        super().__init__(config, bridgeManager)

        # Name ourselves with the timestamp the datastore
        # was created.
        self.name = f"{type(self).__name__}@{time.time()}"
        log.debug("Creating datastore %s", self.name)

        # Storage of datasets, keyed by dataset_id
        self.datasets: dict[DatasetId, Any] = {}

        # Records is distinct in order to track concrete composite components
        # where we register multiple components for a single dataset.
        self.records: dict[DatasetId, StoredMemoryItemInfo] = {}

        # Related records that share the same parent
        self.related: dict[DatasetId, set[DatasetId]] = {}

        self._trashedIds: set[DatasetId] = set()

    @classmethod
    def _create_from_config(
        cls,
        config: DatastoreConfig,
        bridgeManager: DatastoreRegistryBridgeManager,
        butlerRoot: ResourcePathExpression | None,
    ) -> InMemoryDatastore:
        return InMemoryDatastore(config, bridgeManager)

    def clone(self, bridgeManager: DatastoreRegistryBridgeManager) -> InMemoryDatastore:
        clone = InMemoryDatastore(self.config, bridgeManager)
        # Sharing these objects is not thread-safe, but this class is only used
        # in single-threaded test code.
        clone.datasets = self.datasets
        clone.records = self.records
        clone.related = self.related
        clone._trashedIds = self._trashedIds
        return clone

    @classmethod
    def setConfigRoot(cls, root: str, config: Config, full: Config, overwrite: bool = True) -> None:
        """Set any filesystem-dependent config options for this Datastore to
        be appropriate for a new empty repository with the given root.

        Does nothing in this implementation.

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
        return

    def _get_stored_item_info(self, dataset_id: DatasetId) -> StoredMemoryItemInfo:
        # Docstring inherited from GenericBaseDatastore.
        return self.records[dataset_id]

    def _remove_stored_item_info(self, dataset_id: DatasetId) -> None:
        # Docstring inherited from GenericBaseDatastore.
        # If a component has been removed previously then we can sometimes
        # be asked to remove it again. Other datastores ignore this
        # so also ignore here
        if dataset_id not in self.records:
            return
        record = self.records[dataset_id]
        del self.records[dataset_id]
        self.related[record.parentID].remove(dataset_id)

    def removeStoredItemInfo(self, ref: DatasetIdRef) -> None:
        """Remove information about the file associated with this dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            The dataset that has been removed.

        Notes
        -----
        This method is actually not used by this implementation, but there are
        some tests that check that this method works, so we keep it for now.
        """
        self._remove_stored_item_info(ref.id)

    def _get_dataset_info(self, dataset_id: DatasetId) -> tuple[DatasetId, StoredMemoryItemInfo]:
        """Check that the dataset is present and return the real ID and
        associated information.

        Parameters
        ----------
        dataset_id : `DatasetRef`
            Target `DatasetRef`

        Returns
        -------
        realID : `int`
            The dataset ID associated with this ref that should be used. This
            could either be the ID of the supplied `DatasetRef` or the parent.
        storageInfo : `StoredMemoryItemInfo`
            Associated storage information.

        Raises
        ------
        FileNotFoundError
            Raised if the dataset is not present in this datastore.
        """
        try:
            storedItemInfo = self._get_stored_item_info(dataset_id)
        except KeyError:
            raise FileNotFoundError(f"No such file dataset in memory: {dataset_id}") from None
        realID = dataset_id
        if storedItemInfo.parentID is not None:
            realID = storedItemInfo.parentID

        if realID not in self.datasets:
            raise FileNotFoundError(f"No such file dataset in memory: {dataset_id}")

        return realID, storedItemInfo

    def knows(self, ref: DatasetRef) -> bool:
        """Check if the dataset is known to the datastore.

        This datastore does not distinguish dataset existence from knowledge
        of a dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required dataset.

        Returns
        -------
        exists : `bool`
            `True` if the dataset is known to the datastore.
        """
        return self.exists(ref)

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
        try:
            self._get_dataset_info(ref.id)
        except FileNotFoundError:
            return False
        return True

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
        log.debug("Retrieve %s from %s with parameters %s", ref, self.name, parameters)

        realID, storedItemInfo = self._get_dataset_info(ref.id)

        # We have a write storage class and a read storage class and they
        # can be different for concrete composites or if overridden.
        if storageClass is not None:
            ref = ref.overrideStorageClass(storageClass)
        refStorageClass = ref.datasetType.storageClass
        writeStorageClass = storedItemInfo.storageClass

        component = ref.datasetType.component()

        # Check that the supplied parameters are suitable for the type read
        # If this is a derived component we validate against the composite
        isDerivedComponent = False
        if component in writeStorageClass.derivedComponents:
            writeStorageClass.validateParameters(parameters)
            isDerivedComponent = True
        else:
            refStorageClass.validateParameters(parameters)

        inMemoryDataset = self.datasets[realID]

        # if this is a read only component we need to apply parameters
        # before we retrieve the component. We assume that the parameters
        # will affect the data globally, before the derived component
        # is selected.
        if isDerivedComponent:
            inMemoryDataset = writeStorageClass.delegate().handleParameters(inMemoryDataset, parameters)
            # Then disable parameters for later
            parameters = {}

        # Check if we have a component.
        if component:
            # In-memory datastore must have stored the dataset as a single
            # object in the write storage class. We therefore use that
            # storage class delegate to obtain the component.
            inMemoryDataset = writeStorageClass.delegate().getComponent(inMemoryDataset, component)

        # Since there is no formatter to process parameters, they all must be
        # passed to the assembler.
        inMemoryDataset = post_process_get(
            inMemoryDataset, refStorageClass, parameters, isComponent=component is not None
        )

        # Last minute type conversion.
        return refStorageClass.coerce_type(inMemoryDataset)

    def put(self, inMemoryDataset: Any, ref: DatasetRef, provenance: DatasetProvenance | None = None) -> None:
        """Write a InMemoryDataset with a given `DatasetRef` to the store.

        Parameters
        ----------
        inMemoryDataset : `object`
            The dataset to store.
        ref : `DatasetRef`
            Reference to the associated Dataset.
        provenance : `DatasetProvenance` or `None`, optional
            Any provenance that should be attached to the serialized dataset.

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
        if not self.constraints.isAcceptable(ref):
            # Raise rather than use boolean return value.
            raise DatasetTypeNotSupportedError(
                f"Dataset {ref} has been rejected by this datastore via configuration."
            )

        # May need to coerce the in memory dataset to the correct
        # python type, otherwise parameters may not work.
        try:
            delegate = ref.datasetType.storageClass.delegate()
        except TypeError:
            # TypeError is raised when a storage class doesn't have a delegate.
            delegate = None
        if not delegate or not delegate.can_accept(inMemoryDataset):
            inMemoryDataset = ref.datasetType.storageClass.coerce_type(inMemoryDataset)

        # Update provenance.
        if delegate:
            inMemoryDataset = delegate.add_provenance(inMemoryDataset, ref, provenance=provenance)

        self.datasets[ref.id] = inMemoryDataset
        log.debug("Store %s in %s", ref, self.name)

        # Store time we received this content, to allow us to optionally
        # expire it. Instead of storing a filename here, we include the
        # ID of this datasetRef so we can find it from components.
        itemInfo = StoredMemoryItemInfo(time.time(), ref.datasetType.storageClass, parentID=ref.id)

        # We have to register this content with registry.
        # Currently this assumes we have a file so we need to use stub entries
        self.records[ref.id] = itemInfo
        self.related.setdefault(itemInfo.parentID, set()).add(ref.id)

        if self._transaction is not None:
            self._transaction.registerUndo("put", self.remove, ref)

    def put_new(self, in_memory_dataset: Any, ref: DatasetRef) -> Mapping[str, DatasetRef]:
        # It is OK to call put() here because registry is not populating
        # bridges as we return empty dict from this method.
        self.put(in_memory_dataset, ref)
        # As ephemeral we return empty dict.
        return {}

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

        Notes
        -----
        The URIs returned for in-memory datastores are not usable but
        provide an indication of the associated dataset.
        """
        # Include the dataID as a URI query
        query = urlencode(ref.dataId.required)

        # if this has never been written then we have to guess
        if not self.exists(ref):
            if not predict:
                raise FileNotFoundError(f"Dataset {ref} not in this datastore")
            name = f"{ref.datasetType.name}"
            fragment = "#predicted"
        else:
            realID, _ = self._get_dataset_info(ref.id)
            name = f"{id(self.datasets[realID])}_{ref.id}"
            fragment = ""

        return DatasetRefURIs(ResourcePath(f"mem://{name}?{query}{fragment}"), {})

    def getURI(self, ref: DatasetRef, predict: bool = False) -> ResourcePath:
        """URI to the Dataset.

        Always uses "mem://" URI prefix.

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
            to the concept of a URI the returned URI string will be
            descriptive. The returned URI is not guaranteed to be obtainable.

        Raises
        ------
        FileNotFoundError
            A URI has been requested for a dataset that does not exist and
            guessing is not allowed.
        AssertionError
            Raised if an internal error occurs.
        """
        primary, _ = self.getURIs(ref, predict)
        if primary is None:
            # This should be impossible since this datastore does
            # not disassemble. This check also helps mypy.
            raise AssertionError(f"Unexpectedly got no URI for in-memory datastore for {ref}")
        return primary

    def ingest_zip(self, zip_path: ResourcePath, transfer: str | None, *, dry_run: bool = False) -> None:
        raise NotImplementedError("Can only ingest a Zip into a file datastore.")

    def retrieveArtifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePath,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool | None = False,
        write_index: bool = True,
        add_prefix: bool = False,
    ) -> dict[ResourcePath, ArtifactIndexInfo]:
        """Retrieve the file artifacts associated with the supplied refs.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets for which artifacts are to be retrieved.
            A single ref can result in multiple artifacts. The refs must
            be resolved.
        destination : `lsst.resources.ResourcePath`
            Location to write the artifacts.
        transfer : `str`, optional
            Method to use to transfer the artifacts. Must be one of the options
            supported by `lsst.resources.ResourcePath.transfer_from()`.
            "move" is not allowed.
        preserve_path : `bool`, optional
            If `True` the full path of the artifact within the datastore
            is preserved. If `False` the final file component of the path
            is used.
        overwrite : `bool`, optional
            If `True` allow transfers to overwrite existing files at the
            destination.
        write_index : `bool`, optional
            If `True` write a file at the top level containing a serialization
            of a `ZipIndex` for the downloaded datasets.
        add_prefix : `bool`, optional
            If `True` and if ``preserve_path`` is `False`, apply a prefix to
            the filenames corresponding to some part of the dataset ref ID.
            This can be used to guarantee uniqueness.

        Notes
        -----
        Not implemented by this datastore.
        """
        # Could conceivably launch a FileDatastore to use formatters to write
        # the data but this is fraught with problems.
        raise NotImplementedError("Can not write artifacts to disk from in-memory datastore.")

    def forget(self, refs: Iterable[DatasetRef]) -> None:
        # Docstring inherited.
        refs = list(refs)
        for ref in refs:
            self._remove_stored_item_info(ref.id)

    @transactional
    def trash(self, ref: DatasetRef | Iterable[DatasetRef], ignore_errors: bool = False) -> None:
        """Indicate to the Datastore that a dataset can be removed.

        Parameters
        ----------
        ref : `DatasetRef` or iterable thereof
            Reference to the required Dataset(s).
        ignore_errors : `bool`, optional
            Indicate that errors should be ignored.

        Raises
        ------
        FileNotFoundError
            Attempt to remove a dataset that does not exist. Only relevant
            if a single dataset ref is given.

        Notes
        -----
        Concurrency should not normally be an issue for the in memory datastore
        since all internal changes are isolated to solely this process and
        the registry only changes rows associated with this process.
        """
        if isinstance(ref, DatasetRef):
            # Check that this dataset is known to datastore
            try:
                self._get_dataset_info(ref.id)
            except Exception as e:
                if ignore_errors:
                    log.warning(
                        "Error encountered moving dataset %s to trash in datastore %s: %s", ref, self.name, e
                    )
                else:
                    raise
            log.debug("Trash %s in datastore %s", ref, self.name)
            ref_list = [ref]
        else:
            ref_list = list(ref)
            log.debug("Bulk trashing of datasets in datastore %s", self.name)

        def _rollbackMoveToTrash(refs: Iterable[DatasetIdRef]) -> None:
            for ref in refs:
                self._trashedIds.remove(ref.id)

        assert self._transaction is not None, "Must be in transaction"
        with self._transaction.undoWith(f"Trash {len(ref_list)} datasets", _rollbackMoveToTrash, ref_list):
            self._trashedIds.update(ref.id for ref in ref_list)

    def emptyTrash(
        self, ignore_errors: bool = False, refs: Collection[DatasetRef] | None = None, dry_run: bool = False
    ) -> set[ResourcePath]:
        """Remove all datasets from the trash.

        Parameters
        ----------
        ignore_errors : `bool`, optional
            Ignore errors.
        refs : `collections.abc.Collection` [ `DatasetRef` ] or `None`
            Explicit list of datasets that can be removed from trash. If listed
            datasets are not already stored in the trash table they will be
            ignored. If `None` every entry in the trash table will be
            processed.
        dry_run : `bool`, optional
            If `True`, the trash table will be queried and results reported
            but no artifacts will be removed.

        Returns
        -------
        removed : `set` [ `lsst.resources.ResourcePath` ]
            List of artifacts that were removed. Empty for this datastore.

        Notes
        -----
        The internal tracking of datasets is affected by this method and
        transaction handling is not supported if there is a problem before
        the datasets themselves are deleted.

        Concurrency should not normally be an issue for the in memory datastore
        since all internal changes are isolated to solely this process and
        the registry only changes rows associated with this process.
        """
        log.debug("Emptying trash in datastore %s", self.name)

        trashed_ids = self._trashedIds
        if refs:
            selected_ids = {ref.id for ref in refs}
            if selected_ids is not None:
                trashed_ids = {tid for tid in trashed_ids if tid in selected_ids}

        if dry_run:
            log.info(
                "Would attempt remove %s dataset%s.", len(trashed_ids), "s" if len(trashed_ids) != 1 else ""
            )
            return set()

        for dataset_id in trashed_ids:
            try:
                realID, _ = self._get_dataset_info(dataset_id)
            except FileNotFoundError:
                # Dataset already removed so ignore it
                continue
            except Exception as e:
                if ignore_errors:
                    log.warning(
                        "Emptying trash in datastore %s but encountered an error with dataset %s: %s",
                        self.name,
                        dataset_id,
                        e,
                    )
                    continue
                else:
                    raise

            # Determine whether all references to this dataset have been
            # removed and we can delete the dataset itself
            allRefs = self.related[realID]
            remainingRefs = allRefs - {dataset_id}
            if not remainingRefs:
                log.debug("Removing artifact %s from datastore %s", realID, self.name)
                del self.datasets[realID]

            # Remove this entry
            self._remove_stored_item_info(dataset_id)

        # Empty the trash table
        self._trashedIds = self._trashedIds - trashed_ids
        return set()

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
        This method is a no-op.
        """
        return

    def _overrideTransferMode(self, *datasets: Any, transfer: str | None = None) -> str | None:
        # Docstring is inherited from base class
        return transfer

    def validateKey(self, lookupKey: LookupKey, entity: DatasetRef | DatasetType | StorageClass) -> None:
        # Docstring is inherited from base class
        return

    def getLookupKeys(self) -> set[LookupKey]:
        # Docstring is inherited from base class
        return self.constraints.getLookupKeys()

    def needs_expanded_data_ids(
        self,
        transfer: str | None,
        entity: DatasetRef | DatasetType | StorageClass | None = None,
    ) -> bool:
        # Docstring inherited.
        return False

    def import_records(self, data: Mapping[str, DatastoreRecordData]) -> None:
        # Docstring inherited from the base class.
        return

    def export_records(self, refs: Iterable[DatasetIdRef]) -> Mapping[str, DatastoreRecordData]:
        # Docstring inherited from the base class.

        # In-memory Datastore records cannot be exported or imported
        return {}

    def export_predicted_records(self, refs: Iterable[DatasetIdRef]) -> dict[str, DatastoreRecordData]:
        # Docstring inherited from the base class.

        # In-memory Datastore records cannot be exported or imported
        return {}

    def get_opaque_table_definitions(self) -> Mapping[str, DatastoreOpaqueTable]:
        # Docstring inherited from the base class.
        return {}
