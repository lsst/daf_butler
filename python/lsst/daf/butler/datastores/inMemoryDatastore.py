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

"""In-memory datastore."""

__all__ = ("StoredMemoryItemInfo", "InMemoryDatastore")

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple, Union
from urllib.parse import urlencode

from lsst.daf.butler import (
    DatasetId,
    DatasetRef,
    DatasetRefURIs,
    DatastoreRecordData,
    StorageClass,
    StoredDatastoreItemInfo,
)
from lsst.daf.butler.core.utils import transactional
from lsst.daf.butler.registry.interfaces import DatastoreRegistryBridge
from lsst.resources import ResourcePath

from .genericDatastore import GenericBaseDatastore

if TYPE_CHECKING:
    from lsst.daf.butler import Config, DatasetType, LookupKey
    from lsst.daf.butler.registry.interfaces import DatasetIdRef, DatastoreRegistryBridgeManager

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class StoredMemoryItemInfo(StoredDatastoreItemInfo):
    """Internal InMemoryDatastore Metadata associated with a stored
    DatasetRef.
    """

    __slots__ = {"timestamp", "storageClass", "parentID", "dataset_id"}

    timestamp: float
    """Unix timestamp indicating the time the dataset was stored."""

    storageClass: StorageClass
    """StorageClass associated with the dataset."""

    parentID: DatasetId
    """ID of the parent `DatasetRef` if this entry is a concrete
    composite. Not used if the dataset being stored is not a
    virtual component of a composite
    """

    dataset_id: DatasetId
    """DatasetId associated with this record."""


class InMemoryDatastore(GenericBaseDatastore):
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
    butlerRoot : `str`, optional
        Unused parameter.

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

    datasets: Dict[DatasetId, Any]
    """Internal storage of datasets indexed by dataset ID."""

    records: Dict[DatasetId, StoredMemoryItemInfo]
    """Internal records about stored datasets."""

    def __init__(
        self,
        config: Union[Config, str],
        bridgeManager: DatastoreRegistryBridgeManager,
        butlerRoot: Optional[str] = None,
    ):
        super().__init__(config, bridgeManager)

        # Name ourselves with the timestamp the datastore
        # was created.
        self.name = "{}@{}".format(type(self).__name__, time.time())
        log.debug("Creating datastore %s", self.name)

        # Storage of datasets, keyed by dataset_id
        self.datasets: Dict[DatasetId, Any] = {}

        # Records is distinct in order to track concrete composite components
        # where we register multiple components for a single dataset.
        self.records: Dict[DatasetId, StoredMemoryItemInfo] = {}

        # Related records that share the same parent
        self.related: Dict[DatasetId, Set[DatasetId]] = {}

        self._bridge = bridgeManager.register(self.name, ephemeral=True)

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

    @property
    def bridge(self) -> DatastoreRegistryBridge:
        # Docstring inherited from GenericBaseDatastore.
        return self._bridge

    def addStoredItemInfo(self, refs: Iterable[DatasetRef], infos: Iterable[StoredMemoryItemInfo]) -> None:
        # Docstring inherited from GenericBaseDatastore.
        for ref, info in zip(refs, infos):
            if ref.id is None:
                raise RuntimeError(f"Can not store unresolved DatasetRef {ref}")
            self.records[ref.id] = info
            self.related.setdefault(info.parentID, set()).add(ref.id)

    def getStoredItemInfo(self, ref: DatasetIdRef) -> StoredMemoryItemInfo:
        # Docstring inherited from GenericBaseDatastore.
        if ref.id is None:
            raise RuntimeError(f"Can not retrieve unresolved DatasetRef {ref}")
        return self.records[ref.id]

    def getStoredItemsInfo(self, ref: DatasetIdRef) -> List[StoredMemoryItemInfo]:
        # Docstring inherited from GenericBaseDatastore.
        return [self.getStoredItemInfo(ref)]

    def removeStoredItemInfo(self, ref: DatasetIdRef) -> None:
        # Docstring inherited from GenericBaseDatastore.
        # If a component has been removed previously then we can sometimes
        # be asked to remove it again. Other datastores ignore this
        # so also ignore here
        if ref.id is None:
            raise RuntimeError(f"Can not remove unresolved DatasetRef {ref}")
        if ref.id not in self.records:
            return
        record = self.records[ref.id]
        del self.records[ref.id]
        self.related[record.parentID].remove(ref.id)

    def _get_dataset_info(self, ref: DatasetIdRef) -> Tuple[DatasetId, StoredMemoryItemInfo]:
        """Check that the dataset is present and return the real ID and
        associated information.

        Parameters
        ----------
        ref : `DatasetRef`
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
            storedItemInfo = self.getStoredItemInfo(ref)
        except KeyError:
            raise FileNotFoundError(f"No such file dataset in memory: {ref}") from None
        realID = ref.id
        if storedItemInfo.parentID is not None:
            realID = storedItemInfo.parentID

        if realID not in self.datasets:
            raise FileNotFoundError(f"No such file dataset in memory: {ref}")

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
            self._get_dataset_info(ref)
        except FileNotFoundError:
            return False
        return True

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

        log.debug("Retrieve %s from %s with parameters %s", ref, self.name, parameters)

        realID, storedItemInfo = self._get_dataset_info(ref)

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
        inMemoryDataset = self._post_process_get(
            inMemoryDataset, refStorageClass, parameters, isComponent=component is not None
        )

        # Last minute type conversion.
        return refStorageClass.coerce_type(inMemoryDataset)

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

        if ref.id is None:
            raise RuntimeError(f"Can not store unresolved DatasetRef {ref}")

        # May need to coerce the in memory dataset to the correct
        # python type, otherwise parameters may not work.
        inMemoryDataset = ref.datasetType.storageClass.coerce_type(inMemoryDataset)

        self._validate_put_parameters(inMemoryDataset, ref)

        self.datasets[ref.id] = inMemoryDataset
        log.debug("Store %s in %s", ref, self.name)

        # Store time we received this content, to allow us to optionally
        # expire it. Instead of storing a filename here, we include the
        # ID of this datasetRef so we can find it from components.
        itemInfo = StoredMemoryItemInfo(
            time.time(), ref.datasetType.storageClass, parentID=ref.id, dataset_id=ref.getCheckedId()
        )

        # We have to register this content with registry.
        # Currently this assumes we have a file so we need to use stub entries
        # TODO: Add to ephemeral part of registry
        self._register_datasets([(ref, itemInfo)])

        if self._transaction is not None:
            self._transaction.registerUndo("put", self.remove, ref)

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

        Notes
        -----
        The URIs returned for in-memory datastores are not usable but
        provide an indication of the associated dataset.
        """

        # Include the dataID as a URI query
        query = urlencode(ref.dataId)

        # if this has never been written then we have to guess
        if not self.exists(ref):
            if not predict:
                raise FileNotFoundError("Dataset {} not in this datastore".format(ref))
            name = f"{ref.datasetType.name}"
            fragment = "#predicted"
        else:
            realID, _ = self._get_dataset_info(ref)
            name = f"{id(self.datasets[realID])}?{query}"
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

    def retrieveArtifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePath,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: Optional[bool] = False,
    ) -> List[ResourcePath]:
        """Retrieve the file artifacts associated with the supplied refs.

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
        self._bridge.forget(refs)
        for ref in refs:
            self.removeStoredItemInfo(ref)

    @transactional
    def trash(self, ref: Union[DatasetRef, Iterable[DatasetRef]], ignore_errors: bool = False) -> None:
        """Indicate to the Datastore that a dataset can be removed.

        Parameters
        ----------
        ref : `DatasetRef` or iterable thereof
            Reference to the required Dataset(s).
        ignore_errors: `bool`, optional
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
        if not isinstance(ref, DatasetRef):
            log.debug("Bulk trashing of datasets in datastore %s", self.name)
            self.bridge.moveToTrash(ref, transaction=self._transaction)
            return

        log.debug("Trash %s in datastore %s", ref, self.name)

        # Check that this dataset is known to datastore
        try:
            self._get_dataset_info(ref)

            # Move datasets to trash table
            self.bridge.moveToTrash([ref], transaction=self._transaction)
        except Exception as e:
            if ignore_errors:
                log.warning(
                    "Error encountered moving dataset %s to trash in datastore %s: %s", ref, self.name, e
                )
            else:
                raise

    def emptyTrash(self, ignore_errors: bool = False) -> None:
        """Remove all datasets from the trash.

        Parameters
        ----------
        ignore_errors : `bool`, optional
            Ignore errors.

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
        with self._bridge.emptyTrash() as trash_data:
            trashed, _ = trash_data
            for ref, _ in trashed:
                try:
                    realID, _ = self._get_dataset_info(ref)
                except FileNotFoundError:
                    # Dataset already removed so ignore it
                    continue
                except Exception as e:
                    if ignore_errors:
                        log.warning(
                            "Emptying trash in datastore %s but encountered an error with dataset %s: %s",
                            self.name,
                            ref.id,
                            e,
                        )
                        continue
                    else:
                        raise

                # Determine whether all references to this dataset have been
                # removed and we can delete the dataset itself
                allRefs = self.related[realID]
                remainingRefs = allRefs - {ref.id}
                if not remainingRefs:
                    log.debug("Removing artifact %s from datastore %s", realID, self.name)
                    del self.datasets[realID]

                # Remove this entry
                self.removeStoredItemInfo(ref)

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
        This method is a no-op.
        """
        return

    def _overrideTransferMode(self, *datasets: Any, transfer: Optional[str] = None) -> Optional[str]:
        # Docstring is inherited from base class
        return transfer

    def validateKey(self, lookupKey: LookupKey, entity: Union[DatasetRef, DatasetType, StorageClass]) -> None:
        # Docstring is inherited from base class
        return

    def getLookupKeys(self) -> Set[LookupKey]:
        # Docstring is inherited from base class
        return self.constraints.getLookupKeys()

    def needs_expanded_data_ids(
        self,
        transfer: Optional[str],
        entity: Optional[Union[DatasetRef, DatasetType, StorageClass]] = None,
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
