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

"""In-memory datastore."""

__all__ = ("StoredMemoryItemInfo", "InMemoryDatastore")

import time
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Any

from lsst.daf.butler import StoredDatastoreItemInfo, StorageClass
from .genericDatastore import GenericBaseDatastore

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class StoredMemoryItemInfo(StoredDatastoreItemInfo):
    """Internal InMemoryDatastore Metadata associated with a stored
    DatasetRef.
    """
    __slots__ = {"timestamp", "storageClass", "parentID"}

    timestamp: float
    """Unix timestamp indicating the time the dataset was stored."""

    storageClass: StorageClass
    """StorageClass associated with the dataset."""

    parentID: Optional[int]
    """ID of the parent `DatasetRef` if this entry is a concrete
    composite. Not used if the dataset being stored is not a
    virtual component of a composite
    """


class InMemoryDatastore(GenericBaseDatastore):
    """Basic Datastore for writing to an in memory cache.

    This datastore is ephemeral in that the contents of the datastore
    disappear when the Python process completes.  This also means that
    other processes can not access this datastore.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration.
    registry : `Registry`, optional
        Unused parameter.
    butlerRoot : `str`, optional
        Unused parameter.

    Notes
    -----
    InMemoryDatastore does not support any file-based ingest.
    """

    defaultConfigFile = "datastores/inMemoryDatastore.yaml"
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    isEphemeral = True
    """A new datastore is created every time and datasets disappear when
    the process shuts down."""

    datasets: Dict[int, Any]
    """Internal storage of datasets indexed by dataset ID."""

    records: Dict[int, StoredMemoryItemInfo]
    """Internal records about stored datasets."""

    def __init__(self, config, registry=None, butlerRoot=None):
        super().__init__(config, registry)

        # Name ourselves with the timestamp the datastore
        # was created.
        self.name = "{}@{}".format(type(self).__name__, time.time())
        log.debug("Creating datastore %s", self.name)

        # Storage of datasets, keyed by dataset_id
        self.datasets = {}

        # Records is distinct in order to track concrete composite components
        # where we register multiple components for a single dataset.
        self.records = {}

        # Related records that share the same parent
        self.related = {}

    @classmethod
    def setConfigRoot(cls, root, config, full, overwrite=True):
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

    def addStoredItemInfo(self, refs, infos):
        # Docstring inherited from GenericBaseDatastore.
        for ref, info in zip(refs, infos):
            self.records[ref.id] = info
            self.related.setdefault(info.parentID, set()).add(ref.id)

    def getStoredItemInfo(self, ref):
        # Docstring inherited from GenericBaseDatastore.
        return self.records[ref.id]

    def getStoredItemsInfo(self, ref):
        # Docstring inherited from GenericBaseDatastore.
        return [self.getStoredItemInfo(ref)]

    def removeStoredItemInfo(self, ref):
        # Docstring inherited from GenericBaseDatastore.
        # If a component has been removed previously then we can sometimes
        # be asked to remove it again. Other datastores ignore this
        # so also ignore here
        if ref.id not in self.records:
            return
        record = self.records[ref.id]
        del self.records[ref.id]
        self.related[record.parentID].remove(ref.id)

    def _get_dataset_info(self, ref):
        """Check that the dataset is present and return the real ID and
        associated information.

        Parameters
        ----------
        ref : `DatasetRef`
            Target `DatasetRef`

        Returns
        -------
        realID : `int`
            The dataset ID associated with this ref that shoul be used. This
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
        try:
            self._get_dataset_info(ref)
        except FileNotFoundError:
            return False
        return True

    def get(self, ref, parameters=None):
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

        log.debug("Retrieve %s from %s with parameters %s", ref, self.name, parameters)

        realID, storedItemInfo = self._get_dataset_info(ref)

        # We have a write storage class and a read storage class and they
        # can be different for concrete composites.
        readStorageClass = ref.datasetType.storageClass
        writeStorageClass = storedItemInfo.storageClass

        # Check that the supplied parameters are suitable for the type read
        readStorageClass.validateParameters(parameters)

        inMemoryDataset = self.datasets[realID]

        component = ref.datasetType.component()

        # Different storage classes implies a component request
        if readStorageClass != writeStorageClass:

            if component is None:
                raise ValueError("Storage class inconsistency ({} vs {}) but no"
                                 " component requested".format(readStorageClass.name,
                                                               writeStorageClass.name))

            # Concrete composite written as a single object (we hope)
            inMemoryDataset = writeStorageClass.assembler().getComponent(inMemoryDataset, component)

        # Since there is no formatter to process parameters, they all must be
        # passed to the assembler.
        return self._post_process_get(inMemoryDataset, readStorageClass, parameters,
                                      isComponent=component is not None)

    def put(self, inMemoryDataset, ref):
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

        self._validate_put_parameters(inMemoryDataset, ref)

        self.datasets[ref.id] = inMemoryDataset
        log.debug("Store %s in %s", ref, self.name)

        # Store time we received this content, to allow us to optionally
        # expire it. Instead of storing a filename here, we include the
        # ID of this datasetRef so we can find it from components.
        itemInfo = StoredMemoryItemInfo(time.time(), ref.datasetType.storageClass,
                                        parentID=ref.id)

        # We have to register this content with registry.
        # Currently this assumes we have a file so we need to use stub entries
        # TODO: Add to ephemeral part of registry
        self._register_datasets([(ref, itemInfo)])

        if self._transaction is not None:
            self._transaction.registerUndo("put", self.remove, ref)

    def getUri(self, ref, predict=False):
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
            URI string pointing to the dataset within the datastore. If the
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

        """

        # if this has never been written then we have to guess
        if not self.exists(ref):
            if not predict:
                raise FileNotFoundError("Dataset {} not in this datastore".format(ref))
            name = "{}#predicted".format(ref.datasetType.name)
        else:
            realID, _ = self._get_dataset_info(ref)
            name = '{}'.format(id(self.datasets[realID]))

        return "mem://{}".format(name)

    def trash(self, ref, ignore_errors=False):
        """Indicate to the Datastore that a dataset can be removed.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
        ignore_errors: `bool`, optional
            Indicate that errors should be ignored.

        Raises
        ------
        FileNotFoundError
            Attempt to remove a dataset that does not exist.

        Notes
        -----
        Concurrency should not normally be an issue for the in memory datastore
        since all internal changes are isolated to solely this process and
        the registry only changes rows associated with this process.
        """

        log.debug("Trash %s in datastore %s", ref, self.name)

        # Check that this dataset is known to datastore
        try:
            self._get_dataset_info(ref)

            # Move datasets to trash table
            self._move_to_trash_in_registry(ref)
        except Exception as e:
            if ignore_errors:
                log.warning("Error encountered moving dataset %s to trash in datastore %s: %s",
                            ref, self.name, e)
            else:
                raise

    def emptyTrash(self, ignore_errors=False):
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
        trashed = self.registry.getTrashedDatasets(self.name)

        for ref in trashed:
            try:
                realID, _ = self._get_dataset_info(ref)
            except Exception as e:
                if ignore_errors:
                    log.warning("Emptying trash in datastore %s but encountered an error with dataset %s: %s",
                                self.name, ref.id, e)
                    continue
                else:
                    raise

            # Determine whether all references to this dataset have been
            # removed and we can delete the dataset itself
            allRefs = self.related[realID]
            theseRefs = {r.id for r in ref.flatten([ref])}
            remainingRefs = allRefs - theseRefs
            if not remainingRefs:
                log.debug("Removing artifact %s from datastore %s", realID, self.name)
                del self.datasets[realID]

            # Remove this entry
            self.removeStoredItemInfo(ref)

        # Inform registry that we have handled these items
        # This should work even if another process is clearing out those rows
        self.registry.emptyDatasetLocationsTrash(self.name, trashed)

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
        This method is a no-op.
        """
        return

    def _overrideTransferMode(self, *datasets: Any, transfer: Optional[str] = None) -> str:
        # Docstring is inherited from base class
        return transfer

    def validateKey(self, lookupKey, entity):
        # Docstring is inherited from base class
        return

    def getLookupKeys(self):
        # Docstring is inherited from base class
        return self.constraints.getLookupKeys()
