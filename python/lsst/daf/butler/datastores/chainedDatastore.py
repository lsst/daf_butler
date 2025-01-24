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

"""Chained datastore."""

from __future__ import annotations

__all__ = ("ChainedDatastore",)

import itertools
import logging
import time
import warnings
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from lsst.daf.butler import DatasetRef, DatasetTypeNotSupportedError, DimensionUniverse, FileDataset
from lsst.daf.butler.datastore import (
    DatasetRefURIs,
    Datastore,
    DatastoreConfig,
    DatastoreOpaqueTable,
    DatastoreValidationError,
)
from lsst.daf.butler.datastore.constraints import Constraints
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from lsst.daf.butler.datastores.file_datastore.retrieve_artifacts import ArtifactIndexInfo, ZipIndex
from lsst.resources import ResourcePath
from lsst.utils import doImportType

if TYPE_CHECKING:
    from lsst.daf.butler import Config, DatasetProvenance, DatasetType, LookupKey, StorageClass
    from lsst.daf.butler.registry.interfaces import DatasetIdRef, DatastoreRegistryBridgeManager
    from lsst.resources import ResourcePathExpression

log = logging.getLogger(__name__)


class _IngestPrepData(Datastore.IngestPrepData):
    """Helper class for ChainedDatastore ingest implementation.

    Parameters
    ----------
    children : `list` of `tuple`
        Pairs of `Datastore`, `IngestPrepData` for all child datastores.
    """

    def __init__(self, children: list[tuple[Datastore, Datastore.IngestPrepData, set[ResourcePath]]]):
        super().__init__(itertools.chain.from_iterable(data.refs.values() for _, data, _ in children))
        self.children = children


class ChainedDatastore(Datastore):
    """Chained Datastores to allow read and writes from multiple datastores.

    A ChainedDatastore is configured with multiple datastore configurations.
    A ``put()`` is always sent to each datastore. A ``get()``
    operation is sent to each datastore in turn and the first datastore
    to return a valid dataset is used.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration.  This configuration must include a ``datastores`` field
        as a sequence of datastore configurations.  The order in this sequence
        indicates the order to use for read operations.
    bridgeManager : `DatastoreRegistryBridgeManager`
        Object that manages the interface between `Registry` and datastores.
    datastores : `list` [`Datastore`]
        All the child datastores known to this datastore.

    Notes
    -----
    ChainedDatastore never supports `None` or `"move"` as an `ingest` transfer
    mode.  It supports `"copy"`, `"symlink"`, `"relsymlink"`
    and `"hardlink"` if and only if all its child datastores do.
    """

    defaultConfigFile = "datastores/chainedDatastore.yaml"
    """Path to configuration defaults. Accessed within the ``configs`` resource
    or relative to a search path. Can be None if no defaults specified.
    """

    containerKey = "datastores"
    """Key to specify where child datastores are configured."""

    datastores: list[Datastore]
    """All the child datastores known to this datastore."""

    datastoreConstraints: Sequence[Constraints | None]
    """Constraints to be applied to each of the child datastores."""

    universe: DimensionUniverse
    """Dimension universe associated with the butler."""

    @classmethod
    def setConfigRoot(cls, root: str, config: Config, full: Config, overwrite: bool = True) -> None:
        """Set any filesystem-dependent config options for child Datastores to
        be appropriate for a new empty repository with the given root.

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
        # Extract the part of the config we care about updating
        datastoreConfig = DatastoreConfig(config, mergeDefaults=False)

        # And the subset of the full config that we can use for reference.
        # Do not bother with defaults because we are told this already has
        # them.
        fullDatastoreConfig = DatastoreConfig(full, mergeDefaults=False)

        # Loop over each datastore config and pass the subsets to the
        # child datastores to process.

        containerKey = cls.containerKey
        for idx, (child, fullChild) in enumerate(
            zip(datastoreConfig[containerKey], fullDatastoreConfig[containerKey], strict=True)
        ):
            childConfig = DatastoreConfig(child, mergeDefaults=False)
            fullChildConfig = DatastoreConfig(fullChild, mergeDefaults=False)
            datastoreClass = doImportType(fullChildConfig["cls"])
            if not issubclass(datastoreClass, Datastore):
                raise TypeError(f"Imported child class {fullChildConfig['cls']} is not a Datastore")
            newroot = f"{root}/{datastoreClass.__qualname__}_{idx}"
            datastoreClass.setConfigRoot(newroot, childConfig, fullChildConfig, overwrite=overwrite)

            # Reattach to parent
            datastoreConfig[containerKey, idx] = childConfig

        # Reattach modified datastore config to parent
        # If this has a datastore key we attach there, otherwise we assume
        # this information goes at the top of the config hierarchy.
        if DatastoreConfig.component in config:
            config[DatastoreConfig.component] = datastoreConfig
        else:
            config.update(datastoreConfig)

        return

    def __init__(
        self,
        config: DatastoreConfig,
        bridgeManager: DatastoreRegistryBridgeManager,
        datastores: list[Datastore],
    ):
        super().__init__(config, bridgeManager)

        self.datastores = list(datastores)

        # Name ourself based on our children
        if self.datastores:
            # We must set the names explicitly
            self._names = [d.name for d in self.datastores]
            childNames = ",".join(self.names)
        else:
            childNames = f"(empty@{time.time()})"
            self._names = [childNames]
        self.name = f"{type(self).__qualname__}[{childNames}]"

        # We declare we are ephemeral if all our child datastores declare
        # they are ephemeral
        self.isEphemeral = all(d.isEphemeral for d in self.datastores)

        # per-datastore override constraints
        if "datastore_constraints" in self.config:
            overrides = self.config["datastore_constraints"]

            if len(overrides) != len(self.datastores):
                raise DatastoreValidationError(
                    f"Number of registered datastores ({len(self.datastores)})"
                    " differs from number of constraints overrides"
                    f" {len(overrides)}"
                )

            self.datastoreConstraints = [
                Constraints(c.get("constraints"), universe=bridgeManager.universe) for c in overrides
            ]

        else:
            self.datastoreConstraints = (None,) * len(self.datastores)

        self.universe = bridgeManager.universe

        log.debug("Created %s (%s)", self.name, ("ephemeral" if self.isEphemeral else "permanent"))

    @classmethod
    def _create_from_config(
        cls,
        config: DatastoreConfig,
        bridgeManager: DatastoreRegistryBridgeManager,
        butlerRoot: ResourcePathExpression | None,
    ) -> ChainedDatastore:
        # Scan for child datastores and instantiate them with the same registry
        datastores = []
        for c in config["datastores"]:
            c = DatastoreConfig(c)
            datastoreType = doImportType(c["cls"])
            if not issubclass(datastoreType, Datastore):
                raise TypeError(f"Imported child class {c['cls']} is not a Datastore")
            datastore = datastoreType._create_from_config(c, bridgeManager, butlerRoot=butlerRoot)
            log.debug("Creating child datastore %s", datastore.name)
            datastores.append(datastore)

        return ChainedDatastore(config, bridgeManager, datastores)

    def clone(self, bridgeManager: DatastoreRegistryBridgeManager) -> Datastore:
        datastores = [ds.clone(bridgeManager) for ds in self.datastores]
        return ChainedDatastore(self.config, bridgeManager, datastores)

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._names)

    @property
    def roots(self) -> dict[str, ResourcePath | None]:
        # Docstring inherited.
        roots = {}
        for datastore in self.datastores:
            roots.update(datastore.roots)
        return roots

    def __str__(self) -> str:
        chainName = ", ".join(str(ds) for ds in self.datastores)
        return chainName

    def _set_trust_mode(self, mode: bool) -> None:
        for datastore in self.datastores:
            datastore._set_trust_mode(mode)

    def knows(self, ref: DatasetRef) -> bool:
        """Check if the dataset is known to any of the datastores.

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
        for datastore in self.datastores:
            if datastore.knows(ref):
                log.debug("%s known to datastore %s", ref, datastore.name)
                return True
        return False

    def knows_these(self, refs: Iterable[DatasetRef]) -> dict[DatasetRef, bool]:
        # Docstring inherited from the base class.
        refs_known: dict[DatasetRef, bool] = {}
        for datastore in self.datastores:
            refs_known.update(datastore.knows_these(refs))

            # No need to check in next datastore for refs that are known.
            # We only update entries that were initially False.
            refs = [ref for ref, known in refs_known.items() if not known]

        return refs_known

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
            Mapping from dataset to boolean indicating existence in any
            of the child datastores.
        """
        dataset_existence: dict[DatasetRef, bool] = {}
        for datastore in self.datastores:
            dataset_existence.update(datastore.mexists(refs, artifact_existence=artifact_existence))

            # For next datastore no point asking about ones we know
            # exist already. No special exemption for ephemeral datastores.
            refs = [ref for ref, exists in dataset_existence.items() if not exists]

        return dataset_existence

    def exists(self, ref: DatasetRef) -> bool:
        """Check if the dataset exists in one of the datastores.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required dataset.

        Returns
        -------
        exists : `bool`
            `True` if the entity exists in one of the child datastores.
        """
        for datastore in self.datastores:
            if datastore.exists(ref):
                log.debug("Found %s in datastore %s", ref, datastore.name)
                return True
        return False

    def get(
        self,
        ref: DatasetRef,
        parameters: Mapping[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
    ) -> Any:
        """Load an InMemoryDataset from the store.

        The dataset is returned from the first datastore that has
        the dataset.

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
        for datastore in self.datastores:
            try:
                inMemoryObject = datastore.get(ref, parameters, storageClass=storageClass)
                log.debug("Found dataset %s in datastore %s", ref, datastore.name)
                return inMemoryObject
            except FileNotFoundError:
                pass

        raise FileNotFoundError(f"Dataset {ref} could not be found in any of the datastores")

    def prepare_get_for_external_client(self, ref: DatasetRef) -> object | None:
        datastore = self._get_matching_datastore(ref)
        if datastore is None:
            return None

        return datastore.prepare_get_for_external_client(ref)

    def _get_matching_datastore(self, ref: DatasetRef) -> Datastore | None:
        """Return the first child datastore that owns the specified dataset."""
        for datastore in self.datastores:
            if datastore.knows(ref):
                return datastore

        return None

    def put(self, inMemoryDataset: Any, ref: DatasetRef, provenance: DatasetProvenance | None = None) -> None:
        """Write a InMemoryDataset with a given `DatasetRef` to each
        datastore.

        The put() to child datastores can fail with
        `DatasetTypeNotSupportedError`.  The put() for this datastore will be
        deemed to have succeeded so long as at least one child datastore
        accepted the inMemoryDataset.

        Parameters
        ----------
        inMemoryDataset : `object`
            The dataset to store.
        ref : `DatasetRef`
            Reference to the associated Dataset.
        provenance : `DatasetProvenance` or `None`, optional
            Any provenance that should be attached to the serialized dataset.
            Not supported by all serialization mechanisms.

        Raises
        ------
        TypeError
            Supplied object and storage class are inconsistent.
        DatasetTypeNotSupportedError
            All datastores reported `DatasetTypeNotSupportedError`.
        """
        log.debug("Put %s", ref)

        # Confirm that we can accept this dataset
        if not self.constraints.isAcceptable(ref):
            # Raise rather than use boolean return value.
            raise DatasetTypeNotSupportedError(
                f"Dataset {ref} has been rejected by this datastore via configuration."
            )

        isPermanent = False
        nsuccess = 0
        npermanent = 0
        nephemeral = 0
        for datastore, constraints in zip(self.datastores, self.datastoreConstraints, strict=True):
            if (
                constraints is not None and not constraints.isAcceptable(ref)
            ) or not datastore.constraints.isAcceptable(ref):
                log.debug("Datastore %s skipping put via configuration for ref %s", datastore.name, ref)
                continue

            if datastore.isEphemeral:
                nephemeral += 1
            else:
                npermanent += 1
            try:
                datastore.put(inMemoryDataset, ref, provenance=provenance)
                nsuccess += 1
                if not datastore.isEphemeral:
                    isPermanent = True
            except DatasetTypeNotSupportedError:
                pass

        if nsuccess == 0:
            raise DatasetTypeNotSupportedError(f"None of the chained datastores supported ref {ref}")

        if not isPermanent and npermanent > 0:
            warnings.warn(f"Put of {ref} only succeeded in ephemeral databases", stacklevel=2)

        if self._transaction is not None:
            self._transaction.registerUndo("put", self.remove, ref)

    def put_new(self, in_memory_dataset: Any, ref: DatasetRef) -> Mapping[str, DatasetRef]:
        # Docstring inherited from base class.
        log.debug("Put %s", ref)

        # Confirm that we can accept this dataset
        if not self.constraints.isAcceptable(ref):
            # Raise rather than use boolean return value.
            raise DatasetTypeNotSupportedError(
                f"Dataset {ref} has been rejected by this datastore via configuration."
            )

        isPermanent = False
        nsuccess = 0
        npermanent = 0
        nephemeral = 0
        stored_refs: dict[str, DatasetRef] = {}
        for datastore, constraints in zip(self.datastores, self.datastoreConstraints, strict=True):
            if (
                constraints is not None and not constraints.isAcceptable(ref)
            ) or not datastore.constraints.isAcceptable(ref):
                log.debug("Datastore %s skipping put via configuration for ref %s", datastore.name, ref)
                continue

            if datastore.isEphemeral:
                nephemeral += 1
            else:
                npermanent += 1
            try:
                stored_ref_map = datastore.put_new(in_memory_dataset, ref)
                stored_refs.update(stored_ref_map)
                nsuccess += 1
                if not datastore.isEphemeral:
                    isPermanent = True
            except DatasetTypeNotSupportedError:
                pass

        if nsuccess == 0:
            raise DatasetTypeNotSupportedError(f"None of the chained datastores supported ref {ref}")

        if not isPermanent and npermanent > 0:
            warnings.warn(f"Put of {ref} only succeeded in ephemeral databases", stacklevel=2)

        if self._transaction is not None:
            self._transaction.registerUndo("put", self.remove, ref)

        return stored_refs

    def _overrideTransferMode(self, *datasets: Any, transfer: str | None = None) -> str | None:
        # Docstring inherited from base class.
        if transfer != "auto":
            return transfer
        # Ask each datastore what they think auto means
        transfers = {d._overrideTransferMode(*datasets, transfer=transfer) for d in self.datastores}

        # Remove any untranslated "auto" values
        transfers.discard(transfer)

        if len(transfers) == 1:
            return transfers.pop()
        if not transfers:
            # Everything reported "auto"
            return transfer

        raise RuntimeError(
            "Chained datastore does not yet support different transfer modes"
            f" from 'auto' in each child datastore (wanted {transfers})"
        )

    def _prepIngest(self, *datasets: FileDataset, transfer: str | None = None) -> _IngestPrepData:
        # Docstring inherited from Datastore._prepIngest.
        if transfer is None:
            raise NotImplementedError("ChainedDatastore does not support transfer=None.")

        def isDatasetAcceptable(dataset: FileDataset, *, name: str, constraints: Constraints) -> bool:
            acceptable = [ref for ref in dataset.refs if constraints.isAcceptable(ref)]
            if not acceptable:
                log.debug(
                    "Datastore %s skipping ingest via configuration for refs %s",
                    name,
                    ", ".join(str(ref) for ref in dataset.refs),
                )
                return False
            else:
                return True

        # Filter down to just datasets the chained datastore's own
        # configuration accepts.
        okForParent: list[FileDataset] = [
            dataset
            for dataset in datasets
            if isDatasetAcceptable(dataset, name=self.name, constraints=self.constraints)
        ]

        # Iterate over nested datastores and call _prepIngest on each.
        # Save the results to a list:
        children: list[tuple[Datastore, Datastore.IngestPrepData, set[ResourcePath]]] = []
        # ...and remember whether all of the failures are due to
        # NotImplementedError being raised.
        allFailuresAreNotImplementedError = True
        for datastore, constraints in zip(self.datastores, self.datastoreConstraints, strict=True):
            okForChild: list[FileDataset]
            if constraints is not None:
                okForChild = [
                    dataset
                    for dataset in okForParent
                    if isDatasetAcceptable(dataset, name=datastore.name, constraints=constraints)
                ]
            else:
                okForChild = okForParent
            try:
                prepDataForChild = datastore._prepIngest(*okForChild, transfer=transfer)
            except NotImplementedError:
                log.debug(
                    "Skipping ingest for datastore %s because transfer mode %s is not supported.",
                    datastore.name,
                    transfer,
                )
                continue
            allFailuresAreNotImplementedError = False
            if okForChild:
                # Do not store for later if a datastore has rejected
                # everything.
                # Include the source paths if this is a "move". It's clearer
                # to find the paths now rather than try to infer how
                # each datastore has stored them in the internal prep class.
                paths = (
                    {ResourcePath(dataset.path, forceDirectory=False) for dataset in okForChild}
                    if transfer == "move"
                    else set()
                )
                children.append((datastore, prepDataForChild, paths))
        if allFailuresAreNotImplementedError:
            raise NotImplementedError(f"No child datastore supports transfer mode {transfer}.")
        return _IngestPrepData(children=children)

    def _finishIngest(
        self,
        prepData: _IngestPrepData,
        *,
        transfer: str | None = None,
        record_validation_info: bool = True,
    ) -> None:
        # Docstring inherited from Datastore._finishIngest.
        # For "move" we must use "copy" and then delete the input
        # data at the end. This has no rollback option if the ingest
        # subsequently fails. If there is only one active datastore
        # accepting any files we can leave it as "move"
        actual_transfer: str | None
        if transfer == "move" and len(prepData.children) > 1:
            actual_transfer = "copy"
        else:
            actual_transfer = transfer
        to_be_deleted: set[ResourcePath] = set()
        for datastore, prepDataForChild, paths in prepData.children:
            datastore._finishIngest(
                prepDataForChild, transfer=actual_transfer, record_validation_info=record_validation_info
            )
            to_be_deleted.update(paths)
        if actual_transfer != transfer:
            # These datasets were copied but now need to be deleted.
            # This can not be rolled back.
            for uri in to_be_deleted:
                uri.remove()

    def getManyURIs(
        self,
        refs: Iterable[DatasetRef],
        predict: bool = False,
        allow_missing: bool = False,
    ) -> dict[DatasetRef, DatasetRefURIs]:
        # Docstring inherited

        uris: dict[DatasetRef, DatasetRefURIs] = {}
        ephemeral_uris: dict[DatasetRef, DatasetRefURIs] = {}

        missing_refs = set(refs)

        # If predict is True we don't want to predict a dataset in the first
        # datastore if it actually exists in a later datastore, so in that
        # case check all datastores with predict=False first, and then try
        # again with predict=True.
        for p in (False, True) if predict else (False,):
            if not missing_refs:
                break
            for datastore in self.datastores:
                try:
                    got_uris = datastore.getManyURIs(missing_refs, p, allow_missing=True)
                except NotImplementedError:
                    # some datastores may not implement generating URIs
                    continue
                if datastore.isEphemeral:
                    # Only use these as last resort so do not constrain
                    # subsequent queries.
                    ephemeral_uris.update(got_uris)
                    continue

                missing_refs -= got_uris.keys()
                uris.update(got_uris)
                if not missing_refs:
                    break

        if missing_refs and ephemeral_uris:
            ephemeral_refs = missing_refs.intersection(ephemeral_uris.keys())
            for ref in ephemeral_refs:
                uris[ref] = ephemeral_uris[ref]
                missing_refs.remove(ref)

        if missing_refs and not allow_missing:
            raise FileNotFoundError(f"Dataset(s) {missing_refs} not in this datastore.")

        return uris

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
        The returned URI is from the first datastore in the list that has
        the dataset with preference given to the first dataset coming from
        a permanent datastore. If no datastores have the dataset and prediction
        is allowed, the predicted URI for the first datastore in the list will
        be returned.
        """
        log.debug("Requesting URIs for %s", ref)
        predictedUri: DatasetRefURIs | None = None
        predictedEphemeralUri: DatasetRefURIs | None = None
        firstEphemeralUri: DatasetRefURIs | None = None
        for datastore in self.datastores:
            if datastore.exists(ref):
                if not datastore.isEphemeral:
                    uri = datastore.getURIs(ref)
                    log.debug("Retrieved non-ephemeral URI: %s", uri)
                    return uri
                elif not firstEphemeralUri:
                    firstEphemeralUri = datastore.getURIs(ref)
            elif predict:
                if not predictedUri and not datastore.isEphemeral:
                    predictedUri = datastore.getURIs(ref, predict)
                elif not predictedEphemeralUri and datastore.isEphemeral:
                    predictedEphemeralUri = datastore.getURIs(ref, predict)

        if firstEphemeralUri:
            log.debug("Retrieved ephemeral URI: %s", firstEphemeralUri)
            return firstEphemeralUri

        if predictedUri:
            log.debug("Retrieved predicted URI: %s", predictedUri)
            return predictedUri

        if predictedEphemeralUri:
            log.debug("Retrieved predicted ephemeral URI: %s", predictedEphemeralUri)
            return predictedEphemeralUri

        raise FileNotFoundError(f"Dataset {ref} not in any datastore")

    def getURI(self, ref: DatasetRef, predict: bool = False) -> ResourcePath:
        """URI to the Dataset.

        The returned URI is from the first datastore in the list that has
        the dataset with preference given to the first dataset coming from
        a permanent datastore. If no datastores have the dataset and prediction
        is allowed, the predicted URI for the first datastore in the list will
        be returned.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required Dataset.
        predict : `bool`
            If `True`, allow URIs to be returned of datasets that have not
            been written.

        Returns
        -------
        uri : `lsst.resources.ResourcePath`
            URI pointing to the dataset within the datastore. If the
            dataset does not exist in the datastore, and if ``predict`` is
            `True`, the URI will be a prediction and will include a URI
            fragment "#predicted".

        Notes
        -----
        If the datastore does not have entities that relate well
        to the concept of a URI the returned URI string will be
        descriptive. The returned URI is not guaranteed to be obtainable.

        Raises
        ------
        FileNotFoundError
            A URI has been requested for a dataset that does not exist and
            guessing is not allowed.
        RuntimeError
            Raised if a request is made for a single URI but multiple URIs
            are associated with this dataset.
        """
        log.debug("Requesting URI for %s", ref)
        primary, components = self.getURIs(ref, predict)
        if primary is None or components:
            raise RuntimeError(
                f"Dataset ({ref}) includes distinct URIs for components. Use Datastore.getURIs() instead."
            )
        return primary

    def retrieveArtifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePath,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool = False,
        write_index: bool = True,
        add_prefix: bool = False,
    ) -> dict[ResourcePath, ArtifactIndexInfo]:
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
        write_index : `bool`, optional
            If `True` write a file at the top level containing a serialization
            of a `ZipIndex` for the downloaded datasets.
        add_prefix : `bool`, optional
            Add a prefix based on the DatasetId. Only used if ``preserve_path``
            is `False`.

        Returns
        -------
        artifact_map : `dict` [ `lsst.resources.ResourcePath`, \
                `ArtifactIndexInfo` ]
            Mapping of retrieved file to associated index information.
        """
        if not destination.isdir():
            raise ValueError(f"Destination location must refer to a directory. Given {destination}")

        # Using getURIs is not feasible since it becomes difficult to
        # determine the path within the datastore later on. For now
        # follow getURIs implementation approach.

        pending = set(refs)

        # There is a question as to whether an exception should be raised
        # early if some of the refs are missing, or whether files should be
        # transferred until a problem is hit. Prefer to complain up front.
        # Use the datastore integer as primary key.
        grouped_by_datastore: dict[int, set[DatasetRef]] = {}

        for number, datastore in enumerate(self.datastores):
            if datastore.isEphemeral:
                # In the future we will want to distinguish in-memory from
                # caching datastore since using an on-disk local
                # cache is exactly what we should be doing.
                continue
            try:
                # Checking file existence is expensive. Have the option
                # of checking whether the datastore knows of these datasets
                # instead, which is fast but can potentially lead to
                # retrieveArtifacts failing.
                knows = datastore.knows_these(pending)
                datastore_refs = {ref for ref, exists in knows.items() if exists}
            except NotImplementedError:
                # Some datastores may not support retrieving artifacts
                continue

            if datastore_refs:
                grouped_by_datastore[number] = datastore_refs

                # Remove these from the pending list so that we do not bother
                # looking for them any more.
                pending = pending - datastore_refs

        if pending:
            raise RuntimeError(f"Some datasets were not found in any datastores: {pending}")

        # Now do the transfer.
        merged_artifact_map: dict[ResourcePath, ArtifactIndexInfo] = {}
        for number, datastore_refs in grouped_by_datastore.items():
            artifact_map = self.datastores[number].retrieveArtifacts(
                datastore_refs,
                destination,
                transfer=transfer,
                preserve_path=preserve_path,
                overwrite=overwrite,
                write_index=False,  # Disable index writing regardless.
                add_prefix=add_prefix,
            )
            merged_artifact_map.update(artifact_map)

        if write_index:
            index = ZipIndex.from_artifact_map(refs, merged_artifact_map, destination)
            index.write_index(destination)

        return merged_artifact_map

    def ingest_zip(self, zip_path: ResourcePath, transfer: str | None) -> None:
        """Ingest an indexed Zip file and contents.

        The Zip file must have an index file as created by `retrieveArtifacts`.

        Parameters
        ----------
        zip_path : `lsst.resources.ResourcePath`
            Path to the Zip file.
        transfer : `str`
            Method to use for transferring the Zip file into the datastore.

        Notes
        -----
        Datastore constraints are bypassed with Zip ingest. A zip file can
        contain multiple dataset types. Should the entire Zip be rejected
        if one dataset type is in the constraints list? If configured to
        reject everything, ingest should not be attempted.

        The Zip file is given to each datastore in turn, ignoring datastores
        where it is not supported. Is deemed successful if any of the
        datastores accept the file.
        """
        index = ZipIndex.from_zip_file(zip_path)
        refs = index.refs.to_refs(self.universe)

        # For now raise if any refs are not supported.
        # Being selective will require that we return the ingested refs
        # to the caller so that registry can be modified to remove the
        # entries.
        if any(not self.constraints.isAcceptable(ref) for ref in refs):
            raise DatasetTypeNotSupportedError(
                "Some of the refs in the given Zip file are not acceptable to this datastore."
            )

        n_success = 0
        final_exception: Exception | None = None
        for number, (datastore, constraints) in enumerate(
            zip(self.datastores, self.datastoreConstraints, strict=True)
        ):
            if datastore.isEphemeral:
                continue

            # There can be constraints for the datastore in the configuration
            # of the chaining, or constraints in the configuration of the
            # datastore itself.
            if any(
                (constraints is not None and not constraints.isAcceptable(ref))
                or not datastore.constraints.isAcceptable(ref)
                for ref in refs
            ):
                log.debug("Datastore %s skipping zip ingest due to constraints", datastore.name)
                continue
            try:
                datastore.ingest_zip(zip_path, transfer=transfer)
            except NotImplementedError:
                continue
            except Exception as e:
                final_exception = e
            else:
                n_success += 1

        if n_success:
            return
        if final_exception:
            raise final_exception
        raise RuntimeError("Ingest was not successful in any datastores.")

    def remove(self, ref: DatasetRef) -> None:
        """Indicate to the datastore that a dataset can be removed.

        The dataset will be removed from each datastore.  The dataset is
        not required to exist in every child datastore.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required dataset.

        Raises
        ------
        FileNotFoundError
            Attempt to remove a dataset that does not exist.  Raised if none
            of the child datastores removed the dataset.
        """
        log.debug("Removing %s", ref)
        self.trash(ref, ignore_errors=False)
        self.emptyTrash(ignore_errors=False)

    def forget(self, refs: Iterable[DatasetRef]) -> None:
        for datastore in tuple(self.datastores):
            datastore.forget(refs)

    def trash(self, ref: DatasetRef | Iterable[DatasetRef], ignore_errors: bool = True) -> None:
        if isinstance(ref, DatasetRef):
            ref_label = str(ref)
        else:
            ref_label = "bulk datasets"

        log.debug("Trashing %s", ref_label)

        counter = 0
        for datastore in self.datastores:
            try:
                datastore.trash(ref, ignore_errors=ignore_errors)
                counter += 1
            except FileNotFoundError:
                pass

        if counter == 0:
            err_msg = f"Could not mark for removal from any child datastore: {ref_label}"
            if ignore_errors:
                log.warning(err_msg)
            else:
                raise FileNotFoundError(err_msg)

    def emptyTrash(self, ignore_errors: bool = True) -> None:
        for datastore in self.datastores:
            datastore.emptyTrash(ignore_errors=ignore_errors)

    def transfer(self, inputDatastore: Datastore, ref: DatasetRef) -> None:
        """Retrieve a dataset from an input `Datastore`,
        and store the result in this `Datastore`.

        Parameters
        ----------
        inputDatastore : `Datastore`
            The external `Datastore` from which to retreive the Dataset.
        ref : `DatasetRef`
            Reference to the required dataset in the input data store.

        Returns
        -------
        results : `list`
            List containing the return value from the ``put()`` to each
            child datastore.
        """
        assert inputDatastore is not self  # unless we want it for renames?
        inMemoryDataset = inputDatastore.get(ref)
        self.put(inMemoryDataset, ref)

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
        This method checks each datastore in turn.
        """
        # Need to catch each of the datastore outputs and ensure that
        # all are tested.
        failures = []
        for datastore in self.datastores:
            try:
                datastore.validateConfiguration(entities, logFailures=logFailures)
            except DatastoreValidationError as e:
                if logFailures:
                    log.critical("Datastore %s failed validation", datastore.name)
                failures.append(f"Datastore {self.name}: {e}")

        if failures:
            msg = ";\n".join(failures)
            raise DatastoreValidationError(msg)

    def validateKey(self, lookupKey: LookupKey, entity: DatasetRef | DatasetType | StorageClass) -> None:
        # Docstring is inherited from base class
        failures = []
        for datastore in self.datastores:
            try:
                datastore.validateKey(lookupKey, entity)
            except DatastoreValidationError as e:
                failures.append(f"Datastore {self.name}: {e}")

        if failures:
            msg = ";\n".join(failures)
            raise DatastoreValidationError(msg)

    def getLookupKeys(self) -> set[LookupKey]:
        # Docstring is inherited from base class
        keys = set()
        for datastore in self.datastores:
            keys.update(datastore.getLookupKeys())

        keys.update(self.constraints.getLookupKeys())
        for p in self.datastoreConstraints:
            if p is not None:
                keys.update(p.getLookupKeys())

        return keys

    def needs_expanded_data_ids(
        self,
        transfer: str | None,
        entity: DatasetRef | DatasetType | StorageClass | None = None,
    ) -> bool:
        # Docstring inherited.
        # We can't safely use `self.datastoreConstraints` with `entity` to
        # check whether a child datastore would even want to ingest this
        # dataset, because we don't want to filter out datastores that might
        # need an expanded data ID based in incomplete information (e.g. we
        # pass a StorageClass, but the constraint dispatches on DatasetType).
        # So we pessimistically check if any datastore would need an expanded
        # data ID for this transfer mode.
        return any(datastore.needs_expanded_data_ids(transfer, entity) for datastore in self.datastores)

    def import_records(self, data: Mapping[str, DatastoreRecordData]) -> None:
        # Docstring inherited from the base class.

        for datastore in self.datastores:
            datastore.import_records(data)

    def export_records(self, refs: Iterable[DatasetIdRef]) -> Mapping[str, DatastoreRecordData]:
        # Docstring inherited from the base class.

        all_records: dict[str, DatastoreRecordData] = {}

        # Merge all sub-datastore records into one structure
        for datastore in self.datastores:
            sub_records = datastore.export_records(refs)
            for name, record_data in sub_records.items():
                # All datastore names must be unique in a chain.
                if name in all_records:
                    raise ValueError("Non-unique datastore name found in datastore {datastore}")
                all_records[name] = record_data

        return all_records

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

        # Exporting from a chain has the potential for a dataset to be
        # in one or more of the datastores in the chain. We only need one
        # of them since we assume the datasets are the same in all (but
        # the file format could be different of course since that is a
        # per-datastore configuration).
        # We also do not know whether any of the datastores in the chain
        # support file export.

        # Ensure we have an ordered sequence that is not an iterator or set.
        if not isinstance(refs, Sequence):
            refs = list(refs)

        # If any of the datasets are missing entirely we need to raise early
        # before we try to run the export. This can be a little messy but is
        # better than exporting files from the first datastore and then finding
        # that one is missing but is not in the second datastore either.
        known = [datastore.knows_these(refs) for datastore in self.datastores]
        refs_known: set[DatasetRef] = set()
        for known_to_this in known:
            refs_known.update({ref for ref, knows_this in known_to_this.items() if knows_this})
        missing_count = len(refs) - len(refs_known)
        if missing_count:
            raise FileNotFoundError(f"Not all datasets known to this datastore. Missing {missing_count}")

        # To allow us to slot each result into the right place after
        # asking each datastore, create a dict with the index.
        ref_positions = {ref: i for i, ref in enumerate(refs)}

        # Presize the final export list.
        exported: list[FileDataset | None] = [None] * len(refs)

        # The order of the returned dataset has to match the order of the
        # given refs, even if they are all from different datastores.
        for i, datastore in enumerate(self.datastores):
            known_to_this = known[i]
            filtered = [ref for ref, knows in known_to_this.items() if knows and ref in ref_positions]

            try:
                this_export = datastore.export(filtered, directory=directory, transfer=transfer)
            except NotImplementedError:
                # Try the next datastore.
                continue

            for ref, export in zip(filtered, this_export, strict=True):
                # Get the position and also delete it from the list.
                exported[ref_positions.pop(ref)] = export

        # Every dataset should be accounted for because of the earlier checks
        # but make sure that we did fill all the slots to appease mypy.
        for i, dataset in enumerate(exported):
            if dataset is None:
                raise FileNotFoundError(f"Failed to export dataset {refs[i]}.")
            yield dataset

    def transfer_from(
        self,
        source_datastore: Datastore,
        refs: Collection[DatasetRef],
        transfer: str = "auto",
        artifact_existence: dict[ResourcePath, bool] | None = None,
        dry_run: bool = False,
    ) -> tuple[set[DatasetRef], set[DatasetRef]]:
        # Docstring inherited
        # mypy does not understand "type(self) is not type(source)"
        if isinstance(source_datastore, ChainedDatastore):
            # Both the source and destination are chained datastores.
            source_datastores = tuple(source_datastore.datastores)
        else:
            # The source datastore is different, forward everything to the
            # child datastores.
            source_datastores = (source_datastore,)

        if not refs:
            # Nothing to transfer.
            return set(), set()

        # Need to know the set of all possible refs that could be transferred.
        remaining_refs = set(refs)

        missing_from_source: set[DatasetRef] | None = None
        all_accepted = set()
        nsuccess = 0
        for source_child in source_datastores:
            # If we are reading from a chained datastore, it's possible that
            # only a subset of the datastores know about the dataset. We can't
            # ask the receiving datastore to copy it when it doesn't exist
            # so we have to filter again based on what the source datastore
            # understands.
            known_to_source = source_child.knows_these(list(refs))

            # Need to know that there is a possibility that some of these
            # datasets exist but are unknown to the source datastore if
            # trust is enabled.
            if getattr(source_child, "trustGetRequest", False):
                unknown = [ref for ref, known in known_to_source.items() if not known]
                existence = source_child.mexists(unknown, artifact_existence)
                for ref, exists in existence.items():
                    known_to_source[ref] = exists

            missing = {ref for ref, known in known_to_source.items() if not known}
            if missing:
                if missing_from_source is None:
                    missing_from_source = missing
                else:
                    missing_from_source &= missing

            # Try to transfer from each source datastore to each child
            # datastore. Have to make sure we don't transfer something
            # we've already transferred to this destination on later passes.

            # Filter the initial list based on the datasets we have
            # not yet transferred.
            these_refs = []
            for ref in refs:
                if ref in remaining_refs and known_to_source[ref]:
                    these_refs.append(ref)

            if not these_refs:
                # Already transferred all datasets known to this datastore.
                continue

            for datastore, constraints in zip(self.datastores, self.datastoreConstraints, strict=True):
                if constraints is not None:
                    filtered_refs = []
                    for ref in these_refs:
                        if constraints.isAcceptable(ref):
                            filtered_refs.append(ref)
                        else:
                            log.debug("Rejecting ref by constraints: %s", ref)
                else:
                    filtered_refs = list(these_refs)
                try:
                    accepted, _ = datastore.transfer_from(
                        source_child,
                        filtered_refs,
                        transfer,
                        artifact_existence,
                        dry_run=dry_run,
                    )
                except (TypeError, NotImplementedError):
                    # The datastores were incompatible.
                    continue
                else:
                    nsuccess += 1

                # Remove the accepted datasets from those remaining.
                remaining_refs = remaining_refs - accepted

                # Keep track of everything we have accepted.
                all_accepted.update(accepted)

        if missing_from_source:
            for ref in missing_from_source:
                log.warning("Asked to transfer dataset %s but no file artifacts exist for it", ref)

        if nsuccess == 0:
            raise TypeError(f"None of the child datastores could accept transfers from {source_datastore!r}")

        return all_accepted, remaining_refs

    def get_opaque_table_definitions(self) -> Mapping[str, DatastoreOpaqueTable]:
        # Docstring inherited from the base class.
        tables: dict[str, DatastoreOpaqueTable] = {}
        for datastore in self.datastores:
            tables.update(datastore.get_opaque_table_definitions())
        return tables

    def set_retrieve_dataset_type_method(self, method: Callable[[str], DatasetType | None] | None) -> None:
        # Docstring inherited from the base class.
        for datastore in self.datastores:
            datastore.set_retrieve_dataset_type_method(method)
