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

"""Support for generic data stores."""

from __future__ import annotations

__all__ = (
    "DatasetRefURIs",
    "Datastore",
    "DatastoreConfig",
    "DatastoreOpaqueTable",
    "DatastoreTransaction",
    "DatastoreValidationError",
    "NullDatastore",
)

import contextlib
import dataclasses
import logging
import time
from abc import ABCMeta, abstractmethod
from collections import abc, defaultdict
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from lsst.utils import doImportType

from .._config import Config, ConfigSubset
from .._exceptions import DatasetTypeNotSupportedError, ValidationError
from .._file_dataset import FileDataset
from .._storage_class import StorageClassFactory
from ._transfer import FileTransferInfo, FileTransferSource
from .constraints import Constraints

if TYPE_CHECKING:
    from lsst.resources import ResourcePath, ResourcePathExpression

    from .. import ddl
    from .._config_support import LookupKey
    from .._dataset_provenance import DatasetProvenance
    from .._dataset_ref import DatasetId, DatasetRef
    from .._dataset_type import DatasetType
    from .._storage_class import StorageClass
    from ..datastores.file_datastore.retrieve_artifacts import ArtifactIndexInfo
    from ..registry.interfaces import DatasetIdRef, DatastoreRegistryBridgeManager
    from .record_data import DatastoreRecordData
    from .stored_file_info import StoredDatastoreItemInfo

_LOG = logging.getLogger(__name__)


class DatastoreConfig(ConfigSubset):
    """Configuration for Datastores."""

    component = "datastore"
    requiredKeys = ("cls",)
    defaultConfigFile = "datastore.yaml"


class DatastoreValidationError(ValidationError):
    """There is a problem with the Datastore configuration."""

    pass


@dataclasses.dataclass(frozen=True)
class Event:
    """Representation of an event that can be rolled back."""

    __slots__ = {"name", "undoFunc", "args", "kwargs"}
    name: str
    undoFunc: Callable
    args: tuple
    kwargs: dict


@dataclasses.dataclass(frozen=True)
class DatastoreOpaqueTable:
    """Definition of the opaque table which stores datastore records.

    Table definition contains `.ddl.TableSpec` for a table and a class
    of a record which must be a subclass of `StoredDatastoreItemInfo`.
    """

    __slots__ = {"table_spec", "record_class"}
    table_spec: ddl.TableSpec
    record_class: type[StoredDatastoreItemInfo]


class IngestPrepData:
    """A helper base class for `Datastore` ingest implementations.

    Datastore implementations will generally need a custom implementation of
    this class.

    Should be accessed as ``Datastore.IngestPrepData`` instead of via direct
    import.

    Parameters
    ----------
    refs : iterable of `DatasetRef`
        References for the datasets that can be ingested by this datastore.
    """

    def __init__(self, refs: Iterable[DatasetRef]):
        self.refs = {ref.id: ref for ref in refs}


class DatastoreTransaction:
    """Keeps a log of `Datastore` activity and allow rollback.

    Parameters
    ----------
    parent : `DatastoreTransaction`, optional
        The parent transaction (if any).

    Notes
    -----
    This transaction object must be thread safe.
    """

    Event: ClassVar[type] = Event

    parent: DatastoreTransaction | None
    """The parent transaction. (`DatastoreTransaction`, optional)"""

    def __init__(self, parent: DatastoreTransaction | None = None):
        self.parent = parent
        self._log: list[Event] = []

    def registerUndo(self, name: str, undoFunc: Callable, *args: Any, **kwargs: Any) -> None:
        """Register event with undo function.

        Parameters
        ----------
        name : `str`
            Name of the event.
        undoFunc : `~collections.abc.Callable`
            Function to undo this event.
        *args : `tuple`
            Positional arguments to ``undoFunc``.
        **kwargs
            Keyword arguments to ``undoFunc``.
        """
        self._log.append(self.Event(name, undoFunc, args, kwargs))

    @contextlib.contextmanager
    def undoWith(self, name: str, undoFunc: Callable, *args: Any, **kwargs: Any) -> Iterator[None]:
        """Register undo function if nested operation succeeds.

        Calls `registerUndo`.

        This can be used to wrap individual undo-able statements within a
        DatastoreTransaction block.  Multiple statements that can fail
        separately should not be part of the same `undoWith` block.

        All arguments are forwarded directly to `registerUndo`.

        Parameters
        ----------
        name : `str`
            The name to associate with this event.
        undoFunc : `~collections.abc.Callable`
            Function to undo this event.
        *args : `tuple`
            Positional arguments for ``undoFunc``.
        **kwargs : `typing.Any`
            Keyword arguments for ``undoFunc``.
        """
        try:
            yield None
        except BaseException:
            raise
        else:
            self.registerUndo(name, undoFunc, *args, **kwargs)

    def rollback(self) -> None:
        """Roll back all events in this transaction."""
        log = logging.getLogger(__name__)
        while self._log:
            ev = self._log.pop()
            try:
                log.debug(
                    "Rolling back transaction: %s: %s(%s,%s)",
                    ev.name,
                    ev.undoFunc,
                    ",".join(str(a) for a in ev.args),
                    ",".join(f"{k}={v}" for k, v in ev.kwargs.items()),
                )
            except Exception:
                # In case we had a problem in stringification of arguments
                log.warning("Rolling back transaction: %s", ev.name)
            try:
                ev.undoFunc(*ev.args, **ev.kwargs)
            except BaseException as e:
                # Deliberately swallow error that may occur in unrolling
                log.warning("Exception: %s caught while unrolling: %s", e, ev.name)
                pass

    def commit(self) -> None:
        """Commit this transaction."""
        if self.parent is None:
            # Just forget about the events, they have already happened.
            return
        else:
            # We may still want to events from this transaction as part of
            # the parent.
            self.parent._log.extend(self._log)


@dataclasses.dataclass
class DatasetRefURIs(abc.Sequence):
    """Represents the primary and component ResourcePath(s) associated with a
    DatasetRef.

    This is used in places where its members used to be represented as a tuple
    `(primaryURI, componentURIs)`. To maintain backward compatibility this
    inherits from Sequence and so instances can be treated as a two-item
    tuple.

    Parameters
    ----------
    primaryURI : `lsst.resources.ResourcePath` or `None`, optional
        The URI to the primary artifact associated with this dataset. If the
        dataset was disassembled within the datastore this may be `None`.
    componentURIs : `dict` [`str`, `~lsst.resources.ResourcePath`] or `None`
        The URIs to any components associated with the dataset artifact
        indexed by component name. This can be empty if there are no
        components.
    """

    def __init__(
        self,
        primaryURI: ResourcePath | None = None,
        componentURIs: dict[str, ResourcePath] | None = None,
    ):
        self.primaryURI = primaryURI
        self.componentURIs = componentURIs or {}

    def __getitem__(self, index: Any) -> Any:
        """Get primaryURI and componentURIs by index.

        Provides support for tuple-like access.
        """
        if index == 0:
            return self.primaryURI
        elif index == 1:
            return self.componentURIs
        raise IndexError("list index out of range")

    def __len__(self) -> int:
        """Get the number of data members.

        Provides support for tuple-like access.
        """
        return 2

    def __repr__(self) -> str:
        return f"DatasetRefURIs({repr(self.primaryURI)}, {repr(self.componentURIs)})"


class Datastore(FileTransferSource, metaclass=ABCMeta):
    """Datastore interface.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Load configuration either from an existing config instance or by
        referring to a configuration file.
    bridgeManager : `DatastoreRegistryBridgeManager`
        Object that manages the interface between `Registry` and datastores.

    See Also
    --------
    lsst.daf.butler.Butler
    """

    defaultConfigFile: ClassVar[str | None] = None
    """Path to configuration defaults. Accessed within the ``config`` resource
    or relative to a search path. Can be None if no defaults specified.
    """

    containerKey: ClassVar[str | None] = None
    """Name of the key containing a list of subconfigurations that also
    need to be merged with defaults and will likely use different Python
    datastore classes (but all using DatastoreConfig).  Assumed to be a
    list of configurations that can be represented in a DatastoreConfig
    and containing a "cls" definition. None indicates that no containers
    are expected in this Datastore."""

    isEphemeral: bool = False
    """Indicate whether this Datastore is ephemeral or not.  An ephemeral
    datastore is one where the contents of the datastore will not exist
    across process restarts.  This value can change per-instance."""

    config: DatastoreConfig
    """Configuration used to create Datastore."""

    name: str
    """Label associated with this Datastore."""

    storageClassFactory: StorageClassFactory
    """Factory for creating storage class instances from name."""

    constraints: Constraints
    """Constraints to apply when putting datasets into the datastore."""

    # MyPy does not like for this to be annotated as any kind of type, because
    # it can't do static checking on type variables that can change at runtime.
    IngestPrepData: ClassVar[Any] = IngestPrepData
    """Helper base class for ingest implementations.
    """

    @classmethod
    @abstractmethod
    def setConfigRoot(cls, root: str, config: Config, full: Config, overwrite: bool = True) -> None:
        """Set filesystem-dependent config options for this datastore.

        The options will be appropriate for a new empty repository with the
        given root.

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
        raise NotImplementedError()

    @staticmethod
    def fromConfig(
        config: Config,
        bridgeManager: DatastoreRegistryBridgeManager,
        butlerRoot: ResourcePathExpression | None = None,
    ) -> Datastore:
        """Create datastore from type specified in config file.

        Parameters
        ----------
        config : `Config` or `~lsst.resources.ResourcePathExpression`
            Configuration instance.
        bridgeManager : `DatastoreRegistryBridgeManager`
            Object that manages the interface between `Registry` and
            datastores.
        butlerRoot : `str`, optional
            Butler root directory.
        """
        config = DatastoreConfig(config)
        cls = doImportType(config["cls"])
        if not issubclass(cls, Datastore):
            raise TypeError(f"Imported child class {config['cls']} is not a Datastore")
        return cls._create_from_config(config=config, bridgeManager=bridgeManager, butlerRoot=butlerRoot)

    def __init__(
        self,
        config: DatastoreConfig,
        bridgeManager: DatastoreRegistryBridgeManager,
    ):
        self.config = config
        self.name = "ABCDataStore"
        self._transaction: DatastoreTransaction | None = None

        # All Datastores need storage classes and constraints
        self.storageClassFactory = StorageClassFactory()

        # And read the constraints list
        constraintsConfig = self.config.get("constraints")
        self.constraints = Constraints(constraintsConfig, universe=bridgeManager.universe)

    @classmethod
    @abstractmethod
    def _create_from_config(
        cls,
        config: DatastoreConfig,
        bridgeManager: DatastoreRegistryBridgeManager,
        butlerRoot: ResourcePathExpression | None,
    ) -> Datastore:
        """`Datastore`.``fromConfig`` calls this to instantiate Datastore
        subclasses.  This is the primary constructor for the individual
        Datastore subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def clone(self, bridgeManager: DatastoreRegistryBridgeManager) -> Datastore:
        """Make an independent copy of this Datastore with a different
        `DatastoreRegistryBridgeManager` instance.

        Parameters
        ----------
        bridgeManager : `DatastoreRegistryBridgeManager`
            New `DatastoreRegistryBridgeManager` object to use when
            instantiating managers.

        Returns
        -------
        datastore : `Datastore`
            New `Datastore` instance with the same configuration as the
            existing instance.
        """
        raise NotImplementedError()

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return self.name

    @property
    def names(self) -> tuple[str, ...]:
        """Names associated with this datastore returned as a list.

        Can be different to ``name`` for a chaining datastore.
        """
        # Default implementation returns solely the name itself
        return (self.name,)

    @property
    def roots(self) -> dict[str, ResourcePath | None]:
        """Return the root URIs for each named datastore.

        Mapping from datastore name to root URI. The URI can be `None`
        if a datastore has no concept of a root URI.
        (`dict` [`str`, `ResourcePath` | `None`])
        """
        return {self.name: None}

    @contextlib.contextmanager
    def transaction(self) -> Iterator[DatastoreTransaction]:
        """Context manager supporting `Datastore` transactions.

        Transactions can be nested, and are to be used in combination with
        `Registry.transaction`.
        """
        self._transaction = DatastoreTransaction(self._transaction)
        try:
            yield self._transaction
        except BaseException:
            self._transaction.rollback()
            raise
        else:
            self._transaction.commit()
        self._transaction = self._transaction.parent

    def _set_trust_mode(self, mode: bool) -> None:
        """Set the trust mode for this datastore.

        Parameters
        ----------
        mode : `bool`
            If `True`, get requests will be attempted even if the datastore
            does not know about the dataset.

        Notes
        -----
        This is a private method to indicate that trust mode might be a
        transitory property that we do not want to make fully public. For now
        only a `~lsst.daf.butler.datastores.FileDatastore` understands this
        concept. By default this method does nothing.
        """
        return

    @abstractmethod
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
        raise NotImplementedError()

    def knows_these(self, refs: Iterable[DatasetRef]) -> dict[DatasetRef, bool]:
        """Check which of the given datasets are known to this datastore.

        This is like ``mexist()`` but does not check that the file exists.

        Parameters
        ----------
        refs : iterable `DatasetRef`
            The datasets to check.

        Returns
        -------
        exists : `dict`[`DatasetRef`, `bool`]
            Mapping of dataset to boolean indicating whether the dataset
            is known to the datastore.
        """
        # Non-optimized default calls knows() repeatedly.
        return {ref: self.knows(ref) for ref in refs}

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
        """
        existence: dict[DatasetRef, bool] = {}
        # Non-optimized default.
        for ref in refs:
            existence[ref] = self.exists(ref)
        return existence

    @abstractmethod
    def exists(self, datasetRef: DatasetRef) -> bool:
        """Check if the dataset exists in the datastore.

        Parameters
        ----------
        datasetRef : `DatasetRef`
            Reference to the required dataset.

        Returns
        -------
        exists : `bool`
            `True` if the entity exists in the `Datastore`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def get(
        self,
        datasetRef: DatasetRef,
        parameters: Mapping[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
    ) -> Any:
        """Load an `InMemoryDataset` from the store.

        Parameters
        ----------
        datasetRef : `DatasetRef`
            Reference to the required Dataset.
        parameters : `dict`
            `StorageClass`-specific parameters that specify a slice of the
            Dataset to be loaded.
        storageClass : `StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the dataset type definition for this dataset. Specifying a
            read `StorageClass` can force a different type to be returned.
            This type must be compatible with the original type.

        Returns
        -------
        inMemoryDataset : `object`
            Requested Dataset or slice thereof as an InMemoryDataset.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def prepare_get_for_external_client(self, ref: DatasetRef) -> object | None:
        """Retrieve serializable data that can be used to execute a ``get()``.

        Parameters
        ----------
        ref : `DatasetRef`
            Reference to the required dataset.

        Returns
        -------
        payload : `object` | `None`
            Serializable payload containing the information needed to perform a
            get() operation.  This payload may be sent over the wire to another
            system to perform the get().  Returns `None` if the dataset is not
            known to this datastore.
        """
        raise NotImplementedError()

    @abstractmethod
    def put(
        self, inMemoryDataset: Any, datasetRef: DatasetRef, provenance: DatasetProvenance | None = None
    ) -> None:
        """Write a `InMemoryDataset` with a given `DatasetRef` to the store.

        Parameters
        ----------
        inMemoryDataset : `object`
            The Dataset to store.
        datasetRef : `DatasetRef`
            Reference to the associated Dataset.
        provenance : `DatasetProvenance` or `None`, optional
            Any provenance that should be attached to the serialized dataset.
            Not supported by all serialization mechanisms.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def put_new(self, in_memory_dataset: Any, ref: DatasetRef) -> Mapping[str, DatasetRef]:
        """Write a `InMemoryDataset` with a given `DatasetRef` to the store.

        Parameters
        ----------
        in_memory_dataset : `object`
            The Dataset to store.
        ref : `DatasetRef`
            Reference to the associated Dataset.

        Returns
        -------
        datastore_refs : `~collections.abc.Mapping` [`str`, `DatasetRef`]
            Mapping of a datastore name to dataset reference stored in that
            datastore, reference will include datastore records. Only
            non-ephemeral datastores will appear in this mapping.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def _overrideTransferMode(self, *datasets: FileDataset, transfer: str | None = None) -> str | None:
        """Allow ingest transfer mode to be defaulted based on datasets.

        Parameters
        ----------
        *datasets : `FileDataset`
            Each positional argument is a struct containing information about
            a file to be ingested, including its path (either absolute or
            relative to the datastore root, if applicable), a complete
            `DatasetRef` (with ``dataset_id not None``), and optionally a
            formatter class or its fully-qualified string name.  If a formatter
            is not provided, this method should populate that attribute with
            the formatter the datastore would use for `put`.  Subclasses are
            also permitted to modify the path attribute (typically to put it
            in what the datastore considers its standard form).
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            See `ingest` for details of transfer modes.

        Returns
        -------
        newTransfer : `str`
            Transfer mode to use. Will be identical to the supplied transfer
            mode unless "auto" is used.
        """
        if transfer != "auto":
            return transfer
        raise RuntimeError(f"{transfer} is not allowed without specialization.")

    def _prepIngest(self, *datasets: FileDataset, transfer: str | None = None) -> IngestPrepData:
        """Process datasets to identify which ones can be ingested.

        Parameters
        ----------
        *datasets : `FileDataset`
            Each positional argument is a struct containing information about
            a file to be ingested, including its path (either absolute or
            relative to the datastore root, if applicable), a complete
            `DatasetRef` (with ``dataset_id not None``), and optionally a
            formatter class or its fully-qualified string name.  If a formatter
            is not provided, this method should populate that attribute with
            the formatter the datastore would use for `put`.  Subclasses are
            also permitted to modify the path attribute (typically to put it
            in what the datastore considers its standard form).
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            See `ingest` for details of transfer modes.

        Returns
        -------
        data : `IngestPrepData`
            An instance of a subclass of `IngestPrepData`, used to pass
            arbitrary data from `_prepIngest` to `_finishIngest`.  This should
            include only the datasets this datastore can actually ingest;
            others should be silently ignored (`Datastore.ingest` will inspect
            `IngestPrepData.refs` and raise `DatasetTypeNotSupportedError` if
            necessary).

        Raises
        ------
        NotImplementedError
            Raised if the datastore does not support the given transfer mode
            (including the case where ingest is not supported at all).
        FileNotFoundError
            Raised if one of the given files does not exist.
        FileExistsError
            Raised if transfer is not `None` but the (internal) location the
            file would be moved to is already occupied.

        Notes
        -----
        This method (along with `_finishIngest`) should be implemented by
        subclasses to provide ingest support instead of implementing `ingest`
        directly.

        `_prepIngest` should not modify the data repository or given files in
        any way; all changes should be deferred to `_finishIngest`.

        When possible, exceptions should be raised in `_prepIngest` instead of
        `_finishIngest`.  `NotImplementedError` exceptions that indicate that
        the transfer mode is not supported must be raised by `_prepIngest`
        instead of `_finishIngest`.
        """
        raise NotImplementedError(f"Datastore {self} does not support direct file-based ingest.")

    def _finishIngest(
        self, prepData: IngestPrepData, *, transfer: str | None = None, record_validation_info: bool = True
    ) -> None:
        """Complete an ingest operation.

        Parameters
        ----------
        prepData : `IngestPrepData`
            An instance of a subclass of `IngestPrepData`.  Guaranteed to be
            the direct result of a call to `_prepIngest` on this datastore.
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

        Raises
        ------
        FileNotFoundError
            Raised if one of the given files does not exist.
        FileExistsError
            Raised if transfer is not `None` but the (internal) location the
            file would be moved to is already occupied.

        Notes
        -----
        This method (along with `_prepIngest`) should be implemented by
        subclasses to provide ingest support instead of implementing `ingest`
        directly.
        """
        raise NotImplementedError(f"Datastore {self} does not support direct file-based ingest.")

    def ingest(
        self, *datasets: FileDataset, transfer: str | None = None, record_validation_info: bool = True
    ) -> None:
        """Ingest one or more files into the datastore.

        Parameters
        ----------
        *datasets : `FileDataset`
            Each positional argument is a struct containing information about
            a file to be ingested, including its path (either absolute or
            relative to the datastore root, if applicable), a complete
            `DatasetRef` (with ``dataset_id not None``), and optionally a
            formatter class or its fully-qualified string name.  If a formatter
            is not provided, the one the datastore would use for ``put`` on
            that dataset is assumed.
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            If `None` (default), the file must already be in a location
            appropriate for the datastore (e.g. within its root directory),
            and will not be modified.  Other choices include "move", "copy",
            "link", "symlink", "relsymlink", and "hardlink". "link" is a
            special transfer mode that will first try to make a hardlink and
            if that fails a symlink will be used instead.  "relsymlink" creates
            a relative symlink rather than use an absolute path.
            Most datastores do not support all transfer modes.
            "auto" is a special option that will let the
            data store choose the most natural option for itself.
        record_validation_info : `bool`, optional
            If `True`, the default, the datastore can record validation
            information associated with the file. If `False` the datastore
            will not attempt to track any information such as checksums
            or file sizes. This can be useful if such information is tracked
            in an external system or if the file is to be compressed in place.
            It is up to the datastore whether this parameter is relevant.

        Raises
        ------
        NotImplementedError
            Raised if the datastore does not support the given transfer mode
            (including the case where ingest is not supported at all).
        DatasetTypeNotSupportedError
            Raised if one or more files to be ingested have a dataset type that
            is not supported by the datastore.
        FileNotFoundError
            Raised if one of the given files does not exist.
        FileExistsError
            Raised if transfer is not `None` but the (internal) location the
            file would be moved to is already occupied.

        Notes
        -----
        Subclasses should implement `_prepIngest` and `_finishIngest` instead
        of implementing `ingest` directly.  Datastores that hold and
        delegate to child datastores may want to call those methods as well.

        Subclasses are encouraged to document their supported transfer modes
        in their class documentation.
        """
        # Allow a datastore to select a default transfer mode
        transfer = self._overrideTransferMode(*datasets, transfer=transfer)
        prepData = self._prepIngest(*datasets, transfer=transfer)
        refs = {ref.id: ref for dataset in datasets for ref in dataset.refs}
        if refs.keys() != prepData.refs.keys():
            unsupported = refs.keys() - prepData.refs.keys()
            # Group unsupported refs by DatasetType for an informative
            # but still concise error message.
            byDatasetType = defaultdict(list)
            for datasetId in unsupported:
                ref = refs[datasetId]
                byDatasetType[ref.datasetType].append(ref)
            raise DatasetTypeNotSupportedError(
                "DatasetType(s) not supported in ingest: "
                + ", ".join(f"{k.name} ({len(v)} dataset(s))" for k, v in byDatasetType.items())
            )
        self._finishIngest(prepData, transfer=transfer, record_validation_info=record_validation_info)

    def transfer_from(
        self,
        source_datastore: FileTransferSource,
        refs: Collection[DatasetRef],
        transfer: str = "auto",
        artifact_existence: dict[ResourcePath, bool] | None = None,
        dry_run: bool = False,
    ) -> tuple[set[DatasetRef], set[DatasetRef]]:
        """Transfer dataset artifacts from another datastore to this one.

        Parameters
        ----------
        source_datastore : `Datastore`
            The datastore from which to transfer artifacts. That datastore
            must be compatible with this datastore receiving the artifacts.
        refs : `~collections.abc.Collection` of `DatasetRef`
            The datasets to transfer from the source datastore.
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            Choices include "move", "copy",
            "link", "symlink", "relsymlink", and "hardlink". "link" is a
            special transfer mode that will first try to make a hardlink and
            if that fails a symlink will be used instead.  "relsymlink" creates
            a relative symlink rather than use an absolute path.
            Most datastores do not support all transfer modes.
            "auto" (the default) is a special option that will let the
            data store choose the most natural option for itself.
            If the source location and transfer location are identical the
            transfer mode will be ignored.
        artifact_existence : `dict` [`lsst.resources.ResourcePath`, `bool`]
            Optional mapping of datastore artifact to existence. Updated by
            this method with details of all artifacts tested. Can be `None`
            if the caller is not interested.
        dry_run : `bool`, optional
            Process the supplied source refs without updating the target
            datastore.

        Returns
        -------
        accepted : `set` [`DatasetRef`]
            The datasets that were transferred.
        rejected : `set` [`DatasetRef`]
            The datasets that were rejected due to a constraints violation.

        Raises
        ------
        TypeError
            Raised if the two datastores are not compatible.
        """
        if type(self) is not type(source_datastore):
            raise TypeError(
                f"Datastore mismatch between this datastore ({type(self)}) and the "
                f"source datastore ({type(source_datastore)})."
            )

        raise NotImplementedError(f"Datastore {type(self)} must implement a transfer_from method.")

    def getManyURIs(
        self,
        refs: Iterable[DatasetRef],
        predict: bool = False,
        allow_missing: bool = False,
    ) -> dict[DatasetRef, DatasetRefURIs]:
        """Return URIs associated with many datasets.

        Parameters
        ----------
        refs : iterable of `DatasetIdRef`
            References to the required datasets.
        predict : `bool`, optional
            If `True`, allow URIs to be returned of datasets that have not
            been written.
        allow_missing : `bool`
            If `False`, and ``predict`` is `False`, will raise if a
            `DatasetRef` does not exist.

        Returns
        -------
        URIs : `dict` of [`DatasetRef`, `DatasetRefUris`]
            A dict of primary and component URIs, indexed by the passed-in
            refs.

        Raises
        ------
        FileNotFoundError
            A URI has been requested for a dataset that does not exist and
            guessing is not allowed.

        Notes
        -----
        In file-based datastores, getManyURIs does not check that the file is
        really there, it's assuming it is if datastore is aware of the file
        then it actually exists.
        """
        uris: dict[DatasetRef, DatasetRefURIs] = {}
        missing_refs = []
        for ref in refs:
            try:
                uris[ref] = self.getURIs(ref, predict=predict)
            except FileNotFoundError:
                missing_refs.append(ref)
        if missing_refs and not allow_missing:
            num_missing = len(missing_refs)
            raise FileNotFoundError(
                f"Missing {num_missing} refs from datastore out of "
                f"{num_missing + len(uris)} and predict=False."
            )
        return uris

    @abstractmethod
    def getURIs(self, datasetRef: DatasetRef, predict: bool = False) -> DatasetRefURIs:
        """Return URIs associated with dataset.

        Parameters
        ----------
        datasetRef : `DatasetRef`
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
        raise NotImplementedError()

    @abstractmethod
    def getURI(self, datasetRef: DatasetRef, predict: bool = False) -> ResourcePath:
        """URI to the Dataset.

        Parameters
        ----------
        datasetRef : `DatasetRef`
            Reference to the required Dataset.
        predict : `bool`
            If `True` attempt to predict the URI for a dataset if it does
            not exist in datastore.

        Returns
        -------
        uri : `str`
            URI string pointing to the Dataset within the datastore. If the
            Dataset does not exist in the datastore, the URI may be a guess.
            If the datastore does not have entities that relate well
            to the concept of a URI the returned URI string will be
            descriptive. The returned URI is not guaranteed to be obtainable.

        Raises
        ------
        FileNotFoundError
            A URI has been requested for a dataset that does not exist and
            guessing is not allowed.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
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
        """Retrieve the artifacts associated with the supplied refs.

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

        Returns
        -------
        artifact_map : `dict` [ `lsst.resources.ResourcePath`, \
                `ArtifactIndexInfo` ]
            Mapping of retrieved file to associated index information.

        Notes
        -----
        For non-file datastores the artifacts written to the destination
        may not match the representation inside the datastore. For example
        a hierarchical data structure in a NoSQL database may well be stored
        as a JSON file.
        """
        raise NotImplementedError()

    @abstractmethod
    def ingest_zip(self, zip_path: ResourcePath, transfer: str | None, *, dry_run: bool = False) -> None:
        """Ingest an indexed Zip file and contents.

        The Zip file must have an index file as created by `retrieveArtifacts`.

        Parameters
        ----------
        zip_path : `lsst.resources.ResourcePath`
            Path to the Zip file.
        transfer : `str`
            Method to use for transferring the Zip file into the datastore.
        dry_run : `bool`, optional
            If `True` the ingest will be processed without any modifications
            made to the target datastore and as if the target datastore did not
            have any of the datasets.
        """
        raise NotImplementedError()

    @abstractmethod
    def remove(self, datasetRef: DatasetRef) -> None:
        """Indicate to the Datastore that a Dataset can be removed.

        Parameters
        ----------
        datasetRef : `DatasetRef`
            Reference to the required Dataset.

        Raises
        ------
        FileNotFoundError
            When Dataset does not exist.

        Notes
        -----
        Some Datastores may implement this method as a silent no-op to
        disable Dataset deletion through standard interfaces.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def forget(self, refs: Iterable[DatasetRef]) -> None:
        """Indicate to the Datastore that it should remove all records of the
        given datasets, without actually deleting them.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            References to the datasets being forgotten.

        Notes
        -----
        Asking a datastore to forget a `DatasetRef` it does not hold should be
        a silent no-op, not an error.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def trash(self, ref: DatasetRef | Iterable[DatasetRef], ignore_errors: bool = True) -> None:
        """Indicate to the Datastore that a Dataset can be moved to the trash.

        Parameters
        ----------
        ref : `DatasetRef` or iterable thereof
            Reference(s) to the required Dataset.
        ignore_errors : `bool`, optional
            Determine whether errors should be ignored. When multiple
            refs are being trashed there will be no per-ref check.

        Raises
        ------
        FileNotFoundError
            When Dataset does not exist and errors are not ignored. Only
            checked if a single ref is supplied (and not in a list).

        Notes
        -----
        Some Datastores may implement this method as a silent no-op to
        disable Dataset deletion through standard interfaces.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def emptyTrash(
        self, ignore_errors: bool = True, refs: Collection[DatasetRef] | None = None, dry_run: bool = False
    ) -> set[ResourcePath]:
        """Remove all datasets from the trash.

        Parameters
        ----------
        ignore_errors : `bool`, optional
            Determine whether errors should be ignored.
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
            List of artifacts that were removed. Can return nothing if
            artifacts cannot be represented by URIs.

        Notes
        -----
        Some Datastores may implement this method as a silent no-op to
        disable Dataset deletion through standard interfaces.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def transfer(self, inputDatastore: Datastore, datasetRef: DatasetRef) -> None:
        """Transfer a dataset from another datastore to this datastore.

        Parameters
        ----------
        inputDatastore : `Datastore`
            The external `Datastore` from which to retrieve the Dataset.
        datasetRef : `DatasetRef`
            Reference to the required Dataset.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def export(
        self,
        refs: Iterable[DatasetRef],
        *,
        directory: ResourcePathExpression | None = None,
        transfer: str | None = "auto",
    ) -> Iterable[FileDataset]:
        """Export datasets for transfer to another data repository.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            Dataset references to be exported.
        directory : `str`, optional
            Path to a directory that should contain files corresponding to
            output datasets.  Ignored if ``transfer`` is explicitly `None`.
        transfer : `str`, optional
            Mode that should be used to move datasets out of the repository.
            Valid options are the same as those of the ``transfer`` argument
            to ``ingest``, and datastores may similarly signal that a transfer
            mode is not supported by raising `NotImplementedError`. If "auto"
            is given and no ``directory`` is specified, `None` will be
            implied.

        Returns
        -------
        dataset : iterable of `DatasetTransfer`
            Structs containing information about the exported datasets, in the
            same order as ``refs``.

        Raises
        ------
        NotImplementedError
            Raised if the given transfer mode is not supported.
        """
        raise NotImplementedError(f"Transfer mode {transfer} not supported.")

    @abstractmethod
    def validateConfiguration(
        self, entities: Iterable[DatasetRef | DatasetType | StorageClass], logFailures: bool = False
    ) -> None:
        """Validate some of the configuration for this datastore.

        Parameters
        ----------
        entities : iterable of `DatasetRef`, `DatasetType`, or `StorageClass`
            Entities to test against this configuration.    Can be differing
            types.
        logFailures : `bool`, optional
            If `True`, output a log message for every validation error
            detected.

        Raises
        ------
        DatastoreValidationError
            Raised if there is a validation problem with a configuration.

        Notes
        -----
        Which parts of the configuration are validated is at the discretion
        of each Datastore implementation.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def validateKey(self, lookupKey: LookupKey, entity: DatasetRef | DatasetType | StorageClass) -> None:
        """Validate a specific look up key with supplied entity.

        Parameters
        ----------
        lookupKey : `LookupKey`
            Key to use to retrieve information from the datastore
            configuration.
        entity : `DatasetRef`, `DatasetType`, or `StorageClass`
            Entity to compare with configuration retrieved using the
            specified lookup key.

        Raises
        ------
        DatastoreValidationError
            Raised if there is a problem with the combination of entity
            and lookup key.

        Notes
        -----
        Bypasses the normal selection priorities by allowing a key that
        would normally not be selected to be validated.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def getLookupKeys(self) -> set[LookupKey]:
        """Return all the lookup keys relevant to this datastore.

        Returns
        -------
        keys : `set` of `LookupKey`
            The keys stored internally for looking up information based
            on `DatasetType` name or `StorageClass`.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def needs_expanded_data_ids(
        self,
        transfer: str | None,
        entity: DatasetRef | DatasetType | StorageClass | None = None,
    ) -> bool:
        """Test whether this datastore needs expanded data IDs to ingest.

        Parameters
        ----------
        transfer : `str` or `None`
            Transfer mode for ingest.
        entity : `DatasetRef` or `DatasetType` or `StorageClass` or `None`, \
                optional
            Object representing what will be ingested.  If not provided (or not
            specific enough), `True` may be returned even if expanded data
            IDs aren't necessary.

        Returns
        -------
        needed : `bool`
            If `True`, expanded data IDs may be needed.  `False` only if
            expansion definitely isn't necessary.
        """
        return True

    @abstractmethod
    def import_records(
        self,
        data: Mapping[str, DatastoreRecordData],
    ) -> None:
        """Import datastore location and record data from an in-memory data
        structure.

        Parameters
        ----------
        data : `~collections.abc.Mapping` [ `str`, `DatastoreRecordData` ]
            Datastore records indexed by datastore name.  May contain data for
            other `Datastore` instances (generally because they are chained to
            this one), which should be ignored.

        Notes
        -----
        Implementations should generally not check that any external resources
        (e.g. files) referred to by these records actually exist, for
        performance reasons; we expect higher-level code to guarantee that they
        do.

        Implementations are responsible for calling
        `DatastoreRegistryBridge.insert` on all datasets in ``data.locations``
        where the key is in `names`, as well as loading any opaque table data.

        Implementations may assume that datasets are either fully present or
        not at all (single-component exports are not permitted).
        """
        raise NotImplementedError()

    @abstractmethod
    def export_records(
        self,
        refs: Iterable[DatasetIdRef],
    ) -> Mapping[str, DatastoreRecordData]:
        """Export datastore records and locations to an in-memory data
        structure.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetIdRef` ]
            Datasets to save.  This may include datasets not known to this
            datastore, which should be ignored.  May not include component
            datasets.

        Returns
        -------
        data : `~collections.abc.Mapping` [ `str`, `DatastoreRecordData` ]
            Exported datastore records indexed by datastore name.
        """
        raise NotImplementedError()

    def set_retrieve_dataset_type_method(self, method: Callable[[str], DatasetType | None] | None) -> None:
        """Specify a method that can be used by datastore to retrieve
        registry-defined dataset type.

        Parameters
        ----------
        method : `~collections.abc.Callable` | `None`
            Method that takes a name of the dataset type and returns a
            corresponding `DatasetType` instance as defined in Registry. If
            dataset type name is not known to registry `None` is returned.

        Notes
        -----
        This method is only needed for a Datastore supporting a "trusted" mode
        when it does not have an access to datastore records and needs to
        guess dataset location based on its stored dataset type.
        """
        pass

    @abstractmethod
    def get_opaque_table_definitions(self) -> Mapping[str, DatastoreOpaqueTable]:
        """Make definitions of the opaque tables used by this Datastore.

        Returns
        -------
        tables : `~collections.abc.Mapping` [ `str`, `.ddl.TableSpec` ]
            Mapping of opaque table names to their definitions. This can be an
            empty mapping if Datastore does not use opaque tables to keep
            datastore records.
        """
        raise NotImplementedError()

    def get_file_info_for_transfer(
        self, refs: Iterable[DatasetRef], artifact_existence: dict[ResourcePath, bool]
    ) -> dict[DatasetId, list[FileTransferInfo]]:
        raise NotImplementedError(f"Transferring files is not supported by datastore {self}")


class NullDatastore(Datastore):
    """A datastore that implements the `Datastore` API but always fails when
    it accepts any request.

    Parameters
    ----------
    config : `Config` or `~lsst.resources.ResourcePathExpression` or `None`
        Ignored.
    bridgeManager : `DatastoreRegistryBridgeManager` or `None`
        Ignored.
    butlerRoot : `~lsst.resources.ResourcePathExpression` or `None`
        Ignored.
    """

    @classmethod
    def _create_from_config(
        cls,
        config: Config,
        bridgeManager: DatastoreRegistryBridgeManager,
        butlerRoot: ResourcePathExpression | None = None,
    ) -> NullDatastore:
        return NullDatastore(config, bridgeManager, butlerRoot)

    def clone(self, bridgeManager: DatastoreRegistryBridgeManager) -> Datastore:
        return self

    @classmethod
    def setConfigRoot(cls, root: str, config: Config, full: Config, overwrite: bool = True) -> None:
        # Nothing to do. This is not a real Datastore.
        pass

    def __init__(
        self,
        config: Config | ResourcePathExpression | None,
        bridgeManager: DatastoreRegistryBridgeManager | None,
        butlerRoot: ResourcePathExpression | None = None,
    ):
        # Name ourselves with the timestamp the datastore
        # was created.
        self.name = f"{type(self).__name__}@{time.time()}"
        _LOG.debug("Creating datastore %s", self.name)
        self._transaction: DatastoreTransaction | None = None
        return

    def knows(self, ref: DatasetRef) -> bool:
        return False

    def exists(self, datasetRef: DatasetRef) -> bool:
        return False

    def get(
        self,
        datasetRef: DatasetRef,
        parameters: Mapping[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
    ) -> Any:
        raise FileNotFoundError("This is a no-op datastore that can not access a real datastore")

    def put(
        self, inMemoryDataset: Any, datasetRef: DatasetRef, provenance: DatasetProvenance | None = None
    ) -> None:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def put_new(self, in_memory_dataset: Any, ref: DatasetRef) -> Mapping[str, DatasetRef]:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def ingest(
        self, *datasets: FileDataset, transfer: str | None = None, record_validation_info: bool = True
    ) -> None:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def transfer_from(
        self,
        source_datastore: FileTransferSource,
        refs: Iterable[DatasetRef],
        transfer: str = "auto",
        artifact_existence: dict[ResourcePath, bool] | None = None,
        dry_run: bool = False,
    ) -> tuple[set[DatasetRef], set[DatasetRef]]:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def getURIs(self, datasetRef: DatasetRef, predict: bool = False) -> DatasetRefURIs:
        raise FileNotFoundError("This is a no-op datastore that can not access a real datastore")

    def getURI(self, datasetRef: DatasetRef, predict: bool = False) -> ResourcePath:
        raise FileNotFoundError("This is a no-op datastore that can not access a real datastore")

    def ingest_zip(self, zip_path: ResourcePath, transfer: str | None, *, dry_run: bool = False) -> None:
        raise NotImplementedError("Can only ingest a Zip into a real datastore.")

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
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def remove(self, datasetRef: DatasetRef) -> None:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def forget(self, refs: Iterable[DatasetRef]) -> None:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def trash(self, ref: DatasetRef | Iterable[DatasetRef], ignore_errors: bool = True) -> None:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def emptyTrash(
        self, ignore_errors: bool = True, refs: Collection[DatasetRef] | None = None, dry_run: bool = False
    ) -> set[ResourcePath]:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def transfer(self, inputDatastore: Datastore, datasetRef: DatasetRef) -> None:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def export(
        self,
        refs: Iterable[DatasetRef],
        *,
        directory: ResourcePathExpression | None = None,
        transfer: str | None = "auto",
    ) -> Iterable[FileDataset]:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def validateConfiguration(
        self, entities: Iterable[DatasetRef | DatasetType | StorageClass], logFailures: bool = False
    ) -> None:
        # No configuration so always validates.
        pass

    def validateKey(self, lookupKey: LookupKey, entity: DatasetRef | DatasetType | StorageClass) -> None:
        pass

    def getLookupKeys(self) -> set[LookupKey]:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def import_records(
        self,
        data: Mapping[str, DatastoreRecordData],
    ) -> None:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def export_records(
        self,
        refs: Iterable[DatasetIdRef],
    ) -> Mapping[str, DatastoreRecordData]:
        raise NotImplementedError("This is a no-op datastore that can not access a real datastore")

    def get_opaque_table_definitions(self) -> Mapping[str, DatastoreOpaqueTable]:
        return {}
