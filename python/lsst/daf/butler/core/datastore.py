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

"""
Support for generic data stores.
"""

from __future__ import annotations

__all__ = ("DatastoreConfig", "Datastore", "DatastoreValidationError")

import contextlib
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Optional, Type, Callable, ClassVar, Any, Generator, Iterable
from dataclasses import dataclass
from abc import ABCMeta, abstractmethod

from lsst.utils import doImport
from .config import ConfigSubset, Config
from .exceptions import ValidationError, DatasetTypeNotSupportedError
from .constraints import Constraints
from .storageClass import StorageClassFactory

if TYPE_CHECKING:
    from ..registry import Registry
    from .datasets import DatasetRef
    from .repoTransfer import FileDataset


class DatastoreConfig(ConfigSubset):
    component = "datastore"
    requiredKeys = ("cls",)
    defaultConfigFile = "datastore.yaml"


class DatastoreValidationError(ValidationError):
    """There is a problem with the Datastore configuration.
    """
    pass


@dataclass(frozen=True)
class Event:
    __slots__ = {"name", "undoFunc", "args", "kwargs"}
    name: str
    undoFunc: Callable
    args: tuple
    kwargs: dict


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
        The parent transaction (if any)
    """
    Event: ClassVar[Type] = Event

    parent: Optional['DatastoreTransaction']
    """The parent transaction. (`DatastoreTransaction`, optional)"""

    def __init__(self, parent=None):
        self.parent = parent
        self._log = []

    def registerUndo(self, name: str, undoFunc: Callable, *args: Any, **kwargs: Any) -> None:
        """Register event with undo function.

        Parameters
        ----------
        name : `str`
            Name of the event.
        undoFunc : func
            Function to undo this event.
        args : `tuple`
            Positional arguments to `undoFunc`.
        kwargs : `dict`
            Keyword arguments to `undoFunc`.
        """
        self._log.append(self.Event(name, undoFunc, args, kwargs))

    @contextlib.contextmanager
    def undoWith(self, name: str, undoFunc: Callable, *args: Any, **kwargs: Any) -> Generator:
        """A context manager that calls `registerUndo` if the nested operation
        does not raise an exception.

        This can be used to wrap individual undo-able statements within a
        DatastoreTransaction block.  Multiple statements that can fail
        separately should not be part of the same `undoWith` block.

        All arguments are forwarded directly to `registerUndo`.
        """
        try:
            yield None
        except BaseException:
            raise
        else:
            self.registerUndo(name, undoFunc, *args, **kwargs)

    def rollback(self) -> None:
        """Roll back all events in this transaction.
        """
        while self._log:
            ev = self._log.pop()
            try:
                ev.undoFunc(*ev.args, **ev.kwargs)
            except BaseException as e:
                # Deliberately swallow error that may occur in unrolling
                log = logging.getLogger(__name__)
                log.warning("Exception: %s caught while unrolling: %s", e, ev.name)
                pass

    def commit(self) -> None:
        """Commit this transaction.
        """
        if self.parent is None:
            # Just forget about the events, they have already happened.
            return
        else:
            # We may still want to events from this transaction as part of
            # the parent.
            self.parent._log.extend(self._log)


class Datastore(metaclass=ABCMeta):
    """Datastore interface.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Load configuration either from an existing config instance or by
        referring to a configuration file.
    registry : `Registry`
        Registry to use for storing internal information about the datasets.
    butlerRoot : `str`, optional
        New datastore root to use to override the configuration value.
    """

    defaultConfigFile: ClassVar[Optional[str]] = None
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    containerKey: ClassVar[Optional[str]] = None
    """Name of the key containing a list of subconfigurations that also
    need to be merged with defaults and will likely use different Python
    datastore classes (but all using DatastoreConfig).  Assumed to be a
    list of configurations that can be represented in a DatastoreConfig
    and containing a "cls" definition. None indicates that no containers
    are expected in this Datastore."""

    isEphemeral: ClassVar[bool] = False
    """Indicate whether this Datastore is ephemeral or not.  An ephemeral
    datastore is one where the contents of the datastore will not exist
    across process restarts."""

    config: DatastoreConfig
    """Configuration used to create Datastore."""

    registry: Registry
    """`Registry` to use when recording the writing of Datasets."""

    name: str
    """Label associated with this Datastore."""

    storageClassFactory: StorageClassFactory
    """Factory for creating storage class instances from name."""

    constraints: Constraints
    """Constraints to apply when putting datasets into the datastore."""

    IngestPrepData: ClassVar[Type] = IngestPrepData
    """Helper base class for ingest implementations.
    """

    @classmethod
    @abstractmethod
    def setConfigRoot(cls, root: str, config: Config, full: Config, overwrite: bool = True):
        """Set any filesystem-dependent config options for this Datastore to
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
        raise NotImplementedError()

    @staticmethod
    def fromConfig(config: Config, registry: Registry, butlerRoot: Optional[str] = None) -> 'Datastore':
        """Create datastore from type specified in config file.

        Parameters
        ----------
        config : `Config`
            Configuration instance.
        registry : `Registry`
            Registry to be used by the Datastore for internal data.
        butlerRoot : `str`, optional
            Butler root directory.
        """
        cls = doImport(config["datastore", "cls"])
        return cls(config=config, registry=registry, butlerRoot=butlerRoot)

    def __init__(self, config, registry, butlerRoot=None):
        self.config = DatastoreConfig(config)
        self.registry = registry
        self.name = "ABCDataStore"
        self._transaction = None

        # All Datastores need storage classes and constraints
        self.storageClassFactory = StorageClassFactory()

        # And read the constraints list
        constraintsConfig = self.config.get("constraints")
        self.constraints = Constraints(constraintsConfig, universe=self.registry.dimensions)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    @contextlib.contextmanager
    def transaction(self):
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

    @abstractmethod
    def exists(self, datasetRef):
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
    def get(self, datasetRef, parameters=None):
        """Load an `InMemoryDataset` from the store.

        Parameters
        ----------
        datasetRef : `DatasetRef`
            Reference to the required Dataset.
        parameters : `dict`
            `StorageClass`-specific parameters that specify a slice of the
            Dataset to be loaded.

        Returns
        -------
        inMemoryDataset : `object`
            Requested Dataset or slice thereof as an InMemoryDataset.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def put(self, inMemoryDataset, datasetRef):
        """Write a `InMemoryDataset` with a given `DatasetRef` to the store.

        Parameters
        ----------
        inMemoryDataset : `InMemoryDataset`
            The Dataset to store.
        datasetRef : `DatasetRef`
            Reference to the associated Dataset.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def _prepIngest(self, *datasets: FileDataset, transfer: Optional[str] = None) -> IngestPrepData:
        """Process datasets to identify which ones can be ingested into this
        Datastore.

        Parameters
        ----------
        datasets : `FileDataset`
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
            If `None` (default), the file must already be in a location
            appropriate for the datastore (e.g. within its root directory),
            and will not be modified.  Other choices include "move", "copy",
            "symlink", and "hardlink".  Most datastores do not support all
            transfer modes.

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
        raise NotImplementedError(
            "Datastore does not support direct file-based ingest."
        )

    def _finishIngest(self, prepData: IngestPrepData, *, transfer: Optional[str] = None):
        """Complete an ingest operation.

        Parameters
        ----------
        data : `IngestPrepData`
            An instance of a subclass of `IngestPrepData`.  Guaranteed to be
            the direct result of a call to `_prepIngest` on this datastore.
        transfer : `str`, optional
            How (and whether) the dataset should be added to the datastore.
            If `None` (default), the file must already be in a location
            appropriate for the datastore (e.g. within its root directory),
            and will not be modified.  Other choices include "move", "copy",
            "symlink", and "hardlink".  Most datastores do not support all
            transfer modes.

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
        raise NotImplementedError(
            "Datastore does not support direct file-based ingest."
        )

    def ingest(self, *datasets: FileDataset, transfer: Optional[str] = None):
        """Ingest one or more files into the datastore.

        Parameters
        ----------
        datasets : `FileDataset`
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
            "symlink", and "hardlink".  Most datastores do not support all
            transfer modes.

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
                "DatasetType(s) not supported in ingest: " +
                ", ".join(f"{k.name} ({len(v)} dataset(s))" for k, v in byDatasetType.items())
            )
        self._finishIngest(prepData, transfer=transfer)

    @abstractmethod
    def getUri(self, datasetRef):
        """URI to the Dataset.

        Parameters
        ----------
        datasetRef : `DatasetRef`
            Reference to the required Dataset.

        Returns
        -------
        uri : `str`
            URI string pointing to the Dataset within the datastore. If the
            Dataset does not exist in the datastore, the URI may be a guess.
            If the datastore does not have entities that relate well
            to the concept of a URI the returned URI string will be
            descriptive. The returned URI is not guaranteed to be obtainable.
        """
        raise NotImplementedError("Must be implemented by subclass")

    @abstractmethod
    def remove(self, datasetRef):
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
    def transfer(self, inputDatastore, datasetRef):
        """Retrieve a Dataset from an input `Datastore`, and store the result
        in this `Datastore`.

        Parameters
        ----------
        inputDatastore : `Datastore`
            The external `Datastore` from which to retreive the Dataset.
        datasetRef : `DatasetRef`
            Reference to the required Dataset.
        """
        raise NotImplementedError("Must be implemented by subclass")

    def export(self, refs: Iterable[DatasetRef], *,
               directory: Optional[str] = None, transfer: Optional[str] = None) -> Iterable[FileDataset]:
        """Export datasets for transfer to another data repository.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            Dataset references to be exported.
        directory : `str`, optional
            Path to a directory that should contain files corresponding to
            output datasets.  Ignored if ``transfer`` is `None`.
        transfer : `str`, optional
            Mode that should be used to move datasets out of the repository.
            Valid options are the same as those of the ``transfer`` argument
            to ``ingest``, and datastores may similarly signal that a transfer
            mode is not supported by raising `NotImplementedError`.

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
    def validateConfiguration(self, entities, logFailures=False):
        """Validate some of the configuration for this datastore.

        Parameters
        ----------
        entities : `DatasetRef`, `DatasetType`, or `StorageClass`
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
    def validateKey(self, lookupKey, entity, logFailures=False):
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
    def getLookupKeys(self):
        """Return all the lookup keys relevant to this datastore.

        Returns
        -------
        keys : `set` of `LookupKey`
            The keys stored internally for looking up information based
            on `DatasetType` name or `StorageClass`.
        """
        raise NotImplementedError("Must be implemented by subclass")
