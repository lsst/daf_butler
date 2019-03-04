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

import contextlib
import logging
from collections import namedtuple
from abc import ABCMeta, abstractmethod

from lsst.utils import doImport
from .config import ConfigSubset
from .exceptions import ValidationError

__all__ = ("DatastoreConfig", "Datastore", "DatastoreValidationError")


class DatastoreConfig(ConfigSubset):
    component = "datastore"
    requiredKeys = ("cls",)
    defaultConfigFile = "datastore.yaml"


class DatastoreValidationError(ValidationError):
    """There is a problem with the Datastore configuration.
    """
    pass


class DatastoreTransaction:
    """Keeps a log of `Datastore` activity and allow rollback.

    Parameters
    ----------
    parent : `DatastoreTransaction`, optional
        The parent transaction (if any)

    Attributes
    ----------
    parent : `DatastoreTransaction`
        The parent transaction.
    """
    Event = namedtuple("Event", ["name", "undoFunc", "args", "kwargs"])

    def __init__(self, parent=None):
        self.parent = parent
        self._log = []

    def registerUndo(self, name, undoFunc, *args, **kwargs):
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
    def undoWith(self, name, undoFunc, *args, **kwargs):
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

    def rollback(self):
        """Roll back all events in this transaction.
        """
        while self._log:
            name, undoFunc, args, kwargs = self._log.pop()
            try:
                undoFunc(*args, **kwargs)
            except BaseException as e:
                # Deliberately swallow error that may occur in unrolling
                log = logging.getLogger(__name__)
                log.warn("Exception: %s caught while unrolling: %s", e, name)
                pass

    def commit(self):
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

    Attributes
    ----------
    config : `DatastoreConfig`
        Configuration used to create Datastore.
    registry : `Registry`
        `Registry` to use when recording the writing of Datasets.
    name : `str`
        Label associated with this Datastore.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Load configuration
    """

    defaultConfigFile = None
    """Path to configuration defaults. Relative to $DAF_BUTLER_DIR/config or
    absolute path. Can be None if no defaults specified.
    """

    containerKey = None
    """Name of the key containing a list of subconfigurations that also
    need to be merged with defaults and will likely use different Python
    datastore classes (but all using DatastoreConfig).  Assumed to be a
    list of configurations that can be represented in a DatastoreConfig
    and containing a "cls" definition. None indicates that no containers
    are expected in this Datastore."""

    isEphemeral = False
    """Indicate whether this Datastore is ephemeral or not.  An ephemeral
    datastore is one where the contents of the datastore will not exist
    across process restarts."""

    @classmethod
    @abstractmethod
    def setConfigRoot(cls, root, config, full):
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
            should be copied from `full` to `Config`.
        """
        raise NotImplementedError()

    @staticmethod
    def fromConfig(config, registry):
        """Create datastore from type specified in config file.

        Parameters
        ----------
        config : `Config`
            Configuration instance.
        """
        cls = doImport(config["datastore", "cls"])
        return cls(config=config, registry=registry)

    def __init__(self, config, registry):
        self.config = DatastoreConfig(config)
        self.registry = registry
        self.name = "ABCDataStore"
        self._transaction = None

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

    def ingest(self, path, ref, formatter=None, transfer=None):
        """Add an on-disk file with the given `DatasetRef` to the store,
        possibly transferring it.

        The caller is responsible for ensuring that the given (or predicted)
        Formatter is consistent with how the file was written; `ingest` will
        in general silently ignore incorrect formatters (as it cannot
        efficiently verify their correctness), deferring errors until ``get``
        is first called on the ingested dataset.

        Datastores are not required to implement this method, but must do so
        in order to support direct raw data ingest.

        Parameters
        ----------
        path : `str`
            File path, relative to the repository root.
        ref : `DatasetRef`
            Reference to the associated Dataset.
        formatter : `Formatter` (optional)
            Formatter that should be used to retreive the Dataset.  If not
            provided, the formatter will be constructed according to
            Datastore configuration.
        transfer : str (optional)
            If not None, must be one of 'move', 'copy', 'hardlink', or
            'symlink' indicating how to transfer the file.
            Datastores need not support all options, but must raise
            NotImplementedError if the passed option is not supported.
            That includes None, which indicates that the file should be
            ingested at its current location with no transfer.  If a
            Datastore does support ingest-without-transfer in general,
            but the given path is not appropriate, an exception other
            than NotImplementedError that better describes the problem
            should be raised.

        Raises
        ------
        NotImplementedError
            Raised if the given transfer mode is not supported.
        """
        raise NotImplementedError(
            "Datastore does not support direct file-based ingest."
        )

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

    @abstractmethod
    def validateConfiguration(self, *entities, logFailures=False):
        """Validate some of the configuration for this datastore.

        Parameters
        ----------
        *entities : `DatasetRef`, `DatasetType`, or `StorageClass`
            Entities to test against this configuration.
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
