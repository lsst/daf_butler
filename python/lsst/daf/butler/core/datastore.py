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
from collections import namedtuple

from lsst.daf.butler.core.utils import doImport
from lsst.log import Log

from abc import ABCMeta, abstractmethod
from .config import ConfigSubset

__all__ = ("DatastoreConfig", "Datastore")


class DatastoreConfig(ConfigSubset):
    component = "datastore"
    requiredKeys = ("cls",)
    defaultConfigFile = "datastore.yaml"


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
        `name` : str
            Name of the event.
        `undoFunc` : func
            Function to undo this event.
        `*args` : tuple
            Positional arguments to `undoFunc`.
        `**kwargs` : dict
            Keyword arguments to `undoFunc`.
        """
        self._log.append(self.Event(name, undoFunc, args, kwargs))

    def rollback(self):
        """Roll back all events in this transaction.
        """
        while self._log:
            name, undoFunc, args, kwargs = self._log.pop()
            try:
                undoFunc(*args, **kwargs)
            except BaseException as e:
                # Deliberately swallow error that may occur in unrolling
                log = Log.getLogger("lsst.daf.butler.datastore.DatastoreTransaction")
                log.debug("Exception: %s caught while unrolling: %s", e, name)
                pass

    def commit(self):
        """Commit this transaction.
        """
        if self.parent is None:
            # Just forget about the events, they have already happened.
            return
        else:
            # We may still want to events from this transaction as part of the parent.
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
            A Butler-level config object to update (but not a
            `ButlerConfig`, to avoid included expanded defaults).
        full : `ButlerConfig`
            A complete Butler config with all defaults expanded;
            repository-specific options that should not be obtained
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
        cls = doImport(config['datastore.cls'])
        return cls(config=config, registry=registry)

    def __init__(self, config, registry):
        self.config = DatastoreConfig(config)
        self.registry = registry
        self.name = "ABCDataStore"
        self._transaction = None

    @contextlib.contextmanager
    def transaction(self):
        """Context manager supporting `Datastore` transactions.

        Transactions can be nested, and are to be used in combination with
        `Registry.transaction`.
        """
        self._transaction = DatastoreTransaction(self._transaction)
        try:
            yield
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
