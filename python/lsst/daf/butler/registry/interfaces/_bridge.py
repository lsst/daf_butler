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

__all__ = ("DatastoreRegistryBridgeManager", "DatastoreRegistryBridge", "FakeDatasetRef", "DatasetIdRef")

from abc import ABC, abstractmethod
from typing import (
    Any,
    ContextManager,
    Iterable,
    Type,
    TYPE_CHECKING,
    Union,
)

from ...core.utils import immutable
from ...core import DatasetRef

if TYPE_CHECKING:
    from ...core import DatasetType, DimensionUniverse
    from ._database import Database, StaticTablesContext
    from ._datasets import DatasetRecordStorageManager
    from ._opaque import OpaqueTableStorageManager


@immutable
class FakeDatasetRef:
    """A fake `DatasetRef` that can be used internally by butler where
    only the dataset ID is available.

    Should only be used when registry can not be used to create a full
    `DatasetRef` from the ID.  A particular use case is during dataset
    deletion when solely the ID is available.

    Parameters
    ----------
    id : `int`
        The dataset ID.
    """
    __slots__ = ("id",)

    def __new__(cls, id: int) -> FakeDatasetRef:
        self = super().__new__(cls)
        self.id = id
        return self

    def __str__(self) -> str:
        return f"dataset_id={self.id}"

    def __repr__(self) -> str:
        return f"FakeDatasetRef({self.id})"

    def __eq__(self, other: Any) -> Union[bool, NotImplemented]:
        try:
            return self.id == other.id
        except AttributeError:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self.id)

    id: int
    """Unique integer that identifies this dataset.
    """

    def getCheckedId(self) -> int:
        """Return ``self.id``.

        This trivial method exists for compatibility with `DatasetRef`, for
        which checking is actually done.

        Returns
        -------
        id : `int`
            ``self.id``.
        """
        return self.id

    @property
    def datasetType(self) -> DatasetType:
        raise AttributeError("A FakeDatasetRef can not be associated with a valid DatasetType")


DatasetIdRef = Union[DatasetRef, FakeDatasetRef]
"""A type-annotation alias that matches both `DatasetRef` and `FakeDatasetRef`.
"""


class DatastoreRegistryBridge(ABC):
    """An abstract base class that defines the interface that a `Datastore`
    uses to communicate with a `Registry`.

    Parameters
    ----------
    datastoreName : `str`
        Name of the `Datastore` as it should appear in `Registry` tables
        referencing it.
    """
    def __init__(self, datastoreName: str):
        self.datastoreName = datastoreName

    @abstractmethod
    def insert(self, refs: Iterable[DatasetIdRef]) -> None:
        """Record that a datastore holds the given datasets.

        Parameters
        ----------
        refs : `Iterable` of `DatasetIdRef`
            References to the datasets.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``any(ref.id is None for ref in refs)``.
        """
        raise NotImplementedError()

    @abstractmethod
    def moveToTrash(self, refs: Iterable[DatasetIdRef]) -> None:
        """Move dataset location information to trash.

        Parameters
        ----------
        refs : `Iterable` of `DatasetIdRef`
            References to the datasets.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``any(ref.id is None for ref in refs)``.
        """
        raise NotImplementedError()

    @abstractmethod
    def check(self, refs: Iterable[DatasetIdRef]) -> Iterable[DatasetIdRef]:
        """Check which refs are listed for this datastore.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` of `DatasetIdRef`
            References to the datasets.

        Returns
        -------
        present : `Iterable` [ `DatasetIdRef` ]
            Datasets from ``refs`` that are recorded as being in this
            datastore.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``any(ref.id is None for ref in refs)``.
        """
        raise NotImplementedError()

    @abstractmethod
    def emptyTrash(self) -> ContextManager[Iterable[DatasetIdRef]]:
        """Retrieve all the dataset ref IDs that are in the trash
        associated for this datastore, and then remove them if the context
        exists without an exception being raised.

        Returns
        -------
        ids : `set` of `DatasetIdRef`
            The IDs of datasets that can be safely removed from this datastore.
            Can be empty.

        Examples
        --------
        Typical usage by a Datastore is something like::

            with self.bridge.emptyTrash() as iter:
                for ref in iter:
                    # Remove artifacts associated with ref.id,
                    # raise an exception if something goes wrong.

        Notes
        -----
        The object yielded by the context manager may be a single-pass
        iterator.  If multiple passes are required, it should be converted to
        a `list` or other container.

        Datastores should never raise (except perhaps in testing) when an
        artifact cannot be removed only because it is already gone - this
        condition is an unavoidable outcome of concurrent delete operations,
        and must not be considered and error for those to be safe.
        """
        raise NotImplementedError()

    datastoreName: str
    """The name of the `Datastore` as it should appear in `Registry` tables
    (`str`).
    """


class DatastoreRegistryBridgeManager(ABC):
    """An abstract base class that defines the interface between `Registry`
    and `Datastore` when a new `Datastore` is constructed.

    Parameters
    ----------
    opaque : `OpaqueTableStorageManager`
        Manager object for opaque table storage in the `Registry`.
    universe : `DimensionUniverse`
        All dimensions know to the `Registry`.

    Notes
    -----
    Datastores are passed an instance of `DatastoreRegistryBridgeManager` at
    construction, and should use it to obtain and keep any of the following:

    - a `DatastoreRegistryBridge` instance to record in the `Registry` what is
      present in the datastore (needed by all datastores that are not just
      forwarders);

    - one or more `OpaqueTableStorage` instance if they wish to store internal
      records in the `Registry` database;

    - the `DimensionUniverse`, if they need it to (e.g.) construct or validate
      filename templates.

    """
    def __init__(self, *, opaque: OpaqueTableStorageManager, universe: DimensionUniverse):
        self.opaque = opaque
        self.universe = universe

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *,
                   opaque: OpaqueTableStorageManager,
                   datasets: Type[DatasetRecordStorageManager],
                   universe: DimensionUniverse,
                   ) -> DatastoreRegistryBridgeManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.
        opaque : `OpaqueTableStorageManager`
            Registry manager object for opaque (to Registry) tables, provided
            to allow Datastores to store their internal information inside the
            Registry database.
        datasets : subclass of `DatasetRecordStorageManager`
            Concrete class that will be used to manage the core dataset tables
            in this registry; should be used only to create foreign keys to
            those tables.
        universe : `DimensionUniverse`
            All dimensions known to the registry.

        Returns
        -------
        manager : `DatastoreRegistryBridgeManager`
            An instance of a concrete `DatastoreRegistryBridgeManager`
            subclass.
        """
        raise NotImplementedError()

    @abstractmethod
    def refresh(self) -> None:
        """Ensure all other operations on this manager are aware of any
        collections that may have been registered by other clients since it
        was initialized or last refreshed.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(self, name: str, *, ephemeral: bool = True) -> DatastoreRegistryBridge:
        """Register a new `Datastore` associated with this `Registry`.

        This method should be called by all `Datastore` classes aside from
        those that only forward storage to other datastores.

        Parameters
        ----------
        name : `str`
            Name of the datastore, as it should appear in `Registry` tables.
        ephemeral : `bool`, optional
            If `True` (`False` is default), return a bridge object that is
            backed by storage that will not last past the end of the current
            process.  This should be used whenever the same is true of the
            dataset's artifacts.

        Returns
        -------
        bridge : `DatastoreRegistryBridge`
            Object that provides the interface this `Datastore` should use to
            communicate with the `Regitry`.
        """
        raise NotImplementedError()

    @abstractmethod
    def findDatastores(self, ref: DatasetRef) -> Iterable[str]:
        """Retrieve datastore locations for a given dataset.

        Parameters
        ----------
        ref : `DatasetRef`
            A reference to the dataset for which to retrieve storage
            information.

        Returns
        -------
        datastores : `Iterable` [ `str` ]
            All the matching datastores holding this dataset. Empty if the
            dataset does not exist anywhere.

        Raises
        ------
        AmbiguousDatasetError
            Raised if ``ref.id`` is `None`.
        """
        raise NotImplementedError()

    opaque: OpaqueTableStorageManager
    """Registry manager object for opaque (to Registry) tables, provided
    to allow Datastores to store their internal information inside the
    Registry database.
    """

    universe: DimensionUniverse
    """All dimensions known to the `Registry`.
    """
