from __future__ import annotations

__all__ = ["DatasetTableManager", "DatasetTableRecords", "Select"]

from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    Iterator,
    Optional,
    Union,
    Tuple,
    Type,
    TypeVar,
    TYPE_CHECKING,
)

import sqlalchemy

from ...core.datasets import DatasetType, ResolvedDatasetHandle
from ...core.dimensions import DimensionUniverse, DataCoordinate
from ...core.schema import FieldSpec, TableSpec
from ...core.timespan import Timespan

from ..iterables import DataIdIterable, SingleDatasetTypeIterable, DatasetIterable
from ..quantum import Quantum

if TYPE_CHECKING:
    from .database import Database, StaticTablesContext
    from .collections import CollectionManager
    from .quanta import QuantumTableManager


T = TypeVar("T")


class Select:
    """Tag class used to indicate that a field should be returned in
    a SELECT query.
    """
    pass


Select.Or = Union[T, Type[Select]]


class DatasetTableRecords(ABC):
    """An interface that manages the records associated with a particular
    `DatasetType` in a `RegistryLayer`.

    Parameters
    ----------
    datasetType : `DatasetType`
        Dataset type whose records this object manages.
    """
    def __init__(self, datasetType: DatasetType):
        self.datasetType = datasetType

    @abstractmethod
    def insert(self, run: str, dataIds: DataIdIterable, *,
               quantum: Optional[Quantum] = None) -> SingleDatasetTypeIterable:
        """Insert one or more dataset entries into the database.

        Parameters
        ----------
        run : `str`
            The name of the `CollectionType.RUN` collection this dataset will
            be associated with.
        dataIds : `DataIdIterable`
            Expanded data IDs (`ExpandedDataCoordinate` instances) for the
            datasets to be added.   The dimensions of all data IDs must be the
            same as ``self.datasetType.dimensions``.
        quantum : `Quantum`, optional
            The `Quantum` instance that should be recorded as responsible for
            producing this dataset.

        Returns
        -------
        datasets : `SingleDatasetTypeIterable`
            References to the inserted datasets.

        Notes
        -----
        This method does not insert component datasets recursively, as those
        have a different `DatasetType` than their parent and hence are managed
        by a different `DatasetTableRecords` instance.
        """
        raise NotImplementedError()

    @abstractmethod
    def find(self, collection: str, dataId: DataCoordinate,
             timespan: Optional[Timespan[Optional[datetime]]] = None
             ) -> Optional[ResolvedDatasetHandle]:
        """Search a collection for a dataset with the given data ID.

        Parameters
        ----------
        collection : `str`
            Name of the collection to search.
        dataId: `DataCoordinate`
            Complete (but not necessarily expanded) data ID to search with,
            with ``dataId.graph == self.datasetType.dimensions``.
        timespan: `Timespan`, optional
            Time period whose validity range the dataset must overlap.  Must
            be `None` if `collection` is not a `CollectionType.CALIBRATION`
            collection.

        Notes
        -----
        If multiple datasets match the data ID and validity range constraints,
        the one returned is arbitrary; use higher-level interfaces (based on
        `select`) to search for multiple datasets.  Multiple results should
        only be possible for dataset types with `DatasetUniqueness.NONSINGULAR`
        or `CollectionType.CALIBRATION` collections.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, datasets: SingleDatasetTypeIterable):
        """Completely remove a dataset from this layer.

        The dataset must be removed from any datastores before this method is
        called.

        Parameters
        ----------
        datasets : `SingleDatasetTypeIterable`
            Datasets to be associated.  All datasets must be resolved (have
            ``id`` and ``origin`` attributes that are not `None`) and have the
            same `DatasetType` as ``self``.
        """
        raise NotImplementedError()

    @abstractmethod
    def associate(self, collection: str, datasets: SingleDatasetTypeIterable, *,
                  timespan: Optional[Timespan[Optional[datetime]]] = None):
        """Associate one or more datasets with a collection.

        Parameters
        ----------
        collection : `str`
            Name of the collection.  Must refer to a collection registered with
            type `~CollectionType.TAGGED` or `~CollectionType.CALIBRATION`;
            associations with `~CollectionType.RUN` collections are fixed when
            the dataset is inserted.
        datasets : `SingleDatasetTypeIterable`
            Datasets to be associated.  All datasets must be resolved (have
            ``id`` and ``origin`` attributes that are not `None`) and have the
            same `DatasetType` as ``self``.
        timespan : `Timespan`, optional
            Validity ranges for the datasets when associating with a
            `~CollectionType.CALIBRATION` collection.  Must be `None` for
            `~CollectionType.TAGGED` collections.

        Notes
        -----
        Associating a dataset with `~DatasetUniqueness.STANDARD` uniqueness
        into a `~CollectionType.TAGGED` collection that already contains a
        different dataset with the same `DatasetType` and data ID will remove
        the existing dataset from that collection.

        Associating the same dataset into a collection multiple times is a
        no-op, but is still not permitted on read-only databases.
        """
        raise NotImplementedError()

    @abstractmethod
    def disassociate(self, collection: str, datasets: SingleDatasetTypeIterable):
        """Remove one or more datasets from a collection.

        Parameters
        ----------
        collection : `str`
            Name of the collection.  Must refer to a collection registered with
            type `~CollectionType.TAGGED` or `~CollectionType.CALIBRATION`;
            associations with `~CollectionType.RUN` collections are fixed when
            the dataset is inserted.
        datasets : `SingleDatasetTypeIterable`
            Datasets to be associated.  All datasets must be resolved (have
            ``id`` and ``origin`` attributes that are not `None`) and have the
            same `DatasetType` as ``self``.
        """
        raise NotImplementedError()

    @abstractmethod
    def select(self, collection: Select.Or[str] = Select,
               dataId: Select.Or[DataCoordinate] = Select,
               id: Select.Or[int] = Select,
               origin: Select.Or[int] = Select,
               run: Select.Or[str] = Select,
               timespan: Optional[Select.Or[Timespan[datetime]]] = None
               ) -> Optional[sqlalchemy.sql.FromClause]:
        """Return a SQLAlchemy object that represents a ``SELECT`` query for
        this `DatasetType` in this layer.

        All arguments can either be a value that constrains the query or
        the `Select` tag object to indicate that the value should be returned
        in the columns in the ``SELECT`` clause.  The default is `Select`
        (except for `Timespan`).

        Parameters
        ----------
        collection : `str` or `Select`
            The name of the collection to search, or an instruction to return
            the collection ID (see `CollectionManager`) in the selected
            columns as ``collection_id``.
        dataId : `DataCoordinate` or `Select`
            The data ID to restrict results with, or an instruction to return
            the data ID via columns with names
            ``self.datasetType.dimensions.names``.
        id : `int` or `Select`
            The autoincrement primary key value for the dataset, or an
            instruction to return it via a ``dataset_id`` column.
        origin : `int` or `Select`
            The origin primary key value for the dataset, or an instruction to
            return it via a ``dataset_origin`` column.
        run : `str` or `Select`
            The name of the run to search, or an instruction to return
            the run ID (see `CollectionManager`) in the selected
            columns as ``run_id``.
        timespan : `Timespan` or `Select`, optional
            The timespan returned the validity ranges of returned datasets
            must overlap, or an instruction to return those validity ranges
            as ``TIMESPAN_FIELD_SPECS.begin.name`` and
            ``TIMESPAN_FIELD_SPECS.end.name``.  Must be `None` if `collection`
            is not the name of a `~CollectionType.CALIBRATION` collection.

        Returns
        -------
        selectable : `sqlalchemy.sql.FromClause` or `None`
            SQLAlchemy object representing a ``SELECT`` query, or `None` if
            it is known that there are no datasets of this `DatasetType` in
            this layer that match the given constraints.
        """
        raise NotImplementedError()

    datasetType: DatasetType
    """Dataset type whose records this object manages (`DatasetType`).
    """


class DatasetTableManager(ABC):
    """An interface that manages the tables that describe datasets in a
    `RegistryLayer`.

    `DatasetTableManager` primarily serves as a container and factory for
    `DatasetTableRecords` instances, which each provide access to the records
    for a different `DatasetType`.
    """

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *, collections: CollectionManager,
                   quanta: Type[QuantumTableManager], universe: DimensionUniverse) -> DatasetTableManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        schema : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.
        collections: `CollectionManager`
            Manager object for the collections in the same layer.
        quanta: `type`
            The concrete `QuantumTableManager` subclass that will be used to
            manage the quanta in the same layer.  This is a type rather than
            an instance to avoid a circular initialization relationship; the
            expectation is that `initialize` implementations will need to call
            `QuantumTableManager.addQuantumForeignKey` only.
        universe : `DimensionUniverse`
            Universe graph containing dimensions known to this `Registry`.

        Returns
        -------
        manager : `DatasetTableManager`
            An instance of a concrete `DatasetTableManager` subclass.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def addDatasetForeignKey(cls, tableSpec: TableSpec, *, name: str = "dataset",
                             onDelete: Optional[str] = None, **kwds) -> Tuple[FieldSpec, FieldSpec]:
        """Add a foreign key (field and constraint) referencing the dataset
        table.

        Parameters
        ----------
        tableSpec : `TableSpec`
            Specification for the table that should reference the dataset
            table.  Will be modified in place.
        name: `str`, optional
            A name to use for the prefix of the new field; the full names are
            ``{name}_id`` and ``{name}_origin``.
        onDelete: `str`, optional
            One of "CASCADE" or "SET NULL", indicating what should happen to
            the referencing row if the collection row is deleted.  `None`
            indicates that this should be an integrity error.
        kwds
            Additional keyword arguments are forwarded to the `FieldSpec`
            constructor (only the ``name`` and ``dtype`` arguments are
            otherwise provided).

        Returns
        -------
        idSpec : `FieldSpec`
            Specification for the ID field.
        originSpec : `FieldSpec`
            Specification for the origin field.
        """
        raise NotImplementedError()

    @abstractmethod
    def refresh(self, *, universe: DimensionUniverse):
        """Ensure all other operations on this manager are aware of any
        dataset ypes that may have been registered by other clients since
        it was initialized or last refreshed.
        """
        raise NotImplementedError()

    @abstractmethod
    def getRecordsForType(self, datasetType: DatasetType) -> Optional[DatasetTableRecords]:
        """Return an object that provides access to the records associated with
        the given `DatasetType`, if one exists in this layer.

        Parameters
        ----------
        datasetType : `DatasetType`
            Dataset type for which records should be returned.

        Returns
        -------
        records : `DatasetTableRecords` or `None`
            The object representing the records for the given dataset type in
            this layer, or `None` if there are no records for that dataset type
            in this layer.

        Note
        ----
        Dataset types registered by another client of the same layer since
        the last call to `initialize` or `refresh` may not be found.
        """
        raise NotImplementedError()

    @abstractmethod
    def registerType(self, datasetType: DatasetType) -> DatasetTableRecords:
        """Ensure that this layer can hold records for the given `DatasetType`,
        creating new tables as necessary.

        Parameters
        ----------
        datasetType : `DatasetType`
            Dataset type for which a table should created (as necessary) and
            an associated `DatasetTableRecords` returned.

        Returns
        -------
        records : `DatasetTableRecords`
            The object representing the records for the given dataset type in
            this layer.

        Raises
        ------
        TransactionInterruption
            Raised if this operation is invoked within a `Database.transaction`
            context.
        """
        raise NotImplementedError()

    @abstractmethod
    def selectTypes(self) -> sqlalchemy.sql.FromClause:
        """Return a SQLAlchemy object that represents the logical dataset_type
        table in this layer.

        Returns
        -------
        selectable : `sqlalchemy.sql.FromClause`
            SQLAlchemy object that can be used in a ``SELECT`` query.

        Notes
        -----
        The returned object is guaranteed to have (at least) ``name`` and
        ``storage_class`` columns (both `str`), and include all dataset types
        in this layer.
        """
        raise NotImplementedError()

    @abstractmethod
    def iterTypes(self) -> Iterator[DatasetType]:
        """Return an iterator over the the dataset types present in this layer.

        Note
        ----
        Dataset types registered by another client of the same layer since
        the last call to `initialize` or `refresh` may not be included.
        """
        raise NotImplementedError()

    @abstractmethod
    def getHandle(self, id: int, origin: int) -> Optional[ResolvedDatasetHandle]:
        """Return a `ResolvedDatasetHandle` for the given dataset primary key
        values.

        Parameters
        ----------
        id : `int`
            Autoincrement primary key value for the dataset.
        origin : `int`
            Primary key value indicating where this dataset originated.

        Returns
        -------
        handle : `ResolvedDatasetHandle` or `None`
            Object representing the dataset, or `None` if no dataset with the
            given primary key values exists in this layer.
        """
        raise NotImplementedError()

    @abstractmethod
    def insertLocations(self, datastoreName: str, datasets: DatasetIterable, *,
                        ephemeral: bool = False):
        """Record that the given datasets are present in a datastore.

        Parameters
        ----------
        datastoreName : `str`
            Name of the datastore these datasets are being added to.
        datasets: `DatasetIterable`
            Handles for the datasets that are being added.
        ephemeral : `bool`
            If `True` (`False` is default), these associations should persist
            only for the duration of this `Registry` client.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetchLocations(self, dataset: ResolvedDatasetHandle) -> Iterator[str]:
        """Return the names of the datastores in which the given dataset is
        present.

        Parameters
        ----------
        dataset : `ResolvedDatasetHandle`
            The dataset for which datastore information should be retrieved.

        Returns
        -------
        datastoreNames : `~collections.abc.Iterable` of `str`
            Names of datastores.
        """
        raise NotImplementedError()

    @abstractmethod
    def deleteLocations(self, datastoreName: str, datasets: DatasetIterable):
        """Record that the given datasets have been removed from a datastore.

        Parameters
        ----------
        datastoreName : `str`
            Name of the datastore these datasets are being removed from.
        datasets: `DatasetIterable`
            Handles for the datasets that are being removed.
        """
        raise NotImplementedError()
