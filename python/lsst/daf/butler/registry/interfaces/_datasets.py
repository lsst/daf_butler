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

__all__ = ("DatasetRecordStorageManager", "DatasetRecordStorage")

from abc import ABC, abstractmethod
from typing import (
    Any,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    TYPE_CHECKING,
)

from ...core import (
    DataCoordinate,
    DatasetRef,
    DatasetType,
    ddl,
    SimpleQuery,
)

if TYPE_CHECKING:
    from ...core import DimensionUniverse
    from ._database import Database, StaticTablesContext
    from ._collections import CollectionManager, CollectionRecord, RunRecord


class DatasetRecordStorage(ABC):
    """An interface that manages the records associated with a particular
    `DatasetType`.

    Parameters
    ----------
    datasetType : `DatasetType`
        Dataset type whose records this object manages.
    """
    def __init__(self, datasetType: DatasetType):
        self.datasetType = datasetType

    @abstractmethod
    def insert(self, run: RunRecord, dataIds: Iterable[DataCoordinate]) -> Iterator[DatasetRef]:
        """Insert one or more dataset entries into the database.

        Parameters
        ----------
        run : `RunRecord`
            The record object describing the `~CollectionType.RUN` collection
            this dataset will be associated with.
        dataIds : `Iterable` [ `DataCoordinate` ]
            Expanded data IDs (`DataCoordinate` instances) for the
            datasets to be added.   The dimensions of all data IDs must be the
            same as ``self.datasetType.dimensions``.

        Returns
        -------
        datasets : `Iterable` [ `DatasetRef` ]
            References to the inserted datasets.
        """
        raise NotImplementedError()

    @abstractmethod
    def find(self, collection: CollectionRecord, dataId: DataCoordinate) -> Optional[DatasetRef]:
        """Search a collection for a dataset with the given data ID.

        Parameters
        ----------
        collection : `CollectionRecord`
            The record object describing the collection to search for the
            dataset.  May have any `CollectionType`.
        dataId: `DataCoordinate`
            Complete (but not necessarily expanded) data ID to search with,
            with ``dataId.graph == self.datasetType.dimensions``.

        Returns
        -------
        ref : `DatasetRef`
            A resolved `DatasetRef` (without components populated), or `None`
            if no matching dataset was found.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, datasets: Iterable[DatasetRef]) -> None:
        """Fully delete the given datasets from the registry.

        Parameters
        ----------
         datasets : `Iterable` [ `DatasetRef` ]
            Datasets to be deleted.  All datasets must be resolved and have
            the same `DatasetType` as ``self``.

        Raises
        ------
        AmbiguousDatasetError
            Raised if any of the given `DatasetRef` instances is unresolved.
        """
        raise NotImplementedError()

    @abstractmethod
    def associate(self, collection: CollectionRecord, datasets: Iterable[DatasetRef]) -> None:
        """Associate one or more datasets with a collection.

        Parameters
        ----------
        collection : `CollectionRecord`
            The record object describing the collection.  ``collection.type``
            must be `~CollectionType.TAGGED`.
        datasets : `Iterable` [ `DatasetRef` ]
            Datasets to be associated.  All datasets must be resolved and have
            the same `DatasetType` as ``self``.

        Raises
        ------
        AmbiguousDatasetError
            Raised if any of the given `DatasetRef` instances is unresolved.

        Notes
        -----
        Associating a dataset with into collection that already contains a
        different dataset with the same `DatasetType` and data ID will remove
        the existing dataset from that collection.

        Associating the same dataset into a collection multiple times is a
        no-op, but is still not permitted on read-only databases.
        """
        raise NotImplementedError()

    @abstractmethod
    def disassociate(self, collection: CollectionRecord, datasets: Iterable[DatasetRef]) -> None:
        """Remove one or more datasets from a collection.

        Parameters
        ----------
        collection : `CollectionRecord`
            The record object describing the collection.  ``collection.type``
            must be `~CollectionType.TAGGED`.
        datasets : `Iterable` [ `DatasetRef` ]
            Datasets to be disassociated.  All datasets must be resolved and
            have the same `DatasetType` as ``self``.

        Raises
        ------
        AmbiguousDatasetError
            Raised if any of the given `DatasetRef` instances is unresolved.
        """
        raise NotImplementedError()

    @abstractmethod
    def select(self, collection: CollectionRecord,
               dataId: SimpleQuery.Select.Or[DataCoordinate] = SimpleQuery.Select,
               id: SimpleQuery.Select.Or[Optional[int]] = SimpleQuery.Select,
               run: SimpleQuery.Select.Or[None] = SimpleQuery.Select,
               ) -> Optional[SimpleQuery]:
        """Return a SQLAlchemy object that represents a ``SELECT`` query for
        this `DatasetType`.

        All arguments can either be a value that constrains the query or
        the `SimpleQuery.Select` tag object to indicate that the value should
        be returned in the columns in the ``SELECT`` clause.  The default is
        `SimpleQuery.Select`.

        Parameters
        ----------
        collection : `CollectionRecord`
            The record object describing the collection to query.  May not be
            of type `CollectionType.CHAINED`.
        dataId : `DataCoordinate` or `Select`
            The data ID to restrict results with, or an instruction to return
            the data ID via columns with names
            ``self.datasetType.dimensions.names``.
        id : `int`, `Select` or None,
            The integer primary key value for the dataset, an instruction to
            return it via a ``id`` column, or `None` to ignore it
            entirely.
        run : `None` or `Select`
            If `Select` (default), include the dataset's run key value (as
            column labeled with the return value of
            ``CollectionManager.getRunForiegnKeyName``).
            If `None`, do not include this column (to constrain the run,
            pass a `RunRecord` as the ``collection`` argument instead.)

        Returns
        -------
        query : `SimpleQuery` or `None`
            A struct containing the SQLAlchemy object that representing a
            simple ``SELECT`` query, or `None` if it is known that there are
            no datasets of this `DatasetType` that match the given constraints.
        """
        raise NotImplementedError()

    datasetType: DatasetType
    """Dataset type whose records this object manages (`DatasetType`).
    """


class DatasetRecordStorageManager(ABC):
    """An interface that manages the tables that describe datasets.

    `DatasetRecordStorageManager` primarily serves as a container and factory
    for `DatasetRecordStorage` instances, which each provide access to the
    records for a different `DatasetType`.
    """

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, context: StaticTablesContext, *, collections: CollectionManager,
                   universe: DimensionUniverse) -> DatasetRecordStorageManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present.
        collections: `CollectionManager`
            Manager object for the collections in this `Registry`.
        universe : `DimensionUniverse`
            Universe graph containing all dimensions known to this `Registry`.

        Returns
        -------
        manager : `DatasetRecordStorageManager`
            An instance of a concrete `DatasetRecordStorageManager` subclass.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def addDatasetForeignKey(cls, tableSpec: ddl.TableSpec, *,
                             name: str = "dataset", constraint: bool = True, onDelete: Optional[str] = None,
                             **kwargs: Any) -> ddl.FieldSpec:
        """Add a foreign key (field and constraint) referencing the dataset
        table.

        Parameters
        ----------
        tableSpec : `ddl.TableSpec`
            Specification for the table that should reference the dataset
            table.  Will be modified in place.
        name: `str`, optional
            A name to use for the prefix of the new field; the full name is
            ``{name}_id``.
        onDelete: `str`, optional
            One of "CASCADE" or "SET NULL", indicating what should happen to
            the referencing row if the collection row is deleted.  `None`
            indicates that this should be an integrity error.
        constraint: `bool`, optional
            If `False` (`True` is default), add a field that can be joined to
            the dataset primary key, but do not add a foreign key constraint.
        **kwargs
            Additional keyword arguments are forwarded to the `ddl.FieldSpec`
            constructor (only the ``name`` and ``dtype`` arguments are
            otherwise provided).

        Returns
        -------
        idSpec : `ddl.FieldSpec`
            Specification for the ID field.
        """
        raise NotImplementedError()

    @abstractmethod
    def refresh(self, *, universe: DimensionUniverse) -> None:
        """Ensure all other operations on this manager are aware of any
        dataset types that may have been registered by other clients since
        it was initialized or last refreshed.
        """
        raise NotImplementedError()

    def __getitem__(self, name: str) -> DatasetRecordStorage:
        """Return the object that provides access to the records associated
        with the given `DatasetType` name.

        This is simply a convenience wrapper for `find` that raises `KeyError`
        when the dataset type is not found.

        Returns
        -------
        records : `DatasetRecordStorage`
            The object representing the records for the given dataset type.

        Raises
        ------
        KeyError
            Raised if there is no dataset type with the given name.

        Notes
        -----
        Dataset types registered by another client of the same repository since
        the last call to `initialize` or `refresh` may not be found.
        """
        result = self.find(name)
        if result is None:
            raise KeyError(f"Dataset type with name '{name}' not found.")
        return result

    @abstractmethod
    def find(self, name: str) -> Optional[DatasetRecordStorage]:
        """Return an object that provides access to the records associated with
        the given `DatasetType` name, if one exists.

        Parameters
        ----------
        name : `str`
            Name of the dataset type.

        Returns
        -------
        records : `DatasetRecordStorage` or `None`
            The object representing the records for the given dataset type, or
            `None` if there are no records for that dataset type.

        Notes
        -----
        Dataset types registered by another client of the same repository since
        the last call to `initialize` or `refresh` may not be found.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(self, datasetType: DatasetType) -> Tuple[DatasetRecordStorage, bool]:
        """Ensure that this `Registry` can hold records for the given
        `DatasetType`, creating new tables as necessary.

        Parameters
        ----------
        datasetType : `DatasetType`
            Dataset type for which a table should created (as necessary) and
            an associated `DatasetRecordStorage` returned.

        Returns
        -------
        records : `DatasetRecordStorage`
            The object representing the records for the given dataset type.
        inserted : `bool`
            `True` if the dataset type did not exist in the registry before.

        Notes
        -----
        This operation may not be invoked within a `Database.transaction`
        context.
        """
        raise NotImplementedError()

    @abstractmethod
    def __iter__(self) -> Iterator[DatasetType]:
        """Return an iterator over the the dataset types present in this layer.

        Notes
        -----
        Dataset types registered by another client of the same layer since
        the last call to `initialize` or `refresh` may not be included.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDatasetRef(self, id: int, *, universe: DimensionUniverse) -> Optional[DatasetRef]:
        """Return a `DatasetRef` for the given dataset primary key
        value.

        Parameters
        ----------
        id : `int`
            Autoincrement primary key value for the dataset.
        universe : `DimensionUniverse`
            All known dimensions.

        Returns
        -------
        ref : `DatasetRef` or `None`
            Object representing the dataset, or `None` if no dataset with the
            given primary key values exists in this layer.
        """
        raise NotImplementedError()
