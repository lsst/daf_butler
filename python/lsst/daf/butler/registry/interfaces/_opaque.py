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

"""Interfaces for the objects that manage opaque (logical) tables within a
`Registry`.
"""

__all__ = ["OpaqueTableStorageManager", "OpaqueTableStorage"]

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Mapping, Optional

from ...core.ddl import TableSpec
from ._database import Database, StaticTablesContext
from ._versioning import VersionedExtension

if TYPE_CHECKING:
    from ...core.datastore import DatastoreTransaction


class OpaqueTableStorage(ABC):
    """An interface that manages the records associated with a particular
    opaque table in a `Registry`.

    Parameters
    ----------
    name : `str`
        Name of the opaque table.
    """

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def insert(self, *data: dict, transaction: DatastoreTransaction | None = None) -> None:
        """Insert records into the table

        Parameters
        ----------
        *data
            Each additional positional argument is a dictionary that represents
            a single row to be added.
        transaction : `DatastoreTransaction`, optional
            Transaction object that can be used to enable an explicit rollback
            of the insert to be registered. Can be ignored if rollback is
            handled via a different mechanism, such as by a database. Can be
            `None` if no external transaction is available.
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch(self, **where: Any) -> Iterator[Mapping[str, Any]]:
        """Retrieve records from an opaque table.

        Parameters
        ----------
        **where
            Additional keyword arguments are interpreted as equality
            constraints that restrict the returned rows (combined with AND);
            keyword arguments are column names and values are the values they
            must have.

        Yields
        ------
        row : `dict`
            A dictionary representing a single result row.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, columns: Iterable[str], *rows: dict) -> None:
        """Remove records from an opaque table.

        Parameters
        ----------
        columns: `~collections.abc.Iterable` of `str`
            The names of columns that will be used to constrain the rows to
            be deleted; these will be combined via ``AND`` to form the
            ``WHERE`` clause of the delete query.
        *rows
            Positional arguments are the keys of rows to be deleted, as
            dictionaries mapping column name to value.  The keys in all
            dictionaries must be exactly the names in ``columns``.
        """
        raise NotImplementedError()

    name: str
    """The name of the logical table this instance manages (`str`).
    """


class OpaqueTableStorageManager(VersionedExtension):
    """An interface that manages the opaque tables in a `Registry`.

    `OpaqueTableStorageManager` primarily serves as a container and factory for
    `OpaqueTableStorage` instances, which each provide access to the records
    for a different (logical) opaque table.

    Notes
    -----
    Opaque tables are primarily used by `Datastore` instances to manage their
    internal data in the same database that hold the `Registry`, but are not
    limited to this.

    While an opaque table in a multi-layer `Registry` may in fact be the union
    of multiple tables in different layers, we expect this to be rare, as
    `Registry` layers will typically correspond to different leaf `Datastore`
    instances (each with their own opaque table) in a `ChainedDatastore`.
    """

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, context: StaticTablesContext) -> OpaqueTableStorageManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        context : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.

        Returns
        -------
        manager : `OpaqueTableStorageManager`
            An instance of a concrete `OpaqueTableStorageManager` subclass.
        """
        raise NotImplementedError()

    def __getitem__(self, name: str) -> OpaqueTableStorage:
        """Interface to `get` that raises `LookupError` instead of returning
        `None` on failure.
        """
        r = self.get(name)
        if r is None:
            raise LookupError(f"No logical table with name '{name}' found.")
        return r

    @abstractmethod
    def get(self, name: str) -> Optional[OpaqueTableStorage]:
        """Return an object that provides access to the records associated with
        an opaque logical table.

        Parameters
        ----------
        name : `str`
            Name of the logical table.

        Returns
        -------
        records : `OpaqueTableStorage` or `None`
            The object representing the records for the given table in this
            layer, or `None` if there are no records for that table in this
            layer.

        Notes
        -----
        Opaque tables must be registered with the layer (see `register`) by
        the same client before they can safely be retrieved with `get`.
        Unlike most other manager classes, the set of opaque tables cannot be
        obtained from an existing data repository.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(self, name: str, spec: TableSpec) -> OpaqueTableStorage:
        """Ensure that this layer can hold records for the given opaque logical
        table, creating new tables as necessary.

        Parameters
        ----------
        name : `str`
            Name of the logical table.
        spec : `TableSpec`
            Schema specification for the table to be created.

        Returns
        -------
        records : `OpaqueTableStorage`
            The object representing the records for the given element in this
            layer.

        Notes
        -----
        This operation may not be invoked within a transaction context block.
        """
        raise NotImplementedError()
