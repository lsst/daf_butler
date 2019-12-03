from __future__ import annotations

__all__ = ["OpaqueTableManager", "OpaqueTableRecords"]

from abc import ABC, abstractmethod
from typing import (
    Any,
    Iterator,
    Optional,
)

from ...core.schema import TableSpec
from .database import Database, StaticTablesContext


class OpaqueTableRecords(ABC):
    """An interface that manages the records associated with a particular
    opaque table in a `RegistryLayer`.

    Parameters
    ----------
    name : `str`
        Name of the opaque table.
    """
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def insert(self, *data: dict):
        raise NotImplementedError()

    @abstractmethod
    def fetch(self, **where: Any) -> Iterator[dict]:
        raise NotImplementedError()

    @abstractmethod
    def delete(self, **where: Any):
        raise NotImplementedError()

    name: str


class OpaqueTableManager(ABC):
    """An interface that manages the opaque tables in a `RegistryLayer`.

    `OpaqueTableManager` primarily serves as a container and factory for
    `OpaqueTableRecords` instances, which each provide access to the records
    for a different (logical) opaque table.

    Notes
    -----
    Opaque tables are primarily used by `Datastore` instances to manage their
    internal data in the same database that hold the `Registry`, but are not
    limited to this.

    While an opaque table in a multi-layers `Registry` may in fact be the union
    of multiple tables in different layers, we expect this to be rare, as
    `Registry` layers will typically correspond to different leaf `Datastore`
    instances (each with their own opaque table) in a `ChainedDatastore`.
    """

    @classmethod
    @abstractmethod
    def initialize(cls, db: Database, context: StaticTablesContext) -> OpaqueTableManager:
        """Construct an instance of the manager.

        Parameters
        ----------
        db : `Database`
            Interface to the underlying database engine and namespace.
        schema : `StaticTablesContext`
            Context object obtained from `Database.declareStaticTables`; used
            to declare any tables that should always be present in a layer
            implemented with this manager.

        Returns
        -------
        manager : `OpaqueTableManager`
            An instance of a concrete `OpaqueTableManager` subclass.
        """
        raise NotImplementedError()

    @abstractmethod
    def get(self, name: str) -> Optional[OpaqueTableRecords]:
        """Return an object that provides access to the records associated with
        an opaque logical table.

        Parameters
        ----------
        name : `str`
            Name of the logical table.

        Returns
        -------
        records : `OpaqueTableRecords` or `None`
            The object representing the records for the given table in this
            layer, or `None` if there are no records for that table in this
            layer.

        Note
        ----
        Opaque tables must be registered with the layer (see `register`) by
        the same client before they can safely be retrieved with `get`.
        Unlike most other manager classes, the set of opaque tables cannot be
        obtained from an existing data repository.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(self, name: str, spec: TableSpec) -> OpaqueTableRecords:
        """Ensure that this layer can hold records for the given opaque logical
        table, creating new tables as necessary.

        Parameters
        ----------
        name : `str`
            Name of the logical table.

        Returns
        -------
        records : `OpaqueTableRecords`
            The object representing the records for the given element in this
            layer.

        Raises
        ------
        TransactionInterruption
            Raised if this operation is invoked within a `Database.transaction`
            context.
        """
        raise NotImplementedError()
