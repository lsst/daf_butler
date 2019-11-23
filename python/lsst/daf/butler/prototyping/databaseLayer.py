from __future__ import annotations

__all__ = ["DatabaseLayer"]

from abc import ABC, abstractmethod
from collections import namedtuple
from typing import (
    Optional,
    Tuple,
    Type,
)

import sqlalchemy

from ..core.schema import TableSpec


class DatabaseLayer(ABC):

    def __init__(self, origin: int, connection: sqlalchemy.engine.Connection):
        self.origin = origin
        self.connection = connection

    @abstractmethod
    def isWriteable(self) -> bool:
        pass

    @abstractmethod
    def ensureTableExists(self, name: str, spec: TableSpec) -> sqlalchemy.schema.Table:
        pass

    @abstractmethod
    def getExistingTable(self, name: str) -> sqlalchemy.schema.Table:
        pass

    @abstractmethod
    def sync(self, table: sqlalchemy.schema.Table,
             keys: dict,  # find the matching record with these
             compared: Optional[dict] = None,  # these values must be the same on return
             extra: Optional[dict] = None,  # these values should be used only when inserting
             ) -> sqlalchemy.engine.RowProxy:
        pass

    origin: int
    connection: sqlalchemy.engine.Connection


def makeTableStruct(cls) -> Type[Tuple]:
    """Decorator that transforms a class containing TableSpec declarations
    into a struct whos instances hold the corresponding SQLAlchemy table
    objects.

    The returned class will be a subclass of both the decorated class and a
    `collections.namedtuple` constructed with the names of all class attributes
    of type `TableSpec`.

    Constructing an instance of the returned class with a single
    `DatabaseLayer` argument will call `DatabaseLayer.ensureTableExists` on all
    tables with those schema specifications, yielding a
    `collections.namedtuple` subclass instance whose attributes are
    `sqlalchemy.schema.Table` instances.

    For example::

        @makeTableStruct
        class MyTables:
            table1 = TableSpec(
                fields=[
                    FieldSpec("id", dtype=sqlalchemy.BigInteger,
                              autoincrement=True, primaryKey=True),
                ]
            )
            table2 = TableSpec(
                fields=[
                    FieldSpec("name", dtype=sqlalchemy.String, length=32,
                              primaryKey=True),
                ]
            )

        db: DatabaseLayer = ...  # connect to some database

        # Create declared tables if they don't already exist, and get
        # SQLAlchemy objects representing them regardless.
        tables = MyTables(db)

        # Run a query against one of those tables.
        for row in db.connection.execute(tables.table1.select()).fetchall():
            ...  # do something with results

    """
    specs = {}
    for name, attr in cls.__dict__.items():
        if isinstance(attr, TableSpec):
            specs[name] = attr
    TupleBase = namedtuple("_" + cls.__name__, specs.keys())

    def __new__(cls, db: DatabaseLayer):  # noqa:807
        return cls._make(db.ensureTableExists(name, spec) for name, spec in specs.items())

    return type(cls.__name__, (TupleBase,), {"__new__": __new__})
