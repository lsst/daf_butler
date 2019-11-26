from __future__ import annotations

__all__ = ["Database", "SynchronizationConflict"]

from abc import ABC, abstractmethod
from collections import namedtuple
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    Type,
)

import sqlalchemy

from ...core.schema import TableSpec


class SynchronizationConflict(RuntimeError):
    """Exception raised when an attempt to synchronize rows fails because
    the existing row has different values than the one that would be
    inserted.
    """


class Database(ABC):

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
             *,
             keys: Dict[str, Any],
             compared: Optional[Dict[str, Any]] = None,
             extra: Optional[Dict[str, Any]] = None,
             returning: Optional[Iterable[str]] = None,
             ) -> Tuple[Optional[Dict[str, Any], bool]]:
        """Insert into a table as necessary to ensure database contains
        values equivalent to the given ones.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table to be queried and possibly inserted into.
        keys : `dict`
            Column name-value pairs used to search for an existing row; must
            be a combination that can be used to select a single row if one
            exists.  If such a row does not exist, these values are used in
            the insert.
        compared : `dict`, optional
            Column name-value pairs that are compared to those in any existing
            row.  If such a row does not exist, these rows are used in the
            insert.
        extra : `dict`, optional
            Column name-value pairs that are ignored if a matching row exists,
            but used in an insert if one is necessary.
        returning : `str`, optional
            The names of columns whose values should be returned.

        Returns
        -------
        row : `dict`, optional
            The value of the fields indicated by ``returning``, or `None` if
            ``returning`` is `None`.
        inserted : `bool`
            If `True`, a new row was inserted.

        Raises
        ------
        SynchronizationConflict
            Raised if the values in ``compared`` do match the values in the
            database.
        """
        pass

    @abstractmethod
    def insert(self, table: sqlalchemy.schema.Table, *rows: dict, returning: Optional[str] = None
               ) -> Optional[Iterator[int]]:
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
    `Database` argument will call `Database.ensureTableExists` on all
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

        db: Database = ...  # connect to some database

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

    def __new__(cls, db: Database):  # noqa:807
        return cls._make(db.ensureTableExists(name, spec) for name, spec in specs.items())

    return type(cls.__name__, (TupleBase,), {"__new__": __new__})
