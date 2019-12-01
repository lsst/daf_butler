from __future__ import annotations

__all__ = ["Database", "SynchronizationConflict"]

from abc import ABC, abstractmethod
from collections import namedtuple
from contextlib import contextmanager
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
)

import sqlalchemy

from ...core.schema import TableSpec, FieldSpec, ForeignKeySpec


class SynchronizationConflict(RuntimeError):
    """Exception raised when an attempt to synchronize rows fails because
    the existing row has different values than the one that would be
    inserted.
    """


class TransactionInterruption(RuntimeError):
    """Exception raised when a database operation that will interrupt a
    transaction is performed in the middle of a transaction.

    This is considered a logic error that should result in reordering calls to
    `Database` methods.  It is raised whenever a transaction is active that
    *could* be interrupted, regardless of whether it actually would be given a
    particular `Database` engine or state, to ensure code depending on
    `Database` does not accidentally depend on an implementation.
    """


class StaticTableSchema:
    """Helper class used to declare the static schema for a registry layer
    in a database.

    An instance of this class is returned by `Database.declareStaticTables`,
    which should be the only way this class is ever used.
    """

    def __init__(self, db: Database):
        self._db = db

    def add(self, name: str, spec: TableSpec) -> sqlalchemy.schema.Table:
        """Add a new table to the schema, returning its sqlalchemy
        representation.

        The new table may not actually be created until the end of the
        context created by `Database.declareStaticTables`, allowing tables
        to be declared in any order even in the presence of foreign key
        relationships.
        """
        return self._db._convertTableSpec(name, spec, self._db.metadata)


class Database(ABC):
    """An abstract interface that represents a particular database engine's
    representation of a single schema/namespace/database.

    Parameters
    ----------
    origin : `int`
        An integer ID that should be used as the default for any datasets,
        quanta, or other entities that use a (autoincrement, origin) compound
        primary key.
    engine: `sqlalchemy.engine.Engine`.
        SQLAlchemy engine object.  May be shared with other `Database`
        instances.
    connection : `sqlalchemy.engine.Connection`
        SQLAlchemy connection object.  May be shared with other `Database`
        instances.
    """

    def __init__(self, *, origin: int,
                 engine: sqlalchemy.engine.Engine,
                 connection: sqlalchemy.engine.Connection):
        self.origin = origin
        self._engine = engine
        self._connection = connection
        self._metadata = None
        self._transactions = []

    @contextmanager
    def transaction(self, *, savepoint: bool = False) -> None:
        """Return a context manager that represents a transaction.

        Parameters
        ----------
        savepoint : `bool`
            If `True`, issue a ``SAVEPOINT`` command that allows this
            transaction to be rolled back or committed independent of any
            outer transaction.  If `False` (default), transactions may still
            be nested, but nothing will be committed until the outermost
            transaction context ends.
        """
        if savepoint:
            trans = self._connection.begin_nested()
        else:
            trans = self._connection.begin()
        self._transactions.append(trans)
        try:
            yield
            trans.commit()
        except BaseException:
            trans.rollback()
            raise
        finally:
            self._transactions.pop()

    @contextmanager
    def declareStaticTables(self, *, create: bool) -> StaticTableSchema:
        """Return a context manager in which the database's static DDL schema
        can be declared.

        Parameters
        ----------
        create : `bool` If `True`, attempt to create all tables at the end of
            the context.  If `False`, they will be assumed to already exist.

        Returns
        -------
        schema : `StaticTableSchema` A helper object that is used to add new
            tables.

        Example
        -------
        Given a `Database` instance ``db``::

            with db.declareStaticTables(create=True) as schema:
                schema.add("table1", TableSpec(...))
                schema.add("table2", TableSpec(...))

        Notes
        -----
        A database's static DDL schema must be declared before any dynamic
        tables are managed via calls to `ensureTableExists` or
        `getExistingTable`.  The order in which static schema tables are added
        inside the context block is unimportant; they will automatically be
        sorted and added in an order consistent with their foreign key
        relationships.
        """
        self._metadata = sqlalchemy.MetaData()
        try:
            yield StaticTableSchema(self)
            if create:
                self._metadata.create_all(self._engine)
        except BaseException:
            self._metadata = None
            raise

    @abstractmethod
    def isWriteable(self) -> bool:
        """Return `True` if this database can be modified by this client.
        """
        raise NotImplementedError()

    @abstractmethod
    def _convertFieldSpec(self, spec: FieldSpec, metadata: sqlalchemy.MetaData,
                          **kwds) -> sqlalchemy.schema.Column:
        """Convert a `FieldSpec` to a `sqlalchemy.schema.Column`.

        Parameters
        ----------
        spec : `FieldSpec`
            Specification for the field to be added.
        metadata : `sqlalchemy.MetaData`
            SQLAlchemy representation of the DDL schema this field's table is
            being added to.
        kwds
            Additional keyword arguments to forward to the
            `sqlalchemy.schema.Column` constructor.  This is provided to make
            it easier for derived classes to delegate to ``super()`` while
            making only minor changes.

        Returns
        -------
        column : `sqlalchemy.schema.Column`
            SQLAlchemy representation of the field.
        """
        # TODO: provide default implementation.
        raise NotImplementedError()

    @abstractmethod
    def _convertForeignKeySpec(self, spec: ForeignKeySpec, metadata: sqlalchemy.MetaData,
                               **kwds) -> sqlalchemy.schema.ForeignKeyConstraint:
        """Convert a `ForeignKeySpec` to a
        `sqlalchemy.schema.ForeignKeyConstraint`.

        Parameters
        ----------
        spec : `ForeignKeySpec`
            Specification for the foreign key to be added.
        metadata : `sqlalchemy.MetaData`
            SQLAlchemy representation of the DDL schema this constraint is
            being added to.
        kwds
            Additional keyword arguments to forward to the
            `sqlalchemy.schema.ForeignKeyConstraint` constructor.  This is
            provided to make it easier for derived classes to delegate to
            ``super()`` while making only minor changes.

        Returns
        -------
        constraint : `sqlalchemy.schema.ForeignKeyConstraint`
            SQLAlchemy representation of the constraint.
        """
        # TODO: provide default implementation.
        raise NotImplementedError()

    @abstractmethod
    def _convertTableSpec(self, name: str, spec: TableSpec, metadata: sqlalchemy.MetaData,
                          **kwds) -> sqlalchemy.schema.Table:
        """Convert a `TableSpec` to a `sqlalchemy.schema.Table`.

        Parameters
        ----------
        spec : `TableSpec`
            Specification for the foreign key to be added.
        metadata : `sqlalchemy.MetaData`
            SQLAlchemy representation of the DDL schema this table is being
            added to.
        kwds
            Additional keyword arguments to forward to the
            `sqlalchemy.schema.Table` constructor.  This is provided to make it
            easier for derived classes to delegate to ``super()`` while making
            only minor changes.

        Returns
        -------
        table : `sqlalchemy.schema.Table`
            SQLAlchemy representation of the table.
        """
        # TODO: provide default implementation.
        raise NotImplementedError()

    def ensureTableExists(self, name: str, spec: TableSpec) -> sqlalchemy.sql.FromClause:
        """Ensure that a table with the given name and specification exists,
        creating it if necessary.

        Parameters
        ----------
        name : `str`
            Name of the table (not including namespace qualifiers).
        spec : `TableSpec`
            Specification for the table.  This will be used when creating the
            table, and *may* be used when obtaining an existing table to check
            for consistency, but no such check is guaranteed.

        Returns
        -------
        table : `sqlalchemy.schema.Table`
            SQLAlchemy representation of the table.

        Raises
        ------
        TransactionInterruption
            Raised if a transaction is active when this method is called.

        Notes
        -----
        This method may not be called within transactions.  It may be called on
        read-only databases if and only if the table does in fact already
        exist.

        Subclasses may override this method, but usually should not need to.
        """
        if self._transaction:
            raise TransactionInterruption("Table creation interrupts transactions.")
        assert self._metadata is not None, "Static tables must be declared before dynamic tables."
        # TODO: should we pass any kwargs down to the `sqlalchemy.schema.Table`
        # call here?
        table = self._convertTableSpec(name, spec, self._metadata)
        # TODO: does the the line below work if the table exists but the
        # database is read-only?
        table.create(self._engine, checkfirst=True)
        return table

    def getExistingTable(self, name: str, spec: TableSpec) -> sqlalchemy.schema.Table:
        """Obtain an existing table with the given name and specification.

        Parameters
        ----------
        name : `str`
            Name of the table (not including namespace qualifiers).
        spec : `TableSpec`
            Specification for the table.  This will be used when creating the
            SQLAlchemy representation of the table, and it *may* be used to
            check that the actual table in the database is consistent, but no
            such check is guaranteed.

        Returns
        -------
        table : `sqlalchemy.schema.Table`
            SQLAlchemy representation of the table.

        Notes
        -----
        This method can be called within transactions and never modifies the
        database.

        Subclasses may override this method, but usually should not need to.
        """
        assert self._metadata is not None, "Static tables must be declared before dynamic tables."
        # TODO: should we pass any kwargs down to the `sqlalchemy.schema.Table`
        # call here?
        return self._convertTableSpec(name, spec, self._metadata)

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
        TransactionInterruption
            Raised if a transaction is active when this method is called.
        SynchronizationConflict
            Raised if the values in ``compared`` do match the values in the
            database.

        Notes
        -----
        This method may not be called within transactions.  It may be called on
        read-only databases if and only if the matching row does in fact
        already exist.

        Subclasses should override `_sync` instead of this method, which only
        provides a thin layer of error-checking and argument normalization.
        """
        if self._transaction:
            raise TransactionInterruption("Row synchronization interrupts transactions.")
        return self._sync(table, keys=keys if keys else {},
                          compared=compared if compared else {},
                          extra=extra, returning=returning)

    @abstractmethod
    def _sync(self, table: sqlalchemy.schema.Table,
              *,
              keys: Dict[str, Any],
              compared: Dict[str, Any],
              extra: Dict[str, Any],
              returning: Optional[Iterable[str]] = None
              ) -> Tuple[Optional[Dict[str, Any], bool]]:
        """Protected implementation for `sync`.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table to be queried and possibly inserted into.
        keys : `dict`
            Column name-value pairs used to search for an existing row; must
            be a combination that can be used to select a single row if one
            exists.  If such a row does not exist, these values are used in
            the insert.
        compared : `dict`
            Column name-value pairs that are compared to those in any existing
            row.  If such a row does not exist, these rows are used in the
            insert.
        extra : `dict`
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

        Notes
        -----
        Implementations may assume that a transaction is not active when this
        method is called, as this is guaranteed by `sync` before it delegates
        to this method.  Implementation may begin (and end!) their own
        transactions internally.
        """
        pass

    def insert(self, table: sqlalchemy.schema.Table, *rows: dict, returnIds: bool = False,
               ) -> Optional[List[int]]:
        """Insert one or more rows into a table, optionally returning
        autoincrement primary key values.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table rows should be inserted into.
        returnIds: `bool`
            If `True` (`False` is default), return the values of the table's
            autoincrement primary key field (which much exist).
        rows
            Positional arguments are the rows to be inserted, as dictionaries
            mapping column name to value.  The keys in all dictionaries must
            be the same.

        Returns
        -------
        ids : `None`, or `list` of `int`
            If ``returnIds`` is `True`, a `list` containing the inserted
            values for the table's autoincrement primary key.

        Notes
        -----
        The default implementation uses bulk insert syntax when ``returning``
        is `None`, and a loop over single-row insert operations when it is not.

        Derived classes should reimplement when they can provide a more
        efficient implementation (especially for the latter case).

        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.
        """
        if not returnIds:
            self._connection.execute(table.insert(), *rows)
        else:
            ids = []
            for row in rows:
                primaryKey = self._connection.execute(table.insert(), row).inserted_primary_key
                id = primaryKey[0]
                if isinstance(id, list):  # TODO: is this check really needed?
                    id = id[0]
                ids.append(id)
            return ids

    @abstractmethod
    def replace(self, table: sqlalchemy.schema.Table, *rows: dict):
        """Insert one or more rows into a table, replacing any existing rows
        for which insertion of a new row would violate a unique constraint.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table rows should be inserted into.
        rows
            Positional arguments are the rows to be inserted, as dictionaries
            mapping column name to value.  The keys in all dictionaries must
            be the same.

        Notes
        -----
        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.
        """
        raise NotImplementedError()

    def delete(self, table: sqlalchemy.schema.Table, columns: Iterable[str], *rows: dict) -> int:
        """Delete one or more rows from a table.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table that rows should be deleted from.
        keys: `~collections.abc.Iterable` of `str`
            The names of columns that will be used to constrain the rows to
            be deleted; these will be combined via ``AND`` to form the
            ``WHERE`` clause of the delete query.
        rows
            Positional arguments are the keys of rows to be deleted, as
            dictionaries mapping column name to value.  The keys in all
            dictionaries must exactly the names in ``keys``.

        Return
        ------
        count : `int`
            Number of rows deleted.

        Notes
        -----
        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.

        The default implementation should be sufficient for most derived
        classes.
        """
        sql = table.delete().where(
            sqlalchemy.sql.and_(
                *[table.columns[name] == sqlalchemy.sql.bindparam(name) for name in columns]
            )
        )
        return self._connection.execute(sql, *rows).rowcount

    def update(self, table: sqlalchemy.schema.Table, keys: Iterable[str], values: Iterable[str],
               *rows: dict) -> int:
        """Update one or more rows in a table.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table containing the rows to be updated.
        keys : `~collections.abc.Iterable` of `str`
            The names of columns that will be used to search for existing
            rows.
        values : `~collections.abc.Iterable` of `str`
            The names of columns to be updated.
        rows
            Positional arguments are the rows to be updated, as dictionaries
            mapping column name to value.  The keys in all dictionaries must
            be the combination of the names in ``keys`` and ``values``.

        Returns
        -------
        count : `int`
            Number of rows matched (regardless of whether the update actually
            modified them).

        Notes
        -----
        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.

        The default implementation should be sufficient for most derived
        classes.
        """
        sql = table.update().values(
            {table.columns[name]: sqlalchemy.sql.bindparam(name) for name in values}
        ).where(
            sqlalchemy.sql.and_(*[table.columns[name] == sqlalchemy.sql.bindparam(name) for name in keys])
        )
        return self._connection.execute(sql, *rows).rowcount

    def query(self, sql: sqlalchemy.sql.FromClause, *args, **kwds) -> sqlalchemy.engine.ResultProxy:
        """Run a SELECT query against the database.

        Parameters
        ----------
        sql : `sqlalchemy.sql.FromClause`
            A SQLAlchemy representation of a ``SELECT`` query.
        args
            Additional positional arguments are forwarded to
            `sqlalchemy.engine.Connection.execute`.
        kwds
            Additional keyword arguments are forwarded to
            `sqlalchemy.engine.Connection.execute`.

        Returns
        -------
        result : `sqlalchemy.engine.ResultProxy`
            Query results.

        Notes
        -----
        The default implementation should be sufficient for most derived
        classes.
        """
        # TODO: should we guard against non-SELECT queries here?
        return self._connection.execute(sql, *args, **kwds)

    origin: int


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
        with db.declareStaticTables(create=True) as schema:
            tables = MyTables(schema)

        # Run a query against one of those tables.
        for row in db.connection.execute(tables.table1.select()).fetchall():
            ...  # do something with results

    """
    specs = {}
    for name, attr in cls.__dict__.items():
        if isinstance(attr, TableSpec):
            specs[name] = attr
    TupleBase = namedtuple("_" + cls.__name__, specs.keys())

    def __new__(cls, helper: StaticTableSchema):  # noqa:807
        return cls._make(helper.add(name, spec) for name, spec in specs.items())

    return type(cls.__name__, (TupleBase,), {"__new__": __new__})
