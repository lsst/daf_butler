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

__all__ = [
    "Database",
    "ReadOnlyDatabaseError",
    "StaticTablesContext",
    "TransactionInterruption",
]

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

import sqlalchemy

from .. import ddl


def _checkExistingTableDefinition(name: str, spec: ddl.TableSpec, inspection: Dict[str, Any]):
    """Test that the definition of a table in a `ddl.TableSpec` and from
    database introspection are consistent.

    Parameters
    ----------
    name : `str`
        Name of the table (only used in error messages).
    spec : `ddl.TableSpec`
        Specification of the table.
    inspection : `dict`
        Dictionary returned by
        `sqlalchemy.engine.reflection.Inspector.get_columns`.

    Raises
    ------
    RuntimeError
        Raised if the definitions are inconsistent.
    """
    columnNames = [c["name"] for c in inspection]
    if spec.fields.names != set(columnNames):
        raise RuntimeError(f"Table '{name}' exists but is defined differently in the database; "
                           f"specification has columns {list(spec.fields.names)}, while the "
                           f"table in the database has {columnNames}.")


class TransactionInterruption(RuntimeError):
    """Exception raised when a database operation that will interrupt a
    transaction is performed in the middle of a transaction.

    This is considered a logic error that should result in reordering calls to
    `Database` methods.  It is raised whenever a transaction is active that
    *could* be interrupted, regardless of whether it actually would be given a
    particular database engine or state, to ensure code depending on `Database`
    does not accidentally depend on an implementation.
    """


class ReadOnlyDatabaseError(RuntimeError):
    """Exception raised when a write operation is called on a read-only
    `Database`.
    """


class StaticTablesContext:
    """Helper class used to declare the static schema for a registry layer
    in a database.

    An instance of this class is returned by `Database.declareStaticTables`,
    which should be the only way it should be constructed.
    """

    def __init__(self, db: Database):
        self._db = db
        self._foreignKeys = []
        self._inspector = sqlalchemy.engine.reflection.Inspector(self._db._engine)
        self._tableNames = frozenset(self._inspector.get_table_names(schema=self._db.namespace))

    def addTable(self, name: str, spec: ddl.TableSpec) -> sqlalchemy.schema.Table:
        """Add a new table to the schema, returning its sqlalchemy
        representation.

        The new table may not actually be created until the end of the
        context created by `Database.declareStaticTables`, allowing tables
        to be declared in any order even in the presence of foreign key
        relationships.
        """
        if name in self._tableNames:
            _checkExistingTableDefinition(name, spec, self._inspector.get_columns(name,
                                                                                  schema=self._db.namespace))
        table = self._db._convertTableSpec(name, spec, self._db._metadata)
        for foreignKeySpec in spec.foreignKeys:
            self._foreignKeys.append(
                (table, self._db._convertForeignKeySpec(name, foreignKeySpec, self._db._metadata))
            )
        return table

    def addTableTuple(self, specs: Tuple[ddl.TableSpec, ...]) -> Tuple[sqlalchemy.schema.Table, ...]:
        """Add a named tuple of tables to the schema, returning their
        SQLAlchemy representations in a named tuple of the same type.

        The new tables may not actually be created until the end of the
        context created by `Database.declareStaticTables`, allowing tables
        to be declared in any order even in the presence of foreign key
        relationships.

        Notes
        -----
        ``specs`` *must* be an instance of a type created by
        `collections.namedtuple`, not just regular tuple, and the returned
        object is guaranteed to be the same.  Because `~collections.namedtuple`
        is just a factory for `type` objects, not an actual type itself,
        we cannot represent this with type annotations.
        """
        return specs._make(self.addTable(name, spec) for name, spec in zip(specs._fields, specs))


class Database(ABC):
    """An abstract interface that represents a particular database engine's
    representation of a single schema/namespace/database.

    Parameters
    ----------
    origin : `int`
        An integer ID that should be used as the default for any datasets,
        quanta, or other entities that use a (autoincrement, origin) compound
        primary key.
    namespace : `str`, optional
        Name of the schema or namespace this instance is associated with.
        This is passed as the ``schema`` argument when constructing a
        `sqlalchemy.schema.MetaData` instance.  We use ``namespace`` instead to
        avoid confusion between "schema means namespace" and "schema means
        table definitions".
    engine: `sqlalchemy.engine.Engine`.
        SQLAlchemy engine object.  May be shared with other `Database`
        instances.
    connection : `sqlalchemy.engine.Connection`, optional
        SQLAlchemy connection object.  May be shared with other `Database`
        instances.  Constructed from ``engine`` if not provided.

    Notes
    -----
    `Database` requires all write operations to go through its special named
    methods.  Our write patterns are sufficiently simple that we don't really
    need the full flexibility of SQL insert/update/delete syntax, and we need
    non-standard (but common) functionality in these operations sufficiently
    often that it seems worthwhile to provide our own generic API.

    In contrast, `Database.query` allows arbitrary ``SELECT`` queries (via
    their SQLAlchemy representation) to be run, as we expect these to require
    significantly more sophistication while still being limited to standard
    SQL.

    `Database` itself has several underscore-prefixed attributes:

     - ``_engine``: the `sqlalchemy.engine.Engine` object
     - ``_connection``: the `sqlachemy.engine.Connection` object
     - ``_metadata``: the `sqlalchemy.schema.MetaData` object
     - ``_transactions``: a list of active `sqlalchemy.engine.Transaction`
       objects

    These are considered protected (derived classes may access them, but other
    code should not), and read-only, aside from executing SQL via
    ``_connection``.
    """

    def __init__(self, *, origin: int, namespace: Optional[str] = None,
                 engine: sqlalchemy.engine.Engine,
                 connection: Optional[sqlalchemy.engine.Connection] = None):
        self.origin = origin
        self.namespace = namespace
        self._engine = engine
        self._connection = connection if connection is not None else engine.connect()
        self._metadata = None
        self._transactions = []

    @contextmanager
    def transaction(self, *, savepoint: bool = False, interrupting: bool = False) -> None:
        """Return a context manager that represents a transaction.

        Parameters
        ----------
        savepoint : `bool`
            If `True`, issue a ``SAVEPOINT`` command that allows child
            transaction blocks that also use ``savepoint=True`` to roll back
            without rolling back this transaction.
        interrupting : `bool`
            If `True`, this transaction block needs to be able to interrupt
            any existing one in order to yield correct behavior, and hence
            `TransactionInterruption` should be raised if this is not the
            outermost transaction.

        Raises
        ------
        TransactionInterruption
            Raised if ``interrupting`` is `True` and a transaction is already
            active.
        """
        if interrupting and self._transactions:
            raise TransactionInterruption("Logic error in transaction nesting: an operation that would "
                                          "the active transation context has been requested.")
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
    def declareStaticTables(self, *, create: bool) -> StaticTablesContext:
        """Return a context manager in which the database's static DDL schema
        can be declared.

        Parameters
        ----------
        create : `bool`
            If `True`, attempt to create all tables at the end of the context.
            If `False`, they will be assumed to already exist.

        Returns
        -------
        schema : `StaticTablesContext`
            A helper object that is used to add new tables.

        Raises
        ------
        ReadOnlyDatabaseError
            Raised if ``create`` is `True`, `Database.isWriteable` is `False`,
            and one or more declared tables do not already exist.

        Example
        -------
        Given a `Database` instance ``db``::

            with db.declareStaticTables(create=True) as schema:
                schema.addTable("table1", TableSpec(...))
                schema.addTable("table2", TableSpec(...))

        Notes
        -----
        A database's static DDL schema must be declared before any dynamic
        tables are managed via calls to `ensureTableExists` or
        `getExistingTable`.  The order in which static schema tables are added
        inside the context block is unimportant; they will automatically be
        sorted and added in an order consistent with their foreign key
        relationships.
        """
        if create and not self.isWriteable():
            raise ReadOnlyDatabaseError(f"Cannot create tables in read-only database {self}.")
        self._metadata = sqlalchemy.MetaData(schema=self.namespace)
        try:
            context = StaticTablesContext(self)
            yield context
            for table, foreignKey in context._foreignKeys:
                table.append_constraint(foreignKey)
            if create:
                if self.namespace is not None:
                    if self.namespace not in context._inspector.get_schema_names():
                        self._engine.execute(sqlalchemy.schema.CreateSchema(self.namespace))
                self._metadata.create_all(self._engine)
        except BaseException:
            self._metadata.drop_all(self._engine)
            self._metadata = None
            raise

    @abstractmethod
    def isWriteable(self) -> bool:
        """Return `True` if this database can be modified by this client.
        """
        raise NotImplementedError()

    @abstractmethod
    def __str__(self) -> str:
        """Return a human-readable identifier for this `Database`, including
        any namespace or schema that identifies its names within a `Registry`.
        """
        raise NotImplementedError()

    def shrinkDatabaseEntityName(self, original: str) -> str:
        """Return a version of the given name that fits within this database
        engine's length limits for table, constraint, indexes, and sequence
        names.

        Implementations should not assume that simple truncation is safe,
        because multiple long names often begin with the same prefix.

        The default implementation simply returns the given name.

        Parameters
        ----------
        original : `str`
            The original name.

        Returns
        -------
        shrunk : `str`
            The new, possibly shortened name.
        """
        return original

    def expandDatabaseEntityName(self, shrunk: str) -> str:
        """Retrieve the original name for a database entity that was too long
        to fit within the database engine's limits.

        Parameters
        ----------
        original : `str`
            The original name.

        Returns
        -------
        shrunk : `str`
            The new, possibly shortened name.
        """
        return shrunk

    def _convertFieldSpec(self, table: str, spec: ddl.FieldSpec, metadata: sqlalchemy.MetaData,
                          **kwds) -> sqlalchemy.schema.Column:
        """Convert a `FieldSpec` to a `sqlalchemy.schema.Column`.

        Parameters
        ----------
        table : `str`
            Name of the table this column is being added to.
        spec : `FieldSpec`
            Specification for the field to be added.
        metadata : `sqlalchemy.MetaData`
            SQLAlchemy representation of the DDL schema this field's table is
            being added to.
        **kwds
            Additional keyword arguments to forward to the
            `sqlalchemy.schema.Column` constructor.  This is provided to make
            it easier for derived classes to delegate to ``super()`` while
            making only minor changes.

        Returns
        -------
        column : `sqlalchemy.schema.Column`
            SQLAlchemy representation of the field.
        """
        args = [spec.name, spec.getSizedColumnType()]
        if spec.autoincrement:
            # Generate a sequence to use for auto incrementing for databases
            # that do not support it natively.  This will be ignored by
            # sqlalchemy for databases that do support it.
            args.append(sqlalchemy.Sequence(self.shrinkDatabaseEntityName(f"{table}_seq_{spec.name}"),
                                            metadata=metadata))
        return sqlalchemy.schema.Column(*args, nullable=spec.nullable, primary_key=spec.primaryKey,
                                        comment=spec.doc, **kwds)

    def _convertForeignKeySpec(self, table: str, spec: ddl.ForeignKeySpec, metadata: sqlalchemy.MetaData,
                               **kwds) -> sqlalchemy.schema.ForeignKeyConstraint:
        """Convert a `ForeignKeySpec` to a
        `sqlalchemy.schema.ForeignKeyConstraint`.

        Parameters
        ----------
        table : `str`
            Name of the table this foreign key is being added to.
        spec : `ForeignKeySpec`
            Specification for the foreign key to be added.
        metadata : `sqlalchemy.MetaData`
            SQLAlchemy representation of the DDL schema this constraint is
            being added to.
        **kwds
            Additional keyword arguments to forward to the
            `sqlalchemy.schema.ForeignKeyConstraint` constructor.  This is
            provided to make it easier for derived classes to delegate to
            ``super()`` while making only minor changes.

        Returns
        -------
        constraint : `sqlalchemy.schema.ForeignKeyConstraint`
            SQLAlchemy representation of the constraint.
        """
        name = self.shrinkDatabaseEntityName(
            "_".join(["fkey", table, spec.table] + list(spec.target) + list(spec.source))
        )
        return sqlalchemy.schema.ForeignKeyConstraint(
            spec.source,
            [f"{spec.table}.{col}" for col in spec.target],
            name=name,
            ondelete=spec.onDelete
        )

    def _convertTableSpec(self, name: str, spec: ddl.TableSpec, metadata: sqlalchemy.MetaData,
                          **kwds) -> sqlalchemy.schema.Table:
        """Convert a `TableSpec` to a `sqlalchemy.schema.Table`.

        Parameters
        ----------
        spec : `TableSpec`
            Specification for the foreign key to be added.
        metadata : `sqlalchemy.MetaData`
            SQLAlchemy representation of the DDL schema this table is being
            added to.
        **kwds
            Additional keyword arguments to forward to the
            `sqlalchemy.schema.Table` constructor.  This is provided to make it
            easier for derived classes to delegate to ``super()`` while making
            only minor changes.

        Returns
        -------
        table : `sqlalchemy.schema.Table`
            SQLAlchemy representation of the table.

        Notes
        -----
        This method does not handle ``spec.foreignKeys`` at all, in order to
        avoid circular dependencies.  These are added by higher-level logic in
        `ensureTableExists`, `getExistingTable`, and `declareStaticTables`.
        """
        args = [self._convertFieldSpec(name, fieldSpec, metadata) for fieldSpec in spec.fields]
        args.extend(
            sqlalchemy.schema.UniqueConstraint(
                *columns,
                name=self.shrinkDatabaseEntityName("_".join([name, "unq"] + list(columns)))
            )
            for columns in spec.unique if columns not in spec.indexes
        )
        args.extend(
            sqlalchemy.schema.Index(
                self.shrinkDatabaseEntityName("_".join([name, "idx"] + list(columns))),
                *columns,
                unique=(columns in spec.unique)
            )
            for columns in spec.indexes
        )
        return sqlalchemy.schema.Table(name, metadata, *args, comment=spec.doc, info=spec, **kwds)

    def ensureTableExists(self, name: str, spec: ddl.TableSpec) -> sqlalchemy.sql.FromClause:
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
        ReadOnlyDatabaseError
            Raised if `isWriteable` returns `False`, and the table does not
            already exist.
        RuntimeError
            Raised if the table exists but ``spec`` is inconsistent with its
            definition.

        Notes
        -----
        This method may not be called within transactions.  It may be called on
        read-only databases if and only if the table does in fact already
        exist.

        Subclasses may override this method, but usually should not need to.
        """
        if self._transactions:
            raise TransactionInterruption("Table creation interrupts transactions.")
        assert self._metadata is not None, "Static tables must be declared before dynamic tables."
        table = self.getExistingTable(name, spec)
        if table is not None:
            return table
        if not self.isWriteable():
            raise ReadOnlyDatabaseError(
                f"Table {name} does not exist, and cannot be created "
                f"because database {self} is read-only."
            )
        table = self._convertTableSpec(name, spec, self._metadata)
        for foreignKeySpec in spec.foreignKeys:
            table.append_constraint(self._convertForeignKeySpec(name, foreignKeySpec, self._metadata))
        table.create(self._engine)
        return table

    def getExistingTable(self, name: str, spec: ddl.TableSpec) -> Optional[sqlalchemy.schema.Table]:
        """Obtain an existing table with the given name and specification.

        Parameters
        ----------
        name : `str`
            Name of the table (not including namespace qualifiers).
        spec : `TableSpec`
            Specification for the table.  This will be used when creating the
            SQLAlchemy representation of the table, and it is used to
            check that the actual table in the database is consistent.

        Returns
        -------
        table : `sqlalchemy.schema.Table` or `None`
            SQLAlchemy representation of the table, or `None` if it does not
            exist.

        Raises
        ------
        RuntimeError
            Raised if the table exists but ``spec`` is inconsistent with its
            definition.

        Notes
        -----
        This method can be called within transactions and never modifies the
        database.

        Subclasses may override this method, but usually should not need to.
        """
        assert self._metadata is not None, "Static tables must be declared before dynamic tables."
        table = self._metadata.tables.get(name if self.namespace is None else f"{self.namespace}.{name}")
        if table is not None:
            if spec.fields.names != set(table.columns.keys()):
                raise RuntimeError(f"Table '{name}' has already been defined differently; the new "
                                   f"specification has columns {list(spec.fields.names)}, while the "
                                   f"previous definition has {list(table.columns.keys())}.")
        else:
            inspector = sqlalchemy.engine.reflection.Inspector(self._engine)
            if name in inspector.get_table_names(schema=self.namespace):
                _checkExistingTableDefinition(name, spec, inspector.get_columns(name, schema=self.namespace))
                table = self._convertTableSpec(name, spec, self._metadata)
                for foreignKeySpec in spec.foreignKeys:
                    table.append_constraint(self._convertForeignKeySpec(name, foreignKeySpec, self._metadata))
                return table
        return table

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

        Raises
        ------
        ReadOnlyDatabaseError
            Raised if `isWriteable` returns `False` when this method is called.

        Notes
        -----
        The default implementation uses bulk insert syntax when ``returnIds``
        is `False`, and a loop over single-row insert operations when it is
        `True`.

        Derived classes should reimplement when they can provide a more
        efficient implementation (especially for the latter case).

        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.
        """
        if not self.isWriteable():
            raise ReadOnlyDatabaseError(f"Attempt to insert into read-only database '{self}'.")
        if not returnIds:
            self._connection.execute(table.insert(), *rows)
        else:
            sql = table.insert()
            return [self._connection.execute(sql, row).inserted_primary_key[0] for row in rows]

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
    """An integer ID that should be used as the default for any datasets,
    quanta, or other entities that use a (autoincrement, origin) compound
    primary key (`int`).
    """

    namespace: Optional[str]
    """The schema or namespace this database instance is associated with
    (`str` or `None`).
    """
