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
    "DatabaseConflictError",
    "SchemaAlreadyDefinedError",
    "Session",
    "StaticTablesContext",
]

import uuid
import warnings
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, Type, Union

import astropy.time
import sqlalchemy

from ...core import TimespanDatabaseRepresentation, ddl, time_utils
from ...core.named import NamedValueAbstractSet
from .._exceptions import ConflictingDefinitionError

_IN_SAVEPOINT_TRANSACTION = "IN_SAVEPOINT_TRANSACTION"


def _checkExistingTableDefinition(name: str, spec: ddl.TableSpec, inspection: List[Dict[str, Any]]) -> None:
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
    DatabaseConflictError
        Raised if the definitions are inconsistent.
    """
    columnNames = [c["name"] for c in inspection]
    if spec.fields.names != set(columnNames):
        raise DatabaseConflictError(
            f"Table '{name}' exists but is defined differently in the database; "
            f"specification has columns {list(spec.fields.names)}, while the "
            f"table in the database has {columnNames}."
        )


class ReadOnlyDatabaseError(RuntimeError):
    """Exception raised when a write operation is called on a read-only
    `Database`.
    """


class DatabaseConflictError(ConflictingDefinitionError):
    """Exception raised when database content (row values or schema entities)
    are inconsistent with what this client expects.
    """


class SchemaAlreadyDefinedError(RuntimeError):
    """Exception raised when trying to initialize database schema when some
    tables already exist.
    """


class StaticTablesContext:
    """Helper class used to declare the static schema for a registry layer
    in a database.

    An instance of this class is returned by `Database.declareStaticTables`,
    which should be the only way it should be constructed.
    """

    def __init__(self, db: Database):
        self._db = db
        self._foreignKeys: List[Tuple[sqlalchemy.schema.Table, sqlalchemy.schema.ForeignKeyConstraint]] = []
        self._inspector = sqlalchemy.inspect(self._db._engine)
        self._tableNames = frozenset(self._inspector.get_table_names(schema=self._db.namespace))
        self._initializers: List[Callable[[Database], None]] = []

    def addTable(self, name: str, spec: ddl.TableSpec) -> sqlalchemy.schema.Table:
        """Add a new table to the schema, returning its sqlalchemy
        representation.

        The new table may not actually be created until the end of the
        context created by `Database.declareStaticTables`, allowing tables
        to be declared in any order even in the presence of foreign key
        relationships.
        """
        name = self._db._mangleTableName(name)
        if name in self._tableNames:
            _checkExistingTableDefinition(
                name, spec, self._inspector.get_columns(name, schema=self._db.namespace)
            )
        metadata = self._db._metadata
        assert metadata is not None, "Guaranteed by context manager that returns this object."
        table = self._db._convertTableSpec(name, spec, metadata)
        for foreignKeySpec in spec.foreignKeys:
            self._foreignKeys.append((table, self._db._convertForeignKeySpec(name, foreignKeySpec, metadata)))
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
        return specs._make(  # type: ignore
            self.addTable(name, spec) for name, spec in zip(specs._fields, specs)  # type: ignore
        )

    def addInitializer(self, initializer: Callable[[Database], None]) -> None:
        """Add a method that does one-time initialization of a database.

        Initialization can mean anything that changes state of a database
        and needs to be done exactly once after database schema was created.
        An example for that could be population of schema attributes.

        Parameters
        ----------
        initializer : callable
            Method of a single argument which is a `Database` instance.
        """
        self._initializers.append(initializer)


class Session:
    """Class representing a persistent connection to a database.

    Parameters
    ----------
    db : `Database`
        Database instance.

    Notes
    -----
    Instances of Session class should not be created by client code;
    `Database.session` should be used to create context for a session::

        with db.session() as session:
            session.method()
            db.method()

    In the current implementation sessions can be nested and transactions can
    be nested within a session. All nested sessions and transaction share the
    same database connection.

    Session class represents a limited subset of database API that requires
    persistent connection to a database (e.g. temporary tables which have
    lifetime of a session). Potentially most of the database API could be
    associated with a Session class.
    """

    def __init__(self, db: Database):
        self._db = db

    def makeTemporaryTable(self, spec: ddl.TableSpec, name: Optional[str] = None) -> sqlalchemy.schema.Table:
        """Create a temporary table.

        Parameters
        ----------
        spec : `TableSpec`
            Specification for the table.
        name : `str`, optional
            A unique (within this session/connetion) name for the table.
            Subclasses may override to modify the actual name used.  If not
            provided, a unique name will be generated.

        Returns
        -------
        table : `sqlalchemy.schema.Table`
            SQLAlchemy representation of the table.

        Notes
        -----
        Temporary tables may be created, dropped, and written to even in
        read-only databases - at least according to the Python-level
        protections in the `Database` classes.  Server permissions may say
        otherwise, but in that case they probably need to be modified to
        support the full range of expected read-only butler behavior.

        Temporary table rows are guaranteed to be dropped when a connection is
        closed.  `Database` implementations are permitted to allow the table to
        remain as long as this is transparent to the user (i.e. "creating" the
        temporary table in a new session should not be an error, even if it
        does nothing).

        It may not be possible to use temporary tables within transactions with
        some database engines (or configurations thereof).
        """
        if name is None:
            name = f"tmp_{uuid.uuid4().hex}"
        metadata = self._db._metadata
        if metadata is None:
            raise RuntimeError("Cannot create temporary table before static schema is defined.")
        table = self._db._convertTableSpec(
            name, spec, metadata, prefixes=["TEMPORARY"], schema=sqlalchemy.schema.BLANK_SCHEMA
        )
        if table.key in self._db._tempTables:
            if table.key != name:
                raise ValueError(
                    f"A temporary table with name {name} (transformed to {table.key} by "
                    f"Database) already exists."
                )
        for foreignKeySpec in spec.foreignKeys:
            table.append_constraint(self._db._convertForeignKeySpec(name, foreignKeySpec, metadata))
        with self._db._connection() as connection:
            table.create(connection)
        self._db._tempTables.add(table.key)
        return table

    def dropTemporaryTable(self, table: sqlalchemy.schema.Table) -> None:
        """Drop a temporary table.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            A SQLAlchemy object returned by a previous call to
            `makeTemporaryTable`.
        """
        if table.key in self._db._tempTables:
            with self._db._connection() as connection:
                table.drop(connection)
            self._db._tempTables.remove(table.key)
        else:
            raise TypeError(f"Table {table.key} was not created by makeTemporaryTable.")

    @contextmanager
    def temporary_table(
        self, spec: ddl.TableSpec, name: Optional[str] = None
    ) -> Iterator[sqlalchemy.schema.Table]:
        """Return a context manager that creates and then drops a context
        manager.

        Parameters
        ----------
        spec : `ddl.TableSpec`
            Specification for the columns.  Unique and foreign key constraints
            may be ignored.
        name : `str`, optional
            If provided, the name of the SQL construct.  If not provided, an
            opaque but unique identifier is generated.

        Returns
        -------
        table : `sqlalchemy.schema.Table`
            SQLAlchemy representation of the table.
        """
        table = self.makeTemporaryTable(spec=spec, name=name)
        try:
            yield table
        finally:
            self.dropTemporaryTable(table)


class Database(ABC):
    """An abstract interface that represents a particular database engine's
    representation of a single schema/namespace/database.

    Parameters
    ----------
    origin : `int`
        An integer ID that should be used as the default for any datasets,
        quanta, or other entities that use a (autoincrement, origin) compound
        primary key.
    engine : `sqlalchemy.engine.Engine`
        The SQLAlchemy engine for this `Database`.
    namespace : `str`, optional
        Name of the schema or namespace this instance is associated with.
        This is passed as the ``schema`` argument when constructing a
        `sqlalchemy.schema.MetaData` instance.  We use ``namespace`` instead to
        avoid confusion between "schema means namespace" and "schema means
        table definitions".

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

     - ``_engine``: SQLAlchemy object representing its engine.
     - ``_connection``: method returning a context manager for
       `sqlalchemy.engine.Connection` object.
     - ``_metadata``: the `sqlalchemy.schema.MetaData` object representing
        the tables and other schema entities.

    These are considered protected (derived classes may access them, but other
    code should not), and read-only, aside from executing SQL via
    ``_connection``.
    """

    def __init__(self, *, origin: int, engine: sqlalchemy.engine.Engine, namespace: Optional[str] = None):
        self.origin = origin
        self.namespace = namespace
        self._engine = engine
        self._session_connection: Optional[sqlalchemy.engine.Connection] = None
        self._metadata: Optional[sqlalchemy.schema.MetaData] = None
        self._tempTables: Set[str] = set()

    def __repr__(self) -> str:
        # Rather than try to reproduce all the parameters used to create
        # the object, instead report the more useful information of the
        # connection URL.
        if self._engine.url.password is not None:
            uri = str(self._engine.url.set(password="***"))
        else:
            uri = str(self._engine.url)
        if self.namespace:
            uri += f"#{self.namespace}"
        return f'{type(self).__name__}("{uri}")'

    @classmethod
    def makeDefaultUri(cls, root: str) -> Optional[str]:
        """Create a default connection URI appropriate for the given root
        directory, or `None` if there can be no such default.
        """
        return None

    @classmethod
    def fromUri(
        cls, uri: str, *, origin: int, namespace: Optional[str] = None, writeable: bool = True
    ) -> Database:
        """Construct a database from a SQLAlchemy URI.

        Parameters
        ----------
        uri : `str`
            A SQLAlchemy URI connection string.
        origin : `int`
            An integer ID that should be used as the default for any datasets,
            quanta, or other entities that use a (autoincrement, origin)
            compound primary key.
        namespace : `str`, optional
            A database namespace (i.e. schema) the new instance should be
            associated with.  If `None` (default), the namespace (if any) is
            inferred from the URI.
        writeable : `bool`, optional
            If `True`, allow write operations on the database, including
            ``CREATE TABLE``.

        Returns
        -------
        db : `Database`
            A new `Database` instance.
        """
        return cls.fromEngine(
            cls.makeEngine(uri, writeable=writeable), origin=origin, namespace=namespace, writeable=writeable
        )

    @classmethod
    @abstractmethod
    def makeEngine(cls, uri: str, *, writeable: bool = True) -> sqlalchemy.engine.Engine:
        """Create a `sqlalchemy.engine.Engine` from a SQLAlchemy URI.

        Parameters
        ----------
        uri : `str`
            A SQLAlchemy URI connection string.
        writeable : `bool`, optional
            If `True`, allow write operations on the database, including
            ``CREATE TABLE``.

        Returns
        -------
        engine : `sqlalchemy.engine.Engine`
            A database engine.

        Notes
        -----
        Subclasses that support other ways to connect to a database are
        encouraged to add optional arguments to their implementation of this
        method, as long as they maintain compatibility with the base class
        call signature.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def fromEngine(
        cls,
        engine: sqlalchemy.engine.Engine,
        *,
        origin: int,
        namespace: Optional[str] = None,
        writeable: bool = True,
    ) -> Database:
        """Create a new `Database` from an existing `sqlalchemy.engine.Engine`.

        Parameters
        ----------
        engine : `sqlalchemy.engine.Engine`
            The engine for the database.  May be shared between `Database`
            instances.
        origin : `int`
            An integer ID that should be used as the default for any datasets,
            quanta, or other entities that use a (autoincrement, origin)
            compound primary key.
        namespace : `str`, optional
            A different database namespace (i.e. schema) the new instance
            should be associated with.  If `None` (default), the namespace
            (if any) is inferred from the connection.
        writeable : `bool`, optional
            If `True`, allow write operations on the database, including
            ``CREATE TABLE``.

        Returns
        -------
        db : `Database`
            A new `Database` instance.

        Notes
        -----
        This method allows different `Database` instances to share the same
        engine, which is desirable when they represent different namespaces
        can be queried together.
        """
        raise NotImplementedError()

    @contextmanager
    def session(self) -> Iterator:
        """Return a context manager that represents a session (persistent
        connection to a database).
        """
        if self._session_connection is not None:
            # session already started, just reuse that
            yield Session(self)
        else:
            try:
                # open new connection and close it when done
                self._session_connection = self._engine.connect()
                yield Session(self)
            finally:
                if self._session_connection is not None:
                    self._session_connection.close()
                    self._session_connection = None
                # Temporary tables only live within session
                self._tempTables = set()

    @contextmanager
    def transaction(
        self,
        *,
        interrupting: bool = False,
        savepoint: bool = False,
        lock: Iterable[sqlalchemy.schema.Table] = (),
    ) -> Iterator:
        """Return a context manager that represents a transaction.

        Parameters
        ----------
        interrupting : `bool`, optional
            If `True` (`False` is default), this transaction block may not be
            nested without an outer one, and attempting to do so is a logic
            (i.e. assertion) error.
        savepoint : `bool`, optional
            If `True` (`False` is default), create a `SAVEPOINT`, allowing
            exceptions raised by the database (e.g. due to constraint
            violations) during this transaction's context to be caught outside
            it without also rolling back all operations in an outer transaction
            block.  If `False`, transactions may still be nested, but a
            rollback may be generated at any level and affects all levels, and
            commits are deferred until the outermost block completes.  If any
            outer transaction block was created with ``savepoint=True``, all
            inner blocks will be as well (regardless of the actual value
            passed).  This has no effect if this is the outermost transaction.
        lock : `Iterable` [ `sqlalchemy.schema.Table` ], optional
            A list of tables to lock for the duration of this transaction.
            These locks are guaranteed to prevent concurrent writes and allow
            this transaction (only) to acquire the same locks (others should
            block), but only prevent concurrent reads if the database engine
            requires that in order to block concurrent writes.

        Notes
        -----
        All transactions on a connection managed by one or more `Database`
        instances _must_ go through this method, or transaction state will not
        be correctly managed.
        """
        # need a connection, use session to manage it
        with self.session():
            assert self._session_connection is not None
            connection = self._session_connection
            assert not (interrupting and connection.in_transaction()), (
                "Logic error in transaction nesting: an operation that would "
                "interrupt the active transaction context has been requested."
            )
            # We remember whether we are already in a SAVEPOINT transaction via
            # the connection object's 'info' dict, which is explicitly for user
            # information like this.  This is safer than a regular `Database`
            # instance attribute, because it guards against multiple `Database`
            # instances sharing the same connection.  The need to use our own
            # flag here to track whether we're in a nested transaction should
            # go away in SQLAlchemy 1.4, which seems to have a
            # `Connection.in_nested_transaction()` method.
            savepoint = savepoint or connection.info.get(_IN_SAVEPOINT_TRANSACTION, False)
            connection.info[_IN_SAVEPOINT_TRANSACTION] = savepoint
            trans: sqlalchemy.engine.Transaction
            if connection.in_transaction() and savepoint:
                trans = connection.begin_nested()
            elif not connection.in_transaction():
                # Use a regular (non-savepoint) transaction always for the
                # outermost context.
                trans = connection.begin()
            else:
                # Nested non-savepoint transactions, don't do anything.
                trans = None
            self._lockTables(connection, lock)
            try:
                yield
                if trans is not None:
                    trans.commit()
            except BaseException:
                if trans is not None:
                    trans.rollback()
                raise
            finally:
                if not connection.in_transaction():
                    connection.info.pop(_IN_SAVEPOINT_TRANSACTION, None)

    @contextmanager
    def _connection(self) -> Iterator[sqlalchemy.engine.Connection]:
        """Return context manager for Connection."""
        if self._session_connection is not None:
            # It means that we are in Session context, but we may not be in
            # transaction context. Start a short transaction in that case.
            if self._session_connection.in_transaction():
                yield self._session_connection
            else:
                with self._session_connection.begin():
                    yield self._session_connection
        else:
            # Make new connection and transaction, transaction will be
            # committed on context exit.
            with self._engine.begin() as connection:
                yield connection

    @abstractmethod
    def _lockTables(
        self, connection: sqlalchemy.engine.Connection, tables: Iterable[sqlalchemy.schema.Table] = ()
    ) -> None:
        """Acquire locks on the given tables.

        This is an implementation hook for subclasses, called by `transaction`.
        It should not be called directly by other code.

        Parameters
        ----------
        connection : `sqlalchemy.engine.Connection`
            Database connection object. It is guaranteed that transaction is
            already in a progress for this connection.
        tables : `Iterable` [ `sqlalchemy.schema.Table` ], optional
            A list of tables to lock for the duration of this transaction.
            These locks are guaranteed to prevent concurrent writes and allow
            this transaction (only) to acquire the same locks (others should
            block), but only prevent concurrent reads if the database engine
            requires that in order to block concurrent writes.
        """
        raise NotImplementedError()

    def isTableWriteable(self, table: sqlalchemy.schema.Table) -> bool:
        """Check whether a table is writeable, either because the database
        connection is read-write or the table is a temporary table.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            SQLAlchemy table object to check.

        Returns
        -------
        writeable : `bool`
            Whether this table is writeable.
        """
        return self.isWriteable() or table.key in self._tempTables

    def assertTableWriteable(self, table: sqlalchemy.schema.Table, msg: str) -> None:
        """Raise if the given table is not writeable, either because the
        database connection is read-write or the table is a temporary table.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            SQLAlchemy table object to check.
        msg : `str`, optional
            If provided, raise `ReadOnlyDatabaseError` instead of returning
            `False`, with this message.
        """
        if not self.isTableWriteable(table):
            raise ReadOnlyDatabaseError(msg)

    @contextmanager
    def declareStaticTables(self, *, create: bool) -> Iterator[StaticTablesContext]:
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

        Examples
        --------
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
            if create and context._tableNames:
                # Looks like database is already initalized, to avoid danger
                # of modifying/destroying valid schema we refuse to do
                # anything in this case
                raise SchemaAlreadyDefinedError(f"Cannot create tables in non-empty database {self}.")
            yield context
            for table, foreignKey in context._foreignKeys:
                table.append_constraint(foreignKey)
            if create:
                if self.namespace is not None:
                    if self.namespace not in context._inspector.get_schema_names():
                        with self._connection() as connection:
                            connection.execute(sqlalchemy.schema.CreateSchema(self.namespace))
                # In our tables we have columns that make use of sqlalchemy
                # Sequence objects. There is currently a bug in sqlalchemy that
                # causes a deprecation warning to be thrown on a property of
                # the Sequence object when the repr for the sequence is
                # created. Here a filter is used to catch these deprecation
                # warnings when tables are created.
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=sqlalchemy.exc.SADeprecationWarning)
                    self._metadata.create_all(self._engine)
                # call all initializer methods sequentially
                for init in context._initializers:
                    init(self)
        except BaseException:
            self._metadata = None
            raise

    @abstractmethod
    def isWriteable(self) -> bool:
        """Return `True` if this database can be modified by this client."""
        raise NotImplementedError()

    @abstractmethod
    def __str__(self) -> str:
        """Return a human-readable identifier for this `Database`, including
        any namespace or schema that identifies its names within a `Registry`.
        """
        raise NotImplementedError()

    @property
    def dialect(self) -> sqlalchemy.engine.Dialect:
        """The SQLAlchemy dialect for this database engine
        (`sqlalchemy.engine.Dialect`).
        """
        return self._engine.dialect

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

    def _mangleTableName(self, name: str) -> str:
        """Map a logical, user-visible table name to the true table name used
        in the database.

        The default implementation returns the given name unchanged.

        Parameters
        ----------
        name : `str`
            Input table name.  Should not include a namespace (i.e. schema)
            prefix.

        Returns
        -------
        mangled : `str`
            Mangled version of the table name (still with no namespace prefix).

        Notes
        -----
        Reimplementations of this method must be idempotent - mangling an
        already-mangled name must have no effect.
        """
        return name

    def _makeColumnConstraints(self, table: str, spec: ddl.FieldSpec) -> List[sqlalchemy.CheckConstraint]:
        """Create constraints based on this spec.

        Parameters
        ----------
        table : `str`
            Name of the table this column is being added to.
        spec : `FieldSpec`
            Specification for the field to be added.

        Returns
        -------
        constraint : `list` of `sqlalchemy.CheckConstraint`
            Constraint added for this column.
        """
        # By default we return no additional constraints
        return []

    def _convertFieldSpec(
        self, table: str, spec: ddl.FieldSpec, metadata: sqlalchemy.MetaData, **kwargs: Any
    ) -> sqlalchemy.schema.Column:
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
        **kwargs
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
            args.append(
                sqlalchemy.Sequence(
                    self.shrinkDatabaseEntityName(f"{table}_seq_{spec.name}"), metadata=metadata
                )
            )
        assert spec.doc is None or isinstance(spec.doc, str), f"Bad doc for {table}.{spec.name}."
        return sqlalchemy.schema.Column(
            *args,
            nullable=spec.nullable,
            primary_key=spec.primaryKey,
            comment=spec.doc,
            server_default=spec.default,
            **kwargs,
        )

    def _convertForeignKeySpec(
        self, table: str, spec: ddl.ForeignKeySpec, metadata: sqlalchemy.MetaData, **kwargs: Any
    ) -> sqlalchemy.schema.ForeignKeyConstraint:
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
        **kwargs
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
            "_".join(
                ["fkey", table, self._mangleTableName(spec.table)] + list(spec.target) + list(spec.source)
            )
        )
        return sqlalchemy.schema.ForeignKeyConstraint(
            spec.source,
            [f"{self._mangleTableName(spec.table)}.{col}" for col in spec.target],
            name=name,
            ondelete=spec.onDelete,
        )

    def _convertExclusionConstraintSpec(
        self,
        table: str,
        spec: Tuple[Union[str, Type[TimespanDatabaseRepresentation]], ...],
        metadata: sqlalchemy.MetaData,
    ) -> sqlalchemy.schema.Constraint:
        """Convert a `tuple` from `ddl.TableSpec.exclusion` into a SQLAlchemy
        constraint representation.

        Parameters
        ----------
        table : `str`
            Name of the table this constraint is being added to.
        spec : `tuple` [ `str` or `type` ]
            A tuple of `str` column names and the `type` object returned by
            `getTimespanRepresentation` (which must appear exactly once),
            indicating the order of the columns in the index used to back the
            constraint.
        metadata : `sqlalchemy.MetaData`
            SQLAlchemy representation of the DDL schema this constraint is
            being added to.

        Returns
        -------
        constraint : `sqlalchemy.schema.Constraint`
            SQLAlchemy representation of the constraint.

        Raises
        ------
        NotImplementedError
            Raised if this database does not support exclusion constraints.
        """
        raise NotImplementedError(f"Database {self} does not support exclusion constraints.")

    def _convertTableSpec(
        self, name: str, spec: ddl.TableSpec, metadata: sqlalchemy.MetaData, **kwargs: Any
    ) -> sqlalchemy.schema.Table:
        """Convert a `TableSpec` to a `sqlalchemy.schema.Table`.

        Parameters
        ----------
        spec : `TableSpec`
            Specification for the foreign key to be added.
        metadata : `sqlalchemy.MetaData`
            SQLAlchemy representation of the DDL schema this table is being
            added to.
        **kwargs
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
        name = self._mangleTableName(name)
        args = [self._convertFieldSpec(name, fieldSpec, metadata) for fieldSpec in spec.fields]

        # Add any column constraints
        for fieldSpec in spec.fields:
            args.extend(self._makeColumnConstraints(name, fieldSpec))

        # Track indexes added for primary key and unique constraints, to make
        # sure we don't add duplicate explicit or foreign key indexes for
        # those.
        allIndexes = {tuple(fieldSpec.name for fieldSpec in spec.fields if fieldSpec.primaryKey)}
        args.extend(
            sqlalchemy.schema.UniqueConstraint(
                *columns, name=self.shrinkDatabaseEntityName("_".join([name, "unq"] + list(columns)))
            )
            for columns in spec.unique
        )
        allIndexes.update(spec.unique)
        args.extend(
            sqlalchemy.schema.Index(
                self.shrinkDatabaseEntityName("_".join([name, "idx"] + list(index.columns))),
                *index.columns,
                unique=(index.columns in spec.unique),
                **index.kwargs,
            )
            for index in spec.indexes
            if index.columns not in allIndexes
        )
        allIndexes.update(index.columns for index in spec.indexes)
        args.extend(
            sqlalchemy.schema.Index(
                self.shrinkDatabaseEntityName("_".join((name, "fkidx") + fk.source)),
                *fk.source,
            )
            for fk in spec.foreignKeys
            if fk.addIndex and fk.source not in allIndexes
        )

        args.extend(self._convertExclusionConstraintSpec(name, excl, metadata) for excl in spec.exclusion)

        assert spec.doc is None or isinstance(spec.doc, str), f"Bad doc for {name}."
        return sqlalchemy.schema.Table(name, metadata, *args, comment=spec.doc, info=spec, **kwargs)

    def ensureTableExists(self, name: str, spec: ddl.TableSpec) -> sqlalchemy.schema.Table:
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
        ReadOnlyDatabaseError
            Raised if `isWriteable` returns `False`, and the table does not
            already exist.
        DatabaseConflictError
            Raised if the table exists but ``spec`` is inconsistent with its
            definition.

        Notes
        -----
        This method may not be called within transactions.  It may be called on
        read-only databases if and only if the table does in fact already
        exist.

        Subclasses may override this method, but usually should not need to.
        """
        # TODO: if _engine is used to make a table then it uses separate
        # connection and should not interfere with current transaction
        assert (
            self._session_connection is None or not self._session_connection.in_transaction()
        ), "Table creation interrupts transactions."
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
        try:
            with self._connection() as connection:
                table.create(connection)
        except sqlalchemy.exc.DatabaseError:
            # Some other process could have created the table meanwhile, which
            # usually causes OperationalError or ProgrammingError. We cannot
            # use IF NOT EXISTS clause in this case due to PostgreSQL race
            # condition on server side which causes IntegrityError. Instead we
            # catch these exceptions (they all inherit DatabaseError) and
            # re-check whether table is now there.
            table = self.getExistingTable(name, spec)
            if table is None:
                raise
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
        DatabaseConflictError
            Raised if the table exists but ``spec`` is inconsistent with its
            definition.

        Notes
        -----
        This method can be called within transactions and never modifies the
        database.

        Subclasses may override this method, but usually should not need to.
        """
        assert self._metadata is not None, "Static tables must be declared before dynamic tables."
        name = self._mangleTableName(name)
        table = self._metadata.tables.get(name if self.namespace is None else f"{self.namespace}.{name}")
        if table is not None:
            if spec.fields.names != set(table.columns.keys()):
                raise DatabaseConflictError(
                    f"Table '{name}' has already been defined differently; the new "
                    f"specification has columns {list(spec.fields.names)}, while "
                    f"the previous definition has {list(table.columns.keys())}."
                )
        else:
            inspector = sqlalchemy.inspect(self._engine)
            if name in inspector.get_table_names(schema=self.namespace):
                _checkExistingTableDefinition(name, spec, inspector.get_columns(name, schema=self.namespace))
                table = self._convertTableSpec(name, spec, self._metadata)
                for foreignKeySpec in spec.foreignKeys:
                    table.append_constraint(self._convertForeignKeySpec(name, foreignKeySpec, self._metadata))
                return table
        return table

    @classmethod
    def getTimespanRepresentation(cls) -> Type[TimespanDatabaseRepresentation]:
        """Return a `type` that encapsulates the way `Timespan` objects are
        stored in this database.

        `Database` does not automatically use the return type of this method
        anywhere else; calling code is responsible for making sure that DDL
        and queries are consistent with it.

        Returns
        -------
        TimespanReprClass : `type` (`TimespanDatabaseRepresention` subclass)
            A type that encapsulates the way `Timespan` objects should be
            stored in this database.

        Notes
        -----
        There are two big reasons we've decided to keep timespan-mangling logic
        outside the `Database` implementations, even though the choice of
        representation is ultimately up to a `Database` implementation:

         - Timespans appear in relatively few tables and queries in our
           typical usage, and the code that operates on them is already aware
           that it is working with timespans.  In contrast, a
           timespan-representation-aware implementation of, say, `insert`,
           would need to have extra logic to identify when timespan-mangling
           needed to occur, which would usually be useless overhead.

         - SQLAlchemy's rich SELECT query expression system has no way to wrap
           multiple columns in a single expression object (the ORM does, but
           we are not using the ORM).  So we would have to wrap _much_ more of
           that code in our own interfaces to encapsulate timespan
           representations there.
        """
        return TimespanDatabaseRepresentation.Compound

    def sync(
        self,
        table: sqlalchemy.schema.Table,
        *,
        keys: Dict[str, Any],
        compared: Optional[Dict[str, Any]] = None,
        extra: Optional[Dict[str, Any]] = None,
        returning: Optional[Sequence[str]] = None,
        update: bool = False,
    ) -> Tuple[Optional[Dict[str, Any]], Union[bool, Dict[str, Any]]]:
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
        returning : `~collections.abc.Sequence` of `str`, optional
            The names of columns whose values should be returned.
        update : `bool`, optional
            If `True` (`False` is default), update the existing row with the
            values in ``compared`` instead of raising `DatabaseConflictError`.

        Returns
        -------
        row : `dict`, optional
            The value of the fields indicated by ``returning``, or `None` if
            ``returning`` is `None`.
        inserted_or_updated : `bool` or `dict`
            If `True`, a new row was inserted; if `False`, a matching row
            already existed.  If a `dict` (only possible if ``update=True``),
            then an existing row was updated, and the dict maps the names of
            the updated columns to their *old* values (new values can be
            obtained from ``compared``).

        Raises
        ------
        DatabaseConflictError
            Raised if the values in ``compared`` do not match the values in the
            database.
        ReadOnlyDatabaseError
            Raised if `isWriteable` returns `False`, and no matching record
            already exists.

        Notes
        -----
        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.

        It may be called on read-only databases if and only if the matching row
        does in fact already exist.
        """

        def check() -> Tuple[int, Optional[Dict[str, Any]], Optional[List]]:
            """Query for a row that matches the ``key`` argument, and compare
            to what was given by the caller.

            Returns
            -------
            n : `int`
                Number of matching rows.  ``n != 1`` is always an error, but
                it's a different kind of error depending on where `check` is
                being called.
            bad : `dict` or `None`
                The subset of the keys of ``compared`` for which the existing
                values did not match the given one, mapped to the existing
                values in the database.  Once again, ``not bad`` is always an
                error, but a different kind on context.  `None` if ``n != 1``
            result : `list` or `None`
                Results in the database that correspond to the columns given
                in ``returning``, or `None` if ``returning is None``.
            """
            toSelect: Set[str] = set()
            if compared is not None:
                toSelect.update(compared.keys())
            if returning is not None:
                toSelect.update(returning)
            if not toSelect:
                # Need to select some column, even if we just want to see
                # how many rows we get back.
                toSelect.add(next(iter(keys.keys())))
            selectSql = (
                sqlalchemy.sql.select(*[table.columns[k].label(k) for k in toSelect])
                .select_from(table)
                .where(sqlalchemy.sql.and_(*[table.columns[k] == v for k, v in keys.items()]))
            )
            with self._connection() as connection:
                fetched = list(connection.execute(selectSql).mappings())
            if len(fetched) != 1:
                return len(fetched), None, None
            existing = fetched[0]
            if compared is not None:

                def safeNotEqual(a: Any, b: Any) -> bool:
                    if isinstance(a, astropy.time.Time):
                        return not time_utils.TimeConverter().times_equal(a, b)
                    return a != b

                inconsistencies = {
                    k: existing[k] for k, v in compared.items() if safeNotEqual(existing[k], v)
                }
            else:
                inconsistencies = {}
            if returning is not None:
                toReturn: Optional[list] = [existing[k] for k in returning]
            else:
                toReturn = None
            return 1, inconsistencies, toReturn

        def format_bad(inconsistencies: Dict[str, Any]) -> str:
            """Format the 'bad' dictionary of existing values returned by
            ``check`` into a string suitable for an error message.
            """
            assert compared is not None, "Should not be able to get inconsistencies without comparing."
            return ", ".join(f"{k}: {v!r} != {compared[k]!r}" for k, v in inconsistencies.items())

        if self.isTableWriteable(table):
            # Try an insert first, but allow it to fail (in only specific
            # ways).
            row = keys.copy()
            if compared is not None:
                row.update(compared)
            if extra is not None:
                row.update(extra)
            with self.transaction():
                inserted = bool(self.ensure(table, row))
                inserted_or_updated: Union[bool, Dict[str, Any]]
                # Need to perform check() for this branch inside the
                # transaction, so we roll back an insert that didn't do
                # what we expected.  That limits the extent to which we
                # can reduce duplication between this block and the other
                # ones that perform similar logic.
                n, bad, result = check()
                if n < 1:
                    raise ConflictingDefinitionError(
                        f"Attempted to ensure {row} exists by inserting it with ON CONFLICT IGNORE, "
                        f"but a post-insert query on {keys} returned no results. "
                        f"Insert was {'' if inserted else 'not '}reported as successful. "
                        "This can occur if the insert violated a database constraint other than the "
                        "unique constraint or primary key used to identify the row in this call."
                    )
                elif n > 1:
                    raise RuntimeError(
                        f"Keys passed to sync {keys.keys()} do not comprise a "
                        f"unique constraint for table {table.name}."
                    )
                elif bad:
                    assert (
                        compared is not None
                    ), "Should not be able to get inconsistencies without comparing."
                    if inserted:
                        raise RuntimeError(
                            f"Conflict ({bad}) in sync after successful insert; this is "
                            "possible if the same table is being updated by a concurrent "
                            "process that isn't using sync, but it may also be a bug in "
                            "daf_butler."
                        )
                    elif update:
                        with self._connection() as connection:
                            connection.execute(
                                table.update()
                                .where(sqlalchemy.sql.and_(*[table.columns[k] == v for k, v in keys.items()]))
                                .values(**{k: compared[k] for k in bad.keys()})
                            )
                        inserted_or_updated = bad
                    else:
                        raise DatabaseConflictError(
                            f"Conflict in sync for table {table.name} on column(s) {format_bad(bad)}."
                        )
                else:
                    inserted_or_updated = inserted
        else:
            # Database is not writeable; just see if the row exists.
            n, bad, result = check()
            if n < 1:
                raise ReadOnlyDatabaseError("sync needs to insert, but database is read-only.")
            elif n > 1:
                raise RuntimeError("Keys passed to sync do not comprise a unique constraint.")
            elif bad:
                if update:
                    raise ReadOnlyDatabaseError("sync needs to update, but database is read-only.")
                else:
                    raise DatabaseConflictError(
                        f"Conflict in sync for table {table.name} on column(s) {format_bad(bad)}."
                    )
            inserted_or_updated = False
        if returning is None:
            return None, inserted_or_updated
        else:
            assert result is not None
            return {k: v for k, v in zip(returning, result)}, inserted_or_updated

    def insert(
        self,
        table: sqlalchemy.schema.Table,
        *rows: dict,
        returnIds: bool = False,
        select: Optional[sqlalchemy.sql.expression.SelectBase] = None,
        names: Optional[Iterable[str]] = None,
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
        select : `sqlalchemy.sql.SelectBase`, optional
            A SELECT query expression to insert rows from.  Cannot be provided
            with either ``rows`` or ``returnIds=True``.
        names : `Iterable` [ `str` ], optional
            Names of columns in ``table`` to be populated, ordered to match the
            columns returned by ``select``.  Ignored if ``select`` is `None`.
            If not provided, the columns returned by ``select`` must be named
            to match the desired columns of ``table``.
        *rows
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
        self.assertTableWriteable(table, f"Cannot insert into read-only table {table}.")
        if select is not None and (rows or returnIds):
            raise TypeError("'select' is incompatible with passing value rows or returnIds=True.")
        if not rows and select is None:
            if returnIds:
                return []
            else:
                return None
        with self._connection() as connection:
            if not returnIds:
                if select is not None:
                    if names is None:
                        # columns() is deprecated since 1.4, but
                        # selected_columns() method did not exist in 1.3.
                        if hasattr(select, "selected_columns"):
                            names = select.selected_columns.keys()
                        else:
                            names = select.columns.keys()
                    connection.execute(table.insert().from_select(names, select))
                else:
                    connection.execute(table.insert(), rows)
                return None
            else:
                sql = table.insert()
                return [connection.execute(sql, row).inserted_primary_key[0] for row in rows]

    @abstractmethod
    def replace(self, table: sqlalchemy.schema.Table, *rows: dict) -> None:
        """Insert one or more rows into a table, replacing any existing rows
        for which insertion of a new row would violate the primary key
        constraint.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table rows should be inserted into.
        *rows
            Positional arguments are the rows to be inserted, as dictionaries
            mapping column name to value.  The keys in all dictionaries must
            be the same.

        Raises
        ------
        ReadOnlyDatabaseError
            Raised if `isWriteable` returns `False` when this method is called.

        Notes
        -----
        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.

        Implementations should raise a `sqlalchemy.exc.IntegrityError`
        exception when a constraint other than the primary key would be
        violated.

        Implementations are not required to support `replace` on tables
        with autoincrement keys.
        """
        raise NotImplementedError()

    @abstractmethod
    def ensure(self, table: sqlalchemy.schema.Table, *rows: dict, primary_key_only: bool = False) -> int:
        """Insert one or more rows into a table, skipping any rows for which
        insertion would violate a unique constraint.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table rows should be inserted into.
        *rows
            Positional arguments are the rows to be inserted, as dictionaries
            mapping column name to value.  The keys in all dictionaries must
            be the same.
        primary_key_only : `bool`, optional
            If `True` (`False` is default), only skip rows that violate the
            primary key constraint, and raise an exception (and rollback
            transactions) for other constraint violations.

        Returns
        -------
        count : `int`
            The number of rows actually inserted.

        Raises
        ------
        ReadOnlyDatabaseError
            Raised if `isWriteable` returns `False` when this method is called.
            This is raised even if the operation would do nothing even on a
            writeable database.

        Notes
        -----
        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.

        Implementations are not required to support `ensure` on tables
        with autoincrement keys.
        """
        raise NotImplementedError()

    def delete(self, table: sqlalchemy.schema.Table, columns: Iterable[str], *rows: dict) -> int:
        """Delete one or more rows from a table.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table that rows should be deleted from.
        columns: `~collections.abc.Iterable` of `str`
            The names of columns that will be used to constrain the rows to
            be deleted; these will be combined via ``AND`` to form the
            ``WHERE`` clause of the delete query.
        *rows
            Positional arguments are the keys of rows to be deleted, as
            dictionaries mapping column name to value.  The keys in all
            dictionaries must be exactly the names in ``columns``.

        Returns
        -------
        count : `int`
            Number of rows deleted.

        Raises
        ------
        ReadOnlyDatabaseError
            Raised if `isWriteable` returns `False` when this method is called.

        Notes
        -----
        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.

        The default implementation should be sufficient for most derived
        classes.
        """
        self.assertTableWriteable(table, f"Cannot delete from read-only table {table}.")
        if columns and not rows:
            # If there are no columns, this operation is supposed to delete
            # everything (so we proceed as usual).  But if there are columns,
            # but no rows, it was a constrained bulk operation where the
            # constraint is that no rows match, and we should short-circuit
            # while reporting that no rows were affected.
            return 0
        sql = table.delete()
        columns = list(columns)  # Force iterators to list

        # More efficient to use IN operator if there is only one
        # variable changing across all rows.
        content: Dict[str, Set] = defaultdict(set)
        if len(columns) == 1:
            # Nothing to calculate since we can always use IN
            column = columns[0]
            changing_columns = [column]
            content[column] = set(row[column] for row in rows)
        else:
            for row in rows:
                for k, v in row.items():
                    content[k].add(v)
            changing_columns = [col for col, values in content.items() if len(values) > 1]

        if len(changing_columns) != 1:
            # More than one column changes each time so do explicit bind
            # parameters and have each row processed separately.
            whereTerms = [table.columns[name] == sqlalchemy.sql.bindparam(name) for name in columns]
            if whereTerms:
                sql = sql.where(sqlalchemy.sql.and_(*whereTerms))
            with self._connection() as connection:
                return connection.execute(sql, rows).rowcount
        else:
            # One of the columns has changing values but any others are
            # fixed. In this case we can use an IN operator and be more
            # efficient.
            name = changing_columns.pop()

            # Simple where clause for the unchanging columns
            clauses = []
            for k, v in content.items():
                if k == name:
                    continue
                column = table.columns[k]
                # The set only has one element
                clauses.append(column == v.pop())

            # The IN operator will not work for "infinite" numbers of
            # rows so must batch it up into distinct calls.
            in_content = list(content[name])
            n_elements = len(in_content)

            rowcount = 0
            iposn = 0
            n_per_loop = 1_000  # Controls how many items to put in IN clause
            with self._connection() as connection:
                for iposn in range(0, n_elements, n_per_loop):
                    endpos = iposn + n_per_loop
                    in_clause = table.columns[name].in_(in_content[iposn:endpos])

                    newsql = sql.where(sqlalchemy.sql.and_(*clauses, in_clause))
                    rowcount += connection.execute(newsql).rowcount
            return rowcount

    def deleteWhere(self, table: sqlalchemy.schema.Table, where: sqlalchemy.sql.ClauseElement) -> int:
        """Delete rows from a table with pre-constructed WHERE clause.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table that rows should be deleted from.
        where: `sqlalchemy.sql.ClauseElement`
            The names of columns that will be used to constrain the rows to
            be deleted; these will be combined via ``AND`` to form the
            ``WHERE`` clause of the delete query.

        Returns
        -------
        count : `int`
            Number of rows deleted.

        Raises
        ------
        ReadOnlyDatabaseError
            Raised if `isWriteable` returns `False` when this method is called.

        Notes
        -----
        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.

        The default implementation should be sufficient for most derived
        classes.
        """
        self.assertTableWriteable(table, f"Cannot delete from read-only table {table}.")

        sql = table.delete().where(where)
        with self._connection() as connection:
            return connection.execute(sql).rowcount

    def update(self, table: sqlalchemy.schema.Table, where: Dict[str, str], *rows: dict) -> int:
        """Update one or more rows in a table.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Table containing the rows to be updated.
        where : `dict` [`str`, `str`]
            A mapping from the names of columns that will be used to search for
            existing rows to the keys that will hold these values in the
            ``rows`` dictionaries.  Note that these may not be the same due to
            SQLAlchemy limitations.
        *rows
            Positional arguments are the rows to be updated.  The keys in all
            dictionaries must be the same, and may correspond to either a
            value in the ``where`` dictionary or the name of a column to be
            updated.

        Returns
        -------
        count : `int`
            Number of rows matched (regardless of whether the update actually
            modified them).

        Raises
        ------
        ReadOnlyDatabaseError
            Raised if `isWriteable` returns `False` when this method is called.

        Notes
        -----
        May be used inside transaction contexts, so implementations may not
        perform operations that interrupt transactions.

        The default implementation should be sufficient for most derived
        classes.
        """
        self.assertTableWriteable(table, f"Cannot update read-only table {table}.")
        if not rows:
            return 0
        sql = table.update().where(
            sqlalchemy.sql.and_(*[table.columns[k] == sqlalchemy.sql.bindparam(v) for k, v in where.items()])
        )
        with self._connection() as connection:
            return connection.execute(sql, rows).rowcount

    def query(
        self, sql: sqlalchemy.sql.Selectable, *args: Any, **kwargs: Any
    ) -> sqlalchemy.engine.ResultProxy:
        """Run a SELECT query against the database.

        Parameters
        ----------
        sql : `sqlalchemy.sql.Selectable`
            A SQLAlchemy representation of a ``SELECT`` query.
        *args
            Additional positional arguments are forwarded to
            `sqlalchemy.engine.Connection.execute`.
        **kwargs
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
        # We are returning a Result object so we need to take care of
        # connection lifetime. If this is happening in transaction context
        # then just use existing connection, otherwise make a special
        # connection which will be closed when result is closed.
        #
        # TODO: May be better approach would be to make this method return a
        # context manager, but this means big changes for callers of this
        # method.
        if self._session_connection is not None:
            connection = self._session_connection
        else:
            connection = self._engine.connect(close_with_result=True)
        # TODO: should we guard against non-SELECT queries here?
        return connection.execute(sql, *args, **kwargs)

    @abstractmethod
    def constant_rows(
        self,
        fields: NamedValueAbstractSet[ddl.FieldSpec],
        *rows: dict,
        name: Optional[str] = None,
    ) -> sqlalchemy.sql.FromClause:
        """Return a SQLAlchemy object that represents a small number of
        constant-valued rows.

        Parameters
        ----------
        fields : `NamedValueAbstractSet` [ `ddl.FieldSpec` ]
            The columns of the rows.  Unique and foreign key constraints are
            ignored.
        *rows : `dict`
            Values for the rows.
        name : `str`, optional
            If provided, the name of the SQL construct.  If not provided, an
            opaque but unique identifier is generated.

        Returns
        -------
        from_clause : `sqlalchemy.sql.FromClause`
            SQLAlchemy object representing the given rows.  This is guaranteed
            to be something that can be directly joined into a ``SELECT``
            query's ``FROM`` clause, and will not involve a temporary table
            that needs to be cleaned up later.

        Notes
        -----
        The default implementation uses the SQL-standard ``VALUES`` construct,
        but support for that construct is varied enough across popular RDBMSs
        that the method is still marked abstract to force explicit opt-in via
        delegation to `super`.
        """
        if name is None:
            name = f"tmp_{uuid.uuid4().hex}"
        return sqlalchemy.sql.values(
            *[sqlalchemy.Column(field.name, field.getSizedColumnType()) for field in fields],
            name=name,
        ).data([tuple(row[name] for name in fields.names) for row in rows])

    def get_constant_rows_max(self) -> int:
        """Return the maximum number of rows that should be passed to
        `constant_rows` for this backend.

        Returns
        -------
        max : `int`
            Maximum number of rows.

        Notes
        -----
        This should reflect typical performance profiles (or a guess at these),
        not just hard database engine limits.
        """
        return 100

    origin: int
    """An integer ID that should be used as the default for any datasets,
    quanta, or other entities that use a (autoincrement, origin) compound
    primary key (`int`).
    """

    namespace: Optional[str]
    """The schema or namespace this database instance is associated with
    (`str` or `None`).
    """
