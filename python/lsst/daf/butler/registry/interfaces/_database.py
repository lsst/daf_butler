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
    "StaticTablesContext",
]

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)
import uuid
import warnings

import astropy.time
import sqlalchemy

from ...core import ddl, time_utils
from .._exceptions import ConflictingDefinitionError


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
        raise DatabaseConflictError(f"Table '{name}' exists but is defined differently in the database; "
                                    f"specification has columns {list(spec.fields.names)}, while the "
                                    f"table in the database has {columnNames}.")


class ReadOnlyDatabaseError(RuntimeError):
    """Exception raised when a write operation is called on a read-only
    `Database`.
    """


class DatabaseConflictError(ConflictingDefinitionError):
    """Exception raised when database content (row values or schema entities)
    are inconsistent with what this client expects.
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
        self._inspector = sqlalchemy.engine.reflection.Inspector(self._db._connection)
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
        return specs._make(self.addTable(name, spec)                     # type: ignore
                           for name, spec in zip(specs._fields, specs))  # type: ignore

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


class Database(ABC):
    """An abstract interface that represents a particular database engine's
    representation of a single schema/namespace/database.

    Parameters
    ----------
    origin : `int`
        An integer ID that should be used as the default for any datasets,
        quanta, or other entities that use a (autoincrement, origin) compound
        primary key.
    connection : `sqlalchemy.engine.Connection`
        The SQLAlchemy connection this `Database` wraps.
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

     - ``_connection``: SQLAlchemy object representing the connection.
     - ``_metadata``: the `sqlalchemy.schema.MetaData` object representing
        the tables and other schema entities.

    These are considered protected (derived classes may access them, but other
    code should not), and read-only, aside from executing SQL via
    ``_connection``.
    """

    def __init__(self, *, origin: int, connection: sqlalchemy.engine.Connection,
                 namespace: Optional[str] = None):
        self.origin = origin
        self.namespace = namespace
        self._connection = connection
        self._metadata: Optional[sqlalchemy.schema.MetaData] = None
        self._tempTables: Set[str] = set()

    @classmethod
    def makeDefaultUri(cls, root: str) -> Optional[str]:
        """Create a default connection URI appropriate for the given root
        directory, or `None` if there can be no such default.
        """
        return None

    @classmethod
    def fromUri(cls, uri: str, *, origin: int, namespace: Optional[str] = None,
                writeable: bool = True) -> Database:
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
        return cls.fromConnection(cls.connect(uri, writeable=writeable),
                                  origin=origin,
                                  namespace=namespace,
                                  writeable=writeable)

    @classmethod
    @abstractmethod
    def connect(cls, uri: str, *, writeable: bool = True) -> sqlalchemy.engine.Connection:
        """Create a `sqlalchemy.engine.Connection` from a SQLAlchemy URI.

        Parameters
        ----------
        uri : `str`
            A SQLAlchemy URI connection string.
        origin : `int`
            An integer ID that should be used as the default for any datasets,
            quanta, or other entities that use a (autoincrement, origin)
            compound primary key.
        writeable : `bool`, optional
            If `True`, allow write operations on the database, including
            ``CREATE TABLE``.

        Returns
        -------
        connection : `sqlalchemy.engine.Connection`
            A database connection.

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
    def fromConnection(cls, connection: sqlalchemy.engine.Connection, *, origin: int,
                       namespace: Optional[str] = None, writeable: bool = True) -> Database:
        """Create a new `Database` from an existing
        `sqlalchemy.engine.Connection`.

        Parameters
        ----------
        connection : `sqllachemy.engine.Connection`
            The connection for the the database.  May be shared between
            `Database` instances.
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
        connection, which is desirable when they represent different namespaces
        can be queried together.  This also ties their transaction state,
        however; starting a transaction in any database automatically starts
        on in all other databases.
        """
        raise NotImplementedError()

    @contextmanager
    def transaction(self, *, interrupting: bool = False) -> Iterator:
        """Return a context manager that represents a transaction.

        Parameters
        ----------
        interrupting : `bool`
            If `True`, this transaction block needs to be able to interrupt
            any existing one in order to yield correct behavior.
        """
        assert not (interrupting and self._connection.in_transaction()), (
            "Logic error in transaction nesting: an operation that would "
            "interrupt the active transaction context has been requested."
        )
        if self._connection.in_transaction():
            trans = self._connection.begin_nested()
        else:
            # Use a regular (non-savepoint) transaction only for the outermost
            # context.
            trans = self._connection.begin()
        try:
            yield
            trans.commit()
        except BaseException:
            trans.rollback()
            raise

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
            yield context
            for table, foreignKey in context._foreignKeys:
                table.append_constraint(foreignKey)
            if create:
                if self.namespace is not None:
                    if self.namespace not in context._inspector.get_schema_names():
                        self._connection.execute(sqlalchemy.schema.CreateSchema(self.namespace))
                # In our tables we have columns that make use of sqlalchemy
                # Sequence objects. There is currently a bug in sqlalchmey that
                # causes a deprecation warning to be thrown on a property of
                # the Sequence object when the repr for the sequence is
                # created. Here a filter is used to catch these deprecation
                # warnings when tables are created.
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=sqlalchemy.exc.SADeprecationWarning)
                    self._metadata.create_all(self._connection)
                # call all initializer methods sequentially
                for init in context._initializers:
                    init(self)
        except BaseException:
            # TODO: this is potentially dangerous if we run it on
            # pre-existing schema.
            self._metadata.drop_all(self._connection)
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

    def _convertFieldSpec(self, table: str, spec: ddl.FieldSpec, metadata: sqlalchemy.MetaData,
                          **kwds: Any) -> sqlalchemy.schema.Column:
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
        assert spec.doc is None or isinstance(spec.doc, str), f"Bad doc for {table}.{spec.name}."
        return sqlalchemy.schema.Column(*args, nullable=spec.nullable, primary_key=spec.primaryKey,
                                        comment=spec.doc, **kwds)

    def _convertForeignKeySpec(self, table: str, spec: ddl.ForeignKeySpec, metadata: sqlalchemy.MetaData,
                               **kwds: Any) -> sqlalchemy.schema.ForeignKeyConstraint:
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
            "_".join(["fkey", table, self._mangleTableName(spec.table)]
                     + list(spec.target) + list(spec.source))
        )
        return sqlalchemy.schema.ForeignKeyConstraint(
            spec.source,
            [f"{self._mangleTableName(spec.table)}.{col}" for col in spec.target],
            name=name,
            ondelete=spec.onDelete
        )

    def _convertTableSpec(self, name: str, spec: ddl.TableSpec, metadata: sqlalchemy.MetaData,
                          **kwds: Any) -> sqlalchemy.schema.Table:
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
                *columns,
                name=self.shrinkDatabaseEntityName("_".join([name, "unq"] + list(columns)))
            )
            for columns in spec.unique
        )
        allIndexes.update(spec.unique)
        args.extend(
            sqlalchemy.schema.Index(
                self.shrinkDatabaseEntityName("_".join([name, "idx"] + list(columns))),
                *columns,
                unique=(columns in spec.unique)
            )
            for columns in spec.indexes if columns not in allIndexes
        )
        allIndexes.update(spec.indexes)
        args.extend(
            sqlalchemy.schema.Index(
                self.shrinkDatabaseEntityName("_".join((name, "fkidx") + fk.source)),
                *fk.source,
            )
            for fk in spec.foreignKeys if fk.addIndex and fk.source not in allIndexes
        )
        assert spec.doc is None or isinstance(spec.doc, str), f"Bad doc for {name}."
        return sqlalchemy.schema.Table(name, metadata, *args, comment=spec.doc, info=spec, **kwds)

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
        assert not self._connection.in_transaction(), "Table creation interrupts transactions."
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
        table.create(self._connection)
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
                raise DatabaseConflictError(f"Table '{name}' has already been defined differently; the new "
                                            f"specification has columns {list(spec.fields.names)}, while "
                                            f"the previous definition has {list(table.columns.keys())}.")
        else:
            inspector = sqlalchemy.engine.reflection.Inspector(self._connection)
            if name in inspector.get_table_names(schema=self.namespace):
                _checkExistingTableDefinition(name, spec, inspector.get_columns(name, schema=self.namespace))
                table = self._convertTableSpec(name, spec, self._metadata)
                for foreignKeySpec in spec.foreignKeys:
                    table.append_constraint(self._convertForeignKeySpec(name, foreignKeySpec, self._metadata))
                return table
        return table

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
        table = self._convertTableSpec(name, spec, self._metadata, prefixes=['TEMPORARY'],
                                       schema=sqlalchemy.schema.BLANK_SCHEMA)
        if table.key in self._tempTables:
            if table.key != name:
                raise ValueError(f"A temporary table with name {name} (transformed to {table.key} by "
                                 f"Database) already exists.")
        for foreignKeySpec in spec.foreignKeys:
            table.append_constraint(self._convertForeignKeySpec(name, foreignKeySpec, self._metadata))
        table.create(self._connection)
        self._tempTables.add(table.key)
        return table

    def dropTemporaryTable(self, table: sqlalchemy.schema.Table) -> None:
        """Drop a temporary table.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            A SQLAlchemy object returned by a previous call to
            `makeTemporaryTable`.
        """
        if table.key in self._tempTables:
            table.drop(self._connection)
            self._tempTables.remove(table.key)
        else:
            raise TypeError(f"Table {table.key} was not created by makeTemporaryTable.")

    def sync(self, table: sqlalchemy.schema.Table, *,
             keys: Dict[str, Any],
             compared: Optional[Dict[str, Any]] = None,
             extra: Optional[Dict[str, Any]] = None,
             returning: Optional[Sequence[str]] = None,
             ) -> Tuple[Optional[Dict[str, Any]], bool]:
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

        Returns
        -------
        row : `dict`, optional
            The value of the fields indicated by ``returning``, or `None` if
            ``returning`` is `None`.
        inserted : `bool`
            If `True`, a new row was inserted.

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
        This method may not be called within transactions.  It may be called on
        read-only databases if and only if the matching row does in fact
        already exist.
        """

        def check() -> Tuple[int, Optional[List[str]], Optional[List]]:
            """Query for a row that matches the ``key`` argument, and compare
            to what was given by the caller.

            Returns
            -------
            n : `int`
                Number of matching rows.  ``n != 1`` is always an error, but
                it's a different kind of error depending on where `check` is
                being called.
            bad : `list` of `str`, or `None`
                The subset of the keys of ``compared`` for which the existing
                values did not match the given one.  Once again, ``not bad``
                is always an error, but a different kind on context.  `None`
                if ``n != 1``
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
            selectSql = sqlalchemy.sql.select(
                [table.columns[k].label(k) for k in toSelect]
            ).select_from(table).where(
                sqlalchemy.sql.and_(*[table.columns[k] == v for k, v in keys.items()])
            )
            fetched = list(self._connection.execute(selectSql).fetchall())
            if len(fetched) != 1:
                return len(fetched), None, None
            existing = fetched[0]
            if compared is not None:

                def safeNotEqual(a: Any, b: Any) -> bool:
                    if isinstance(a, astropy.time.Time):
                        return not time_utils.times_equal(a, b)
                    return a != b

                inconsistencies = [f"{k}: {existing[k]!r} != {v!r}"
                                   for k, v in compared.items()
                                   if safeNotEqual(existing[k], v)]
            else:
                inconsistencies = []
            if returning is not None:
                toReturn: Optional[list] = [existing[k] for k in returning]
            else:
                toReturn = None
            return 1, inconsistencies, toReturn

        if self.isWriteable() or table.key in self._tempTables:
            # Database is writeable.  Try an insert first, but allow it to fail
            # (in only specific ways).
            row = keys.copy()
            if compared is not None:
                row.update(compared)
            if extra is not None:
                row.update(extra)
            insertSql = table.insert().values(row)
            try:
                with self.transaction(interrupting=True):
                    self._connection.execute(insertSql)
                    # Need to perform check() for this branch inside the
                    # transaction, so we roll back an insert that didn't do
                    # what we expected.  That limits the extent to which we
                    # can reduce duplication between this block and the other
                    # ones that perform similar logic.
                    n, bad, result = check()
                    if n < 1:
                        raise RuntimeError("Insertion in sync did not seem to affect table.  This is a bug.")
                    elif n > 1:
                        raise RuntimeError(f"Keys passed to sync {keys.keys()} do not comprise a "
                                           f"unique constraint for table {table.name}.")
                    elif bad:
                        raise RuntimeError(
                            f"Conflict ({bad}) in sync after successful insert; this is "
                            f"possible if the same table is being updated by a concurrent "
                            f"process that isn't using sync, but it may also be a bug in "
                            f"daf_butler."
                        )
                # No exceptions, so it looks like we inserted the requested row
                # successfully.
                inserted = True
            except sqlalchemy.exc.IntegrityError as err:
                # Most likely cause is that an equivalent row already exists,
                # but it could also be some other constraint.  Query for the
                # row we think we matched to resolve that question.
                n, bad, result = check()
                if n < 1:
                    # There was no matched row; insertion failed for some
                    # completely different reason.  Just re-raise the original
                    # IntegrityError.
                    raise
                elif n > 2:
                    # There were multiple matched rows, which means we
                    # conflicted *and* the arguments were bad to begin with.
                    raise RuntimeError(f"Keys passed to sync {keys.keys()} do not comprise a "
                                       f"unique constraint for table {table.name}.") from err
                elif bad:
                    # No logic bug, but data conflicted on the keys given.
                    raise DatabaseConflictError(f"Conflict in sync for table "
                                                f"{table.name} on column(s) {bad}.") from err
                # The desired row is already present and consistent with what
                # we tried to insert.
                inserted = False
        else:
            assert not self._connection.in_transaction(), (
                "Calling sync within a transaction block is an error even "
                "on a read-only database."
            )
            # Database is not writeable; just see if the row exists.
            n, bad, result = check()
            if n < 1:
                raise ReadOnlyDatabaseError("sync needs to insert, but database is read-only.")
            elif n > 1:
                raise RuntimeError("Keys passed to sync do not comprise a unique constraint.")
            elif bad:
                raise DatabaseConflictError(f"Conflict in sync on column(s) {bad}.")
            inserted = False
        if returning is None:
            return None, inserted
        else:
            assert result is not None
            return {k: v for k, v in zip(returning, result)}, inserted

    def insert(self, table: sqlalchemy.schema.Table, *rows: dict, returnIds: bool = False,
               select: Optional[sqlalchemy.sql.Select] = None,
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
        select : `sqlalchemy.sql.Select`, optional
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
        if not (self.isWriteable() or table.key in self._tempTables):
            raise ReadOnlyDatabaseError(f"Attempt to insert into read-only database '{self}'.")
        if select is not None and (rows or returnIds):
            raise TypeError("'select' is incompatible with passing value rows or returnIds=True.")
        if not rows and select is None:
            if returnIds:
                return []
            else:
                return None
        if not returnIds:
            if select is not None:
                if names is None:
                    names = select.columns.keys()
                self._connection.execute(table.insert().from_select(names, select))
            else:
                self._connection.execute(table.insert(), *rows)
            return None
        else:
            sql = table.insert()
            return [self._connection.execute(sql, row).inserted_primary_key[0] for row in rows]

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
            dictionaries must exactly the names in ``columns``.

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
        if not (self.isWriteable() or table.key in self._tempTables):
            raise ReadOnlyDatabaseError(f"Attempt to delete from read-only database '{self}'.")
        if columns and not rows:
            # If there are no columns, this operation is supposed to delete
            # everything (so we proceed as usual).  But if there are columns,
            # but no rows, it was a constrained bulk operation where the
            # constraint is that no rows match, and we should short-circuit
            # while reporting that no rows were affected.
            return 0
        sql = table.delete()
        whereTerms = [table.columns[name] == sqlalchemy.sql.bindparam(name) for name in columns]
        if whereTerms:
            sql = sql.where(sqlalchemy.sql.and_(*whereTerms))
        return self._connection.execute(sql, *rows).rowcount

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
        if not (self.isWriteable() or table.key in self._tempTables):
            raise ReadOnlyDatabaseError(f"Attempt to update read-only database '{self}'.")
        if not rows:
            return 0
        sql = table.update().where(
            sqlalchemy.sql.and_(*[table.columns[k] == sqlalchemy.sql.bindparam(v) for k, v in where.items()])
        )
        return self._connection.execute(sql, *rows).rowcount

    def query(self, sql: sqlalchemy.sql.FromClause,
              *args: Any, **kwds: Any) -> sqlalchemy.engine.ResultProxy:
        """Run a SELECT query against the database.

        Parameters
        ----------
        sql : `sqlalchemy.sql.FromClause`
            A SQLAlchemy representation of a ``SELECT`` query.
        *args
            Additional positional arguments are forwarded to
            `sqlalchemy.engine.Connection.execute`.
        **kwds
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
