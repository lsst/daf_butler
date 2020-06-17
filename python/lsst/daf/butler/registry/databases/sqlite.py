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

__all__ = ["SqliteDatabase"]

from contextlib import closing
import copy
from typing import Any, ContextManager, Dict, Iterable, List, Optional
from dataclasses import dataclass
import os
import urllib.parse

import sqlite3
import sqlalchemy
import sqlalchemy.ext.compiler

from ..interfaces import Database, ReadOnlyDatabaseError, StaticTablesContext
from ...core import ddl


def _onSqlite3Connect(dbapiConnection: sqlite3.Connection,
                      connectionRecord: sqlalchemy.pool._ConnectionRecord) -> None:
    assert isinstance(dbapiConnection, sqlite3.Connection)
    # Prevent pysqlite from emitting BEGIN and COMMIT statements.
    dbapiConnection.isolation_level = None
    # Enable foreign keys
    with closing(dbapiConnection.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.execute("PRAGMA busy_timeout = 300000;")  # in ms, so 5min (way longer than should be needed)


def _onSqlite3Begin(connection: sqlalchemy.engine.Connection) -> sqlalchemy.engine.Connection:
    assert connection.dialect.name == "sqlite"
    # Replace pysqlite's buggy transaction handling that never BEGINs with our
    # own that does, and tell SQLite to try to acquire a lock as soon as we
    # start a transaction (this should lead to more blocking and fewer
    # deadlocks).
    connection.execute("BEGIN IMMEDIATE")
    return connection


class _Replace(sqlalchemy.sql.Insert):
    """A SQLAlchemy query that compiles to INSERT ... ON CONFLICT REPLACE
    on the primary key constraint for the table.
    """
    pass


# Hard to infer what types these should be from SQLAlchemy docs; just disable
# static typing by calling everything "Any".
@sqlalchemy.ext.compiler.compiles(_Replace, "sqlite")
def _replace(insert: Any, compiler: Any, **kwargs: Any) -> Any:
    """Generate an INSERT ... ON CONFLICT REPLACE query.
    """
    # SQLite and PostgreSQL use similar syntax for their ON CONFLICT extension,
    # but SQLAlchemy only knows about PostgreSQL's, so we have to compile some
    # custom text SQL ourselves.
    result = compiler.visit_insert(insert, **kwargs)
    preparer = compiler.preparer
    pk_columns = ", ".join([preparer.format_column(col) for col in insert.table.primary_key])
    result += f" ON CONFLICT ({pk_columns})"
    columns = [preparer.format_column(col) for col in insert.table.columns
               if col.name not in insert.table.primary_key]
    updates = ", ".join([f"{col} = excluded.{col}" for col in columns])
    result += f" DO UPDATE SET {updates}"
    return result


_AUTOINCR_TABLE_SPEC = ddl.TableSpec(
    fields=[ddl.FieldSpec(name="id", dtype=sqlalchemy.Integer, primaryKey=True)]
)


@dataclass
class _AutoincrementCompoundKeyWorkaround:
    """A workaround for SQLite's lack of support for compound primary keys that
    include an autoincrement field.
    """

    table: sqlalchemy.schema.Table
    """A single-column internal table that can be inserted into to yield
    autoincrement values (`sqlalchemy.schema.Table`).
    """

    column: str
    """The name of the column in the original table that needs to be populated
    with values from the internal table (`str`).
    """


class SqliteDatabase(Database):
    """An implementation of the `Database` interface for SQLite3.

    Parameters
    ----------
    connection : `sqlalchemy.engine.Connection`
        An existing connection created by a previous call to `connect`.
    origin : `int`
        An integer ID that should be used as the default for any datasets,
        quanta, or other entities that use a (autoincrement, origin) compound
        primary key.
    namespace : `str`, optional
        The namespace (schema) this database is associated with.  If `None`,
        the default schema for the connection is used (which may be `None`).
    writeable : `bool`, optional
            If `True`, allow write operations on the database, including
            ``CREATE TABLE``.

    Notes
    -----
    The case where ``namespace is not None`` is not yet tested, and may be
    broken; we need an API for attaching to different databases in order to
    write those tests, but haven't yet worked out what is common/different
    across databases well enough to define it.
    """

    def __init__(self, *, connection: sqlalchemy.engine.Connection, origin: int,
                 namespace: Optional[str] = None, writeable: bool = True):
        super().__init__(origin=origin, connection=connection, namespace=namespace)
        # Get the filename from a call to 'PRAGMA database_list'.
        with closing(connection.connection.cursor()) as cursor:
            dbList = list(cursor.execute("PRAGMA database_list").fetchall())
        if len(dbList) == 0:
            raise RuntimeError("No database in connection.")
        if namespace is None:
            namespace = "main"
        for _, dbname, filename in dbList:
            if dbname == namespace:
                break
        else:
            raise RuntimeError(f"No '{namespace}' database in connection.")
        if not filename:
            self.filename = None
        else:
            self.filename = filename
        self._writeable = writeable
        self._autoincr: Dict[str, _AutoincrementCompoundKeyWorkaround] = {}

    @classmethod
    def makeDefaultUri(cls, root: str) -> Optional[str]:
        return "sqlite:///" + os.path.join(root, "gen3.sqlite3")

    @classmethod
    def connect(cls, uri: Optional[str] = None, *, filename: Optional[str] = None,
                writeable: bool = True) -> sqlalchemy.engine.Connection:
        """Create a `sqlalchemy.engine.Connection` from a SQLAlchemy URI or
        filename.

        Parameters
        ----------
        uri : `str`
            A SQLAlchemy URI connection string.
        filename : `str`
            Name of the SQLite database file, or `None` to use an in-memory
            database.  Ignored if ``uri is not None``.
        origin : `int`
            An integer ID that should be used as the default for any datasets,
            quanta, or other entities that use a (autoincrement, origin)
            compound primary key.
        writeable : `bool`, optional
            If `True`, allow write operations on the database, including
            ``CREATE TABLE``.

        Returns
        -------
        cs : `sqlalchemy.engine.Connection`
            A database connection and transaction state.
        """
        # In order to be able to tell SQLite that we want a read-only or
        # read-write connection, we need to make the SQLite DBAPI connection
        # with a "URI"-based connection string.  SQLAlchemy claims it can do
        # this
        # (https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#uri-connections),
        # but it doesn't seem to work as advertised.  To work around this, we
        # use the 'creator' argument to sqlalchemy.engine.create_engine, which
        # lets us pass a callable that creates the DBAPI connection.
        if uri is None:
            if filename is None:
                target = ":memory:"
                uri = "sqlite://"
            else:
                target = f"file:{filename}"
                uri = f"sqlite:///{filename}"
        else:
            parsed = urllib.parse.urlparse(uri)
            queries = parsed.query.split("&")
            if "uri=true" in queries:
                # This is a SQLAlchemy URI that is already trying to make a
                # SQLite connection via a SQLite URI, and hence there may
                # be URI components for both SQLite and SQLAlchemy.  We
                # don't need to support that, and it'd be a
                # reimplementation of all of the (broken) logic in
                # SQLAlchemy for doing this, so we just don't.
                raise NotImplementedError("SQLite connection strings with 'uri=true' are not supported.")
            # This is just a SQLAlchemy URI with a non-URI SQLite
            # connection string inside it.  Pull that out so we can use it
            # in the creator call.
            if parsed.path.startswith("/"):
                filename = parsed.path[1:]
                target = f"file:{filename}"
            else:
                filename = None
                target = ":memory:"
        if filename is None:
            if not writeable:
                raise NotImplementedError("Read-only :memory: databases are not supported.")
        else:
            if writeable:
                target += '?mode=rwc&uri=true'
            else:
                target += '?mode=ro&uri=true'

        def creator() -> sqlite3.Connection:
            return sqlite3.connect(target, check_same_thread=False, uri=True)

        engine = sqlalchemy.engine.create_engine(uri, poolclass=sqlalchemy.pool.NullPool,
                                                 creator=creator)

        sqlalchemy.event.listen(engine, "connect", _onSqlite3Connect)
        sqlalchemy.event.listen(engine, "begin", _onSqlite3Begin)
        try:
            return engine.connect()
        except sqlalchemy.exc.OperationalError as err:
            raise RuntimeError(f"Error creating connection with uri='{uri}', filename='{filename}', "
                               f"target={target}.") from err

    @classmethod
    def fromConnection(cls, connection: sqlalchemy.engine.Connection, *, origin: int,
                       namespace: Optional[str] = None, writeable: bool = True) -> Database:
        return cls(connection=connection, origin=origin, writeable=writeable, namespace=namespace)

    def isWriteable(self) -> bool:
        return self._writeable

    def __str__(self) -> str:
        if self.filename:
            return f"SQLite3@{self.filename}"
        else:
            return "SQLite3@:memory:"

    # MyPy claims that the return type here isn't covariant with the return
    # type of the base class method, which is formally correct but irrelevant
    # - the base class return type is _GeneratorContextManager, but only
    # because it's generated by the contextmanager decorator.
    def declareStaticTables(self, *, create: bool) -> ContextManager[StaticTablesContext]:  # type: ignore
        # If the user asked for an in-memory, writeable database, assume
        # create=True.  This is only really relevant for tests, and it's
        # convenient there.
        return super().declareStaticTables(create=(create if self.filename else self.isWriteable()))

    def _convertFieldSpec(self, table: str, spec: ddl.FieldSpec, metadata: sqlalchemy.MetaData,
                          **kwargs: Any) -> sqlalchemy.schema.Column:
        if spec.autoincrement:
            if not spec.primaryKey:
                raise RuntimeError(f"Autoincrement field {table}.{spec.name} that is not a "
                                   f"primary key is not supported.")
            if spec.dtype != sqlalchemy.Integer:
                # SQLite's autoincrement is really limited; it only works if
                # the column type is exactly "INTEGER".  But it also doesn't
                # care about the distinctions between different integer types,
                # so it's safe to change it.
                spec = copy.copy(spec)
                spec.dtype = sqlalchemy.Integer
        return super()._convertFieldSpec(table, spec, metadata, **kwargs)

    def _makeColumnConstraints(self, table: str, spec: ddl.FieldSpec) -> List[sqlalchemy.CheckConstraint]:
        # For sqlite we force constraints on all string columns since sqlite
        # ignores everything otherwise and this leads to problems with
        # other databases.

        constraints = []
        if spec.dtype == sqlalchemy.String:
            name = self.shrinkDatabaseEntityName("_".join([table, "len", spec.name]))
            constraints.append(sqlalchemy.CheckConstraint(f"length({spec.name})<={spec.length}"
                                                          # Oracle converts
                                                          # empty strings to
                                                          # NULL so check
                                                          f" AND length({spec.name})>=1",
                                                          name=name))

        constraints.extend(super()._makeColumnConstraints(table, spec))
        return constraints

    def _convertTableSpec(self, name: str, spec: ddl.TableSpec, metadata: sqlalchemy.MetaData,
                          **kwargs: Any) -> sqlalchemy.schema.Table:
        primaryKeyFieldNames = set(field.name for field in spec.fields if field.primaryKey)
        autoincrFieldNames = set(field.name for field in spec.fields if field.autoincrement)
        if len(autoincrFieldNames) > 1:
            raise RuntimeError("At most one autoincrement field per table is allowed.")
        if len(primaryKeyFieldNames) > 1 and len(autoincrFieldNames) > 0:
            # SQLite's default rowid-based autoincrement doesn't work if the
            # field is just one field in a compound primary key.  As a
            # workaround, we create an extra table with just one column that
            # we'll insert into to generate those IDs.  That's only safe if
            # that single-column table's records are already unique with just
            # the autoincrement field, not the rest of the primary key.  In
            # practice, that means the single-column table's records are those
            # for which origin == self.origin.
            autoincrFieldName, = autoincrFieldNames
            otherPrimaryKeyFieldNames = primaryKeyFieldNames - autoincrFieldNames
            if otherPrimaryKeyFieldNames != {"origin"}:
                # We need the only other field in the key to be 'origin'.
                raise NotImplementedError(
                    "Compound primary keys with an autoincrement are only supported in SQLite "
                    "if the only non-autoincrement primary key field is 'origin'."
                )
            self._autoincr[name] = _AutoincrementCompoundKeyWorkaround(
                table=self._convertTableSpec(f"_autoinc_{name}", _AUTOINCR_TABLE_SPEC, metadata, **kwargs),
                column=autoincrFieldName
            )
        if not spec.recycleIds:
            kwargs = dict(kwargs, sqlite_autoincrement=True)
        return super()._convertTableSpec(name, spec, metadata, **kwargs)

    def insert(self, table: sqlalchemy.schema.Table, *rows: dict, returnIds: bool = False,
               select: Optional[sqlalchemy.sql.Select] = None,
               names: Optional[Iterable[str]] = None,
               ) -> Optional[List[int]]:
        autoincr = self._autoincr.get(table.name)
        if autoincr is not None:
            if select is not None:
                raise NotImplementedError(
                    "Cannot do INSERT INTO ... SELECT on a SQLite table with a simulated autoincrement "
                    "compound primary key"
                )
            # This table has a compound primary key that includes an
            # autoincrement.  That doesn't work natively in SQLite, so we
            # insert into a single-column table and use those IDs.
            if not rows:
                return [] if returnIds else None
            if autoincr.column in rows[0]:
                # Caller passed the autoincrement key values explicitly in the
                # first row.  They had better have done the same for all rows,
                # or SQLAlchemy would have a problem, even if we didn't.
                assert all(autoincr.column in row for row in rows)
                # We need to insert only the values that correspond to
                # ``origin == self.origin`` into the single-column table, to
                # make sure we don't generate conflicting keys there later.
                rowsForAutoincrTable = [dict(id=row[autoincr.column])
                                        for row in rows if row["origin"] == self.origin]
                # Insert into the autoincr table and the target table inside
                # a transaction.  The main-table insertion can take care of
                # returnIds for us.
                with self.transaction():
                    self._connection.execute(autoincr.table.insert(), *rowsForAutoincrTable)
                    return super().insert(table, *rows, returnIds=returnIds)
            else:
                # Caller did not pass autoincrement key values on the first
                # row.  Make sure they didn't ever do that, and also make
                # sure the origin that was passed in is always self.origin,
                # because we can't safely generate autoincrement values
                # otherwise.
                assert all(autoincr.column not in row and row["origin"] == self.origin for row in rows)
                # Insert into the autoincr table one by one to get the
                # primary key values back, then insert into the target table
                # in the same transaction.
                with self.transaction():
                    newRows = []
                    ids = []
                    for row in rows:
                        newRow = row.copy()
                        id = self._connection.execute(autoincr.table.insert()).inserted_primary_key[0]
                        newRow[autoincr.column] = id
                        newRows.append(newRow)
                        ids.append(id)
                    # Don't ever ask to returnIds here, because we've already
                    # got them.
                    super().insert(table, *newRows)
                if returnIds:
                    return ids
                else:
                    return None
        else:
            return super().insert(table, *rows, select=select, names=names, returnIds=returnIds)

    def replace(self, table: sqlalchemy.schema.Table, *rows: dict) -> None:
        if not (self.isWriteable() or table.key in self._tempTables):
            raise ReadOnlyDatabaseError(f"Attempt to replace into read-only database '{self}'.")
        if not rows:
            return
        if table.name in self._autoincr:
            raise NotImplementedError(
                "replace does not support compound primary keys with autoincrement fields."
            )
        self._connection.execute(_Replace(table), *rows)

    filename: Optional[str]
    """Name of the file this database is connected to (`str` or `None`).

    Set to `None` for in-memory databases.
    """
