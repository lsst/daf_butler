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

__all__ = ["PostgresqlDatabase"]

from contextlib import contextmanager, closing
from typing import Iterator, Optional

import sqlalchemy

from ..interfaces import Database, ReadOnlyDatabaseError
from ..nameShrinker import NameShrinker


class PostgresqlDatabase(Database):
    """An implementation of the `Database` interface for PostgreSQL.

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
    This currently requires the psycopg2 driver to be used as the backend for
    SQLAlchemy.  Running the tests for this class requires the
    ``testing.postgresql`` be installed, which we assume indicates that a
    PostgreSQL server is installed and can be run locally in userspace.
    """

    def __init__(self, *, connection: sqlalchemy.engine.Connection, origin: int,
                 namespace: Optional[str] = None, writeable: bool = True):
        super().__init__(origin=origin, connection=connection, namespace=namespace)
        dbapi = connection.connection
        try:
            dsn = dbapi.get_dsn_parameters()
        except (AttributeError, KeyError) as err:
            raise RuntimeError("Only the psycopg2 driver for PostgreSQL is supported.") from err
        if namespace is None:
            namespace = connection.execute("SELECT current_schema();").scalar()
        self.namespace = namespace
        self.dbname = dsn.get("dbname")
        self._writeable = writeable
        self._shrinker = NameShrinker(connection.engine.dialect.max_identifier_length)

    @classmethod
    def connect(cls, uri: str, *, writeable: bool = True) -> sqlalchemy.engine.Connection:
        return sqlalchemy.engine.create_engine(uri, poolclass=sqlalchemy.pool.NullPool).connect()

    @classmethod
    def fromConnection(cls, connection: sqlalchemy.engine.Connection, *, origin: int,
                       namespace: Optional[str] = None, writeable: bool = True) -> Database:
        return cls(connection=connection, origin=origin, namespace=namespace, writeable=writeable)

    @contextmanager
    def transaction(self, *, interrupting: bool = False) -> Iterator[None]:
        with super().transaction(interrupting=interrupting):
            if not self.isWriteable():
                with closing(self._connection.connection.cursor()) as cursor:
                    cursor.execute("SET TRANSACTION READ ONLY")
            yield

    def isWriteable(self) -> bool:
        return self._writeable

    def __str__(self) -> str:
        return f"PostgreSQL@{self.dbname}:{self.namespace}"

    def shrinkDatabaseEntityName(self, original: str) -> str:
        return self._shrinker.shrink(original)

    def expandDatabaseEntityName(self, shrunk: str) -> str:
        return self._shrinker.expand(shrunk)

    def replace(self, table: sqlalchemy.schema.Table, *rows: dict) -> None:
        if not (self.isWriteable() or table.key in self._tempTables):
            raise ReadOnlyDatabaseError(f"Attempt to replace into read-only database '{self}'.")
        if not rows:
            return
        # This uses special support for UPSERT in PostgreSQL backend:
        # https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#insert-on-conflict-upsert
        query = sqlalchemy.dialects.postgresql.dml.insert(table)
        # In the SET clause assign all columns using special `excluded`
        # pseudo-table.  If some column in the table does not appear in the
        # INSERT list this will set it to NULL.
        excluded = query.excluded
        data = {column.name: getattr(excluded, column.name)
                for column in table.columns
                if column.name not in table.primary_key}
        query = query.on_conflict_do_update(constraint=table.primary_key, set_=data)
        self._connection.execute(query, *rows)
