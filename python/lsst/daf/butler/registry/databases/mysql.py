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

__all__ = ["MySqlDatabase"]

from contextlib import closing, contextmanager
from typing import Iterator, Optional

import sqlalchemy

from ..interfaces import Database, ReadOnlyDatabaseError
from ..nameShrinker import NameShrinker


class MySqlDatabase(Database):
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
    This currently requires the mysqlclient driver to be used as the backend
    for SQLAlchemy.
    """

    def __init__(
        self,
        *,
        connection: sqlalchemy.engine.Connection,
        origin: int,
        namespace: Optional[str] = None,
        writeable: bool = True,
    ):
        super().__init__(origin=origin, connection=connection, namespace=namespace)
        # Relationship of the database specified in the connection and
        # the supplied namespace is uncertain.
        if namespace is not None:
            # This will warn if the schema/database already exists
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {namespace};")
            connection.execute(f"USE {namespace};")
        self.namespace = namespace
        self.dbname = "No idea"
        self._writeable = writeable
        self._shrinker = NameShrinker(64)  # connection.engine.dialect.max_identifier_length is wrong

    @classmethod
    def connect(cls, uri: str, *, writeable: bool = True) -> sqlalchemy.engine.Connection:
        return sqlalchemy.engine.create_engine(uri, poolclass=sqlalchemy.pool.NullPool).connect()

    @classmethod
    def fromConnection(
        cls,
        connection: sqlalchemy.engine.Connection,
        *,
        origin: int,
        namespace: Optional[str] = None,
        writeable: bool = True,
    ) -> Database:
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
        return f"MySQL@{self.dbname}:{self.namespace}"

    def shrinkDatabaseEntityName(self, original: str) -> str:
        return self._shrinker.shrink(original)

    def expandDatabaseEntityName(self, shrunk: str) -> str:
        return self._shrinker.expand(shrunk)

    def replace(self, table: sqlalchemy.schema.Table, *rows: dict) -> None:
        # This is all wrong
        if not self.isWriteable():
            raise ReadOnlyDatabaseError(f"Attempt to replace into read-only database '{self}'.")
        if not rows:
            return
        raise NotImplementedError("No support for replace in MySQL")
