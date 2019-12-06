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
from typing import Optional

import sqlite3
import sqlalchemy

from ..interfaces import Database


def _onSqlite3Connect(dbapiConnection, connectionRecord):
    assert isinstance(dbapiConnection, sqlite3.Connection)
    # Prevent pysqlite from emitting BEGIN and COMMIT statements.
    dbapiConnection.isolation_level = None
    # Enable foreign keys
    with closing(dbapiConnection.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.execute("PRAGMA busy_timeout = 300000;")  # in ms, so 5min (way longer than should be needed)


def _onSqlite3Begin(connection):
    assert connection.dialect.name == "sqlite"
    # Replace pysqlite's buggy transaction handling that never BEGINs with our
    # own that does, and tell SQLite to try to acquire a lock as soon as we
    # start a transaction (this should lead to more blocking and fewer
    # deadlocks).
    connection.execute("BEGIN IMMEDIATE")
    return connection


class SqliteDatabase(Database):
    """An implementation of the `Database` interface for SQLite3.

    Parameters
    ----------
    origin : `int`
        An integer ID that should be used as the default for any datasets,
        quanta, or other entities that use a (autoincrement, origin) compound
        primary key.
    filename : `str`, optional
        Absolute or relative path to the database file to load, or `None` to
        create a temporary in-memory database.
    writeable : `bool`, optional
        If `True` (default) allow writes to the database.
    """

    def __init__(self, *, origin: int, filename: Optional[str] = None, writeable: bool = True):
        target = f"file:{filename}" if filename is not None else ":memory:"
        if not writeable:
            target += '?mode=ro&uri=true'

        def creator():
            return sqlite3.connect(target, check_same_thread=False, uri=True)

        uri = f"sqlite:///{filename}"
        engine = sqlalchemy.engine.create_engine(uri, poolclass=sqlalchemy.pool.NullPool,
                                                 creator=creator)
        sqlalchemy.event.listen(engine, "connect", _onSqlite3Connect)
        sqlalchemy.event.listen(engine, "begin", _onSqlite3Begin)
        super().__init__(origin=origin, engine=engine)
        self._writeable = writeable
        self.filename = filename

    def isWriteable(self) -> bool:
        return self._writeable

    def __str__(self) -> str:
        return f"SQLite3@{self.filename}"

    filename: Optional[str]
    """Name of the file this database is connected to (`str` or `None`).

    Set to `None` for in-memory databases.
    """
