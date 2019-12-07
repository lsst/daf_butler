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

from typing import Optional

import sqlalchemy

from ..interfaces import Database


class PostgresqlDatabase(Database):
    """An implementation of the `Database` interface for PostgreSQL.

    Parameters
    ----------
    origin : `int`
        An integer ID that should be used as the default for any datasets,
        quanta, or other entities that use a (autoincrement, origin) compound
        primary key.
    uri : `str`, optional
        Database connection string.
    namespace : `str`, optional
        Namespace (schema) for all tables used by this database.  May be `None`
        only if a schema is included in ``uri``.
    writeable : `bool`, optional
        If `True` (default) allow writes to the database.

    Notes
    -----
    This currently requires the psycopg2 driver to be used as the backend for
    SQLAlchemy.  Running the tests for this class requires the
    ``testing.postgresql`` be installed, which we assume indicates that a
    PostgreSQL server is installed and can be run locally in userspace.
    """

    def __init__(self, *, origin: int, uri: str, namespace: Optional[str] = None, writeable: bool = True):
        engine = sqlalchemy.engine.create_engine(uri, pool_size=1)
        dbapi = engine.raw_connection()
        try:
            dsn = dbapi.get_dsn_parameters()
        except (AttributeError, KeyError) as err:
            raise RuntimeError(f"Only the psycopg2 driver for PostgreSQL is supported.") from err
        if namespace is None:
            namespace = dsn.get("schema")
            if namespace is None:
                raise RuntimeError("Namespace (schema) must be provided as an argument or included in URI.")
        else:
            if dsn.get("schema", namespace) != namespace:
                raise RuntimeError("'namespace' and 'uri' arguments specify different schemas.")
        super().__init__(origin=origin, engine=engine, namespace=namespace)
        if not writeable:
            dbapi.set_session(readonly=True)
        self.dbname = dsn["dbname"]
        self._writeable = writeable

    def isWriteable(self) -> bool:
        return self._writeable

    def __str__(self) -> str:
        return f"PostgreSQL@{self.dbname}:{self.namespace}"
