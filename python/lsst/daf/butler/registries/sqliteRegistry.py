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

from contextlib import closing

from sqlalchemy.engine import Engine
from sqlalchemy import event

from sqlite3 import Connection as SQLite3Connection

from .sqlRegistry import SqlRegistry

__all__ = ("SqliteRegistry", )


@event.listens_for(Engine, "connect")
def _enableForeignKeysOnSqlite3(dbapiConnection, connectionRecord):
    if isinstance(dbapiConnection, SQLite3Connection):
        with closing(dbapiConnection.cursor()) as cursor:
            cursor.execute("PRAGMA foreign_keys=ON;")


class SqliteRegistry(SqlRegistry):
    """Registry backed by a SQLite database.

    Parameters
    ----------
    config : `SqlRegistryConfig` or `str`
        Load configuration
    """

    def __init__(self, registryConfig, schemaConfig):
        super().__init__(registryConfig, schemaConfig)
