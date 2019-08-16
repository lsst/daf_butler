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

__all__ = ("SqliteRegistry", )

from contextlib import closing

from sqlalchemy import event
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from sqlite3 import Connection as SQLite3Connection

from lsst.daf.butler.core.config import Config
from lsst.daf.butler.core.registry import RegistryConfig
from ..core.connectionString import ConnectionStringFactory
from lsst.daf.butler.core.repoRelocation import replaceRoot

from .sqlRegistry import SqlRegistry, SqlRegistryConfig


def _onSqlite3Connect(dbapiConnection, connectionRecord):
    assert isinstance(dbapiConnection, SQLite3Connection)
    # Prevent pysqlite from emitting BEGIN and COMMIT statements.
    dbapiConnection.isolation_level = None
    # Enable foreign keys
    with closing(dbapiConnection.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.execute("PRAGMA busy_timeout = 300000;")  # in ms, so 5min (way longer than should be needed)


def _onSqlite3Begin(connection):
    assert connection.dialect.name == "sqlite"
    # Replace pysqlite's buggy transaction handling that never BEGINs with our
    # own that does, and tell SQLite not to try to acquire a lock as soon as
    # we start a transaction (this should lead to more blocking and fewer
    # deadlocks).
    connection.execute("BEGIN IMMEDIATE")
    return connection


class SqliteRegistry(SqlRegistry):
    """Registry backed by a SQLite database.

    Parameters
    ----------
    config : `SqlRegistryConfig` or `str`
        Load configuration
    """

    @classmethod
    def setConfigRoot(cls, root, config, full, overwrite=True):
        """Set any filesystem-dependent config options for this Registry to
        be appropriate for a new empty repository with the given root.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the data repository.
        config : `Config`
            A Butler-level config object to update (but not a
            `ButlerConfig`, to avoid included expanded defaults).
        full : `ButlerConfig`
            A complete Butler config with all defaults expanded;
            repository-specific options that should not be obtained
            from defaults when Butler instances are constructed
            should be copied from ``full`` to ``config``.
        overwrite : `bool`, optional
            If `False`, do not modify a value in ``config`` if the value
            already exists.  Default is always to overwrite with the provided
            ``root``.

        Notes
        -----
        If a keyword is explicitly defined in the supplied ``config`` it
        will not be overridden by this method if ``overwrite`` is `False`.
        This allows explicit values set in external configs to be retained.
        """
        super().setConfigRoot(root, config, full, overwrite=overwrite)
        Config.updateParameters(RegistryConfig, config, full,
                                toUpdate={"db": f"sqlite:///{root}/gen3.sqlite3"},
                                overwrite=overwrite)

    def __init__(self, registryConfig, schemaConfig, dimensionConfig, create=False, butlerRoot=None):
        registryConfig = SqlRegistryConfig(registryConfig)
        if "db" in registryConfig:
            registryConfig["db"] = replaceRoot(registryConfig["db"], butlerRoot)
        if ":memory:" in registryConfig.get("db", ""):
            create = True
        super().__init__(registryConfig, schemaConfig, dimensionConfig, create, butlerRoot=butlerRoot)

    def _createEngine(self):
        conStrFactory = ConnectionStringFactory()
        conStr = conStrFactory.fromConfig(self.config)
        engine = create_engine(conStr, poolclass=NullPool,
                               connect_args={"check_same_thread": False})
        event.listen(engine, "connect", _onSqlite3Connect)
        event.listen(engine, "begin", _onSqlite3Begin)
        return engine
