# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import gc
import secrets
import unittest
from collections.abc import Iterator
from contextlib import contextmanager

import sqlalchemy

from .._butler_config import ButlerConfig
from .._config import Config

try:
    from testing.postgresql import Postgresql
except ImportError:
    Postgresql = None


@contextmanager
def setup_postgres_test_db() -> Iterator[TemporaryPostgresInstance]:
    """Set up a temporary postgres instance that can be used for testing the
    Butler.
    """
    if Postgresql is None:
        raise unittest.SkipTest("testing.postgresql module not available.")

    with Postgresql() as server:
        engine = sqlalchemy.engine.create_engine(server.url())
        instance = TemporaryPostgresInstance(server, engine)
        with instance.begin() as connection:
            connection.execute(sqlalchemy.text("CREATE EXTENSION btree_gist;"))

        yield instance

        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        engine.dispose()


class TemporaryPostgresInstance:  # numpydoc ignore=PR01
    """Wrapper for a temporary postgres database with utilities for connecting
    a Butler to it.
    """

    def __init__(self, server: Postgresql, engine: sqlalchemy.Engine) -> None:
        self._server = server
        self._engine = engine

    @property
    def url(self) -> str:
        """Return connection URL for the temporary database server."""
        return self._server.url()

    @contextmanager
    def begin(self) -> Iterator[sqlalchemy.Connection]:
        """Return a SQLAlchemy connection to the test database."""
        with self._engine.begin() as connection:
            yield connection

    def patch_butler_config(self, config: ButlerConfig | Config) -> None:  # numpydoc ignore=PR01
        """Modify a butler configuration in-place to point the registry to the
        temporary database in a new empty namespace.
        """
        config["registry", "db"] = self.url
        config["registry", "namespace"] = self.generate_namespace_name()

    def patch_registry_config(self, config: Config) -> None:  # numpydoc ignore=PR01
        """Modify a registry configuration in-place to point the database
        connection to the temporary database in a new empty namespace.
        """
        config["db"] = self.url
        config["namespace"] = self.generate_namespace_name()

    def generate_namespace_name(self) -> str:
        """Return a unique namespace name that can be used to separate the data
        from multiple tests.
        """
        return f"namespace_{secrets.token_hex(8).lower()}"

    def server_major_version(self) -> int:
        """Return the major version number of the Postgres server (e.g. 13 or
        16).
        """
        from ..registry.databases.postgresql import get_postgres_server_version

        with self.begin() as connection:
            return get_postgres_server_version(connection)[0]
