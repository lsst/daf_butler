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

__all__ = ("RegistryConfig",)

from typing import Type, TYPE_CHECKING

from lsst.utils import doImport

from .connectionString import ConnectionStringFactory
from .config import ConfigSubset
from .repoRelocation import replaceRoot

if TYPE_CHECKING:
    from ..registry.interfaces import Database


class RegistryConfig(ConfigSubset):
    component = "registry"
    requiredKeys = ("db",)
    defaultConfigFile = "registry.yaml"

    def getDialect(self):
        """Parses the `db` key of the config and returns the database dialect.

        Returns
        -------
        dialect : `str`
            Dialect found in the connection string.
        """
        conStr = ConnectionStringFactory.fromConfig(self)
        return conStr.get_backend_name()

    def getRegistryClass(self):
        """Returns registry class targeted by configuration values.

        The appropriate class is determined from the `cls` key, if it exists.
        Otherwise the `db` key is parsed and the correct class is determined
        from a list of aliases found under `clsMap` key of the registry config.

        Returns
        -------
        registry : `type`
           Class of type `Registry` targeted by the registry configuration.
        """
        if self.get("cls") is not None:
            registryClass = self.get("cls")
        else:
            dialect = self.getDialect()
            if dialect not in self["clsMap"]:
                raise ValueError(f"Connection string dialect has no known aliases. Received: {dialect}")
            registryClass = self.get(("clsMap", dialect))

        return doImport(registryClass)

    def getDatabaseClass(self) -> Type[Database]:
        """Returns the `Database` class targeted by configuration values.

        The appropriate class is determined by parsing the `db` key to extract
        the dialect, and then looking that up under the `engines` key of the
        registry config.
        """
        dialect = self.getDialect()
        if dialect not in self["engines"]:
            raise ValueError(f"Connection string dialect has no known aliases. Received: {dialect}")
        databaseClass = self["engines", dialect]
        return doImport(databaseClass)

    def makeDefaultDatabaseUri(self, root: str):
        """Return a default 'db' URI for the registry configured here that is
        appropriate for a new empty repository with the given root.

        Parameters
        ----------
        root : `str`
            Filesystem path to the root of the data repository.

        Returns
        -------
        uri : `str`
            URI usable as the 'db' string in a `RegistryConfig`.
        """
        DatabaseClass = self.getDatabaseClass()
        return DatabaseClass.makeDefaultUri(root)

    def replaceRoot(self, root: str):
        """Replace any occurrences of `BUTLER_ROOT_TAG` in the connection
        with the given root directory.
        """
        self["db"] = replaceRoot(self["db"], root)

    @property
    def connectionString(self):
        """Return the connection string to the underlying database
        (`sqlalchemy.engine.url.URL`).
        """
        return ConnectionStringFactory.fromConfig(self)
