# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("RegistryConfig",)

from typing import TYPE_CHECKING

from lsst.utils import doImportType

from .._config import ConfigSubset
from ..repo_relocation import replaceRoot
from .connectionString import ConnectionStringFactory
from .interfaces import Database

if TYPE_CHECKING:
    import sqlalchemy
    from lsst.resources import ResourcePathExpression


class RegistryConfig(ConfigSubset):
    """Configuration specific to a butler Registry."""

    component = "registry"
    requiredKeys = ("db",)
    defaultConfigFile = "registry.yaml"

    def getDialect(self) -> str:
        """Parse the `db` key of the config and returns the database dialect.

        Returns
        -------
        dialect : `str`
            Dialect found in the connection string.
        """
        conStr = ConnectionStringFactory.fromConfig(self)
        return conStr.get_backend_name()

    def getDatabaseClass(self) -> type[Database]:
        """Return the `Database` class targeted by configuration values.

        The appropriate class is determined by parsing the `db` key to extract
        the dialect, and then looking that up under the `engines` key of the
        registry config.
        """
        dialect = self.getDialect()
        if dialect not in self["engines"]:
            raise ValueError(f"Connection string dialect has no known aliases. Received: {dialect}")
        databaseClassName = self["engines", dialect]
        databaseClass = doImportType(databaseClassName)
        if not issubclass(databaseClass, Database):
            raise TypeError(f"Imported database class {databaseClassName} is not a Database")
        return databaseClass

    def makeDefaultDatabaseUri(self, root: str) -> str | None:
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

    def replaceRoot(self, root: ResourcePathExpression | None) -> None:
        """Replace any occurrences of `BUTLER_ROOT_TAG` in the connection
        with the given root directory.

        Parameters
        ----------
        root : `lsst.resources.ResourcePathExpression`, or `None`
            String to substitute for `BUTLER_ROOT_TAG`.  Passing `None` here is
            allowed only as a convenient way to raise an exception
            (`ValueError`).

        Raises
        ------
        ValueError
            Raised if ``root`` is not set but a value is required.
        """
        self["db"] = replaceRoot(self["db"], root)

    @property
    def connectionString(self) -> sqlalchemy.engine.url.URL:
        """Return the connection string to the underlying database
        (`sqlalchemy.engine.url.URL`).
        """
        return ConnectionStringFactory.fromConfig(self)
