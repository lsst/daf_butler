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

__all__ = [
    "ButlerVersionsManager",
    "IncompatibleVersionError",
    "MissingManagerError",
    "ManagerMismatchError",
]

import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING

from .interfaces import VersionedExtension, VersionTuple

if TYPE_CHECKING:
    from .interfaces import ButlerAttributeManager


_LOG = logging.getLogger(__name__)


class IncompatibleVersionError(RuntimeError):
    """Exception raised when configured version number is not compatible with
    database version.
    """

    pass


class MissingManagerError(RuntimeError):
    """Exception raised when manager name is missing from registry."""

    pass


class ManagerMismatchError(RuntimeError):
    """Exception raised when configured manager name does not match name
    stored in the database.
    """

    pass


class ButlerVersionsManager:
    """Utility class to manage and verify schema version compatibility.

    Parameters
    ----------
    attributes : `ButlerAttributeManager`
        Attribute manager instance.
    """

    def __init__(self, attributes: ButlerAttributeManager):
        self._attributes = attributes
        # Maps manager type to its class name and schema version.
        self._cache: Mapping[str, tuple[str, VersionTuple | None]] | None = None
        self._emptyFlag: bool | None = None

    @classmethod
    def _managerConfigKey(cls, name: str) -> str:
        """Return key used to store manager config.

        Parameters
        ----------
        name : `str`
            Name of the namager type, e.g. "dimensions"

        Returns
        -------
        key : `str`
            Name of the key in attributes table.
        """
        return f"config:registry.managers.{name}"

    @classmethod
    def _managerVersionKey(cls, extensionName: str) -> str:
        """Return key used to store manager version.

        Parameters
        ----------
        extensionName : `str`
            Extension name (e.g. its class name).

        Returns
        -------
        key : `str`
            Name of the key in attributes table.
        """
        return f"version:{extensionName}"

    @property
    def _manager_data(self) -> Mapping[str, tuple[str, VersionTuple | None]]:
        """Retrieve per-manager type name and schema version."""
        if not self._cache:
            self._cache = {}

            # Number of items in attributes table is small, read all of them
            # in a single query and filter later.
            attributes = dict(self._attributes.items())

            for name, value in attributes.items():
                if name.startswith("config:registry.managers."):
                    _, _, manager_type = name.rpartition(".")
                    manager_class = value
                    version_str = attributes.get(self._managerVersionKey(manager_class))
                    if version_str is None:
                        self._cache[manager_type] = (manager_class, None)
                    else:
                        version = VersionTuple.fromString(version_str)
                        self._cache[manager_type] = (manager_class, version)

        return self._cache

    @staticmethod
    def checkCompatibility(old_version: VersionTuple, new_version: VersionTuple, update: bool) -> bool:
        """Compare two versions for compatibility.

        Parameters
        ----------
        old_version : `VersionTuple`
            Old schema version, typically one stored in a database.
        new_version : `VersionTuple`
            New schema version, typically version defined in configuration.
        update : `bool`
            If True then read-write access is expected.
        """
        if old_version.major != new_version.major:
            # different major versions are not compatible at all
            return False
        if old_version.minor != new_version.minor:
            # different minor versions are backward compatible for read
            # access only
            return new_version.minor > old_version.minor and not update
        # patch difference does not matter
        return True

    def storeManagersConfig(self, managers: Mapping[str, VersionedExtension]) -> None:
        """Store configured extension names and their versions.

        Parameters
        ----------
        managers : `~collections.abc.Mapping` [`str`, `type`]
            Collection of manager extension classes, the key is a manager type,
            e.g. "datasets".

        Notes
        -----
        For each extension we store two records:
        - with the key "config:registry.managers.{name}" and fully qualified
          class name as a value,
        - with the key "version:{fullExtensionName}" and version number in its
          string format as a value.
        """
        for name, extension in managers.items():
            key = self._managerConfigKey(name)
            value = extension.extensionName()
            self._attributes.set(key, value)
            _LOG.debug("saved manager config %s=%s", key, value)

            version = extension.newSchemaVersion()
            if version:
                key = self._managerVersionKey(extension.extensionName())
                value = str(version)
                self._attributes.set(key, value)
                _LOG.debug("saved manager version %s=%s", key, value)

    @property
    def _attributesEmpty(self) -> bool:
        """True if attributes table is empty."""
        # There are existing repositories where attributes table was not
        # filled, we don't want to force schema migration in this case yet
        # (and we don't have tools) so we allow this as valid use case and
        # skip all checks but print a warning.
        if self._emptyFlag is None:
            self._emptyFlag = self._attributes.empty()
            if self._emptyFlag:
                _LOG.warning("Attributes table is empty, schema may need an upgrade.")
        return self._emptyFlag

    def checkManagersConfig(self, managers: Mapping[str, type[VersionedExtension]]) -> None:
        """Compare configured manager names versions with stored in database.

        Parameters
        ----------
        managers : `~collections.abc.Mapping` [ `str`, `type`]
            The configured managers to check.

        Raises
        ------
        ManagerMismatchError
            Raised if manager names are different.
        MissingManagerError
            Raised if database has no stored manager name.
        IncompatibleVersionError
            Raised if versions are not compatible.
        """
        if self._attributesEmpty:
            return

        manager_data = self._manager_data

        missing = []
        mismatch = []
        for name, extension in managers.items():
            try:
                manager_class, _ = manager_data[name]
                _LOG.debug("found manager config %s=%s", name, manager_class)
            except KeyError:
                missing.append(name)
                continue
            if extension.extensionName() != manager_class:
                mismatch.append(f"{name}: configured {extension.extensionName()}, stored: {manager_class}")
        if missing:
            raise MissingManagerError("Cannot find stored configuration for managers: " + ", ".join(missing))
        if mismatch:
            raise ManagerMismatchError(
                "Configured managers do not match registry-stored names:\n" + "\n".join(mismatch)
            )

    def managerVersions(self) -> Mapping[str, VersionTuple]:
        """Return schema versions for each manager.

        Returns
        -------
        versions : `~collections.abc.Mapping` [`str`, `VersionTuple`]
            Mapping of managert type (e.g. "datasets") to its schema version.
        """
        versions = {}
        for manager_type, (_, version) in self._manager_data.items():
            if version is not None:
                versions[manager_type] = version
        return versions
