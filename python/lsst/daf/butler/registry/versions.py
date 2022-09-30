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

__all__ = [
    "ButlerVersionsManager",
    "IncompatibleVersionError",
    "MissingVersionError",
    "MissingManagerError",
    "ManagerMismatchError",
    "DigestMismatchError",
]

import logging
from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, Optional

from deprecated.sphinx import deprecated

from .interfaces import VersionedExtension, VersionTuple

if TYPE_CHECKING:
    from .interfaces import ButlerAttributeManager


_LOG = logging.getLogger(__name__)


class MissingVersionError(RuntimeError):
    """Exception raised when existing database is missing attributes with
    version numbers.
    """

    pass


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


class DigestMismatchError(RuntimeError):
    """Exception raised when schema digest is not equal to stored digest."""

    pass


class VersionInfo:
    """Representation of version information as defined by configuration.

    Parameters
    ----------
    version : `VersionTuple`
        Version number in parsed format.
    digest : `str`, optional
        Optional digest of the corresponding part of the schema definition.

    Notes
    -----
    Schema digest is supposed to help with detecting unintentional schema
    changes in the code without upgrading schema version. Digest is
    constructed whom the set of table definitions and is compared to a digest
    defined in configuration, if two digests differ it means schema was
    changed. Intentional schema updates will need to update both configured
    schema version and schema digest.
    """

    def __init__(self, version: VersionTuple, digest: Optional[str] = None):
        self.version = version
        self.digest = digest


class ButlerVersionsManager:
    """Utility class to manage and verify schema version compatibility.

    Parameters
    ----------
    attributes : `ButlerAttributeManager`
        Attribute manager instance.
    managers : `dict` [`str`, `VersionedExtension`]
        Mapping of extension type as defined in configuration (e.g.
        "collections") to corresponding instance of manager.
    """

    def __init__(self, attributes: ButlerAttributeManager, managers: Mapping[str, Any]):
        self._attributes = attributes
        self._managers: MutableMapping[str, VersionedExtension] = {}
        # we only care about managers implementing VersionedExtension interface
        for name, manager in managers.items():
            if isinstance(manager, VersionedExtension):
                self._managers[name] = manager
            elif manager is not None:
                # All regular managers need to support versioning mechanism.
                _LOG.warning("extension %r does not implement VersionedExtension", name)
        self._emptyFlag: Optional[bool] = None

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
    def _managerVersionKey(cls, extension: VersionedExtension) -> str:
        """Return key used to store manager version.

        Parameters
        ----------
        extension : `VersionedExtension`
            Instance of the extension.

        Returns
        -------
        key : `str`
            Name of the key in attributes table.
        """
        return "version:" + extension.extensionName()

    @classmethod
    def _managerDigestKey(cls, extension: VersionedExtension) -> str:
        """Return key used to store manager schema digest.

        Parameters
        ----------
        extension : `VersionedExtension`
            Instance of the extension.

        Returns
        -------
        key : `str`
            Name of the key in attributes table.
        """
        return "schema_digest:" + extension.extensionName()

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

    def storeManagersConfig(self) -> None:
        """Store configured extension names in attributes table.

        For each extension we store a record with the key
        "config:registry.managers.{name}" and fully qualified class name as a
        value.
        """
        for name, extension in self._managers.items():
            key = self._managerConfigKey(name)
            value = extension.extensionName()
            self._attributes.set(key, value)
            _LOG.debug("saved manager config %s=%s", key, value)
        self._emptyFlag = False

    def storeManagersVersions(self) -> None:
        """Store current manager versions in registry arttributes.

        For each extension we store two records:

            - record with the key "version:{fullExtensionName}" and version
              number in its string format as a value,
            - record with the key "schema_digest:{fullExtensionName}" and
              schema digest as a value.
        """
        for extension in self._managers.values():

            version = extension.currentVersion()
            if version:
                key = self._managerVersionKey(extension)
                value = str(version)
                self._attributes.set(key, value)
                _LOG.debug("saved manager version %s=%s", key, value)

            digest = extension.schemaDigest()
            if digest is not None:
                key = self._managerDigestKey(extension)
                self._attributes.set(key, digest)
                _LOG.debug("saved manager schema digest %s=%s", key, digest)

        self._emptyFlag = False

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

    def checkManagersConfig(self) -> None:
        """Compare configured manager names with stored in database.

        Raises
        ------
        ManagerMismatchError
            Raised if manager names are different.
        MissingManagerError
            Raised if database has no stored manager name.
        """
        if self._attributesEmpty:
            return

        missing = []
        mismatch = []
        for name, extension in self._managers.items():
            key = self._managerConfigKey(name)
            storedMgr = self._attributes.get(key)
            _LOG.debug("found manager config %s=%s", key, storedMgr)
            if storedMgr is None:
                missing.append(name)
                continue
            if extension.extensionName() != storedMgr:
                mismatch.append(f"{name}: configured {extension.extensionName()}, stored: {storedMgr}")
        if missing:
            raise MissingManagerError("Cannot find stored configuration for managers: " + ", ".join(missing))
        if mismatch:
            raise ManagerMismatchError(
                "Configured managers do not match registry-stored names:\n" + "\n".join(missing)
            )

    def checkManagersVersions(self, writeable: bool) -> None:
        """Compare configured versions with the versions stored in database.

        Parameters
        ----------
        writeable : `bool`
            If ``True`` then read-write access needs to be checked.

        Raises
        ------
        IncompatibleVersionError
            Raised if versions are not compatible.
        MissingVersionError
            Raised if database has no stored version for one or more groups.
        """
        if self._attributesEmpty:
            return

        for extension in self._managers.values():
            version = extension.currentVersion()
            if version:
                key = self._managerVersionKey(extension)
                storedVersionStr = self._attributes.get(key)
                _LOG.debug("found manager version %s=%s, current version %s", key, storedVersionStr, version)
                if storedVersionStr is None:
                    raise MissingVersionError(f"Failed to read version number {key}")
                storedVersion = VersionTuple.fromString(storedVersionStr)
                if not self.checkCompatibility(storedVersion, version, writeable):
                    raise IncompatibleVersionError(
                        f"Configured version {version} is not compatible with stored version "
                        f"{storedVersion} for extension {extension.extensionName()}"
                    )

    @deprecated(reason="Schema checksums are ignored", category=FutureWarning, version="v24.0")
    def checkManagersDigests(self) -> None:
        """Compare current schema digests with digests stored in database.

        Raises
        ------
        DigestMismatchError
            Raised if digests are not equal.

        Notes
        -----
        This method is not used currently and will probably disappear in the
        future as we remove schema checksums.
        """
        if self._attributesEmpty:
            return

        for extension in self._managers.values():
            digest = extension.schemaDigest()
            if digest is not None:
                key = self._managerDigestKey(extension)
                storedDigest = self._attributes.get(key)
                _LOG.debug("found manager schema digest %s=%s, current digest %s", key, storedDigest, digest)
                if storedDigest != digest:
                    raise DigestMismatchError(
                        f"Current schema digest '{digest}' is not the same as stored digest "
                        f"'{storedDigest}' for extension {extension.extensionName()}"
                    )
