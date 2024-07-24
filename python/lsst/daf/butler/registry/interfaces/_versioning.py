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
    "IncompatibleVersionError",
    "VersionTuple",
    "VersionedExtension",
]

from abc import ABC, abstractmethod
from typing import NamedTuple


class IncompatibleVersionError(RuntimeError):
    """Exception raised when extension implemention is not compatible with
    schema version defined in database.
    """

    pass


class VersionTuple(NamedTuple):
    """Class representing a version number.

    Attributes
    ----------
    major : `int`
        Major version number.
    minor : `int`
        Minor version number.
    patch : `int`
        Patch level.
    """

    major: int
    minor: int
    patch: int

    @classmethod
    def fromString(cls, versionStr: str) -> VersionTuple:
        """Extract version number from a string.

        Parameters
        ----------
        versionStr : `str`
            Version number in string form "X.Y.Z", all components must be
            present.

        Returns
        -------
        version : `VersionTuple`
            Parsed version tuple.

        Raises
        ------
        ValueError
            Raised if string has an invalid format.
        """
        try:
            version = tuple(int(v) for v in versionStr.split("."))
        except ValueError as exc:
            raise ValueError(f"Invalid version  string '{versionStr}'") from exc
        if len(version) != 3:
            raise ValueError(f"Invalid version  string '{versionStr}', must consist of three numbers")
        return cls(*version)

    def checkCompatibility(self, registry_schema_version: VersionTuple, update: bool) -> bool:
        """Compare implementation schema version with schema version in
        registry.

        Parameters
        ----------
        registry_schema_version : `VersionTuple`
            Schema version that exists in registry or defined in a
            configuration for a registry to be created.
        update : `bool`
            If True then read-write access is expected.

        Returns
        -------
        compatible : `bool`
            True if schema versions are compatible.

        Notes
        -----
        This method implements default rules for checking schema compatibility:

            - if major numbers differ, schemas are not compatible;
            - otherwise, if minor versions are different then newer version can
              read schema made by older version, but cannot write into it;
              older version can neither read nor write into newer schema;
            - otherwise, different patch versions are totally compatible.

        Extensions that implement different versioning model will need to
        override their `VersionedExtension.checkCompatibility` method.
        """
        if self.major != registry_schema_version.major:
            # different major versions are not compatible at all
            return False
        if self.minor != registry_schema_version.minor:
            # different minor versions are backward compatible for read
            # access only
            return self.minor > registry_schema_version.minor and not update
        # patch difference does not matter
        return True

    def __str__(self) -> str:
        """Transform version tuple into a canonical string form."""
        return f"{self.major}.{self.minor}.{self.patch}"


class VersionedExtension(ABC):
    """Interface for extension classes with versions.

    Parameters
    ----------
    registry_schema_version : `VersionTuple` or `None`
        Schema version of this extension as defined in registry. If `None`, it
        means that registry schema was not initialized yet and the extension
        should expect that schema version returned by `newSchemaVersion` method
        will be used to initialize the registry database. If not `None`, it
        is guaranteed that this version has passed compatibility check.
    """

    def __init__(self, *, registry_schema_version: VersionTuple | None = None):
        self._registry_schema_version = registry_schema_version

    @classmethod
    def extensionName(cls) -> str:
        """Return full name of the extension.

        This name should match the name defined in registry configuration. It
        is also stored in registry attributes. Default implementation returns
        full class name.

        Returns
        -------
        name : `str`
            Full extension name.
        """
        return f"{cls.__module__}.{cls.__name__}"

    @classmethod
    @abstractmethod
    def currentVersions(cls) -> list[VersionTuple]:
        """Return schema version(s) supported by this extension class.

        Returns
        -------
        version : `list` [`VersionTuple`]
            Schema versions for this extension. Empty list is returned if an
            extension does not require its version to be saved or checked.
        """
        raise NotImplementedError()

    def newSchemaVersion(self) -> VersionTuple | None:
        """Return schema version for newly created registry.

        Returns
        -------
        version : `VersionTuple` or `None`
            Schema version created by this extension. `None` is returned if an
            extension does not require its version to be saved or checked.

        Notes
        -----
        Extension classes that support multiple schema versions need to
        override `_newDefaultSchemaVersion` method.
        """
        return self.clsNewSchemaVersion(self._registry_schema_version)

    @classmethod
    def clsNewSchemaVersion(cls, schema_version: VersionTuple | None) -> VersionTuple | None:
        """Class method which returns schema version to use for newly created
        registry database.

        Parameters
        ----------
        schema_version : `VersionTuple` or `None`
            Configured schema version or `None` if default schema version
            should be created. If not `None` then it is guaranteed to be
            compatible with `currentVersions`.

        Returns
        -------
        version : `VersionTuple` or `None`
            Schema version created by this extension. `None` is returned if an
            extension does not require its version to be saved or checked.

        Notes
        -----
        Default implementation of this method can work in simple cases. If
        the extension only supports single schema version than that version is
        returned. If the extension supports multiple schema versions and
        ``schema_version`` is not `None` then ``schema_version`` is returned.
        If the extension supports multiple schema versions, but
        ``schema_version`` is `None` it calls ``_newDefaultSchemaVersion``
        method which needs to be reimplemented in a subsclass.
        """
        my_versions = cls.currentVersions()
        if not my_versions:
            return None
        elif len(my_versions) == 1:
            return my_versions[0]
        else:
            if schema_version is not None:
                assert schema_version in my_versions, "Schema version must be compatible."
                return schema_version
            else:
                return cls._newDefaultSchemaVersion()

    @classmethod
    def _newDefaultSchemaVersion(cls) -> VersionTuple:
        """Return default shema version for new registry for extensions that
        support multiple schema versions.

        Notes
        -----
        Default implementation simply raises an exception. Managers which
        support multiple schema versions must re-implement this method.
        """
        raise NotImplementedError(
            f"Extension {cls.extensionName()} supports multiple schema versions, "
            "its newSchemaVersion() method needs to be re-implemented."
        )

    @classmethod
    def checkCompatibility(cls, registry_schema_version: VersionTuple, update: bool) -> None:
        """Check that schema version defined in registry is compatible with
        current implementation.

        Parameters
        ----------
        registry_schema_version : `VersionTuple`
            Schema version that exists in registry or defined in a
            configuration for a registry to be created.
        update : `bool`
            If True then read-write access is expected.

        Raises
        ------
        IncompatibleVersionError
            Raised if schema version is not supported by implementation.

        Notes
        -----
        Default implementation uses `VersionTuple.checkCompatibility` on
        the versions returned from `currentVersions` method. Subclasses that
        support different compatibility model will overwrite this method.
        """
        # Extensions that do not define current versions are compatible with
        # anything.
        if my_versions := cls.currentVersions():
            # If there are multiple version supported one of them should be
            # compatible to succeed.
            for version in my_versions:
                if version.checkCompatibility(registry_schema_version, update):
                    return
            raise IncompatibleVersionError(
                f"Extension versions {my_versions} is not compatible with registry "
                f"schema version {registry_schema_version} for extension {cls.extensionName()}"
            )

    @classmethod
    def checkNewSchemaVersion(cls, schema_version: VersionTuple) -> None:
        """Verify that requested schema version can be created by an extension.

        Parameters
        ----------
        schema_version : `VersionTuple`
            Schema version that this extension is asked to create.

        Notes
        -----
        This method may be used only occasionally when a specific schema
        version is given in a regisitry config file. This can be used with an
        extension that supports multiple schem versions to make it create new
        schema with a non-default version number. Default implementation
        compares requested version with one of the version returned from
        `currentVersions`.
        """
        if my_versions := cls.currentVersions():
            # If there are multiple version supported one of them should be
            # compatible to succeed.
            for version in my_versions:
                # Need to have an exact match
                if version == schema_version:
                    return
            raise IncompatibleVersionError(
                f"Extension {cls.extensionName()} cannot create schema version {schema_version}, "
                f"supported schema versions: {my_versions}"
            )
