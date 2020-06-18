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
    "ButlerVersionsManager", "IncompatibleVersionError", "MissingVersionError"
]

import hashlib
import logging
from typing import (
    TYPE_CHECKING,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
)

import sqlalchemy

if TYPE_CHECKING:
    from .interfaces import (
        ButlerAttributeManager,
    )
    from ..core import Config


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


class VersionTuple(NamedTuple):
    """Class representing a version number.

    Parameters
    ----------
    major, minor, patch : `int`
        Version number componenets
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
            Version number in string form "X.Y.Z", all componenets must be
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

    def __str__(self) -> str:
        """Transform version tuple into a canonical string form.
        """
        return f"{self.major}.{self.minor}.{self.patch}"


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
    versions : `dict` [`str`, `VersionInfo`]
        Mapping of the group name to corresponding schema version and digest.
        Group represents a piece of overall database schema, group names are
        typically defined by configuration.
    """
    def __init__(self, versions: Mapping[str, VersionInfo]):
        self._versions = versions
        self._tablesGroups: MutableMapping[str, List[sqlalchemy.schema.Table]] = {}

    @classmethod
    def fromConfig(cls, schemaVersionConfig: Config) -> ButlerVersionsManager:
        """Make `ButlerVersionsManager` instance based on configuration.

        Parameters
        ----------
        schemaVersionConfig : `Config`
            Configuration object describing schema versions, typically
            "schema_versions" sub-object of registry configuration.

        Returns
        -------
        manager : `ButlerVersionsManager`
            New instance of the versions manager.
        """
        versions = {}
        for key, vdict in schemaVersionConfig.items():
            version = VersionTuple.fromString(vdict["version"])
            digest = vdict.get("digest")
            versions[key] = VersionInfo(version, digest)
        return cls(versions)

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

    @staticmethod
    def schemaDigest(tables: Iterable[sqlalchemy.schema.Table]) -> str:
        """Calculate digest for a schema.

        Parameters
        ----------
        tables : iterable [`sqlalchemy.schema.Table`]
            Set of tables comprising the schema.

        Returns
        -------
        digest : `str`
            String representation of the digest of the schema.

        Notes
        -----
        It is not specified what kind of implementation is used to calculate
        digest string. The only requirement for that is that result should be
        stable over time as this digest string will be stored in the
        configuration and probably in the database too. It should detect (by
        producing different digests) sensible changes to the schema, but it
        also should be stable w.r.t. changes that do not actually change the
        schema (e.g. change in the order of columns or keys.) Current
        implementation is likely incomplete in that it does not detect all
        possible changes (e.g. some constraints may not be included into
        total digest). Digest checking is optional and can be disabled in
        configuration if configured digest is an empty string, we should delay
        activating that check until we have a stable implementation for this
        method.
        """

        def tableSchemaRepr(table: sqlalchemy.schema.Table) -> str:
            """Make string representation of a single table schema.
            """
            tableSchemaRepr = [table.name]
            schemaReps = []
            for column in table.columns:
                columnRep = f"COL,{column.name},{column.type}"
                if column.primary_key:
                    columnRep += ",PK"
                if column.nullable:
                    columnRep += ",NULL"
                schemaReps += [columnRep]
            for fkConstr in table.foreign_key_constraints:
                fkRep = f"FK,{fkConstr.name}"
                for fk in fkConstr.elements:
                    fkRep += f"{fk.column.name}->{fk.target_fullname}"
                schemaReps += [fkRep]
            schemaReps.sort()
            tableSchemaRepr += schemaReps
            return ";".join(tableSchemaRepr)

        md5 = hashlib.md5()
        tableSchemas = sorted(tableSchemaRepr(table) for table in tables)
        for tableRepr in tableSchemas:
            md5.update(tableRepr.encode())
        digest = md5.hexdigest()
        return digest

    def addTable(self, group: str, table: sqlalchemy.schema.Table) -> None:
        """Add a table to specified schema group.

        Table schema added to a group will be used when calculating digest
        for that group.

        Parameters
        ----------
        group : `str`
            Schema group name, e.g. "core", or " dimensions".
        table : `sqlalchemy.schema.Table`
            Table schema.
        """
        self._tablesGroups.setdefault(group, []).append(table)

    def storeVersions(self, attributes: ButlerAttributeManager) -> None:
        """Store configured schema versions in registry arttributes.

        Parameters
        ----------
        attributes : `ButlerAttributeManager`
            Attribute manager instance.
        """
        for key, vInfo in self._versions.items():
            # attribute name reflects configuration path in "registry" config
            attributes.set(f"schema_versions.{key}.version", str(vInfo.version))
            # TODO: we could also store digest in the database but I'm not
            # sure that digest calculation is stable enough at this point.

    def checkVersionDigests(self) -> None:
        """Compare current schema digest to a configured digest.

        It calculates digest to all schema groups using tables added to each
        group with `addTable` method. If digest is different from a configured
        digest for the same group it generates logging warning message.
        """
        for group, tables in self._tablesGroups.items():
            if group in self._versions:
                configDigest = self._versions[group].digest
                if configDigest:
                    digest = self.schemaDigest(tables)
                    if digest != configDigest:
                        _LOG.warning("Digest mismatch for %s schema. Configured digest: '%s', "
                                     "actual digest '%s'.", group, configDigest, digest)

    def checkStoredVersions(self, attributes: ButlerAttributeManager, writeable: bool) -> None:
        """Compare configured versions with the versions stored in database.

        Parameters
        ----------
        attributes : `ButlerAttributeManager`
            Attribute manager instance.
        writeable : `bool`
            If ``True`` then read-write access needs to be checked.

        Raises
        ------
        IncompatibleVersionError
            Raised if versions are not compatible.
        MissingVersionError
            Raised if database has no stored version for one or more groups.
        """
        for key, vInfo in self._versions.items():
            storedVersionStr = attributes.get(f"schema_versions.{key}.version")
            if storedVersionStr is None:
                raise MissingVersionError(f"Failed to read version number for group {key}")
            storedVersion = VersionTuple.fromString(storedVersionStr)
            if not self.checkCompatibility(storedVersion, vInfo.version, writeable):
                raise IncompatibleVersionError(
                    f"Configured version {vInfo.version} is not compatible with stored version "
                    f"{storedVersion} for group {key}"
                )
