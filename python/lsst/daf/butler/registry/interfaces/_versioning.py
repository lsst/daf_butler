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
    "VersionTuple",
    "VersionedExtension",
]

import hashlib
from abc import ABC, abstractmethod
from typing import Iterable, NamedTuple, Optional

import sqlalchemy


class VersionTuple(NamedTuple):
    """Class representing a version number.

    Parameters
    ----------
    major, minor, patch : `int`
        Version number components
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

    def __str__(self) -> str:
        """Transform version tuple into a canonical string form."""
        return f"{self.major}.{self.minor}.{self.patch}"


class VersionedExtension(ABC):
    """Interface for extension classes with versions."""

    @classmethod
    @abstractmethod
    def currentVersion(cls) -> Optional[VersionTuple]:
        """Return extension version as defined by current implementation.

        This method can return ``None`` if an extension does not require
        its version to be saved or checked.

        Returns
        -------
        version : `VersionTuple`
            Current extension version or ``None``.
        """
        raise NotImplementedError()

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

    @abstractmethod
    def schemaDigest(self) -> Optional[str]:
        """Return digest for schema piece managed by this extension.

        Returns
        -------
        digest : `str` or `None`
            String representation of the digest of the schema, ``None`` should
            be returned if schema digest is not to be saved or checked. The
            length of the returned string cannot exceed the length of the
            "value" column of butler attributes table, currently 65535
            characters.

        Notes
        -----
        There is no exact definition of digest format, any string should work.
        The only requirement for string contents is that it has to remain
        stable over time if schema does not change but it should produce
        different string for any change in the schema. In many cases default
        implementation in `_defaultSchemaDigest` can be used as a reasonable
        choice.
        """
        raise NotImplementedError()

    def _defaultSchemaDigest(
        self, tables: Iterable[sqlalchemy.schema.Table], dialect: sqlalchemy.engine.Dialect
    ) -> str:
        """Calculate digest for a schema based on list of tables schemas.

        Parameters
        ----------
        tables : iterable [`sqlalchemy.schema.Table`]
            Set of tables comprising the schema.
        dialect : `sqlalchemy.engine.Dialect`, optional
            Dialect used to stringify types; needed to support dialect-specific
            types.

        Returns
        -------
        digest : `str`
            String representation of the digest of the schema.

        Notes
        -----
        It is not specified what kind of implementation is used to calculate
        digest string. The only requirement for that is that result should be
        stable over time as this digest string will be stored in the database.
        It should detect (by producing different digests) sensible changes to
        the schema, but it also should be stable w.r.t. changes that do
        not actually change the schema (e.g. change in the order of columns or
        keys.) Current implementation is likely incomplete in that it does not
        detect all possible changes (e.g. some constraints may not be included
        into total digest).
        """

        def tableSchemaRepr(table: sqlalchemy.schema.Table) -> str:
            """Make string representation of a single table schema."""
            tableSchemaRepr = [table.name]
            schemaReps = []
            for column in table.columns:
                columnRep = f"COL,{column.name},{column.type.compile(dialect=dialect)}"
                if column.primary_key:
                    columnRep += ",PK"
                if column.nullable:
                    columnRep += ",NULL"
                schemaReps += [columnRep]
            for fkConstr in table.foreign_key_constraints:
                # for foreign key we include only one side of relations into
                # digest, other side could be managed by different extension
                fkReps = ["FK", fkConstr.name] + [fk.column.name for fk in fkConstr.elements]
                fkRep = ",".join(fkReps)
                schemaReps += [fkRep]
            # sort everything to keep it stable
            schemaReps.sort()
            tableSchemaRepr += schemaReps
            return ";".join(tableSchemaRepr)

        md5 = hashlib.md5()
        tableSchemas = sorted(tableSchemaRepr(table) for table in tables)
        for tableRepr in tableSchemas:
            md5.update(tableRepr.encode())
        digest = md5.hexdigest()
        return digest
