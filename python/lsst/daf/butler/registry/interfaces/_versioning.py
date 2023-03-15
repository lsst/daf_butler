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

from abc import ABC, abstractmethod
from typing import NamedTuple, Optional


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
