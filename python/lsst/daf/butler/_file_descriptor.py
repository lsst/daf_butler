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

__all__ = ("FileDescriptor",)

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._location import Location
    from ._storage_class import StorageClass


class FileDescriptor:
    """Describes a particular file.

    Parameters
    ----------
    location : `Location`
        Storage location.
    storageClass : `StorageClass`
        `StorageClass` associated with this file when it was stored.
    readStorageClass : `StorageClass`, optional
        Storage class associated with reading the file. Defines the
        Python type that the in memory Dataset will have. Will default
        to the ``storageClass`` if not specified.
    parameters : `dict`, optional
        Additional parameters that can be used for reading and writing.
    """

    __slots__ = ("location", "storageClass", "_readStorageClass", "parameters")

    def __init__(
        self,
        location: Location,
        storageClass: StorageClass,
        readStorageClass: StorageClass | None = None,
        parameters: Mapping[str, Any] | None = None,
    ):
        self.location = location
        self._readStorageClass = readStorageClass
        self.storageClass = storageClass
        self.parameters = dict(parameters) if parameters is not None else None

    def __repr__(self) -> str:
        optionals: dict[str, Any] = {}
        if self._readStorageClass is not None:
            optionals["readStorageClass"] = self._readStorageClass
        if self.parameters:
            optionals["parameters"] = self.parameters

        # order is preserved in the dict
        options = ", ".join(f"{k}={v!r}" for k, v in optionals.items())

        r = f"{self.__class__.__name__}({self.location!r}, {self.storageClass!r}"
        if options:
            r = r + ", " + options
        r = r + ")"
        return r

    @property
    def readStorageClass(self) -> StorageClass:
        """Storage class to use when reading. (`StorageClass`).

        Will default to ``storageClass`` if none specified.
        """
        if self._readStorageClass is None:
            return self.storageClass
        return self._readStorageClass
