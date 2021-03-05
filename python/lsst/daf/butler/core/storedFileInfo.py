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

__all__ = ("StoredFileInfo", "StoredDatastoreItemInfo")

import inspect
from dataclasses import dataclass
from typing import Optional

from .formatter import Formatter, FormatterParameter
from .storageClass import StorageClass


class StoredDatastoreItemInfo:
    """Internal information associated with a stored dataset in a `Datastore`.

    This is an empty base class.  Datastore implementations are expected to
    write their own subclasses.
    """

    __slots__ = ()


@dataclass(frozen=True)
class StoredFileInfo(StoredDatastoreItemInfo):
    """Datastore-private metadata associated with a Datastore file."""

    __slots__ = {"formatter", "path", "storageClass", "component",
                 "checksum", "file_size"}

    def __init__(self, formatter: FormatterParameter,
                 path: str,
                 storageClass: StorageClass,
                 component: Optional[str],
                 checksum: Optional[str],
                 file_size: int):

        # Use these shenanigans to allow us to use a frozen dataclass
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "storageClass", storageClass)
        object.__setattr__(self, "component", component)
        object.__setattr__(self, "checksum", checksum)
        object.__setattr__(self, "file_size", file_size)

        if isinstance(formatter, str):
            # We trust that this string refers to a Formatter
            formatterStr = formatter
        elif isinstance(formatter, Formatter) or \
                (inspect.isclass(formatter) and issubclass(formatter, Formatter)):
            formatterStr = formatter.name()
        else:
            raise TypeError(f"Supplied formatter '{formatter}' is not a Formatter")
        object.__setattr__(self, "formatter", formatterStr)

    formatter: str
    """Fully-qualified name of Formatter. If a Formatter class or instance
    is given the name will be extracted."""

    path: str
    """Path to dataset within Datastore."""

    storageClass: StorageClass
    """StorageClass associated with Dataset."""

    component: Optional[str]
    """Component associated with this file. Can be None if the file does
    not refer to a component of a composite."""

    checksum: Optional[str]
    """Checksum of the serialized dataset."""

    file_size: int
    """Size of the serialized dataset in bytes."""
