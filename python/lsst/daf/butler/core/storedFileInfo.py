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

__all__ = ("StoredFileInfo", "StoredDatastoreItemInfo")

import inspect
from dataclasses import dataclass
from typing import Optional

from .formatter import Formatter
from .storageClass import StorageClass


class StoredDatastoreItemInfo:
    """Internal information associated with a stored dataset in a `Datastore`.

    This is an empty base class.  Datastore implementations are expected to
    write their own subclasses.
    """
    pass


@dataclass
class StoredFileInfo(StoredDatastoreItemInfo):
    """Datastore-private metadata associated with a file stored in a Datastore.
    """
    formatter: str
    """Fully-qualified name of Formatter."""

    path: str
    """Path to dataset within Datastore."""

    storageClass: StorageClass
    """StorageClass associated with Dataset."""

    checksum: Optional[str]
    """Checksum of the serialized dataset."""

    file_size: int
    """Size of the serialized dataset in bytes."""

    def __post_init__(self):
        # This modification prevents the data class from being frozen.
        if isinstance(self.formatter, str):
            # We trust that this string refers to a Formatter
            pass
        elif isinstance(self.formatter, Formatter) or \
                (inspect.isclass(self.formatter) and issubclass(self.formatter, Formatter)):
            self.formatter = self.formatter.name()
        else:
            raise ValueError(f"Supplied formatter '{self.formatter}' is not a Formatter")
