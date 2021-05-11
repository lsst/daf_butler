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
from typing import Optional, Any, Dict, TYPE_CHECKING, Type

from ._butlerUri import ButlerURI
from .location import Location, LocationFactory
from .formatter import Formatter, FormatterParameter
from .storageClass import StorageClass, StorageClassFactory

if TYPE_CHECKING:
    from .datasets import DatasetRef

# String to use when a Python None is encountered
NULLSTR = "__NULL_STRING__"


class StoredDatastoreItemInfo:
    """Internal information associated with a stored dataset in a `Datastore`.

    This is an empty base class.  Datastore implementations are expected to
    write their own subclasses.
    """

    __slots__ = ()

    def file_location(self, factory: LocationFactory) -> Location:
        """Return the location of artifact.

        Parameters
        ----------
        factory : `LocationFactory`
            Factory relevant to the datastore represented by this item.

        Returns
        -------
        location : `Location`
            The location of the item within this datastore.
        """
        raise NotImplementedError("The base class does not know how to locate an item in a datastore.")

    @classmethod
    def from_record(cls: Type[StoredDatastoreItemInfo], record: Dict[str, Any]) -> StoredDatastoreItemInfo:
        """Create instance from database record.

        Parameters
        ----------
        record : `dict`
            The record associated with this item.

        Returns
        -------
        info : instance of the relevant type.
            The newly-constructed item corresponding to the record.
        """
        raise NotImplementedError()


@dataclass(frozen=True)
class StoredFileInfo(StoredDatastoreItemInfo):
    """Datastore-private metadata associated with a Datastore file."""

    __slots__ = {"formatter", "path", "storageClass", "component",
                 "checksum", "file_size"}

    storageClassFactory = StorageClassFactory()

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

    def to_record(self, ref: DatasetRef) -> Dict[str, Any]:
        """Convert the supplied ref to a database record."""
        component = ref.datasetType.component()
        if component is None and self.component is not None:
            component = self.component
        if component is None:
            # Use empty string since we want this to be part of the
            # primary key.
            component = NULLSTR

        return dict(dataset_id=ref.id, formatter=self.formatter, path=self.path,
                    storage_class=self.storageClass.name, component=component,
                    checksum=self.checksum, file_size=self.file_size)

    def file_location(self, factory: LocationFactory) -> Location:
        """Return the location of artifact.

        Parameters
        ----------
        factory : `LocationFactory`
            Factory relevant to the datastore represented by this item.

        Returns
        -------
        location : `Location`
            The location of the item within this datastore.
        """
        uriInStore = ButlerURI(self.path, forceAbsolute=False)
        if uriInStore.isabs():
            location = Location(None, uriInStore)
        else:
            location = factory.fromPath(uriInStore)
        return location

    @classmethod
    def from_record(cls: Type[StoredFileInfo], record: Dict[str, Any]) -> StoredFileInfo:
        """Create instance from database record.

        Parameters
        ----------
        record : `dict`
            The record associated with this item.

        Returns
        -------
        info : `StoredFileInfo`
            The newly-constructed item corresponding to the record.
        """
        # Convert name of StorageClass to instance
        storageClass = cls.storageClassFactory.getStorageClass(record["storage_class"])
        component = record["component"] if (record["component"]
                                            and record["component"] != NULLSTR) else None

        info = StoredFileInfo(formatter=record["formatter"],
                              path=record["path"],
                              storageClass=storageClass,
                              component=component,
                              checksum=record["checksum"],
                              file_size=record["file_size"])
        return info
