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

__all__ = ("StoredDatastoreItemInfo", "StoredFileInfo", "SerializedStoredFileInfo")

import inspect
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pydantic
from lsst.resources import ResourcePath
from lsst.utils import doImportType
from lsst.utils.introspection import get_full_type_name

from .._formatter import Formatter, FormatterParameter, FormatterV2
from .._location import Location, LocationFactory
from .._storage_class import StorageClass, StorageClassFactory

if TYPE_CHECKING:
    from .._dataset_ref import DatasetRef

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
    def from_record(cls: type[StoredDatastoreItemInfo], record: Mapping[str, Any]) -> StoredDatastoreItemInfo:
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

    def to_record(self, **kwargs: Any) -> dict[str, Any]:
        """Convert record contents to a dictionary.

        Parameters
        ----------
        **kwargs
            Additional items to add to returned record.
        """
        raise NotImplementedError()

    def update(self, **kwargs: Any) -> StoredDatastoreItemInfo:
        """Create a new class with everything retained apart from the
        specified values.

        Parameters
        ----------
        **kwargs : `~collections.abc.Mapping`
            Values to override.

        Returns
        -------
        updated : `StoredDatastoreItemInfo`
            A new instance of the object with updated values.
        """
        raise NotImplementedError()

    @classmethod
    def to_records(
        cls, records: Iterable[StoredDatastoreItemInfo], **kwargs: Any
    ) -> tuple[str, Iterable[Mapping[str, Any]]]:
        """Convert a collection of records to dictionaries.

        Parameters
        ----------
        records : `~collections.abc.Iterable` [ `StoredDatastoreItemInfo` ]
            A collection of records, all records must be of the same type.
        **kwargs
            Additional items to add to each returned record.

        Returns
        -------
        class_name : `str`
            Name of the record class.
        records : `list` [ `dict` ]
            Records in their dictionary representation.
        """
        if not records:
            return "", []
        classes = {record.__class__ for record in records}
        assert len(classes) == 1, f"Records have to be of the same class: {classes}"
        return get_full_type_name(classes.pop()), [record.to_record(**kwargs) for record in records]

    @classmethod
    def from_records(
        cls, class_name: str, records: Iterable[Mapping[str, Any]]
    ) -> list[StoredDatastoreItemInfo]:
        """Convert collection of dictionaries to records.

        Parameters
        ----------
        class_name : `str`
            Name of the record class.
        records : `~collections.abc.Iterable` [ `dict` ]
            Records in their dictionary representation.

        Returns
        -------
        infos : `list` [`StoredDatastoreItemInfo`]
            Sequence of records converted to typed representation.

        Raises
        ------
        TypeError
            Raised if ``class_name`` is not a sub-class of
            `StoredDatastoreItemInfo`.
        """
        try:
            klass = doImportType(class_name)
        except ImportError:
            # Prior to DM-41043 we were embedding a lsst.daf.butler.core
            # path in the serialized form, which we never wanted; fix this
            # one case.
            if class_name == "lsst.daf.butler.core.storedFileInfo.StoredFileInfo":
                klass = StoredFileInfo
            else:
                raise
        if not issubclass(klass, StoredDatastoreItemInfo):
            raise TypeError(f"Class {class_name} is not a subclass of StoredDatastoreItemInfo")
        return [klass.from_record(record) for record in records]


@dataclass(frozen=True, slots=True)
class StoredFileInfo(StoredDatastoreItemInfo):
    """Datastore-private metadata associated with a Datastore file.

    Parameters
    ----------
    formatter : `Formatter` or `FormatterV2` or `str`
        The formatter to use for this dataset.
    path : `str`
        Path to the artifact associated with this dataset.
    storageClass : `StorageClass`
        The storage class associated with this dataset.
    component : `str` or `None`, optional
        The component if disassembled.
    checksum : `str` or `None`, optional
        The checksum of the artifact.
    file_size : `int`
        The size of the file in bytes. -1 indicates the size is not known.
    """

    storageClassFactory = StorageClassFactory()

    def __init__(
        self,
        formatter: FormatterParameter,
        path: str,
        storageClass: StorageClass,
        component: str | None,
        checksum: str | None,
        file_size: int,
    ):
        # Use these shenanigans to allow us to use a frozen dataclass
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "storageClass", storageClass)
        object.__setattr__(self, "component", component)
        object.__setattr__(self, "checksum", checksum)
        object.__setattr__(self, "file_size", file_size)

        if isinstance(formatter, str):
            # We trust that this string refers to a Formatter
            formatterStr = formatter
        elif isinstance(formatter, Formatter | FormatterV2) or (
            inspect.isclass(formatter) and issubclass(formatter, Formatter | FormatterV2)
        ):
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

    component: str | None
    """Component associated with this file. Can be `None` if the file does
    not refer to a component of a composite."""

    checksum: str | None
    """Checksum of the serialized dataset."""

    file_size: int
    """Size of the serialized dataset in bytes."""

    def rebase(self, ref: DatasetRef) -> StoredFileInfo:
        """Return a copy of the record suitable for a specified reference.

        Parameters
        ----------
        ref : `DatasetRef`
            DatasetRef which provides component name and dataset ID for the
            new returned record.

        Returns
        -------
        record : `StoredFileInfo`
            New record instance.
        """
        # take component from the ref, rest comes from self
        component = ref.datasetType.component()
        if component is None:
            component = self.component
        return self.update(component=component)

    def to_record(self, **kwargs: Any) -> dict[str, Any]:
        """Convert the supplied ref to a database record.

        Parameters
        ----------
        **kwargs : `typing.Any`
            Additional information to be added to the record.
        """
        component = self.component
        if component is None:
            # Use empty string since we want this to be part of the
            # primary key.
            component = NULLSTR
        return dict(
            formatter=self.formatter,
            path=self.path,
            storage_class=self.storageClass.name,
            component=component,
            checksum=self.checksum,
            file_size=self.file_size,
            **kwargs,
        )

    def to_simple(self) -> SerializedStoredFileInfo:
        record = self.to_record()
        # We allow None on the model but the record contains a "null string"
        # instead
        record["component"] = self.component
        return SerializedStoredFileInfo.model_validate(record)

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
        uriInStore = ResourcePath(self.path, forceAbsolute=False, forceDirectory=False)
        if uriInStore.isabs():
            location = Location(None, uriInStore)
        else:
            location = factory.from_uri(uriInStore, trusted_path=True)
        return location

    @classmethod
    def from_record(cls: type[StoredFileInfo], record: Mapping[str, Any]) -> StoredFileInfo:
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
        component = record["component"] if (record["component"] and record["component"] != NULLSTR) else None
        info = cls(
            formatter=record["formatter"],
            path=record["path"],
            storageClass=storageClass,
            component=component,
            checksum=record["checksum"],
            file_size=record["file_size"],
        )
        return info

    @classmethod
    def from_simple(cls: type[StoredFileInfo], model: SerializedStoredFileInfo) -> StoredFileInfo:
        return cls.from_record(dict(model))

    def update(self, **kwargs: Any) -> StoredFileInfo:
        new_args = {}
        for k in self.__slots__:
            if k in kwargs:
                new_args[k] = kwargs.pop(k)
            else:
                new_args[k] = getattr(self, k)
        if kwargs:
            raise ValueError(f"Unexpected keyword arguments for update: {', '.join(kwargs)}")
        return type(self)(**new_args)

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (self.from_record, (self.to_record(),))


class SerializedStoredFileInfo(pydantic.BaseModel):
    """Serialized representation of `StoredFileInfo` properties."""

    formatter: str
    """Fully-qualified name of Formatter."""

    path: str
    """Path to dataset within Datastore."""

    storage_class: str
    """Name of the StorageClass associated with Dataset."""

    component: str | None
    """Component associated with this file. Can be `None` if the file does
    not refer to a component of a composite."""

    checksum: str | None
    """Checksum of the serialized dataset."""

    file_size: int
    """Size of the serialized dataset in bytes."""
