# This file is part of butler4.
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

__all__ = (
    "DateTimeArrowScalar",
    "DateTimeArrowType",
    "RegionArrowScalar",
    "RegionArrowType",
    "TimespanArrowScalar",
    "TimespanArrowType",
    "ToArrow",
    "UUIDArrowScalar",
    "UUIDArrowType",
)

import uuid
from abc import ABC, abstractmethod
from typing import Any, ClassVar, final

import astropy.time
import pyarrow as pa

from lsst.sphgeom import Region

from ._timespan import Timespan
from .time_utils import TimeConverter


class ToArrow(ABC):
    """An abstract interface for converting objects to an Arrow field of the
    appropriate type.
    """

    @staticmethod
    def for_primitive(name: str, data_type: pa.DataType, nullable: bool) -> ToArrow:
        """Return a converter for a primitive type already supported by Arrow.

        Parameters
        ----------
        name : `str`
            Name of the schema field.
        data_type : `pyarrow.DataType`
            Arrow data type object.
        nullable : `bool`
            Whether the field should permit null or `None` values.

        Returns
        -------
        to_arrow : `ToArrow`
            Converter instance.
        """
        return _ToArrowPrimitive(name, data_type, nullable)

    @staticmethod
    def for_uuid(name: str, nullable: bool = True) -> ToArrow:
        """Return a converter for `uuid.UUID`.

        Parameters
        ----------
        name : `str`
            Name of the schema field.
        nullable : `bool`
            Whether the field should permit null or `None` values.

        Returns
        -------
        to_arrow : `ToArrow`
            Converter instance.
        """
        return _ToArrowUUID(name, nullable)

    @staticmethod
    def for_region(name: str, nullable: bool = True) -> ToArrow:
        """Return a converter for `lsst.sphgeom.Region`.

        Parameters
        ----------
        name : `str`
            Name of the schema field.
        nullable : `bool`
            Whether the field should permit null or `None` values.

        Returns
        -------
        to_arrow : `ToArrow`
            Converter instance.
        """
        return _ToArrowRegion(name, nullable)

    @staticmethod
    def for_timespan(name: str, nullable: bool = True) -> ToArrow:
        """Return a converter for `lsst.daf.butler.Timespan`.

        Parameters
        ----------
        name : `str`
            Name of the schema field.
        nullable : `bool`
            Whether the field should permit null or `None` values.

        Returns
        -------
        to_arrow : `ToArrow`
            Converter instance.
        """
        return _ToArrowTimespan(name, nullable)

    @staticmethod
    def for_datetime(name: str, nullable: bool = True) -> ToArrow:
        """Return a converter for `astropy.time.Time`, stored as TAI
        nanoseconds since 1970-01-01.

        Parameters
        ----------
        name : `str`
            Name of the schema field.
        nullable : `bool`
            Whether the field should permit null or `None` values.

        Returns
        -------
        to_arrow : `ToArrow`
            Converter instance.
        """
        return _ToArrowDateTime(name, nullable)

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the field."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def nullable(self) -> bool:
        """Whether the field permits null or `None` values."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def data_type(self) -> pa.DataType:
        """Arrow data type for the field this converter produces."""
        raise NotImplementedError()

    @property
    def field(self) -> pa.Field:
        """Arrow field this converter produces."""
        return pa.field(self.name, self.data_type, self.nullable)

    def dictionary_encoded(self) -> ToArrow:
        """Return a new converter with the same name and type, but using
        dictionary encoding (to 32-bit integers) to compress duplicate values.
        """
        return _ToArrowDictionary(self)

    @abstractmethod
    def append(self, value: Any, column: list[Any]) -> None:
        """Append an object's arrow representation to a `list`.

        Parameters
        ----------
        value : `object`
            Original value to be converted to a row in an Arrow column.
        column : `list`
            List of values to append to.  The type of value to append is
            implementation-defined; the only requirement is that `finish` be
            able to handle this `list` later.
        """
        raise NotImplementedError()

    @abstractmethod
    def finish(self, column: list[Any]) -> pa.Array:
        """Convert a list of values constructed via `append` into an Arrow
        array.

        Parameters
        ----------
        column : `list`
            List of column values populated by `append`.
        """
        raise NotImplementedError()


class _ToArrowPrimitive(ToArrow):
    """`ToArrow` implementation for primitive types supported direct by Arrow.

    Should be constructed via the `ToArrow.for_primitive` factory method.
    """

    def __init__(self, name: str, data_type: pa.DataType, nullable: bool):
        self._name = name
        self._data_type = data_type
        self._nullable = nullable

    @property
    def name(self) -> str:
        # Docstring inherited.
        return self._name

    @property
    def nullable(self) -> bool:
        # Docstring inherited.
        return self._nullable

    @property
    def data_type(self) -> pa.DataType:
        # Docstring inherited.
        return self._data_type

    def append(self, value: Any, column: list[Any]) -> None:
        # Docstring inherited.
        column.append(value)

    def finish(self, column: list[Any]) -> pa.Array:
        # Docstring inherited.
        return pa.array(column, self._data_type)


class _ToArrowDictionary(ToArrow):
    """`ToArrow` implementation for Arrow dictionary fields.

    Should be constructed via the `ToArrow.dictionary_encoded` factory method.
    """

    def __init__(self, to_arrow_value: ToArrow):
        self._to_arrow_value = to_arrow_value

    @property
    def name(self) -> str:
        # Docstring inherited.
        return self._to_arrow_value.name

    @property
    def nullable(self) -> bool:
        # Docstring inherited.
        return self._to_arrow_value.nullable

    @property
    def data_type(self) -> pa.DataType:
        # Docstring inherited.
        # We hard-code int32 as the index type here because that's what
        # the pa.Arrow.dictionary_encode() method does.
        return pa.dictionary(pa.int32(), self._to_arrow_value.data_type)

    def append(self, value: Any, column: list[Any]) -> None:
        # Docstring inherited.
        self._to_arrow_value.append(value, column)

    def finish(self, column: list[Any]) -> pa.Array:
        # Docstring inherited.
        return self._to_arrow_value.finish(column).dictionary_encode()


class _ToArrowUUID(ToArrow):
    """`ToArrow` implementation for `uuid.UUID` fields.

    Should be constructed via the `ToArrow.for_uuid` factory method.
    """

    def __init__(self, name: str, nullable: bool):
        self._name = name
        self._nullable = nullable

    storage_type: ClassVar[pa.DataType] = pa.binary(16)

    @property
    def name(self) -> str:
        # Docstring inherited.
        return self._name

    @property
    def nullable(self) -> bool:
        # Docstring inherited.
        return self._nullable

    @property
    def data_type(self) -> pa.DataType:
        # Docstring inherited.
        return UUIDArrowType()

    def append(self, value: uuid.UUID | None, column: list[bytes | None]) -> None:
        # Docstring inherited.
        column.append(value.bytes if value is not None else None)

    def finish(self, column: list[Any]) -> pa.Array:
        # Docstring inherited.
        storage_array = pa.array(column, self.storage_type)
        return pa.ExtensionArray.from_storage(UUIDArrowType(), storage_array)


class _ToArrowRegion(ToArrow):
    """`ToArrow` implementation for `lsst.sphgeom.Region` fields.

    Should be constructed via the `ToArrow.for_region` factory method.
    """

    def __init__(self, name: str, nullable: bool):
        self._name = name
        self._nullable = nullable

    storage_type: ClassVar[pa.DataType] = pa.binary()

    @property
    def name(self) -> str:
        # Docstring inherited.
        return self._name

    @property
    def nullable(self) -> bool:
        # Docstring inherited.
        return self._nullable

    @property
    def data_type(self) -> pa.DataType:
        # Docstring inherited.
        return RegionArrowType()

    def append(self, value: Region | None, column: list[bytes | None]) -> None:
        # Docstring inherited.
        column.append(value.encode() if value is not None else None)

    def finish(self, column: list[Any]) -> pa.Array:
        # Docstring inherited.
        storage_array = pa.array(column, self.storage_type)
        return pa.ExtensionArray.from_storage(RegionArrowType(), storage_array)


class _ToArrowTimespan(ToArrow):
    """`ToArrow` implementation for `lsst.daf.butler.timespan` fields.

    Should be constructed via the `ToArrow.for_timespan` factory method.
    """

    def __init__(self, name: str, nullable: bool):
        self._name = name
        self._nullable = nullable

    storage_type: ClassVar[pa.DataType] = pa.struct(
        [
            pa.field("begin_nsec", pa.int64(), nullable=False),
            pa.field("end_nsec", pa.int64(), nullable=False),
        ]
    )

    @property
    def name(self) -> str:
        # Docstring inherited.
        return self._name

    @property
    def nullable(self) -> bool:
        # Docstring inherited.
        return self._nullable

    @property
    def data_type(self) -> pa.DataType:
        # Docstring inherited.
        return TimespanArrowType()

    def append(self, value: Timespan | None, column: list[pa.StructScalar | None]) -> None:
        # Docstring inherited.
        column.append({"begin_nsec": value.nsec[0], "end_nsec": value.nsec[1]} if value is not None else None)

    def finish(self, column: list[Any]) -> pa.Array:
        # Docstring inherited.
        storage_array = pa.array(column, self.storage_type)
        return pa.ExtensionArray.from_storage(TimespanArrowType(), storage_array)


class _ToArrowDateTime(ToArrow):
    """`ToArrow` implementation for `astropy.time.Time` fields.

    Should be constructed via the `ToArrow.for_datetime` factory method.
    """

    def __init__(self, name: str, nullable: bool):
        self._name = name
        self._nullable = nullable

    storage_type: ClassVar[pa.DataType] = pa.int64()

    @property
    def name(self) -> str:
        # Docstring inherited.
        return self._name

    @property
    def nullable(self) -> bool:
        # Docstring inherited.
        return self._nullable

    @property
    def data_type(self) -> pa.DataType:
        # Docstring inherited.
        return DateTimeArrowType()

    def append(self, value: astropy.time.Time | None, column: list[int | None]) -> None:
        # Docstring inherited.
        column.append(TimeConverter().astropy_to_nsec(value) if value is not None else None)

    def finish(self, column: list[Any]) -> pa.Array:
        # Docstring inherited.
        storage_array = pa.array(column, self.storage_type)
        return pa.ExtensionArray.from_storage(DateTimeArrowType(), storage_array)


@final
class UUIDArrowType(pa.ExtensionType):
    """An Arrow extension type for `astropy.time.Time`, stored as TAI
    nanoseconds since 1970-01-01.
    """

    def __init__(self) -> None:
        super().__init__(_ToArrowTimespan.storage_type, "astropy.time.Time")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> UUIDArrowType:
        return cls()

    def __arrow_ext_scalar_class__(self) -> type[UUIDArrowScalar]:
        return UUIDArrowScalar


@final
class UUIDArrowScalar(pa.ExtensionScalar):
    """An Arrow scalar type for `uuid.UUID`.

    Use the standard `as_py` method to convert to an actual `uuid.UUID`
    instance.
    """

    def as_py(self) -> astropy.time.Time:
        return uuid.UUID(bytes=self.value.as_py())


@final
class RegionArrowType(pa.ExtensionType):
    """An Arrow extension type for lsst.sphgeom.Region."""

    def __init__(self) -> None:
        super().__init__(_ToArrowRegion.storage_type, "lsst.sphgeom.Region")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> RegionArrowType:
        return cls()

    def __arrow_ext_scalar_class__(self) -> type[RegionArrowScalar]:
        return RegionArrowScalar


@final
class RegionArrowScalar(pa.ExtensionScalar):
    """An Arrow scalar type for `lsst.sphgeom.Region`.

    Use the standard `as_py` method to convert to an actual region.
    """

    def as_py(self) -> Region:
        return Region.decode(self.value.as_py())


@final
class TimespanArrowType(pa.ExtensionType):
    """An Arrow extension type for lsst.daf.butler.Timespan."""

    def __init__(self) -> None:
        super().__init__(_ToArrowTimespan.storage_type, "lsst.daf.butler.Timespan")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> TimespanArrowType:
        return cls()

    def __arrow_ext_scalar_class__(self) -> type[TimespanArrowScalar]:
        return TimespanArrowScalar


@final
class TimespanArrowScalar(pa.ExtensionScalar):
    """An Arrow scalar type for `lsst.daf.butler.Timespan`.

    Use the standard `as_py` method to convert to an actual timespan.
    """

    def as_py(self) -> Timespan | None:
        if self.value is None:
            return None
        else:
            return Timespan(
                None, None, _nsec=(self.value["begin_nsec"].as_py(), self.value["end_nsec"].as_py())
            )


@final
class DateTimeArrowType(pa.ExtensionType):
    """An Arrow extension type for `astropy.time.Time`, stored as TAI
    nanoseconds since 1970-01-01.
    """

    def __init__(self) -> None:
        super().__init__(_ToArrowTimespan.storage_type, "astropy.time.Time")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> DateTimeArrowType:
        return cls()

    def __arrow_ext_scalar_class__(self) -> type[DateTimeArrowScalar]:
        return DateTimeArrowScalar


@final
class DateTimeArrowScalar(pa.ExtensionScalar):
    """An Arrow scalar type for `astropy.time.Time`, stored as TAI
    nanoseconds since 1970-01-01.

    Use the standard `as_py` method to convert to an actual `astropy.time.Time`
    instance.
    """

    def as_py(self) -> astropy.time.Time:
        return TimeConverter().nsec_to_astropy(self.value.as_py())


pa.register_extension_type(RegionArrowType())
pa.register_extension_type(TimespanArrowType())
pa.register_extension_type(DateTimeArrowType())
