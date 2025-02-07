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

__all__ = (
    "ColumnLiteral",
    "LiteralValue",
    "make_column_literal",
)

import datetime
import numbers
import uuid
import warnings
from base64 import b64decode, b64encode
from functools import cached_property
from typing import Literal, TypeAlias, Union, final

import astropy.coordinates
import astropy.time
import erfa

import lsst.sphgeom

from ..._timespan import Timespan
from ...time_utils import TimeConverter
from ._base import ColumnLiteralBase

LiteralValue: TypeAlias = (
    int
    | str
    | float
    | bytes
    | uuid.UUID
    | astropy.time.Time
    | datetime.datetime
    | Timespan
    | lsst.sphgeom.Region
    | lsst.sphgeom.LonLat
)


@final
class IntColumnLiteral(ColumnLiteralBase):
    """A literal `int` value in a column expression."""

    expression_type: Literal["int"] = "int"

    value: int
    """The wrapped value."""

    @classmethod
    def from_value(cls, value: int) -> IntColumnLiteral:
        """Construct from the wrapped value.

        Parameters
        ----------
        value : `int`
            Value to wrap.

        Returns
        -------
        expression : `IntColumnLiteral`
            Literal expression object.
        """
        return cls.model_construct(value=value)

    def __str__(self) -> str:
        return repr(self.value)


@final
class StringColumnLiteral(ColumnLiteralBase):
    """A literal `str` value in a column expression."""

    expression_type: Literal["string"] = "string"

    value: str
    """The wrapped value."""

    @classmethod
    def from_value(cls, value: str) -> StringColumnLiteral:
        """Construct from the wrapped value.

        Parameters
        ----------
        value : `str`
            Value to wrap.

        Returns
        -------
        expression : `StrColumnLiteral`
            Literal expression object.
        """
        return cls.model_construct(value=value)

    def __str__(self) -> str:
        return repr(self.value)


@final
class FloatColumnLiteral(ColumnLiteralBase):
    """A literal `float` value in a column expression."""

    expression_type: Literal["float"] = "float"

    value: float
    """The wrapped value."""

    @classmethod
    def from_value(cls, value: float) -> FloatColumnLiteral:
        """Construct from the wrapped value.

        Parameters
        ----------
        value : `float`
            Value to wrap.

        Returns
        -------
        expression : `FloatColumnLiteral`
            Literal expression object.
        """
        return cls.model_construct(value=value)

    def __str__(self) -> str:
        return repr(self.value)


@final
class HashColumnLiteral(ColumnLiteralBase):
    """A literal `bytes` value representing a hash in a column expression.

    The original value is base64-encoded when serialized and decoded on first
    use.
    """

    expression_type: Literal["hash"] = "hash"

    encoded: bytes
    """The wrapped value after base64 encoding."""

    @cached_property
    def value(self) -> bytes:
        """The wrapped value."""
        return b64decode(self.encoded)

    @classmethod
    def from_value(cls, value: bytes) -> HashColumnLiteral:
        """Construct from the wrapped value.

        Parameters
        ----------
        value : `bytes`
            Value to wrap.

        Returns
        -------
        expression : `HashColumnLiteral`
            Literal expression object.
        """
        return cls.model_construct(encoded=b64encode(value))

    def __str__(self) -> str:
        return "(bytes)"


@final
class UUIDColumnLiteral(ColumnLiteralBase):
    """A literal `uuid.UUID` value in a column expression."""

    expression_type: Literal["uuid"] = "uuid"

    value: uuid.UUID
    """The wrapped value."""

    @classmethod
    def from_value(cls, value: uuid.UUID) -> UUIDColumnLiteral:
        """Construct from the wrapped value.

        Parameters
        ----------
        value : `uuid.UUID`
            Value to wrap.

        Returns
        -------
        expression : `UUIDColumnLiteral`
            Literal expression object.
        """
        return cls.model_construct(value=value)

    def __str__(self) -> str:
        return str(self.value)


@final
class DateTimeColumnLiteral(ColumnLiteralBase):
    """A literal `astropy.time.Time` value in a column expression.

    The time is converted into TAI nanoseconds since 1970-01-01 when serialized
    and restored from that on first use.
    """

    expression_type: Literal["datetime"] = "datetime"

    nsec: int
    """TAI nanoseconds since 1970-01-01."""

    @cached_property
    def value(self) -> astropy.time.Time:
        """The wrapped value."""
        return TimeConverter().nsec_to_astropy(self.nsec)

    @classmethod
    def from_value(cls, value: astropy.time.Time) -> DateTimeColumnLiteral:
        """Construct from the wrapped value.

        Parameters
        ----------
        value : `astropy.time.Time`
            Value to wrap.

        Returns
        -------
        expression : `DateTimeColumnLiteral`
            Literal expression object.
        """
        return cls.model_construct(nsec=TimeConverter().astropy_to_nsec(value))

    def __str__(self) -> str:
        # Trap dubious year warnings in case we have timespans from
        # simulated data in the future
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            return self.value.tai.strftime("%Y-%m-%dT%H:%M:%S")


@final
class TimespanColumnLiteral(ColumnLiteralBase):
    """A literal `Timespan` value in a column expression.

    The timespan bounds are converted into TAI nanoseconds since 1970-01-01
    when serialized and the timespan is restored from that on first use.
    """

    expression_type: Literal["timespan"] = "timespan"

    begin_nsec: int
    """TAI nanoseconds since 1970-01-01 for the lower bound of the timespan
    (inclusive).
    """

    end_nsec: int
    """TAI nanoseconds since 1970-01-01 for the upper bound of the timespan
    (exclusive).
    """

    @cached_property
    def value(self) -> Timespan:
        """The wrapped value."""
        return Timespan(None, None, _nsec=(self.begin_nsec, self.end_nsec))

    @classmethod
    def from_value(cls, value: Timespan) -> TimespanColumnLiteral:
        """Construct from the wrapped value.

        Parameters
        ----------
        value : `..Timespan`
            Value to wrap.

        Returns
        -------
        expression : `TimespanColumnLiteral`
            Literal expression object.
        """
        return cls.model_construct(begin_nsec=value.nsec[0], end_nsec=value.nsec[1])

    def __str__(self) -> str:
        return str(self.value)


@final
class RegionColumnLiteral(ColumnLiteralBase):
    """A literal `lsst.sphgeom.Region` value in a column expression.

    The region is encoded to base64 `bytes` when serialized, and decoded on
    first use.
    """

    expression_type: Literal["region"] = "region"

    encoded: bytes
    """The wrapped value after base64 encoding."""

    @cached_property
    def value(self) -> bytes:
        """The wrapped value."""
        return lsst.sphgeom.Region.decode(b64decode(self.encoded))

    @classmethod
    def from_value(cls, value: lsst.sphgeom.Region) -> RegionColumnLiteral:
        """Construct from the wrapped value.

        Parameters
        ----------
        value : `..Region`
            Value to wrap.

        Returns
        -------
        expression : `RegionColumnLiteral`
            Literal expression object.
        """
        return cls.model_construct(encoded=b64encode(value.encode()))

    def __str__(self) -> str:
        return "(region)"


ColumnLiteral: TypeAlias = Union[
    IntColumnLiteral,
    StringColumnLiteral,
    FloatColumnLiteral,
    HashColumnLiteral,
    UUIDColumnLiteral,
    DateTimeColumnLiteral,
    TimespanColumnLiteral,
    RegionColumnLiteral,
]


def make_column_literal(value: LiteralValue) -> ColumnLiteral:
    """Construct a `ColumnLiteral` from the value it will wrap.

    Parameters
    ----------
    value : `LiteralValue`
        Value to wrap.

    Returns
    -------
    expression : `ColumnLiteral`
        Literal expression object.
    """
    match value:
        case int():
            return IntColumnLiteral.from_value(value)
        case str():
            return StringColumnLiteral.from_value(value)
        case float():
            return FloatColumnLiteral.from_value(value)
        case uuid.UUID():
            return UUIDColumnLiteral.from_value(value)
        case bytes():
            return HashColumnLiteral.from_value(value)
        case astropy.time.Time():
            return DateTimeColumnLiteral.from_value(value)
        case datetime.date():
            return DateTimeColumnLiteral.from_value(astropy.time.Time(value))
        case Timespan():
            return TimespanColumnLiteral.from_value(value)
        case lsst.sphgeom.Region():
            return RegionColumnLiteral.from_value(value)
        case lsst.sphgeom.LonLat():
            return _make_region_literal_from_lonlat(value)
        case astropy.coordinates.SkyCoord():
            icrs = value.transform_to("icrs")
            if not icrs.isscalar:
                raise ValueError(
                    "Astropy SkyCoord contained an array of points,"
                    f" but it should be only a single point: {value}"
                )

            ra = icrs.ra.degree
            dec = icrs.dec.degree
            lon_lat = lsst.sphgeom.LonLat.fromDegrees(ra, dec)
            return _make_region_literal_from_lonlat(lon_lat)
        case numbers.Integral():
            # numpy.int64 and other integer-like values.
            return IntColumnLiteral.from_value(int(value))

    raise TypeError(f"Invalid type {type(value).__name__!r} of value {value!r} for column literal.")


def _make_region_literal_from_lonlat(point: lsst.sphgeom.LonLat) -> RegionColumnLiteral:
    vec = lsst.sphgeom.UnitVector3d(point)
    # Convert the point to a Region by representing it as a zero-radius
    # Circle.
    region = lsst.sphgeom.Circle(vec)
    return RegionColumnLiteral.from_value(region)
