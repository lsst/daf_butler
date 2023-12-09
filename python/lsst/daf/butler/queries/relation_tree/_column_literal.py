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
    "make_column_literal",
)

import warnings
from base64 import b64decode, b64encode
from functools import cached_property
from typing import Literal, TypeAlias, Union

import astropy.time
import erfa
from lsst.sphgeom import Region

from ..._timespan import Timespan
from ...time_utils import TimeConverter
from ._base import ColumnExpressionBase

LiteralValue: TypeAlias = Union[int, str, float, bytes, astropy.time.Time, Timespan, Region]


class IntColumnLiteral(ColumnExpressionBase):
    """A literal `int` value in a column expression."""

    expression_type: Literal["int"] = "int"
    value: int

    @classmethod
    def from_value(cls, value: int) -> IntColumnLiteral:
        return cls.model_construct(value=value)

    @property
    def precedence(self) -> int:
        return 0

    def __str__(self) -> str:
        return repr(self.value)


class StringColumnLiteral(ColumnExpressionBase):
    """A literal `str` value in a column expression."""

    expression_type: Literal["str"] = "str"
    value: str

    @classmethod
    def from_value(cls, value: str) -> StringColumnLiteral:
        return cls.model_construct(value=value)

    @property
    def precedence(self) -> int:
        return 0

    def __str__(self) -> str:
        return repr(self.value)


class FloatColumnLiteral(ColumnExpressionBase):
    """A literal `float` value in a column expression."""

    expression_type: Literal["float"] = "float"
    value: float

    @classmethod
    def from_value(cls, value: float) -> FloatColumnLiteral:
        return cls.model_construct(value=value)

    @property
    def precedence(self) -> int:
        return 0

    def __str__(self) -> str:
        return repr(self.value)


class BytesColumnLiteral(ColumnExpressionBase):
    """A literal `bytes` value in a column expression.

    The original value is base64-encoded when serialized and decoded on first
    use.
    """

    expression_type: Literal["bytes"] = "bytes"
    encoded: bytes

    @cached_property
    def value(self) -> bytes:
        return b64decode(self.encoded)

    @classmethod
    def from_value(cls, value: bytes) -> BytesColumnLiteral:
        return cls.model_construct(encoded=b64encode(value))

    @property
    def precedence(self) -> int:
        return 0

    def __str__(self) -> str:
        return "(bytes)"


class TimeColumnLiteral(ColumnExpressionBase):
    """A literal `astropy.time.Time` value in a column expression.

    The time is converted into TAI nanoseconds since 1970-01-01 when serialized
    and restored from that on first use.
    """

    expression_type: Literal["time"] = "time"
    nsec: int

    @cached_property
    def value(self) -> astropy.time.Time:
        return TimeConverter().nsec_to_astropy(self.nsec)

    @classmethod
    def from_value(cls, value: astropy.time.Time) -> TimeColumnLiteral:
        return cls.model_construct(nsec=TimeConverter().astropy_to_nsec(value))

    @property
    def precedence(self) -> int:
        return 0

    def __str__(self) -> str:
        # Trap dubious year warnings in case we have timespans from
        # simulated data in the future
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            return self.value.tai.strftime("%Y-%m-%dT%H:%M:%S")


class TimespanColumnLiteral(ColumnExpressionBase):
    """A literal `Timespan` value in a column expression.

    The timespan bounds are converted into TAI nanoseconds since 1970-01-01
    when serialized and the timespan is restored from that on first use.
    """

    expression_type: Literal["timespan"] = "timespan"
    begin_nsec: int
    end_nsec: int

    @cached_property
    def value(self) -> astropy.time.Time:
        return Timespan(None, None, _nsec=(self.begin_nsec, self.end_nsec))

    @classmethod
    def from_value(cls, value: Timespan) -> TimespanColumnLiteral:
        return cls.model_construct(begin_nsec=value._nsec[0], end_nsec=value._nsec[1])

    @property
    def precedence(self) -> int:
        return 0

    def __str__(self) -> str:
        return str(self.value)


class RegionColumnLiteral(ColumnExpressionBase):
    """A literal `lsst.sphgeom.Region` value in a column expression.

    The region is encoded to base64 `bytes` when serialized, and decoded on
    first use.
    """

    expression_type: Literal["region"] = "region"

    encoded: bytes

    @cached_property
    def value(self) -> bytes:
        return Region.decode(b64decode(self.encoded))

    @classmethod
    def from_value(cls, value: Region) -> RegionColumnLiteral:
        return cls.model_construct(encoded=b64encode(value.encode()))

    @property
    def precedence(self) -> int:
        return 0

    def __str__(self) -> str:
        return "(bytes)"


ColumnLiteral: TypeAlias = Union[
    IntColumnLiteral,
    StringColumnLiteral,
    FloatColumnLiteral,
    BytesColumnLiteral,
    TimeColumnLiteral,
    TimespanColumnLiteral,
    RegionColumnLiteral,
]


def make_column_literal(value: LiteralValue) -> ColumnLiteral:
    """Construct a `ColumnLiteral` from its value."""
    match value:
        case int():
            return IntColumnLiteral.from_value(value)
        case str():
            return StringColumnLiteral.from_value(value)
        case float():
            return FloatColumnLiteral.from_value(value)
        case bytes():
            return BytesColumnLiteral.from_value(value)
        case astropy.time.Time():
            return TimeColumnLiteral.from_value(value)
        case Timespan():
            return TimespanColumnLiteral.from_value(value)
        case Region():
            return RegionColumnLiteral.from_value(value)
    raise TypeError(f"Invalid type {type(value).__name__} of value {value!r} for column literal.")
