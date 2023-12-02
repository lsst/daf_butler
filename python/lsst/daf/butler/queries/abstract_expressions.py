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
    "DiscriminatedExpression",
    "DiscriminatedOrderExpression",
    "DiscriminatedPredicate",
)

from base64 import b64decode, b64encode
from functools import cached_property
from typing import TYPE_CHECKING, Annotated, Generic, Literal, TypeAlias, Union, cast

import astropy.time
import pydantic
from lsst.sphgeom import Region

from .._timespan import Timespan
from ..dimensions import DataIdValue, DimensionGroup, DimensionUniverse
from ..time_utils import TimeConverter

if TYPE_CHECKING:
    from .abstract_relations import _D, DiscriminatedRelation


LiteralValue: TypeAlias = Union[int, str, float, bytes, astropy.time.Time, Timespan, Region]


class IntColumnLiteral(pydantic.BaseModel):
    """A literal `int` value in a column expression."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["int"] = "int"
    value: int

    @classmethod
    def from_value(cls, value: int) -> IntColumnLiteral:
        return cls.model_construct(value=value)


class StringColumnLiteral(pydantic.BaseModel):
    """A literal `str` value in a column expression."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["str"] = "str"
    value: str

    @classmethod
    def from_value(cls, value: str) -> StringColumnLiteral:
        return cls.model_construct(value=value)


class FloatColumnLiteral(pydantic.BaseModel):
    """A literal `float` value in a column expression."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["float"] = "float"
    value: float

    @classmethod
    def from_value(cls, value: float) -> FloatColumnLiteral:
        return cls.model_construct(value=value)


class BytesColumnLiteral(pydantic.BaseModel):
    """A literal `bytes` value in a column expression.

    The original value is base64-encoded when serialized and decoded on first
    use.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["bytes"] = "bytes"
    encoded: bytes

    @cached_property
    def value(self) -> bytes:
        return b64decode(self.encoded)

    @classmethod
    def from_value(cls, value: bytes) -> BytesColumnLiteral:
        return cls.model_construct(encoded=b64encode(value))


class TimeColumnLiteral(pydantic.BaseModel):
    """A literal `astropy.time.Time` value in a column expression.

    The time is converted into TAI nanoseconds since 1970-01-01 when serialized
    and restored from that on first use.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["time"] = "time"
    nsec: int

    @cached_property
    def value(self) -> astropy.time.Time:
        return TimeConverter().nsec_to_astropy(self.nsec)

    @classmethod
    def from_value(cls, value: astropy.time.Time) -> TimeColumnLiteral:
        return cls.model_construct(nsec=TimeConverter().astropy_to_nsec(value))


class TimespanColumnLiteral(pydantic.BaseModel):
    """A literal `Timespan` value in a column expression.

    The timespan bounds are converted into TAI nanoseconds since 1970-01-01
    when serialized and the timespan is restored from that on first use.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["timespan"] = "timespan"
    begin_nsec: int
    end_nsec: int

    @cached_property
    def value(self) -> astropy.time.Time:
        return Timespan(None, None, _nsec=(self.begin_nsec, self.end_nsec))

    @classmethod
    def from_value(cls, value: Timespan) -> TimespanColumnLiteral:
        return cls.model_construct(begin_nsec=value._nsec[0], end_nsec=value._nsec[1])


class RegionColumnLiteral(pydantic.BaseModel):
    """A literal `lsst.sphgeom.Region` value in a column expression.

    The region is encoded to base64 `bytes` when serialized, and decoded on
    first use.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["region"] = "region"

    encoded: bytes

    @cached_property
    def value(self) -> bytes:
        return Region.decode(b64decode(self.encoded))

    @classmethod
    def from_value(cls, value: Region) -> RegionColumnLiteral:
        return cls.model_construct(encoded=b64encode(value.encode()))


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


class DimensionKeyReference(pydantic.BaseModel):
    """A column expression that references a dimension primary key column."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["dimension_key"] = "dimension_key"
    dimension: str


class DimensionFieldReference(pydantic.BaseModel):
    """A column expression that references a dimension record column that is
    not a primary ket.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["dimension_field"] = "dimension_field"
    element: str
    field: str


class DatasetFieldReference(pydantic.BaseModel):
    """A column expression that references a column associated with a dataset
    type.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["dataset_field"] = "dataset_field"
    dataset_type: str | None
    field: str


ColumnReference: TypeAlias = Union[
    DimensionKeyReference,
    DimensionFieldReference,
    DatasetFieldReference,
]


class UnaryExpression(pydantic.BaseModel):
    """A unary operation on a column expression that returns a non-bool."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["unary"] = "unary"
    operand: DiscriminatedExpression
    operator: Literal["-", "begin_of", "end_of"]


class BinaryExpression(pydantic.BaseModel):
    """A binary operation on column expressions that returns a non-bool."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["binary"] = "binary"
    a: DiscriminatedExpression
    b: DiscriminatedExpression
    operator: Literal["+", "-", "*", "/", "%"]


Expression: TypeAlias = Union[
    ColumnLiteral,
    ColumnReference,
    UnaryExpression,
    BinaryExpression,
]


DiscriminatedExpression: TypeAlias = Annotated[Expression, pydantic.Field(discriminator="expression_type")]


class Reversed(pydantic.BaseModel):
    """A tag wrapper for `AbstractExpression` that indicate sorting in
    reverse order.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    expression_type: Literal["reversed"] = "reversed"
    operand: DiscriminatedExpression


DiscriminatedOrderExpression: TypeAlias = Annotated[
    Union[Expression, Reversed], pydantic.Field(discriminator="type")
]


class LogicalAnd(pydantic.BaseModel, Generic[_D]):
    """A boolean column expression that is `True` only if all of its operands
    are `True`.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["and"] = "and"
    operands: tuple[DiscriminatedPredicate[_D], ...] = pydantic.Field(min_length=2)

    def with_universe(self, universe: DimensionUniverse) -> LogicalAnd[DimensionGroup]:
        new_operands: list[DiscriminatedPredicate[DimensionGroup]] = []
        any_new: bool = False
        for operand in self.operands:
            new_operand = operand.with_universe(universe)
            if new_operand is not operand:
                any_new = True
        if any_new:
            return LogicalAnd.model_construct(operands=tuple(new_operands))
        else:
            return cast(LogicalAnd[DimensionGroup], self)


class LogicalOr(pydantic.BaseModel, Generic[_D]):
    """A boolean column expression that is `True` if any of its operands are
    `True`.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["or"] = "or"
    operands: tuple[DiscriminatedPredicate[_D], ...] = pydantic.Field(min_length=2)

    def with_universe(self, universe: DimensionUniverse) -> LogicalOr[DimensionGroup]:
        new_operands: list[DiscriminatedPredicate[DimensionGroup]] = []
        any_new: bool = False
        for operand in self.operands:
            new_operand = operand.with_universe(universe)
            if new_operand is not operand:
                any_new = True
        if any_new:
            return LogicalOr.model_construct(operands=tuple(new_operands))
        else:
            return cast(LogicalOr[DimensionGroup], self)


class LogicalNot(pydantic.BaseModel, Generic[_D]):
    """A boolean column expression that inverts its operand."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["not"] = "not"
    operand: DiscriminatedPredicate[_D]

    def with_universe(self, universe: DimensionUniverse) -> LogicalNot[DimensionGroup]:
        if (new_operand := self.operand.with_universe(universe)) is self.operand:
            return cast(LogicalNot[DimensionGroup], self)
        return LogicalNot.model_construct(operand=new_operand)


class IsNull(pydantic.BaseModel):
    """A boolean column expression that tests whether its operand is NULL."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["is_null"] = "is_null"
    operand: DiscriminatedExpression

    def with_universe(self, universe: DimensionUniverse) -> IsNull:
        return self


class Comparison(pydantic.BaseModel):
    """A boolean columns expression formed by comparing two non-boolean
    expressions.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["comparison"] = "comparison"
    a: DiscriminatedExpression
    b: DiscriminatedExpression
    operator: Literal["=", "!=", "<", ">", ">=", "<=", "overlaps"]

    def with_universe(self, universe: DimensionUniverse) -> Comparison:
        return self


class InContainer(pydantic.BaseModel):
    """A boolean column expression that tests whether one expression is a
    member of an explicit sequence of other expressions.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["in_container"] = "in_container"
    member: DiscriminatedExpression
    container: tuple[DiscriminatedExpression, ...]

    def with_universe(self, universe: DimensionUniverse) -> InContainer:
        return self


class InRange(pydantic.BaseModel):
    """A boolean column expression that tests whether its expression is
    included in an integer range.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["in_range"] = "in_range"
    member: DiscriminatedExpression
    start: int = 0
    stop: int | None = None
    step: int = 1

    def with_universe(self, universe: DimensionUniverse) -> InRange:
        return self


class InRelation(pydantic.BaseModel, Generic[_D]):
    """A boolean column expression that tests whether its expression is
    included single-column projection of a relation.

    This is primarily intended to be used on dataset ID columns, but it may
    be useful for other columns as well.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["in_relation"] = "in_relation"
    member: DiscriminatedExpression
    column: DiscriminatedExpression
    relation: DiscriminatedRelation[_D]

    def with_universe(self, universe: DimensionUniverse) -> InRelation:
        if self.relation.dimensions is None or isinstance(self.relation.dimensions, DimensionGroup):
            return cast(InRelation[DimensionGroup], self)
        return InRelation.model_construct(
            member=self.member, column=self.column, relation=self.relation.with_universe(universe)
        )


class StringPredicate(pydantic.BaseModel, Generic[_D]):
    """A tag wrapper for boolean column expressions created by parsing a string
    expression.

    Remembering the original string is useful for error reporting.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["string_predicate"] = "string_predicate"
    where: str
    tree: DiscriminatedPredicate[_D]

    def with_universe(self, universe: DimensionUniverse) -> StringPredicate[DimensionGroup]:
        if (new_tree := self.tree.with_universe(universe)) is self.tree:
            return cast(StringPredicate[DimensionGroup], self)
        return StringPredicate.model_construct(where=self.where, tree=new_tree)


class DataCoordinateConstraint(pydantic.BaseModel, Generic[_D]):
    """A boolean column expression defined by interpreting data ID's key-value
    pairs as a logical AND of equality constraints.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["data_coordinate_constraint"] = "data_coordinate_constraint"

    dimensions: _D
    """The dimensions of the data ID."""

    values: tuple[DataIdValue, ...]
    """The required values of the data ID."""

    def with_universe(self, universe: DimensionUniverse) -> DataCoordinateConstraint[DimensionGroup]:
        if isinstance(self.dimensions, DimensionGroup):
            return cast(DataCoordinateConstraint[DimensionGroup], self.dimensions)
        from .abstract_relations import _validate_dimensions

        return DataCoordinateConstraint.model_construct(
            dimensions=_validate_dimensions(universe, self.dimensions), values=self.values
        )


Predicate: TypeAlias = Union[
    LogicalAnd[_D],
    LogicalOr[_D],
    LogicalNot[_D],
    IsNull,
    Comparison,
    InContainer,
    InRange,
    InRelation[_D],
    StringPredicate[_D],
    DataCoordinateConstraint[_D],
]

DiscriminatedPredicate = Annotated[Predicate[_D], pydantic.Field(discriminator="predicate_type")]
