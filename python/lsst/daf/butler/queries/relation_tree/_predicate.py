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
    "Predicate",
    "LiteralTrue",
    "LiteralFalse",
    "LogicalAnd",
    "LogicalOr",
    "LogicalNot",
    "IsNull",
    "Comparison",
    "InContainer",
    "InRange",
    "InRelation",
    "DataCoordinateConstraint",
    "ComparisonOperator",
)

import itertools
from typing import TYPE_CHECKING, Annotated, Literal, TypeAlias, TypeVar, Union, final, overload

import pydantic

from ...dimensions import DataCoordinate, DataIdValue, DimensionGroup
from ._base import InvalidRelationError, PredicateBase
from ._column_expression import ColumnExpression
from ._column_reference import ColumnReference

if TYPE_CHECKING:
    from ._root_relation import RootRelation


ComparisonOperator: TypeAlias = Literal["==", "!=", "<", ">", ">=", "<=", "overlaps"]


_T = TypeVar("_T", bound="Predicate")


@final
class LiteralTrue(PredicateBase):
    """A boolean column expression that always evaluates to `True`."""

    predicate_type: Literal["literal_true"] = "literal_true"

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        return set()

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 5

    def logical_and(self, other: _T) -> _T:
        # Docstring inherited.
        return other

    def logical_or(self, other: Predicate) -> LiteralTrue:
        # Docstring inherited.
        return self

    def logical_not(self) -> LiteralFalse:
        # Docstring inherited.
        return LiteralFalse()


@final
class LiteralFalse(PredicateBase):
    """A boolean column expression that always evaluates to `False`."""

    predicate_type: Literal["literal_false"] = "literal_false"

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        return set()

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 5

    def logical_and(self, other: Predicate) -> LiteralFalse:
        # Docstring inherited.
        return self

    def logical_or(self, other: _T) -> _T:
        # Docstring inherited.
        return other

    def logical_not(self) -> LiteralTrue:
        # Docstring inherited.
        return LiteralTrue()


@final
class LogicalAnd(PredicateBase):
    """A boolean column expression that is `True` only if all of its operands
    are `True`.
    """

    predicate_type: Literal["and"] = "and"

    operands: tuple[LogicalAndOperand, ...] = pydantic.Field(min_length=2)
    """Upstream boolean expressions to combine."""

    @staticmethod
    def fold(first: Predicate, /, *args: Predicate) -> Predicate:
        """Combine a sequence of boolean expressions with logical AND.

        Parameters
        ----------
        first : `relation_tree.Predicate`
            First operand (required).
        *args
            Additional operands.

        Returns
        -------
        logical_and : `relation_tree.Predicate`
            A boolean expression that evaluates to `True` only if all operands
            evaluate to `True.
        """
        result = first
        for arg in args:
            result = result.logical_and(arg)
        return result

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        result: set[ColumnReference] = set()
        for operand in self.operands:
            result.update(operand.gather_required_columns())
        return result

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 6

    @overload
    def logical_and(self, other: LiteralFalse) -> LiteralFalse:
        ...

    @overload
    def logical_and(self, other: LogicalAnd | LogicalAndOperand | LiteralTrue) -> LogicalAnd:
        ...

    def logical_and(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        match other:
            case LogicalAnd():
                return LogicalAnd.model_construct(operands=self.operands + other.operands)
            case LiteralTrue():
                return self
            case LiteralFalse():
                return other
            case _:
                return LogicalAnd.model_construct(operands=self.operands + (other,))

    @overload
    def logical_or(self, other: LiteralTrue) -> LiteralTrue:
        ...

    @overload
    def logical_or(self, other: LogicalAnd | LogicalAndOperand | LiteralFalse) -> LogicalAnd:
        ...

    def logical_or(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        match other:
            case LogicalAnd():
                return LogicalAnd.model_construct(
                    operands=tuple(
                        [a.logical_or(b) for a, b in itertools.product(self.operands, other.operands)]
                    ),
                )
            case LiteralTrue():
                return other
            case LiteralFalse():
                return self
            case _:
                return LogicalAnd.model_construct(
                    operands=tuple([a.logical_or(other) for a in self.operands]),
                )

    def logical_not(self) -> Predicate:
        # Docstring inherited.
        first, *rest = self.operands
        result: Predicate = first.logical_not()
        for operand in rest:
            result = result.logical_or(operand.logical_not())
        return result

    def __str__(self) -> str:
        return " AND ".join(
            str(operand) if operand.precedence <= self.precedence else f"({operand})"
            for operand in self.operands
        )


@final
class LogicalOr(PredicateBase):
    """A boolean column expression that is `True` if any of its operands are
    `True`.
    """

    predicate_type: Literal["or"] = "or"

    operands: tuple[LogicalOrOperand, ...] = pydantic.Field(min_length=2)
    """Upstream boolean expressions to combine."""

    @staticmethod
    def fold(first: Predicate, /, *args: Predicate) -> Predicate:
        """Combine a sequence of boolean expressions with logical OR.

        Parameters
        ----------
        first : `relation_tree.Predicate`
            First operand (required).
        *args
            Additional operands.

        Returns
        -------
        logical_or : `relation_tree.Predicate`
            A boolean expression that evaluates to `True` if any operand
            evaluates to `True.
        """
        result = first
        for arg in args:
            result = result.logical_or(arg)
        return result

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        result: set[ColumnReference] = set()
        for operand in self.operands:
            result.update(operand.gather_required_columns())
        return result

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 7

    @overload
    def logical_and(self, other: LiteralFalse) -> LiteralFalse:
        ...

    @overload
    def logical_and(self, other: LogicalAnd | LogicalAndOperand | LiteralTrue) -> LogicalAnd:
        ...

    def logical_and(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_and(self, other)

    @overload
    def logical_or(self, other: LiteralTrue) -> LiteralTrue:
        ...

    @overload
    def logical_or(self, other: LogicalAnd) -> LogicalAnd:
        ...

    @overload
    def logical_or(self, other: LogicalAndOperand | LiteralFalse) -> LogicalOr:
        ...

    def logical_or(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        match other:
            case LogicalAnd():
                return LogicalAnd.model_construct(
                    operands=tuple([self.logical_or(b) for b in other.operands])
                )
            case LiteralTrue():
                return other
            case LiteralFalse():
                return self
            case LogicalOr():
                return LogicalOr.model_construct(operands=self.operands + other.operands)
            case _:
                return LogicalOr.model_construct(operands=self.operands + (other,))

    def logical_not(self) -> LogicalAnd:
        # Docstring inherited.
        return LogicalAnd.model_construct(operands=tuple([x.logical_not() for x in self.operands]))

    def __str__(self) -> str:
        return " OR ".join(
            str(operand) if operand.precedence <= self.precedence else f"({operand})"
            for operand in self.operands
        )


@final
class LogicalNot(PredicateBase):
    """A boolean column expression that inverts its operand."""

    predicate_type: Literal["not"] = "not"

    operand: LogicalNotOperand
    """Upstream boolean expression to invert."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        return self.operand.gather_required_columns()

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 4

    @overload
    def logical_and(self, other: LiteralFalse) -> LiteralFalse:
        ...

    @overload
    def logical_and(self, other: LogicalAnd | LogicalAndOperand | LiteralTrue) -> LogicalAnd:
        ...

    def logical_and(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_and(self, other)

    @overload
    def logical_or(self, other: LiteralTrue) -> LogicalNot:
        ...

    @overload
    def logical_or(self, other: LogicalAnd) -> LogicalAnd:
        ...

    @overload
    def logical_or(self, other: LogicalAndOperand | LiteralFalse) -> LogicalAndOperand:
        ...

    def logical_or(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_or(self, other)

    def logical_not(self) -> LogicalNotOperand:
        # Docstring inherited.
        return self.operand

    def __str__(self) -> str:
        if self.operand.precedence <= self.precedence:
            return f"NOT {self.operand}"
        else:
            return f"NOT ({self.operand})"


@final
class IsNull(PredicateBase):
    """A boolean column expression that tests whether its operand is NULL."""

    predicate_type: Literal["is_null"] = "is_null"

    operand: ColumnExpression
    """Upstream expression to test."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        return self.operand.gather_required_columns()

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 5

    @overload
    def logical_and(self, other: LiteralFalse) -> LiteralFalse:
        ...

    @overload
    def logical_and(self, other: LogicalAnd | LogicalAndOperand | LiteralTrue) -> LogicalAnd:
        ...

    def logical_and(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_and(self, other)

    @overload
    def logical_or(self, other: LiteralTrue) -> LiteralTrue:
        ...

    @overload
    def logical_or(self, other: LogicalAnd) -> LogicalAnd:
        ...

    @overload
    def logical_or(self, other: LogicalAndOperand | LiteralFalse) -> LogicalAndOperand:
        ...

    def logical_or(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_or(self, other)

    def logical_not(self) -> LogicalOrOperand:
        # Docstring inherited.
        return LogicalNot.model_construct(operand=self)

    def __str__(self) -> str:
        if self.operand.precedence <= self.precedence:
            return f"{self.operand} IS NULL"
        else:
            return f"({self.operand}) IS NULL"


@final
class Comparison(PredicateBase):
    """A boolean columns expression formed by comparing two non-boolean
    expressions.
    """

    predicate_type: Literal["comparison"] = "comparison"

    a: ColumnExpression
    """Left-hand side expression for the comparison."""

    b: ColumnExpression
    """Right-hand side expression for the comparison."""

    operator: ComparisonOperator
    """Comparison operator."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        result = self.a.gather_required_columns()
        result.update(self.b.gather_required_columns())
        return result

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 5

    @overload
    def logical_and(self, other: LiteralFalse) -> LiteralFalse:
        ...

    @overload
    def logical_and(self, other: LogicalAnd | LogicalAndOperand | LiteralTrue) -> LogicalAnd:
        ...

    def logical_and(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_and(self, other)

    @overload
    def logical_or(self, other: LiteralTrue) -> LiteralTrue:
        ...

    @overload
    def logical_or(self, other: LogicalAnd) -> LogicalAnd:
        ...

    @overload
    def logical_or(self, other: LogicalAndOperand | LiteralFalse) -> LogicalAndOperand:
        ...

    def logical_or(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_or(self, other)

    def logical_not(self) -> LogicalOrOperand:
        # Docstring inherited.
        return LogicalNot.model_construct(operand=self)

    def __str__(self) -> str:
        a = str(self.a) if self.a.precedence <= self.precedence else f"({self.a})"
        b = str(self.b) if self.b.precedence <= self.precedence else f"({self.b})"
        return f"{a} {self.operator.upper()} {b}"

    @pydantic.model_validator(mode="after")
    def _validate_column_types(self) -> Comparison:
        if self.a.column_type != self.b.column_type:
            raise InvalidRelationError(
                f"Column types for comparison {self} do not agree "
                f"({self.a.column_type}, {self.b.column_type})."
            )
        match (self.operator, self.a.column_type):
            case ("==" | "!=", _):
                pass
            case ("<" | ">" | ">=" | "<=", "int" | "string" | "float" | "datetime"):
                pass
            case ("overlaps", "region" | "timespan"):
                pass
            case _:
                raise InvalidRelationError(
                    f"Invalid column type {self.a.column_type} for operator {self.operator!r}."
                )
        return self


@final
class InContainer(PredicateBase):
    """A boolean column expression that tests whether one expression is a
    member of an explicit sequence of other expressions.
    """

    predicate_type: Literal["in_container"] = "in_container"

    member: ColumnExpression
    """Expression to test for membership."""

    container: tuple[ColumnExpression, ...]
    """Expressions representing the elements of the container."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        result = self.member.gather_required_columns()
        for operand in self.container:
            result.update(operand.gather_required_columns())
        return result

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 5

    @overload
    def logical_and(self, other: LiteralFalse) -> LiteralFalse:
        ...

    @overload
    def logical_and(self, other: LogicalAnd | LogicalAndOperand | LiteralTrue) -> LogicalAnd:
        ...

    def logical_and(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_and(self, other)

    @overload
    def logical_or(self, other: LiteralTrue) -> LiteralTrue:
        ...

    @overload
    def logical_or(self, other: LogicalAnd) -> LogicalAnd:
        ...

    @overload
    def logical_or(self, other: LogicalAndOperand | LiteralFalse) -> LogicalAndOperand:
        ...

    def logical_or(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_or(self, other)

    def logical_not(self) -> LogicalOrOperand:
        # Docstring inherited.
        return LogicalNot.model_construct(operand=self)

    def __str__(self) -> str:
        m = str(self.member) if self.member.precedence <= self.precedence else f"({self.member})"
        return f"{m} IN [{', '.join(str(item) for item in self.container)}]"


@final
class InRange(PredicateBase):
    """A boolean column expression that tests whether its expression is
    included in an integer range.
    """

    predicate_type: Literal["in_range"] = "in_range"

    member: ColumnExpression
    """Expression to test for membership."""

    start: int = 0
    """Inclusive lower bound for the range."""

    stop: int | None = None
    """Exclusive upper bound for the range."""

    step: int = 1
    """Difference between values in the range."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        return self.member.gather_required_columns()

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 5

    @overload
    def logical_and(self, other: LiteralFalse) -> LiteralFalse:
        ...

    @overload
    def logical_and(self, other: LogicalAnd | LogicalAndOperand | LiteralTrue) -> LogicalAnd:
        ...

    def logical_and(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_and(self, other)

    @overload
    def logical_or(self, other: LiteralTrue) -> LiteralTrue:
        ...

    @overload
    def logical_or(self, other: LogicalAnd) -> LogicalAnd:
        ...

    @overload
    def logical_or(self, other: LogicalAndOperand | LiteralFalse) -> LogicalAndOperand:
        ...

    def logical_or(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_or(self, other)

    def logical_not(self) -> LogicalOrOperand:
        # Docstring inherited.
        return LogicalNot.model_construct(operand=self)

    def __str__(self) -> str:
        s = f"{self.start if self.start else ''}..{self.stop if self.stop is not None else ''}"
        if self.step != 1:
            s = f"{s}:{self.step}"
        m = str(self.member) if self.member.precedence <= self.precedence else f"({self.member})"
        return f"{m} IN {s}"


@final
class InRelation(PredicateBase):
    """A boolean column expression that tests whether its expression is
    included single-column projection of a relation.

    This is primarily intended to be used on dataset ID columns, but it may
    be useful for other columns as well.
    """

    predicate_type: Literal["in_relation"] = "in_relation"

    member: ColumnExpression
    """Expression to test for membership."""

    column: ColumnExpression
    """Expression to extract from `relation`."""

    relation: RootRelation
    """Relation whose rows from `column` represent the container."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        # We're only gathering columns from the relation this predicate is
        # attached to, not `self.column`, which belongs to `self.relation`.
        return self.member.gather_required_columns()

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 5

    @overload
    def logical_and(self, other: LiteralFalse) -> LiteralFalse:
        ...

    @overload
    def logical_and(self, other: LogicalAnd | LogicalAndOperand | LiteralTrue) -> LogicalAnd:
        ...

    def logical_and(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_and(self, other)

    @overload
    def logical_or(self, other: LiteralTrue) -> LiteralTrue:
        ...

    @overload
    def logical_or(self, other: LogicalAnd) -> LogicalAnd:
        ...

    @overload
    def logical_or(self, other: LogicalAndOperand | LiteralFalse) -> LogicalAndOperand:
        ...

    def logical_or(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_or(self, other)

    def logical_not(self) -> LogicalOrOperand:
        # Docstring inherited.
        return LogicalNot.model_construct(operand=self)

    def __str__(self) -> str:
        m = str(self.member) if self.member.precedence <= self.precedence else f"({self.member})"
        c = str(self.column) if self.column.precedence <= self.precedence else f"({self.column})"
        return f"{m} IN [{{{self.relation}}}.{c}]"


@final
class DataCoordinateConstraint(PredicateBase):
    """A boolean column expression defined by interpreting data ID's key-value
    pairs as a logical AND of equality constraints.
    """

    predicate_type: Literal["data_coordinate_constraint"] = "data_coordinate_constraint"

    dimensions: DimensionGroup
    """The dimensions of the data ID."""

    values: tuple[DataIdValue, ...]
    """The required values of the data ID."""

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 5

    @overload
    def logical_and(self, other: LiteralFalse) -> LiteralFalse:
        ...

    @overload
    def logical_and(self, other: LogicalAnd | LogicalAndOperand | LiteralTrue) -> LogicalAnd:
        ...

    def logical_and(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_and(self, other)

    @overload
    def logical_or(self, other: LiteralTrue) -> LiteralTrue:
        ...

    @overload
    def logical_or(self, other: LogicalAnd) -> LogicalAnd:
        ...

    @overload
    def logical_or(self, other: LogicalAndOperand | LiteralFalse) -> LogicalAndOperand:
        ...

    def logical_or(self, other: Predicate) -> Predicate:
        # Docstring inherited.
        return _base_logical_or(self, other)

    def logical_not(self) -> LogicalOrOperand:
        # Docstring inherited.
        return LogicalNot.model_construct(operand=self)

    def __str__(self) -> str:
        return str(DataCoordinate.from_required_values(self.dimensions, self.values))


@overload
def _base_logical_and(a: LogicalAndOperand, b: LiteralTrue) -> LogicalAndOperand:
    ...


@overload
def _base_logical_and(a: LogicalAndOperand, b: LiteralFalse) -> LiteralFalse:
    ...


@overload
def _base_logical_and(a: LogicalAndOperand, b: LogicalAnd | LogicalAndOperand) -> LogicalAnd:
    ...


def _base_logical_and(a: LogicalAndOperand, b: Predicate) -> Predicate:
    match b:
        case LogicalAnd():
            return LogicalAnd.model_construct(operands=(a,) + b.operands)
        case LiteralTrue():
            return a
        case LiteralFalse():
            return b
        case _:
            return LogicalAnd.model_construct(operands=(a, b))


@overload
def _base_logical_or(a: LogicalOrOperand, b: LiteralTrue) -> LiteralTrue:
    ...


@overload
def _base_logical_or(a: LogicalOrOperand, b: LogicalAnd) -> LogicalAnd:
    ...


@overload
def _base_logical_or(a: LogicalOrOperand, b: LogicalAndOperand | LiteralFalse) -> LogicalAndOperand:
    ...


def _base_logical_or(a: LogicalOrOperand, b: Predicate) -> Predicate:
    match b:
        case LogicalAnd():
            return LogicalAnd.model_construct(
                operands=tuple(_base_logical_or(a, b_operand) for b_operand in b.operands)
            )
        case LiteralTrue():
            return b
        case LiteralFalse():
            return a
        case LogicalOr():
            return LogicalOr.model_construct(operands=(a,) + b.operands)
        case _:
            return LogicalOr.model_construct(operands=(a, b))


_LogicalNotOperand = Union[
    IsNull,
    Comparison,
    InContainer,
    InRange,
    InRelation,
    DataCoordinateConstraint,
]
_LogicalOrOperand = Union[_LogicalNotOperand, LogicalNot]
_LogicalAndOperand = Union[_LogicalOrOperand, LogicalOr]


LogicalNotOperand = Annotated[_LogicalNotOperand, pydantic.Field(discriminator="predicate_type")]
LogicalOrOperand = Annotated[_LogicalOrOperand, pydantic.Field(discriminator="predicate_type")]
LogicalAndOperand = Annotated[_LogicalAndOperand, pydantic.Field(discriminator="predicate_type")]

Predicate = Annotated[
    Union[_LogicalAndOperand, LogicalAnd, LiteralTrue, LiteralFalse],
    pydantic.Field(discriminator="predicate_type"),
]
