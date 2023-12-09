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
    "LogicalAnd",
    "LogicalOr",
    "LogicalNot",
    "IsNull",
    "Comparison",
    "InContainer",
    "InRange",
    "InRelation",
    "StringPredicate",
    "DataCoordinateConstraint",
    "ComparisonOperator",
)

from typing import TYPE_CHECKING, Annotated, Literal, TypeAlias, Union

import pydantic

from ...dimensions import DataCoordinate, DataIdValue, DimensionGroup
from ._base import PredicateBase
from ._column_expression import ColumnExpression
from ._column_reference import ColumnReference

if TYPE_CHECKING:
    from ._relation import RootRelation


ComparisonOperator: TypeAlias = Literal["==", "!=", "<", ">", ">=", "<=", "overlaps"]


class LogicalAnd(PredicateBase):
    """A boolean column expression that is `True` only if all of its operands
    are `True`.
    """

    predicate_type: Literal["and"] = "and"
    operands: tuple[Predicate, ...] = pydantic.Field(min_length=2)

    def gather_required_columns(self) -> set[ColumnReference]:
        result = self.operands[0].gather_required_columns()
        for operand in self.operands[1:]:
            result.update(operand.gather_required_columns())
        return result

    @property
    def precedence(self) -> int:
        return 6

    def __str__(self) -> str:
        return " AND ".join(
            str(operand) if operand.precedence <= self.precedence else f"({operand})"
            for operand in self.operands
        )

    def _flatten_and(self) -> tuple[Predicate, ...]:
        return self.operands


class LogicalOr(PredicateBase):
    """A boolean column expression that is `True` if any of its operands are
    `True`.
    """

    predicate_type: Literal["or"] = "or"
    operands: tuple[Predicate, ...] = pydantic.Field(min_length=2)

    def gather_required_columns(self) -> set[ColumnReference]:
        result = self.operands[0].gather_required_columns()
        for operand in self.operands[1:]:
            result.update(operand.gather_required_columns())
        return result

    @property
    def precedence(self) -> int:
        return 7

    def __str__(self) -> str:
        return " OR ".join(
            str(operand) if operand.precedence <= self.precedence else f"({operand})"
            for operand in self.operands
        )

    def _flatten_or(self) -> tuple[Predicate, ...]:
        return self.operands


class LogicalNot(PredicateBase):
    """A boolean column expression that inverts its operand."""

    predicate_type: Literal["not"] = "not"
    operand: Predicate

    def gather_required_columns(self) -> set[ColumnReference]:
        return self.operand.gather_required_columns()

    @property
    def precedence(self) -> int:
        return 4

    def __str__(self) -> str:
        if self.operand.precedence <= self.precedence:
            return f"NOT {self.operand}"
        else:
            return f"NOT ({self.operand})"


class IsNull(PredicateBase):
    """A boolean column expression that tests whether its operand is NULL."""

    predicate_type: Literal["is_null"] = "is_null"
    operand: ColumnExpression

    def gather_required_columns(self) -> set[ColumnReference]:
        return self.operand.gather_required_columns()

    @property
    def precedence(self) -> int:
        return 5

    def __str__(self) -> str:
        if self.operand.precedence <= self.precedence:
            return f"{self.operand} IS NULL"
        else:
            return f"({self.operand}) IS NULL"


class Comparison(PredicateBase):
    """A boolean columns expression formed by comparing two non-boolean
    expressions.
    """

    predicate_type: Literal["comparison"] = "comparison"
    a: ColumnExpression
    b: ColumnExpression
    operator: ComparisonOperator

    def gather_required_columns(self) -> set[ColumnReference]:
        result = self.a.gather_required_columns()
        result.update(self.b.gather_required_columns())
        return result

    @property
    def precedence(self) -> int:
        return 5

    def __str__(self) -> str:
        a = str(self.a) if self.a.precedence <= self.precedence else f"({self.a})"
        b = str(self.b) if self.b.precedence <= self.precedence else f"({self.b})"
        return f"{a} {self.operator.upper()} {b}"


class InContainer(PredicateBase):
    """A boolean column expression that tests whether one expression is a
    member of an explicit sequence of other expressions.
    """

    predicate_type: Literal["in_container"] = "in_container"
    member: ColumnExpression
    container: tuple[ColumnExpression, ...]

    def gather_required_columns(self) -> set[ColumnReference]:
        result = self.member.gather_required_columns()
        for operand in self.container:
            result.update(operand.gather_required_columns())
        return result

    @property
    def precedence(self) -> int:
        return 5

    def __str__(self) -> str:
        m = str(self.member) if self.member.precedence <= self.precedence else f"({self.member})"
        return f"{m} IN [{', '.join(str(item) for item in self.container)}]"


class InRange(PredicateBase):
    """A boolean column expression that tests whether its expression is
    included in an integer range.
    """

    predicate_type: Literal["in_range"] = "in_range"
    member: ColumnExpression
    start: int = 0
    stop: int | None = None
    step: int = 1

    def gather_required_columns(self) -> set[ColumnReference]:
        return self.member.gather_required_columns()

    @property
    def precedence(self) -> int:
        return 5

    def __str__(self) -> str:
        s = f"{self.start if self.start else ''}..{self.stop if self.stop is not None else ''}"
        if self.step != 1:
            s = f"{s}:{self.step}"
        m = str(self.member) if self.member.precedence <= self.precedence else f"({self.member})"
        return f"{m} IN {s}"


class InRelation(PredicateBase):
    """A boolean column expression that tests whether its expression is
    included single-column projection of a relation.

    This is primarily intended to be used on dataset ID columns, but it may
    be useful for other columns as well.
    """

    predicate_type: Literal["in_relation"] = "in_relation"
    member: ColumnExpression
    column: ColumnExpression
    relation: RootRelation

    @property
    def precedence(self) -> int:
        return 5

    def gather_required_columns(self) -> set[ColumnReference]:
        # We're only gathering columns from the relation this predicate is
        # attached to, not `self.column`, which belongs to `self.relation`.
        return self.member.gather_required_columns()

    def __str__(self) -> str:
        m = str(self.member) if self.member.precedence <= self.precedence else f"({self.member})"
        c = str(self.column) if self.column.precedence <= self.precedence else f"({self.column})"
        return f"{m} IN [{{{self.relation}}}.{c}]"


class StringPredicate(PredicateBase):
    """A tag wrapper for boolean column expressions created by parsing a string
    expression.

    Remembering the original string is useful for error reporting.
    """

    predicate_type: Literal["string_predicate"] = "string_predicate"
    where: str
    tree: Predicate

    def gather_required_columns(self) -> set[ColumnReference]:
        return self.tree.gather_required_columns()

    @property
    def precedence(self) -> int:
        return 5

    def __str__(self) -> str:
        return f'parsed("{self.where}")'


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
        return 5

    def __str__(self) -> str:
        return str(DataCoordinate.from_required_values(self.dimensions, self.values))


Predicate = Annotated[
    Union[
        LogicalAnd,
        LogicalOr,
        LogicalNot,
        IsNull,
        Comparison,
        InContainer,
        InRange,
        InRelation,
        StringPredicate,
        DataCoordinateConstraint,
    ],
    pydantic.Field(discriminator="predicate_type"),
]
