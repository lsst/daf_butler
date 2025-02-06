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
    "BinaryExpression",
    "BinaryOperator",
    "ColumnExpression",
    "OrderExpression",
    "Reversed",
    "UnaryExpression",
    "UnaryOperator",
    "is_one_datetime_and_one_ingest_date",
    "is_one_timespan_and_one_datetime",
    "validate_order_expression",
)

from typing import TYPE_CHECKING, Annotated, Literal, NamedTuple, TypeAlias, TypeVar, final

import pydantic

from ..._exceptions import InvalidQueryError
from ...column_spec import ColumnType
from ._base import ColumnExpressionBase
from ._column_literal import ColumnLiteral
from ._column_reference import ColumnReference
from ._column_set import ColumnSet

if TYPE_CHECKING:
    from ..visitors import ColumnExpressionVisitor


_T = TypeVar("_T")


UnaryOperator: TypeAlias = Literal["-", "begin_of", "end_of"]
BinaryOperator: TypeAlias = Literal["+", "-", "*", "/", "%"]


@final
class UnaryExpression(ColumnExpressionBase):
    """A unary operation on a column expression that returns a non-boolean
    value.
    """

    expression_type: Literal["unary"] = "unary"

    operand: ColumnExpression
    """Expression this one operates on."""

    operator: UnaryOperator
    """Operator this expression applies."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        self.operand.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        self.operand.gather_governors(governors)

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        match self.operator:
            case "-":
                return self.operand.column_type
            case "begin_of" | "end_of":
                return "datetime"
        raise AssertionError(f"Invalid unary expression operator {self.operator}.")

    def __str__(self) -> str:
        s = str(self.operand)
        if not (self.operand.is_literal or self.operand.is_column_reference):
            s = f"({s})"
        match self.operator:
            case "-":
                return f"-{s}"
            case "begin_of":
                return f"{s}.begin"
            case "end_of":
                return f"{s}.end"

    @pydantic.model_validator(mode="after")
    def _validate_types(self) -> UnaryExpression:
        match (self.operator, self.operand.column_type):
            case ("-", "int" | "float"):
                pass
            case ("begin_of" | "end_of", "timespan"):
                pass
            case _:
                raise InvalidQueryError(
                    f"Invalid column type {self.operand.column_type} for operator {self.operator!r}."
                )
        return self

    def visit(self, visitor: ColumnExpressionVisitor[_T]) -> _T:
        # Docstring inherited.
        return visitor.visit_unary_expression(self)


@final
class BinaryExpression(ColumnExpressionBase):
    """A binary operation on column expressions that returns a non-boolean
    value.
    """

    expression_type: Literal["binary"] = "binary"

    a: ColumnExpression
    """Left-hand side expression this one operates on."""

    b: ColumnExpression
    """Right-hand side expression this one operates on."""

    operator: BinaryOperator
    """Operator this expression applies.

    Integer '/' and '%' are defined as in SQL, not Python (though the
    definitions are the same for positive arguments).
    """

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        self.a.gather_required_columns(columns)
        self.b.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        self.a.gather_governors(governors)
        self.b.gather_governors(governors)

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        return self.a.column_type

    def __str__(self) -> str:
        a = str(self.a)
        b = str(self.b)
        if not (self.a.is_literal or self.a.is_column_reference):
            a = f"({a})"
        if not (self.b.is_literal or self.b.is_column_reference):
            b = f"({b})"
        return f"{a} {self.operator} {b}"

    @pydantic.model_validator(mode="after")
    def _validate_types(self) -> BinaryExpression:
        if self.a.column_type != self.b.column_type:
            raise InvalidQueryError(
                f"Column types for operator {self.operator} do not agree "
                f"({self.a.column_type}, {self.b.column_type})."
            )
        match (self.operator, self.a.column_type):
            case ("+" | "-" | "*" | "/", "int" | "float"):
                pass
            case ("%", "int"):
                pass
            case _:
                raise InvalidQueryError(
                    f"Invalid column type {self.a.column_type} for operator {self.operator!r}."
                )
        return self

    def visit(self, visitor: ColumnExpressionVisitor[_T]) -> _T:
        # Docstring inherited.
        return visitor.visit_binary_expression(self)


# Union without Pydantic annotation for the discriminator, for use in nesting
# in other unions that will add that annotation.  It's not clear whether it
# would work to just nest the annotated ones, but it seems safest not to rely
# on undocumented behavior.
_ColumnExpression: TypeAlias = ColumnLiteral | ColumnReference | UnaryExpression | BinaryExpression


ColumnExpression: TypeAlias = Annotated[_ColumnExpression, pydantic.Field(discriminator="expression_type")]


@final
class Reversed(ColumnExpressionBase):
    """A tag wrapper for `ColumnExpression` that indicates sorting in
    reverse order.
    """

    expression_type: Literal["reversed"] = "reversed"

    operand: ColumnExpression
    """Expression to sort on in reverse."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        self.operand.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        self.operand.gather_governors(governors)

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        return self.operand.column_type

    def __str__(self) -> str:
        return f"{self.operand} DESC"

    def visit(self, visitor: ColumnExpressionVisitor[_T]) -> _T:
        # Docstring inherited.
        return visitor.visit_reversed(self)


def validate_order_expression(expression: _ColumnExpression | Reversed) -> _ColumnExpression | Reversed:
    """Check that a column expression can be used for sorting.

    Parameters
    ----------
    expression : `OrderExpression`
        Expression to check.

    Returns
    -------
    expression : `OrderExpression`
        The checked expression; returned to make this usable as a Pydantic
        validator.

    Raises
    ------
    InvalidQueryError
        Raised if this expression is not one that can be used for sorting.
    """
    if expression.column_type not in ("int", "string", "float", "datetime", "ingest_date"):
        raise InvalidQueryError(f"Column type {expression.column_type} of {expression} is not ordered.")
    return expression


OrderExpression: TypeAlias = Annotated[
    _ColumnExpression | Reversed,
    pydantic.Field(discriminator="expression_type"),
    pydantic.AfterValidator(validate_order_expression),
]


class TimespanAndDatetime(NamedTuple):
    timespan: ColumnExpression
    datetime: ColumnExpression


def is_one_timespan_and_one_datetime(
    a: ColumnExpression, b: ColumnExpression
) -> TimespanAndDatetime | None:  # numpydoc ignore=PR01
    """Check whether the two columns ``a`` and `b`` include one datetime column
    and one timespan column.

    Returns
    -------
    which_is_which : `TimespanAndDatetime` | None
        An object telling which column is the datetime and which is the
        timespan, or `None` if the types were not as expected.
    """
    if a.column_type == "timespan" and b.column_type == "datetime":
        return TimespanAndDatetime(a, b)
    elif a.column_type == "datetime" and b.column_type == "timespan":
        return TimespanAndDatetime(b, a)
    else:
        return None


def is_one_datetime_and_one_ingest_date(
    a: ColumnExpression, b: ColumnExpression
) -> bool:  # numpydoc ignore=PR01
    """Return `True` if the two columns ``a`` and `b`` include one datetime
    column and one ingest_date column.
    """
    return (a.column_type == "datetime" and b.column_type == "ingest_date") or (
        a.column_type == "ingest_date" and b.column_type == "datetime"
    )
