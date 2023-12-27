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
    "ColumnExpression",
    "OrderExpression",
    "UnaryExpression",
    "BinaryExpression",
    "Reversed",
)

from typing import Annotated, Literal, TypeAlias, Union, final

import pydantic

from ...column_spec import ColumnType
from ._base import ColumnExpressionBase, InvalidRelationError
from ._column_literal import ColumnLiteral
from ._column_reference import ColumnReference, _ColumnReference


@final
class UnaryExpression(ColumnExpressionBase):
    """A unary operation on a column expression that returns a non-bool."""

    expression_type: Literal["unary"] = "unary"

    operand: ColumnExpression
    """Expression this one operates on."""

    operator: Literal["-", "begin_of", "end_of"]
    """Operator this expression applies."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        return self.operand.gather_required_columns()

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 1

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
        if self.operand.precedence >= self.precedence:
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
            case ("-" "int" | "float"):
                pass
            case ("begin_of" | "end_of", "timespan"):
                pass
            case _:
                raise InvalidRelationError(
                    f"Invalid column type {self.operand.column_type} for operator {self.operator!r}."
                )
        return self


@final
class BinaryExpression(ColumnExpressionBase):
    """A binary operation on column expressions that returns a non-bool."""

    expression_type: Literal["binary"] = "binary"

    a: ColumnExpression
    """Left-hand side expression this one operates on."""

    b: ColumnExpression
    """Right-hand side expression this one operates on."""

    operator: Literal["+", "-", "*", "/", "%"]
    """Operator this expression applies.

    Integer '/' and '%' are defined as in SQL, not Python (though the
    definitions are the same for positive arguments).
    """

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        result = self.a.gather_required_columns()
        result.update(self.b.gather_required_columns())
        return result

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        match self.operator:
            case "*" | "/" | "%":
                return 2
            case "+" | "-":
                return 3

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        return self.a.column_type

    def __str__(self) -> str:
        a = str(self.a)
        b = str(self.b)
        match self.operator:
            case "*" | "+":
                if self.a.precedence > self.precedence:
                    a = f"({a})"
                if self.b.precedence > self.precedence:
                    b = f"({b})"
            case _:
                if self.a.precedence >= self.precedence:
                    a = f"({a})"
                if self.b.precedence >= self.precedence:
                    b = f"({b})"
        return f"({a} {self.operator} {b})"

    @pydantic.model_validator(mode="after")
    def _validate_types(self) -> BinaryExpression:
        if self.a.column_type != self.b.column_type:
            raise InvalidRelationError(
                f"Column types for operator {self.operator} do not agree "
                f"({self.a.column_type}, {self.b.column_type})."
            )
        match (self.operator, self.a.column_type):
            case ("+" | "-" | "*" | "/", "int" | "float"):
                pass
            case ("%", "int"):
                pass
            case _:
                raise InvalidRelationError(
                    f"Invalid column type {self.a.column_type} for operator {self.operator!r}."
                )
        return self


# Union without Pydantic annotation for the discriminator, for use in nesting
# in other unions that will add that annotation.  It's not clear whether it
# would work to just nest the annotated ones, but it seems safest not to rely
# on undocumented behavior.
_ColumnExpression: TypeAlias = Union[
    ColumnLiteral,
    _ColumnReference,
    UnaryExpression,
    BinaryExpression,
]


ColumnExpression: TypeAlias = Annotated[_ColumnExpression, pydantic.Field(discriminator="expression_type")]


@final
class Reversed(ColumnExpressionBase):
    """A tag wrapper for `AbstractExpression` that indicate sorting in
    reverse order.
    """

    expression_type: Literal["reversed"] = "reversed"

    operand: ColumnExpression
    """Expression to sort on in reverse."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        return self.operand.gather_required_columns()

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return self.operand.precedence

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        return self.operand.column_type

    def __str__(self) -> str:
        return f"{self.operand} DESC"


OrderExpression: TypeAlias = Annotated[
    Union[_ColumnExpression, Reversed], pydantic.Field(discriminator="expression_type")
]
