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
    "_ColumnExpression",
    "ColumnExpression",
    "OrderExpression",
    "OrderExpression",
)

from typing import Annotated, Literal, TypeAlias, Union

import pydantic

from ._base import ColumnExpressionBase
from ._column_literal import ColumnLiteral
from ._column_reference import ColumnReference, _ColumnReference


class UnaryExpression(ColumnExpressionBase):
    """A unary operation on a column expression that returns a non-bool."""

    expression_type: Literal["unary"] = "unary"
    operand: ColumnExpression
    operator: Literal["-", "begin_of", "end_of"]

    def gather_required_columns(self) -> set[ColumnReference]:
        return self.operand.gather_required_columns()


class BinaryExpression(ColumnExpressionBase):
    """A binary operation on column expressions that returns a non-bool."""

    expression_type: Literal["binary"] = "binary"
    a: ColumnExpression
    b: ColumnExpression
    operator: Literal["+", "-", "*", "/", "%"]

    def gather_required_columns(self) -> set[ColumnReference]:
        result = self.a.gather_required_columns()
        result.update(self.b.gather_required_columns())
        return result


_ColumnExpression: TypeAlias = Union[
    ColumnLiteral,
    _ColumnReference,
    UnaryExpression,
    BinaryExpression,
]


ColumnExpression: TypeAlias = Annotated[_ColumnExpression, pydantic.Field(discriminator="expression_type")]


class Reversed(ColumnExpressionBase):
    """A tag wrapper for `AbstractExpression` that indicate sorting in
    reverse order.
    """

    expression_type: Literal["reversed"] = "reversed"
    operand: ColumnExpression


OrderExpression: TypeAlias = Annotated[
    Union[_ColumnExpression, Reversed], pydantic.Field(discriminator="expression_type")
]
