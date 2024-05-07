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

from collections.abc import Set
from typing import Literal, NamedTuple, TypeAlias

import astropy.time

from .._exceptions import InvalidQueryError
from .._timespan import Timespan
from ..column_spec import ColumnType
from ..dimensions import DimensionUniverse
from ..registry.queries.expressions.categorize import ExpressionConstant, categorizeConstant
from ..registry.queries.expressions.parser import Node, RangeLiteral, TreeVisitor, parse_expression
from ._identifiers import IdentifierContext, interpret_identifier
from .tree import (
    BinaryExpression,
    ColumnExpression,
    ComparisonOperator,
    LiteralValue,
    Predicate,
    UnaryExpression,
    make_column_literal,
)

BindValue = LiteralValue | list[LiteralValue] | tuple[LiteralValue] | Set[LiteralValue]


def convert_expression_string_to_predicate(
    expression: str, *, context: IdentifierContext, universe: DimensionUniverse
) -> Predicate:
    """Convert a Butler query expression string to a `Predicate` for use in a
    QueryTree.

    Parameters
    ----------
    expression : `str`
        Butler expression query string, as used by the old query system to
        specify filtering.
    context : `IdentifierContext`
        Contextual information that helps determine the meaning of an
        identifier used in a query.
    universe : `DimensionUniverse`
        Dimension metadata for the Butler database being queried.

    Returns
    -------
    predicate : `Predicate`
        Predicate corresponding to that filter, for use in `QueryTree`.
    """
    try:
        tree = parse_expression(expression)
    except Exception as exc:
        raise InvalidQueryError(f"Failed to parse expression '{expression}'") from exc

    converter = _ConversionVisitor(context, universe)
    predicate = tree.visit(converter)
    assert isinstance(
        predicate, Predicate
    ), "The grammar should guarantee that we get a predicate back at the top level."

    return predicate


class _ColExpr(NamedTuple):
    """Represents a portion of the original expression that has been converted
    to a ColumnExpression object.
    """

    # This wrapper object mostly exists to help with typing and match() --
    # ColumnExpression is a big discriminated union, and mypy was having a lot
    # of trouble dealing with it in the context of _VisitorResult's extra
    # layers of union.

    value: ColumnExpression

    @property
    def column_type(self) -> ColumnType:
        return self.value.column_type


class _Null:
    """Class representing a literal 'null' value in the expression."""

    column_type: Literal["null"] = "null"


class _RangeLiteral(NamedTuple):
    """Class representing a range expression."""

    value: RangeLiteral
    column_type: Literal["range"] = "range"


class _Sequence(NamedTuple):
    value: list[ColumnExpression]
    column_type: Literal["sequence"] = "sequence"


_VisitorResult: TypeAlias = Predicate | _ColExpr | _Null | _RangeLiteral | _Sequence


class _ConversionVisitor(TreeVisitor[_VisitorResult]):
    def __init__(self, context: IdentifierContext, universe: DimensionUniverse):
        super().__init__()
        self.context = context
        self.universe = universe

    def visitBinaryOp(
        self, operator: str, lhs: _VisitorResult, rhs: _VisitorResult, node: Node
    ) -> _VisitorResult:
        match (operator, lhs, rhs):
            # Handle boolean operators.
            case ["OR", Predicate() as lhs, Predicate() as rhs]:
                return lhs.logical_or(rhs)
            case ["AND", Predicate() as lhs, Predicate() as rhs]:
                return lhs.logical_and(rhs)

            # Handle comparison operators.
            case [("=" | "!=" | "<" | ">" | "<=" | ">="), _ColExpr() as lhs, _ColExpr() as rhs]:
                return Predicate.compare(
                    a=lhs.value, b=rhs.value, operator=_convert_comparison_operator(operator)
                )

            # Allow equality comparisons with None/NULL.  We don't have an 'IS'
            # operator.
            case ["=", _ColExpr() as lhs, _Null()]:
                return Predicate.is_null(lhs.value)
            case ["!=", _ColExpr() as lhs, _Null()]:
                return Predicate.is_null(lhs.value).logical_not()
            case ["=", _Null(), _ColExpr() as rhs]:
                return Predicate.is_null(rhs.value)
            case ["!=", _Null(), _ColExpr() as rhs]:
                return Predicate.is_null(rhs.value).logical_not()

            # Handle arithmetic operations
            case [("+" | "-" | "*" | "/" | "%") as op, _ColExpr() as lhs, _ColExpr() as rhs]:
                return _ColExpr(BinaryExpression(a=lhs.value, b=rhs.value, operator=op))

        raise InvalidQueryError(
            f"Invalid types {lhs.column_type}, {rhs.column_type} for binary operator {operator!r} "
            f"in expression {node!s}."
        )

    def visitIsIn(
        self, lhs: _VisitorResult, values: list[_VisitorResult], not_in: bool, node: Node
    ) -> _VisitorResult:
        raise NotImplementedError("IN not supported yet")

    def visitIdentifier(self, name: str, node: Node) -> _VisitorResult:
        name = name.lower()

        if name in self.context.bind:
            value = self.context.bind[name]
            # Lists of values do not have a direct representation in the new
            # query system, so we have to handle them separately here.
            if isinstance(value, list | tuple | Set):
                literals: list[ColumnExpression] = [make_column_literal(item) for item in value]
                types = set({item.column_type for item in literals})
                if len(types) > 1:
                    raise InvalidQueryError(
                        f"Mismatched types in bind iterable: {value} has a mix of {types}."
                    )
                return _Sequence(literals)

        # The other constants are handled in interpret_identifier().
        if categorizeConstant(name) == ExpressionConstant.NULL:
            return _Null()

        return _ColExpr(interpret_identifier(self.context, name))

    def visitNumericLiteral(self, value: str, node: Node) -> _VisitorResult:
        numeric: int | float
        try:
            numeric = int(value)
        except ValueError:
            # int() raises for float-like strings
            numeric = float(value)
        return _make_literal(numeric)

    def visitParens(self, expression: _VisitorResult, node: Node) -> _VisitorResult:
        return expression

    def visitPointNode(self, ra: _VisitorResult, dec: _VisitorResult, node: Node) -> _VisitorResult:
        raise NotImplementedError("POINT() function is not supported yet")

    def visitRangeLiteral(
        self, start: int, stop: int, stride: int | None, node: RangeLiteral
    ) -> _VisitorResult:
        # Consumed by visitIsIn.
        return _RangeLiteral(node)

    def visitStringLiteral(self, value: str, node: Node) -> _VisitorResult:
        return _make_literal(value)

    def visitTimeLiteral(self, value: astropy.time.Time, node: Node) -> _VisitorResult:
        return _make_literal(value)

    def visitTupleNode(self, items: tuple[_VisitorResult, ...], node: Node) -> _VisitorResult:
        if len(items) != 2:
            raise InvalidQueryError(f"Timespan tuple should have exactly two items (begin, end) in '{node}'")

        begin = _to_timespan_bound(items[0], node)
        end = _to_timespan_bound(items[1], node)
        return _make_literal(Timespan(begin, end))

    def visitUnaryOp(self, operator: str, operand: _VisitorResult, node: Node) -> _VisitorResult:
        # Docstring inherited.
        match (operator, operand):
            case ["NOT", Predicate() as operand]:
                return operand.logical_not()
            case ["+", _ColExpr(column_type="int" | "float") as operand]:
                # + is a no-op.
                return operand
            case ["-", _ColExpr(column_type="int" | "float", value=expr)]:
                return _ColExpr(UnaryExpression(operand=expr, operator="-"))
        raise InvalidQueryError(
            f"Unary operator {operator!r} is not valid for operand of type {operand.column_type} in {node!s}."
        )


def _make_literal(value: LiteralValue) -> _ColExpr:
    return _ColExpr(make_column_literal(value))


def _to_timespan_bound(value: _VisitorResult, node: Node) -> astropy.time.Time | None:
    match (value):
        case _ColExpr(value=expr) if expr.expression_type == "datetime":
            return expr.value
        case _Null():
            return None

    raise InvalidQueryError(
        f'Invalid type in timespan tuple "{node}" '
        '(Note that date/time strings must be preceded by "T" to be recognized).'
    )


def _convert_comparison_operator(value: str) -> ComparisonOperator:
    """Convert an expression-string comparison operator to the format
    used by QueryTree.
    """
    match value:
        case "=":
            return "=="
        case "OVERLAPS":
            return "overlaps"
        case ("!=" | "<" | ">" | "<=" | ">=") as op:
            return op
        case _:
            raise AssertionError(f"Unhandled comparison operator {value}")
