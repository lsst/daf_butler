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
import lsst.sphgeom

from .._exceptions import InvalidQueryError
from .._timespan import Timespan
from ..column_spec import ColumnType
from ..dimensions import DimensionUniverse
from ..registry.queries.expressions.categorize import ExpressionConstant, categorizeConstant
from ..registry.queries.expressions.parser import Node, PointNode, RangeLiteral, TreeVisitor, parse_expression
from ._identifiers import IdentifierContext, interpret_identifier
from .tree import (
    BinaryExpression,
    ColumnExpression,
    ColumnReference,
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
    if tree is None:
        return Predicate.from_bool(True)
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
            case [("=" | "!=" | "<" | ">" | "<=" | ">=" | "OVERLAPS"), _ColExpr() as lhs, _ColExpr() as rhs]:
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
            # Boolean columns can be null, but will have been converted to
            # Predicate, so we need additional cases.
            case ["=" | "!=", Predicate() as pred, _Null()] | ["=" | "!=", _Null(), Predicate() as pred]:
                column_ref = _get_boolean_column_reference(pred)
                if column_ref is not None:
                    match operator:
                        case "=":
                            return Predicate.is_null(column_ref)
                        case "!=":
                            return Predicate.is_null(column_ref).logical_not()

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
        assert isinstance(lhs, _ColExpr), "LHS of IN guaranteed to be scalar by parser."
        predicates = [_convert_in_clause_to_predicate(lhs.value, rhs, node) for rhs in values]
        result = Predicate.from_bool(False).logical_or(*predicates)
        if not_in:
            result = result.logical_not()
        return result

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

        column_expression = interpret_identifier(self.context, name)
        if column_expression.column_type == "bool":
            # Expression-handling code (in this file and elsewhere) expects
            # boolean-valued expressions to be represented as Predicate, not a
            # ColumnExpression.

            # We should only be getting direct references to a column, not a
            # more complicated expression.
            # (Anything more complicated should be a Predicate already.)
            assert (
                column_expression.expression_type == "dataset_field"
                or column_expression.expression_type == "dimension_field"
                or column_expression.expression_type == "dimension_key"
            )
            return Predicate.from_bool_expression(column_expression)
        else:
            return _ColExpr(column_expression)

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

    def visitPointNode(self, ra: _VisitorResult, dec: _VisitorResult, node: PointNode) -> _VisitorResult:
        ra_value = _get_float_literal_value(ra, node.ra)
        dec_value = _get_float_literal_value(dec, node.dec)

        lon_lat = lsst.sphgeom.LonLat.fromDegrees(ra_value, dec_value)
        return _make_literal(lon_lat)

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


def _convert_in_clause_to_predicate(lhs: ColumnExpression, rhs: _VisitorResult, node: Node) -> Predicate:
    """Convert ``lhs IN rhs`` expression to an equivalent ``Predicate``
    value.
    """
    match rhs:
        case _Sequence():
            return Predicate.in_container(lhs, rhs.value)
        case _RangeLiteral():
            stride = rhs.value.stride
            if stride is None:
                stride = 1
            # Expression strings use inclusive ranges, but Predicate uses
            # ranges that exclude the stop value.
            stop = rhs.value.stop + 1
            return Predicate.in_range(lhs, rhs.value.start, stop, stride)
        case _ColExpr():
            return Predicate.compare(lhs, "==", rhs.value)
        case _Null():
            return Predicate.is_null(lhs)
        case _:
            raise InvalidQueryError(f"Invalid IN expression: '{node!s}")


def _get_boolean_column_reference(predicate: Predicate) -> ColumnReference | None:
    """Unwrap a predicate to recover the boolean ColumnReference it contains.
    Returns `None` if this Predicate contains anything other than a single
    boolean ColumnReference operand.

    This undoes the ColumnReference to Predicate conversion that occurs in
    visitIdentifier for boolean columns.
    """
    if len(predicate.operands) == 1 and len(predicate.operands[0]) == 1:
        predicate_leaf = predicate.operands[0][0]
        if predicate_leaf.predicate_type == "boolean_wrapper":
            return predicate_leaf.operand

    return None


def _get_float_literal_value(value: _VisitorResult, node: Node) -> float:
    """If the given ``value`` is a literal `float` or `int` expression, return
    it as a float.  Otherwise raise an `InvalidQueryError` identifying a
    problem with the given ``node``.
    """
    if isinstance(value, _ColExpr):
        expr = value.value
        if expr.expression_type == "float":
            return expr.value
        elif expr.expression_type == "int":
            return float(expr.value)
        elif expr.expression_type == "unary" and expr.operator == "-":
            return -1 * _get_float_literal_value(_ColExpr(expr.operand), node)

    raise InvalidQueryError(f"Expression '{node}' in POINT() is not a literal number.")
