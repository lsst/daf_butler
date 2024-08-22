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

__all__ = ("make_string_expression_predicate", "ExpressionTypeError")

import builtins
import datetime
import types
import warnings
from collections.abc import Mapping, Set
from typing import Any, cast

import astropy.time
import astropy.utils.exceptions
from lsst.daf.relation import (
    ColumnContainer,
    ColumnExpression,
    ColumnExpressionSequence,
    ColumnLiteral,
    ColumnTag,
    Predicate,
    sql,
)

# We import the some modules rather than types within them because match syntax
# uses qualified names with periods to distinguish literals from captures.
from .... import _timespan, timespan_database_representation
from ...._column_tags import DatasetColumnTag, DimensionKeyColumnTag, DimensionRecordColumnTag
from ...._column_type_info import ColumnTypeInfo
from ....dimensions import DataCoordinate, Dimension, DimensionGroup, DimensionUniverse
from ..._exceptions import UserExpressionError, UserExpressionSyntaxError
from .categorize import ExpressionConstant, categorizeConstant, categorizeElementId
from .check import CheckVisitor
from .normalForm import NormalForm, NormalFormExpression
from .parser import Node, TreeVisitor, parse_expression

# As of astropy 4.2, the erfa interface is shipped independently and
# ErfaWarning is no longer an AstropyWarning
try:
    import erfa
except ImportError:
    erfa = None


class ExpressionTypeError(TypeError):
    """Exception raised when the types in a query expression are not
    compatible with the operators or other syntax.
    """


def make_string_expression_predicate(
    string: str,
    dimensions: DimensionGroup,
    *,
    column_types: ColumnTypeInfo,
    bind: Mapping[str, Any] | None = None,
    data_id: DataCoordinate | None = None,
    defaults: DataCoordinate | None = None,
    dataset_type_name: str | None = None,
    allow_orphans: bool = False,
) -> tuple[Predicate | None, Mapping[str, Set[str]]]:
    """Create a predicate by parsing and analyzing a string expression.

    Parameters
    ----------
    string : `str`
        String to parse.
    dimensions : `DimensionGroup`
        The dimensions the query would include in the absence of this WHERE
        expression.
    column_types : `ColumnTypeInfo`
        Information about column types.
    bind : `~collections.abc.Mapping` [ `str`, `Any` ], optional
        Literal values referenced in the expression.
    data_id : `DataCoordinate`, optional
        A fully-expanded data ID identifying dimensions known in advance.
        If not provided, will be set to an empty data ID.
        ``dataId.hasRecords()`` must return `True`.
    defaults : `DataCoordinate`, optional
        A data ID containing default for governor dimensions.  Ignored
        unless ``check=True``.
    dataset_type_name : `str` or `None`, optional
        The name of the dataset type to assume for unqualified dataset
        columns, or `None` if there are no such identifiers.
    allow_orphans : `bool`, optional
        If `True`, permit expressions to refer to dimensions without
        providing a value for their governor dimensions (e.g. referring to
        a visit without an instrument).  Should be left to default to
        `False` in essentially all new code.

    Returns
    -------
    predicate : `lsst.daf.relation.colum_expressions.Predicate` or `None`
        New predicate derived from the string expression, or `None` if the
        string is empty.
    governor_constraints : `~collections.abc.Mapping` [ `str` , \
            `~collections.abc.Set` ]
        Constraints on dimension values derived from the expression and data
        ID.
    """
    governor_constraints: dict[str, Set[str]] = {}
    if data_id is None:
        data_id = DataCoordinate.make_empty(dimensions.universe)
    if not string:
        for dimension in data_id.dimensions.governors:
            governor_constraints[dimension] = {cast(str, data_id[dimension])}
        return None, governor_constraints
    try:
        tree = parse_expression(string)
    except Exception as exc:
        raise UserExpressionSyntaxError(f"Failed to parse user expression {string!r}.") from exc
    assert tree is not None, "Should only happen if the string is empty, and that's handled above."
    if bind is None:
        bind = {}
    if bind:
        for identifier in bind:
            if identifier in dimensions.universe.elements.names:
                raise RuntimeError(f"Bind parameter key {identifier!r} conflicts with a dimension element.")
            table, _, column = identifier.partition(".")
            if column and table in dimensions.universe.elements.names:
                raise RuntimeError(f"Bind parameter key {identifier!r} looks like a dimension column.")
    if defaults is None:
        defaults = DataCoordinate.make_empty(dimensions.universe)
    # Convert the expression to disjunctive normal form (ORs of ANDs).
    # That's potentially super expensive in the general case (where there's
    # a ton of nesting of ANDs and ORs).  That won't be the case for the
    # expressions we expect, and we actually use disjunctive normal instead
    # of conjunctive (i.e.  ANDs of ORs) because I think the worst-case is
    # a long list of OR'd-together data IDs, which is already in or very
    # close to disjunctive normal form.
    expr = NormalFormExpression.fromTree(tree, NormalForm.DISJUNCTIVE)
    # Check the expression for consistency and completeness.
    visitor = CheckVisitor(data_id, dimensions, bind, defaults, allow_orphans=allow_orphans)
    try:
        summary = expr.visit(visitor)
    except UserExpressionError as err:
        exprOriginal = str(tree)
        exprNormal = str(expr.toTree())
        if exprNormal == exprOriginal:
            msg = f'Error in query expression "{exprOriginal}": {err}'
        else:
            msg = f'Error in query expression "{exprOriginal}" (normalized to "{exprNormal}"): {err}'
        raise UserExpressionError(msg) from None
    for dimension_name, values in summary.dimension_constraints.items():
        if dimension_name in dimensions.universe.governor_dimensions.names:
            governor_constraints[dimension_name] = cast(Set[str], values)
    converter = PredicateConversionVisitor(bind, dataset_type_name, dimensions.universe, column_types)
    predicate = tree.visit(converter)
    return predicate, governor_constraints


VisitorResult = Predicate | ColumnExpression | ColumnContainer


class PredicateConversionVisitor(TreeVisitor[VisitorResult]):
    def __init__(
        self,
        bind: Mapping[str, Any],
        dataset_type_name: str | None,
        universe: DimensionUniverse,
        column_types: ColumnTypeInfo,
    ):
        self.bind = bind
        self.dataset_type_name = dataset_type_name
        self.universe = universe
        self.column_types = column_types

    OPERATOR_MAP = {
        "=": "__eq__",
        "!=": "__ne__",
        "<": "__lt__",
        ">": "__gt__",
        "<=": "__le__",
        ">=": "__ge__",
        "+": "__add__",
        "-": "__sub__",
        "*": "__mul__",
    }

    def to_datetime(self, time: astropy.time.Time) -> datetime.datetime:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=astropy.utils.exceptions.AstropyWarning)
            if erfa is not None:
                warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            return time.to_datetime()

    def visitBinaryOp(
        self, operator: str, lhs: VisitorResult, rhs: VisitorResult, node: Node
    ) -> VisitorResult:
        # Docstring inherited.
        b = builtins
        match (operator, lhs, rhs):
            case ["OR", Predicate() as lhs, Predicate() as rhs]:
                return lhs.logical_or(rhs)
            case ["AND", Predicate() as lhs, Predicate() as rhs]:
                return lhs.logical_and(rhs)
            # Allow all comparisons between expressions of the same type for
            # sortable types.
            case [
                "=" | "!=" | "<" | ">" | "<=" | ">=",
                ColumnExpression(
                    dtype=b.int | b.float | b.str | astropy.time.Time | datetime.datetime
                ) as lhs,
                ColumnExpression() as rhs,
            ] if lhs.dtype is rhs.dtype:
                return lhs.predicate_method(self.OPERATOR_MAP[operator], rhs)
            # Allow comparisons between datetime expressions and
            # astropy.time.Time literals/binds (only), by coercing the
            # astropy.time.Time version to datetime.
            case [
                "=" | "!=" | "<" | ">" | "<=" | ">=",
                ColumnLiteral(dtype=astropy.time.Time) as lhs,
                ColumnExpression(dtype=datetime.datetime) as rhs,
            ]:
                lhs = ColumnLiteral(self.to_datetime(lhs.value), datetime.datetime)
                return lhs.predicate_method(self.OPERATOR_MAP[operator], rhs)
            case [
                "=" | "!=" | "<" | ">" | "<=" | ">=",
                ColumnExpression(dtype=datetime.datetime) as lhs,
                ColumnLiteral(dtype=astropy.time.Time) as rhs,
            ]:
                rhs = ColumnLiteral(self.to_datetime(rhs.value), datetime.datetime)
                return lhs.predicate_method(self.OPERATOR_MAP[operator], rhs)
            # Allow comparisons between astropy.time.Time expressions and
            # datetime literals/binds, by coercing the
            # datetime literals to astropy.time.Time (in UTC scale).
            case [
                "=" | "!=" | "<" | ">" | "<=" | ">=",
                ColumnLiteral(dtype=datetime.datetime) as lhs,
                ColumnExpression(dtype=astropy.time.Time) as rhs,
            ]:
                lhs = ColumnLiteral(astropy.time.Time(lhs.value, scale="utc"), astropy.time.Time)
                return lhs.predicate_method(self.OPERATOR_MAP[operator], rhs)
            case [
                "=" | "!=" | "<" | ">" | "<=" | ">=",
                ColumnExpression(dtype=astropy.time.Time) as lhs,
                ColumnLiteral(dtype=datetime.datetime) as rhs,
            ]:
                rhs = ColumnLiteral(astropy.time.Time(rhs.value, scale="utc"), astropy.time.Time)
                return lhs.predicate_method(self.OPERATOR_MAP[operator], rhs)
            # Allow equality comparisons with None/NULL.  We don't have an 'IS'
            # operator.
            case ["=" | "!=", ColumnExpression(dtype=types.NoneType) as lhs, ColumnExpression() as rhs]:
                return lhs.predicate_method(self.OPERATOR_MAP[operator], rhs)
            case ["=" | "!=", ColumnExpression() as lhs, ColumnExpression(dtype=types.NoneType) as rhs]:
                return lhs.predicate_method(self.OPERATOR_MAP[operator], rhs)
            # Comparisions between Time and Timespan need have the Timespan on
            # the lhs, since that (actually TimespanDatabaseRepresentation) is
            # what actually has the methods.
            case [
                "<",
                ColumnExpression(dtype=astropy.time.Time) as lhs,
                ColumnExpression(dtype=_timespan.Timespan) as rhs,
            ]:
                return rhs.predicate_method(self.OPERATOR_MAP[">"], lhs)
            case [
                ">",
                ColumnExpression(dtype=astropy.time.Time) as lhs,
                ColumnExpression(dtype=_timespan.Timespan) as rhs,
            ]:
                return rhs.predicate_method(self.OPERATOR_MAP["<"], lhs)
            # Enable other comparisons between times and Timespans (many of the
            # combinations matched by this branch will have already been
            # covered by a preceding branch).
            case [
                "<" | ">",
                ColumnExpression(dtype=_timespan.Timespan | astropy.time.Time) as lhs,
                ColumnExpression(dtype=_timespan.Timespan | astropy.time.Time) as rhs,
            ]:
                return lhs.predicate_method(self.OPERATOR_MAP[operator], rhs)
            # Enable "overlaps" operations between timespans, and between times
            # and timespans.  The latter resolve to the `Timespan.contains` or
            # `TimespanDatabaseRepresentation.contains` methods, but we use
            # OVERLAPS in the string expression language to keep that simple.
            case [
                "OVERLAPS",
                ColumnExpression(dtype=_timespan.Timespan) as lhs,
                ColumnExpression(dtype=_timespan.Timespan) as rhs,
            ]:
                return lhs.predicate_method("overlaps", rhs)
            case [
                "OVERLAPS",
                ColumnExpression(dtype=_timespan.Timespan) as lhs,
                ColumnExpression(dtype=astropy.time.Time) as rhs,
            ]:
                return lhs.predicate_method("overlaps", rhs)
            case [
                "OVERLAPS",
                ColumnExpression(dtype=astropy.time.Time) as lhs,
                ColumnExpression(dtype=_timespan.Timespan) as rhs,
            ]:
                return rhs.predicate_method("overlaps", lhs)
            # Enable arithmetic operators on numeric types, without any type
            # coercion or broadening.
            case [
                "+" | "-" | "*",
                ColumnExpression(dtype=b.int | b.float) as lhs,
                ColumnExpression() as rhs,
            ] if lhs.dtype is rhs.dtype:
                return lhs.method(self.OPERATOR_MAP[operator], rhs, dtype=lhs.dtype)
            case ["/", ColumnExpression(dtype=b.float) as lhs, ColumnExpression(dtype=b.float) as rhs]:
                return lhs.method("__truediv__", rhs, dtype=b.float)
            case ["/", ColumnExpression(dtype=b.int) as lhs, ColumnExpression(dtype=b.int) as rhs]:
                # SQLAlchemy maps Python's '/' (__truediv__) operator directly
                # to SQL's '/', despite those being defined differently for
                # integers.  Our expression language uses the SQL definition,
                # and we only care about these expressions being evaluated in
                # SQL right now, but we still want to guard against it being
                # evaluated in Python and producing a surprising answer, so we
                # mark it as being supported only by a SQL engine.
                return lhs.method(
                    "__truediv__",
                    rhs,
                    dtype=b.int,
                    supporting_engine_types={sql.Engine},
                )
            case ["%", ColumnExpression(dtype=b.int) as lhs, ColumnExpression(dtype=b.int) as rhs]:
                return lhs.method("__mod__", rhs, dtype=b.int)
        assert (
            lhs.dtype is not None and rhs.dtype is not None
        ), "Expression converter should not yield untyped nodes."
        raise ExpressionTypeError(
            f"Invalid types {lhs.dtype.__name__}, {rhs.dtype.__name__} for binary operator {operator!r} "
            f"in expression {node!s}."
        )

    def visitIdentifier(self, name: str, node: Node) -> VisitorResult:
        # Docstring inherited.
        if name in self.bind:
            value = self.bind[name]
            if isinstance(value, list | tuple | Set):
                elements = []
                all_dtypes = set()
                for item in value:
                    dtype = type(item)
                    all_dtypes.add(dtype)
                    elements.append(ColumnExpression.literal(item, dtype=dtype))
                if len(all_dtypes) > 1:
                    raise ExpressionTypeError(
                        f"Mismatched types in bind iterable: {value} has a mix of {all_dtypes}."
                    )
                elif not elements:
                    # Empty container
                    return ColumnContainer.sequence([])
                else:
                    (dtype,) = all_dtypes
                    return ColumnContainer.sequence(elements, dtype=dtype)
            return ColumnExpression.literal(value, dtype=type(value))
        tag: ColumnTag
        match categorizeConstant(name):
            case ExpressionConstant.INGEST_DATE:
                assert self.dataset_type_name is not None
                tag = DatasetColumnTag(self.dataset_type_name, "ingest_date")
                return ColumnExpression.reference(tag, self.column_types.ingest_date_pytype)
            case ExpressionConstant.NULL:
                return ColumnExpression.literal(None, type(None))
            case None:
                pass
            case _:
                raise AssertionError("Check for enum values should be exhaustive.")
        element, column = categorizeElementId(self.universe, name)
        if column is not None:
            tag = DimensionRecordColumnTag(element.name, column)
            dtype = (
                _timespan.Timespan
                if column == timespan_database_representation.TimespanDatabaseRepresentation.NAME
                else element.RecordClass.fields.standard[column].getPythonType()
            )
            if dtype is bool:
                # ColumnExpression is for non-boolean columns only.  Booleans
                # are represented as Predicate.
                return Predicate.reference(tag)
            else:
                return ColumnExpression.reference(tag, dtype)
        else:
            tag = DimensionKeyColumnTag(element.name)
            assert isinstance(element, Dimension)
            return ColumnExpression.reference(tag, element.primaryKey.getPythonType())

    def visitIsIn(
        self, lhs: VisitorResult, values: list[VisitorResult], not_in: bool, node: Node
    ) -> VisitorResult:
        # Docstring inherited.
        clauses: list[Predicate] = []
        items: list[ColumnExpression] = []
        assert isinstance(lhs, ColumnExpression), "LHS of IN guaranteed to be scalar by parser."
        for rhs_item in values:
            match rhs_item:
                case ColumnExpressionSequence(
                    items=rhs_items, dtype=rhs_dtype
                ) if rhs_dtype is None or rhs_dtype == lhs.dtype:
                    items.extend(rhs_items)
                case ColumnContainer(dtype=lhs.dtype):
                    clauses.append(rhs_item.contains(lhs))
                case ColumnExpression(dtype=lhs.dtype):
                    items.append(rhs_item)
                case _:
                    raise ExpressionTypeError(
                        f"Invalid type {rhs_item.dtype} for element in {lhs.dtype} IN expression '{node}'."
                    )
        if items:
            clauses.append(ColumnContainer.sequence(items, dtype=lhs.dtype).contains(lhs))
        result = Predicate.logical_or(*clauses)
        if not_in:
            result = result.logical_not()
        return result

    def visitNumericLiteral(self, value: str, node: Node) -> VisitorResult:
        # Docstring inherited.
        try:
            return ColumnExpression.literal(int(value), dtype=int)
        except ValueError:
            return ColumnExpression.literal(float(value), dtype=float)

    def visitParens(self, expression: VisitorResult, node: Node) -> VisitorResult:
        # Docstring inherited.
        return expression

    def visitPointNode(self, ra: VisitorResult, dec: VisitorResult, node: Node) -> VisitorResult:
        # Docstring inherited.

        # this is a placeholder for future extension, we enabled syntax but
        # do not support actual use just yet.
        raise NotImplementedError("POINT() function is not supported yet")

    def visitRangeLiteral(self, start: int, stop: int, stride: int | None, node: Node) -> VisitorResult:
        # Docstring inherited.
        return ColumnContainer.range_literal(range(start, stop + 1, stride or 1))

    def visitStringLiteral(self, value: str, node: Node) -> VisitorResult:
        # Docstring inherited.
        return ColumnExpression.literal(value, dtype=str)

    def visitTimeLiteral(self, value: astropy.time.Time, node: Node) -> VisitorResult:
        # Docstring inherited.
        return ColumnExpression.literal(value, dtype=astropy.time.Time)

    def visitTupleNode(self, items: tuple[VisitorResult, ...], node: Node) -> VisitorResult:
        # Docstring inherited.
        match items:
            case [
                ColumnLiteral(value=begin, dtype=astropy.time.Time | types.NoneType),
                ColumnLiteral(value=end, dtype=astropy.time.Time | types.NoneType),
            ]:
                return ColumnExpression.literal(_timespan.Timespan(begin, end), dtype=_timespan.Timespan)
        raise ExpressionTypeError(
            f'Invalid type(s) ({items[0].dtype}, {items[1].dtype}) in timespan tuple "{node}" '
            '(Note that date/time strings must be preceded by "T" to be recognized).'
        )

    def visitUnaryOp(self, operator: str, operand: VisitorResult, node: Node) -> VisitorResult:
        # Docstring inherited.
        match (operator, operand):
            case ["NOT", Predicate() as operand]:
                return operand.logical_not()
            case ["+", ColumnExpression(dtype=builtins.int | builtins.float) as operand]:
                return operand.method("__pos__")
            case ["-", ColumnExpression(dtype=builtins.int | builtins.float) as operand]:
                return operand.method("__neg__")
        raise ExpressionTypeError(
            f"Unary operator {operator!r} is not valid for operand of type {operand.dtype!s} in {node!s}."
        )
