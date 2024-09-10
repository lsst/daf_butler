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

__all__ = ("ExpressionFactory", "ExpressionProxy", "ScalarExpressionProxy", "TimespanProxy", "RegionProxy")

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import TYPE_CHECKING

import astropy.time
from lsst.sphgeom import Region

from .._exceptions import InvalidQueryError
from ..dimensions import Dimension, DimensionElement, DimensionUniverse
from . import tree

if TYPE_CHECKING:
    from .._timespan import Timespan
    from ._query import Query

# This module uses ExpressionProxy and its subclasses to wrap ColumnExpression,
# but it just returns OrderExpression and Predicate objects directly, because
# we don't need to overload any operators or define any methods on those.


class ExpressionProxy(ABC):
    """A wrapper for column expressions that overloads comparison operators
    to return new expression proxies.
    """

    def __repr__(self) -> str:
        return str(self._expression)

    @property
    def is_null(self) -> tree.Predicate:
        """A boolean expression that tests whether this expression is NULL."""
        return tree.Predicate.is_null(self._expression)

    @staticmethod
    def _make_expression(other: object) -> tree.ColumnExpression:
        if isinstance(other, ExpressionProxy):
            return other._expression
        else:
            return tree.make_column_literal(other)

    def _make_comparison(self, other: object, operator: tree.ComparisonOperator) -> tree.Predicate:
        return tree.Predicate.compare(a=self._expression, b=self._make_expression(other), operator=operator)

    @property
    @abstractmethod
    def _expression(self) -> tree.ColumnExpression:
        raise NotImplementedError()


class ScalarExpressionProxy(ExpressionProxy):
    """An `ExpressionProxy` specialized for simple single-value columns."""

    @property
    def desc(self) -> tree.Reversed:
        """An ordering expression that indicates that the sort on this
        expression should be reversed.
        """
        return tree.Reversed(operand=self._expression)

    def as_boolean(self) -> tree.Predicate:
        """If this scalar expression is a boolean, convert it to a `Predicate`
        so it can be used as a boolean expression.

        Raises
        ------
        InvalidQueryError
            If this expression is not a boolean.

        Returns
        -------
        predicate : `Predicate`
            This expression converted to a `Predicate`.
        """
        expr = self._expression
        raise InvalidQueryError(
            f"Expression '{expr}' with type"
            f" '{expr.column_type}' can't be used directly as a boolean value."
            " Use a comparison operator like '>' or '==' instead."
        )

    def __eq__(self, other: object) -> tree.Predicate:  # type: ignore[override]
        return self._make_comparison(other, "==")

    def __ne__(self, other: object) -> tree.Predicate:  # type: ignore[override]
        return self._make_comparison(other, "!=")

    def __lt__(self, other: object) -> tree.Predicate:  # type: ignore[override]
        return self._make_comparison(other, "<")

    def __le__(self, other: object) -> tree.Predicate:  # type: ignore[override]
        return self._make_comparison(other, "<=")

    def __gt__(self, other: object) -> tree.Predicate:  # type: ignore[override]
        return self._make_comparison(other, ">")

    def __ge__(self, other: object) -> tree.Predicate:  # type: ignore[override]
        return self._make_comparison(other, ">=")

    def __neg__(self) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(tree.UnaryExpression(operand=self._expression, operator="-"))

    def __add__(self, other: object) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(
            tree.BinaryExpression(a=self._expression, b=self._make_expression(other), operator="+")
        )

    def __radd__(self, other: object) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(
            tree.BinaryExpression(a=self._make_expression(other), b=self._expression, operator="+")
        )

    def __sub__(self, other: object) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(
            tree.BinaryExpression(a=self._expression, b=self._make_expression(other), operator="-")
        )

    def __rsub__(self, other: object) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(
            tree.BinaryExpression(a=self._make_expression(other), b=self._expression, operator="-")
        )

    def __mul__(self, other: object) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(
            tree.BinaryExpression(a=self._expression, b=self._make_expression(other), operator="*")
        )

    def __rmul__(self, other: object) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(
            tree.BinaryExpression(a=self._make_expression(other), b=self._expression, operator="*")
        )

    def __truediv__(self, other: object) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(
            tree.BinaryExpression(a=self._expression, b=self._make_expression(other), operator="/")
        )

    def __rtruediv__(self, other: object) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(
            tree.BinaryExpression(a=self._make_expression(other), b=self._expression, operator="/")
        )

    def __mod__(self, other: object) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(
            tree.BinaryExpression(a=self._expression, b=self._make_expression(other), operator="%")
        )

    def __rmod__(self, other: object) -> ScalarExpressionProxy:
        return ResolvedScalarExpressionProxy(
            tree.BinaryExpression(a=self._make_expression(other), b=self._expression, operator="%")
        )

    def in_range(self, start: int = 0, stop: int | None = None, step: int = 1) -> tree.Predicate:
        """Return a boolean expression that tests whether this expression is
        within a literal integer range.

        Parameters
        ----------
        start : `int`, optional
            Lower bound (inclusive) for the slice.
        stop : `int` or `None`, optional
            Upper bound (exclusive) for the slice, or `None` for no bound.
        step : `int`, optional
            Spacing between integers in the range.

        Returns
        -------
        predicate : `tree.Predicate`
            Boolean expression object.
        """
        return tree.Predicate.in_range(self._expression, start=start, stop=stop, step=step)

    def in_iterable(self, others: Iterable) -> tree.Predicate:
        """Return a boolean expression that tests whether this expression
        evaluates to a value that is in an iterable of other expressions.

        Parameters
        ----------
        others : `collections.abc.Iterable`
            An iterable of `ExpressionProxy` or values to be interpreted as
            literals.

        Returns
        -------
        predicate : `tree.Predicate`
            Boolean expression object.
        """
        return tree.Predicate.in_container(self._expression, [self._make_expression(item) for item in others])

    def in_query(self, column: ExpressionProxy, query: Query) -> tree.Predicate:
        """Return a boolean expression that test whether this expression
        evaluates to a value that is in a single-column selection from another
        query.

        Parameters
        ----------
        column : `ExpressionProxy`
            Proxy for the column to extract from ``query``.
        query : `Query`
            Query to select from.

        Returns
        -------
        predicate : `tree.Predicate`
            Boolean expression object.
        """
        return tree.Predicate.in_query(self._expression, column._expression, query._tree)


class ResolvedScalarExpressionProxy(ScalarExpressionProxy):
    """A `ScalarExpressionProxy` backed by an actual expression.

    Parameters
    ----------
    expression : `.tree.ColumnExpression`
        Expression that backs this proxy.
    """

    def __init__(self, expression: tree.ColumnExpression):
        self._expr = expression

    @property
    def _expression(self) -> tree.ColumnExpression:
        return self._expr


class BooleanScalarExpressionProxy(ScalarExpressionProxy):
    """A `ScalarExpressionProxy` representing a boolean column.  You should
    call `as_boolean()` on this object to convert it to an instance of
    `Predicate` before attempting to use it.

    Parameters
    ----------
    expression : `.tree.ColumnReference`
        Boolean column reference that backs this proxy.
    """

    # This is a hack/work-around to make static typing work when referencing
    # dimension record metadata boolean columns.  From the perspective of
    # typing, anything boolean should be a `Predicate`, but the type system has
    # no way of knowing whether a given column is a bool or some other type.

    def __init__(self, expression: tree.ColumnReference) -> None:
        if expression.column_type != "bool":
            raise ValueError(f"Expression is a {expression.column_type}, not a 'bool': {expression}")
        self._boolean_expression = expression

    @property
    def is_null(self) -> tree.Predicate:
        return ResolvedScalarExpressionProxy(self._boolean_expression).is_null

    def as_boolean(self) -> tree.Predicate:
        return tree.Predicate.from_bool_expression(self._boolean_expression)

    @property
    def _expression(self) -> tree.ColumnExpression:
        raise InvalidQueryError(
            f"Boolean expression '{self._boolean_expression}' can't be used directly in other expressions."
            " Call the 'as_boolean()' method to convert it to a Predicate instead."
        )


class TimespanProxy(ExpressionProxy):
    """An `ExpressionProxy` specialized for timespan columns and literals.

    Parameters
    ----------
    expression : `.tree.ColumnExpression`
        Expression that backs this proxy.
    """

    def __init__(self, expression: tree.ColumnExpression):
        self._expr = expression

    @property
    def begin(self) -> ScalarExpressionProxy:
        """An expression representing the lower bound (inclusive)."""
        return ResolvedScalarExpressionProxy(
            tree.UnaryExpression(operand=self._expression, operator="begin_of")
        )

    @property
    def end(self) -> ScalarExpressionProxy:
        """An expression representing the upper bound (exclusive)."""
        return ResolvedScalarExpressionProxy(
            tree.UnaryExpression(operand=self._expression, operator="end_of")
        )

    def overlaps(self, other: TimespanProxy | Timespan | astropy.time.Time) -> tree.Predicate:
        """Return a boolean expression representing an overlap test between
        this timespan and another timespan or a datetime.

        Parameters
        ----------
        other : `TimespanProxy` or `Timespan`
            Expression or literal to compare to.

        Returns
        -------
        predicate : `tree.Predicate`
            Boolean expression object.
        """
        return self._make_comparison(other, "overlaps")

    @property
    def _expression(self) -> tree.ColumnExpression:
        return self._expr


class RegionProxy(ExpressionProxy):
    """An `ExpressionProxy` specialized for region columns and literals.

    Parameters
    ----------
    expression : `.tree.ColumnExpression`
        Expression that backs this proxy.
    """

    def __init__(self, expression: tree.ColumnExpression):
        self._expr = expression

    def overlaps(self, other: RegionProxy | Region) -> tree.Predicate:
        """Return a boolean expression representing an overlap test between
        this region and another.

        Parameters
        ----------
        other : `RegionProxy` or `Region`
            Expression or literal to compare to.

        Returns
        -------
        predicate : `tree.Predicate`
            Boolean expression object.
        """
        return self._make_comparison(other, "overlaps")

    @property
    def _expression(self) -> tree.ColumnExpression:
        return self._expr


class DimensionElementProxy(ScalarExpressionProxy):
    """An expression-creation proxy for a dimension element logical table.

    Parameters
    ----------
    element : `DimensionElement`
        Element this object wraps.

    Notes
    -----
    The (dynamic) attributes of this object are expression proxies for the
    non-dimension fields of the element's records.
    """

    def __init__(self, element: DimensionElement):
        self._element = element

    @property
    def _expression(self) -> tree.ColumnExpression:
        if isinstance(self._element, Dimension):
            return tree.DimensionKeyReference(dimension=self._element)
        else:
            raise TypeError(f"Proxy expression {self!r} is does not resolve to a column.")

    def __repr__(self) -> str:
        return self._element.name

    def __getattr__(self, field: str) -> ScalarExpressionProxy:
        if field in self._element.schema.dimensions.names:
            if field not in self._element.dimensions.names:
                # This is a dimension self-reference, like visit.id.
                return self
            return DimensionElementProxy(self._element.dimensions[field])
        try:
            expression = tree.DimensionFieldReference(element=self._element, field=field)
        except InvalidQueryError:
            raise AttributeError(field) from None
        if expression.column_type == "bool":
            return BooleanScalarExpressionProxy(expression)
        else:
            return ResolvedScalarExpressionProxy(expression)

    @property
    def region(self) -> RegionProxy:
        try:
            expression = tree.DimensionFieldReference(element=self._element, field="region")
        except InvalidQueryError:
            raise AttributeError("region")
        return RegionProxy(expression)

    @property
    def timespan(self) -> TimespanProxy:
        try:
            expression = tree.DimensionFieldReference(element=self._element, field="timespan")
        except InvalidQueryError:
            raise AttributeError("timespan") from None
        return TimespanProxy(expression)

    def __dir__(self) -> list[str]:
        # We only want timespan and region to appear in dir() for elements that
        # have them, but we can't implement them in getattr without muddling
        # the type annotations.
        result = [entry for entry in super().__dir__() if entry != "timespan" and entry != "region"]
        result.extend(self._element.schema.names)
        return result


class DatasetTypeProxy:
    """An expression-creation proxy for a dataset type's logical table.

    Parameters
    ----------
    dataset_type : `str`
        Dataset type name or wildcard.  Wildcards are usable only when the
        query contains exactly one dataset type or a wildcard.

    Notes
    -----
    The attributes of this object are expression proxies for the fields
    associated with datasets.
    """

    def __init__(self, dataset_type: str):
        self._dataset_type = dataset_type

    def __repr__(self) -> str:
        return self._dataset_type

    # Attributes are actually fixed, but we implement them with __getattr__
    # and __dir__ to avoid repeating the list.  And someday they might expand
    # to include Datastore record fields.

    def __getattr__(self, field: str) -> ScalarExpressionProxy:
        if not tree.is_dataset_field(field):
            raise AttributeError(field)
        expression = tree.DatasetFieldReference(dataset_type=self._dataset_type, field=field)
        return ResolvedScalarExpressionProxy(expression)

    @property
    def timespan(self) -> TimespanProxy:
        try:
            expression = tree.DatasetFieldReference(dataset_type=self._dataset_type, field="timespan")
        except InvalidQueryError:
            raise AttributeError("timespan") from None
        return TimespanProxy(expression)

    def __dir__(self) -> list[str]:
        result = list(super().__dir__())
        # "timespan" will be added by delegation to super() and we don't want
        # it to appear twice.
        result.extend(name for name in tree.DATASET_FIELD_NAMES if name != "timespan")
        return result


class ExpressionFactory:
    """A factory for creating column expressions that uses operator overloading
    to form a mini-language.

    Instances of this class are usually obtained from
    `Query.expression_factory`; see that property's documentation for more
    information.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Object that describes all dimensions.
    """

    def __init__(self, universe: DimensionUniverse):
        self._universe = universe

    def __getattr__(self, name: str) -> DimensionElementProxy:
        try:
            element = self._universe.elements[name]
        except KeyError:
            raise AttributeError(name)
        return DimensionElementProxy(element)

    def __getitem__(self, name: str) -> DatasetTypeProxy:
        return DatasetTypeProxy(name)

    def not_(self, operand: tree.Predicate) -> tree.Predicate:
        """Apply a logical NOT operation to a boolean expression.

        Parameters
        ----------
        operand : `tree.Predicate`
            Expression to invetree.

        Returns
        -------
        logical_not : `tree.Predicate`
            A boolean expression that evaluates to the opposite of ``operand``.
        """
        return operand.logical_not()

    def all(self, first: tree.Predicate, /, *args: tree.Predicate) -> tree.Predicate:
        """Combine a sequence of boolean expressions with logical AND.

        Parameters
        ----------
        first : `tree.Predicate`
            First operand (required).
        *args
            Additional operands.

        Returns
        -------
        logical_and : `tree.Predicate`
            A boolean expression that evaluates to `True` only if all operands
            evaluate to `True.
        """
        return first.logical_and(*args)

    def any(self, first: tree.Predicate, /, *args: tree.Predicate) -> tree.Predicate:
        """Combine a sequence of boolean expressions with logical OR.

        Parameters
        ----------
        first : `tree.Predicate`
            First operand (required).
        *args
            Additional operands.

        Returns
        -------
        logical_or : `tree.Predicate`
            A boolean expression that evaluates to `True` if any operand
            evaluates to `True.
        """
        return first.logical_or(*args)

    @staticmethod
    def literal(value: object) -> ExpressionProxy:
        """Return an expression proxy that represents a literal value.

        Expression proxy objects obtained from this factory can generally be
        compared directly to literals, so calling this method directly in user
        code should rarely be necessary.

        Parameters
        ----------
        value : `object`
            Value to include as a literal in an expression tree.

        Returns
        -------
        expression : `ExpressionProxy`
            Expression wrapper for this literal.
        """
        expression = tree.make_column_literal(value)
        match expression.expression_type:
            case "timespan":
                return TimespanProxy(expression)
            case "region":
                return RegionProxy(expression)
            case "bool":
                raise NotImplementedError("Boolean literals are not supported.")
            case _:
                return ResolvedScalarExpressionProxy(expression)

    @staticmethod
    def unwrap(proxy: ExpressionProxy) -> tree.ColumnExpression:
        """Return the column expression object that backs a proxy.

        Parameters
        ----------
        proxy : `ExpressionProxy`
            Proxy constructed via an `ExpressionFactory`.

        Returns
        -------
        expression : `tree.ColumnExpression`
            Underlying column expression object.
        """
        return proxy._expression
