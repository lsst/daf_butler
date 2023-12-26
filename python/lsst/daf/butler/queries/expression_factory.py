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

from collections.abc import Iterable
from types import EllipsisType
from typing import TYPE_CHECKING, get_args

from lsst.sphgeom import Region

from ..dimensions import DimensionElement, DimensionUniverse
from . import relation_tree as rt

if TYPE_CHECKING:
    from .._timespan import Timespan
    from ._query import RelationQuery

# This module uses ExpressionProxy and its subclasses to wrap ColumnExpression,
# but it just returns OrderExpression and Predicate objects directly, because
# we don't need to overload any operators or define any methods on those.


class ExpressionProxy:
    """A wrapper for column expressions that overloads comparison operators
    to return new expression proxies.

    Parameters
    ----------
    expression : `relation_tree.ColumnExpression`
        Underlying expression object.
    """

    def __init__(self, expression: rt.ColumnExpression):
        self._expression = expression

    def __repr__(self) -> str:
        return str(self._expression)

    @property
    def is_null(self) -> rt.Predicate:
        """A boolean expression that tests whether this expression is NULL."""
        return rt.IsNull.model_construct(operand=self._expression)

    @staticmethod
    def _make_expression(other: object) -> rt.ColumnExpression:
        if isinstance(other, ExpressionProxy):
            return other._expression
        else:
            return rt.make_column_literal(other)

    def _make_comparison(self, other: object, operator: rt.ComparisonOperator) -> rt.Predicate:
        return rt.Comparison.model_construct(
            a=self._expression, b=self._make_expression(other), operator=operator
        )


class ScalarExpressionProxy(ExpressionProxy):
    """An `ExpressionProxy` specialized for simple single-value columns."""

    @property
    def desc(self) -> rt.Reversed:
        """An ordering expression that indicates that the sort on this
        expression should be reversed.
        """
        return rt.Reversed.model_construct(operand=self._expression)

    def __eq__(self, other: object) -> rt.Predicate:  # type: ignore[override]
        return self._make_comparison(other, "==")

    def __ne__(self, other: object) -> rt.Predicate:  # type: ignore[override]
        return self._make_comparison(other, "!=")

    def __lt__(self, other: object) -> rt.Predicate:  # type: ignore[override]
        return self._make_comparison(other, "<")

    def __le__(self, other: object) -> rt.Predicate:  # type: ignore[override]
        return self._make_comparison(other, "<=")

    def __gt__(self, other: object) -> rt.Predicate:  # type: ignore[override]
        return self._make_comparison(other, ">")

    def __ge__(self, other: object) -> rt.Predicate:  # type: ignore[override]
        return self._make_comparison(other, ">=")

    def in_range(self, start: int = 0, stop: int | None = None, step: int = 1) -> rt.Predicate:
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
        predicate : `relation_tree.Predicate`
            Boolean expression object.
        """
        return rt.InRange.model_construct(member=self._expression, start=start, stop=stop, step=step)

    def in_iterable(self, others: Iterable) -> rt.Predicate:
        """Return a boolean expression that tests whether this expression
        evaluates to a value that is in an iterable of other expressions.

        Parameters
        ----------
        others : `collections.abc.Iterable`
            An iterable of `ExpressionProxy` or values to be interpreted as
            literals.

        Returns
        -------
        predicate : `relation_tree.Predicate`
            Boolean expression object.
        """
        return rt.InContainer.model_construct(
            member=self._expression, container=tuple([self._make_expression(item) for item in others])
        )

    def in_query(self, column: ExpressionProxy, query: RelationQuery) -> rt.Predicate:
        """Return a boolean expression that test whether this expression
        evaluates to a value that is in a single-column selection from another
        query.

        Parameters
        ----------
        column : `ExpressionProxy`
            Proxy for the column to extract from ``query``.
        query : `RelationQuery`
            Query to select from.

        Returns
        -------
        predicate : `relation_tree.Predicate`
            Boolean expression object.
        """
        return rt.InRelation.model_construct(
            member=self._expression, column=column._expression, relation=query._tree
        )


class TimespanProxy(ExpressionProxy):
    """An `ExpressionProxy` specialized for timespan columns and literals."""

    @property
    def begin(self) -> ExpressionProxy:
        """An expression representing the lower bound (inclusive)."""
        return ExpressionProxy(
            rt.UnaryExpression.model_construct(operand=self._expression, operator="begin_of")
        )

    @property
    def end(self) -> ExpressionProxy:
        """An expression representing the upper bound (exclusive)."""
        return ExpressionProxy(
            rt.UnaryExpression.model_construct(operand=self._expression, operator="end_of")
        )

    def overlaps(self, other: TimespanProxy | Timespan) -> rt.Predicate:
        """Return a boolean expression representing an overlap test between
        this timespan and another.

        Parameters
        ----------
        other : `TimespanProxy` or `Timespan`
            Expression or literal to compare to.

        Returns
        -------
        predicate : `relation_tree.Predicate`
            Boolean expression object.
        """
        return self._make_comparison(other, "overlaps")


class RegionProxy(ExpressionProxy):
    """An `ExpressionProxy` specialized for region columns and literals."""

    def overlaps(self, other: RegionProxy | Region) -> rt.Predicate:
        """Return a boolean expression representing an overlap test between
        this region and another.

        Parameters
        ----------
        other : `RegionProxy` or `Region`
            Expression or literal to compare to.

        Returns
        -------
        predicate : `relation_tree.Predicate`
            Boolean expression object.
        """
        return self._make_comparison(other, "overlaps")


class DimensionElementProxy:
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

    def __repr__(self) -> str:
        return self._element.name

    def __getattr__(self, field: str) -> ExpressionProxy:
        expression = rt.DimensionFieldReference(element=self._element.name, field=field)
        match field:
            case "region":
                return RegionProxy(expression)
            case "timespan":
                return TimespanProxy(expression)
        return ScalarExpressionProxy(expression)

    def __dir__(self) -> list[str]:
        result = list(super().__dir__())
        result.extend(self._element.RecordClass.fields.facts.names)
        if self._element.spatial:
            result.append("region")
        if self._element.temporal:
            result.append("temporal")
        return result


class DimensionProxy(ScalarExpressionProxy, DimensionElementProxy):
    """An expression-creation proxy for a dimension logical table.

    Parameters
    ----------
    dimension : `DimensionElement`
        Element this object wraps.

    Notes
    -----
    This class combines record-field attribute access from `DimensionElement`
    proxy with direct interpretation as a dimension key column via
    `ScalarExpressionProxy`.  For example::

        x = query.expression_factory
        query.where(
            x.detector.purpose == "SCIENCE",  # field access
            x.detector > 100,  # direct usage as an expression
        )
    """

    def __init__(self, dimension: DimensionElement):
        ScalarExpressionProxy.__init__(self, rt.DimensionKeyReference(dimension=dimension.name))
        DimensionElementProxy.__init__(self, dimension)


class DatasetTypeProxy:
    """An expression-creation proxy for a dataset type's logical table.

    Parameters
    ----------
    dataset_type : `str` or ``...``
        Dataset type name or wildcard.  Wildcards are usable only when the
        query contains exactly one dataset type or a wildcard.

    Notes
    -----
    The attributes of this object are expression proxies for the fields
    associated with datasets rather than their dimensions.
    """

    def __init__(self, dataset_type: rt.StringOrWildcard):
        self._dataset_type = dataset_type

    def __repr__(self) -> str:
        return self._dataset_type if self._dataset_type is not ... else "(...)"

    # Attributes are actually fixed, but we implement them with __getattr__
    # and __dir__ to avoid repeating the list.  And someday they might expand
    # to include Datastore record fields.

    def __getattr__(self, field: str) -> ExpressionProxy:
        if field not in get_args(rt.DatasetFieldName):
            raise AttributeError(field)
        expression = rt.DatasetFieldReference(dataset_type=self._dataset_type, field=field)
        if field == "timespan":
            return TimespanProxy(expression)
        return ScalarExpressionProxy(expression)

    def __dir__(self) -> list[str]:
        result = list(super().__dir__())
        result.extend(get_args(rt.DatasetFieldName))
        return result


class ExpressionFactory:
    """A factory for creating column expressions that uses operator overloading
    to form a mini-language.

    Instances of this class are usually obtained from
    `RelationQuery.expression_factory`; see that property's documentation for
    more information.

    Parameters
    ----------
    universe : `DimensionUniverse`
        Object that describes all dimensions.
    """

    def __init__(self, universe: DimensionUniverse):
        self._universe = universe

    def __getattr__(self, name: str) -> DimensionElementProxy:
        element = self._universe.elements[name]
        if element in self._universe.dimensions:
            return DimensionProxy(element)
        return DimensionElementProxy(element)

    def __getitem__(self, name: str | EllipsisType) -> DatasetTypeProxy:
        return DatasetTypeProxy(name)

    def not_(self, operand: rt.Predicate) -> rt.Predicate:
        """Apply a logical NOT operation to a boolean expression.

        Parameters
        ----------
        operand : `relation_tree.Predicate`
            Expression to invert.

        Returns
        -------
        logical_not : `relation_tree.Predicate`
            A boolean expression that evaluates to the opposite of ``operand``.
        """
        return operand.logical_not()

    def all(self, first: rt.Predicate, /, *args: rt.Predicate) -> rt.Predicate:
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

    def any(self, first: rt.Predicate, /, *args: rt.Predicate) -> rt.Predicate:
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
        expression = rt.make_column_literal(value)
        match expression.expression_type:
            case "timespan":
                return TimespanProxy(expression)
            case "region":
                return RegionProxy(expression)
            case _:
                return ScalarExpressionProxy(expression)
