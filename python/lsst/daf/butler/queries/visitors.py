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
    "ColumnExpressionVisitor",
    "PredicateVisitFlags",
    "PredicateVisitor",
    "SimplePredicateVisitor",
)

import enum
from abc import abstractmethod
from typing import Generic, TypeVar, final

from . import tree

_T = TypeVar("_T")
_L = TypeVar("_L")
_A = TypeVar("_A")
_O = TypeVar("_O")


class PredicateVisitFlags(enum.Flag):
    """Flags that provide information about the location of a predicate term
    in the larger tree.
    """

    HAS_AND_SIBLINGS = enum.auto()
    HAS_OR_SIBLINGS = enum.auto()
    INVERTED = enum.auto()


class ColumnExpressionVisitor(Generic[_T]):
    """A visitor interface for traversing a `ColumnExpression` tree.

    Notes
    -----
    Unlike `Predicate`, the concrete column expression types need to be
    public for various reasons, and hence the visitor interface uses them
    directly in its arguments.

    This interface includes `Reversed` (which is part of the `OrderExpression`
    union but not the `ColumnExpression` union) because it is simpler to have
    just one visitor interface and disable support for it at runtime as
    appropriate.
    """

    @abstractmethod
    def visit_literal(self, expression: tree.ColumnLiteral) -> _T:
        """Visit a column expression that wraps a literal value.

        Parameters
        ----------
        expression : `tree.ColumnLiteral`
            Expression to visit.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_dimension_key_reference(self, expression: tree.DimensionKeyReference) -> _T:
        """Visit a column expression that represents a dimension column.

        Parameters
        ----------
        expression : `tree.DimensionKeyReference`
            Expression to visit.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_dimension_field_reference(self, expression: tree.DimensionFieldReference) -> _T:
        """Visit a column expression that represents a dimension record field.

        Parameters
        ----------
        expression : `tree.DimensionFieldReference`
            Expression to visit.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_dataset_field_reference(self, expression: tree.DatasetFieldReference) -> _T:
        """Visit a column expression that represents a dataset field.

        Parameters
        ----------
        expression : `tree.DatasetFieldReference`
            Expression to visit.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_unary_expression(self, expression: tree.UnaryExpression) -> _T:
        """Visit a column expression that represents a unary operation.

        Parameters
        ----------
        expression : `tree.UnaryExpression`
            Expression to visit.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_binary_expression(self, expression: tree.BinaryExpression) -> _T:
        """Visit a column expression that wraps a binary operation.

        Parameters
        ----------
        expression : `tree.BinaryExpression`
            Expression to visit.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_reversed(self, expression: tree.Reversed) -> _T:
        """Visit a column expression that switches sort order from ascending
        to descending.

        Parameters
        ----------
        expression : `tree.Reversed`
            Expression to visit.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()


class PredicateVisitor(Generic[_A, _O, _L]):
    """A visitor interface for traversing a `Predicate`.

    Notes
    -----
    The concrete `PredicateLeaf` types are only semi-public (they appear in
    the serialized form of a `Predicate`, but their types should not generally
    be referenced directly outside of the module in which they are defined).
    As a result, visiting these objects unpacks their attributes into the
    visit method arguments.
    """

    @abstractmethod
    def visit_boolean_wrapper(self, value: tree.ColumnExpression, flags: PredicateVisitFlags) -> _L:
        """Visit a boolean-valued column expression.

        Parameters
        ----------
        value : `tree.ColumnExpression`
            Column expression, guaranteed to have `column_type == "bool"`.
        flags : `PredicateVisitFlags`
            Information about where this leaf appears in the larger predicate
            tree.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_comparison(
        self,
        a: tree.ColumnExpression,
        operator: tree.ComparisonOperator,
        b: tree.ColumnExpression,
        flags: PredicateVisitFlags,
    ) -> _L:
        """Visit a binary comparison between column expressions.

        Parameters
        ----------
        a : `tree.ColumnExpression`
            First column expression in the comparison.
        operator : `str`
            Enumerated string representing the comparison operator to apply.
            May be and of "==", "!=", "<", ">", "<=", ">=", or "overlaps".
        b : `tree.ColumnExpression`
            Second column expression in the comparison.
        flags : `PredicateVisitFlags`
            Information about where this leaf appears in the larger predicate
            tree.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_is_null(self, operand: tree.ColumnExpression, flags: PredicateVisitFlags) -> _L:
        """Visit a predicate leaf that tests whether a column expression is
        NULL.

        Parameters
        ----------
        operand : `tree.ColumnExpression`
            Column expression to test.
        flags : `PredicateVisitFlags`
            Information about where this leaf appears in the larger predicate
            tree.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_in_container(
        self,
        member: tree.ColumnExpression,
        container: tuple[tree.ColumnExpression, ...],
        flags: PredicateVisitFlags,
    ) -> _L:
        """Visit a predicate leaf that tests whether a column expression is
        a member of a container.

        Parameters
        ----------
        member : `tree.ColumnExpression`
            Column expression that may be a member of the container.
        container : `~collections.abc.Iterable` [ `tree.ColumnExpression` ]
            Container of column expressions to test for membership in.
        flags : `PredicateVisitFlags`
            Information about where this leaf appears in the larger predicate
            tree.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_in_range(
        self,
        member: tree.ColumnExpression,
        start: int,
        stop: int | None,
        step: int,
        flags: PredicateVisitFlags,
    ) -> _L:
        """Visit a predicate leaf that tests whether a column expression is
        a member of an integer range.

        Parameters
        ----------
        member : `tree.ColumnExpression`
            Column expression that may be a member of the range.
        start : `int`, optional
            Beginning of the range, inclusive.
        stop : `int` or `None`, optional
            End of the range, exclusive.
        step : `int`, optional
            Offset between values in the range.
        flags : `PredicateVisitFlags`
            Information about where this leaf appears in the larger predicate
            tree.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def visit_in_query_tree(
        self,
        member: tree.ColumnExpression,
        column: tree.ColumnExpression,
        query_tree: tree.QueryTree,
        flags: PredicateVisitFlags,
    ) -> _L:
        """Visit a predicate leaf that tests whether a column expression is
        a member of a container.

        Parameters
        ----------
        member : `tree.ColumnExpression`
            Column expression that may be present in the query.
        column : `tree.ColumnExpression`
            Column to project from the query.
        query_tree : `QueryTree`
            Query tree to select from.
        flags : `PredicateVisitFlags`
            Information about where this leaf appears in the larger predicate
            tree.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_logical_not(self, original: tree.PredicateLeaf, result: _L, flags: PredicateVisitFlags) -> _L:
        """Apply a logical NOT to the result of visiting an inverted predicate
        leaf.

        Parameters
        ----------
        original : `PredicateLeaf`
            The original operand of the logical NOT operation.
        result : `object`
            Implementation-defined result of visiting the operand.
        flags : `PredicateVisitFlags`
            Information about where this leaf appears in the larger predicate
            tree.  Never has `PredicateVisitFlags.INVERTED` set.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_logical_or(
        self,
        originals: tuple[tree.PredicateLeaf, ...],
        results: tuple[_L, ...],
        flags: PredicateVisitFlags,
    ) -> _O:
        """Apply a logical OR operation to the result of visiting a `tuple` of
        predicate leaf objects.

        Parameters
        ----------
        originals : `tuple` [ `PredicateLeaf`, ... ]
            Original leaf objects in the logical OR.
        results : `tuple` [ `object`, ... ]
            Result of visiting the leaf objects.
        flags : `PredicateVisitFlags`
            Information about where this leaf appears in the larger predicate
            tree.  Never has `PredicateVisitFlags.INVERTED` or
            `PredicateVisitFlags.HAS_OR_SIBLINGS` set.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_logical_and(self, originals: tree.PredicateOperands, results: tuple[_O, ...]) -> _A:
        """Apply a logical AND operation to the result of visiting a nested
        `tuple` of predicate leaf objects.

        Parameters
        ----------
        originals : `tuple` [ `tuple` [ `PredicateLeaf`, ... ], ... ]
            Nested tuple of predicate leaf objects, with inner tuples
            corresponding to groups that should be combined with logical OR.
        results : `tuple` [ `object`, ... ]
            Result of visiting the leaf objects.

        Returns
        -------
        result : `object`
            Implementation-defined.
        """
        raise NotImplementedError()

    @final
    def _visit_logical_not(self, operand: tree.LogicalNotOperand, flags: PredicateVisitFlags) -> _L:
        return self.apply_logical_not(
            operand, operand.visit(self, flags | PredicateVisitFlags.INVERTED), flags
        )

    @final
    def _visit_logical_or(self, operands: tuple[tree.PredicateLeaf, ...], flags: PredicateVisitFlags) -> _O:
        nested_flags = flags
        if len(operands) > 1:
            nested_flags |= PredicateVisitFlags.HAS_OR_SIBLINGS
        return self.apply_logical_or(
            operands, tuple([operand.visit(self, nested_flags) for operand in operands]), flags
        )

    @final
    def _visit_logical_and(self, operands: tree.PredicateOperands) -> _A:
        if len(operands) > 1:
            nested_flags = PredicateVisitFlags.HAS_AND_SIBLINGS
        else:
            nested_flags = PredicateVisitFlags(0)
        return self.apply_logical_and(
            operands, tuple([self._visit_logical_or(or_group, nested_flags) for or_group in operands])
        )


class SimplePredicateVisitor(
    PredicateVisitor[tree.Predicate | None, tree.Predicate | None, tree.Predicate | None]
):
    """An intermediate base class for predicate visitor implementations that
    either return `None` or a new `Predicate`.

    Notes
    -----
    This class implements all leaf-node visitation methods to return `None`,
    which is interpreted by the ``apply*`` method implementations as indicating
    that the leaf is unmodified.  Subclasses can thus override only certain
    visitation methods and either return `None` if there is no result, or
    return a replacement `Predicate` to construct a new tree.
    """

    def visit_boolean_wrapper(
        self, value: tree.ColumnExpression, flags: PredicateVisitFlags
    ) -> tree.Predicate | None:
        return None

    def visit_comparison(
        self,
        a: tree.ColumnExpression,
        operator: tree.ComparisonOperator,
        b: tree.ColumnExpression,
        flags: PredicateVisitFlags,
    ) -> tree.Predicate | None:
        # Docstring inherited.
        return None

    def visit_is_null(
        self, operand: tree.ColumnExpression, flags: PredicateVisitFlags
    ) -> tree.Predicate | None:
        # Docstring inherited.
        return None

    def visit_in_container(
        self,
        member: tree.ColumnExpression,
        container: tuple[tree.ColumnExpression, ...],
        flags: PredicateVisitFlags,
    ) -> tree.Predicate | None:
        # Docstring inherited.
        return None

    def visit_in_range(
        self,
        member: tree.ColumnExpression,
        start: int,
        stop: int | None,
        step: int,
        flags: PredicateVisitFlags,
    ) -> tree.Predicate | None:
        # Docstring inherited.
        return None

    def visit_in_query_tree(
        self,
        member: tree.ColumnExpression,
        column: tree.ColumnExpression,
        query_tree: tree.QueryTree,
        flags: PredicateVisitFlags,
    ) -> tree.Predicate | None:
        # Docstring inherited.
        return None

    def apply_logical_not(
        self, original: tree.PredicateLeaf, result: tree.Predicate | None, flags: PredicateVisitFlags
    ) -> tree.Predicate | None:
        # Docstring inherited.
        if result is None:
            return None
        from . import tree

        return tree.Predicate._from_leaf(original).logical_not()

    def apply_logical_or(
        self,
        originals: tuple[tree.PredicateLeaf, ...],
        results: tuple[tree.Predicate | None, ...],
        flags: PredicateVisitFlags,
    ) -> tree.Predicate | None:
        # Docstring inherited.
        if all(result is None for result in results):
            return None
        from . import tree

        return tree.Predicate.from_bool(False).logical_or(
            *[
                tree.Predicate._from_leaf(original) if result is None else result
                for original, result in zip(originals, results)
            ]
        )

    def apply_logical_and(
        self,
        originals: tree.PredicateOperands,
        results: tuple[tree.Predicate | None, ...],
    ) -> tree.Predicate | None:
        # Docstring inherited.
        if all(result is None for result in results):
            return None
        from . import tree

        return tree.Predicate.from_bool(True).logical_and(
            *[
                tree.Predicate._from_or_group(original) if result is None else result
                for original, result in zip(originals, results)
            ]
        )
