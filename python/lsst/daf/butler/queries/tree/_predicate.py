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
    "ComparisonOperator",
    "LogicalNotOperand",
    "Predicate",
    "PredicateLeaf",
    "PredicateOperands",
)

import itertools
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import TYPE_CHECKING, Annotated, Literal, TypeAlias, TypeVar, cast, final

import pydantic

from ..._exceptions import InvalidQueryError
from ._base import QueryTreeBase
from ._column_expression import (
    ColumnExpression,
    ColumnReference,
    is_one_datetime_and_one_ingest_date,
    is_one_timespan_and_one_datetime,
)

if TYPE_CHECKING:
    from ..visitors import PredicateVisitFlags, PredicateVisitor
    from ._column_set import ColumnSet
    from ._query_tree import QueryTree

ComparisonOperator: TypeAlias = Literal["==", "!=", "<", ">", ">=", "<=", "overlaps"]


_L = TypeVar("_L")
_A = TypeVar("_A")
_O = TypeVar("_O")


class PredicateLeafBase(QueryTreeBase, ABC):
    """Base class for leaf nodes of the `Predicate` tree.

    This is a closed hierarchy whose concrete, `~typing.final` derived classes
    are members of the `PredicateLeaf` union.  That union should generally
    be used in type annotations rather than the technically-open base class.
    """

    @abstractmethod
    def gather_required_columns(self, columns: ColumnSet) -> None:
        """Add any columns required to evaluate this predicate leaf to the
        given column set.

        Parameters
        ----------
        columns : `ColumnSet`
            Set of columns to modify in place.
        """
        raise NotImplementedError()

    @abstractmethod
    def gather_governors(self, governors: set[str]) -> None:
        """Add any governor dimensions that need to be fully identified for
        this column expression to be sound.

        Parameters
        ----------
        governors : `set` [ `str` ]
            Set of governor dimension names to modify in place.
        """
        raise NotImplementedError()

    def invert(self) -> PredicateLeaf:
        """Return a new leaf that is the logical not of this one."""
        return LogicalNot.model_construct(operand=cast("LogicalNotOperand", self))

    @abstractmethod
    def visit(self, visitor: PredicateVisitor[_A, _O, _L], flags: PredicateVisitFlags) -> _L:
        """Invoke the visitor interface.

        Parameters
        ----------
        visitor : `PredicateVisitor`
            Visitor to invoke a method on.
        flags : `PredicateVisitFlags`
            Flags that provide information about where this leaf appears in the
            larger predicate tree.

        Returns
        -------
        result : `object`
            Forwarded result from the visitor.
        """
        raise NotImplementedError()


@final
class Predicate(QueryTreeBase):
    """A boolean column expression.

    Notes
    -----
    Predicate is the only class representing a boolean column expression that
    should be used outside of this module (though the objects it nests appear
    in its serialized form and hence are not fully private).  It provides
    several `classmethod` factories for constructing those nested types inside
    a `Predicate` instance, and `PredicateVisitor` subclasses should be used
    to process them.
    """

    operands: PredicateOperands
    """Nested tuple of operands, with outer items combined via AND and inner
    items combined via OR.
    """

    @property
    def column_type(self) -> Literal["bool"]:
        """A string enumeration value representing the type of the column
        expression.
        """
        return "bool"

    @classmethod
    def from_bool(cls, value: bool) -> Predicate:
        """Construct a predicate that always evaluates to `True` or `False`.

        Parameters
        ----------
        value : `bool`
            Value the predicate should evaluate to.

        Returns
        -------
        predicate : `Predicate`
            Predicate that evaluates to the given boolean value.
        """
        # The values for True and False here make sense if you think about
        # calling `all` and `any` with empty sequences; note that the
        # `self.operands` attribute is evaluated as:
        #
        #    value = all(any(or_group) for or_group in self.operands)
        #
        return cls.model_construct(operands=() if value else ((),))

    @classmethod
    def from_bool_expression(cls, value: ColumnReference) -> Predicate:
        """Construct a predicate that wraps a boolean ColumnReference, taking
        on the value of the underlying ColumnReference.

        Parameters
        ----------
        value : `ColumnExpression`
            Boolean-valued expression to convert to Predicate.

        Returns
        -------
        predicate : `Predicate`
            Predicate representing the expression.
        """
        if value.column_type != "bool":
            raise ValueError(f"ColumnExpression must have column type 'bool', not '{value.column_type}'")

        return cls._from_leaf(BooleanWrapper(operand=value))

    @classmethod
    def compare(cls, a: ColumnExpression, operator: ComparisonOperator, b: ColumnExpression) -> Predicate:
        """Construct a predicate representing a binary comparison between
        two non-boolean column expressions.

        Parameters
        ----------
        a : `ColumnExpression`
            First column expression in the comparison.
        operator : `str`
            Enumerated string representing the comparison operator to apply.
            May be and of "==", "!=", "<", ">", "<=", ">=", or "overlaps".
        b : `ColumnExpression`
            Second column expression in the comparison.

        Returns
        -------
        predicate : `Predicate`
            Predicate representing the comparison.
        """
        return cls._from_leaf(Comparison(a=a, operator=operator, b=b))

    @classmethod
    def is_null(cls, operand: ColumnExpression) -> Predicate:
        """Construct a predicate that tests whether a column expression is
        NULL.

        Parameters
        ----------
        operand : `ColumnExpression`
            Column expression to test.

        Returns
        -------
        predicate : `Predicate`
            Predicate representing the NULL check.
        """
        return cls._from_leaf(IsNull(operand=operand))

    @classmethod
    def in_container(cls, member: ColumnExpression, container: Iterable[ColumnExpression]) -> Predicate:
        """Construct a predicate that tests whether one column expression is
        a member of a container of other column expressions.

        Parameters
        ----------
        member : `ColumnExpression`
            Column expression that may be a member of the container.
        container : `~collections.abc.Iterable` [ `ColumnExpression` ]
            Container of column expressions to test for membership in.

        Returns
        -------
        predicate : `Predicate`
            Predicate representing the membership test.
        """
        return cls._from_leaf(InContainer(member=member, container=tuple(container)))

    @classmethod
    def in_range(
        cls, member: ColumnExpression, start: int = 0, stop: int | None = None, step: int = 1
    ) -> Predicate:
        """Construct a predicate that tests whether an integer column
        expression is part of a strided range.

        Parameters
        ----------
        member : `ColumnExpression`
            Column expression that may be a member of the range.
        start : `int`, optional
            Beginning of the range, inclusive.
        stop : `int` or `None`, optional
            End of the range, exclusive.
        step : `int`, optional
            Offset between values in the range.

        Returns
        -------
        predicate : `Predicate`
            Predicate representing the membership test.
        """
        return cls._from_leaf(InRange(member=member, start=start, stop=stop, step=step))

    @classmethod
    def in_query(cls, member: ColumnExpression, column: ColumnExpression, query_tree: QueryTree) -> Predicate:
        """Construct a predicate that tests whether a column expression is
        present in a single-column projection of a query tree.

        Parameters
        ----------
        member : `ColumnExpression`
            Column expression that may be present in the query.
        column : `ColumnExpression`
            Column to project from the query.
        query_tree : `QueryTree`
            Query tree to select from.

        Returns
        -------
        predicate : `Predicate`
            Predicate representing the membership test.
        """
        return cls._from_leaf(InQuery(member=member, column=column, query_tree=query_tree))

    def gather_required_columns(self, columns: ColumnSet) -> None:
        """Add any columns required to evaluate this predicate to the given
        column set.

        Parameters
        ----------
        columns : `ColumnSet`
            Set of columns to modify in place.
        """
        for or_group in self.operands:
            for operand in or_group:
                operand.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        """Add any governor dimensions that need to be fully identified for
        this column expression to be sound.

        Parameters
        ----------
        governors : `set` [ `str` ]
            Set of governor dimension names to modify in place.
        """
        for or_group in self.operands:
            for operand in or_group:
                operand.gather_governors(governors)

    def logical_and(self, *args: Predicate) -> Predicate:
        """Construct a predicate representing the logical AND of this predicate
        and one or more others.

        Parameters
        ----------
        *args : `Predicate`
            Other predicates.

        Returns
        -------
        predicate : `Predicate`
            Predicate representing the logical AND.
        """
        operands = self.operands
        for arg in args:
            operands = self._impl_and(operands, arg.operands)
        if not all(operands):
            # If any item in operands is an empty tuple (i.e. False), simplify.
            operands = ((),)
        return Predicate.model_construct(operands=operands)

    def logical_or(self, *args: Predicate) -> Predicate:
        """Construct a predicate representing the logical OR of this predicate
        and one or more others.

        Parameters
        ----------
        *args : `Predicate`
            Other predicates.

        Returns
        -------
        predicate : `Predicate`
            Predicate representing the logical OR.
        """
        operands = self.operands
        for arg in args:
            operands = self._impl_or(operands, arg.operands)
        return Predicate.model_construct(operands=operands)

    def logical_not(self) -> Predicate:
        """Construct a predicate representing the logical NOT of this
        predicate.

        Returns
        -------
        predicate : `Predicate`
            Predicate representing the logical NOT.
        """
        new_operands: PredicateOperands = ((),)
        for or_group in self.operands:
            new_group: PredicateOperands = ()
            for leaf in or_group:
                new_group = self._impl_and(new_group, ((leaf.invert(),),))
            new_operands = self._impl_or(new_operands, new_group)
        return Predicate.model_construct(operands=new_operands)

    def __str__(self) -> str:
        and_terms = []
        for or_group in self.operands:
            match len(or_group):
                case 0:
                    and_terms.append("False")
                case 1:
                    and_terms.append(str(or_group[0]))
                case _:
                    or_str = " OR ".join(str(operand) for operand in or_group)
                    if len(self.operands) > 1:
                        and_terms.append(f"({or_str})")
                    else:
                        and_terms.append(or_str)
        if not and_terms:
            return "True"
        return " AND ".join(and_terms)

    def visit(self, visitor: PredicateVisitor[_A, _O, _L]) -> _A:
        """Invoke the visitor interface.

        Parameters
        ----------
        visitor : `PredicateVisitor`
            Visitor to invoke a method on.

        Returns
        -------
        result : `object`
            Forwarded result from the visitor.
        """
        return visitor._visit_logical_and(self.operands)

    @classmethod
    def _from_leaf(cls, leaf: PredicateLeaf) -> Predicate:
        return cls._from_or_group((leaf,))

    @classmethod
    def _from_or_group(cls, or_group: tuple[PredicateLeaf, ...]) -> Predicate:
        return Predicate.model_construct(operands=(or_group,))

    @classmethod
    def _impl_and(cls, a: PredicateOperands, b: PredicateOperands) -> PredicateOperands:
        # We could simplify cases where both sides have some of the same leaf
        # expressions; even using 'is' tests would simplify some cases where
        # converting to conjunctive normal form twice leads to a lot of
        # duplication, e.g. NOT ((A AND B) OR (C AND D)) or any kind of
        # double-negation.  Right now those cases seem pathological enough to
        # be not worth our time.
        return a + b if a is not b else a

    @classmethod
    def _impl_or(cls, a: PredicateOperands, b: PredicateOperands) -> PredicateOperands:
        # Same comment re simplification as in _impl_and applies here.
        return tuple([a_operand + b_operand for a_operand, b_operand in itertools.product(a, b)])


@final
class LogicalNot(PredicateLeafBase):
    """A boolean column expression that inverts its operand."""

    predicate_type: Literal["not"] = "not"

    operand: LogicalNotOperand
    """Upstream boolean expression to invert."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        self.operand.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        self.operand.gather_governors(governors)

    def __str__(self) -> str:
        return f"NOT {self.operand}"

    def invert(self) -> LogicalNotOperand:
        # Docstring inherited.
        return self.operand

    def visit(self, visitor: PredicateVisitor[_A, _O, _L], flags: PredicateVisitFlags) -> _L:
        # Docstring inherited.
        return visitor._visit_logical_not(self.operand, flags)


class BooleanWrapper(PredicateLeafBase):
    """Pass-through to a pre-existing boolean column expression."""

    predicate_type: Literal["boolean_wrapper"] = "boolean_wrapper"

    operand: ColumnReference
    """Wrapped expression that will be used as the value for this predicate."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        self.operand.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        self.operand.gather_governors(governors)

    def __str__(self) -> str:
        return f"{self.operand}"

    def visit(self, visitor: PredicateVisitor[_A, _O, _L], flags: PredicateVisitFlags) -> _L:
        # Docstring inherited.
        return visitor.visit_boolean_wrapper(self.operand, flags)


@final
class IsNull(PredicateLeafBase):
    """A boolean column expression that tests whether its operand is NULL."""

    predicate_type: Literal["is_null"] = "is_null"

    operand: ColumnExpression
    """Upstream expression to test."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        self.operand.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        self.operand.gather_governors(governors)

    def __str__(self) -> str:
        return f"{self.operand} IS NULL"

    def visit(self, visitor: PredicateVisitor[_A, _O, _L], flags: PredicateVisitFlags) -> _L:
        # Docstring inherited.
        return visitor.visit_is_null(self.operand, flags)


@final
class Comparison(PredicateLeafBase):
    """A boolean columns expression formed by comparing two non-boolean
    expressions.
    """

    predicate_type: Literal["comparison"] = "comparison"

    a: ColumnExpression
    """Left-hand side expression for the comparison."""

    b: ColumnExpression
    """Right-hand side expression for the comparison."""

    operator: ComparisonOperator
    """Comparison operator."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        self.a.gather_required_columns(columns)
        self.b.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        self.a.gather_governors(governors)
        self.b.gather_governors(governors)

    def __str__(self) -> str:
        return f"{self.a} {self.operator.upper()} {self.b}"

    def visit(self, visitor: PredicateVisitor[_A, _O, _L], flags: PredicateVisitFlags) -> _L:
        # Docstring inherited.
        return visitor.visit_comparison(self.a, self.operator, self.b, flags)

    @pydantic.model_validator(mode="after")
    def _validate_column_types(self) -> Comparison:
        if self.operator == "overlaps" and is_one_timespan_and_one_datetime(self.a, self.b):
            # Allow mixed-type comparison of datetime overlaps timespan.
            pass
        elif is_one_datetime_and_one_ingest_date(self.a, self.b):
            if self.operator in ("==", "!=", "<", ">", ">=", "<="):
                # ingest_date might be one of two different column types
                # (integer TAI nanoseconds like "datetime", or TIMESTAMP), but
                # either one can be compared with a "datetime" column.
                pass
        elif self.a.column_type == self.b.column_type:
            # Most operators require matching column types.
            match (self.operator, self.a.column_type):
                case ("==" | "!=", _):
                    pass
                case ("<" | ">" | ">=" | "<=", "int" | "string" | "float" | "datetime"):
                    pass
                case ("overlaps", "region" | "timespan"):
                    pass
                case _:
                    raise InvalidQueryError(
                        f"Invalid column type {self.a.column_type} for operator {self.operator!r}."
                    )
        else:
            raise InvalidQueryError(
                f"Column types for comparison {self} do not agree "
                f"({self.a.column_type}, {self.b.column_type})."
            )

        return self


@final
class InContainer(PredicateLeafBase):
    """A boolean column expression that tests whether one expression is a
    member of an explicit sequence of other expressions.
    """

    predicate_type: Literal["in_container"] = "in_container"

    member: ColumnExpression
    """Expression to test for membership."""

    container: tuple[ColumnExpression, ...]
    """Expressions representing the elements of the container."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        self.member.gather_required_columns(columns)
        for item in self.container:
            item.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        self.member.gather_governors(governors)
        for item in self.container:
            item.gather_governors(governors)

    def __str__(self) -> str:
        return f"{self.member} IN [{', '.join(str(item) for item in self.container)}]"

    def visit(self, visitor: PredicateVisitor[_A, _O, _L], flags: PredicateVisitFlags) -> _L:
        # Docstring inherited.
        return visitor.visit_in_container(self.member, self.container, flags)

    @pydantic.model_validator(mode="after")
    def _validate(self) -> InContainer:
        if self.member.column_type == "timespan" or self.member.column_type == "region":
            raise InvalidQueryError(
                f"Timespan or region column {self.member} may not be used in IN expressions."
            )
        if not all(item.column_type == self.member.column_type for item in self.container):
            raise InvalidQueryError(f"Column types for membership test {self} do not agree.")
        return self


@final
class InRange(PredicateLeafBase):
    """A boolean column expression that tests whether its expression is
    included in an integer range.
    """

    predicate_type: Literal["in_range"] = "in_range"

    member: ColumnExpression
    """Expression to test for membership."""

    start: int = 0
    """Inclusive lower bound for the range."""

    stop: int | None = None
    """Exclusive upper bound for the range."""

    step: int = 1
    """Difference between values in the range."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        self.member.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        self.member.gather_governors(governors)

    def __str__(self) -> str:
        s = f"{self.start if self.start else ''}:{self.stop if self.stop is not None else ''}"
        if self.step != 1:
            s = f"{s}:{self.step}"
        return f"{self.member} IN {s}"

    def visit(self, visitor: PredicateVisitor[_A, _O, _L], flags: PredicateVisitFlags) -> _L:
        return visitor.visit_in_range(self.member, self.start, self.stop, self.step, flags)

    @pydantic.model_validator(mode="after")
    def _validate(self) -> InRange:
        if self.member.column_type != "int":
            raise InvalidQueryError(f"Column {self.member} is not an integer.")
        if self.step < 1:
            raise InvalidQueryError("Range step must be >= 1.")
        if self.stop is not None and self.stop < self.start:
            raise InvalidQueryError("Range stop must be >= start.")
        return self


@final
class InQuery(PredicateLeafBase):
    """A boolean column expression that tests whether its expression is
    included single-column projection of a relation.

    This is primarily intended to be used on dataset ID columns, but it may
    be useful for other columns as well.
    """

    predicate_type: Literal["in_query"] = "in_query"

    member: ColumnExpression
    """Expression to test for membership."""

    column: ColumnExpression
    """Expression to extract from `query_tree`."""

    query_tree: QueryTree
    """Relation whose rows from `column` represent the container."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        # We're only gathering columns from the query_tree this predicate is
        # attached to, not `self.column`, which belongs to `self.query_tree`.
        self.member.gather_required_columns(columns)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        # We're only gathering governors from the query_tree this predicate is
        # attached to, not `self.column`, which belongs to `self.query_tree`.
        self.member.gather_governors(governors)

    def __str__(self) -> str:
        return f"{self.member} IN (query).{self.column}"

    def visit(self, visitor: PredicateVisitor[_A, _O, _L], flags: PredicateVisitFlags) -> _L:
        # Docstring inherited.
        return visitor.visit_in_query_tree(self.member, self.column, self.query_tree, flags)

    @pydantic.model_validator(mode="after")
    def _validate_column_types(self) -> InQuery:
        if self.member.column_type == "timespan" or self.member.column_type == "region":
            raise InvalidQueryError(
                f"Timespan or region column {self.member} may not be used in IN expressions."
            )
        if self.member.column_type != self.column.column_type:
            raise InvalidQueryError(
                f"Column types for membership test {self} do not agree "
                f"({self.member.column_type}, {self.column.column_type})."
            )

        from ._column_set import ColumnSet

        columns_required_in_tree = ColumnSet(self.query_tree.dimensions)
        self.column.gather_required_columns(columns_required_in_tree)
        if columns_required_in_tree.dimensions != self.query_tree.dimensions:
            raise InvalidQueryError(
                f"Column {self.column} requires dimensions {columns_required_in_tree.dimensions}, "
                f"but query tree only has {self.query_tree.dimensions}."
            )
        if not columns_required_in_tree.dataset_fields.keys() <= self.query_tree.datasets.keys():
            raise InvalidQueryError(
                f"Column {self.column} requires dataset types "
                f"{set(columns_required_in_tree.dataset_fields.keys())} that are not present in query tree."
            )
        return self


LogicalNotOperand: TypeAlias = IsNull | Comparison | InContainer | InRange | InQuery | BooleanWrapper
PredicateLeaf: TypeAlias = Annotated[
    LogicalNotOperand | LogicalNot, pydantic.Field(discriminator="predicate_type")
]

PredicateOperands: TypeAlias = tuple[tuple[PredicateLeaf, ...], ...]
