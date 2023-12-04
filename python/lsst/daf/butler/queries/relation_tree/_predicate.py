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

from lsst.daf.butler.queries.relation_tree._column_reference import ColumnReference

__all__ = ("Predicate",)

from typing import TYPE_CHECKING, Annotated, Literal, Union

import pydantic

from ...dimensions import DataIdValue, DimensionGroup
from ._base import PredicateBase
from ._column_expression import ColumnExpression

if TYPE_CHECKING:
    from ._relation import Relation


class LogicalAnd(PredicateBase):
    """A boolean column expression that is `True` only if all of its operands
    are `True`.
    """

    predicate_type: Literal["and"] = "and"
    operands: tuple[Predicate, ...] = pydantic.Field(min_length=2)

    def gather_required_columns(self) -> set[ColumnReference]:
        result = self.operands[0].gather_required_columns()
        for operand in self.operands[1:]:
            result.update(operand.gather_required_columns())
        return result


class LogicalOr(PredicateBase):
    """A boolean column expression that is `True` if any of its operands are
    `True`.
    """

    predicate_type: Literal["or"] = "or"
    operands: tuple[Predicate, ...] = pydantic.Field(min_length=2)

    def gather_required_columns(self) -> set[ColumnReference]:
        result = self.operands[0].gather_required_columns()
        for operand in self.operands[1:]:
            result.update(operand.gather_required_columns())
        return result


class LogicalNot(PredicateBase):
    """A boolean column expression that inverts its operand."""

    predicate_type: Literal["not"] = "not"
    operand: Predicate

    def gather_required_columns(self) -> set[ColumnReference]:
        return self.operand.gather_required_columns()


class IsNull(PredicateBase):
    """A boolean column expression that tests whether its operand is NULL."""

    predicate_type: Literal["is_null"] = "is_null"
    operand: ColumnExpression


class Comparison(PredicateBase):
    """A boolean columns expression formed by comparing two non-boolean
    expressions.
    """

    predicate_type: Literal["comparison"] = "comparison"
    a: ColumnExpression
    b: ColumnExpression
    operator: Literal["=", "!=", "<", ">", ">=", "<=", "overlaps"]

    def gather_required_columns(self) -> set[ColumnReference]:
        result = self.a.gather_required_columns()
        result.update(self.b.gather_required_columns())
        return result


class InContainer(PredicateBase):
    """A boolean column expression that tests whether one expression is a
    member of an explicit sequence of other expressions.
    """

    predicate_type: Literal["in_container"] = "in_container"
    member: ColumnExpression
    container: tuple[ColumnExpression, ...]

    def gather_required_columns(self) -> set[ColumnReference]:
        result = self.member.gather_required_columns()
        for operand in self.container:
            result.update(operand.gather_required_columns())
        return result


class InRange(PredicateBase):
    """A boolean column expression that tests whether its expression is
    included in an integer range.
    """

    predicate_type: Literal["in_range"] = "in_range"
    member: ColumnExpression
    start: int = 0
    stop: int | None = None
    step: int = 1

    def gather_required_columns(self) -> set[ColumnReference]:
        return self.member.gather_required_columns()


class InRelation(PredicateBase):
    """A boolean column expression that tests whether its expression is
    included single-column projection of a relation.

    This is primarily intended to be used on dataset ID columns, but it may
    be useful for other columns as well.
    """

    predicate_type: Literal["in_relation"] = "in_relation"
    member: ColumnExpression
    column: ColumnExpression
    relation: Relation

    def gather_required_columns(self) -> set[ColumnReference]:
        # We're only gathering columns from the relation this predicate is
        # attached, not `self.column`, which belongs to `self.relation`.
        return self.member.gather_required_columns()


class StringPredicate(PredicateBase):
    """A tag wrapper for boolean column expressions created by parsing a string
    expression.

    Remembering the original string is useful for error reporting.
    """

    predicate_type: Literal["string_predicate"] = "string_predicate"
    where: str
    tree: Predicate

    def gather_required_columns(self) -> set[ColumnReference]:
        return self.tree.gather_required_columns()


class DataCoordinateConstraint(PredicateBase):
    """A boolean column expression defined by interpreting data ID's key-value
    pairs as a logical AND of equality constraints.
    """

    predicate_type: Literal["data_coordinate_constraint"] = "data_coordinate_constraint"

    dimensions: DimensionGroup
    """The dimensions of the data ID."""

    values: tuple[DataIdValue, ...]
    """The required values of the data ID."""


Predicate = Annotated[
    Union[
        LogicalAnd,
        LogicalOr,
        LogicalNot,
        IsNull,
        Comparison,
        InContainer,
        InRange,
        InRelation,
        StringPredicate,
        DataCoordinateConstraint,
    ],
    pydantic.Field(discriminator="predicate_type"),
]
