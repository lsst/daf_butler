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

__all__ = ("Predicate",)

from typing import TYPE_CHECKING, Annotated, Literal, Union

import pydantic

from ...dimensions import DataIdValue
from ._column_expression import ColumnExpression

if TYPE_CHECKING:
    from ._relation import DimensionNameSet, Relation


class LogicalAnd(pydantic.BaseModel):
    """A boolean column expression that is `True` only if all of its operands
    are `True`.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["and"] = "and"
    operands: tuple[Predicate, ...] = pydantic.Field(min_length=2)


class LogicalOr(pydantic.BaseModel):
    """A boolean column expression that is `True` if any of its operands are
    `True`.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["or"] = "or"
    operands: tuple[Predicate, ...] = pydantic.Field(min_length=2)


class LogicalNot(pydantic.BaseModel):
    """A boolean column expression that inverts its operand."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["not"] = "not"
    operand: Predicate


class IsNull(pydantic.BaseModel):
    """A boolean column expression that tests whether its operand is NULL."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["is_null"] = "is_null"
    operand: ColumnExpression


class Comparison(pydantic.BaseModel):
    """A boolean columns expression formed by comparing two non-boolean
    expressions.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["comparison"] = "comparison"
    a: ColumnExpression
    b: ColumnExpression
    operator: Literal["=", "!=", "<", ">", ">=", "<=", "overlaps"]


class InContainer(pydantic.BaseModel):
    """A boolean column expression that tests whether one expression is a
    member of an explicit sequence of other expressions.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["in_container"] = "in_container"
    member: ColumnExpression
    container: tuple[ColumnExpression, ...]


class InRange(pydantic.BaseModel):
    """A boolean column expression that tests whether its expression is
    included in an integer range.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["in_range"] = "in_range"
    member: ColumnExpression
    start: int = 0
    stop: int | None = None
    step: int = 1


class InRelation(pydantic.BaseModel):
    """A boolean column expression that tests whether its expression is
    included single-column projection of a relation.

    This is primarily intended to be used on dataset ID columns, but it may
    be useful for other columns as well.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["in_relation"] = "in_relation"
    member: ColumnExpression
    column: ColumnExpression
    relation: Relation


class StringPredicate(pydantic.BaseModel):
    """A tag wrapper for boolean column expressions created by parsing a string
    expression.

    Remembering the original string is useful for error reporting.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["string_predicate"] = "string_predicate"
    where: str
    tree: Predicate


class DataCoordinateConstraint(pydantic.BaseModel):
    """A boolean column expression defined by interpreting data ID's key-value
    pairs as a logical AND of equality constraints.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    predicate_type: Literal["data_coordinate_constraint"] = "data_coordinate_constraint"

    dimensions: DimensionNameSet
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
