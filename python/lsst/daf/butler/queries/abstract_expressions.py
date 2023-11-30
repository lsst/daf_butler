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
    "AbstractExpression",
    "AbstractOrderExpression",
    "AbstractPredicate",
)


import dataclasses
from typing import TYPE_CHECKING, Literal, TypeAlias, Union

import astropy.time
from lsst.sphgeom import Region

from .._column_tags import DatasetColumnTag, DimensionKeyColumnTag, DimensionRecordColumnTag
from .._timespan import Timespan
from ..dimensions import DataCoordinate

if TYPE_CHECKING:
    from .abstract_relations import AbstractRelation


LiteralValue: TypeAlias = Union[int, bytes, str, float, astropy.time.Time, Timespan, Region]


@dataclasses.dataclass(frozen=True)
class ColumnLiteral:
    """A column expression that is a literal Python value."""

    value: LiteralValue


@dataclasses.dataclass(frozen=True)
class ColumnReference:
    """A column expression that refers to a column obtainable from an abstract
    relation.
    """

    column: DimensionKeyColumnTag | DimensionRecordColumnTag | DatasetColumnTag


@dataclasses.dataclass(frozen=True)
class UnaryExpression:
    """A unary operation on a column expression that returns a non-bool."""

    operand: AbstractExpression
    operator: Literal["-", "begin_of", "end_of"]


@dataclasses.dataclass(frozen=True)
class BinaryExpression:
    """A binary operation on column expressions that returns a non-bool."""

    a: AbstractExpression
    b: AbstractExpression
    operator: Literal["+", "-", "*", "/", "%"]


AbstractExpression: TypeAlias = Union[ColumnLiteral, ColumnReference, UnaryExpression, BinaryExpression]


@dataclasses.dataclass(frozen=True)
class Reversed:
    """A tag wrapper for `AbstractExpression` that indicate sorting in
    reverse order.
    """

    operand: AbstractExpression


AbstractOrderExpression: TypeAlias = Union[AbstractExpression, Reversed]


@dataclasses.dataclass(frozen=True)
class LogicalAnd:
    """A boolean column expression that is `True` only if all of its operands
    are `True`.
    """

    operands: tuple[AbstractPredicate]


@dataclasses.dataclass(frozen=True)
class LogicalOr:
    """A boolean column expression that is `True` if any of its operands are
    `True`.
    """

    operands: tuple[AbstractPredicate]


@dataclasses.dataclass(frozen=True)
class LogicalNot:
    """A boolean column expression that inverts its operand."""

    operand: AbstractPredicate


@dataclasses.dataclass(frozen=True)
class IsNull:
    """A boolean column expression that tests whether its operand is NULL."""

    operand: AbstractExpression


@dataclasses.dataclass(frozen=True)
class Comparison:
    """A boolean columns expression formed by comparing two non-boolean
    expressions.
    """

    a: AbstractExpression
    b: AbstractExpression
    operator: Literal["=", "!=", "<", ">", ">=", "<=", "overlaps"]


@dataclasses.dataclass(frozen=True)
class InContainer:
    """A boolean column expression that tests whether one expression is a
    member of an explicit sequence of other expressions.
    """

    member: AbstractExpression
    container: tuple[AbstractExpression, ...]


@dataclasses.dataclass(frozen=True)
class InRange:
    """A boolean column expression that tests whether its expression is
    included in an integer range.
    """

    member: AbstractExpression
    range: range


@dataclasses.dataclass(frozen=True)
class InRelation:
    """A boolean column expression that tests whether its expression is
    included single-column projection of a relation.

    This is primarily intended to be used on dataset ID columns, but it may
    be useful for other columns as well.
    """

    member: AbstractExpression
    column: DimensionKeyColumnTag | DimensionRecordColumnTag | DatasetColumnTag
    relation: AbstractRelation


@dataclasses.dataclass(frozen=True)
class StringPredicate:
    """A tag wrapper for boolean column expressions created by parsing a string
    expression.

    Remembering the original string is useful for error reporting.
    """

    where: str
    tree: AbstractPredicate


@dataclasses.dataclass(frozen=True)
class DataCoordinateConstraint:
    """A boolean column expression defined by interpreting data ID's key-value
    pairs as a logical AND of equality constraints.
    """

    data_coordinate: DataCoordinate


AbstractPredicate: TypeAlias = Union[
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
]
