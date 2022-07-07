# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
    "make_dimension_value_predicate",
    "make_data_coordinate_predicates",
    "make_temporal_constraint_predicate",
)

import operator
from collections.abc import Callable, Mapping, Sequence, Set
from typing import Generic, TypeVar, cast

import sqlalchemy
from lsst.daf.relation import EngineTag, Predicate

from ...core import (
    ColumnTag,
    DataCoordinate,
    DataIdValue,
    DimensionKeyColumnTag,
    LogicalColumn,
    TemporalConstraint,
    TimespanDatabaseRepresentation,
)

_C = TypeVar("_C")
_R = TypeVar("_R")
_V = TypeVar("_V")


class _ConstantBinaryOp(Generic[_C, _V, _R]):
    """A generic callable that indexes a mapping with a column tag and then
    applies a boolean operation to the result and a constant value.

    Parameters
    ----------
    column : `ColumnTag`
        Column to extract from the given mapping when called, providing one
        operand to the given operation.
    value
        Value that provides the other operand.
    operation : `Callable`, optional
        Binary operation to invoke; should almost always be a function from
        the built-in `operator` module.

    Notes
    -----
    This is used to implement simple `~lsst.daf.relation.Predicate` objects in
    both the `~lsst.daf.relation.sql` engine (where mapping values are
    `sqlalchemy.sql.ColumnElement` objects) and the
    `~lsst.daf.relation.iteration` engine (where mapping values are regular
    Python objects).  This works on any operator or method that SQLAlchemy
    overloads, which is the vast majority.
    """

    def __init__(self, column: ColumnTag, value: _V, operator: Callable[[_C, _V], _R] = operator.eq):
        self._column = column
        self._value = value
        self._operator = operator

    __slots__ = ("_column", "_value", "_operator")

    def __call__(self, arg: Mapping[ColumnTag, _C]) -> _R:
        return self._operator(arg[self._column], self._value)


class _TemporalConstraintSqlOverlaps:
    """A callable that implements a temporal overlap constraint in the
    `~lsst.daf.relation.sql` engine.

    Parameters
    ----------
    column : `ColumnTag`
        Timespan column in the relation the predicate will be applied to.
    constraint_logical_columns : `Sequence` [ \
        `TimespanDatabaseRepresentation` ]
        Literal `TimespanDatabaseRepresentation` objects representing (as a
        union) the temporal constraint the column values must overlap.
    """

    def __init__(
        self, column: ColumnTag, constraint_logical_columns: Sequence[TimespanDatabaseRepresentation]
    ):
        assert column.is_timespan, "Only works on timespan columns."
        self._column = column
        self._constraint_logical_columns = constraint_logical_columns

    __slots__ = ("_column", "_constraint_logical_columns")

    def __call__(self, logical_columns: Mapping[ColumnTag, LogicalColumn]) -> sqlalchemy.sql.ColumnElement:
        logical_column = cast(TimespanDatabaseRepresentation, logical_columns[self._column])
        overlaps = [
            logical_column.overlaps(constraint_logical_column)
            for constraint_logical_column in self._constraint_logical_columns
        ]
        return sqlalchemy.sql.or_(logical_column.isNull(), *overlaps)


def make_dimension_value_predicate(
    engines: Set[EngineTag],
    dimension_name: str,
    value: DataIdValue,
    operator_name: str = "eq",
) -> Predicate:
    """Return a `~lsst.daf.relation.Predicate` that compares a dimension column
    to a constant value.

    Parameters
    ----------
    engines : `~collections.abc.Set` [ `EngineTag` ]
        Engines the predicate support.  May only include
        `lsst.daf.relation.sql.Engine` and `lsst.daf.relation.iteration.Engine`
        instances.
    dimension_name : `str`
        Name of the dimension.
    value
        Constant value to compare to.
    operator_name : `str`
        Name of binary comparison operator from the built-in `operator` module
        ("eq", "lt", etc.).

    Returns
    -------
    predicate : `lsst.daf.relation.Predicate`
        New predicate.
    """
    column = DimensionKeyColumnTag(dimension_name)
    return Predicate(
        name="constant_binary_op",
        columns_required=frozenset({column}),
        general_state={"value": value, "operator": operator_name},
        engine_state={
            engine: _ConstantBinaryOp(column, value, getattr(operator, operator_name)) for engine in engines
        },
    )


def make_data_coordinate_predicates(
    engines: Set[EngineTag],
    data_coordinate: DataCoordinate,
    full: bool | None = None,
) -> set[Predicate]:
    """Return a `set` of `~lsst.daf.relation.Predicate` objects that represent
    a data ID constraint.

    Parameters
    ----------
    engines : `~collections.abc.Set` [ `EngineTag` ]
        Engines the predicate support.  May only include
        `lsst.daf.relation.sql.Engine` and `lsst.daf.relation.iteration.Engine`
        instances.
    data_coordinate : `DataCoordinate`
        Data ID whose keys and values should be transformed to predicate
        equality constraints.
    full : `bool`, optional
        Whether to include constraints on implied dimensions (default is to
        include implied dimensions if ``data_coordinate`` has them).

    Returns
    -------
    predicates : `set` [ `lsst.daf.relation.Predicate` ]
        Set of predicates (one for each dimension).
    """
    if full is None:
        full = data_coordinate.hasFull()
    dimension_names = data_coordinate.graph.required.names if not full else data_coordinate.graph.names
    return {
        make_dimension_value_predicate(engines, dimension_name, data_coordinate[dimension_name])
        for dimension_name in dimension_names
    }


def make_temporal_constraint_predicate(
    engines: Set[EngineTag],
    column: ColumnTag,
    constraint: TemporalConstraint,
    timespan_cls: type[TimespanDatabaseRepresentation],
) -> Predicate:
    """Return a `~lsst.daf.relation.Predicate` that compares a dimension column
    to a constant value.

    Parameters
    ----------
    engines : `~collections.abc.Set` [ `EngineTag` ]
        Engines the predicate support.  May only include
        `lsst.daf.relation.sql.Engine` instances.
    column : `ColumnTag`
        Identifier for a timespan column.
    constraint : `TemporalConstraint`
        Temporal constraint selected rows must overlap.
    timespan_cls : `type` [ `TimespanDatabaseRepresentation` ]
        Type that encapsulates how timespans are stored in this database.

    Returns
    -------
    predicate : `lsst.daf.relation.Predicate`
        New predicate.
    """
    callable = _TemporalConstraintSqlOverlaps(column, timespan_cls.from_constraint(constraint))
    return Predicate(
        name="temporal_constraint",
        columns_required=frozenset({column}),
        general_state={"constraint": constraint},
        engine_state={engine: callable for engine in engines},
    )
