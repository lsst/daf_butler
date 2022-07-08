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
    "make_data_coordinate_predicates",
    "TemporalConstraintOverlap",
)

from collections.abc import Mapping, Set
from typing import Any, cast

import sqlalchemy
from lsst.daf.relation import DictWriter, EngineTag, Predicate, sql
from lsst.daf.relation.expressions import ConstantComparisonPredicate

from ...core import (
    ColumnTag,
    ColumnTypeInfo,
    DataCoordinate,
    DimensionKeyColumnTag,
    LogicalColumn,
    TemporalConstraint,
    TimespanDatabaseRepresentation,
)


class TemporalConstraintOverlap(Predicate):
    """A callable that implements a temporal overlap constraint in the
    `~lsst.daf.relation.sql` engine.

    Parameters
    ----------
    column : `ColumnTag`
        Timespan column in the relation the predicate will be applied to.
    constraint : `TemporalConstraint`
        Temporal constraint selected rows must overlap.
    """

    def __init__(
        self,
        column: ColumnTag,
        constraint: TemporalConstraint,
    ):
        assert column.is_timespan, "Only works on timespan columns."
        self._column = column
        self._constraint = constraint

    def __eq__(self, other: Any) -> bool:
        if self.__class__ != other.__class__:
            return NotImplemented
        return self._column == other._column and self._constraint == other._constraint

    def __hash__(self) -> int:
        return hash(self._column)

    @property
    def columns_required(self) -> Set[ColumnTag]:
        return {self._column}

    def supports_engine(self, engine: EngineTag) -> bool:
        return isinstance(engine, sql.Engine)

    def serialize(self, writer: DictWriter[ColumnTag]) -> dict[str, Any]:
        return {
            "type": "temporal_constraint_overlap",
            "column": writer.write_column(self._column),
            "constraint": list(self._constraint._to_ranges()),
        }

    def to_sql_boolean(
        self,
        logical_columns: Mapping[ColumnTag, LogicalColumn],
        column_types: sql.ColumnTypeInfo[ColumnTag, LogicalColumn],
    ) -> sqlalchemy.sql.ColumnElement:
        logical_column = cast(TimespanDatabaseRepresentation, logical_columns[self._column])
        column_types = cast(ColumnTypeInfo, column_types)
        overlaps = [
            logical_column.overlaps(constraint_logical_column)
            for constraint_logical_column in column_types.timespan_cls.from_constraint(self._constraint)
        ]
        return sqlalchemy.sql.or_(logical_column.isNull(), *overlaps)


def make_data_coordinate_predicates(
    data_coordinate: DataCoordinate,
    full: bool | None = None,
) -> set[Predicate]:
    """Return a `set` of `~lsst.daf.relation.Predicate` objects that represent
    a data ID constraint.

    Parameters
    ----------
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
        ConstantComparisonPredicate(DimensionKeyColumnTag(dimension_name), data_coordinate[dimension_name])
        for dimension_name in dimension_names
    }
