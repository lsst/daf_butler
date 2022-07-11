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
    "SpatialConstraintSkyPixOverlap",
    "SpatialConstraintRegionOverlap",
    "TemporalConstraintOverlap",
)

from collections.abc import Mapping, Set
from typing import Any, cast

import sqlalchemy
from lsst.daf.relation import DictWriter, EngineTag, Predicate, iteration, sql
from lsst.daf.relation.expressions import ConstantComparisonPredicate
from lsst.sphgeom import DISJOINT

from ...core import (
    ColumnTag,
    ColumnTypeInfo,
    DataCoordinate,
    DimensionKeyColumnTag,
    LogicalColumn,
    SkyPixDimension,
    SpatialConstraint,
    TemporalConstraint,
    TimespanDatabaseRepresentation,
)


class SpatialConstraintSkyPixOverlap(Predicate):
    """A callable that implements a spatial overlap constraint on a skypix
    dimension column in the `~lsst.daf.relation.sql` engine.

    Parameters
    ----------
    dimension : `SkyPixDimension`
        Dimension that identifies the column and the pixelization to render
        the spatial constraint's region into.
    constraint : `SpatialConstraint`
        Spatial constraint selected rows must overlap.

    Notes
    -----
    This predicate provides a conservative overlap, i.e. it includes all rows
    that *might* overlap the constraint, but may also return some that do not.
    `SpatialConstraintRegionOverlap` (only executable in the
    `lsst.daf.relation.iteration` engine) can be used to filter out these.
    """

    def __init__(
        self,
        dimension: SkyPixDimension,
        constraint: SpatialConstraint,
    ):
        self._dimension = dimension
        self._column = DimensionKeyColumnTag(dimension.name)
        self._constraint = constraint

    @property
    def columns_required(self) -> Set[ColumnTag]:
        return {self._column}

    def supports_engine(self, engine: EngineTag) -> bool:
        return isinstance(engine, sql.Engine)

    def serialize(self, writer: DictWriter[ColumnTag]) -> dict[str, Any]:
        return {
            "type": "spatial_constraint_skypix_overlap",
            "dimension": self._dimension.name,
            "region": self._constraint.region.encode().hex(),
        }

    def to_sql_boolean(
        self,
        logical_columns: Mapping[ColumnTag, LogicalColumn],
        column_types: sql.ColumnTypeInfo[ColumnTag, LogicalColumn],
    ) -> sqlalchemy.sql.ColumnElement:
        sql_column = cast(sqlalchemy.sql.ColumnElement, logical_columns[self._column])
        column_types = cast(ColumnTypeInfo, column_types)
        overlaps = [
            sql_column.between(begin, end - 1) if begin != end - 1 else sql_column == begin
            for begin, end in self._constraint.ranges(self._dimension)
        ]
        return sqlalchemy.sql.or_(*overlaps)


class SpatialConstraintRegionOverlap(Predicate):
    """A callable that implements a spatial overlap constraint on a skypix
    dimension column in the `~lsst.daf.relation.sql` engine.

    Parameters
    ----------
    column : `ColumnTag`
        Spatial region column in the relation that the predicate will be
        applied to.
    constraint : `SpatialConstraint`
        Spatial constraint selected rows must overlap.
    """

    def __init__(
        self,
        column: ColumnTag,
        constraint: SpatialConstraint,
    ):
        self._column = column
        self._constraint = constraint

    @property
    def columns_required(self) -> Set[ColumnTag]:
        return {self._column}

    def supports_engine(self, engine: EngineTag) -> bool:
        return isinstance(engine, iteration.Engine)

    def serialize(self, writer: DictWriter[ColumnTag]) -> dict[str, Any]:
        return {
            "type": "spatial_constraint_region_overlap",
            "column": writer.write_column(self._column),
            "region": self._constraint.region.encode().hex(),
        }

    def test_iteration_row(self, row: iteration.typing.Row[ColumnTag]) -> bool:
        return not (self._constraint.region.relate(row[self._column]) & DISJOINT)


class TemporalConstraintOverlap(Predicate):
    """A callable that implements a temporal overlap constraint in the
    `~lsst.daf.relation.sql` engine.

    Parameters
    ----------
    column : `ColumnTag`
        Timespan column in the relation that the predicate will be applied to.
    constraint : `TemporalConstraint`
        Temporal constraint selected rows must overlap.

    Notes
    -----
    NULL timespans in the database are defined as overlapping any temporal
    constraint, even any empty one.  This is a pragmatic choice because we use
    NULL timespans in dataset subqueries against non-CALIBRATION collections,
    which we want to match any temporal constraint; it would be slightly
    cleaner to move the IS NULL test out of this predicate, but at present
    there's no good place to put it, since there's no general boolean
    expression tree for Predicates - they're always ANDed together.
    """

    def __init__(
        self,
        column: ColumnTag,
        constraint: TemporalConstraint,
    ):
        assert column.is_timespan, "Only works on timespan columns."
        self._column = column
        self._constraint = constraint

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
) -> list[Predicate]:
    """Return a `list` of `~lsst.daf.relation.Predicate` objects that represent
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
    predicates : `list` [ `lsst.daf.relation.Predicate` ]
        List of predicates (one for each dimension).
    """
    if full is None:
        full = data_coordinate.hasFull()
    dimension_names = data_coordinate.graph.required.names if not full else data_coordinate.graph.names
    return [
        ConstantComparisonPredicate(DimensionKeyColumnTag(dimension_name), data_coordinate[dimension_name])
        for dimension_name in dimension_names
    ]
