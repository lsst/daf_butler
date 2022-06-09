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

__all__ = ("Predicate",)

from abc import ABC, abstractmethod
from typing import Mapping, Optional, Sequence

import sqlalchemy
from lsst.utils.classes import cached_getter

from .._spatial_regions import SpatialConstraint
from ..dimensions import DataCoordinate, SkyPixDimension
from ..timespan import TemporalConstraint
from ._column_tag_set import ColumnTagSet
from ._column_tags import ColumnTag, DimensionKeyColumnTag, LogicalColumn
from ._column_type_info import ColumnTypeInfo
from ._local_constraints import LocalConstraints


class Predicate(ABC):
    @property
    @abstractmethod
    def columns_required(self) -> ColumnTagSet:
        raise NotImplementedError()

    @property
    def constraints(self) -> LocalConstraints:
        return LocalConstraints.make_full()

    @abstractmethod
    def to_sql_booleans(
        self,
        columns: Mapping[ColumnTag, LogicalColumn],
        column_types: ColumnTypeInfo,
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        raise NotImplementedError()

    @staticmethod
    def from_data_coordinate(data_coordinate: DataCoordinate, full: Optional[bool] = None) -> Predicate:
        return _DataCoordinatePredicate(data_coordinate, full=full)

    @staticmethod
    def from_spatial_constraint(spatial_constraint: SpatialConstraint, skypix: SkyPixDimension) -> Predicate:
        return _SpatialConstraintPredicate(spatial_constraint, skypix)

    @staticmethod
    def from_temporal_constraint(temporal_constraint: TemporalConstraint, column: ColumnTag) -> Predicate:
        return _TemporalConstraintPredicate(temporal_constraint, column)


class _DataCoordinatePredicate(Predicate):
    def __init__(self, data_coordinate: DataCoordinate, full: Optional[bool] = None):
        self._data_coordinate = data_coordinate
        if full is None:
            full = data_coordinate.hasFull()
        self._full = full

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> ColumnTagSet:
        return ColumnTagSet(
            self._data_coordinate.graph.dimensions.names
            if self._full
            else self._data_coordinate.graph.required.names,
            {},
            {},
        )

    @property  # type: ignore
    @cached_getter
    def constraints(self) -> LocalConstraints:
        return LocalConstraints.from_misc(data_id=self._data_coordinate)

    def to_sql_booleans(
        self,
        columns: Mapping[ColumnTag, LogicalColumn],
        column_types: ColumnTypeInfo,
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        tags = set(DimensionKeyColumnTag.generate(self._data_coordinate.graph.required.names))
        if self._full:
            implied_tags = set(DimensionKeyColumnTag.generate(self._data_coordinate.graph.implied.names))
            if implied_tags <= columns.keys():
                tags.update(implied_tags)
        return [columns[tag] == self._data_coordinate[tag.dimension] for tag in tags]


class _SpatialConstraintPredicate(Predicate):
    def __init__(self, spatial_constraint: SpatialConstraint, skypix: SkyPixDimension):
        self._spatial_constraint = spatial_constraint
        self._skypix = skypix
        self._tag = DimensionKeyColumnTag(skypix.name)

    @property
    def columns_required(self) -> ColumnTagSet:
        return ColumnTagSet({self._skypix.name}, {}, {})

    @property  # type: ignore
    @cached_getter
    def constraints(self) -> LocalConstraints:
        return LocalConstraints.from_misc(spatial=self._spatial_constraint)

    def to_sql_booleans(
        self,
        columns: Mapping[ColumnTag, LogicalColumn],
        column_types: ColumnTypeInfo,
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        column = self._tag.index_scalar(columns)
        or_terms = []
        for begin, end in self._spatial_constraint.ranges(self._skypix):
            last = end - 1
            if begin == last:
                or_terms.append(column == begin)
            else:
                or_terms.append(column.between(begin, last))
        if not or_terms:
            return (sqlalchemy.sql.literal(False),)
        elif len(or_terms) == 1:
            return (or_terms[0],)
        else:
            return (sqlalchemy.sql.or_(*or_terms),)


class _TemporalConstraintPredicate(Predicate):
    def __init__(self, temporal_constraint: TemporalConstraint, column: ColumnTag):
        if not column.is_timespan:
            raise TypeError("Temporal predicate requires a timespans columns.")
        self._temporal_constraint = temporal_constraint
        self._column_tag = column

    @property
    def columns_required(self) -> ColumnTagSet:
        return ColumnTagSet._from_iterable((self._column_tag,))

    @property  # type: ignore
    @cached_getter
    def constraints(self) -> LocalConstraints:
        return LocalConstraints.from_misc(temporal=self._temporal_constraint)

    def to_sql_booleans(
        self,
        columns: Mapping[ColumnTag, LogicalColumn],
        column_types: ColumnTypeInfo,
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        logical_column = self._column_tag.index_timespan(columns)
        overlaps = [
            logical_column.overlaps(constraint_column)
            for constraint_column in column_types.timespan_cls.from_constraint(self._temporal_constraint)
        ]
        return (sqlalchemy.sql.or_(logical_column.isNull(), *overlaps),)
