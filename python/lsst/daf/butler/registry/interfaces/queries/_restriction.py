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

__all__ = ("Restriction",)

from abc import ABC, abstractmethod
from typing import AbstractSet, Callable, Mapping, Optional, Sequence

import sqlalchemy
from lsst.utils.classes import cached_getter

from ....core import DataCoordinate, Timespan, TimespanDatabaseRepresentation
from ._column_tags import ColumnTag, DatasetColumnTag, DimensionKeyColumnTag, SqlLogicalColumn


class Restriction(ABC):
    @property
    @abstractmethod
    def columns_required(self) -> AbstractSet[ColumnTag]:
        raise NotImplementedError()

    @abstractmethod
    def to_sql_booleans(
        self, columns: Mapping[ColumnTag, SqlLogicalColumn]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        raise NotImplementedError()

    @staticmethod
    def from_callable(
        func: Callable[[Mapping[ColumnTag, SqlLogicalColumn]], Sequence[sqlalchemy.sql.ColumnElement]],
        columns_required: AbstractSet[ColumnTag],
    ) -> Restriction:
        return _CallableRestriction(func, columns_required)

    @staticmethod
    def from_data_coordinate(data_coordinate: DataCoordinate, full: Optional[bool] = None) -> Restriction:
        return _DataCoordinateRestriction(data_coordinate, full=full)

    @staticmethod
    def from_dataset_timespan(dataset_type_name: str, timespan: Optional[Timespan]) -> Restriction:
        return _DatasetTimespanRestriction(dataset_type_name, timespan)


class _CallableRestriction(Restriction):
    def __init__(
        self,
        func: Callable[[Mapping[ColumnTag, SqlLogicalColumn]], Sequence[sqlalchemy.sql.ColumnElement]],
        columns_required: AbstractSet[ColumnTag],
    ):
        self._func = func
        self._columns_required = columns_required

    @property
    def columns_required(self) -> AbstractSet[ColumnTag]:
        return self._columns_required

    def to_sql_booleans(
        self, columns: Mapping[ColumnTag, SqlLogicalColumn]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        return self._func(columns)


class _DataCoordinateRestriction(Restriction):
    def __init__(self, data_coordinate: DataCoordinate, full: Optional[bool] = None):
        self._data_coordinate = data_coordinate
        self._full = full

    @property  # type: ignore
    @cached_getter
    def columns_required(self) -> AbstractSet[ColumnTag]:
        columns: set[ColumnTag] = set()
        columns.update(DimensionKeyColumnTag.generate(self._data_coordinate.graph.required.names))
        if self._full:
            columns.update(DimensionKeyColumnTag.generate(self._data_coordinate.graph.implied.names))
        return columns

    def to_sql_booleans(
        self, columns: Mapping[ColumnTag, SqlLogicalColumn]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        tags = set(DimensionKeyColumnTag.generate(self._data_coordinate.graph.required.names))
        if self._full or (self._full is None and self._data_coordinate.hasFull()):
            implied_tags = set(DimensionKeyColumnTag.generate(self._data_coordinate.graph.implied.names))
            if implied_tags <= columns.keys():
                tags.update(implied_tags)
        return [columns[tag] == self._data_coordinate[tag.dimension] for tag in tags]


class _DatasetTimespanRestriction(Restriction):
    def __init__(self, dataset_type_name: str, timespan: Optional[Timespan]):
        self._tag = DatasetColumnTag(dataset_type_name, "timespan")
        self._timespan = timespan

    @property
    def columns_required(self) -> AbstractSet[ColumnTag]:
        return {self._tag}

    def to_sql_booleans(
        self, columns: Mapping[ColumnTag, SqlLogicalColumn]
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        columns_timespan: TimespanDatabaseRepresentation = columns[self._tag]  # type: ignore
        return (columns_timespan.overlaps(type(columns_timespan).fromLiteral(self._timespan)),)
