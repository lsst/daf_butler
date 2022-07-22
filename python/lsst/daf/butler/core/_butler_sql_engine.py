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

__all__ = ("ButlerSqlEngine", "LogicalColumn")

import dataclasses
from collections.abc import Iterable, Set
from typing import Union, cast

import sqlalchemy
from lsst.daf.relation import sql

from ._column_tags import ColumnTag
from ._spatial_regions import SpatialRegionDatabaseRepresentation
from .ddl import FieldSpec
from .dimensions import DimensionUniverse
from .timespan import TimespanDatabaseRepresentation

LogicalColumn = Union[
    sqlalchemy.sql.ColumnElement, SpatialRegionDatabaseRepresentation, TimespanDatabaseRepresentation
]
"""A type alias for the types used to represent columns in SQL relations.

This is the butler specialization of the `lsst.daf.relation.sql.LogicalColumn`
concept.
"""


@dataclasses.dataclass(frozen=True, eq=False)
class ButlerSqlEngine(sql.Engine[ColumnTag, LogicalColumn]):
    """A struct that aggregates information about column types that can differ
    across data repositories due to `Registry` and dimension configuration.
    """

    spatial_region_cls: type[SpatialRegionDatabaseRepresentation]
    """An abstraction around the column type or types used for spatial regions
    by this database engine.
    """

    timespan_cls: type[TimespanDatabaseRepresentation]
    """An abstraction around the column type or types used for timespans by
    this database engine.
    """

    universe: DimensionUniverse
    """Object that manages the definitions of all dimension and dimension
    elements.
    """

    dataset_id_spec: FieldSpec
    """Field specification for the dataset primary key column.
    """

    run_key_spec: FieldSpec
    """Field specification for the `~CollectionType.RUN` primary key column.
    """

    def extract_mapping(
        self, tags: Set[ColumnTag], sql_columns: sqlalchemy.sql.ColumnCollection
    ) -> dict[ColumnTag, LogicalColumn]:
        # Docstring inherited.
        result: dict[ColumnTag, LogicalColumn] = {}
        for tag in tags:
            if tag.is_spatial_region:
                result[tag] = self.spatial_region_cls.from_columns(sql_columns, name=str(tag))
            elif tag.is_timespan:
                result[tag] = self.timespan_cls.from_columns(sql_columns, name=str(tag))
            else:
                result[tag] = sql_columns[str(tag)]
        return result

    def select_items(
        self,
        items: Iterable[tuple[ColumnTag, LogicalColumn]],
        sql_from: sqlalchemy.sql.FromClause,
        *extra: sqlalchemy.sql.ColumnElement,
    ) -> sqlalchemy.sql.Select:
        # Docstring inherited.
        select_columns: list[sqlalchemy.sql.ColumnElement] = []
        for tag, logical_column in items:
            if tag.is_spatial_region:
                select_columns.extend(
                    cast(SpatialRegionDatabaseRepresentation, logical_column).flatten(name=str(tag))
                )
            elif tag.is_timespan:
                select_columns.extend(
                    cast(TimespanDatabaseRepresentation, logical_column).flatten(name=str(tag))
                )
            else:
                select_columns.append(cast(sqlalchemy.sql.ColumnElement, logical_column).label(str(tag)))
        select_columns.extend(extra)
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).select_from(sql_from)

    def make_zero_select(self, tags: Set[ColumnTag]) -> sqlalchemy.sql.Select:
        # Docstring inherited.
        select_columns: list[sqlalchemy.sql.ColumnElement] = []
        for tag in tags:
            if tag.is_timespan:
                select_columns.extend(self.timespan_cls.fromLiteral(None).flatten(name=str(tag)))
            else:
                select_columns.append(sqlalchemy.sql.literal(None).label(str(tag)))
        self.handle_empty_columns(select_columns)
        return sqlalchemy.sql.select(*select_columns).where(sqlalchemy.sql.literal(False))
