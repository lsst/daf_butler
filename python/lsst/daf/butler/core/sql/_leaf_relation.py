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

__all__ = ()

from typing import AbstractSet, Mapping, Sequence

import sqlalchemy
from lsst.utils.classes import cached_getter

from ._column_tag_set import ColumnTagSet
from ._column_tags import ColumnTag, LogicalColumn
from ._column_type_info import ColumnTypeInfo
from ._local_constraints import LocalConstraints
from ._postprocessor import Postprocessor
from ._relation import Relation


class _LeafRelation(Relation):
    def __init__(
        self,
        sql_from: sqlalchemy.sql.FromClause,
        columns: Mapping[ColumnTag, LogicalColumn],
        *,
        connections: AbstractSet[frozenset[str]],
        constraints: LocalConstraints,
        column_types: ColumnTypeInfo,
        is_materialized: bool,
        is_unique: bool,
        where: Sequence[sqlalchemy.sql.ColumnElement] = (),
    ):
        self._sql_from = sql_from
        self._columns = columns
        self._constraints = constraints
        self._connections = connections
        self._where = tuple(where)
        self._column_types = column_types
        self._is_materialized = is_materialized
        self._is_unique = is_unique

    @property  # type: ignore
    @cached_getter
    def columns(self) -> ColumnTagSet:
        return ColumnTagSet._from_iterable(self._columns.keys())

    @property
    def constraints(self) -> LocalConstraints:
        return self._constraints

    @property
    def postprocessors(self) -> Sequence[Postprocessor]:
        return ()

    @property
    def connections(self) -> AbstractSet[frozenset[str]]:
        return self._connections

    @property
    def column_types(self) -> ColumnTypeInfo:
        return self._column_types

    @property
    def is_unique(self) -> bool:
        return self._is_unique

    @property
    def is_materialized(self) -> bool:
        return self._is_materialized

    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Mapping[ColumnTag, LogicalColumn],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        return self._sql_from, self._columns, self._where
