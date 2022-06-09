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

__all__ = ("JoinCondition",)

from abc import ABC, abstractmethod
from typing import AbstractSet, Mapping, Sequence

import sqlalchemy

from ._column_tags import ColumnTag, LogicalColumn
from ._column_type_info import ColumnTypeInfo


class JoinCondition(ABC):
    def reversed(self) -> JoinCondition:
        return _ReversedJoinCondition(self)

    @abstractmethod
    def match_lhs(self, columns: AbstractSet[ColumnTag]) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def match_rhs(self, columns: AbstractSet[ColumnTag]) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def to_sql_booleans(
        self,
        lhs: Mapping[ColumnTag, LogicalColumn],
        rhs: Mapping[ColumnTag, LogicalColumn],
        column_types: ColumnTypeInfo,
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        raise NotImplementedError()

    @staticmethod
    def make_temporal(tag1: ColumnTag, tag2: ColumnTag) -> JoinCondition:
        return _TemporalJoinCondition(tag1, tag2)


class _ReversedJoinCondition(JoinCondition):
    def __init__(self, base: JoinCondition):
        self._base = base

    def reversed(self) -> JoinCondition:
        return self._base

    def match_lhs(self, columns: AbstractSet[ColumnTag]) -> bool:
        return self._base.match_rhs(columns)

    def match_rhs(self, columns: AbstractSet[ColumnTag]) -> bool:
        return self._base.match_lhs(columns)

    def to_sql_booleans(
        self,
        lhs: Mapping[ColumnTag, LogicalColumn],
        rhs: Mapping[ColumnTag, LogicalColumn],
        column_types: ColumnTypeInfo,
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        return self._base.to_sql_booleans(rhs, lhs, column_types)


class _TemporalJoinCondition(JoinCondition):
    def __init__(self, tag1: ColumnTag, tag2: ColumnTag):
        self._tag1 = tag1
        self._tag2 = tag2

    def match_lhs(self, columns: AbstractSet[ColumnTag]) -> bool:
        return self._tag1 in columns

    def match_rhs(self, columns: AbstractSet[ColumnTag]) -> bool:
        return self._tag2 in columns

    def to_sql_booleans(
        self,
        lhs: Mapping[ColumnTag, LogicalColumn],
        rhs: Mapping[ColumnTag, LogicalColumn],
        column_types: ColumnTypeInfo,
    ) -> Sequence[sqlalchemy.sql.ColumnElement]:
        if self._tag1 in lhs and self._tag2 in rhs:
            col1 = self._tag1.index_timespan(lhs)
            col2 = self._tag2.index_timespan(rhs)
            return (sqlalchemy.sql.or_(col1.isNull(), col2.isNull(), col1.overlaps(col2)),)
        else:
            return ()
