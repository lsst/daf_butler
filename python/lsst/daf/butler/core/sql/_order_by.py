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

__all__ = ("OrderByTerm",)

from abc import ABC, abstractmethod
from typing import AbstractSet, Mapping

import sqlalchemy

from ._column_tags import ColumnTag, LogicalColumn


class OrderByTerm(ABC):

    __slots__ = ()

    @property
    @abstractmethod
    def columns(self) -> AbstractSet[ColumnTag]:
        raise NotImplementedError()

    def reversed(self) -> OrderByTerm:
        return _DescendingOrderByTerm(self)

    @abstractmethod
    def to_sql_scalar(
        self, columns_available: Mapping[ColumnTag, LogicalColumn]
    ) -> sqlalchemy.sql.ColumnElement:
        raise NotImplementedError()

    @staticmethod
    def asc(tag: ColumnTag) -> OrderByTerm:
        return _SimpleOrderByTerm(tag)

    @staticmethod
    def desc(tag: ColumnTag) -> OrderByTerm:
        return OrderByTerm.asc(tag).reversed()

    @staticmethod
    def from_timespan_bound(tag: ColumnTag, subfield: str) -> OrderByTerm:
        return _TimespanBoundOrderByTerm(tag, subfield)


class _SimpleOrderByTerm(OrderByTerm):

    __slots__ = ("_tag",)

    def __init__(self, tag: ColumnTag):
        self._tag = tag

    @property
    def columns(self) -> AbstractSet[ColumnTag]:
        return {self._tag}

    def to_sql_scalar(
        self, columns_available: Mapping[ColumnTag, LogicalColumn]
    ) -> sqlalchemy.sql.ColumnElement:
        return self._tag.index_scalar(columns_available)


class _DescendingOrderByTerm(OrderByTerm):

    __slots__ = ("_base",)

    def __init__(self, base: OrderByTerm):
        self._base = base

    @property
    def columns(self) -> AbstractSet[ColumnTag]:
        return self._base.columns

    def reversed(self) -> OrderByTerm:
        return self._base

    def to_sql_scalar(
        self, columns_available: Mapping[ColumnTag, LogicalColumn]
    ) -> sqlalchemy.sql.ColumnElement:
        return self._base.to_sql_scalar(columns_available).desc()


class _TimespanBoundOrderByTerm(OrderByTerm):

    __slots__ = ("_tag", "_subfield")

    def __init__(self, tag: ColumnTag, subfield: str):
        self._tag = tag
        self._subfield = subfield

    @property
    def columns(self) -> AbstractSet[ColumnTag]:
        return frozenset({self._tag})

    def to_sql_scalar(
        self, columns_available: Mapping[ColumnTag, LogicalColumn]
    ) -> sqlalchemy.sql.ColumnElement:
        timespan_repr = self._tag.index_timespan(columns_available)
        match self._subfield:
            case "begin":
                return timespan_repr.lower()
            case "end":
                return timespan_repr.upper()
            case _:
                raise ValueError(f"Invalid timespan subfield {self._subfield!r}; expected 'begin' or 'end'.")
