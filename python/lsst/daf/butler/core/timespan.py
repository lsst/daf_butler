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

__all__ = ("Timespan",)

import operator
from typing import Generic, Optional, TypeVar

import sqlalchemy

from .schema import FieldSpec


T = TypeVar("T")


class Timespan(Generic[T], tuple):

    def __new__(cls, begin: T, end: T):
        return tuple.__new__(cls, (begin, end))

    def overlaps(self, other, ops=operator):
        # TODO: handle None/NULL by treating as unbounded.
        return ops.not_(ops.or_(self.end < other.begin, self.begin > other.end))

    def intersection(*args) -> Optional[Timespan]:
        if len(args) == 0:
            return None
        elif len(args) == 1:
            return args[0]
        else:
            begin = max(*[ts.begin for ts in args])
            end = min(*[ts.end for ts in args])
            if begin > end:
                return None
            return Timespan(begin=begin, end=end)

    @property
    def begin(self) -> T:
        return self[0]

    @property
    def end(self) -> T:
        return self[1]

    def __getnewargs__(self) -> tuple:
        return (self.begin, self.end)


TIMESPAN_FIELD_SPECS = Timespan(
    begin=FieldSpec(name="datetime_begin", dtype=sqlalchemy.DateTime),
    end=FieldSpec(name="datetime_end", dtype=sqlalchemy.DateTime),
)


def extractColumnTimespan(table: sqlalchemy.schema.Table) -> Timespan[sqlalchemy.sql.ColumnElement]:
    return Timespan(table.columns[TIMESPAN_FIELD_SPECS.begin.name],
                    table.columns[TIMESPAN_FIELD_SPECS.end.name])
