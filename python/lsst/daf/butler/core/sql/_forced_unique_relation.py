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

from typing import AbstractSet, Iterable, Mapping, Optional, Sequence

import sqlalchemy

from ._column_tag_set import ColumnTagSet
from ._column_tags import ColumnTag, LogicalColumn
from ._column_type_info import ColumnTypeInfo
from ._local_constraints import LocalConstraints
from ._order_by import OrderByTerm
from ._postprocessor import Postprocessor
from ._relation import Relation


class _ForcedUniqueRelation(Relation):
    def __init__(self, base: Relation):
        self._base = base

    @property
    def columns(self) -> ColumnTagSet:
        return self._base.columns

    @property
    def constraints(self) -> LocalConstraints:
        return self._base.constraints

    @property
    def postprocessors(self) -> Sequence[Postprocessor]:
        return self._base.postprocessors

    @property
    def connections(self) -> AbstractSet[frozenset[str]]:
        return self._base.connections

    @property
    def column_types(self) -> ColumnTypeInfo:
        return self._base.column_types

    @property
    def is_unique(self) -> bool:
        return True

    @property
    def doomed_by(self) -> AbstractSet[str]:
        return self._base.doomed_by

    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Optional[Mapping[ColumnTag, LogicalColumn]],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        return self.to_sql_executable().subquery(), None, ()

    def to_sql_executable(
        self,
        *,
        force_unique: bool = False,
        order_by: Iterable[OrderByTerm] = (),
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> sqlalchemy.sql.expression.SelectBase:
        return self._base.to_sql_executable(force_unique=True, order_by=order_by, offset=offset, limit=limit)
