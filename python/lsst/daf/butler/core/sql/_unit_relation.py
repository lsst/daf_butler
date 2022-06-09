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
from ._postprocessor import Postprocessor
from ._relation import Relation


class _UnitRelation(Relation):
    def __init__(self, constant_row: sqlalchemy.sql.FromClause, column_types: ColumnTypeInfo):
        self._column_types = column_types
        self._constant_row = constant_row

    @property
    def columns(self) -> ColumnTagSet:
        return ColumnTagSet(frozenset(), {}, {})

    @property
    def constraints(self) -> LocalConstraints:
        return LocalConstraints.make_full()

    @property
    def postprocessors(self) -> Sequence[Postprocessor]:
        return ()

    @property
    def column_types(self) -> ColumnTypeInfo:
        return self._column_types

    @property
    def is_unique(self) -> bool:
        return True

    @property
    def is_materialized(self) -> bool:
        return True

    @property
    def doomed_by(self) -> AbstractSet[str]:
        return frozenset()

    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Optional[Mapping[ColumnTag, LogicalColumn]],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        return self._constant_row, {}, []

    def _flatten_join_terms(self) -> Iterable[Relation]:
        return ()
