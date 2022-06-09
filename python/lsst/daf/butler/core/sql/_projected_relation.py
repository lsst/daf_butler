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


class _ProjectedRelation(Relation):
    def __init__(self, columns: ColumnTagSet, base: Relation, *, keep_missing: bool):
        self._base = base
        self._columns = columns
        self._postprocessors, todo, missing = Postprocessor.sort_and_check(self._base.postprocessors, columns)
        if missing:
            if keep_missing:
                new_columns = set(columns)
                new_columns.update(missing)
                self._postprocessors = Postprocessor.sort_and_assert(self._base.postprocessors, new_columns)
                self._columns = ColumnTagSet._from_iterable(new_columns)
            else:
                raise RuntimeError(f"Missing column(s) {missing} needed to satisfy postprocessor(s) {todo}.")

    @property
    def columns(self) -> ColumnTagSet:
        return self._columns

    @property
    def constraints(self) -> LocalConstraints:
        return self._base.constraints

    @property
    def postprocessors(self) -> Sequence[Postprocessor]:
        return self._postprocessors

    @property
    def column_types(self) -> ColumnTypeInfo:
        return self._base.column_types

    @property
    def is_materialized(self) -> bool:
        return self._base.is_materialized

    @property
    def doomed_by(self) -> AbstractSet[str]:
        return self._base.doomed_by

    def projected(self, columns: Iterable[ColumnTag], *, keep_missing: bool = False) -> Relation:
        return _ProjectedRelation(
            ColumnTagSet._from_iterable(columns), base=self._base, keep_missing=keep_missing
        )

    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Optional[Mapping[ColumnTag, LogicalColumn]],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        return self._base.to_sql_parts()
