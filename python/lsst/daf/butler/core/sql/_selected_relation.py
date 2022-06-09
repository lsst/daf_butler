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

from typing import Mapping, Optional, Sequence

import sqlalchemy
from lsst.utils.classes import cached_getter

from ._column_tag_set import ColumnTagSet
from ._column_tags import ColumnTag, LogicalColumn
from ._column_type_info import ColumnTypeInfo
from ._local_constraints import LocalConstraints
from ._postprocessor import Postprocessor
from ._predicate import Predicate
from ._relation import Relation


class _SelectedRelation(Relation):
    def __init__(self, *predicates: Predicate, base: Relation):
        for p in predicates:
            if not p.columns_required <= base.columns:
                # TODO: consider better exception type once it becomes clear
                # whether this is always a butler-internal logic bug or not.
                raise RuntimeError(f"Predicate {p} needs columns {p.columns_required - base.columns}.")
        self._predicates = predicates
        self._base = base

    @property
    def columns(self) -> ColumnTagSet:
        return self._base.columns

    @property  # type: ignore
    @cached_getter
    def constraints(self) -> LocalConstraints:
        return self._base.constraints.intersection(*[p.constraints for p in self._predicates])

    @property
    def postprocessors(self) -> Sequence[Postprocessor]:
        return self._base.postprocessors

    @property
    def column_types(self) -> ColumnTypeInfo:
        return self._base.column_types

    @property
    def is_unique(self) -> bool:
        return self._base.is_unique

    def selected(self, *predicates: Predicate) -> Relation:
        return _SelectedRelation(*(self._predicates + predicates), base=self._base)

    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Mapping[ColumnTag, LogicalColumn],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        columns_available: Optional[Mapping[ColumnTag, LogicalColumn]]
        base_from, columns_available, base_where = self._base.to_sql_parts()
        if columns_available is None:
            columns_available = ColumnTag.extract_logical_column_mapping(
                self._base.columns, base_from.columns, self.column_types
            )
        full_where = list(base_where)
        for r in self._predicates:
            full_where.extend(r.to_sql_booleans(columns_available, self.column_types))
        return base_from, columns_available, full_where
