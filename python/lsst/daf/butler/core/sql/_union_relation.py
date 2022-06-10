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

from typing import AbstractSet, Iterable, Iterator, Optional, Sequence

import sqlalchemy
from lsst.utils.classes import cached_getter

from ._column_tag_set import ColumnTagSet
from ._column_tags import ColumnTag
from ._column_type_info import ColumnTypeInfo
from ._local_constraints import LocalConstraints
from ._order_by import OrderByTerm
from ._postprocessor import Postprocessor
from ._predicate import Predicate
from ._relation import Relation


class _UnionRelation(Relation):
    def __init__(self, *terms: Relation, extra_doomed_by: AbstractSet[str]):
        assert (
            len(terms) > 1
        ), "_Union should always be created by a factory that avoids empty or 1-element iterables."
        self._terms = terms
        self._extra_doomed_by = extra_doomed_by

    @property
    def columns(self) -> ColumnTagSet:
        return self._terms[0].columns

    @property  # type: ignore
    @cached_getter
    def constraints(self) -> LocalConstraints:
        return LocalConstraints.union(*[t.constraints for t in self._terms])

    @property  # type: ignore
    @cached_getter
    def postprocessors(self) -> Sequence[Postprocessor]:
        p: list[Postprocessor] = []
        for term in self._terms:
            p.extend(term.postprocessors)
        return Postprocessor.sort_and_assert(p, self.columns)

    @property  # type: ignore
    @cached_getter
    def connections(self) -> AbstractSet[frozenset[str]]:
        first, *rest = self._terms
        result = set(first.connections)
        for term in rest:
            result.intersection_update(term.connections)
        return result

    @property
    def column_types(self) -> ColumnTypeInfo:
        return self._terms[0].column_types

    @property
    def is_unique(self) -> bool:
        return True

    @property  # type: ignore
    @cached_getter
    def doomed_by(self) -> AbstractSet[str]:
        result = set(self._extra_doomed_by)
        for term in self._terms:
            if not term.doomed_by:
                return frozenset()
            result.update(term.doomed_by)
        return result

    def projected(self, columns: Iterable[ColumnTag], *, keep_missing: bool = False) -> Relation:
        return Relation.union(*[term.projected(columns, keep_missing=keep_missing) for term in self._terms])

    def selected(self, *predicates: Predicate) -> Relation:
        return Relation.union(*[term.selected(*predicates) for term in self._terms])

    def to_sql_parts(self) -> tuple[sqlalchemy.sql.FromClause, None, Sequence[sqlalchemy.sql.ColumnElement]]:
        return self.to_sql_executable().subquery(), None, ()

    def to_sql_executable(
        self,
        *,
        force_unique: bool = False,
        order_by: Iterable[OrderByTerm] = (),
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> sqlalchemy.sql.CompoundSelect:
        select: sqlalchemy.sql.CompoundSelect = sqlalchemy.sql.union(
            *[term.to_sql_executable() for term in self._terms]
        )
        # force_unique ignored because UNION always yields unique rows.
        if order_by:
            columns_available = columns_available = ColumnTag.extract_logical_column_mapping(
                self.columns,
                select.selected_columns,
                self.column_types,
            )
            select = select.order_by(*[t.to_sql_scalar(columns_available) for t in order_by])
        if offset:
            select = select.offset(offset)
        if limit is not None:
            select = select.limit(limit)
        return select

    def _flatten_unions(self) -> Iterator[Relation]:
        return iter(self._terms)

    def _flatten_doomed_by(self) -> Iterable[str]:
        result = set(self._extra_doomed_by)
        for term in self._terms:
            result.update(term._flatten_doomed_by())
        return result
