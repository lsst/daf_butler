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

__all__ = ("Relation", "RelationBuilder")

import dataclasses
from abc import ABC, abstractmethod
from typing import AbstractSet, Iterable, Mapping, Optional, Sequence

import sqlalchemy
from lsst.utils.classes import cached_getter

from ._column_tag_set import ColumnTagSet
from ._column_tags import ColumnTag, DimensionKeyColumnTag, LogicalColumn
from ._column_type_info import ColumnTypeInfo
from ._join_condition import JoinCondition
from ._local_constraints import LocalConstraints
from ._order_by import OrderByTerm
from ._postprocessor import Postprocessor
from ._predicate import Predicate


class Relation(ABC):
    @property
    @abstractmethod
    def columns(self) -> ColumnTagSet:
        raise NotImplementedError()

    @property
    @abstractmethod
    def constraints(self) -> LocalConstraints:
        raise NotImplementedError()

    @property
    @abstractmethod
    def postprocessors(self) -> Sequence[Postprocessor]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def connections(self) -> AbstractSet[frozenset[str]]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def column_types(self) -> ColumnTypeInfo:
        raise NotImplementedError()

    @property
    def is_unique(self) -> bool:
        return False

    @property
    def is_materialized(self) -> bool:
        return False

    @property  # type: ignore
    @cached_getter
    def doomed_by(self) -> AbstractSet[str]:
        result = set()
        for key, bounds in self.constraints.dimensions.items():
            if not bounds:
                result.add(f"No values permitted for dimension {key}.")
        if not self.constraints.spatial:
            result.add("Spatial constraint is empty.")
        if not self.constraints.temporal:
            result.add("Temporal constraint is empty.")
        return result

    def postprocessed(self, *postprocessors: Postprocessor) -> Relation:
        from ._postprocessed_relation import _PostprocessedRelation

        if not postprocessors:
            return self
        p = list(self.postprocessors)
        p.extend(postprocessors)
        return _PostprocessedRelation(p, base=self)

    def projected(self, columns: Iterable[ColumnTag], *, keep_missing: bool = False) -> Relation:
        from ._projected_relation import _ProjectedRelation

        columns = ColumnTagSet._from_iterable(columns)
        if columns == self.columns:
            return self
        return _ProjectedRelation(columns, base=self, keep_missing=keep_missing)

    def selected(self, *predicates: Predicate) -> Relation:
        from ._selected_relation import _SelectedRelation

        if not predicates:
            return self
        return _SelectedRelation(*predicates, base=self)

    def forced_unique(self) -> Relation:
        if not self.is_unique:
            from ._forced_unique_relation import _ForcedUniqueRelation

            return _ForcedUniqueRelation(self)
        else:
            return self

    def sliced(
        self, order_by: Iterable[OrderByTerm], offset: int = 0, limit: Optional[int] = None
    ) -> Relation:
        order_by = tuple(order_by)
        # TypeError may seem strange below, but it's what Python usually raises
        # when you pass an invalid combination of arguments to a function.
        if not order_by:
            raise TypeError(
                "Cannot slice an unordered relation; to obtain an arbitrary "
                "set of result rows from an unordered relation, pass offset "
                "and/or limit to_sql_executable when executing it."
            )
        if not offset and limit is None:
            raise TypeError(
                "Cannot order a relation unless it is being sliced with "
                "nontrivial offset and/or limit; to obtain ordered rows from "
                "a relation, pass order_by to to_sql_executable when "
                "executing it."
            )
        from ._sliced_relation import _SlicedRelation

        return _SlicedRelation(self, order_by=order_by, offset=offset, limit=limit)

    @abstractmethod
    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Optional[Mapping[ColumnTag, LogicalColumn]],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        raise NotImplementedError()

    def to_sql_from(self) -> sqlalchemy.sql.FromClause:
        sql_from, columns_available, where = self.to_sql_parts()
        if not where and columns_available is None:
            return sql_from
        return self._sql_parts_to_select(sql_from, columns_available, where).subquery()

    def to_sql_executable(
        self,
        *,
        force_unique: bool = False,
        order_by: Iterable[OrderByTerm] = (),
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> sqlalchemy.sql.expression.SelectBase:
        return self._sql_parts_to_select(
            *self.to_sql_parts(),
            force_unique=force_unique,
            order_by=order_by,
            offset=offset,
            limit=limit,
        )

    def _sql_parts_to_select(
        self,
        sql_from: sqlalchemy.sql.FromClause,
        columns_available: Optional[Mapping[ColumnTag, LogicalColumn]],
        where: Sequence[sqlalchemy.sql.ColumnElement],
        *,
        force_unique: bool = False,
        order_by: Iterable[OrderByTerm] = (),
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> sqlalchemy.sql.Select:
        if columns_available is None:
            columns_available = ColumnTag.extract_logical_column_mapping(
                self.columns,
                sql_from.columns,
                self.column_types,
            )
            columns_to_select = columns_available
        else:
            columns_to_select = {tag: columns_available[tag] for tag in self.columns}
        select = ColumnTag.select_logical_column_items(columns_to_select.items(), sql_from)
        if len(where) == 1:
            select = select.where(where[0])
        elif where:
            select = select.where(sqlalchemy.sql.and_(*where))
        if force_unique and not self.is_unique:
            select = select.distinct()
        if order_by:
            select = select.order_by(*[t.to_sql_scalar(columns_available) for t in order_by])
        if offset:
            select = select.offset(offset)
        if limit is not None:
            select = select.limit(limit)
        return select

    @staticmethod
    def build(sql_from: sqlalchemy.sql.FromClause, column_types: ColumnTypeInfo) -> RelationBuilder:
        return RelationBuilder(sql_from, column_types)

    @staticmethod
    def make_unit(constant_row: sqlalchemy.sql.FromClause, column_types: ColumnTypeInfo) -> Relation:
        from ._unit_relation import _UnitRelation

        return _UnitRelation(constant_row, column_types)

    @staticmethod
    def make_doomed(
        first_message: str,
        *other_messages: str,
        constant_row: sqlalchemy.sql.FromClause,
        columns: ColumnTagSet,
        column_types: ColumnTypeInfo,
        connections: AbstractSet[frozenset[str]] = frozenset(),
    ) -> Relation:
        from ._doomed_relation import _DoomedRelation

        doomed_by = {first_message}
        doomed_by.update(other_messages)
        return _DoomedRelation(constant_row, columns, doomed_by, column_types, connections)

    def join(
        self: Relation,
        *others: Relation,
        conditions: Iterable[JoinCondition] = (),
    ) -> Relation:  # noqa: N805
        from ._join_relation import _JoinRelation

        # If an argument's `_flatten_joins` yields no terms (e.g. a unit
        # relation), we usually want to leave it out of a join, or if there's
        # only one other argument, we want to just return the other argument.
        # But if there are only unit arguments, we want to return that unit
        # argument.
        fallback = self
        terms = list(self._flatten_join_terms())
        conditions = list(conditions)
        for other in others:
            new_terms = list(other._flatten_join_terms())
            if new_terms:
                fallback = other
            terms.extend(new_terms)
            conditions.extend(other._flatten_join_conditions())
        if len(terms) < 2:
            if conditions:
                raise RuntimeError("Cannot add join conditions with only one relation.")
            return fallback
        return _JoinRelation(*terms, conditions=conditions)

    def union(self: Relation, *others: Relation) -> Relation:  # noqa: N805
        from ._union_relation import _UnionRelation

        # See `join` for what this fallback logic does; in this case it's
        # any doomed relation that plays the role of the unit relation, and
        # the doomed_by messages that play the role of the temporal/skypix
        # joins.
        fallback = self
        terms = list(self._flatten_unions())
        extra_doomed_by = set(self._flatten_doomed_by())
        for other in others:
            new_terms = list(other._flatten_unions())
            if new_terms:
                fallback = other
            terms.extend(new_terms)
            extra_doomed_by.update(other._flatten_doomed_by())
        if len(terms) < 2:
            return fallback
        return _UnionRelation(*terms, extra_doomed_by=extra_doomed_by)

    def _flatten_join_terms(self) -> Iterable[Relation]:
        return (self,)

    def _flatten_join_conditions(self) -> Iterable[JoinCondition]:
        return ()

    def _flatten_unions(self) -> Iterable[Relation]:
        if not self.doomed_by:
            return (self,)
        else:
            return ()

    def _flatten_doomed_by(self) -> Iterable[str]:
        if not self.doomed_by:
            return frozenset()
        else:
            return self.doomed_by


@dataclasses.dataclass
class RelationBuilder:

    sql_from: sqlalchemy.sql.FromClause
    column_types: ColumnTypeInfo
    columns: dict[ColumnTag, LogicalColumn] = dataclasses.field(default_factory=dict)
    where: list[sqlalchemy.sql.FromClause] = dataclasses.field(default_factory=list)

    def add(self, *args: str, **kwargs: ColumnTag) -> None:
        for name in args:
            self.columns[DimensionKeyColumnTag(name)] = self.sql_from.columns[name]
        for name, tag in kwargs.items():
            self.columns[tag] = tag.extract_logical_column(
                self.sql_from.columns, self.column_types, name=name
            )

    def finish(
        self,
        constraints: Optional[LocalConstraints] = None,
        is_materialized: bool = False,
        is_unique: bool = False,
        connections: Iterable[frozenset[str]] = frozenset(),
    ) -> Relation:
        from ._leaf_relation import _LeafRelation

        if constraints is None:
            constraints = LocalConstraints.make_full()
        return _LeafRelation(
            self.sql_from,
            self.columns,
            connections=frozenset(connections),
            constraints=constraints,
            where=tuple(self.where),
            column_types=self.column_types,
            is_materialized=is_materialized,
            is_unique=is_unique,
        )
