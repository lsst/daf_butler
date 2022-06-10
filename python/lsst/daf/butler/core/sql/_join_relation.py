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

import enum
from collections import defaultdict, deque
from typing import AbstractSet, Iterable, Iterator, Mapping, NewType, Optional, Sequence

import sqlalchemy
from lsst.utils.classes import cached_getter

from ._column_tag_set import ColumnTagSet
from ._column_tags import ColumnTag, LogicalColumn
from ._column_type_info import ColumnTypeInfo
from ._join_condition import JoinCondition
from ._local_constraints import LocalConstraints
from ._postprocessor import Postprocessor
from ._relation import Relation

_RelationIndex = NewType("_RelationIndex", int)
_ConditionIndex = NewType("_ConditionIndex", int)


class _MatchSide(enum.Enum):
    LHS = enum.auto()
    RHS = enum.auto()

    def reverse_if_rhs(self, condition: JoinCondition) -> JoinCondition:
        if self is _MatchSide.RHS:
            return condition.reversed()
        else:
            return condition


class _JoinRelation(Relation):
    def __init__(
        self,
        *relations: Relation,
        conditions: Iterable[JoinCondition] = (),
        extra_connections: Iterable[frozenset[str]] = (),
    ):
        assert (
            len(relations) > 1
        ), "_Join should always be created by a factory that avoids empty or 1-element iterables."
        self._relations = {_RelationIndex(index): relation for index, relation in enumerate(relations)}
        self._conditions = {_ConditionIndex(index): condition for index, condition in enumerate(conditions)}
        self._matches: defaultdict[_RelationIndex, dict[_ConditionIndex, _MatchSide]] = defaultdict(dict)
        self._extra_connections = frozenset(extra_connections)
        for condition_index, condition in self._conditions.items():
            matched_lhs = False
            matched_rhs = False
            for relation_index, relation in self._relations.items():
                if condition.match_lhs(relation.columns):
                    self._matches[relation_index][condition_index] = _MatchSide.LHS
                    matched_lhs = True
                elif condition.match_rhs(relation.columns):
                    self._matches[relation_index][condition_index] = _MatchSide.RHS
                    matched_rhs = True
            if not (matched_lhs and matched_rhs):
                raise RuntimeError(f"Missing columns for join condition {condition}.")

    @property  # type: ignore
    @cached_getter
    def columns(self) -> ColumnTagSet:
        return ColumnTagSet.union(*[r.columns for r in self._relations.values()])

    @property  # type: ignore
    @cached_getter
    def constraints(self) -> LocalConstraints:
        return LocalConstraints.intersection(*[r.constraints for r in self._relations.values()])

    @property  # type: ignore
    @cached_getter
    def postprocessors(self) -> Sequence[Postprocessor]:
        p: list[Postprocessor] = []
        for relation in self._relations.values():
            p.extend(relation.postprocessors)
        return Postprocessor.sort_and_assert(p, self.columns)

    @property  # type: ignore
    @cached_getter
    def connections(self) -> AbstractSet[frozenset[str]]:
        result: set[frozenset[str]] = set(self._extra_connections)
        for relation in self._relations.values():
            result.update(relation.connections)
        return result

    @property
    def column_types(self) -> ColumnTypeInfo:
        return next(iter(self._relations.values())).column_types

    @property  # type: ignore
    @cached_getter
    def is_unique(self) -> bool:
        return all(r.is_unique for r in self._relations.values())

    @property  # type: ignore
    @cached_getter
    def doomed_by(self) -> AbstractSet[str]:
        result = set(super().doomed_by)
        for relation in self._relations.values():
            result.update(relation.doomed_by)
        return result

    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Mapping[ColumnTag, LogicalColumn],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        first_term, *other_terms = self._sorted_terms()
        full_columns: Optional[Mapping[ColumnTag, LogicalColumn]]
        first_relation, first_condition = first_term
        assert not first_condition, "first relation should not have any join conditions"
        full_sql, full_columns, first_where = first_relation.to_sql_parts()
        full_where = list(first_where)
        if full_columns is None:
            full_columns = ColumnTag.extract_logical_column_mapping(
                first_relation.columns,
                full_sql.columns,
                first_relation.column_types,
            )
        else:
            full_columns = dict(full_columns)
        for term_relation, term_conditions in other_terms:
            term_columns: Optional[Mapping[ColumnTag, LogicalColumn]]
            term_sql, term_columns, term_where = term_relation.to_sql_parts()
            if term_columns is None:
                term_columns = ColumnTag.extract_logical_column_mapping(
                    term_relation.columns,
                    term_sql.columns,
                    term_relation.column_types,
                )
            full_sql = full_sql.join(
                term_sql,
                onclause=self._compute_join_on_condition(
                    full_columns,
                    term_columns,
                    conditions=term_conditions,
                ),
            )
            full_columns.update(term_columns)
            full_where.extend(term_where)
        return full_sql, full_columns, full_where

    def _flatten_join_relations(self) -> Iterable[Relation]:
        return self._relations.values()

    def _flatten_join_conditions(self) -> Iterable[JoinCondition]:
        return self._conditions.values()

    def _flatten_join_extra_connections(self) -> Iterable[frozenset[str]]:
        return self._extra_connections

    def _sorted_terms(self) -> Iterator[tuple[Relation, Iterable[JoinCondition]]]:
        # Sort terms by the number of columns they provide, in reverse, and put
        # them in a deque.  We want to join terms into the SQL query in an
        # order such that each join's ON clause has something in common with
        # the ones that preceded it, and to find out if that's impossible and
        # hence a Cartesian join is needed.  Starting with the terms that have
        # the most columns is a good initial guess for such an ordering.
        # Note that this does not take into account temporal joins.
        todo: deque[_RelationIndex] = deque(
            sorted(self._relations.keys(), key=lambda i: len(self._relations[i].columns))
        )
        assert len(todo) > 1, "No join needed for 0 or 1 clauses."
        # Start an outer loop over all relations.
        # We now refine the relation order, popping terms from the front of
        # `todo` and yielding them when we we have the kind of ON condition
        # we want.
        while todo:
            candidate = todo.popleft()
            yield self._relations[candidate], ()
            columns_seen: set[ColumnTag] = set(self._relations[candidate].columns)
            # A dict of conditions that have one side matched by columns_seen.
            half_matched_conditions: dict[_ConditionIndex, _MatchSide] = self._matches[candidate].copy()
            # A list of relations we haven't been able to join to columns_seen.
            # We'll move relations to `rejected` from `todo` and back in this
            # inner loop, until we either finish or everything gets rejected.
            rejected: list[_RelationIndex] = []
            while todo:
                candidate = todo.popleft()
                # Look for special join conditions that connect the candidate
                # relation to a previous one (or some combination of previous
                # ones, actually).
                fully_matched_conditions = {
                    # We want to guarantee that the condition is ordered such
                    # that the LHS matches columns_seen and the RHS matches
                    # the relation we're about to yield.
                    index: half_matched_conditions[index].reverse_if_rhs(self._conditions[index])
                    # These conditions match both columns_seen (i.e. they're in
                    # half_matched_conditions) and the candidates columns...
                    for index in half_matched_conditions.keys() & self._matches[candidate].keys()
                    # ...and they match each of those on different sides.
                    if half_matched_conditions[index] is not self._matches[candidate][index]
                }
                if (
                    self._relations[candidate].columns.isdisjoint(columns_seen)
                    and not fully_matched_conditions
                ):
                    # We don't have any way to connect already seen columns to
                    # this relation.  We put this relation in the rejected list
                    # for now, and let the loop continue to try the next one.
                    rejected.append(candidate)
                else:
                    # This candidate does have some column overlap.  In
                    # addition to appending it to self._terms, we reset the
                    # `rejected` list by transferring its contents to the end
                    # of `todo`, since this new term may have some column
                    # overlap with those we've previously rejected.
                    yield self._relations[candidate], fully_matched_conditions.values()
                    columns_seen.update(self._relations[candidate].columns)
                    todo.extend(rejected)
                    rejected.clear()
                    # We now add conditions from the candidate to the
                    # half-matched dictionary, but remove those we just
                    # yielded.
                    half_matched_conditions.update(self._matches[candidate])
                    for index in fully_matched_conditions.keys():
                        del half_matched_conditions[index]
            if rejected:
                # We've processed all relations that could be connected to the
                # starting one by at least one column or special JoinCondition.
                # In the future, we could guard against unintentional Cartesian
                # products here (see e.g. DM-33147), by checking for common
                # mistakes, emitting warnings, looking for some feature flag
                # that says to enable them, etc.  For now we preserve current
                # behavior by just permitting them.  But we still need to see
                # if any of these "rejected" relations can be connected to
                # each other.  So we start the algorithm again by returning to
                # the outermost loop.
                todo.extend(rejected)
                rejected.clear()

    def _compute_join_on_condition(
        self,
        a: Mapping[ColumnTag, LogicalColumn],
        b: Mapping[ColumnTag, LogicalColumn],
        *,
        conditions: Iterable[JoinCondition],
    ) -> sqlalchemy.sql.ColumnElement:
        terms: list[sqlalchemy.sql.ColumnElement] = []
        for tag in a.keys() & b.keys():
            terms.append(a[tag] == b[tag])
        for condition in conditions:
            terms.extend(condition.to_sql_booleans(a, b, self.column_types))
        if not terms:
            return sqlalchemy.sql.literal(True)
        elif len(terms) == 1:
            return terms[0]
        else:
            return sqlalchemy.sql.and_(*terms)
