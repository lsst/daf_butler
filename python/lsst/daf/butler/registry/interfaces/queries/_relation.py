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
from collections import deque
from typing import AbstractSet, Iterable, Iterator, Mapping, Optional, Sequence

import sqlalchemy
from lsst.utils.classes import cached_getter

from ...summaries import GovernorDimensionRestriction
from ._column_tags import ColumnTag, ColumnTypeData, DimensionKeyColumnTag, SqlLogicalColumn
from ._restriction import Restriction


class Relation(ABC):
    @property
    @abstractmethod
    def name(self) -> Optional[str]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def columns(self) -> AbstractSet[ColumnTag]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def column_type_data(self) -> ColumnTypeData:
        raise NotImplementedError()

    @abstractmethod
    def projected(self, columns: AbstractSet[ColumnTag], *, name: Optional[str] = None) -> Relation:
        raise NotImplementedError()

    def restricted(self, *restrictions: Restriction, name: Optional[str] = None) -> Relation:
        if not restrictions:
            return self.renamed(name)
        return _Restricted(*restrictions, base=self, columns=self.columns, name=name)

    def renamed(self, name: Optional[str]) -> Relation:
        return self

    @abstractmethod
    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Optional[Mapping[ColumnTag, SqlLogicalColumn]],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        raise NotImplementedError()

    def to_sql_from(self) -> sqlalchemy.sql.FromClause:
        sql_from, columns_available, where = self.to_sql_parts()
        if not where and columns_available is None:
            return sql_from.alias(self.name)
        return self._sql_parts_to_select(sql_from, columns_available, where).alias(self.name)

    def to_sql_executable(self) -> sqlalchemy.sql.expression.SelectBase:
        return self._sql_parts_to_select(*self.to_sql_parts())

    def _sql_parts_to_select(
        self,
        sql_from: sqlalchemy.sql.FromClause,
        columns_available: Optional[Mapping[ColumnTag, SqlLogicalColumn]],
        where: Sequence[sqlalchemy.sql.ColumnElement],
    ) -> sqlalchemy.sql.Select:
        if columns_available is None:
            columns_to_select = ColumnTag.extract_mapping(
                self.columns,
                sql_from.columns,
                self.column_type_data,
            )
        else:
            columns_to_select = {tag: columns_available[tag] for tag in self.columns}
        select = ColumnTag.select(columns_to_select.items(), sql_from)
        if not where:
            return select
        elif len(where) == 1:
            return select.where(where[0])
        else:
            return select.where(sqlalchemy.sql.and_(*where))

    @staticmethod
    def build(sql_from: sqlalchemy.sql.FromClause) -> RelationBuilder:
        return RelationBuilder(sql_from)

    def join(first: Relation, *others: Relation, name: Optional[str] = None) -> Relation:  # noqa: N805
        if not others:
            return first.renamed(name)
        else:
            terms = list(first._flatten_joins())
            for other in others:
                terms.extend(other._flatten_joins())
            return _Join(*terms, name=name)

    def union(first: Relation, *others: Relation, name: Optional[str] = None) -> Relation:  # noqa: N805
        if not others:
            return first.renamed(name)
        else:
            terms = list(first._flatten_unions())
            for other in others:
                terms.extend(other._flatten_unions())
            return _Union(*terms, name=name)

    def _flatten_joins(self) -> Iterator[Relation]:
        yield self

    def _flatten_unions(self) -> Iterator[Relation]:
        yield self


class _SqlPartsRelation(Relation):
    def __init__(
        self,
        sql_from: sqlalchemy.sql.FromClause,
        columns: Mapping[ColumnTag, SqlLogicalColumn],
        *,
        column_type_data: ColumnTypeData,
        name: Optional[str] = None,
        where: Sequence[sqlalchemy.sql.ColumnElement] = (),
    ):
        self._sql_from = sql_from
        self._columns = columns
        self._name = name
        self._where = tuple(where)
        self._column_type_data = column_type_data

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property  # type: ignore
    @cached_getter
    def columns(self) -> AbstractSet[ColumnTag]:
        return self._columns.keys()

    @property
    def column_type_data(self) -> ColumnTypeData:
        return self._column_type_data

    def projected(self, columns: AbstractSet[ColumnTag], *, name: Optional[str] = None) -> Relation:
        assert columns <= self.columns, "projected columns are not a subset."
        return _SqlPartsRelation(
            self._sql_from,
            {tag: self._columns[tag] for tag in columns},
            name=name if name is not None else self._name,
            where=self._where,
            column_type_data=self.column_type_data,
        )

    def renamed(self, name: Optional[str]) -> Relation:
        if name is not None and name != self._name:
            return _SqlPartsRelation(
                self._sql_from,
                columns=self._columns,
                name=name,
                where=self._where,
                column_type_data=self.column_type_data,
            )
        return self

    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Mapping[ColumnTag, SqlLogicalColumn],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        return self._sql_from, self._columns, self._where


@dataclasses.dataclass
class RelationBuilder:

    sql_from: sqlalchemy.sql.FromClause
    columns: dict[ColumnTag, SqlLogicalColumn] = dataclasses.field(default_factory=dict)
    where: list[sqlalchemy.sql.FromClause] = dataclasses.field(default_factory=list)

    def extract_dimension_keys(self, names: Iterable[str]) -> None:
        for name in names:
            self.columns[DimensionKeyColumnTag(name)] = self.sql_from.columns[name]

    def apply_governor_restriction(self, restriction: GovernorDimensionRestriction) -> None:
        for dimension, values in restriction.items():
            if sql_column := self.columns[DimensionKeyColumnTag(dimension.name)]:
                self.where.append(sqlalchemy.sql.or_(*[sql_column == v for v in values]))

    def finish(self, column_type_data: ColumnTypeData, name: Optional[str] = None) -> Relation:
        return _SqlPartsRelation(
            self.sql_from,
            self.columns,
            where=tuple(self.where),
            name=name,
            column_type_data=column_type_data,
        )


class _Restricted(Relation):
    def __init__(
        self,
        *restrictions: Restriction,
        base: Relation,
        columns: Optional[AbstractSet[ColumnTag]],
        name: Optional[str],
    ):
        self._restrictions = restrictions
        self._base = base
        self._columns = columns if columns is not None else base.columns
        self._name = name

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def columns(self) -> AbstractSet[ColumnTag]:
        return self._columns

    @property
    def column_type_data(self) -> ColumnTypeData:
        return self._base.column_type_data

    def projected(self, columns: AbstractSet[ColumnTag], *, name: Optional[str] = None) -> Relation:
        assert columns <= self.columns, "projected columns are not a subset."
        return _Restricted(
            *self._restrictions,
            base=self._base,
            columns=columns,
            name=name if name is not None else self._name,
        )

    def restricted(self, *restrictions: Restriction, name: Optional[str] = None) -> Relation:
        if not restrictions:
            return self.renamed(name)
        return _Restricted(
            *(self._restrictions + restrictions),
            base=self._base,
            columns=self._columns,
            name=name if name is not None else self._name,
        )

    def renamed(self, name: Optional[str]) -> Relation:
        if name is not None and name != self._name:
            return _Restricted(*self._restrictions, base=self._base, columns=self._columns, name=name)
        return self

    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Mapping[ColumnTag, SqlLogicalColumn],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        columns_available: Optional[Mapping[ColumnTag, SqlLogicalColumn]]
        base_from, columns_available, base_where = self._base.to_sql_parts()
        if columns_available is None:
            columns_available = ColumnTag.extract_mapping(
                self._base.columns, base_from.columns, self.column_type_data
            )
        full_where = list(base_where)
        for r in self._restrictions:
            full_where.extend(r.to_sql_booleans(columns_available))
        return base_from, columns_available, full_where


class _Union(Relation):
    def __init__(
        self,
        *terms: Relation,
        name: Optional[str] = None,
    ):
        assert (
            len(terms) > 1
        ), "_Union should always be created by a factory that avoids empty or 1-element iterables."
        self._terms = terms
        self._name = name

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def columns(self) -> AbstractSet[ColumnTag]:
        return self._terms[0].columns

    @property
    def column_type_data(self) -> ColumnTypeData:
        return self._terms[0].column_type_data

    def projected(self, columns: AbstractSet[ColumnTag], *, name: Optional[str] = None) -> Relation:
        return Relation.union(
            *[term.projected(columns) for term in self._terms],
            name=name if name is not None else self._name,
        )

    def restricted(self, *restrictions: Restriction, name: Optional[str] = None) -> Relation:
        if not restrictions:
            return self.renamed(name)
        return _Union(
            *[term.restricted(*restrictions) for term in self._terms],
            name=name if name is not None else self._name,
        )

    def renamed(self, name: Optional[str]) -> Relation:
        if name is not None and name != self._name:
            return _Union(*self._terms, name=name)
        return self

    def to_sql_parts(self) -> tuple[sqlalchemy.sql.FromClause, None, Sequence[sqlalchemy.sql.ColumnElement]]:
        return self.to_sql_executable().subquery(self._name), None, ()

    def to_sql_executable(self) -> sqlalchemy.sql.Select:
        return sqlalchemy.sql.union(*[term.to_sql_executable() for term in self._terms])

    def _flatten_unions(self) -> Iterator[Relation]:
        return iter(self._terms)


class _Join(Relation):
    def __init__(
        self,
        *terms: Relation,
        columns: Optional[AbstractSet[ColumnTag]] = None,
        name: Optional[str] = None,
    ):
        assert (
            len(terms) > 1
        ), "_Join should always be created by a factory that avoids empty or 1-element iterables."
        self._terms = terms
        self._name = name
        self._columns = columns

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def columns(self) -> AbstractSet[ColumnTag]:
        if self._columns is None:
            self._columns = set()
            for term in self._terms:
                self._columns.update(term.columns)
        return self._columns

    @property
    def column_type_data(self) -> ColumnTypeData:
        return self._terms[0].column_type_data

    def projected(self, columns: AbstractSet[ColumnTag], *, name: Optional[str] = None) -> Relation:
        assert columns <= self.columns, "projected columns are not a subset."
        return _Join(*self._terms, columns=columns, name=name if name is not None else self._name)

    def renamed(self, name: Optional[str]) -> Relation:
        if name is not None and name != self._name:
            return _Join(*self._terms, columns=self._columns, name=name)
        return self

    def to_sql_parts(
        self,
    ) -> tuple[
        sqlalchemy.sql.FromClause,
        Mapping[ColumnTag, SqlLogicalColumn],
        Sequence[sqlalchemy.sql.ColumnElement],
    ]:
        first_term, *other_terms = self._sorted_terms()
        full_columns: Optional[Mapping[ColumnTag, SqlLogicalColumn]]
        full_sql, full_columns, first_where = first_term.to_sql_parts()
        full_where = list(first_where)
        if full_columns is None:
            full_columns = ColumnTag.extract_mapping(
                first_term.columns,
                full_sql,
                first_term.column_type_data,
            )
        else:
            full_columns = dict(full_columns)
        for term in other_terms:
            term_columns: Optional[Mapping[ColumnTag, SqlLogicalColumn]]
            term_sql, term_columns, term_where = term.to_sql_parts()
            if term_columns is None:
                term_columns = ColumnTag.extract_mapping(
                    term.columns,
                    full_sql,
                    term.column_type_data,
                )
            full_sql = full_sql.join(
                term_sql, onclause=ColumnTag.join_on_condition(full_columns, term_columns)
            )
            full_columns.update(term_columns)
            full_where.extend(term_where)
        return full_sql, full_columns, full_where

    def _flatten_joins(self) -> Iterator[Relation]:
        return iter(self._terms)

    def _sorted_terms(self) -> Iterator[Relation]:
        # Sort terms by the number of columns they provide, in reverse, and put
        # them in a deque.  We want to join terms into the SQL query in an
        # order such that each join's ON clause has something with the ones
        # that preceded it, and to find out if that's impossible and hence a
        # Cartesian join is needed.  Starting with the terms that have the most
        # columns is a good initial guess for such an ordering.
        todo = deque(sorted(self._terms, key=lambda t: len(t.columns)))
        assert len(todo) > 1, "No join needed for 0 or 1 clauses."
        # We now refine that order, popping terms from the front of `todo` and
        # yielding them when we we have the kind of ON condition we want.
        candidate = todo.popleft()
        yield candidate
        columns_seen: set[ColumnTag] = set(candidate.columns)
        while todo:
            rejected: list[Relation] = []
            candidate = todo.popleft()
            if candidate.columns.isdisjoint(columns_seen):
                # We don't have any overlap between previous-term columns and
                # this term's.  We put it in a rejected list for now, and let
                # the loop continue to try the next one.
                rejected.append(candidate)
            else:
                # This candidate does have some column overlap.  In addition to
                # appending it to self._terms, we reset the `rejected` list by
                # transferring its contents to the end of `todo`, since this
                # new term may have some column overlap with those we've
                # rejected.
                yield candidate
                columns_seen.update(candidate.columns)
                todo.extend(rejected)
                rejected.clear()
        if rejected:
            # In the future, we could guard against unintentional Cartesian
            # products here (see e.g. DM-33147), by checking for common
            # mistakes, emitting warnings, looking for some feature flag that
            # says to enable them, etc.  For now we preserve current behavior
            # by just permitting them.
            yield from rejected
