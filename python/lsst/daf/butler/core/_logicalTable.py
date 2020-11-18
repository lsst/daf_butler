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

__all__ = (
    "LogicalColumn",
    "LogicalTable",
)

from abc import ABC, abstractmethod

from typing import (
    AbstractSet,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import sqlalchemy

from .dimensions import Dimension
from .utils import cached_getter


LogicalColumn = Union[Dimension, Tuple[str, str]]


class LogicalTable(ABC):
    @property
    def name(self) -> Optional[str]:
        return None

    @property
    @abstractmethod
    def columns(self) -> AbstractSet[LogicalColumn]:
        raise NotImplementedError()

    @abstractmethod
    def toSelect(
        self,
        select: Mapping[str, LogicalColumn],
        provide: Iterable[LogicalColumn],
    ) -> Tuple[sqlalchemy.sql.Select, Dict[LogicalColumn, sqlalchemy.sql.ColumnElement]]:
        raise NotImplementedError()

    def join(self, *others: LogicalTable) -> LogicalTable:
        if not others:
            return self
        nested = list(self._flattenNestedJoins())
        for o in others:
            nested.extend(o._flattenNestedJoins())
        return JoinLogicalTable(nested)

    @abstractmethod
    def _toFromClause(
        self,
        provide: Iterable[LogicalColumn],
    ) -> Tuple[sqlalchemy.sql.FromClause, Dict[LogicalColumn, sqlalchemy.sql.ColumnElement]]:
        raise NotImplementedError()

    def _flattenNestedJoins(self) -> Iterator[LogicalTable]:
        yield self


class ActualTable(LogicalTable):
    def __init__(
        self,
        table: sqlalchemy.schema.Table,
        dimensions: Mapping[Dimension, str],
        *,
        metadata: Iterable[str] = (),
        name: Optional[str] = None
    ):
        self._table = table
        self._name = name if name is not None else table.name
        # Tell mypy that the attributes below have heterogeneous keys, even
        # though they don't, so it doesn't complain about us mixing those key
        # types later.  It seems to me that it shouldn't even complain about
        # these assignments, but maybe I'm missing a covariance subtlety.
        self._dimensions: Mapping[LogicalColumn, str] = dimensions  # type: ignore
        self._metadata: AbstractSet[LogicalColumn] = frozenset(metadata)  # type: ignore

    @property
    def name(self) -> str:
        return self._name

    @property  # type: ignore
    @cached_getter
    def columns(self) -> AbstractSet[LogicalColumn]:
        return frozenset(self._dimensions.keys() | self._metadata)

    def toSelect(
        self,
        select: Mapping[str, LogicalColumn],
        provide: Iterable[LogicalColumn],
    ) -> Tuple[sqlalchemy.sql.Select, Dict[LogicalColumn, sqlalchemy.sql.ColumnElement]]:
        sql = sqlalchemy.sql.select(
            [self._table.columns[self._dimensions.get(v, v)].label(k) for k, v in select.items()]
        ).select_from(self._table)
        _, provided = self._toFromClause(provide)
        return sql, provided

    def _toFromClause(
        self,
        provide: Iterable[LogicalColumn],
    ) -> Tuple[sqlalchemy.sql.FromClause, Dict[LogicalColumn, sqlalchemy.sql.ColumnElement]]:
        return self._table, {c: self._table.columns[self._dimensions.get(c, c)] for c in provide}


class JoinLogicalTable(LogicalTable):
    def __init__(self, nested: Sequence[LogicalTable], *, name: Optional[str] = None):
        self._nested = nested
        self._name = name

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property  # type: ignore
    @cached_getter
    def columns(self) -> AbstractSet[LogicalColumn]:
        result: Set[LogicalColumn] = set()
        for table in self._nested:
            result.update(table.columns)
        return frozenset(result)

    def toSelect(
        self,
        select: Mapping[str, LogicalColumn],
        provide: Iterable[LogicalColumn],
    ) -> Tuple[sqlalchemy.sql.Select, Dict[LogicalColumn, sqlalchemy.sql.ColumnElement]]:
        # Find all dimensions referenced at least twice; these are the ones
        # we'll join on.
        seen_dimensions: Set[Dimension] = set()
        join_dimensions: Set[Dimension] = set()
        for table in self._nested:
            for c in table.columns:
                if isinstance(c, Dimension):
                    if c in seen_dimensions:
                        join_dimensions.add(c)
                    else:
                        seen_dimensions.add(c)
        # We'll ask each nested table to provide the columns the caller asked
        # to us to select or provide, as well as all join dimensions (after
        # limiting this later to what each nested table actually has).
        nested_provide = set(provide).union(join_dimensions, select.values())
        # Actually compute the FROM clause, aggregating a list of columns for
        # dimensions as we go.
        from_clause, provided = self._nested[0]._toFromClause(nested_provide & self._nested[0].columns)
        dimension_columns: Dict[Dimension, List[sqlalchemy.sql.ColumnElement]] = {
            k: [v] for k, v in provided.items() if isinstance(k, Dimension)
        }
        for next_table in self._nested[1:]:
            next_from_clause, next_provided = next_table._toFromClause(nested_provide & next_table.columns)
            on_terms: sqlalchemy.sql.ColumnElement = []
            for logical_column, actual_column in next_provided.items():
                if isinstance(logical_column, Dimension):
                    current_columns_for_dimension = dimension_columns.setdefault(logical_column, [])
                    if current_columns_for_dimension:
                        on_terms.extend(c == actual_column for c in current_columns_for_dimension)
                    current_columns_for_dimension.append(actual_column)
            if len(on_terms) == 0:
                from_clause = from_clause.join(next_from_clause, joinon=sqlalchemy.sql.literal(True))
            elif len(on_terms) == 1:
                from_clause = from_clause.join(next_from_clause, joinon=on_terms[0])
            else:
                from_clause = from_clause.join(next_from_clause, joinon=sqlalchemy.sql.and_(*on_terms))
            provided.update(next_provided)
        sql = sqlalchemy.sql.select([provided[v].label(k) for k, v in select.items()]).select_from(
            from_clause
        )
        return sql, {c: provided[c] for c in provide}

    def _toFromClause(
        self,
        provide: Iterable[LogicalColumn],
    ) -> Tuple[sqlalchemy.sql.FromClause, Dict[LogicalColumn, sqlalchemy.sql.ColumnElement]]:
        raise NotImplementedError("JoinLogicalTable instances cannot be used as FROM clause terms.")

    def _flattenNestedJoins(self) -> Iterator[LogicalTable]:
        yield from self._nested
