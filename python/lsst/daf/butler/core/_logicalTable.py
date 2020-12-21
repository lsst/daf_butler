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
    "DirectLogicalTable",
    "LogicalColumnExpression",
    "LogicalColumnFactKey",
    "LogicalColumnKey",
    "LogicalColumnTopologicalExtentKey",
    "LogicalTable",
    "SelectAdapter",
)

from abc import ABC, abstractmethod
from typing import (
    AbstractSet,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import sqlalchemy

from .dimensions import Dimension
from .named import NamedValueAbstractSet, NamedValueSet
from ._topology import SpatialRegionDatabaseRepresentation, TopologicalExtentDatabaseRepresentation
from .timespan import TimespanDatabaseRepresentation
from .utils import cached_getter


class LogicalColumnFactKey(NamedTuple):
    table: str
    column: str


class LogicalColumnTopologicalExtentKey(NamedTuple):
    table: str
    column: Type[TopologicalExtentDatabaseRepresentation]


LogicalColumnKey = Union[
    Dimension,
    LogicalColumnFactKey,
    LogicalColumnTopologicalExtentKey,
]
LogicalColumnExpression = Union[sqlalchemy.sql.ColumnElement, TopologicalExtentDatabaseRepresentation]


class SelectAdapter(ABC):
    @abstractmethod
    def apply(
        self, select: sqlalchemy.sql.Select, provided: Mapping[LogicalColumnKey, LogicalColumnExpression]
    ) -> sqlalchemy.sql.ColumnElement:
        raise NotImplementedError()

    @property
    @abstractmethod
    def needed(self) -> AbstractSet[LogicalColumnKey]:
        raise NotImplementedError()


class LogicalTable(ABC):
    @property
    def name(self) -> Optional[str]:
        return None

    @property
    @abstractmethod
    def dimensions(self) -> NamedValueAbstractSet[Dimension]:
        raise NotImplementedError()

    @property
    def facts(self) -> AbstractSet[LogicalColumnFactKey]:
        return frozenset()

    @property
    def regions(self) -> AbstractSet[str]:
        return frozenset()

    @property
    def timespans(self) -> AbstractSet[str]:
        return frozenset()

    @abstractmethod
    def getTimespanRepresentation(self) -> Type[TimespanDatabaseRepresentation]:
        raise NotImplementedError()

    def getSpatialRegionRepresentation(self) -> Type[SpatialRegionDatabaseRepresentation]:
        return SpatialRegionDatabaseRepresentation

    @property  # type: ignore
    @cached_getter
    def topological_extents(self) -> AbstractSet[LogicalColumnTopologicalExtentKey]:
        result: Set[LogicalColumnTopologicalExtentKey] = set()
        result.update(LogicalColumnTopologicalExtentKey(k, self.getSpatialRegionRepresentation())
                      for k in self.regions)
        result.update(LogicalColumnTopologicalExtentKey(k, self.getTimespanRepresentation())
                      for k in self.timespans)
        return frozenset(result)

    @property  # type: ignore
    @cached_getter
    def columns(self) -> AbstractSet[LogicalColumnKey]:
        result: Set[LogicalColumnKey] = set()
        result.update(self.dimensions)
        result.update(self.facts)
        result.update(self.topological_extents)
        return frozenset(result)

    def join(self, *others: LogicalTable, name: Optional[str] = None) -> LogicalTable:
        if not others:
            return self
        nested = list(self._unpack_if_join())
        for o in others:
            nested.extend(o._unpack_if_join())
        return _JoinLogicalTable(nested, name=name)

    def union_all(self, *others: LogicalTable, name: Optional[str] = None) -> LogicalTable:
        if not others:
            return self
        nested = list(self._unpack_if_union_all())
        for o in others:
            nested.extend(o._unpack_if_union_all())
        return _UnionAllLogicalTable(nested, name=name)

    @abstractmethod
    def select(
        self,
        columns: Optional[Iterable[LogicalColumnKey]] = None,
        adapter: Optional[SelectAdapter] = None,
    ) -> sqlalchemy.sql.Select:
        raise NotImplementedError()

    def extract(
        self,
        column_key: LogicalColumnKey,
        source: Optional[sqlalchemy.sql.FromClause] = None,
    ) -> LogicalColumnExpression:
        expression, _ = self._extract_with_name(column_key, source)
        return expression

    def _as_joinable(
        self, columns: Iterable[LogicalColumnKey]
    ) -> Tuple[sqlalchemy.sql.FromClause, Dict[LogicalColumnKey, LogicalColumnExpression]]:
        joinable = self.select(columns).alias(self.name)
        return joinable, {key: self.extract(key, source=joinable) for key in columns}

    def _unpack_if_join(self) -> Iterator[LogicalTable]:
        yield self

    def _unpack_if_union_all(self) -> Iterator[LogicalTable]:
        yield self

    def _get_column_name(self, column_key: LogicalColumnKey) -> str:
        if isinstance(column_key, Dimension):
            return column_key.name
        elif isinstance(column_key, LogicalColumnFactKey):
            if column_key.table == self.name:
                return column_key.column
            else:
                return f"{column_key.table}_{column_key.column}"
        elif isinstance(column_key, LogicalColumnTopologicalExtentKey):
            if column_key.table == self.name:
                return column_key.column.NAME
            else:
                return f"{column_key.table}_{column_key.column.NAME}"
        raise TypeError(f"Unexpected column key: {column_key!r}.")

    def _get_column_source(self, column_key: LogicalColumnKey) -> sqlalchemy.sql.FromClause:
        raise RuntimeError(
            f"The source table/subquery for column {column_key} is not fixed, "
            f"and must be passed to {type(self).__name__}.extract."
        )

    def _extract_with_name(
        self,
        column_key: LogicalColumnKey,
        source: Optional[sqlalchemy.sql.FromClause] = None,
    ) -> Tuple[LogicalColumnExpression, str]:
        if source is None:
            source = self._get_column_source(column_key)
        column_name = self._get_column_name(column_key)
        if isinstance(column_key, (Dimension, LogicalColumnFactKey)):
            return source.columns[column_name], column_name
        elif isinstance(column_key, LogicalColumnTopologicalExtentKey):
            return column_key.column.fromSelectable(source, column_name), column_name
        raise TypeError(f"Unexpected column key: {column_key!r}.")

    def _flatten_select_expressions(
        self,
        expressions: Iterable[Tuple[LogicalColumnExpression, str]],
    ) -> List[sqlalchemy.sql.ColumnElement]:
        result: List[sqlalchemy.sql.ColumnElement] = []
        for expression, name in expressions:
            if isinstance(expression, TopologicalExtentDatabaseRepresentation):
                result.extend(expression.flatten(name))
            else:
                result.append(expression.label(name))
        return result


class DirectLogicalTable(LogicalTable):
    def __init__(
        self,
        table: sqlalchemy.schema.Table,
        *,
        dimensions: NamedValueAbstractSet[Dimension],
        TimespanReprClass: Type[TimespanDatabaseRepresentation],
        facts: Iterable[str] = (),
        is_spatial: bool = False,
        is_temporal: bool = False,
        name: Optional[str] = None,
        column_names: Optional[Mapping[LogicalColumnKey, str]] = None,
    ):
        if name is None:
            name = table.name
        if column_names is None:
            column_names = {}
        self._table = table
        self._name = name
        self._dimensions = dimensions
        self._facts = frozenset(LogicalColumnFactKey(table=name, column=s) for s in facts)
        self._regions = frozenset({name}) if is_spatial else frozenset()
        self._timespans = frozenset({name}) if is_temporal else frozenset()
        self._column_names = column_names
        self._TimespanReprClass = TimespanReprClass

    @property
    def name(self) -> str:
        return self._name

    @property
    def dimensions(self) -> NamedValueAbstractSet[Dimension]:
        return self._dimensions

    @property
    def facts(self) -> AbstractSet[LogicalColumnFactKey]:
        return self._facts

    @property
    def regions(self) -> AbstractSet[str]:
        return self._regions

    @property
    def timespans(self) -> AbstractSet[str]:
        return self._timespans

    def getTimespanRepresentation(self) -> Type[TimespanDatabaseRepresentation]:
        return self._TimespanReprClass

    def select(
        self,
        columns: Optional[Iterable[LogicalColumnKey]] = None,
        adapter: Optional[SelectAdapter] = None,
    ) -> sqlalchemy.sql.Select:
        if columns is None:
            columns = self.columns
        sql = sqlalchemy.sql.select(
            self._flatten_select_expressions(self._extract_with_name(key) for key in columns)
        ).select_from(
            self._table
        )
        if adapter is not None:
            sql = adapter.apply(sql, {key: self.extract(key) for key in adapter.needed})
        return sql

    def _as_joinable(
        self,
        columns: Iterable[LogicalColumnKey],
    ) -> Tuple[sqlalchemy.sql.FromClause, Dict[LogicalColumnKey, LogicalColumnExpression]]:
        return self._table, {key: self.extract(key) for key in columns}

    def _get_column_name(self, column_key: LogicalColumnKey) -> str:
        name = self._column_names.get(column_key)
        if name is None:
            name = super()._get_column_name(column_key)
        return name

    def _get_column_source(self, column_key: LogicalColumnKey) -> sqlalchemy.sql.FromClause:
        return self._table


class _JoinLogicalTable(LogicalTable):
    def __init__(self, nested: Sequence[LogicalTable], *, name: Optional[str] = None):
        assert len(nested) > 1
        self._nested = nested
        self._name = name

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property  # type: ignore
    @cached_getter
    def dimensions(self) -> NamedValueAbstractSet[Dimension]:
        result = NamedValueSet[Dimension]()
        for table in self._nested:
            result.update(table.dimensions)
        return result.freeze()

    @property  # type: ignore
    @cached_getter
    def facts(self) -> AbstractSet[LogicalColumnFactKey]:
        result: Set[LogicalColumnFactKey] = set()
        for table in self._nested:
            result.update(table.facts)
        return frozenset(result)

    @property  # type: ignore
    @cached_getter
    def regions(self) -> AbstractSet[str]:
        result: Set[str] = set()
        for table in self._nested:
            result.update(table.regions)
        return result

    @property  # type: ignore
    @cached_getter
    def timespans(self) -> AbstractSet[str]:
        result: Set[str] = set()
        for table in self._nested:
            result.update(table.timespans)
        return result

    def getTimespanRepresentation(self) -> Type[TimespanDatabaseRepresentation]:
        return self._nested[0].getTimespanRepresentation()

    def select(
        self,
        columns: Optional[Iterable[LogicalColumnKey]] = None,
        adapter: Optional[SelectAdapter] = None,
    ) -> sqlalchemy.sql.Select:
        if columns is None:
            columns = self.columns
        else:
            columns = frozenset(columns)
        # Find all keys referenced at least twice; these are the ones we'll
        # join on.  These will almost always be dimensions.
        seen_keys: Set[LogicalColumnKey] = set()
        join_keys: Set[Union[Dimension, LogicalColumnFactKey]] = set()
        for table in self._nested:
            for c in table.columns:
                if c in seen_keys:
                    if isinstance(c, LogicalColumnTopologicalExtentKey):
                        raise ValueError(
                            f"Topological extent {c.table}.{c.column.NAME} "
                            f"appears in multiple logical tables."
                        )
                    join_keys.add(c)
                else:
                    seen_keys.add(c)
        # We'll ask each nested table to provide the columns the caller asked
        # to us to select or provide to the adapter, as well as all join keys
        # (we'll limit this later to what each nested table actually has).
        requested_base = set(columns).union(join_keys)
        if adapter is not None:
            requested_base.update(adapter.needed)
        # Actually compute the FROM clause, aggregating a list of columns for
        # dimensions as we go.
        from_clause, provided = self._nested[0]._as_joinable(requested_base & self._nested[0].columns)
        join_columns: Dict[Union[Dimension, LogicalColumnFactKey], List[sqlalchemy.sql.ColumnElement]] = {
            k: [provided[k]] for k in join_keys.intersection(provided.keys())
        }
        for next_table in self._nested[1:]:
            next_from_clause, next_provided = next_table._as_joinable(requested_base & next_table.columns)
            on_terms: sqlalchemy.sql.ColumnElement = []
            for key in join_keys & next_table.columns:
                new_column_for_key = next_provided[key]
                current_columns_for_key = join_columns.setdefault(key, [])
                if current_columns_for_key:
                    on_terms.extend(c == new_column_for_key for c in current_columns_for_key)
                current_columns_for_key.append(new_column_for_key)
            if len(on_terms) == 0:
                from_clause = from_clause.join(next_from_clause, joinon=sqlalchemy.sql.literal(True))
            elif len(on_terms) == 1:
                from_clause = from_clause.join(next_from_clause, joinon=on_terms[0])
            else:
                from_clause = from_clause.join(next_from_clause, joinon=sqlalchemy.sql.and_(*on_terms))
            provided.update(next_provided)
        sql = sqlalchemy.sql.select(
            self._flatten_select_expressions(
                (provided[key], self._get_column_name(key)) for key in columns
            )
        ).select_from(from_clause)
        if adapter is not None:
            sql = adapter.apply(sql, provided)
        return sql

    def _unpack_if_join(self) -> Iterator[LogicalTable]:
        yield from self._nested


class _UnionAllLogicalTable(LogicalTable):
    def __init__(self, nested: Sequence[LogicalTable], *, name: Optional[str] = None):
        assert len(nested) > 1
        self._nested = nested
        self._name = name

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property  # type: ignore
    @cached_getter
    def dimensions(self) -> NamedValueAbstractSet[Dimension]:
        result: Set[Dimension] = set(self._nested[0].dimensions)
        for table in self._nested[1:]:
            result.intersection_update(table.dimensions)
        return NamedValueSet(result).freeze()

    @property  # type: ignore
    @cached_getter
    def facts(self) -> AbstractSet[LogicalColumnFactKey]:
        result: Set[LogicalColumnFactKey] = set(self._nested[0].facts)
        for table in self._nested[1:]:
            result.intersection_update(table.facts)
        return frozenset(result)

    @property  # type: ignore
    @cached_getter
    def regions(self) -> AbstractSet[str]:
        result = set(self._nested[0].regions)
        for table in self._nested[1:]:
            result.intersection_update(table.regions)
        return result

    @property  # type: ignore
    @cached_getter
    def timespans(self) -> AbstractSet[str]:
        result = set(self._nested[0].timespans)
        for table in self._nested[1:]:
            result.intersection_update(table.timespans)
        return result

    def getTimespanRepresentation(self) -> Type[TimespanDatabaseRepresentation]:
        return self._nested[0].getTimespanRepresentation()

    def select(
        self,
        columns: Optional[Iterable[LogicalColumnKey]] = None,
        adapter: Optional[SelectAdapter] = None,
    ) -> sqlalchemy.sql.Select:
        items: List[sqlalchemy.sql.Select] = []
        for table in self._nested:
            item, _ = table.select(columns, adapter=adapter)
            items.append(item)
        return sqlalchemy.sql.union_all(*items)

    def _unpack_if_union_all(self) -> Iterator[LogicalTable]:
        yield from self._nested
