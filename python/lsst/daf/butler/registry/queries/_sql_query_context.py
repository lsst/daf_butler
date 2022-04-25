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

__all__ = ("SqlQueryContext",)

from collections.abc import Iterable, Iterator, Mapping
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any

import sqlalchemy
from lsst.daf.relation import ColumnTag, Engine, Relation, iteration

from ...core import ColumnTypeInfo, TimespanDatabaseRepresentation, is_timespan_column
from ._query_context import QueryContext
from .butler_sql_engine import ButlerSqlEngine

if TYPE_CHECKING:
    from ..interfaces import Database


class SqlQueryContext(QueryContext):
    """An implementation of `sql.QueryContext` for `SqlRegistry`.

    Parameters
    ----------
    db : `Database`
        Object that abstracts the database engine.
    sql_engine : `ButlerSqlEngine`
        Information about column types that can vary with registry
        configuration.
    """

    def __init__(
        self,
        db: Database,
        column_types: ColumnTypeInfo,
    ):
        super().__init__()
        self.sql_engine = ButlerSqlEngine(column_types=column_types)
        self._db = db
        self._exit_stack: ExitStack | None = None

    def __enter__(self) -> SqlQueryContext:
        assert self._exit_stack is None, "Context manager already entered."
        self._exit_stack = ExitStack().__enter__()
        self._exit_stack.enter_context(self._db.session())
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        assert self._exit_stack is not None, "Context manager not yet entered."
        result = self._exit_stack.__exit__(exc_type, exc_value, traceback)
        self._exit_stack = None
        return result

    @property
    def column_types(self) -> ColumnTypeInfo:
        """Information about column types that depend on registry configuration
        (`ColumnTypeInfo`).
        """
        return self.sql_engine.column_types

    @property
    def preferred_engine(self) -> Engine:
        # Docstring inherited.
        return self.sql_engine

    def transfer(self, source: Relation, destination: Engine, materialize_as: str | None) -> Any:
        # Docstring inherited from lsst.daf.relation.Processor.
        if source.engine == self.sql_engine and destination == self.iteration_engine:
            return self._sql_to_iteration(source, materialize_as)
        raise NotImplementedError("Upload to SQL transfers not yet implemented.")

    def materialize(self, base: Relation, name: str) -> Any:
        # Docstring inherited from lsst.daf.relation.Processor.
        if base.engine == self.sql_engine:
            raise NotImplementedError("SQL persistent materialization is not yet implemented.")
        return super().materialize(base, name)

    def _sql_to_iteration(self, source: Relation, materialize_as: str | None) -> iteration.RowIterable:
        """Transfer a relation from a SQL engine to the native iteration
        engine.

        Parameters
        ----------
        source : `~lsst.daf.relation.Relation`
            Input relation, in what is assumed to be a SQL engine.
        materialize_as : `str` or `None`
            If not `None`, the name of a persistent materialization to apply
            in the iteration engine.  This fetches all rows up front.

        Returns
        -------
        destination : `~lsst.daf.relation.iteration.RowIterable`
            Iteration engine payload iterable with the same content as the
            given SQL relation.
        """
        sql_executable = self.sql_engine.to_executable(source)
        rows: iteration.RowIterable = _SqlRowIterable(
            self, sql_executable, _SqlRowTransformer(source.columns, self.sql_engine.column_types)
        )
        if materialize_as is not None:
            rows = rows.materialized()
        return rows


class _SqlRowIterable(iteration.RowIterable):
    """An implementation of `lsst.daf.relation.iteration.RowIterable` that
    executes a SQL query.

    Parameters
    ----------
    context : `SqlQueryContext`
        Context to execute the query with.  If this context has already been
        entered, a lazy iterable will be returned that may or may not use
        server side cursors, since the context's lifetime can be used to manage
        that cursor's lifetime.  If the context has not been entered, all
        results will be fetched up front in raw form, but will still be
        processed into mappings keyed by `ColumnTag` lazily.
    sql_executable : `sqlalchemy.sql.expression.SelectBase`
        SQL query to execute; assumed to have been built by
        `lsst.daf.relation.sql.Engine.to_executable` or similar.
    row_transformer : `_SqlRowTransformer`
        Object that converts SQLAlchemy result-row mappings (with `str`
        keys and possibly-unpacked timespan values) to relation row
        mappings (with `ColumnTag` keys and `LogicalColumn` values).
    """

    def __init__(
        self,
        context: SqlQueryContext,
        sql_executable: sqlalchemy.sql.expression.SelectBase,
        row_transformer: _SqlRowTransformer,
    ):
        self._context = context
        self._sql_executable = sql_executable
        self._row_transformer = row_transformer

    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        with self._context._db.query(self._sql_executable) as sql_result:
            raw_rows = sql_result.mappings()
            if self._context._exit_stack is None:
                raw_rows = raw_rows.fetchall()
        for sql_row in raw_rows:
            yield self._row_transformer.sql_to_relation(sql_row)


class _SqlRowTransformer:
    """Object that converts SQLAlchemy result-row mappings to relation row
    mappings.

    Parameters
    ----------
    columns : `Iterable` [ `ColumnTag` ]
        Set of columns to handle.  Rows must have at least these columns, but
        may have more.
    column_types : `ColumnTypeInfo`
        Information about column types that can vary with registry
        configuration.
    """

    def __init__(self, columns: Iterable[ColumnTag], column_types: ColumnTypeInfo):
        self._scalar_columns = set(columns)
        self._timespan_columns: dict[ColumnTag, type[TimespanDatabaseRepresentation]] = {}
        self._timespan_columns.update(
            {tag: column_types.timespan_cls for tag in columns if is_timespan_column(tag)}
        )
        self._scalar_columns.difference_update(self._timespan_columns.keys())
        self._has_no_columns = not columns

    __slots__ = ("_scalar_columns", "_timespan_columns", "_has_no_columns")

    def sql_to_relation(self, sql_row: Mapping[str, Any]) -> dict[ColumnTag, Any]:
        """Convert a result row from a SQLAlchemy result into the form expected
        by `lsst.daf.relation.iteration`.

        Parameters
        ----------
        sql_row : `Mapping`
            Mapping with `str` keys and possibly-unpacked values for timespan
            columns.

        Returns
        -------
        relation_row : `Mapping`
            Mapping with `ColumnTag` keys and `Timespan` objects for timespan
            columns.
        """
        relation_row = {tag: sql_row[tag.qualified_name] for tag in self._scalar_columns}
        for tag, db_repr_cls in self._timespan_columns.items():
            relation_row[tag] = db_repr_cls.extract(sql_row, name=tag.qualified_name)
        return relation_row

    def relation_to_sql(self, relation_row: Mapping[ColumnTag, Any]) -> dict[str, Any]:
        """Convert a `lsst.daf.relation.iteration` row into the mapping form
        used by SQLAlchemy.

        Parameters
        ----------
        relation_row : `Mapping`
            Mapping with `ColumnTag` keys and `Timespan` objects for timespan
            columns.

        Returns
        -------
        sql_row : `Mapping`
            Mapping with `str` keys and possibly-unpacked values for timespan
            columns.
        """
        sql_row = {tag.qualified_name: relation_row[tag] for tag in self._scalar_columns}
        for tag, db_repr_cls in self._timespan_columns.items():
            db_repr_cls.update(relation_row[tag], result=sql_row, name=tag.qualified_name)
        if self._has_no_columns:
            sql_row["IGNORED"] = None
        return sql_row
