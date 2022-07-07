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
from typing import TYPE_CHECKING, Any, cast

import sqlalchemy
from lsst.daf.relation import Relation, TransferVisitor, iteration, sql

from ...core import ColumnTag, ColumnTypeInfo, TopologicalExtentDatabaseRepresentation
from ._query_context import QueryContext

if TYPE_CHECKING:
    from ..interfaces import Database, Session


class SqlQueryContext(QueryContext):
    """An implementation of `sql.QueryContext` for `SqlRegistry`.

    Parameters
    ----------
    db : `Database`
        Object that abstracts the database engine.
    column_types : `ColumnTypeInfo`
        Information about column types that can vary with registry
        configuration.
    """

    def __init__(
        self,
        db: Database,
        column_types: ColumnTypeInfo,
        engine: sql.Engine,
    ):
        self._db = db
        self._column_types = column_types
        self._engine = engine
        self._session: Session | None = None
        self._exit_stack: ExitStack | None = None

    def __enter__(self) -> SqlQueryContext:
        assert self._exit_stack is None, "Context manager already entered."
        self._exit_stack = ExitStack().__enter__()
        self._session = self._exit_stack.enter_context(self._db.session())
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        assert self._exit_stack is not None, "Context manager not yet entered."
        result = self._exit_stack.__exit__(exc_type, exc_value, traceback)
        self._exit_stack = None
        self._session = None
        return result

    def fetch_iterable(self, relation: Relation[ColumnTag]) -> iteration.RowIterable[ColumnTag]:
        # Docstring inherited.
        if relation.engine.tag is not iteration.engine:
            iteration_relation = self._sql_to_iteration(relation)
        else:
            visitor = TransferVisitor[ColumnTag]({(self._engine, iteration.engine): self._sql_to_iteration})
            iteration_relation = relation.visit(visitor)
        return iteration.engine.execute(iteration_relation)

    def _sql_to_iteration(self, source: Relation[ColumnTag]) -> Relation[ColumnTag]:
        """Transfer a relation from a SQL engine to the native iteration
        engine.

        This is intended to be used as a
        `~lsst.daf.relation.TransferFunction`.

        Parameters
        ----------
        source : `~lsst.daf.relation.Relation`
            Input relation, in what is assumed to be a SQL engine.

        Returns
        -------
        destination : `~lsst.daf.relation.Relation`
            Relation equivalent to ``source``, but in the native iteration
            engine as a leaf relation.
        """
        sql_executable = cast(sql.Engine, source.engine.tag).to_executable(source, self._column_types)
        rows = _SqlRowIterable(
            self._db, sql_executable, _SqlRowTransformer(source.columns, self._column_types)
        )
        return iteration.RowIterableLeaf(
            str(source), iteration.engine, source.columns, unique_keys=source.unique_keys, rows=rows
        )


class _SqlRowIterable(iteration.RowIterable[ColumnTag]):
    """An implementation of `lsst.daf.relation.iteration.RowIterable` that
    executes a SQL query.

    Parameters
    ----------
    db : `Database`
        Database interface to execute the query with.
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
        db: Database,
        sql_executable: sqlalchemy.sql.expression.SelectBase,
        row_transformer: _SqlRowTransformer,
    ):
        self._db = db
        self._sql_executable = sql_executable
        self._row_transformer = row_transformer

    def __iter__(self) -> Iterator[Mapping[ColumnTag, Any]]:
        for sql_row in self._db.query(self._sql_executable).mappings():
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
        self._special_columns: dict[ColumnTag, type[TopologicalExtentDatabaseRepresentation]] = {}
        self._special_columns.update({tag: column_types.timespan_cls for tag in columns if tag.is_timespan})
        self._special_columns.update(
            {tag: column_types.spatial_region_cls for tag in columns if tag.is_spatial_region}
        )
        self._scalar_columns.difference_update(self._special_columns.keys())

    __slots__ = ("_scalar_columns", "_special_columns")

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
        relation_row = {tag: sql_row[str(tag)] for tag in self._scalar_columns}
        for tag, db_repr_cls in self._special_columns.items():
            relation_row[tag] = db_repr_cls.extract(sql_row, name=str(tag))
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
        sql_row = {str(tag): relation_row[tag] for tag in self._scalar_columns}
        for tag, db_repr_cls in self._special_columns.items():
            db_repr_cls.update(relation_row[tag], result=sql_row, name=str(tag))
        return sql_row
