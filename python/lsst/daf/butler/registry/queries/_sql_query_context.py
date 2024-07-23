# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

import itertools
from collections.abc import Iterable, Iterator, Mapping, Set
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, cast

import sqlalchemy
from lsst.daf.relation import (
    BinaryOperationRelation,
    Calculation,
    Chain,
    ColumnTag,
    Deduplication,
    Engine,
    EngineError,
    Join,
    MarkerRelation,
    Projection,
    Relation,
    Transfer,
    UnaryOperation,
    UnaryOperationRelation,
    iteration,
    sql,
)

from ..._column_tags import is_timespan_column
from ..._column_type_info import ColumnTypeInfo, LogicalColumn
from ...timespan_database_representation import TimespanDatabaseRepresentation
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
    column_types : `ColumnTypeInfo`
        Information about column types that can vary with registry
        configuration.
    row_chunk_size : `int`, optional
        Number of rows to insert into temporary tables at once.  If this is
        lower than ``db.get_constant_rows_max()`` it will be set to that value.
    """

    def __init__(
        self,
        db: Database,
        column_types: ColumnTypeInfo,
        row_chunk_size: int = 1000,
    ):
        super().__init__()
        self.sql_engine = ButlerSqlEngine(column_types=column_types, name_shrinker=db.name_shrinker)
        self._row_chunk_size = max(row_chunk_size, db.get_constant_rows_max())
        self._db = db
        self._exit_stack: ExitStack | None = None

    def __enter__(self) -> SqlQueryContext:
        assert self._exit_stack is None, "Context manager already entered."
        self._exit_stack = ExitStack().__enter__()
        self._exit_stack.enter_context(self._db.session())
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool | None:
        assert self._exit_stack is not None, "Context manager not yet entered."
        result = self._exit_stack.__exit__(exc_type, exc_value, traceback)
        self._exit_stack = None
        return result

    @property
    def is_open(self) -> bool:
        return self._exit_stack is not None

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

    def count(self, relation: Relation, *, exact: bool = True, discard: bool = False) -> int:
        # Docstring inherited.
        relation = self._strip_count_invariant_operations(relation, exact).with_only_columns(frozenset())
        if relation.engine == self.sql_engine:
            sql_executable = self.sql_engine.to_executable(
                relation, extra_columns=[sqlalchemy.sql.func.count()]
            )
            with self._db.query(sql_executable) as sql_result:
                return cast(int, sql_result.scalar_one())
        elif (rows := relation.payload) is not None:
            assert isinstance(
                rows, iteration.MaterializedRowIterable
            ), "Query guarantees that only materialized payloads are attached to its relations."
            return len(rows)
        elif discard:
            n = 0
            for _ in self.fetch_iterable(relation):
                n += 1
            return n
        else:
            raise RuntimeError(
                f"Query with relation {relation} has deferred operations that "
                "must be executed in Python in order to obtain an exact "
                "count.  Pass discard=True to run the query and discard the "
                "result rows while counting them, run the query first, or "
                "pass exact=False to obtain an upper bound."
            )

    def any(self, relation: Relation, *, execute: bool = True, exact: bool = True) -> bool:
        # Docstring inherited.
        relation = self._strip_count_invariant_operations(relation, exact).with_only_columns(frozenset())[:1]
        if relation.engine == self.sql_engine:
            sql_executable = self.sql_engine.to_executable(relation)
            with self._db.query(sql_executable) as sql_result:
                return sql_result.one_or_none() is not None
        elif (rows := relation.payload) is not None:
            assert isinstance(
                rows, iteration.MaterializedRowIterable
            ), "Query guarantees that only materialized payloads are attached to its relations."
            return bool(rows)
        elif execute:
            for _ in self.fetch_iterable(relation):
                return True
            return False
        else:
            raise RuntimeError(
                f"Query with relation {relation} has deferred operations that "
                "must be executed in Python in order to obtain an exact "
                "check for whether any rows would be returned.  Pass "
                "execute=True to run the query until at least "
                "one row is found, run the query first, or pass exact=False "
                "test only whether the query _might_ have result rows."
            )

    def transfer(self, source: Relation, destination: Engine, materialize_as: str | None) -> Any:
        # Docstring inherited from lsst.daf.relation.Processor.
        if source.engine == self.sql_engine and destination == self.iteration_engine:
            return self._sql_to_iteration(source, materialize_as)
        if source.engine == self.iteration_engine and destination == self.sql_engine:
            return self._iteration_to_sql(source, materialize_as)
        raise EngineError(f"Unexpected transfer for SqlQueryContext: {source.engine} -> {destination}.")

    def materialize(self, target: Relation, name: str) -> Any:
        # Docstring inherited from lsst.daf.relation.Processor.
        if target.engine == self.sql_engine:
            sql_executable = self.sql_engine.to_executable(target)
            table_spec = self.column_types.make_relation_table_spec(target.columns)
            if self._exit_stack is None:
                raise RuntimeError("This operation requires the QueryContext to have been entered.")
            table = self._exit_stack.enter_context(self._db.temporary_table(table_spec, name=name))
            self._db.insert(table, select=sql_executable)
            payload = sql.Payload[LogicalColumn](table)
            payload.columns_available = self.sql_engine.extract_mapping(target.columns, table.columns)
            return payload
        return super().materialize(target, name)

    def restore_columns(
        self,
        relation: Relation,
        columns_required: Set[ColumnTag],
    ) -> tuple[Relation, set[ColumnTag]]:
        # Docstring inherited.
        if relation.is_locked:
            return relation, set(relation.columns & columns_required)
        match relation:
            case UnaryOperationRelation(operation=operation, target=target):
                new_target, columns_found = self.restore_columns(target, columns_required)
                if not columns_found:
                    return relation, columns_found
                match operation:
                    case Projection(columns=columns):
                        new_columns = columns | columns_found
                        if new_columns != columns:
                            return (
                                Projection(new_columns).apply(new_target),
                                columns_found,
                            )
                    case Calculation(tag=tag):
                        if tag in columns_required:
                            columns_found.add(tag)
                    case Deduplication():
                        # Pulling key columns through the deduplication would
                        # fundamentally change what it does; have to limit
                        # ourselves to non-key columns here, and put a
                        # Projection back in place.
                        columns_found = {c for c in columns_found if not c.is_key}
                        new_target = new_target.with_only_columns(relation.columns | columns_found)
                return relation.reapply(new_target), columns_found
            case BinaryOperationRelation(operation=operation, lhs=lhs, rhs=rhs):
                new_lhs, columns_found_lhs = self.restore_columns(lhs, columns_required)
                new_rhs, columns_found_rhs = self.restore_columns(rhs, columns_required)
                match operation:
                    case Join():
                        return relation.reapply(new_lhs, new_rhs), columns_found_lhs | columns_found_rhs
                    case Chain():
                        if columns_found_lhs != columns_found_rhs:
                            # Got different answers for different join
                            # branches; let's try again with just columns found
                            # in both branches.
                            new_columns_required = columns_found_lhs & columns_found_rhs
                            if not new_columns_required:
                                return relation, set()
                            new_lhs, columns_found_lhs = self.restore_columns(lhs, new_columns_required)
                            new_rhs, columns_found_rhs = self.restore_columns(rhs, new_columns_required)
                            assert columns_found_lhs == columns_found_rhs
                        return relation.reapply(new_lhs, new_rhs), columns_found_lhs
            case MarkerRelation(target=target):
                new_target, columns_found = self.restore_columns(target, columns_required)
                return relation.reapply(new_target), columns_found
        raise AssertionError("Match should be exhaustive and all branches should return.")

    def strip_postprocessing(self, relation: Relation) -> tuple[Relation, list[UnaryOperation]]:
        # Docstring inherited.
        if relation.engine != self.iteration_engine or relation.is_locked:
            return relation, []
        match relation:
            case UnaryOperationRelation(operation=operation, target=target):
                new_target, stripped = self.strip_postprocessing(target)
                stripped.append(operation)
                return new_target, stripped
            case Transfer(destination=self.iteration_engine, target=target):
                return target, []
        return relation, []

    def drop_invalidated_postprocessing(self, relation: Relation, new_columns: Set[ColumnTag]) -> Relation:
        # Docstring inherited.
        if relation.engine != self.iteration_engine or relation.is_locked:
            return relation
        match relation:
            case UnaryOperationRelation(operation=operation, target=target):
                columns_added_here = relation.columns - target.columns
                new_target = self.drop_invalidated_postprocessing(target, new_columns - columns_added_here)
                if operation.columns_required <= new_columns:
                    # This operation can stay...
                    if new_target is target:
                        # ...and nothing has actually changed upstream of it.
                        return relation
                    else:
                        # ...but we should see if it can now be performed in
                        # the preferred engine, since something it didn't
                        # commute with may have been removed.
                        return operation.apply(new_target, preferred_engine=self.preferred_engine)
                else:
                    # This operation needs to be dropped, as we're about to
                    # project away one more more of the columns it needs.
                    return new_target
        return relation

    def _sql_to_iteration(self, source: Relation, materialize_as: str | None) -> iteration.RowIterable:
        """Execute a relation transfer from the SQL engine to the iteration
        engine.

        Parameters
        ----------
        source : `~lsst.daf.relation.Relation`
            Input relation, in what is assumed to be `sql_engine`.
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
            self, sql_executable, _SqlRowTransformer(source.columns, self.sql_engine)
        )
        if materialize_as is not None:
            rows = rows.materialized()
        return rows

    def _iteration_to_sql(self, source: Relation, materialize_as: str | None) -> sql.Payload:
        """Execute a relation transfer from the iteration engine to the SQL
        engine.

        Parameters
        ----------
        source : `~lsst.daf.relation.Relation`
            Input relation, in what is assumed to be `iteration_engine`.
        materialize_as : `str` or `None`
            If not `None`, the name of a persistent materialization to apply
            in the SQL engine.  This sets the name of the temporary table
            and ensures that one is used (instead of `Database.constant_rows`,
            which might otherwise be used for small row sets).

        Returns
        -------
        destination : `~lsst.daf.relation.sql.Payload`
            SQL engine payload struct with the same content as the given
            iteration relation.
        """
        iterable = self.iteration_engine.execute(source)
        table_spec = self.column_types.make_relation_table_spec(source.columns)
        row_transformer = _SqlRowTransformer(source.columns, self.sql_engine)
        sql_rows = []
        iterator = iter(iterable)
        # Iterate over the first chunk of rows and transform them to hold only
        # types recognized by SQLAlchemy (i.e. expand out
        # TimespanDatabaseRepresentations).  Note that this advances
        # `iterator`.
        for relation_row in itertools.islice(iterator, self._row_chunk_size):
            sql_rows.append(row_transformer.relation_to_sql(relation_row))
        name = materialize_as if materialize_as is not None else self.sql_engine.get_relation_name("upload")
        if materialize_as is not None or len(sql_rows) > self._db.get_constant_rows_max():
            if self._exit_stack is None:
                raise RuntimeError("This operation requires the QueryContext to have been entered.")
            # Either we're being asked to insert into a temporary table with a
            # "persistent" lifetime (the duration of the QueryContext), or we
            # have enough rows that the database says we can't use its
            # "constant_rows" construct (e.g. VALUES).  Note that
            # `QueryContext.__init__` guarantees that `self._row_chunk_size` is
            # greater than or equal to `Database.get_constant_rows_max`.
            table = self._exit_stack.enter_context(self._db.temporary_table(table_spec, name))
            while sql_rows:
                self._db.insert(table, *sql_rows)
                sql_rows.clear()
                for relation_row in itertools.islice(iterator, self._row_chunk_size):
                    sql_rows.append(row_transformer.relation_to_sql(relation_row))
            payload = sql.Payload[LogicalColumn](table)
        else:
            # Small number of rows; use Database.constant_rows.
            payload = sql.Payload[LogicalColumn](self._db.constant_rows(table_spec.fields, *sql_rows))
        payload.columns_available = self.sql_engine.extract_mapping(
            source.columns, payload.from_clause.columns
        )
        return payload

    def _strip_empty_invariant_operations(
        self,
        relation: Relation,
        exact: bool,
    ) -> Relation:
        """Return a modified relation tree that strips out relation operations
        that do not affect whether there are any rows from the end of the tree.

        Parameters
        ----------
        relation : `Relation`
            Original relation tree.
        exact : `bool`
            If `False` strip all iteration-engine operations, even those that
            can remove all rows.

        Returns
        -------
        modified : `Relation`
            Modified relation tree.
        """
        if relation.payload is not None or relation.is_locked:
            return relation
        match relation:
            case UnaryOperationRelation(operation=operation, target=target):
                if operation.is_empty_invariant or (not exact and target.engine == self.iteration_engine):
                    return self._strip_empty_invariant_operations(target, exact)
            case Transfer(target=target):
                return self._strip_empty_invariant_operations(target, exact)
            case MarkerRelation(target=target):
                return relation.reapply(self._strip_empty_invariant_operations(target, exact))
        return relation

    def _strip_count_invariant_operations(
        self,
        relation: Relation,
        exact: bool,
    ) -> Relation:
        """Return a modified relation tree that strips out relation operations
        that do not affect the number of rows from the end of the tree.

        Parameters
        ----------
        relation : `Relation`
            Original relation tree.
        exact : `bool`
            If `False` strip all iteration-engine operations, even those that
            can affect the number of rows.

        Returns
        -------
        modified : `Relation`
            Modified relation tree.
        """
        if relation.payload is not None or relation.is_locked:
            return relation
        match relation:
            case UnaryOperationRelation(operation=operation, target=target):
                if operation.is_count_invariant or (not exact and target.engine == self.iteration_engine):
                    return self._strip_count_invariant_operations(target, exact)
            case Transfer(target=target):
                return self._strip_count_invariant_operations(target, exact)
            case MarkerRelation(target=target):
                return relation.reapply(self._strip_count_invariant_operations(target, exact))
        return relation


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
        if self._context._exit_stack is None:
            # Have to read results into memory and close database connection.
            with self._context._db.query(self._sql_executable) as sql_result:
                rows = sql_result.mappings().fetchall()
            for sql_row in rows:
                yield self._row_transformer.sql_to_relation(sql_row)
        else:
            with self._context._db.query(self._sql_executable) as sql_result:
                raw_rows = sql_result.mappings()
                for sql_row in raw_rows:
                    yield self._row_transformer.sql_to_relation(sql_row)


class _SqlRowTransformer:
    """Object that converts SQLAlchemy result-row mappings to relation row
    mappings.

    Parameters
    ----------
    columns : `~collections.abc.Iterable` [ `ColumnTag` ]
        Set of columns to handle.  Rows must have at least these columns, but
        may have more.
    engine : `ButlerSqlEngine`
        Relation engine; used to transform column tags into SQL identifiers and
        obtain column type information.
    """

    def __init__(self, columns: Iterable[ColumnTag], engine: ButlerSqlEngine):
        self._scalar_columns: list[tuple[str, ColumnTag]] = []
        self._timespan_columns: list[tuple[str, ColumnTag, type[TimespanDatabaseRepresentation]]] = []
        for tag in columns:
            if is_timespan_column(tag):
                self._timespan_columns.append(
                    (engine.get_identifier(tag), tag, engine.column_types.timespan_cls)
                )
            else:
                self._scalar_columns.append((engine.get_identifier(tag), tag))
        self._has_no_columns = not (self._scalar_columns or self._timespan_columns)

    __slots__ = ("_scalar_columns", "_timespan_columns", "_has_no_columns")

    def sql_to_relation(self, sql_row: sqlalchemy.RowMapping) -> dict[ColumnTag, Any]:
        """Convert a result row from a SQLAlchemy result into the form expected
        by `lsst.daf.relation.iteration`.

        Parameters
        ----------
        sql_row : `~collections.abc.Mapping`
            Mapping with `str` keys and possibly-unpacked values for timespan
            columns.

        Returns
        -------
        relation_row : `~collections.abc.Mapping`
            Mapping with `ColumnTag` keys and `Timespan` objects for timespan
            columns.
        """
        relation_row = {tag: sql_row[identifier] for identifier, tag in self._scalar_columns}
        for identifier, tag, db_repr_cls in self._timespan_columns:
            relation_row[tag] = db_repr_cls.extract(sql_row, name=identifier)
        return relation_row

    def relation_to_sql(self, relation_row: Mapping[ColumnTag, Any]) -> dict[str, Any]:
        """Convert a `lsst.daf.relation.iteration` row into the mapping form
        used by SQLAlchemy.

        Parameters
        ----------
        relation_row : `~collections.abc.Mapping`
            Mapping with `ColumnTag` keys and `Timespan` objects for timespan
            columns.

        Returns
        -------
        sql_row : `~collections.abc.Mapping`
            Mapping with `str` keys and possibly-unpacked values for timespan
            columns.
        """
        sql_row = {identifier: relation_row[tag] for identifier, tag in self._scalar_columns}
        for identifier, tag, db_repr_cls in self._timespan_columns:
            db_repr_cls.update(relation_row[tag], result=sql_row, name=identifier)
        if self._has_no_columns:
            sql_row["IGNORED"] = None
        return sql_row
