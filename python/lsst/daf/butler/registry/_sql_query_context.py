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

from contextlib import ExitStack
from typing import TYPE_CHECKING, AbstractSet, Any, Iterable, Iterator, Mapping, Optional

import sqlalchemy

from ..core import SpatialConstraint, TemporalConstraint, ddl, sql

if TYPE_CHECKING:
    from .interfaces import Database, Session


class SqlQueryContext(sql.QueryContext):
    """An implementation of `sql.QueryContext` for `SqlRegistry`.

    Parameters
    ----------
    db : `Database`
        Object that abstracts the database engine.
    column_types : `sql.ColumnTypeInfo`
        Information about column types that can vary with registry
        configuration.
    """

    def __init__(
        self,
        db: Database,
        column_types: sql.ColumnTypeInfo,
    ):
        self._column_types = column_types
        self._db = db
        self._session: Optional[Session] = None
        self._exit_stack: Optional[ExitStack] = None

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

    def fetch(
        self,
        relation: sql.Relation,
        force_unique: bool = False,
        order_by: Iterable[sql.OrderByTerm] = (),
        offset: int = 0,
        limit: Optional[int] = None,
    ) -> Iterator[sql.ResultRow]:
        # Docstring inherited.
        sql_executable = relation.to_sql_executable(
            force_unique=force_unique, order_by=order_by, offset=offset, limit=limit
        )
        result_proxy = self._db.query(sql_executable)
        transformer = sql.RowTransformer(relation.columns, self._column_types)
        for raw_row in result_proxy.mappings():
            logical_row = transformer.raw_to_logical(raw_row)
            for p in relation.postprocessors:
                # MyPy can't spot that logical_row can only ever be None on the
                # RHS of this assignment.
                if (logical_row := p.apply(logical_row)) is None:  # type: ignore
                    break
            else:
                yield logical_row

    def upload(
        self,
        columns: AbstractSet[sql.ColumnTag],
        *logical_rows: sql.ResultRow,
        is_unique: bool = False,
        name: Optional[str] = None,
    ) -> sql.Relation:
        """Make external tabular data available to a ``SELECT`` query.

        Parameters
        ----------
        TODO
        *logical_rows : `Mapping`
            Values for the rows.  Keys are `sql.ColumnTag` instances, and
            `Timespan` values should be `Timespan` instances; these will be
            converted to the raw form used by the database as necessary.
        name : `str`, optional
            If provided, the name of the SQL construct.  If not provided, an
            opaque but unique identifier is generated.

        Returns
        -------
        TODO
        """
        if self._session is None:
            # TODO: improve this error message once we have a better idea of
            # what to tell the caller to do about this.
            raise RuntimeError("Cannot execute this query without a temporary table context.")
        assert self._exit_stack is not None, "Should be None iff self._session is None."
        spec = sql.ColumnTag.make_table_spec(columns, self._column_types)
        transformer = sql.RowTransformer(columns, self._column_types)
        raw_rows = [transformer.logical_to_raw(row) for row in logical_rows]
        builder = sql.Relation.build(
            self._exit_stack.enter_context(self._session.upload(spec, *raw_rows, name=name)),
            self._column_types,
        )
        builder.columns.update(
            sql.ColumnTag.extract_logical_column_mapping(
                columns, builder.sql_from.columns, self._column_types
            )
        )
        return builder.finish(is_materialized=True, is_unique=is_unique)

    def _make_temporary_table(
        self, spec: ddl.TableSpec, name: Optional[str] = None
    ) -> sqlalchemy.schema.Table:
        """Create a temporary table.

        Parameters
        ----------
        spec : `TableSpec`
            Specification for the table.
        name : `str`, optional
            A unique (within this session/connetion) name for the table.
            Subclasses may override to modify the actual name used.  If not
            provided, a unique name will be generated.

        Returns
        -------
        table : `sqlalchemy.schema.Table`
            SQLAlchemy representation of the table.

        Notes
        -----
        This is a simple forwarder for `Session.temporary_table` that takes
        ownership of the context manager it returns.
        """
        if self._session is None:
            # TODO: improve this error message once we have a better idea of
            # what to tell the caller to do about this.
            raise RuntimeError("Cannot execute this query without a temporary table context.")
        assert self._exit_stack is not None, "Should be None iff self._session is None."
        return self._exit_stack.enter_context(self._session.temporary_table(spec, name=name))

    def materialize(
        self, relation: sql.Relation, doomed: bool = False, name: Optional[str] = None
    ) -> sql.Relation:
        # TODO: add autoincrement field for better order by preservation.
        # TODO: consider fetching and applying postprocessors instead of
        # forwarding them.
        spec = sql.ColumnTag.make_table_spec(relation.columns, self._column_types)
        table = self._make_temporary_table(spec, name=name)
        if not doomed:
            self._db.insert(table, select=relation.to_sql_executable())
        builder = sql.Relation.build(table, self._column_types)
        builder.columns.update(
            sql.ColumnTag.extract_logical_column_mapping(relation.columns, table.columns, self._column_types)
        )
        result = builder.finish(relation.constraints, is_materialized=True, is_unique=relation.is_unique)
        if relation.postprocessors:
            return result.postprocessed(*relation.postprocessors)
        else:
            return result

    def add_spatial_constraint(
        self,
        relation: sql.Relation,
        constraint: SpatialConstraint,
        dimensions: AbstractSet[str] = frozenset(),
    ) -> sql.Relation:
        raise NotImplementedError("TODO")

    def add_temporal_constraint(
        self,
        relation: sql.Relation,
        constraint: TemporalConstraint,
        columns: AbstractSet[sql.ColumnTag] = frozenset(),
    ) -> sql.Relation:
        raise NotImplementedError("TODO")
