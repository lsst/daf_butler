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
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Mapping, Optional

from ..core import sql

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
    ) -> Iterator[Mapping[sql.ColumnTag, Any]]:
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
