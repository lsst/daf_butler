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

__all__ = ("SqlQueryBackend",)

from typing import TYPE_CHECKING, AbstractSet

import sqlalchemy
from lsst.utils.classes import cached_getter

from ..core import DimensionUniverse, ddl, sql
from ..core.named import NamedValueSet
from ._query_backend import QueryBackend
from ._sql_query_context import SqlQueryContext

if TYPE_CHECKING:
    from .interfaces import Database
    from .managers import RegistryManagerInstances


class SqlQueryBackend(QueryBackend):
    """An implementation of `QueryBackend` for `SqlRegistry`.

    Parameters
    ----------
    db : `Database`
        Object that abstracts the database engine.
    managers : `RegistryManagerInstances`
        Struct containing the manager objects that back a `SqlRegistry`.
    """

    def __init__(
        self,
        db: Database,
        managers: RegistryManagerInstances,
    ):
        self._db = db
        self._managers = managers

    def context(self) -> SqlQueryContext:
        # Docstring inherited.
        return SqlQueryContext(self._db, self._managers.column_types)

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._managers.dimensions.universe

    @property  # type: ignore
    @cached_getter
    def unit_relation(self) -> sql.Relation:
        # Docstring inherited.
        return sql.Relation.make_unit(
            self._db.constant_rows(
                NamedValueSet({ddl.FieldSpec("ignored", dtype=sqlalchemy.Boolean)}), {"ignored": True}
            ),
            self._managers.column_types,
        )

    def make_doomed_relation(self, *messages: str, columns: AbstractSet[sql.ColumnTag]) -> sql.Relation:
        # Docstring inherited.
        spec = sql.ColumnTag.make_table_spec(columns, self._managers.column_types)
        row = {str(tag): None for tag in columns}
        return sql.Relation.make_doomed(
            *messages,
            constant_row=self._db.constant_rows(spec.fields, row),
            columns=sql.ColumnTagSet._from_iterable(columns),
            column_types=self._managers.column_types,
        )
