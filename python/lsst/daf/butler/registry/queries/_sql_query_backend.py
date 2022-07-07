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

from typing import TYPE_CHECKING

from lsst.daf.relation import sql

from ._query_backend import QueryBackend
from ._sql_query_context import SqlQueryContext

if TYPE_CHECKING:
    from ...core import DimensionUniverse
    from ..interfaces import Database
    from ..managers import RegistryManagerInstances


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
        self._engine = sql.Engine("db")

    @property
    def managers(self) -> RegistryManagerInstances:
        # Docstring inherited.
        return self._managers

    def context(self) -> SqlQueryContext:
        # Docstring inherited.
        return SqlQueryContext(self._db, self._managers.column_types, self._engine)

    @property
    def universe(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._managers.dimensions.universe
