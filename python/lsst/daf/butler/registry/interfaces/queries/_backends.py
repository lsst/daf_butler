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
    "QueryBackend",
    "SqlQueryBackend",
)

from abc import ABC, abstractmethod
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, Optional, final

import sqlalchemy

from ....core import DimensionUniverse, ddl
from ....core.named import NamedKeyDict, NamedValueSet
from ..._defaults import RegistryDefaults
from .._database import Database, Session
from ._construction_data import QueryConstructionDataRequest, QueryConstructionDataResult

if TYPE_CHECKING:
    from ...managers import RegistryManagerInstances


class QueryBackend(ABC):
    @property
    @abstractmethod
    def universe(self) -> DimensionUniverse:
        raise NotImplementedError()

    @property
    @abstractmethod
    def defaults(self) -> RegistryDefaults:
        raise NotImplementedError()

    @abstractmethod
    def fetch_construction_data(self, request: QueryConstructionDataRequest) -> QueryConstructionDataResult:
        raise NotImplementedError()


@final
class SqlQueryBackend(QueryBackend):
    def __init__(
        self,
        db: Database,
        managers: RegistryManagerInstances,
        defaults: RegistryDefaults,
    ):
        self.db = db
        self.managers = managers
        self._defaults = defaults
        self._exit_stack: Optional[ExitStack] = None
        self._session: Optional[Session] = None

    @property
    def universe(self) -> DimensionUniverse:
        return self.managers.dimensions.universe

    @property
    def defaults(self) -> RegistryDefaults:
        return self._defaults

    def fetch_construction_data(self, request: QueryConstructionDataRequest) -> QueryConstructionDataResult:
        return QueryConstructionDataResult(
            dataset_types=NamedValueSet(
                request.dataset_types.resolve_dataset_types(self.managers.datasets.parent_dataset_types),
            ),
            collections=NamedKeyDict(
                {
                    record: self.managers.datasets.getCollectionSummary(record)
                    for record in request.collections.iter(
                        self.managers.collections.records, includeChains=True, flattenChains=True
                    )
                }
            ),
        )

    def __enter__(self) -> SqlQueryBackend:
        assert self._exit_stack is None, "Context manager already entered."
        self._exit_stack = ExitStack().__enter__()
        self._session = self._exit_stack.enter_context(self.db.session())
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        assert self._exit_stack is not None, "Context manager not yet entered."
        result = self._exit_stack.__exit__(exc_type, exc_value, traceback)
        self._exit_stack = None
        self._session = None
        return result

    def upload(
        self,
        spec: ddl.TableSpec,
        *rows: dict,
        name: Optional[str] = None,
    ) -> sqlalchemy.sql.FromClause:
        if self._session is None:
            # TODO: improve this error message once we have a better idea of
            # what to tell the caller to do about this.
            raise RuntimeError("Cannot execute this query without a temporary table context.")
        assert self._exit_stack is not None, "Should be None iff self._session is None."
        return self._exit_stack.enter_context(self._session.upload(spec, *rows, name=name))

    def make_temporary_table(
        self, spec: ddl.TableSpec, name: Optional[str] = None
    ) -> sqlalchemy.schema.Table:
        if self._session is None:
            # TODO: improve this error message once we have a better idea of
            # what to tell the caller to do about this.
            raise RuntimeError("Cannot execute this query without a temporary table context.")
        assert self._exit_stack is not None, "Should be None iff self._session is None."
        return self._exit_stack.enter_context(self._session.temporary_table(spec, name=name))
