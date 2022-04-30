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
    "DimensionLogicalTable",
    "DimensionLogicalTableFactory",
)


from typing import AbstractSet, Iterable

from lsst.utils.classes import cached_getter

from ...core import DimensionElement, DimensionGraph, DimensionUniverse
from .._defaults import RegistryDefaults
from ..interfaces.queries import (
    ColumnTag,
    DimensionKeyColumnTag,
    LogicalTable,
    LogicalTableFactory,
    QueryConstructionDataRequest,
    QueryConstructionDataResult,
)
from ..summaries import GovernorDimensionRestriction


class DimensionLogicalTableFactory(LogicalTableFactory):
    def __init__(self, element: DimensionElement):
        self._element = element

    @property
    def data_requested(self) -> QueryConstructionDataRequest:
        return QueryConstructionDataRequest()

    def make_logical_table(
        self,
        data: QueryConstructionDataResult,
        columns_requested: AbstractSet[ColumnTag],
        *,
        defaults: RegistryDefaults,
        universe: DimensionUniverse,
    ) -> LogicalTable:
        return DimensionLogicalTable(self._element, columns_requested)


class DimensionLogicalTable(LogicalTable):
    def __init__(self, element: DimensionElement, extra_columns: Iterable[ColumnTag]):
        self._element = element
        self._extra_columns = frozenset(extra_columns)

    @property
    def dimensions(self) -> DimensionGraph:
        return self._element.graph

    @property
    def governor_restriction(self) -> GovernorDimensionRestriction:
        return GovernorDimensionRestriction.makeFull()

    @property
    def is_doomed(self) -> bool:
        return False

    def diagnostics(self, verbose: bool = False) -> Iterable[str]:
        return ()

    @property  # type: ignore
    @cached_getter
    def columns_provided(self) -> AbstractSet[ColumnTag]:
        columns: set[ColumnTag] = set()
        columns.update(DimensionKeyColumnTag.generate(self._element.dimensions.names))
        columns.update(self._extra_columns)
        return frozenset(columns)
