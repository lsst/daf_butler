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
    "TopologicalJoin",
    "TopologicalJoinFactory",
)

import itertools
from typing import AbstractSet, Iterable

from lsst.utils.classes import cached_getter

from ...core import DimensionGraph, DimensionUniverse, TopologicalSpace
from .._defaults import RegistryDefaults
from ..interfaces.queries import (
    ColumnTag,
    LogicalTable,
    LogicalTableFactory,
    QueryConstructionDataRequest,
    QueryConstructionDataResult,
)
from ..summaries import GovernorDimensionRestriction


class TopologicalJoinFactory(LogicalTableFactory):
    def __init__(
        self,
        target1: LogicalTableFactory,
        target2: LogicalTableFactory,
        /,
        space: TopologicalSpace,
    ):
        self._target1 = target1
        self._target2 = target2
        self._space = space

    @property
    def data_requested(self) -> QueryConstructionDataRequest:
        return self._target1.data_requested.union(self._target2.data_requested)

    def make_logical_table(
        self,
        data: QueryConstructionDataResult,
        columns_requested: AbstractSet[ColumnTag],
        *,
        defaults: RegistryDefaults,
        universe: DimensionUniverse,
    ) -> LogicalTable:
        return TopologicalJoin(
            self._target1.make_logical_table(data, columns_requested, defaults=defaults, universe=universe),
            self._target2.make_logical_table(data, columns_requested, defaults=defaults, universe=universe),
            space=self._space,
        )


class TopologicalJoin(LogicalTable):
    def __init__(self, target1: LogicalTable, target2: LogicalTable, /, space: TopologicalSpace):
        self._target1 = target1
        self._target2 = target2
        self._space = space

    @property  # type: ignore
    @cached_getter
    def dimensions(self) -> DimensionGraph:
        return self._target1.dimensions.union(self._target2.dimensions)

    @property  # type: ignore
    @cached_getter
    def governor_restriction(self) -> GovernorDimensionRestriction:
        return self._target1.governor_dimension_restriction.intersection(
            self._target2.governor_dimension_restriction
        )

    @property
    def is_doomed(self) -> bool:
        return self._target1.is_doomed or self._target2.is_doomed

    def diagnostics(self, verbose: bool = False) -> Iterable[str]:
        return itertools.chain(self._target1.diagnostics(verbose), self._target2.diagnostics(verbose))

    @property  # type: ignore
    @cached_getter
    def columns_provided(self) -> AbstractSet[ColumnTag]:
        columns: set[ColumnTag] = set()
        columns.update(self._target1.columns_provided)
        columns.update(self._target2.columns_provided)
        return frozenset(columns)
