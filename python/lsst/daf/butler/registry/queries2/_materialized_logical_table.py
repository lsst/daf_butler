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

__all__ = ("MaterializedLogicalTable",)

from typing import AbstractSet, Iterable, Optional

from ...core import DatasetType, DimensionGraph
from ...core.named import NamedValueAbstractSet
from ..interfaces.queries import ColumnTag, LogicalTable
from ..summaries import GovernorDimensionRestriction


class MaterializedLogicalTable(LogicalTable):
    def __init__(self, base: LogicalTable, name: Optional[str] = None):
        self._base = base
        self._name = name

    @property
    def dimensions(self) -> DimensionGraph:
        return self._base.dimensions

    @property
    def governor_dimension_restriction(self) -> GovernorDimensionRestriction:
        return self._base.governor_dimension_restriction

    @property
    def dataset_types(self) -> NamedValueAbstractSet[DatasetType]:
        return self._base.dataset_types

    @property
    def is_doomed(self) -> bool:
        return self._base.is_doomed

    def diagnostics(self, verbose: bool = False) -> Iterable[str]:
        return self._base.diagnostics(verbose)

    def insert_join(self, *others: LogicalTable) -> LogicalTable:
        if others:
            return MaterializedLogicalTable(self._base.insert_join(*others))
        else:
            return self

    @property
    def columns_provided(self) -> AbstractSet[ColumnTag]:
        return self._base.columns_provided
