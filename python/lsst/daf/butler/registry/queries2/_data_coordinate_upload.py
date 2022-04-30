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

__all__ = ("DataCoordinateUpload",)


from typing import AbstractSet, Iterable, Optional

from lsst.utils.classes import cached_getter

from ...core import DataCoordinateSet, DimensionGraph
from ..interfaces.queries import ColumnTag, DimensionKeyColumnTag, LogicalTable
from ..summaries import GovernorDimensionRestriction


class DataCoordinateUpload(LogicalTable):
    def __init__(self, iterable: DataCoordinateSet, name: Optional[str] = None):
        self._iterable = iterable
        self._name = name
        raise NotImplementedError("TODO: create table spec.")

    @property
    def dimensions(self) -> DimensionGraph:
        return self._iterable.graph

    @property
    def governor_restriction(self) -> GovernorDimensionRestriction:
        return GovernorDimensionRestriction.makeFull()

    @property
    def is_doomed(self) -> bool:
        return bool(self._iterable)

    def diagnostics(self, verbose: bool = False) -> Iterable[str]:
        if not self._iterable:
            return ("No rows in data ID upload.",)
        else:
            return ()

    @property  # type: ignore
    @cached_getter
    def columns_provided(self) -> AbstractSet[ColumnTag]:
        columns: set[ColumnTag] = set()
        if self._iterable.hasFull():
            columns.update(DimensionKeyColumnTag.generate(self.dimensions.names))
        else:
            columns.update(DimensionKeyColumnTag.generate(self.dimensions.required.names))
        return frozenset(columns)
