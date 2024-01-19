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

__all__ = ("Postprocessing",)

import dataclasses
from collections.abc import Iterable, Iterator

import sqlalchemy
from lsst.sphgeom import Region

from ..dimensions import DimensionElement
from ..queries import tree as qt


@dataclasses.dataclass
class Postprocessing:
    spatial_join_filtering: list[tuple[DimensionElement, DimensionElement]] = dataclasses.field(
        default_factory=list
    )
    spatial_where_filtering: list[tuple[DimensionElement, Region]] = dataclasses.field(default_factory=list)

    # TODO: make sure offset and limit are only set if there is spatial
    # filtering.

    offset: int = 0

    limit: int | None = None

    def __bool__(self) -> bool:
        return bool(self.spatial_join_filtering or self.spatial_where_filtering)

    def gather_columns_required(self, columns: qt.ColumnSet) -> None:
        for element in self.iter_region_dimension_elements():
            columns.update_dimensions(element.minimal_group)
            columns.dimension_fields[element.name].add("region")

    def iter_region_dimension_elements(self) -> Iterator[DimensionElement]:
        for a, b in self.spatial_join_filtering:
            yield a
            yield b
        for element, _ in self.spatial_where_filtering:
            yield element

    def iter_missing(self, columns: qt.ColumnSet) -> Iterator[DimensionElement]:
        done: set[DimensionElement] = set()
        for element in self.iter_region_dimension_elements():
            if element not in done:
                if "region" not in columns.dimension_fields.get(element.name, frozenset()):
                    yield element
                done.add(element)

    def apply(self, rows: Iterable[sqlalchemy.Row]) -> Iterable[sqlalchemy.Row]:
        if not self:
            return rows
        raise NotImplementedError("TODO")
