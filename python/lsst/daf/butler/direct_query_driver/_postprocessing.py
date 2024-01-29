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

__all__ = ("Postprocessing", "ValidityRangeMatchError")

from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, ClassVar

import sqlalchemy
from lsst.sphgeom import DISJOINT, Region

from ..queries import tree as qt

if TYPE_CHECKING:
    from ..dimensions import DimensionElement


class ValidityRangeMatchError(RuntimeError):
    pass


class Postprocessing:
    def __init__(self) -> None:
        self.spatial_join_filtering: list[tuple[DimensionElement, DimensionElement]] = []
        self.spatial_where_filtering: list[tuple[DimensionElement, Region]] = []
        self.check_validity_match_count: bool = False
        self._offset: int = 0
        self._limit: int | None = None

    VALIDITY_MATCH_COUNT: ClassVar[str] = "_VALIDITY_MATCH_COUNT"

    @property
    def offset(self) -> int:
        return self._offset

    @offset.setter
    def offset(self, value: int) -> None:
        if value and not self:
            raise RuntimeError(
                "Postprocessing should only implement 'offset' if it needs to do spatial filtering."
            )
        self._offset = value

    @property
    def limit(self) -> int | None:
        return self._limit

    @limit.setter
    def limit(self, value: int | None) -> None:
        if value and not self:
            raise RuntimeError(
                "Postprocessing should only implement 'limit' if it needs to do spatial filtering."
            )
        self._limit = value

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
            yield from rows
        joins = [
            (
                qt.ColumnSet.get_qualified_name(a.name, "region"),
                qt.ColumnSet.get_qualified_name(b.name, "region"),
            )
            for a, b in self.spatial_join_filtering
        ]
        where = [
            (qt.ColumnSet.get_qualified_name(element.name, "region"), region)
            for element, region in self.spatial_where_filtering
        ]
        for row in rows:
            m = row._mapping
            if any(m[a].relate(m[b]) & DISJOINT for a, b in joins) or any(
                m[field].relate(region) & DISJOINT for field, region in where
            ):
                continue
            if self.check_validity_match_count and m[self.VALIDITY_MATCH_COUNT] > 1:
                raise ValidityRangeMatchError(
                    "Ambiguous calibration validity range match. This usually means a temporal join or "
                    "'where' needs to be added, but it could also mean that multiple validity ranges "
                    "overlap a single output data ID."
                )
            if self._offset:
                self._offset -= 1
                continue
            if self._limit == 0:
                break
            yield row
            if self._limit is not None:
                self._limit -= 1
