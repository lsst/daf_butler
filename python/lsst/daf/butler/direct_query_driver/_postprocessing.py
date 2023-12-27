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
import itertools
from collections.abc import Set
from typing import TYPE_CHECKING

from lsst.sphgeom import Region

from ..dimensions import DimensionElement
from ..queries import relation_tree as rt

if TYPE_CHECKING:
    pass


@dataclasses.dataclass
class Postprocessing:
    spatial_join_filtering: list[tuple[DimensionElement, DimensionElement]] = dataclasses.field(
        default_factory=list
    )
    spatial_where_filtering: list[tuple[DimensionElement, Region]] = dataclasses.field(default_factory=list)

    def gather_columns_required(self) -> Set[rt.ColumnReference]:
        result: set[rt.ColumnReference] = set()
        for element in itertools.chain.from_iterable(self.spatial_join_filtering):
            result.add(rt.DimensionFieldReference.model_construct(element=element, field="region"))
        for element, _ in self.spatial_join_filtering:
            result.add(rt.DimensionFieldReference.model_construct(element=element, field="region"))
        return result

    def __bool__(self) -> bool:
        return bool(self.spatial_join_filtering) or bool(self.spatial_where_filtering)
