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

__all__ = ("FindFirstDataset",)

import dataclasses
from collections.abc import Sequence, Set
from typing import final

from lsst.daf.relation import ColumnTag, Relation, RowFilter
from lsst.utils.classes import cached_getter

from ...core import DatasetColumnTag, DimensionKeyColumnTag


@final
@dataclasses.dataclass(frozen=True)
class FindFirstDataset(RowFilter):
    """A custom relation operation that selects the first dataset from an
    upstream relation according to its collection rank.
    """

    dimensions: Sequence[DimensionKeyColumnTag]
    """Dimensions to group by while finding the first dataset within each group
    (`~collections.abc.Sequence` [ `DimensionKeyColumnTag` ]).
    """

    rank: DatasetColumnTag
    """Dataset rank column whose lowest per-group values should be selected
    (`DatasetColumnTag`).
    """

    @property
    @cached_getter
    def columns_required(self) -> Set[ColumnTag]:
        # Docstring inherited.
        result: set[ColumnTag] = {self.rank}
        result.update(self.dimensions)
        return result

    @property
    def is_empty_invariant(self) -> bool:
        # Docstring inherited.
        return True

    @property
    def is_order_dependent(self) -> bool:
        # Docstring inherited.
        return False

    def __str__(self) -> str:
        return "find_first"

    def applied_min_rows(self, target: Relation) -> int:
        # Docstring inherited.
        return 1 if target.min_rows else 0
