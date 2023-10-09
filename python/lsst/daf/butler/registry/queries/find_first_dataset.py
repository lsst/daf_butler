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

__all__ = ("FindFirstDataset",)

import dataclasses
from collections.abc import Sequence, Set
from typing import final

from lsst.daf.relation import ColumnTag, Relation, RowFilter, UnaryCommutator, UnaryOperationRelation
from lsst.utils.classes import cached_getter

from ..._column_tags import DatasetColumnTag, DimensionKeyColumnTag


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

    def commute(self, current: UnaryOperationRelation) -> UnaryCommutator:
        # Docstring inherited.
        if not self.columns_required <= current.target.columns:
            return UnaryCommutator(
                first=None,
                second=current.operation,
                done=False,
                messages=(
                    f"{current.target} is missing columns "
                    f"{set(self.columns_required - current.target.columns)}",
                ),
            )
        if current.operation.is_count_dependent:
            return UnaryCommutator(
                first=None,
                second=current.operation,
                done=False,
                messages=(f"{current.operation} is count-dependent",),
            )
        return UnaryCommutator(self, current.operation)
