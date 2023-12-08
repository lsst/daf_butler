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

__all__ = ("FindFirst",)

from functools import cached_property
from typing import TYPE_CHECKING, Literal

import pydantic

from ...dimensions import DimensionGroup
from ._base import InvalidRelationError, RelationBase, StringOrWildcard

if TYPE_CHECKING:
    from ._relation import Relation, RootRelation
    from ._select import Select
    from .joins import JoinArg


class FindFirst(RelationBase):
    """An abstract relation that finds the first dataset for each data ID
    in its ordered sequence of collections.

    This operation preserves all dimension columns but drops all dataset
    columns other than those for its target dataset type.
    """

    relation_type: Literal["find_first"] = "find_first"

    operand: Select
    """The upstream relation to operate on.

    This may have more than one `DatasetSearch` joined into it (at any level),
    as long as there is exactly one `DatasetSearch` for the ``dataset_type``
    of this operation.
    """

    dataset_type: str
    """The type of the datasets being searched for."""

    dimensions: DimensionGroup
    """The dimensions of the relation.

    This must be a subset of the dimensions of its operand, and is most
    frequently the dimensions of the dataset type.
    """

    def join(
        self,
        other: Relation,
        spatial_joins: JoinArg = frozenset(),
        temporal_joins: JoinArg = frozenset(),
    ) -> RootRelation:
        # If only Query objects (not *QueryResult) objects can be
        # explicitly joined, we may prohibit this logic branch at a
        # higher level, because only DatasetQueryResult objects should
        # have a tree with a FindFirst in it.
        raise InvalidRelationError(
            "Cannot join relations after a dataset find-first operation has been added. "
            "To avoid this error perform all joins before requesting dataset results."
        )

    def joined_on(self, *, spatial: JoinArg = frozenset(), temporal: JoinArg = frozenset()) -> FindFirst:
        return FindFirst(
            operand=self.operand.joined_on(spatial=spatial, temporal=temporal),
            dataset_type=self.dataset_type,
            dimensions=self.dimensions,
        )

    @cached_property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        return frozenset({self.dataset_type})

    @pydantic.model_validator(mode="after")
    def _validate_dimensions(self) -> FindFirst:
        if not self.dimensions <= self.operand.dimensions:
            raise ValueError(
                f"FindFirst dimensions {self.dimensions} are not a subset of its "
                f"operand's dimensions {self.operand.dimensions}."
            )
        return self

    @pydantic.model_validator(mode="after")
    def _validate_upstream_datasets(self) -> FindFirst:
        if self.dataset_type not in self.operand.available_dataset_types:
            raise ValueError(f"FindFirst target {self.dataset_type!r} is not available in {self.operand}.")
        return self
