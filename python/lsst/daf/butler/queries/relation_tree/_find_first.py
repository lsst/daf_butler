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
from typing import TYPE_CHECKING, Literal, NoReturn, final

import pydantic

from ...dimensions import DimensionGroup
from ._base import InvalidRelationError, RootRelationBase

if TYPE_CHECKING:
    from ._column_expression import OrderExpression
    from ._ordered_slice import OrderedSlice
    from ._predicate import Predicate
    from ._relation import Relation
    from ._select import Select
    from .joins import JoinArg


@final
class FindFirst(RootRelationBase):
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

    @cached_property
    def available_dataset_types(self) -> frozenset[str]:
        # Docstring inherited.
        return frozenset({self.dataset_type})

    def join(self, other: Relation) -> NoReturn:
        # Docstring inherited.
        raise InvalidRelationError(
            "Cannot join relations after a dataset find-first operation has been added. "
            "To avoid this error perform all joins before requesting dataset results."
        )

    def joined_on(self, *, spatial: JoinArg = frozenset(), temporal: JoinArg = frozenset()) -> FindFirst:
        # Docstring inherited.
        return FindFirst.model_construct(
            operand=self.operand.joined_on(spatial=spatial, temporal=temporal),
            dataset_type=self.dataset_type,
            dimensions=self.dimensions,
        )

    def where(self, *terms: Predicate) -> FindFirst:
        # Docstring inherited.
        return FindFirst.model_construct(
            operand=self.operand.where(*terms),
            dataset_type=self.dataset_type,
            dimensions=self.dimensions,
        )

    def order_by(self, *terms: OrderExpression, limit: int | None = None, offset: int = 0) -> OrderedSlice:
        # Docstring inherited.
        return OrderedSlice(operand=self, order_terms=terms, limit=limit, offset=offset)

    def find_first(self, dataset_type: str, dimensions: DimensionGroup) -> FindFirst:
        # Docstring inherited.
        if not (dimensions <= self.dimensions):
            raise InvalidRelationError(
                f"New find-first dimensions {dimensions} are not a subset of the current "
                f"dimensions {self.dimensions}."
            )
        if dataset_type != self.dataset_type:
            raise InvalidRelationError(
                f"Relation is already a find-first search for dataset type {self.dataset_type}."
            )
        return FindFirst.model_construct(
            operand=self.operand, dataset_type=self.dataset_type, dimensions=dimensions
        )

    @pydantic.model_validator(mode="after")
    def _validate_dimensions(self) -> FindFirst:
        if not self.dimensions <= self.operand.dimensions:
            raise InvalidRelationError(
                f"FindFirst dimensions {self.dimensions} are not a subset of its "
                f"operand's dimensions {self.operand.dimensions}."
            )
        return self

    @pydantic.model_validator(mode="after")
    def _validate_upstream_datasets(self) -> FindFirst:
        if self.dataset_type not in self.operand.available_dataset_types:
            raise InvalidRelationError(
                f"FindFirst target {self.dataset_type!r} is not available in {self.operand}."
            )
        return self
