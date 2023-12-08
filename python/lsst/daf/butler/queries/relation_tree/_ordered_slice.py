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

__all__ = ("OrderedSlice",)

from typing import TYPE_CHECKING, Literal

import pydantic

from ...dimensions import DimensionGroup
from ._base import InvalidRelationError, RelationBase, StringOrWildcard
from ._column_expression import OrderExpression
from ._column_reference import (
    ColumnReference,
    DatasetFieldReference,
    DimensionFieldReference,
    DimensionKeyReference,
)

if TYPE_CHECKING:
    from ._relation import OrderedSliceOperand, Relation, RootRelation
    from .joins import JoinArg


class OrderedSlice(RelationBase):
    """An abstract relation operation that sorts and/or integer-slices the rows
    of its operand.
    """

    relation_type: Literal["ordered_slice"] = "ordered_slice"

    operand: OrderedSliceOperand
    """The upstream relation to operate on."""

    order_by: tuple[OrderExpression, ...] = ()
    """Expressions to sort the rows by."""

    begin: int = 0
    """Index of the first row to return."""

    end: int | None = None
    """Index one past the last row to return, or `None` for no bound."""

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions of this abstract relation."""
        return self.operand.dimensions

    @property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        return self.operand.available_dataset_types

    def join(
        self,
        other: Relation,
        spatial_joins: JoinArg = "auto",
        temporal_joins: JoinArg = "auto",
    ) -> RootRelation:
        if self.begin or self.end is not None:
            raise InvalidRelationError(
                "Cannot join relations after an offset/limit slice has been added. "
                "To avoid this error perform all joins before adding an offset/limit slice, "
                "since those can only be performed on the final query."
            )
        if other.relation_type == "ordered_slice":
            raise InvalidRelationError(
                "Cannot join two relations if both are either sorted or sliced. "
                "To avoid this error perform all joins before adding any sorting or slicing, "
                "since those can only be performed on the final query."
            )
        # If this is a pure sort (and the only sort), we can do it downstream
        # of the new join, provided that join is actually allowed.
        return OrderedSlice(
            operand=self.operand.join(other, spatial_joins=spatial_joins, temporal_joins=temporal_joins),
            order_by=self.order_by,
        )

    @pydantic.model_validator(mode="after")
    def _validate_required_columns(self) -> OrderedSlice:
        required_columns: set[ColumnReference] = set()
        for term in self.order_by:
            required_columns.update(term.gather_required_columns())
        for column in required_columns:
            match column:
                case DimensionKeyReference(dimension=dimension):
                    if dimension not in self.dimensions:
                        raise ValueError(f"Order-by column {column} is not in dimensions {self.dimensions}.")
                case DimensionFieldReference(element=element):
                    if element not in self.dimensions.elements:
                        raise ValueError(f"Order-by column {column} is not in dimensions {self.dimensions}.")
                case DatasetFieldReference(dataset_type=dataset_type):
                    if dataset_type not in self.available_dataset_types:
                        raise ValueError(f"Dataset search for order-by column {column} is not present.")
        return self
