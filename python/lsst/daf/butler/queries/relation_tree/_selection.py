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

__all__ = ("Selection",)

from typing import TYPE_CHECKING, Literal

import pydantic

from ...dimensions import DimensionGroup
from ._base import RelationBase, StringOrWildcard
from ._column_reference import DatasetFieldReference, DimensionFieldReference, DimensionKeyReference
from ._predicate import Predicate

if TYPE_CHECKING:
    from ._relation import Relation


class Selection(RelationBase):
    """An abstract relation operation that filters out rows based on a
    boolean expression.
    """

    relation_type: Literal["selection"] = "selection"

    operand: Relation
    """Upstream relation to operate on."""

    predicate: Predicate
    """Boolean expression tree that defines the filter."""

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

    @pydantic.model_validator(mode="after")
    def _validate_required_columns(self) -> Selection:
        for column in self.predicate.gather_required_columns():
            match column:
                case DimensionKeyReference(dimension=dimension):
                    if dimension not in self.dimensions:
                        raise ValueError(f"Predicate column {column} is not in dimensions {self.dimensions}.")
                case DimensionFieldReference(element=element):
                    if element not in self.dimensions.elements:
                        raise ValueError(f"Predicate column {column} is not in dimensions {self.dimensions}.")
                case DatasetFieldReference(dataset_type=dataset_type):
                    if dataset_type not in self.available_dataset_types:
                        raise ValueError(f"Dataset search for predicate column {column} is not present.")
        return self
