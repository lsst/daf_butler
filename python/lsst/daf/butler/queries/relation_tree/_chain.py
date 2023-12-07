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

__all__ = ("Chain",)

import itertools
from typing import TYPE_CHECKING, Literal

import pydantic

from ...dimensions import DimensionGroup
from ._base import RelationBase, StringOrWildcard

if TYPE_CHECKING:
    from ._relation import Relation


class Chain(RelationBase):
    """An abstract relation whose rows are the union of the rows of its
    operands.
    """

    relation_type: Literal["chain"] = "chain"

    operands: tuple[Relation, ...] = pydantic.Field(min_length=2)
    """The upstream relations to combine.

    Order is not necessarily preserved.
    """

    merge_dataset_types: tuple[StringOrWildcard, ...] | None
    """A single dataset type to propagate from each relation in `operands` into
    a single ``...``` dataset type in the chain relation.

    If `None`, all operands must have the same ``available_dataset_types``.
    """

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions of this relation and all of its operands."""
        return self.operands[0].dimensions

    @property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        if self.merge_dataset_types is not None:
            return frozenset({...})
        else:
            return self.operands[0].available_dataset_types

    @pydantic.model_validator(mode="after")
    def _validate_dimensions(self) -> Chain:
        for operand in self.operands[1:]:
            if operand.dimensions != self.operands[0].dimensions:
                raise ValueError(
                    f"Dimension conflict in chain: {self.operands[0].dimensions} != {operand.dimensions}."
                )
        return self

    @pydantic.model_validator(mode="after")
    def _validate_dataset_types(self) -> Chain:
        if self.merge_dataset_types is not None:
            for operand, dataset_type in zip(self.operands, self.merge_dataset_types, strict=True):
                if dataset_type not in operand.available_dataset_types:
                    raise ValueError(f"Operand {operand} is missing its merge dataset type {dataset_type!r}.")
        else:
            for a, b in itertools.combinations(self.operands, 2):
                if a.available_dataset_types != b.available_dataset_types:
                    raise ValueError(f"Operands {a} and {b} have different available dataset types.")
        return self
