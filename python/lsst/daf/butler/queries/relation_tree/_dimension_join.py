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

__all__ = ("DimensionJoin",)

import itertools
from functools import cached_property
from typing import TYPE_CHECKING, Annotated, Literal, TypeAlias

import pydantic

from ...dimensions import DimensionGroup
from ._base import RelationBase, StringOrWildcard

if TYPE_CHECKING:
    from ._relation import Relation


def _validate_join_tuple(original: tuple[str, str]) -> tuple[str, str]:
    if original[0] < original[1]:
        return original
    if original[0] > original[1]:
        return original[::-1]
    raise ValueError("Join tuples may not represent self-joins.")


JoinTuple: TypeAlias = Annotated[tuple[str, str], pydantic.AfterValidator(_validate_join_tuple)]


class DimensionJoin(RelationBase):
    """An abstract relation that represents a join between dimension-element
    tables and (optionally) other relations.

    Notes
    -----
    Joins on dataset IDs are expected to be expressed as
    `abstract_expressions.InRelation` predicates in `Selection` operations.
    That is slightly more powerful (since it can do set differences via
    `abstract_expressions.LogicalNot`) and it keeps the abstract relation tree
    simpler if the only join constraints in play are on dimension columns.
    """

    relation_type: Literal["dimension_join"] = "dimension_join"

    dimensions: DimensionGroup
    """The dimensions of the relation."""

    operands: tuple[Relation, ...] = ()
    """Relations to include in the join other than dimension-element tables.

    Because dimension-element tables are expected to contain the full set of
    values for their primary keys that could exist anywhere, they are only
    actually joined in when resolving this abstract relation if they provide
    a column or relationship not provided by one of these operands.  For
    example, if one operand is a `DatasetSearch` for a dataset with dimensions
    ``{instrument, detector}``, and the dimensions here are
    ``{instrument, physical_filter}``, there is no need to join in the
    ``detector`` table, but the ``physical_filter`` table will be joined in.
    """

    spatial: frozenset[JoinTuple] = frozenset()
    """Pairs of dimension element names that should whose regions on the sky
    must overlap.
    """

    temporal: frozenset[JoinTuple] = frozenset()
    """Pairs of dimension element names and calibration dataset type names
    whose timespans must overlap.
    """

    @cached_property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        result: set[StringOrWildcard] = set()
        for operand in self.operands:
            result.update(operand.available_dataset_types)
        return frozenset(result)

    @pydantic.model_validator(mode="after")
    def _validate_operands(self) -> DimensionJoin:
        for operand in self.operands:
            if not operand.dimensions.issubset(self.dimensions):
                raise ValueError(
                    f"Dimensions {operand.dimensions} of join operand {operand} are not a "
                    f"subset of the join's dimensions {self.dimensions}."
                )
        return self

    @pydantic.model_validator(mode="after")
    def _validate_spatial(self) -> DimensionJoin:
        def check_operand(operand: str) -> str:
            if operand not in self.dimensions.elements:
                raise ValueError(f"Spatial join operand {operand!r} is not in this join's dimensions.")
            family = self.dimensions.universe[operand].spatial
            if family is None:
                raise ValueError(f"Spatial join operand {operand!r} is not associated with a region.")
            return family.name

        for a, b in self.spatial:
            if check_operand(a) == check_operand(b):
                raise ValueError(f"Spatial join between {a!r} and {b!r} is unnecessary.")
        return self

    @pydantic.model_validator(mode="after")
    def _validate_temporal(self) -> DimensionJoin:
        def check_operand(operand: str) -> str:
            if operand in self.dimensions.elements:
                family = self.dimensions.universe[operand].temporal
                if family is None:
                    raise ValueError(f"Temporal join operand {operand!r} is not associated with a region.")
                return family.name
            elif operand in self.available_dataset_types:
                return "validity"
            else:
                raise ValueError(
                    f"Temporal join operand {operand!r} is not in this join's dimensions or dataset types."
                )

        for a, b in self.spatial:
            if check_operand(a) == check_operand(b):
                raise ValueError(f"Temporal join between {a!r} and {b!r} is unnecessary or impossible.")
        return self

    @pydantic.model_validator(mode="after")
    def _validate_upstream_datasets(self) -> DimensionJoin:
        for a, b in itertools.combinations(self.operands, 2):
            if not a.available_dataset_types.isdisjoint(b.available_dataset_types):
                common = a.available_dataset_types & b.available_dataset_types
                if None in common:
                    raise ValueError(f"Upstream relations {a} and {b} both have a dataset wildcard.")
                else:
                    raise ValueError(f"Upstream relations {a} and {b} both have dataset types {common}.")
        return self
