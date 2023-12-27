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

__all__ = ("Select", "make_unit_relation", "make_dimension_relation", "convert_where_args")

import itertools
from collections.abc import Mapping
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal, final

import pydantic

from ...dimensions import DataId, DimensionGroup, DimensionUniverse
from ._base import InvalidRelationError, RelationBase
from ._column_reference import DatasetFieldReference, DimensionFieldReference, DimensionKeyReference
from ._predicate import LiteralTrue, Predicate

if TYPE_CHECKING:
    from ._column_expression import OrderExpression
    from ._find_first import FindFirst
    from ._ordered_slice import OrderedSlice
    from ._relation import JoinOperand, Relation, RootRelation


def make_unit_relation(universe: DimensionUniverse) -> Select:
    """Make an initial relation with empty dimensions and a single logical row.

    This method should be used by `Butler._query` to construct the initial
    relation tree.  This relation is a useful initial state because it is the
    identity relation for joins, in that joining any other relation to this
    relation yields that relation.

    Parameters
    ----------
    universe : `..DimensionUniverse`
        Definitions for all dimensions.

    Returns
    -------
    relation : `Select`
        A select relation with empty dimensions.
    """
    return make_dimension_relation(universe.empty.as_group())


def make_dimension_relation(dimensions: DimensionGroup) -> Select:
    """Make an initial relation with the given dimensions.

    Parameters
    ----------
    dimensions : `..DimensionGroup`
        Definitions for all dimensions.

    Returns
    -------
    relation : `Select`
        A select relation with the given dimensions.
    """
    return Select.model_construct(dimensions=dimensions)


@final
class Select(RelationBase):
    """An abstract relation that combines joins and row-selection within a
    fixed set of dimensions.

    Notes
    -----
    Joins on dataset IDs are expected to be expressed as
    `abstract_expressions.InRelation` predicates used in the `where` attribute.
    That is slightly more powerful (since it can do set differences via
    `abstract_expressions.LogicalNot`) and it keeps the abstract relation tree
    simpler if the only join constraints in play here are on dimension columns.
    """

    relation_type: Literal["select"] = "select"

    dimensions: DimensionGroup
    """The dimensions of the relation."""

    join_operands: tuple[JoinOperand, ...] = ()
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

    where_predicate: Predicate = LiteralTrue()
    """Boolean expression trees whose logical AND defines a row filter."""

    @cached_property
    def available_dataset_types(self) -> frozenset[str]:
        # Docstring inherited.
        result: set[str] = set()
        for operand in self.join_operands:
            result.update(operand.available_dataset_types)
        return frozenset(result)

    def join(self, other: Relation) -> Select | OrderedSlice:
        # Docstring inherited.
        from ._find_first import FindFirst
        from ._ordered_slice import OrderedSlice

        # We intentionally call Select(...) below rather than
        # Select.model_construct(...) here to get validation; if that gets
        # expensive, we could look into running just the validations we can't
        # guarantee via typing and invariants of arguments.
        match other:
            case Select():
                return Select(
                    dimensions=self.dimensions | other.dimensions,
                    join_operands=self.join_operands + other.join_operands,
                    where_predicate=self.where_predicate.logical_and(other.where_predicate),
                )
            case FindFirst() | OrderedSlice():
                return other.join(self)
            case _:  # All JoinOperands
                return Select(
                    dimensions=self.dimensions | other.dimensions,
                    join_operands=self.join_operands + (other,),
                    where_predicate=self.where_predicate,
                )
        raise AssertionError("Invalid relation type for join.")

    def where(self, *terms: Predicate) -> Select:
        # Docstring inherited.
        full_dimension_names: set[str] = set(self.dimensions.names)
        where_predicate = self.where_predicate
        for where_term in terms:
            for column in where_term.gather_required_columns():
                match column:
                    case DimensionKeyReference(dimension=dimension):
                        full_dimension_names.add(dimension.name)
                    case DimensionFieldReference(element=element):
                        full_dimension_names.update(element.minimal_group.names)
                    case DatasetFieldReference(dataset_type=dataset_type):
                        if dataset_type not in self.available_dataset_types:
                            raise InvalidRelationError(f"Dataset search for column {column} is not present.")
            where_predicate = where_predicate.logical_and(where_term)
        full_dimensions = self.dimensions.universe.conform(full_dimension_names)
        return Select(
            dimensions=full_dimensions,
            join_operands=self.join_operands,
            where_predicate=where_predicate,
        )

    def order_by(self, *terms: OrderExpression, limit: int | None = None, offset: int = 0) -> OrderedSlice:
        # Docstring inherited.
        return OrderedSlice(operand=self, order_terms=terms, limit=limit, offset=offset)

    def find_first(self, dataset_type: str, dimensions: DimensionGroup) -> FindFirst:
        # Docstring inherited.
        return FindFirst(operand=self, dataset_type=dataset_type, dimensions=dimensions)

    @pydantic.model_validator(mode="after")
    def _validate_join_operands(self) -> Select:
        for operand in self.join_operands:
            if not operand.dimensions.issubset(self.dimensions):
                raise InvalidRelationError(
                    f"Dimensions {operand.dimensions} of join operand {operand} are not a "
                    f"subset of the join's dimensions {self.dimensions}."
                )
        return self

    @pydantic.model_validator(mode="after")
    def _validate_upstream_datasets(self) -> Select:
        for a, b in itertools.combinations(self.join_operands, 2):
            if not a.available_dataset_types.isdisjoint(b.available_dataset_types):
                common = a.available_dataset_types & b.available_dataset_types
                if None in common:
                    raise InvalidRelationError(
                        f"Upstream relations {a} and {b} both have a dataset wildcard."
                    )
                else:
                    raise InvalidRelationError(
                        f"Upstream relations {a} and {b} both have dataset types {common}."
                    )
        return self

    @pydantic.model_validator(mode="after")
    def _validate_required_columns(self) -> Select:
        for column in self.where_predicate.gather_required_columns():
            match column:
                case DimensionKeyReference(dimension=dimension):
                    if dimension.name not in self.dimensions:
                        raise InvalidRelationError(f"Column {column} is not in dimensions {self.dimensions}.")
                case DimensionFieldReference(element=element):
                    if element not in self.dimensions.elements:
                        raise InvalidRelationError(f"Column {column} is not in dimensions {self.dimensions}.")
                case DatasetFieldReference(dataset_type=dataset_type):
                    if dataset_type not in self.available_dataset_types:
                        raise InvalidRelationError(f"Dataset search for column {column} is not present.")
        return self


def convert_where_args(
    tree: RootRelation, *args: str | Predicate | DataId, bind: Mapping[str, Any] | None = None
) -> list[Predicate]:
    """Convert ``where`` arguments to a list of column expressions.

    Parameters
    ----------
    tree : `RootRelation`
        Relation whose rows will be filtered.
    *args : `str`, `Predicate`, `DataCoordinate`, or `~collections.abc.Mapping`
        Expressions to convert into predicates.
    bind : `~collections.abc.Mapping`, optional
        Mapping from identifier to literal value used when parsing string
        expressions.

    Returns
    -------
    predicates : `list` [ `Predicate` ]
        Standardized predicates, to be combined via logical AND.
    """
    raise NotImplementedError("TODO: Parse string expression.")
