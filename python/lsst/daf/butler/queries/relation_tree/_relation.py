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

__all__ = ("Relation", "DeferredValidationRelation")

import itertools
from functools import cached_property
from typing import Annotated, Literal, TypeAlias, Union

import pydantic

from ...dimensions import DataIdValue, DimensionGroup
from ...pydantic_utils import DeferredValidation
from ._base import RelationBase, StringOrWildcard
from ._column_expression import OrderExpression
from ._column_reference import (
    ColumnReference,
    DatasetFieldReference,
    DimensionFieldReference,
    DimensionKeyReference,
)
from ._predicate import Predicate


def _validate_join_tuple(original: tuple[str, str]) -> tuple[str, str]:
    if original[0] < original[1]:
        return original
    if original[0] > original[1]:
        return original[::-1]
    raise ValueError("Join tuples may not represent self-joins.")


JoinTuple: TypeAlias = Annotated[tuple[str, str], pydantic.AfterValidator(_validate_join_tuple)]


class DatasetSearch(RelationBase):
    """An abstract relation that represents a query for datasets."""

    relation_type: Literal["dataset_search"] = "dataset_search"

    dataset_type: StringOrWildcard
    """The name of the type of datasets returned by the query.

    ``...`` may be used to select all dataset types with the given
    ``dimensions``.
    """

    collections: tuple[str, ...]
    """The collections to search.

    Order matters if this dataset type is later referenced by a `FindFirst`
    operation.  Collection wildcards are always resolved before being included
    in a dataset search.
    """

    dimensions: DimensionGroup
    """The dimensions of the dataset type.

    This must match the dimensions of the dataset type as already defined in
    the butler database, but this cannot generally be verified when a relation
    tree (since it requires a database query) and hence must be checked later.
    """

    @cached_property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        return frozenset({self.dataset_type})


class DataCoordinateUpload(RelationBase):
    """An abstract relation that represents (and holds) user-provided data
    ID values.
    """

    relation_type: Literal["data_coordinate_upload"] = "data_coordinate_upload"

    dimensions: DimensionGroup
    """The dimensions of the data IDs."""

    rows: frozenset[tuple[DataIdValue, ...]]
    """The required values of the data IDs."""

    @property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        return frozenset()

    # We probably should validate that the tuples in 'rows' have the right
    # length (len(dimensions.required)) and maybe the right types, but we might
    # switch to Arrow here before that actually matters.


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


class DimensionProjection(RelationBase):
    """An abstract relation operation that drops dimension columns from its
    operand.

    Any dataset columns present are always preserved.
    """

    relation_type: Literal["dimension_projection"] = "dimension_projection"

    operand: Relation
    """The upstream relation to operate on.
    """

    dimensions: DimensionGroup
    """The dimensions of the new relation.

    This must be a subset of the original relation's dimensions.
    """

    @property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        return self.operand.available_dataset_types

    @pydantic.model_validator(mode="after")
    def _validate_operand_dimensions(self) -> DimensionProjection:
        if not self.operand.dimensions.issuperset(self.dimensions):
            raise ValueError(
                f"Operand {self.operand} dimensions {self.operand.dimensions} are not a "
                f"superset of the projection dimensions {self.dimensions}."
            )
        return self


class OrderedSlice(RelationBase):
    """An abstract relation operation that sorts and/or integer-slices the rows
    of its operand.
    """

    relation_type: Literal["ordered_slice"] = "ordered_slice"

    operand: Relation
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


class FindFirst(RelationBase):
    """An abstract relation that finds the first dataset for each data ID
    in its ordered sequence of collections.

    This operation preserves all dimension columns but drops all dataset
    columns other than those for its target dataset type.
    """

    relation_type: Literal["find_first"] = "find_first"

    operand: Relation
    """The upstream relation to operate on.

    This may have more than one `DatasetSearch` joined into it (at any level),
    as long as there is exactly one `DatasetSearch` for the ``dataset_type``
    of this operation.
    """

    dataset_type: str
    """The type of the datasets being searched for."""

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions of this abstract relation."""
        assert self.operand.dimensions is not None, "checked by validator"
        return self.operand.dimensions

    @cached_property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        return frozenset({self.dataset_type})

    @pydantic.model_validator(mode="after")
    def _validate_upstream_datasets(self) -> FindFirst:
        if self.dataset_type not in self.operand.available_dataset_types:
            raise ValueError(f"FindFirst target {self.dataset_type!r} is not available in {self.operand}.")
        return self


class Materialization(RelationBase):
    """An abstract relation that represents evaluating the upstream relation
    and saving its rows somewhere (e.g. a temporary table or Parquet file).
    """

    relation_type: Literal["materialization"] = "materialization"

    operand: Relation
    """The upstream relation to evaluate."""

    dataset_types: frozenset[StringOrWildcard]
    """Dataset types whose IDs were stored in the materialization."""

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions of this abstract relation.

        The required and implied dimension key columns are always stored, while
        dimension record columns never are and must be joined in again.
        """
        return self.operand.dimensions

    @property
    def available_dataset_types(self) -> frozenset[StringOrWildcard]:
        """The dataset types whose ID columns (at least) are available from
        this relation.
        """
        return self.dataset_types

    @pydantic.model_validator(mode="after")
    def _validate_upstream_datasets(self) -> Materialization:
        if self.dataset_types.issubset(self.operand.available_dataset_types):
            raise ValueError(
                f"Materialization dataset types {self.dataset_types - self.operand.available_dataset_types} "
                f"are not available in {self.operand}."
            )
        return self


Relation: TypeAlias = Annotated[
    Union[
        DatasetSearch,
        DataCoordinateUpload,
        DimensionJoin,
        Selection,
        DimensionProjection,
        OrderedSlice,
        Chain,
        FindFirst,
        Materialization,
    ],
    pydantic.Field(discriminator="relation_type"),
]


DimensionJoin.model_rebuild()
Selection.model_rebuild()
DimensionProjection.model_rebuild()
OrderedSlice.model_rebuild()
Chain.model_rebuild()
FindFirst.model_rebuild()
Materialization.model_rebuild()


class DeferredValidationRelation(DeferredValidation[Relation]):
    pass
