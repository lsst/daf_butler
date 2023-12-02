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

__all__ = ("DiscriminatedRelation",)


from typing import TYPE_CHECKING, Annotated, Generic, Literal, TypeAlias, TypeVar, Union, cast

import pydantic

from ..dimensions import DataIdValue, DimensionGroup, DimensionUniverse
from ..dimensions._group import SortedSequenceSet

if TYPE_CHECKING:
    from .abstract_expressions import DiscriminatedOrderExpression, DiscriminatedPredicate


class UnvalidatedDimensionGroup(pydantic.RootModel[tuple[str, ...]]):
    @property
    def names(self) -> SortedSequenceSet:
        return SortedSequenceSet(self.root)


_D = TypeVar("_D", bound=DimensionGroup | UnvalidatedDimensionGroup)


class JoinTuple(tuple[str, str]):
    """A 2-element `tuple` of `str` used to specify a spatial or temporal join.

    This is just a tuple whose elements are always in lexicographical order,
    ensuring it can be put in `set` without the original order of those
    elements mattering.
    """

    def __new__(cls, a: str, b: str) -> JoinTuple:
        if a <= b:
            return super().__new__(cls, (a, b))  # type: ignore
        else:
            return super().__new__(cls, (b, a))  # type: ignore

    @property
    def a(self) -> str:
        return self[0]

    @property
    def b(self) -> str:
        return self[1]


def _validate_dimensions(
    universe: DimensionUniverse, original: DimensionGroup | UnvalidatedDimensionGroup
) -> DimensionGroup:
    if (dimensions := universe.conform(original.names)).names != original.names:
        raise ValueError(f"Validated dimensions {dimensions.names} != raw dimensions {original.names}.")
    return dimensions


class DatasetSearch(pydantic.BaseModel, Generic[_D]):
    """An abstract relation that represents a query for datasets."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["dataset_search"] = "dataset_search"

    dataset_type: str | None
    """The name of the type of datasets returned by the query.

    `None` may be used to select all dataset types with the given
    ``dimensions``, or all dataset types if ``dimensions`` is `None`.
    """

    collections: tuple[str, ...]
    """The collections to search.

    Order matters if this dataset type is later referenced by a `FindFirst`
    operation.  Collection wildcards are always resolved before being included
    in a dataset search.
    """

    dimensions: _D | None
    """The dimensions of the dataset type.

    If this is not `None`, the dimensions must match the actual dimensions of
    the dataset type.  If it is `None`, this search may include multiple
    dataset types with different dimensions, but it will not be usable as an
    operand in relation operations that require dimensions.
    """

    def with_universe(self, universe: DimensionUniverse) -> DatasetSearch[DimensionGroup]:
        if self.dimensions is None or isinstance(self.dimensions, DimensionGroup):
            return cast(DatasetSearch[DimensionGroup], self)
        dimensions = _validate_dimensions(universe, self.dimensions)
        return DatasetSearch.model_construct(
            dataset_type=self.dataset_type, collections=self.collections, dimensions=dimensions
        )


class DataCoordinateUpload(pydantic.BaseModel, Generic[_D]):
    """An abstract relation that represents (and holds) user-provided data
    ID values.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["data_coordinate_upload"] = "data_coordinate_upload"

    dimensions: _D
    """The dimensions of the data IDs."""

    rows: frozenset[tuple[DataIdValue, ...]]
    """The required values of the data IDs."""

    def with_universe(self, universe: DimensionUniverse) -> DataCoordinateUpload[DimensionGroup]:
        if isinstance(self.dimensions, DimensionGroup):
            return cast(DataCoordinateUpload[DimensionGroup], self)
        dimensions = _validate_dimensions(universe, self.dimensions)
        return DataCoordinateUpload.model_construct(dimensions=dimensions, rows=self.rows)


class DimensionJoin(pydantic.BaseModel, Generic[_D]):
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

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["dimension_join"] = "dimension_join"

    dimensions: _D
    """The dimensions of the relation."""

    operands: tuple[DiscriminatedRelation[_D], ...] = ()
    """Relations to include in the join other than dimension-element tables.

    Because dimension-element tables are expected to contain the full set of
    values for their primary keys that could exist anywhere, they are only
    actually joined in when resolving this abstract relation if they provide
    a column or relationship not provided by one of these operands.  For
    example, if one operand is a `DatasetSearch` for a dataset with dimensions
    ``{instrument, detector}``, and the dimensions here are
    ``{instrument, physical_filter}``, there is no need to join in the
    ``detector`` table, but the ``physical_filter`` table will be joined in.

    This may only include abstract relations whose dimensions are not `None`.
    Relations whose dimensions are *empty* may be included.
    """

    spatial: frozenset[JoinTuple] = frozenset()
    """Pairs of dimension element names that should whose regions on the sky
    must overlap.
    """

    temporal: frozenset[JoinTuple] = frozenset()
    """Pairs of dimension element names and calibration dataset type names
    whose timespans must overlap.
    """

    def with_universe(self, universe: DimensionUniverse) -> DimensionJoin[DimensionGroup]:
        if isinstance(self.dimensions, DimensionGroup):
            return cast(DimensionJoin[DimensionGroup], self)
        dimensions = _validate_dimensions(universe, self.dimensions)
        return DimensionJoin.model_construct(
            dimensions=dimensions,
            operands=tuple([operand.with_universe(universe) for operand in self.operands]),
            spatial=self.spatial,
            temporal=self.temporal,
        )


class Selection(pydantic.BaseModel, Generic[_D]):
    """An abstract relation operation that filters out rows based on a
    boolean expression.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["selection"] = "selection"

    operand: DiscriminatedRelation[_D]
    """Upstream relation to operate on."""

    predicate: DiscriminatedPredicate[_D]
    """Boolean expression tree that defines the filter."""

    @property
    def dimensions(self) -> _D | None:
        """The dimensions of this abstract relation."""
        return self.operand.dimensions

    def with_universe(self, universe: DimensionUniverse) -> Selection[DimensionGroup]:
        if self.dimensions is None or isinstance(self.dimensions, DimensionGroup):
            return cast(Selection[DimensionGroup], self)
        return Selection.model_construct(
            operand=self.operand.with_universe(universe),
            predicate=self.predicate.with_universe(universe),
        )


class DimensionProjection(pydantic.BaseModel, Generic[_D]):
    """An abstract relation operation that drops dimension columns from its
    operand.

    Any dataset columns present are always preserved.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["dimension_projection"] = "dimension_projection"

    operand: DiscriminatedRelation[_D]
    """The upstream relation to operate on.

    This must have dimensions that are not `None`.
    """

    dimensions: _D
    """The dimensions of the new relation.

    This must be a subset of the original relation's dimensions.
    """

    def with_universe(self, universe: DimensionUniverse) -> DimensionProjection[DimensionGroup]:
        if isinstance(self.dimensions, DimensionGroup):
            return cast(DimensionProjection[DimensionGroup], self)
        dimensions = _validate_dimensions(universe, self.dimensions)
        return DimensionProjection.model_construct(
            operand=self.operand.with_universe(universe),
            dimensions=dimensions,
        )


class OrderedSlice(pydantic.BaseModel, Generic[_D]):
    """An abstract relation operation that sorts and/or integer-slices the rows
    of its operand.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["ordered_slice"] = "ordered_slice"

    operand: DiscriminatedRelation[_D]
    """The upstream relation to operate on."""

    order_by: tuple[DiscriminatedOrderExpression, ...] = ()
    """Expressions to sort the rows by."""

    begin: int = 0
    """Index of the first row to return."""

    end: int | None = None
    """Index one past the last row to return, or `None` for no bound."""

    @property
    def dimensions(self) -> _D | None:
        """The dimensions of this abstract relation."""
        return self.operand.dimensions

    def with_universe(self, universe: DimensionUniverse) -> OrderedSlice[DimensionGroup]:
        if self.dimensions is None or isinstance(self.dimensions, DimensionGroup):
            return cast(OrderedSlice[DimensionGroup], self)
        return OrderedSlice.model_construct(
            operand=self.operand.with_universe(universe),
            order_by=self.order_by,
            begin=self.begin,
            end=self.end,
        )


class Chain(pydantic.BaseModel, Generic[_D]):
    """An abstract relation whose rows are the union of the rows of its
    operands.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["chain"] = "chain"

    operands: tuple[DiscriminatedRelation[_D], ...] = pydantic.Field(min_length=2)
    """The upstream relations to combine.

    Order is not necessarily preserved.
    """

    @property
    def dimensions(self) -> _D | None:
        """The dimensions of this relation and all of its operands."""
        return self.operands[0].dimensions

    def with_universe(self, universe: DimensionUniverse) -> Chain[DimensionGroup]:
        if self.dimensions is None or isinstance(self.dimensions, DimensionGroup):
            return cast(Chain[DimensionGroup], self)
        return Chain.model_construct(
            operands=tuple([operand.with_universe(universe) for operand in self.operands])
        )


class FindFirst(pydantic.BaseModel, Generic[_D]):
    """An abstract relation that finds the first dataset for each data ID
    in its ordered sequence of collections.

    This operation preserves all dimension columns but drops all dataset
    columns other than those for its target dataset type.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["find_first"] = "find_first"

    operand: DiscriminatedRelation[_D]
    """The upstream relation to operate on.

    This may have more than one `DatasetSearch` joined into it (at any level),
    as long as there is exactly one `DatasetSearch` for the ``dataset_type``
    of this operation.
    """

    dataset_type: str
    """The type of the datasets being searched for."""

    @property
    def dimensions(self) -> _D | None:
        """The dimensions of this abstract relation."""
        return self.operand.dimensions

    def with_universe(self, universe: DimensionUniverse) -> FindFirst[DimensionGroup]:
        if self.dimensions is None or isinstance(self.dimensions, DimensionGroup):
            return cast(FindFirst[DimensionGroup], self)
        return FindFirst.model_construct(
            operand=self.operand.with_universe(universe), dataset_type=self.dataset_type
        )


class Materialization(pydantic.BaseModel, Generic[_D]):
    """An abstract relation that represent evaluating the upstream relation
    and saving its rows somewhere (e.g. a temporary table or Parquet file).
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["materialization"] = "materialization"

    operand: DiscriminatedRelation[_D]
    """The upstream relation to evaluate."""

    @property
    def dimensions(self) -> _D | None:
        """The dimensions of this abstract relation."""
        return self.operand.dimensions

    def with_universe(self, universe: DimensionUniverse) -> Materialization[DimensionGroup]:
        if self.dimensions is None or isinstance(self.dimensions, DimensionGroup):
            return cast(Materialization[DimensionGroup], self)
        return Materialization.model_construct(operand=self.operand.with_universe(universe))


DiscriminatedRelation: TypeAlias = Annotated[
    Union[
        DatasetSearch[_D],
        DataCoordinateUpload[_D],
        DimensionJoin[_D],
        Selection[_D],
        DimensionProjection[_D],
        OrderedSlice[_D],
        Chain[_D],
        FindFirst[_D],
        Materialization[_D],
    ],
    pydantic.Field(discriminator="relation_type"),
]
