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


from typing import Annotated, Literal, TypeAlias, Union

import pydantic

from ...dimensions import DataIdValue, DimensionGroup
from ...pydantic_utils import DeferredValidation
from ._column_expression import OrderExpression
from ._predicate import Predicate

JoinTuple: TypeAlias = Annotated[tuple[str, str], pydantic.AfterValidator(lambda t: tuple(sorted(t)))]


class DatasetSearch(pydantic.BaseModel):
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

    dimensions: DimensionGroup | None
    """The dimensions of the dataset type.

    If this is not `None`, the dimensions must match the actual dimensions of
    the dataset type.  If it is `None`, this search may include multiple
    dataset types with different dimensions, but it will not be usable as an
    operand in relation operations that require dimensions.
    """


class DataCoordinateUpload(pydantic.BaseModel):
    """An abstract relation that represents (and holds) user-provided data
    ID values.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["data_coordinate_upload"] = "data_coordinate_upload"

    dimensions: DimensionGroup
    """The dimensions of the data IDs."""

    rows: frozenset[tuple[DataIdValue, ...]]
    """The required values of the data IDs."""


class DimensionJoin(pydantic.BaseModel):
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


class Selection(pydantic.BaseModel):
    """An abstract relation operation that filters out rows based on a
    boolean expression.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["selection"] = "selection"

    operand: Relation
    """Upstream relation to operate on."""

    predicate: Predicate
    """Boolean expression tree that defines the filter."""

    @property
    def dimensions(self) -> DimensionGroup | None:
        """The dimensions of this abstract relation."""
        return self.operand.dimensions


class DimensionProjection(pydantic.BaseModel):
    """An abstract relation operation that drops dimension columns from its
    operand.

    Any dataset columns present are always preserved.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["dimension_projection"] = "dimension_projection"

    operand: Relation
    """The upstream relation to operate on.

    This must have dimensions that are not `None`.
    """

    dimensions: DimensionGroup
    """The dimensions of the new relation.

    This must be a subset of the original relation's dimensions.
    """


class OrderedSlice(pydantic.BaseModel):
    """An abstract relation operation that sorts and/or integer-slices the rows
    of its operand.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
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
    def dimensions(self) -> DimensionGroup | None:
        """The dimensions of this abstract relation."""
        return self.operand.dimensions


class Chain(pydantic.BaseModel):
    """An abstract relation whose rows are the union of the rows of its
    operands.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["chain"] = "chain"

    operands: tuple[Relation, ...] = pydantic.Field(min_length=2)
    """The upstream relations to combine.

    Order is not necessarily preserved.
    """

    @property
    def dimensions(self) -> DimensionGroup | None:
        """The dimensions of this relation and all of its operands."""
        return self.operands[0].dimensions


class FindFirst(pydantic.BaseModel):
    """An abstract relation that finds the first dataset for each data ID
    in its ordered sequence of collections.

    This operation preserves all dimension columns but drops all dataset
    columns other than those for its target dataset type.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
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
    def dimensions(self) -> DimensionGroup | None:
        """The dimensions of this abstract relation."""
        return self.operand.dimensions


class Materialization(pydantic.BaseModel):
    """An abstract relation that represent evaluating the upstream relation
    and saving its rows somewhere (e.g. a temporary table or Parquet file).
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid")
    relation_type: Literal["materialization"] = "materialization"

    operand: Relation
    """The upstream relation to evaluate."""

    @property
    def dimensions(self) -> DimensionGroup | None:
        """The dimensions of this abstract relation."""
        return self.operand.dimensions


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
