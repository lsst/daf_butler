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

__all__ = (
    "RootRelation",
    "make_unit_relation",
    "make_dimension_relation",
    "DataCoordinateUploadKey",
    "MaterializationKey",
    "MaterializationSpec",
    "DatasetSpec",
    "FindFirstSpec",
)

import uuid
from collections.abc import Mapping
from functools import cached_property
from typing import TYPE_CHECKING, TypeAlias, final

import pydantic

from ...dimensions import DimensionGroup, DimensionUniverse
from ...pydantic_utils import DeferredValidation
from ._base import InvalidRelationError, RelationTreeBase
from ._column_reference import DatasetFieldReference, DimensionFieldReference, DimensionKeyReference
from ._predicate import LiteralTrue, Predicate

if TYPE_CHECKING:
    from ._column_expression import OrderExpression


DataCoordinateUploadKey: TypeAlias = uuid.UUID

MaterializationKey: TypeAlias = uuid.UUID


def make_unit_relation(universe: DimensionUniverse) -> RootRelation:
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


def make_dimension_relation(dimensions: DimensionGroup) -> RootRelation:
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
    return RootRelation.model_construct(dimensions=dimensions)


@final
class MaterializationSpec(RelationTreeBase):
    datasets: frozenset[str]
    """Dataset types whose IDs were stored in the materialization."""

    dimensions: DimensionGroup
    """Dimensions whose keys were stored in the materialization."""


@final
class DatasetSpec(RelationTreeBase):
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
    tree is validated (since it requires a database query) and hence must be
    checked later.
    """


@final
class FindFirstSpec(RelationTreeBase):
    dataset_type: str
    """The type of the datasets being searched for."""

    dimensions: DimensionGroup
    """The dimensions of the relation.

    This must be a subset of the dimensions of its operand, and is most
    frequently the dimensions of the dataset type.
    """


@final
class RootRelation(RelationTreeBase):
    dimensions: DimensionGroup
    """The dimensions whose keys are joined into the relation."""

    datasets: Mapping[str, DatasetSpec] = pydantic.Field(default_factory=dict)

    data_coordinate_uploads: Mapping[DataCoordinateUploadKey, DimensionGroup] = pydantic.Field(
        default_factory=dict
    )

    materializations: Mapping[MaterializationKey, MaterializationSpec] = pydantic.Field(default_factory=dict)

    predicate: Predicate = LiteralTrue()
    """Boolean expression trees whose logical AND defines a row filter."""

    order_terms: tuple[OrderExpression, ...] = ()
    """Expressions to sort the rows by."""

    offset: int = 0
    """Index of the first row to return."""

    limit: int | None = None
    """Maximum number of rows to return, or `None` for no bound."""

    find_first: FindFirstSpec | None = None
    """A single result dataset type to search collections for in order,
    yielding one dataset for each of the nested dimensions.
    """

    @cached_property
    def available_dataset_types(self) -> frozenset[str]:
        result = frozenset(self.datasets.keys())
        for materialization_spec in self.materializations.values():
            result |= materialization_spec.datasets
        return result

    @cached_property
    def join_operand_dimensions(self) -> frozenset[DimensionGroup]:
        result: set[DimensionGroup] = set(self.data_coordinate_uploads.values())
        for dataset_spec in self.datasets.values():
            result.add(dataset_spec.dimensions)
        for materialization_spec in self.materializations.values():
            result.add(materialization_spec.dimensions)
        return frozenset(result)

    @property
    def result_dimensions(self) -> DimensionGroup:
        return self.dimensions if self.find_first is None else self.find_first.dimensions

    def join(self, other: RootRelation) -> RootRelation:
        """Return a new relation that represents a join between ``self`` and
        ``other``.

        Parameters
        ----------
        other : `RootRelation`
            Relation to join to this one.

        Returns
        -------
        result : `RootRelation`
            A new relation that joins ``self`` and ``other``.

        Raises
        ------
        InvalidRelationError
            Raised if the join is ambiguous or otherwise invalid.
        """
        if self.find_first is not None or other.find_first is not None:
            raise InvalidRelationError(
                "Cannot join queries after a dataset find-first operation has been added. "
                "To avoid this error perform all joins before requesting dataset results."
            )
        if self.offset or self.limit is not None or other.offset or other.limit is not None:
            raise InvalidRelationError(
                "Cannot join queries after an offset/limit slice has been added. "
                "To avoid this error perform all joins before adding an offset/limit slice, "
                "since those can only be performed on the final query."
            )
        if self.order_terms and other.order_terms:  # if one is sorted, fine.
            raise InvalidRelationError(
                "Cannot join two queries if both are either sorted. "
                "To avoid this error perform all joins before adding any sorting, "
                "since those can only be performed on the final query."
            )
        if not self.available_dataset_types.isdisjoint(other.available_dataset_types):
            raise InvalidRelationError(
                "Cannot join when both sides include the same dataset type: "
                f"{self.available_dataset_types & other.available_dataset_types}."
            )
        return RootRelation.model_construct(
            dimensions=self.dimensions | other.dimensions,
            datasets={**self.datasets, **other.datasets},
            data_coordinate_uploads={**self.data_coordinate_uploads, **other.data_coordinate_uploads},
            materializations={**self.materializations, **other.materializations},
            predicate=self.predicate.logical_and(other.predicate),
            order_terms=self.order_terms + other.order_terms,  # at least one of these is empty
        )

    def join_data_coordinate_upload(
        self, key: DataCoordinateUploadKey, dimensions: DimensionGroup
    ) -> RootRelation:
        # TODO: docs
        if key in self.data_coordinate_uploads:
            assert (
                dimensions == self.data_coordinate_uploads[key]
            ), f"Different dimensions for the same data coordinate upload key {key}!"
            return self
        data_coordinate_uploads = dict(self.data_coordinate_uploads)
        data_coordinate_uploads[key] = dimensions
        return self.model_copy(
            update=dict(
                dimensions=self.dimensions | dimensions, data_coordinate_uploads=data_coordinate_uploads
            )
        )

    def join_materialization(
        self, key: MaterializationKey, dimensions: DimensionGroup, datasets: frozenset[str]
    ) -> RootRelation:
        # TODO: docs
        spec = MaterializationSpec.model_construct(dimensions=dimensions, datasets=datasets)
        if key in self.materializations:
            assert spec == self.materializations[key], f"Different specs for the same materialization {key}!"
            return self
        if not spec.datasets.isdisjoint(self.available_dataset_types):
            raise InvalidRelationError(
                f"Materialization includes dataset(s) {spec.datasets & self.available_dataset_types} that "
                "are already present in the query."
            )
        materializations = dict(self.materializations)
        materializations[key] = spec
        return self.model_copy(
            update=dict(dimensions=self.dimensions | spec.dimensions, materializations=materializations)
        )

    def join_dataset(
        self, dataset_type: str, dimensions: DimensionGroup, collections: tuple[str, ...]
    ) -> RootRelation:
        # TODO: docs
        spec = DatasetSpec.model_construct(dimensions=dimensions, collections=collections)
        if dataset_type in self.datasets:
            if spec != self.datasets[dataset_type]:
                raise InvalidRelationError(
                    f"Dataset type {dataset_type!r} is already present in the query, with different "
                    "collections and/or dimensions."
                )
            return self
        if dataset_type in self.available_dataset_types:
            raise InvalidRelationError(
                f"Dataset type {dataset_type!r} is already present in the query via a materialization."
            )
        datasets = dict(self.datasets)
        datasets[dataset_type] = spec
        return self.model_copy(update=dict(dimensions=self.dimensions | spec.dimensions, datasets=datasets))

    def where(self, *terms: Predicate) -> RootRelation:
        """Return a new relation that adds row filtering via a boolean column
        expression.

        Parameters
        ----------
        *terms : `Predicate`
            Boolean column expressions that filter rows.  Arguments are
            combined with logical AND.

        Returns
        -------
        result : `RootRelation`
            A new relation that with row filtering.

        Raises
        ------
        InvalidRelationError
            Raised if a column expression requires a dataset column that is not
            already present in the relation tree.

        Notes
        -----
        If an expression references a dimension or dimension element that is
        not already present in the relation tree, it will be joined in, but
        datasets must already be joined into a relation tree in order to
        reference their fields in expressions.
        """
        full_dimension_names: set[str] = set(self.dimensions.names)
        where_predicate = self.predicate
        for where_term in terms:
            for column in where_term.gather_required_columns():
                match column:
                    case DimensionKeyReference(dimension=dimension):
                        full_dimension_names.add(dimension.name)
                    case DimensionFieldReference(element=element):
                        full_dimension_names.update(element.minimal_group.names)
                    case DatasetFieldReference(dataset_type=dataset_type):
                        if dataset_type not in self.datasets:
                            raise InvalidRelationError(f"Dataset search for column {column} is not present.")
            where_predicate = where_predicate.logical_and(where_term)
        full_dimensions = self.dimensions.universe.conform(full_dimension_names)
        return self.model_copy(update=dict(dimensions=full_dimensions, where_predicate=where_predicate))

    def order_by(self, *terms: OrderExpression, limit: int | None = None, offset: int = 0) -> RootRelation:
        """Return a new relation that sorts and/or applies positional slicing.

        Parameters
        ----------
        *terms : `str` or `OrderExpression`
            Expression objects to use for ordering.
        limit : `int` or `None`, optional
            Upper limit on the number of returned records.
        offset : `int`, optional
            The number of records to skip before returning at most ``limit``
            records.

        Returns
        -------
        result : `RootRelation`
            A new relation object whose results will be sorted and/or
            positionally sliced.

        Raises
        ------
        InvalidRelationError
            Raised if a column expression requires a dataset column that is not
            already present in the queries, or if the query already contains
            offset/limit constraints.

        Notes
        -----
        If an expression references a dimension or dimension element that is
        not already present in the queries, it will be joined in, but datasets
        must already be joined into a queries in order to reference their
        fields in expressions.
        """
        if not terms and not offset and limit is None:
            return self
        if self.offset or self.limit is not None:
            raise InvalidRelationError(
                "Cannot add sorting or new offset/limit to a query that already has offset/limit. "
                "To avoid this error add sorting first (since it is always applied first) or "
                "at the same time that offset/limit are added."
            )
        full_dimension_names: set[str] = set(self.dimensions.names)
        for term in terms:
            for column in term.gather_required_columns():
                match column:
                    case DimensionKeyReference(dimension=dimension):
                        full_dimension_names.add(dimension.name)
                    case DimensionFieldReference(element=element):
                        full_dimension_names.update(element.minimal_group.names)
                    case DatasetFieldReference(dataset_type=dataset_type):
                        if dataset_type not in self.datasets:
                            raise InvalidRelationError(f"Dataset search for column {column} is not present.")
        full_dimensions = self.dimensions.universe.conform(full_dimension_names)
        return self.model_copy(
            update=dict(
                dimensions=full_dimensions, order_terms=terms + self.order_terms, limit=limit, offset=offset
            )
        )

    def find_first_dataset(self, dataset_type: str, dimensions: DimensionGroup) -> RootRelation:
        """Return a new relation that searches a dataset's collections in
        order for the first match for each dataset type and data ID.

        Parameters
        ----------
        dataset_type : `str`
            Name of the dataset type.  Must be available in the relation tree
            already.
        dimensions : `DimensionGroup`
            Dimensions to group by.  This is typically the dimensions of the
            dataset type, but in certain cases (such as calibration lookups)
            it may be useful to user a superset of the dataset type's
            dimensions.

        Returns
        -------
        result : `RootRelation`
            A new root relation that includes the find-first search.
        """
        if self.find_first is not None:
            raise InvalidRelationError(
                f"Cannot add a find-first search for {dataset_type!r} to a query that already has a "
                f"find-first search for {self.find_first.dataset_type!r}."
            )
        if self.offset or self.limit is not None:
            raise InvalidRelationError(
                f"Cannot add a find-first search for {dataset_type!r} to a query that has offset or limit. "
                "To avoid this error add the find-first search and then add the offset or limit constraint, "
                "since this matches the order in which those operations will actually be applied."
            )
        if dataset_type not in self.available_dataset_types:
            raise InvalidRelationError(
                f"Dataset {dataset_type!r} must be joined in before it can be used in a find-first search."
            )
        return self.model_copy(
            update=dict(
                dimensions=self.dimensions | dimensions,
                find_first=FindFirstSpec.model_construct(dataset_type=dataset_type, dimensions=dimensions),
            )
        )

    @pydantic.model_validator(mode="after")
    def _validate_join_operands(self) -> RootRelation:
        for dimensions in self.join_operand_dimensions:
            if not dimensions.issubset(self.dimensions):
                raise InvalidRelationError(
                    f"Dimensions {dimensions} of join operand are not a "
                    f"subset of the root relation's dimensions {self.dimensions}."
                )
        return self

    @pydantic.model_validator(mode="after")
    def _validate_required_columns(self) -> RootRelation:
        required_columns = self.predicate.gather_required_columns()
        for term in self.order_terms:
            required_columns.update(term.gather_required_columns())
        for column in required_columns:
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


class DeferredValidationRootRelation(DeferredValidation[RootRelation]):
    pass
