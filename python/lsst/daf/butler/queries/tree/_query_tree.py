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
    "QueryTree",
    "make_unit_query_tree",
    "make_dimension_query_tree",
    "DataCoordinateUploadKey",
    "MaterializationKey",
    "MaterializationSpec",
    "DatasetSearch",
    "DeferredValidationQueryTree",
)

import uuid
from collections.abc import Mapping
from functools import cached_property
from typing import TypeAlias, final

import pydantic

from ...dimensions import DimensionGroup, DimensionUniverse
from ...pydantic_utils import DeferredValidation
from ._base import InvalidQueryTreeError, QueryTreeBase
from ._column_reference import DatasetFieldReference, DimensionFieldReference, DimensionKeyReference
from ._predicate import LiteralTrue, Predicate

DataCoordinateUploadKey: TypeAlias = uuid.UUID

MaterializationKey: TypeAlias = uuid.UUID


def make_unit_query_tree(universe: DimensionUniverse) -> QueryTree:
    """Make an initial query tree with empty dimensions and a single logical
    row.

    This method should be used by `Butler._query` to construct the initial
    query tree.  This tree is a useful initial state because it is the
    identity for joins, in that joining any other query tree to this
    query tree yields that query tree.

    Parameters
    ----------
    universe : `..DimensionUniverse`
        Definitions for all dimensions.

    Returns
    -------
    tree : `QueryTree`
        A tree with empty dimensions.
    """
    return make_dimension_query_tree(universe.empty.as_group())


def make_dimension_query_tree(dimensions: DimensionGroup) -> QueryTree:
    """Make an initial query tree with the given dimensions.

    Parameters
    ----------
    dimensions : `..DimensionGroup`
        Definitions for all dimensions.

    Returns
    -------
    tree : `QueryTree`
        A tree with the given dimensions.
    """
    return QueryTree.model_construct(dimensions=dimensions)


@final
class MaterializationSpec(QueryTreeBase):
    """Information about a materialized query tree (e.g. one executed into a
    temporary table) that has been joined into another query tree.
    """

    dimensions: DimensionGroup
    """Dimensions whose keys were stored in the materialization."""

    resolved_datasets: frozenset[str]
    """Datasets that were fully resolved in the materialization (only one
    dataset for each data ID).

    The UUIDs of these datasets are stored in the materialization, while those
    for unresolved datasets are not, in order to allow materializations to
    always be unique over dimensions alone.
    """


@final
class DatasetSearch(QueryTreeBase):
    """Information about a dataset search joined into a query tree.

    The dataset type name is the key of the dictionary (in `QueryTree`) where
    this type is used as a value.
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
    tree is validated (since it requires a database query) and hence must be
    checked later.
    """


@final
class QueryTree(QueryTreeBase):
    """A declarative, serializable description of a butler query.

    This class's attributes describe the columns that "available" to be
    returned or used in ``where`` or ``order_by`` expressions, but it does not
    carry information about the columns that are actually included in result
    rows, or what kind of butler primitive (e.g. `DataCoordinate` or
    `DatasetRef`) those rows might be transformed into.
    """

    dimensions: DimensionGroup
    """The dimensions whose keys are joined into the query.
    """

    datasets: Mapping[str, DatasetSearch] = pydantic.Field(default_factory=dict)
    """Dataset searches that have been joined into the query."""

    data_coordinate_uploads: Mapping[DataCoordinateUploadKey, DimensionGroup] = pydantic.Field(
        default_factory=dict
    )
    """Uploaded tables of data ID values that have been joined into the query.
    """

    materializations: Mapping[MaterializationKey, MaterializationSpec] = pydantic.Field(default_factory=dict)
    """Tables of result rows from other queries that have been stored
    temporarily on the server.
    """

    predicate: Predicate = LiteralTrue()
    """Boolean expression trees whose logical AND defines a row filter."""

    find_first_dataset: str | None = None
    """A single result dataset type to search collections for in order,
    yielding one dataset for data ID with ``result_dimensions``.
    """

    @cached_property
    def join_operand_dimensions(self) -> frozenset[DimensionGroup]:
        """A set of sets of the dimensions of all data coordinate uploads,
        dataset searches, and materializations.
        """
        result: set[DimensionGroup] = set(self.data_coordinate_uploads.values())
        for dataset_spec in self.datasets.values():
            result.add(dataset_spec.dimensions)
        for materialization_spec in self.materializations.values():
            result.add(materialization_spec.dimensions)
        return frozenset(result)

    def join(self, other: QueryTree) -> QueryTree:
        """Return a new tree that represents a join between ``self`` and
        ``other``.

        Parameters
        ----------
        other : `QueryTree`
            Tree to join to this one.

        Returns
        -------
        result : `QueryTree`
            A new tree that joins ``self`` and ``other``.

        Raises
        ------
        InvalidQueryTreeError
            Raised if the join is ambiguous or otherwise invalid.
        """
        if self.find_first_dataset is not None or other.find_first_dataset is not None:
            raise InvalidQueryTreeError(
                "Cannot join queries after a dataset find-first operation has been added. "
                "To avoid this error perform all joins before requesting dataset results."
            )
        if not self.datasets.keys().isdisjoint(other.datasets.keys()):
            raise InvalidQueryTreeError(
                "Cannot join when both sides include the same dataset type: "
                f"{self.datasets.keys() & other.datasets.keys()}."
            )
        return QueryTree.model_construct(
            dimensions=self.dimensions | other.dimensions,
            datasets={**self.datasets, **other.datasets},
            data_coordinate_uploads={**self.data_coordinate_uploads, **other.data_coordinate_uploads},
            materializations={**self.materializations, **other.materializations},
            predicate=self.predicate.logical_and(other.predicate),
        )

    def join_data_coordinate_upload(
        self, key: DataCoordinateUploadKey, dimensions: DimensionGroup
    ) -> QueryTree:
        """Return a new tree that joins in an uploaded table of data ID values.

        Parameters
        ----------
        key : `DataCoordinateUploadKey`
            Unique identifier for this upload, as assigned by a `QueryDriver`.
        dimensions : `DimensionGroup`
            Dimensions of the data IDs.

        Returns
        -------
        result : `QueryTree`
            A new tree that joins in the data ID table.
        """
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

    def join_materialization(self, key: MaterializationKey, spec: MaterializationSpec) -> QueryTree:
        """Return a new tree that joins in temporarily stored results from
        another query.

        Parameters
        ----------
        key : `MaterializationKey`
            Unique identifier for this materialization, as assigned by a
            `QueryDriver`.
        spec : `MaterializationSpec`
            Struct containing the dimensions and resolved dataset types stored
            in the materialization.

        Returns
        -------
        result : `QueryTree`
            A new tree that joins in the materialization.
        """
        if key in self.materializations:
            assert spec == self.materializations[key], f"Different specs for the same materialization {key}!"
            return self
        materializations = dict(self.materializations)
        materializations[key] = spec
        return self.model_copy(
            update=dict(dimensions=self.dimensions | spec.dimensions, materializations=materializations)
        )

    def join_dataset(self, dataset_type: str, spec: DatasetSearch) -> QueryTree:
        """Return a new tree joins in a search for a dataset.

        Parameters
        ----------
        dataset_type : `str`
            Name of dataset type to join in.
        spec : `DatasetSpec`
            Struct containing the collection search path and dataset type
            dimensions.

        Returns
        -------
        result : `QueryTree`
            A new tree that joins in the dataset search.

        Raises
        ------
        InvalidQueryTreeError
            Raised if this dataset type is already present in the query tree.
        """
        if dataset_type in self.datasets:
            if spec != self.datasets[dataset_type]:
                raise InvalidQueryTreeError(
                    f"Dataset type {dataset_type!r} is already present in the query, with different "
                    "collections and/or dimensions."
                )
            return self
        datasets = dict(self.datasets)
        datasets[dataset_type] = spec
        return self.model_copy(update=dict(dimensions=self.dimensions | spec.dimensions, datasets=datasets))

    def where(self, *terms: Predicate) -> QueryTree:
        """Return a new tree that adds row filtering via a boolean column
        expression.

        Parameters
        ----------
        *terms : `Predicate`
            Boolean column expressions that filter rows.  Arguments are
            combined with logical AND.

        Returns
        -------
        result : `QueryTree`
            A new tree that with row filtering.

        Raises
        ------
        InvalidQueryTreeError
            Raised if a column expression requires a dataset column that is not
            already present in the query tree.

        Notes
        -----
        If an expression references a dimension or dimension element that is
        not already present in the query tree, it will be joined in, but
        datasets must already be joined into a query tree in order to reference
        their fields in expressions.
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
                            raise InvalidQueryTreeError(f"Dataset search for column {column} is not present.")
            where_predicate = where_predicate.logical_and(where_term)
        full_dimensions = self.dimensions.universe.conform(full_dimension_names)
        return self.model_copy(update=dict(dimensions=full_dimensions, where_predicate=where_predicate))

    def find_first(self, dataset_type: str) -> QueryTree:
        """Return a new tree that searches a dataset's collections in
        order for the first match for each dataset type and data ID.

        Parameters
        ----------
        dataset_type : `str`
            Name of the dataset type.  Must be available in the query tree
            already.

        Returns
        -------
        result : `QueryTree`
            A new tree that includes the find-first search.
        """
        if self.find_first_dataset is not None:
            raise InvalidQueryTreeError(
                f"Cannot add a find-first search for {dataset_type!r} to a query that already has a "
                f"find-first search for {self.find_first_dataset!r}."
            )
        if dataset_type not in self.datasets:
            raise InvalidQueryTreeError(
                f"Dataset {dataset_type!r} must be joined in before it can be used in a find-first search."
            )
        return self.model_copy(update=dict(find_first_dataset=dataset_type))

    @pydantic.model_validator(mode="after")
    def _validate_join_operands(self) -> QueryTree:
        for dimensions in self.join_operand_dimensions:
            if not dimensions.issubset(self.dimensions):
                raise InvalidQueryTreeError(
                    f"Dimensions {dimensions} of join operand are not a "
                    f"subset of the query tree's dimensions {self.dimensions}."
                )
        return self

    @pydantic.model_validator(mode="after")
    def _validate_required_columns(self) -> QueryTree:
        for column in self.predicate.gather_required_columns():
            match column:
                case DimensionKeyReference(dimension=dimension):
                    if dimension.name not in self.dimensions:
                        raise InvalidQueryTreeError(
                            f"Column {column} is not in dimensions {self.dimensions}."
                        )
                case DimensionFieldReference(element=element):
                    if element not in self.dimensions.elements:
                        raise InvalidQueryTreeError(
                            f"Column {column} is not in dimensions {self.dimensions}."
                        )
                case DatasetFieldReference(dataset_type=dataset_type):
                    if dataset_type not in self.datasets:
                        raise InvalidQueryTreeError(f"Dataset search for column {column} is not present.")
        return self


class DeferredValidationQueryTree(DeferredValidation[QueryTree]):
    pass
