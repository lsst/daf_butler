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
    "make_identity_query_tree",
    "DataCoordinateUploadKey",
    "MaterializationKey",
    "DatasetSearch",
    "SerializedQueryTree",
)

import uuid
from collections.abc import Mapping
from typing import TypeAlias, final

import pydantic

from ..._exceptions import InvalidQueryError
from ...dimensions import DimensionGroup, DimensionUniverse
from ...pydantic_utils import DeferredValidation
from ._base import QueryTreeBase
from ._column_set import ColumnSet
from ._predicate import Predicate

DataCoordinateUploadKey: TypeAlias = uuid.UUID

MaterializationKey: TypeAlias = uuid.UUID


def make_identity_query_tree(universe: DimensionUniverse) -> QueryTree:
    """Make an initial query tree with empty dimensions and a single logical
    row.

    This method should be used by `Butler._query` to construct the initial
    query tree.  This tree is a useful initial state because it is the
    identity for joins, in that joining any other query tree to the identity
    yields that query tree.

    Parameters
    ----------
    universe : `..DimensionUniverse`
        Definitions for all dimensions.

    Returns
    -------
    tree : `QueryTree`
        A tree with empty dimensions.
    """
    return QueryTree(dimensions=universe.empty)


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
    """A declarative, serializable description of the row constraints and joins
    in a butler query.

    Notes
    -----
    A `QueryTree` is the struct that represents the serializable form of a
    `Query` object, or one piece (with `ResultSpec` the other) of the
    serializable form of a query results object.

    This class's attributes describe the columns that are "available" to be
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

    materializations: Mapping[MaterializationKey, DimensionGroup] = pydantic.Field(default_factory=dict)
    """Tables of result rows from other queries that have been stored
    temporarily on the server.
    """

    predicate: Predicate = Predicate.from_bool(True)
    """Boolean expression trees whose logical AND defines a row filter."""

    def get_joined_dimension_groups(self) -> frozenset[DimensionGroup]:
        """Return a set of the dimension groups of all data coordinate uploads,
        dataset searches, and materializations.
        """
        result: set[DimensionGroup] = set(self.data_coordinate_uploads.values())
        result.update(self.materializations.values())
        for dataset_spec in self.datasets.values():
            result.add(dataset_spec.dimensions)
        return frozenset(result)

    def join_dimensions(self, dimensions: DimensionGroup) -> QueryTree:
        """Return a new tree that includes additional dimensions.

        Parameters
        ----------
        dimensions : `DimensionGroup`
            Dimensions to include.

        Returns
        -------
        result : `QueryTree`
            A new tree with the additional dimensions.
        """
        return self.model_copy(update=dict(dimensions=self.dimensions | dimensions))

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
        assert key not in self.data_coordinate_uploads, "Query should prevent doing the same upload twice."
        data_coordinate_uploads = dict(self.data_coordinate_uploads)
        data_coordinate_uploads[key] = dimensions
        return self.model_copy(
            update=dict(
                dimensions=self.dimensions | dimensions, data_coordinate_uploads=data_coordinate_uploads
            )
        )

    def join_materialization(self, key: MaterializationKey, dimensions: DimensionGroup) -> QueryTree:
        """Return a new tree that joins in temporarily stored results from
        another query.

        Parameters
        ----------
        key : `MaterializationKey`
            Unique identifier for this materialization, as assigned by a
            `QueryDriver`.
        dimensions : `DimensionGroup`
            The dimensions stored in the materialization.

        Returns
        -------
        result : `QueryTree`
            A new tree that joins in the materialization.
        """
        assert key not in self.data_coordinate_uploads, "Query should prevent duplicate materialization."
        materializations = dict(self.materializations)
        materializations[key] = dimensions
        return self.model_copy(
            update=dict(dimensions=self.dimensions | dimensions, materializations=materializations)
        )

    def join_dataset(self, dataset_type: str, search: DatasetSearch) -> QueryTree:
        """Return a new tree that joins in a search for a dataset.

        Parameters
        ----------
        dataset_type : `str`
            Name of dataset type to join in.
        search : `DatasetSearch`
            Struct containing the collection search path and dataset type
            dimensions.

        Returns
        -------
        result : `QueryTree`
            A new tree that joins in the dataset search.
        """
        if existing := self.datasets.get(dataset_type):
            assert existing == search, "Dataset search should be new or the same."
            return self
        else:
            datasets = dict(self.datasets)
            datasets[dataset_type] = search
            return self.model_copy(
                update=dict(dimensions=self.dimensions | search.dimensions, datasets=datasets)
            )

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
        predicate = self.predicate
        columns = ColumnSet(self.dimensions)
        for where_term in terms:
            where_term.gather_required_columns(columns)
            predicate = predicate.logical_and(where_term)
        if not (columns.dataset_fields.keys() <= self.datasets.keys()):
            raise InvalidQueryError(
                f"Cannot reference dataset type(s) {columns.dataset_fields.keys() - self.datasets.keys()} "
                "that have not been joined."
            )
        return self.model_copy(update=dict(dimensions=columns.dimensions, predicate=predicate))

    @pydantic.model_validator(mode="after")
    def _validate_join_operands(self) -> QueryTree:
        for dimensions in self.get_joined_dimension_groups():
            if not dimensions.issubset(self.dimensions):
                raise InvalidQueryError(
                    f"Dimensions {dimensions} of join operand are not a "
                    f"subset of the query tree's dimensions {self.dimensions}."
                )
        return self

    @pydantic.model_validator(mode="after")
    def _validate_required_columns(self) -> QueryTree:
        columns = ColumnSet(self.dimensions)
        self.predicate.gather_required_columns(columns)
        if not columns.dimensions.issubset(self.dimensions):
            raise InvalidQueryError("Predicate requires dimensions beyond those in the query tree.")
        if not columns.dataset_fields.keys() <= self.datasets.keys():
            raise InvalidQueryError("Predicate requires dataset columns that are not in the query tree.")
        return self


class SerializedQueryTree(DeferredValidation[QueryTree]):
    """A Pydantic-serializable wrapper for `QueryTree` that defers validation
    to the `validated` method, allowing a `.DimensionUniverse` to be provided.
    """

    def to_query_tree(self, universe: DimensionUniverse) -> QueryTree:
        return self.validated(universe=universe)
