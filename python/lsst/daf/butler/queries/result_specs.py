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
    "ResultSpecBase",
    "DataCoordinateResultSpec",
    "DimensionRecordResultSpec",
    "DatasetRefResultSpec",
)

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Annotated, Literal, TypeAlias, cast

import pydantic

from .._exceptions import InvalidQueryError
from ..dimensions import DimensionElement, DimensionGroup, DimensionUniverse
from ..pydantic_utils import DeferredValidation
from .tree import ColumnSet, DatasetFieldName, OrderExpression, QueryTree


class ResultSpecBase(pydantic.BaseModel, ABC):
    """Base class for all query-result specification objects.

    A result specification is a struct that is combined with a `QueryTree` to
    represent a serializable query-results object.
    """

    result_type: str
    """String literal that corresponds to a concrete derived type."""

    order_by: tuple[OrderExpression, ...] = ()
    """Expressions to sort the rows by."""

    limit: int | None = None
    """Maximum number of rows to return, or `None` for no bound."""

    def validate_tree(self, tree: QueryTree) -> None:
        """Check that this result object is consistent with a query tree.

        Parameters
        ----------
        tree : `QueryTree`
            Query tree that defines the joins and row-filtering that these
            results will come from.
        """
        spec = cast(ResultSpec, self)
        if not spec.dimensions <= tree.dimensions:
            raise InvalidQueryError(
                f"Query result specification has dimensions {spec.dimensions} that are not a subset of the "
                f"query's dimensions {tree.dimensions}."
            )
        result_columns = spec.get_result_columns()
        assert result_columns.dimensions == spec.dimensions, "enforced by ResultSpec implementations"
        for dataset_type in result_columns.dataset_fields:
            if dataset_type not in tree.datasets:
                raise InvalidQueryError(f"Dataset {dataset_type!r} is not available from this query.")
        order_by_columns = ColumnSet(spec.dimensions)
        for term in spec.order_by:
            term.gather_required_columns(order_by_columns)
        if not (order_by_columns.dimensions <= spec.dimensions):
            raise InvalidQueryError(
                "Order-by expression may not reference columns that are not in the result dimensions."
            )
        for dataset_type in order_by_columns.dataset_fields.keys():
            if dataset_type not in tree.datasets:
                raise InvalidQueryError(
                    f"Dataset type {dataset_type!r} in order-by expression is not part of the query."
                )

    @property
    def find_first_dataset(self) -> str | None:
        """The dataset type for which find-first resolution is required, if
        any.
        """
        return None

    @abstractmethod
    def get_result_columns(self) -> ColumnSet:
        """Return the columns included in the actual result rows.

        This does not necessarily include all columns required by the
        `order_by` terms that are also a part of this spec.
        """
        raise NotImplementedError()


class DataCoordinateResultSpec(ResultSpecBase):
    """Specification for a query that yields `DataCoordinate` objects."""

    result_type: Literal["data_coordinate"] = "data_coordinate"

    dimensions: DimensionGroup
    """The dimensions of the data IDs returned by this query."""

    include_dimension_records: bool = False
    """Whether the returned data IDs include dimension records."""

    def get_result_columns(self) -> ColumnSet:
        # Docstring inherited.
        result = ColumnSet(self.dimensions)
        if self.include_dimension_records:
            _add_dimension_records_to_column_set(self.dimensions, result)
        return result


class DimensionRecordResultSpec(ResultSpecBase):
    """Specification for a query that yields `DimensionRecord` objects."""

    result_type: Literal["dimension_record"] = "dimension_record"

    element: DimensionElement
    """The name and definition of the dimension records returned by this query.
    """

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions that are required or implied (directly or indirectly)
        by this dimension element.
        """
        return self.element.minimal_group

    def get_result_columns(self) -> ColumnSet:
        # Docstring inherited.
        result = ColumnSet(self.element.minimal_group)
        if self.element not in self.dimensions.universe.skypix_dimensions:
            result.dimension_fields[self.element.name].update(self.element.schema.remainder.names)
        result.drop_dimension_keys(self.element.minimal_group.names - self.element.dimensions.names)
        return result


class DatasetRefResultSpec(ResultSpecBase):
    """Specification for a query that yields `DatasetRef` objects."""

    result_type: Literal["dataset_ref"] = "dataset_ref"

    dataset_type_name: str
    """The dataset type name of the datasets returned by this query."""

    dimensions: DimensionGroup
    """The dimensions of the datasets returned by this query."""

    storage_class_name: str
    """The name of the storage class of the datasets returned by this query."""

    include_dimension_records: bool = False
    """Whether the data IDs returned by this query include dimension records.
    """

    find_first: bool
    """Whether this query should resolve data ID duplicates according to the
    order of the collections to be searched.
    """

    @property
    def find_first_dataset(self) -> str | None:
        # Docstring inherited.
        return self.dataset_type_name if self.find_first else None

    def get_result_columns(self) -> ColumnSet:
        # Docstring inherited.
        result = ColumnSet(self.dimensions)
        result.dataset_fields[self.dataset_type_name].update({"dataset_id", "run"})
        if self.include_dimension_records:
            _add_dimension_records_to_column_set(self.dimensions, result)
        return result


class GeneralResultSpec(ResultSpecBase):
    """Specification for a query that yields a table with
    an explicit list of columns.
    """

    result_type: Literal["general"] = "general"

    dimensions: DimensionGroup
    """The dimensions that span all fields returned by this query."""

    dimension_fields: Mapping[str, set[str]]
    """Dimension record fields included in this query."""

    dataset_fields: Mapping[str, set[DatasetFieldName]]
    """Dataset fields included in this query."""

    find_first: bool
    """Whether this query requires find-first resolution for a dataset.

    This can only be `True` if exactly one dataset type's fields are included
    in the results and the ``collection`` and ``timespan`` fields for that
    dataset are not included.
    """

    @property
    def find_first_dataset(self) -> str | None:
        # Docstring inherited.
        if self.find_first:
            if len(self.dataset_fields) != 1:
                raise InvalidQueryError(
                    "General query with find_first=True cannot have results from multiple "
                    "dataset searches."
                )
            (dataset_type,) = self.dataset_fields.keys()
            return dataset_type
        return None

    def get_result_columns(self) -> ColumnSet:
        # Docstring inherited.
        result = ColumnSet(self.dimensions)
        for element_name, fields_for_element in self.dimension_fields.items():
            result.dimension_fields[element_name].update(fields_for_element)
        for dataset_type, fields_for_dataset in self.dataset_fields.items():
            result.dataset_fields[dataset_type].update(fields_for_dataset)
        return result

    @pydantic.model_validator(mode="after")
    def _validate(self) -> GeneralResultSpec:
        if self.find_first and len(self.dataset_fields) != 1:
            raise InvalidQueryError("find_first=True requires exactly one result dataset type.")
        for element_name, fields_for_element in self.dimension_fields.items():
            if element_name not in self.dimensions.elements:
                raise InvalidQueryError(f"Dimension element {element_name} is not in {self.dimensions}.")
            if not fields_for_element:
                raise InvalidQueryError(
                    f"Empty dimension element field set for {element_name!r} is not permitted."
                )
            elif element_name in self.dimensions.universe.skypix_dimensions.names:
                raise InvalidQueryError(
                    f"Regions for skypix dimension {element_name!r} are not stored; compute them via "
                    f"{element_name}.pixelization.pixel(id) instead."
                )
        for dataset_type, fields_for_dataset in self.dataset_fields.items():
            if not fields_for_dataset:
                raise InvalidQueryError(f"Empty dataset field set for {dataset_type!r} is not permitted.")
        return self


ResultSpec: TypeAlias = Annotated[
    DataCoordinateResultSpec | DimensionRecordResultSpec | DatasetRefResultSpec | GeneralResultSpec,
    pydantic.Field(discriminator="result_type"),
]


class SerializedResultSpec(DeferredValidation[ResultSpec]):
    def to_result_spec(self, universe: DimensionUniverse) -> ResultSpec:
        return self.validated(universe=universe)


def _add_dimension_records_to_column_set(dimensions: DimensionGroup, column_set: ColumnSet) -> None:
    """Add extra columns for generating 'expanded' data IDs that include
    dimension records.
    """
    for element_name in dimensions.elements:
        element = dimensions.universe[element_name]
        if not element.is_cached and element not in dimensions.universe.skypix_dimensions:
            column_set.dimension_fields[element_name].update(element.schema.remainder.names)
