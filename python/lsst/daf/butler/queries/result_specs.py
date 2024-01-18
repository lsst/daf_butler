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

from typing import Annotated, Literal, TypeAlias, Union, cast

import pydantic

from ..dimensions import DimensionElement, DimensionGroup
from .tree import ColumnReference, ColumnSet, InvalidQueryTreeError, OrderExpression, QueryTree


class ResultSpecBase(pydantic.BaseModel):
    """Base class for all query-result specification objects."""

    order_by: tuple[OrderExpression, ...] = ()
    """Expressions to sort the rows by."""

    offset: int = 0
    """Index of the first row to return."""

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
            raise InvalidQueryTreeError(
                f"Query result specification has dimensions {spec.dimensions} that are not a subset of the "
                f"query's dimensions {tree.dimensions}."
            )
        order_by_columns = ColumnSet(spec.dimensions)
        for term in spec.order_by:
            term.gather_required_columns(order_by_columns)
        if not (order_by_columns.dimensions <= spec.dimensions):
            raise InvalidQueryTreeError(
                "Order-by expression may not reference columns that are not in the result dimensions."
            )
        for dataset_type in order_by_columns.dataset_fields.keys():
            if dataset_type not in tree.available_result_datasets:
                raise InvalidQueryTreeError(
                    f"Dataset type {dataset_type!r} in order-by expression is not part of the query."
                )
            if not (tree.datasets[dataset_type].dimensions <= spec.dimensions):
                raise InvalidQueryTreeError(
                    f"Dataset type {dataset_type!r} in order-by expression has dimensions "
                    f"{tree.datasets[dataset_type].dimensions} that are not a subset of the query "
                    f"dimensions {tree.dimensions}."
                )
        result_columns = spec.get_result_columns()
        assert result_columns.dimensions == spec.dimensions, "enforced by ResultSpec implementations"
        if result_columns.dataset_fields.keys() <= tree.available_result_datasets:
            raise InvalidQueryTreeError(
                "Dataset or datasets "
                f"{result_columns.dataset_fields.keys() - tree.available_result_datasets} "
                "are not available from this query."
            )


class DataCoordinateResultSpec(ResultSpecBase):
    """Specification for a query that yields `DataCoordinate` objects."""

    result_type: Literal["data_coordinate"] = "data_coordinate"
    dimensions: DimensionGroup
    include_dimension_records: bool

    def get_result_columns(self) -> ColumnSet:
        """Return the columns included in the actual result rows.

        This does not necessarily include all columns required by the
        `order_by` terms that are also a part of this spec.
        """
        result = ColumnSet(self.dimensions)
        if self.include_dimension_records:
            for element_name in self.dimensions.elements:
                element = self.dimensions.universe[element_name]
                if not element.is_cached:
                    result.dimension_fields[element_name].update(element.schema.remainder.names)
        return result


class DimensionRecordResultSpec(ResultSpecBase):
    """Specification for a query that yields `DimensionRecord` objects."""

    result_type: Literal["dimension_record"] = "dimension_record"
    element: DimensionElement

    @property
    def dimensions(self) -> DimensionGroup:
        return self.element.minimal_group

    def get_result_columns(self) -> ColumnSet:
        """Return the columns included in the actual result rows.

        This does not necessarily include all columns required by the
        `order_by` terms that are also a part of this spec.
        """
        result = ColumnSet(self.element.minimal_group)
        result.dimension_fields[self.element.name].update(self.element.schema.remainder.names)
        return result


class DatasetRefResultSpec(ResultSpecBase):
    """Specification for a query that yields `DatasetRef` objects."""

    result_type: Literal["dataset_ref"] = "dataset_ref"
    dataset_type_name: str
    dimensions: DimensionGroup
    storage_class_name: str
    include_dimension_records: bool

    def get_result_columns(self) -> ColumnSet:
        """Return the columns included in the actual result rows.

        This does not necessarily include all columns required by the
        `order_by` terms that are also a part of this spec.
        """
        result = ColumnSet(self.dimensions)
        result.dataset_fields[self.dataset_type_name].update({"dataset_id", "run"})
        if self.include_dimension_records:
            for element_name in self.dimensions.elements:
                element = self.dimensions.universe[element_name]
                if not element.is_cached:
                    result.dimension_fields[element_name].update(element.schema.remainder.names)
        return result


class GeneralResultSpec(ResultSpecBase):
    """Specification for a query that yields a table with
    an explicit list of columns.
    """

    result_type: Literal["general"] = "general"
    columns: tuple[ColumnReference, ...]

    @property
    def dimensions(self) -> DimensionGroup:
        raise NotImplementedError()

    def get_result_columns(self) -> ColumnSet:
        """Return the columns included in the actual result rows.

        This does not necessarily include all columns required by the
        `order_by` terms that are also a part of this spec.
        """
        result = ColumnSet(self.dimensions)
        for column in self.columns:
            column.gather_required_columns(result)
        return result


ResultSpec: TypeAlias = Annotated[
    Union[DataCoordinateResultSpec, DimensionRecordResultSpec, DatasetRefResultSpec, GeneralResultSpec],
    pydantic.Field(discriminator="result_type"),
]
