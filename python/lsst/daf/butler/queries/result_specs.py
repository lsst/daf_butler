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

from functools import cached_property
from typing import Annotated, Literal, TypeAlias, Union

import pydantic

from .._dataset_type import DatasetType
from ..dimensions import DimensionElement, DimensionGroup
from .tree import (
    ColumnReference,
    DatasetFieldReference,
    DimensionFieldReference,
    DimensionKeyReference,
    OrderExpression,
)


class ResultSpecBase(pydantic.BaseModel):
    """Base class for all query-result specification objects."""

    order_by: tuple[OrderExpression, ...] = ()
    """Expressions to sort the rows by."""

    offset: int = 0
    """Index of the first row to return."""

    limit: int | None = None
    """Maximum number of rows to return, or `None` for no bound."""


class DataCoordinateResultSpec(ResultSpecBase):
    """Specification for a query that yields `DataCoordinate` objects."""

    result_type: Literal["data_coordinate"] = "data_coordinate"
    dimensions: DimensionGroup
    include_dimension_records: bool

    @cached_property
    def columns(self) -> tuple[ColumnReference, ...]:
        result = [
            DimensionKeyReference.model_construct(dimension=self.dimensions.universe.dimensions[name])
            for name in self.dimensions.data_coordinate_keys
        ]
        if self.include_dimension_records:
            raise NotImplementedError("TODO")
        return tuple(result)


class DimensionRecordResultSpec(ResultSpecBase):
    """Specification for a query that yields `DimensionRecord` objects."""

    result_type: Literal["dimension_record"] = "dimension_record"
    element: DimensionElement

    @property
    def dimensions(self) -> DimensionGroup:
        return self.element.minimal_group

    @cached_property
    def columns(self) -> tuple[ColumnReference, ...]:
        result: list[ColumnReference] = [
            DimensionKeyReference.model_construct(dimension=dimension) for dimension in self.element.required
        ]
        result.extend(
            DimensionKeyReference.model_construct(dimension=dimension) for dimension in self.element.implied
        )
        result.extend(
            DimensionFieldReference.model_construct(element=self.element, field=field)
            for field in self.element.schema.remainder.names
        )
        return tuple(result)


class DatasetRefResultSpec(ResultSpecBase):
    """Specification for a query that yields `DatasetRef` objects."""

    result_type: Literal["dataset_ref"] = "dataset_ref"
    dataset_type: DatasetType
    include_dimension_records: bool

    @property
    def dimensions(self) -> DimensionGroup:
        return self.dataset_type.dimensions.as_group()

    @cached_property
    def columns(self) -> tuple[ColumnReference, ...]:
        result: list[ColumnReference] = [
            DimensionKeyReference.model_construct(dimension=self.dimensions.universe.dimensions[name])
            for name in self.dimensions.data_coordinate_keys
        ]
        if self.include_dimension_records:
            raise NotImplementedError("TODO")
        result.append(
            DatasetFieldReference.model_construct(dataset_type=self.dataset_type.name, field="dataset_id")
        )
        result.append(DatasetFieldReference.model_construct(dataset_type=self.dataset_type.name, field="run"))
        return tuple(result)


class GeneralResultSpec(pydantic.BaseModel):
    """Specification for a query that yields a table with
    an explicit list of columns.
    """

    result_type: Literal["general"] = "general"
    columns: tuple[ColumnReference, ...]

    @property
    def dimensions(self) -> DimensionGroup:
        raise NotImplementedError()


ResultSpec: TypeAlias = Annotated[
    Union[DataCoordinateResultSpec, DimensionRecordResultSpec, DatasetRefResultSpec, GeneralResultSpec],
    pydantic.Field(discriminator="result_type"),
]
