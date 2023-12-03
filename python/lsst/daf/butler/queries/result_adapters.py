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
    "DataCoordinateResultSpec",
    "DimensionRecordResultSpec",
    "DatasetRefResultSpec",
    "GeneralResultSpec",
    "ResultSpec",
)

from typing import Annotated, Literal, TypeAlias, Union

import pydantic

from ..dimensions import DimensionElement, DimensionGroup
from .relation_tree import ColumnReference


class DataCoordinateResultSpec(pydantic.BaseModel):
    """Specification for a query that yields `DataCoordinate` objects."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid", strict=True)
    result_type: Literal["data_coordinate"] = "data_coordinate"
    dimensions: DimensionGroup
    with_dimension_records: bool


class DimensionRecordResultSpec(pydantic.BaseModel):
    """Specification for a query that yields `DimensionRecord` objects."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid", strict=True)
    result_type: Literal["dimension_record"] = "dimension_record"
    element: DimensionElement


class DatasetRefResultSpec(pydantic.BaseModel):
    """Specification for a query that yields `DatasetRef` objects."""

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid", strict=True)
    result_type: Literal["dataset_ref"] = "dataset_ref"
    dataset_type_name: str | None
    dimensions: DimensionGroup
    with_dimension_records: bool


class GeneralResultSpec(pydantic.BaseModel):
    """Specification for a query that yields a table with
    an explicit list of columns.
    """

    model_config = pydantic.ConfigDict(frozen=True, extra="forbid", strict=True)
    result_type: Literal["general"] = "general"
    columns: tuple[ColumnReference]


ResultSpec: TypeAlias = Annotated[
    Union[DataCoordinateResultSpec, DimensionRecordResultSpec, DatasetRefResultSpec, GeneralResultSpec],
    pydantic.Field(discriminator="result_type"),
]
