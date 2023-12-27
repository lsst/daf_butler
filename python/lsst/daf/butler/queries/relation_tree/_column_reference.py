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

__all__ = ("ColumnReference", "DimensionKeyReference", "DimensionFieldReference", "DatasetFieldReference")

from typing import Annotated, Literal, TypeAlias, Union, final

import pydantic

from ...column_spec import ColumnType
from ...dimensions import Dimension, DimensionElement
from ._base import ColumnExpressionBase, DatasetFieldName, InvalidRelationError


@final
class DimensionKeyReference(ColumnExpressionBase):
    """A column expression that references a dimension primary key column."""

    expression_type: Literal["dimension_key"] = "dimension_key"

    dimension: Dimension
    """Definition of this dimension."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        return {self}

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 0

    @property
    def qualified_name(self) -> str:
        # Docstring inherited.
        return self.dimension.name

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        return self.dimension.primary_key.type

    def __str__(self) -> str:
        return self.dimension.name


@final
class DimensionFieldReference(ColumnExpressionBase):
    """A column expression that references a dimension record column that is
    not a primary key.
    """

    expression_type: Literal["dimension_field"] = "dimension_field"

    element: DimensionElement
    """Definition of the dimension element."""

    field: str
    """Name of the field (i.e. column) in the element's logical table."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        return {self}

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 0

    @property
    def qualified_name(self) -> str:
        # Docstring inherited.
        return f"{self.element}:{self.field}"

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        return self.element.schema.remainder[self.field].type

    def __str__(self) -> str:
        return f"{self.element}.{self.field}"

    @pydantic.model_validator(mode="after")
    def _validate_field(self) -> DimensionFieldReference:
        if self.field not in self.element.schema.remainder.names:
            raise InvalidRelationError(f"Dimension field {self.element.name}.{self.field} does not exist.")
        return self


@final
class DatasetFieldReference(ColumnExpressionBase):
    """A column expression that references a column associated with a dataset
    type.
    """

    expression_type: Literal["dataset_field"] = "dataset_field"

    dataset_type: str
    """Name of the dataset type to match any dataset type."""

    field: DatasetFieldName
    """Name of the field (i.e. column) in the dataset's logical table."""

    def gather_required_columns(self) -> set[ColumnReference]:
        # Docstring inherited.
        return {self}

    @property
    def precedence(self) -> int:
        # Docstring inherited.
        return 0

    @property
    def qualified_name(self) -> str:
        # Docstring inherited.
        return f"{self.dataset_type}:{self.field}"

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        match self.field:
            case "dataset_id":
                return "uuid"
            case "ingest_date":
                return "datetime"
            case "run":
                return "string"
            case "collection":
                return "string"
            case "rank":
                return "int"
            case "timespan":
                return "timespan"
        raise AssertionError(f"Invalid field {self.field!r} for dataset.")

    def __str__(self) -> str:
        return f"{self.dataset_type}.{self.field}"


# Union without Pydantic annotation for the discriminator, for use in nesting
# in other unions that will add that annotation.  It's not clear whether it
# would work to just nest the annotated ones, but it seems safest not to rely
# on undocumented behavior.
_ColumnReference: TypeAlias = Union[
    DimensionKeyReference,
    DimensionFieldReference,
    DatasetFieldReference,
]

ColumnReference: TypeAlias = Annotated[_ColumnReference, pydantic.Field(discriminator="expression_type")]
