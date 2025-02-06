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

__all__ = ("ColumnReference", "DatasetFieldReference", "DimensionFieldReference", "DimensionKeyReference")

from typing import TYPE_CHECKING, ClassVar, Literal, TypeAlias, TypeVar, Union, final

import pydantic

from ..._exceptions import InvalidQueryError
from ...column_spec import ColumnType
from ...dimensions import Dimension, DimensionElement
from ._base import ANY_DATASET, AnyDatasetType, ColumnExpressionBase, DatasetFieldName

if TYPE_CHECKING:
    from ..visitors import ColumnExpressionVisitor
    from ._column_set import ColumnSet


_T = TypeVar("_T")


@final
class DimensionKeyReference(ColumnExpressionBase):
    """A column expression that references a dimension primary key column."""

    expression_type: Literal["dimension_key"] = "dimension_key"

    is_column_reference: ClassVar[bool] = True

    dimension: Dimension
    """Definition and name of this dimension."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        columns.update_dimensions(self.dimension.minimal_group)

    def gather_governors(self, governors: set[str]) -> None:
        if self.dimension.governor is not None and self.dimension.governor is not self.dimension:
            governors.add(self.dimension.governor.name)

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        return self.dimension.primary_key.type

    def __str__(self) -> str:
        return self.dimension.name

    def visit(self, visitor: ColumnExpressionVisitor[_T]) -> _T:
        # Docstring inherited.
        return visitor.visit_dimension_key_reference(self)


@final
class DimensionFieldReference(ColumnExpressionBase):
    """A column expression that references a dimension record column that is
    not a primary key.
    """

    expression_type: Literal["dimension_field"] = "dimension_field"

    is_column_reference: ClassVar[bool] = True

    element: DimensionElement
    """Definition and name of the dimension element."""

    field: str
    """Name of the field (i.e. column) in the element's logical table."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        columns.update_dimensions(self.element.minimal_group)
        columns.dimension_fields[self.element.name].add(self.field)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        # We assume metadata fields (and certainly region and timespan fields)
        # don't need to be qualified with (e.g.) an instrument or skymap name
        # to make sense, but keys like visit IDs or detector names do need to
        # e qualified.
        if (
            isinstance(self.element, Dimension)
            and self.field in self.element.alternate_keys.names
            and self.element.governor is not None
        ):
            governors.add(self.element.governor.name)

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        return self.element.schema.remainder[self.field].type

    def __str__(self) -> str:
        return f"{self.element}.{self.field}"

    def visit(self, visitor: ColumnExpressionVisitor[_T]) -> _T:
        # Docstring inherited.
        return visitor.visit_dimension_field_reference(self)

    @pydantic.model_validator(mode="after")
    def _validate_field(self) -> DimensionFieldReference:
        if self.field not in self.element.schema.remainder.names:
            raise InvalidQueryError(f"Dimension field {self.element.name}.{self.field} does not exist.")
        return self


@final
class DatasetFieldReference(ColumnExpressionBase):
    """A column expression that references a column associated with a dataset
    type.
    """

    expression_type: Literal["dataset_field"] = "dataset_field"

    is_column_reference: ClassVar[bool] = True

    dataset_type: AnyDatasetType | str
    """Name of the dataset type, or ``ANY_DATSET`` to match any dataset type.
    """

    field: DatasetFieldName
    """Name of the field (i.e. column) in the dataset's logical table."""

    def gather_required_columns(self, columns: ColumnSet) -> None:
        # Docstring inherited.
        columns.dataset_fields[self.dataset_type].add(self.field)

    def gather_governors(self, governors: set[str]) -> None:
        # Docstring inherited.
        pass

    @property
    def column_type(self) -> ColumnType:
        # Docstring inherited.
        match self.field:
            case "dataset_id":
                return "uuid"
            case "ingest_date":
                # See comment at definition of `ColumnType` type alias.
                return "ingest_date"
            case "run":
                return "string"
            case "collection":
                return "string"
            case "timespan":
                return "timespan"
        raise AssertionError(f"Invalid field {self.field!r} for dataset.")

    def __str__(self) -> str:
        if self.dataset_type is ANY_DATASET:
            return self.field
        else:
            return f"{self.dataset_type}.{self.field}"

    def visit(self, visitor: ColumnExpressionVisitor[_T]) -> _T:
        # Docstring inherited.
        return visitor.visit_dataset_field_reference(self)


ColumnReference: TypeAlias = Union[
    DimensionKeyReference,
    DimensionFieldReference,
    DatasetFieldReference,
]
